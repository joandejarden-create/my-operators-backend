#!/usr/bin/env node
/**
 * Probe + optional fill-blank Amenities backfill for matched IHG census rows.
 *
 *   node scripts/backfill-ihg-census-amenities.mjs --probe-only --urls=5
 *   node scripts/backfill-ihg-census-amenities.mjs
 *   node scripts/backfill-ihg-census-amenities.mjs --apply
 */
import "../load-env.js";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { IHG_FETCH_HEADERS } from "../lib/ihg-brand-directory-extract.js";
import {
  extractIhgAmenitiesFromHtml,
  formatIhgAmenitiesText,
  ihgHoteldetailLooksBlocked,
} from "../lib/ihg-hotel-amenities-extract.js";
import { MAP_IHG_CENSUS_BACKFILL } from "../lib/hotel-census/plan-ihg-census-directory-match.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    probeOnly: args.includes("--probe-only"),
    urls: Number(args.find((a) => a.startsWith("--urls="))?.split("=")[1] || 5),
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 250),
    max: Number(args.find((a) => a.startsWith("--max="))?.split("=")[1] || 0) || null,
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchAmenities(url) {
  const res = await fetch(url, { headers: IHG_FETCH_HEADERS, redirect: "follow" });
  const html = await res.text();
  const blocked = ihgHoteldetailLooksBlocked(html, res.url);
  const amenities = blocked ? [] : extractIhgAmenitiesFromHtml(html);
  return {
    url,
    finalUrl: res.url,
    status: res.status,
    htmlLength: html.length,
    blocked,
    amenityCount: amenities.length,
    amenities,
    amenitiesText: formatIhgAmenitiesText(amenities),
    usable: res.ok && !blocked && amenities.length > 0,
  };
}

async function main() {
  const opts = parseArgs();
  mkdirSync(REPORTS, { recursive: true });

  const planPath = join(REPORTS, "ihg-census-directory-match-plan.json");
  if (!existsSync(planPath)) {
    throw new Error("Missing match plan. Run sync-ihg-census-from-directory.mjs first.");
  }
  const plan = JSON.parse(readFileSync(planPath, "utf8"));
  const rows = (plan.planRows || []).filter((r) => r.propertyUrl);

  console.log("=== Probe amenity fetch (sample) ===\n");
  const probeUrls = rows.slice(0, opts.urls).map((r) => r.propertyUrl);
  const probeResults = [];
  for (const url of probeUrls) {
    const r = await fetchAmenities(url);
    probeResults.push(r);
    console.log(url.slice(0, 70), "usable", r.usable, "count", r.amenityCount, r.amenities.slice(0, 5).join("; "));
    if (opts.delayMs) await sleep(opts.delayMs);
  }
  const probeUsable = probeResults.filter((r) => r.usable).length;
  writeFileSync(
    join(REPORTS, "ihg-amenities-fetch-probe.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        probed: probeResults.length,
        usableCount: probeUsable,
        amenityBackfillRecommended: probeUsable > 0,
        note:
          probeUsable === 0
            ? "Amenity fetch blocked or empty — skip Amenities writes."
            : "Amenities extractable from amenity-title highlights — fill-blank only.",
        results: probeResults,
      },
      null,
      2
    )
  );

  if (probeUsable === 0) {
    console.log("\nSKIP amenity backfill — probe failed.");
    return;
  }
  if (opts.probeOnly) {
    console.log("\nProbe-only complete.");
    return;
  }

  console.log("\n=== Plan amenity backfill for matched rows ===\n");
  const base = getPlatformBase();
  if (!base) throw new Error("Missing Airtable config");

  const amenField = MAP_IHG_CENSUS_BACKFILL.amenities;
  const targets = opts.max ? rows.slice(0, opts.max) : rows;

  // Prefetch current Amenities for matched records (fill-blank only)
  /** @type {Map<string, unknown>} */
  const currentAmenities = new Map();
  const idList = targets.map((r) => r.censusRecordId);
  for (let i = 0; i < idList.length; i += 50) {
    const chunk = idList.slice(i, i + 50);
    const formula = `OR(${chunk.map((id) => `RECORD_ID()='${id}'`).join(",")})`;
    const recs = await base(HOTEL_CENSUS_TABLE)
      .select({ filterByFormula: formula, fields: [amenField], pageSize: 100 })
      .all();
    for (const rec of recs) currentAmenities.set(rec.id, rec.fields?.[amenField]);
  }

  /** @type {object[]} */
  const amenityPlan = [];
  /** @type {object[]} */
  const fetchErrors = [];
  let skippedHasValue = 0;
  let skippedEmpty = 0;

  for (const row of targets) {
    if (!isBlankCensusValue(currentAmenities.get(row.censusRecordId))) {
      skippedHasValue++;
      continue;
    }
    try {
      const fetched = await fetchAmenities(row.propertyUrl);
      if (!fetched.usable) {
        skippedEmpty++;
        fetchErrors.push({
          censusRecordId: row.censusRecordId,
          url: row.propertyUrl,
          reason: fetched.blocked ? "blocked" : "no_amenities_parsed",
        });
        if (opts.delayMs) await sleep(opts.delayMs);
        continue;
      }

      amenityPlan.push({
        censusRecordId: row.censusRecordId,
        censusName: row.censusName,
        propertyUrl: row.propertyUrl,
        propertyId: row.propertyId,
        amenityCount: fetched.amenityCount,
        applyFields: { [amenField]: fetched.amenitiesText },
        amenitiesSample: fetched.amenities.slice(0, 12),
      });
    } catch (err) {
      fetchErrors.push({
        censusRecordId: row.censusRecordId,
        url: row.propertyUrl,
        error: String(err?.message || err),
      });
    }
    if (opts.delayMs) await sleep(opts.delayMs);
  }

  const planOut = {
    generatedAt: new Date().toISOString(),
    fieldMapping: { amenities: amenField },
    targetsConsidered: targets.length,
    readyToApply: amenityPlan.length,
    skippedHasValue,
    skippedEmpty,
    fetchErrors: fetchErrors.length,
    fetchErrorSample: fetchErrors.slice(0, 20),
    planRows: amenityPlan,
  };
  writeFileSync(join(REPORTS, "ihg-census-amenities-backfill-plan.json"), JSON.stringify(planOut, null, 2));
  console.log("Ready amenity fills:", amenityPlan.length);
  console.log("Skipped has value:", skippedHasValue, "empty/blocked:", skippedEmpty);

  if (!opts.apply) {
    console.log("Dry-run only. Re-run with --apply to write Amenities.");
    return;
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  const at = new Airtable({ apiKey }).base(baseId);
  let updated = 0;
  let errors = 0;
  let batch = [];
  const log = [];

  async function flush() {
    if (!batch.length) return;
    try {
      await at(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
    } catch (err) {
      errors += batch.length;
      console.error("Batch failed:", err?.message || err);
    }
    batch = [];
  }

  for (const row of amenityPlan) {
    log.push(row);
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= BATCH) await flush();
  }
  await flush();

  writeFileSync(
    join(REPORTS, "ihg-census-amenities-apply-log.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, errors, rows: log }, null, 2)
  );
  console.log("Amenities updated:", updated, "errors:", errors);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
