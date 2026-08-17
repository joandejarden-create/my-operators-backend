#!/usr/bin/env node
/**
 * BWH census enrichment: seed extract → match → dry-run / --apply Website + Property ID.
 * Amenities only when hotelDetails returns real labels (often captcha-blocked).
 *
 *   node scripts/run-bwh-census-enrichment.mjs
 *   node scripts/run-bwh-census-enrichment.mjs --apply
 *   node scripts/run-bwh-census-enrichment.mjs --apply --fetch-amenities
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  DEFAULT_BWH_SEED_JSON,
  extractBwhAmenitiesFromHotelDetails,
  fetchBwhHotelDetails,
  loadBwhDirectorySeed,
} from "../lib/bwh-brand-directory-extract.js";
import {
  MAP_BWH_CENSUS_BACKFILL,
  planBwhCensusDirectoryMatch,
  validateBwhCensusApplyRow,
} from "../lib/hotel-census/plan-bwh-census-directory-match.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    fetchAmenities: args.includes("--fetch-amenities"),
    seedPath: args.find((a) => a.startsWith("--seed="))?.split("=")[1] || DEFAULT_BWH_SEED_JSON,
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 300),
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const opts = parseArgs();
  mkdirSync(REPORTS, { recursive: true });

  const seedRows = loadBwhDirectorySeed(opts.seedPath);
  writeFileSync(
    join(REPORTS, "bwh-cala-directory-extract.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        seedPath: opts.seedPath,
        propertyRows: seedRows,
        count: seedRows.length,
      },
      null,
      2
    )
  );
  console.log("=== BWH directory seed extract ===");
  console.log("Seed rows:", seedRows.length);

  console.log("\n=== BWH census directory match ===\n");
  const plan = await planBwhCensusDirectoryMatch({ seedPath: opts.seedPath });
  writeFileSync(
    join(REPORTS, "bwh-census-enrichment-plan.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2)
  );

  writeFileSync(
    join(REPORTS, "bwh-census-steward-review.csv"),
    [
      "censusRecordId,censusName,censusCountry,propertyId,propertyUrl,matchScore,matchConfidence,nameSim,reason",
      ...[...plan.stewardReview, ...plan.skipped.filter((s) => s.reason === "no_directory_match")].map(
        (r) =>
          [
            r.censusRecordId,
            r.censusName,
            r.censusCountry,
            r.propertyId,
            r.propertyUrl,
            r.matchScore,
            r.matchConfidence,
            r.nameSim,
            r.reason,
          ]
            .map(csvEscape)
            .join(",")
      ),
    ].join("\n")
  );

  console.log("Census scanned:", plan.censusRowsScanned);
  console.log("Ready to apply:", plan.readyToApply);
  console.log("Steward / unmatched:", plan.stewardReview.length, plan.skipped.length);

  const validated = [];
  const validationFailed = [];
  for (const row of plan.planRows) {
    const v = validateBwhCensusApplyRow(row);
    if (v.pass) validated.push(row);
    else validationFailed.push({ ...row, validationErrors: v.errors });
  }
  if (validationFailed.length) {
    writeFileSync(
      join(REPORTS, "bwh-census-validation-failed.json"),
      JSON.stringify(validationFailed, null, 2)
    );
    console.log("Validation failed:", validationFailed.length);
  }

  /** Optional amenities — only real labels from hotelDetails */
  /** @type {object[]} */
  const amenityPlans = [];
  if (opts.fetchAmenities) {
    console.log("\n=== BWH hotelDetails amenities probe ===\n");
    for (const row of validated) {
      if (!row.propertyId) continue;
      const fetched = await fetchBwhHotelDetails(row.propertyId);
      await sleep(opts.delayMs);
      if (!fetched.ok) {
        amenityPlans.push({
          censusRecordId: row.censusRecordId,
          propertyId: row.propertyId,
          status: "blocked_or_empty",
          error: fetched.error,
        });
        continue;
      }
      const labels = extractBwhAmenitiesFromHotelDetails(fetched.json);
      if (!labels.length) {
        amenityPlans.push({
          censusRecordId: row.censusRecordId,
          propertyId: row.propertyId,
          status: "no_amenity_labels",
        });
        continue;
      }
      amenityPlans.push({
        censusRecordId: row.censusRecordId,
        propertyId: row.propertyId,
        status: "ready",
        amenitiesText: labels.join("; "),
        amenityCount: labels.length,
      });
      if (isBlankCensusValue(row.applyFields?.[MAP_BWH_CENSUS_BACKFILL.amenities])) {
        // Only attach if census amenities blank — checked at apply time against live fields later
        row._amenitiesText = labels.join("; ");
      }
    }
    writeFileSync(
      join(REPORTS, "bwh-census-amenities-fetch.json"),
      JSON.stringify({ generatedAt: new Date().toISOString(), amenityPlans }, null, 2)
    );
  }

  console.log("\nMode:", opts.apply ? "APPLY" : "DRY-RUN");
  console.log("Validated rows:", validated.length);

  if (!opts.apply) {
    writeFileSync(
      join(REPORTS, "bwh-census-apply-dry-run.json"),
      JSON.stringify(
        {
          generatedAt: new Date().toISOString(),
          mode: "dry-run",
          fieldMapping: plan.fieldMapping,
          wouldUpdate: validated.length,
          sample: validated.map((r) => ({
            censusRecordId: r.censusRecordId,
            censusName: r.censusName,
            applyFields: r.applyFields,
            matchConfidence: r.matchConfidence,
            matchScore: r.matchScore,
          })),
        },
        null,
        2
      )
    );
    console.log("Dry-run preview: reports/bwh-census-apply-dry-run.json");
    return;
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");
  const base = new Airtable({ apiKey }).base(baseId);

  let updated = 0;
  let errors = 0;
  /** @type {object[]} */
  const log = [];
  let batch = [];

  async function flush() {
    if (!batch.length) return;
    try {
      await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
    } catch (err) {
      errors += batch.length;
      console.error("Batch failed:", err?.message || err);
      for (const b of batch) log.push({ ...b, error: String(err?.message || err) });
    }
    batch = [];
  }

  for (const row of validated) {
    const fields = { ...row.applyFields };
    if (row._amenitiesText) {
      fields[MAP_BWH_CENSUS_BACKFILL.amenities] = row._amenitiesText;
    }
    log.push({
      censusRecordId: row.censusRecordId,
      censusName: row.censusName,
      propertyId: row.propertyId,
      propertyUrl: row.propertyUrl,
      matchConfidence: row.matchConfidence,
      matchScore: row.matchScore,
      applyFields: fields,
    });
    batch.push({ id: row.censusRecordId, fields });
    if (batch.length >= BATCH) await flush();
  }
  await flush();

  writeFileSync(
    join(REPORTS, "bwh-census-enrichment-apply-log.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: "apply",
        updated,
        errors,
        fieldMapping: plan.fieldMapping,
        rows: log,
      },
      null,
      2
    )
  );
  writeFileSync(
    join(REPORTS, "bwh-census-enrichment-apply-log.csv"),
    [
      "censusRecordId,censusName,propertyId,matchConfidence,matchScore,fields",
      ...log.map((r) =>
        [
          r.censusRecordId,
          r.censusName,
          r.propertyId,
          r.matchConfidence,
          r.matchScore,
          Object.keys(r.applyFields || {}).join("|"),
        ]
          .map(csvEscape)
          .join(",")
      ),
    ].join("\n")
  );

  console.log("\nUpdated:", updated, "Errors:", errors);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
