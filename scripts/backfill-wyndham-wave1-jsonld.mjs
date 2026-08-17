#!/usr/bin/env node
/**
 * Wyndham Wave 1 (Dazzler + Trademark): fill-blank Property ID + Hotel Description
 * from official overview JSON-LD when Website is already set.
 * Amenities remain soft-blocked when /services-amenities returns empty.
 *
 *   node scripts/backfill-wyndham-wave1-jsonld.mjs
 *   node scripts/backfill-wyndham-wave1-jsonld.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  parseWyndhamHotelMetadataFromHtml,
  WYNDHAM_FETCH_HEADERS,
} from "../lib/wyndham-brand-directory-extract.js";
import { WYNDHAM_WAVE1_AFFILIATIONS } from "../lib/hotel-census/plan-wyndham-census-sitemap-match.js";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const BATCH = 10;

const MAP = {
  website: "Website",
  propertyId: CENSUS_PROPERTY_ID_FIELD,
  description: CENSUS_DESCRIPTION_FIELD,
};

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 200),
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function canonicalizeWyndhamUrl(url) {
  try {
    const u = new URL(String(url || "").trim());
    if (!u.hostname.includes("wyndhamhotels.com")) return "";
    // Strip locale prefix en-ca / en-uk / es-xl etc.
    const parts = u.pathname.split("/").filter(Boolean);
    if (parts[0] && /^[a-z]{2}(?:-[a-z]{2})?$/i.test(parts[0]) && parts[0].includes("-")) {
      parts.shift();
    }
    u.pathname = "/" + parts.join("/");
    u.search = "";
    u.hash = "";
    return u.href.replace(/\/$/, "");
  } catch {
    return "";
  }
}

/**
 * @param {object} planRow
 */
function validatePlanRow(planRow) {
  const errors = [];
  if (!planRow.censusRecordId) errors.push("missing censusRecordId");
  if (!Object.keys(planRow.applyFields || {}).length) errors.push("no applyFields");
  const pid = planRow.applyFields?.[MAP.propertyId];
  if (pid != null && !/^\d{4,6}$/.test(String(pid))) {
    errors.push("Property ID from JSON-LD must be numeric hotel identifier");
  }
  const desc = planRow.applyFields?.[MAP.description];
  if (desc != null && (typeof desc !== "string" || desc.length < 40)) {
    errors.push("Hotel Description too short");
  }
  return { pass: errors.length === 0, errors };
}

async function main() {
  const opts = parseArgs();
  mkdirSync(REPORTS, { recursive: true });
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  const formula = `OR(${WYNDHAM_WAVE1_AFFILIATIONS.map((a) => `{${CENSUS_FIELDS.affiliation}}="${a}"`).join(",")})`;
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        CENSUS_FIELDS.name,
        CENSUS_FIELDS.affiliation,
        MAP.website,
        MAP.propertyId,
        MAP.description,
      ],
      filterByFormula: formula,
      pageSize: 100,
    })
    .all();

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];

  console.log("=== Wyndham Wave1 JSON-LD backfill (PID + Description) ===\n");
  console.log("Census rows:", records.length);

  let n = 0;
  for (const rec of records) {
    const website = String(rec.fields?.[MAP.website] || "").trim();
    if (!website || !/wyndhamhotels\.com/i.test(website)) {
      skipped.push({ censusRecordId: rec.id, reason: "no_wyndham_website" });
      continue;
    }
    const pidBlank = isBlankCensusValue(rec.fields?.[MAP.propertyId]);
    const descBlank = isBlankCensusValue(rec.fields?.[MAP.description]);
    if (!pidBlank && !descBlank) {
      skipped.push({ censusRecordId: rec.id, reason: "no_blank_pid_or_description" });
      continue;
    }

    n++;
    const fetchUrl = canonicalizeWyndhamUrl(website) || website;
    console.log(` [${n}] ${rec.fields?.name}`);
    const res = await fetch(fetchUrl, { headers: WYNDHAM_FETCH_HEADERS, redirect: "follow" });
    const html = await res.text();
    await sleep(opts.delayMs);

    if (!res.ok || !html) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields?.name,
        reason: "fetch_failed",
        status: res.status,
      });
      continue;
    }

    const meta = parseWyndhamHotelMetadataFromHtml(html);
    if (!meta) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields?.name,
        reason: "no_jsonld_hotel",
      });
      continue;
    }

    const applyFields = {};
    if (pidBlank && meta.identifier && /^\d{4,6}$/.test(meta.identifier)) {
      applyFields[MAP.propertyId] = meta.identifier;
    }
    if (descBlank && meta.description && meta.description.length >= 40) {
      applyFields[MAP.description] = meta.description;
    }

    if (!Object.keys(applyFields).length) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields?.name,
        reason: "jsonld_no_usable_blank_fields",
        identifier: meta.identifier || null,
        hasDescription: Boolean(meta.description),
      });
      continue;
    }

    const row = {
      censusRecordId: rec.id,
      censusName: rec.fields?.name,
      affiliation: rec.fields?.[CENSUS_FIELDS.affiliation],
      propertyUrl: fetchUrl,
      jsonLdIdentifier: meta.identifier || null,
      applyFields,
      fieldMapping: MAP,
    };
    const v = validatePlanRow(row);
    if (!v.pass) {
      skipped.push({ ...row, reason: "validation", errors: v.errors });
      continue;
    }
    planRows.push(row);
  }

  writeFileSync(
    join(REPORTS, "wyndham-wave1-jsonld-backfill-plan.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: opts.apply ? "apply" : "dry-run",
        fieldMapping: MAP,
        readyToApply: planRows.length,
        planRows,
        skipped,
      },
      null,
      2
    )
  );

  console.log("\nReady:", planRows.length, "Skipped:", skipped.length);

  if (!opts.apply) {
    console.log("DRY-RUN — re-run with --apply after review.");
    return;
  }

  let updated = 0;
  let errors = 0;
  let batch = [];
  const log = [];

  async function flush() {
    if (!batch.length) return;
    try {
      await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
    } catch (err) {
      errors += batch.length;
      console.error("Batch failed:", err?.message || err);
    }
    batch = [];
  }

  for (const row of planRows) {
    log.push(row);
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= BATCH) await flush();
  }
  await flush();

  writeFileSync(
    join(REPORTS, "wyndham-wave1-jsonld-backfill-apply-log.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, errors, rows: log }, null, 2)
  );
  console.log("Updated:", updated, "Errors:", errors);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
