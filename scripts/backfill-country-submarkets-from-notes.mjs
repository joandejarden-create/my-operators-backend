#!/usr/bin/env node
/**
 * Backfill Submarket field from Notes prefix for any country.
 *
 *   node scripts/backfill-country-submarkets-from-notes.mjs --country "Dominican Republic" --table demand-anchors
 *   node scripts/backfill-country-submarkets-from-notes.mjs --country "Dominican Republic" --table all --apply
 */
import "../load-env.js";
import { DEMAND_ANCHORS_FIELDS as DA_F } from "../lib/demand-anchors/airtable-demand-anchors-fields.js";
import { TRAVEL_INFRASTRUCTURE_FIELDS as TI_F } from "../lib/travel-infrastructure/airtable-travel-infrastructure-fields.js";
import { getDemandAnchorsAirtableConfig, resolveDemandAnchorsTableName } from "../lib/demand-anchors/demand-anchors-base.js";
import {
  getTravelInfrastructureAirtableConfig,
  resolveTravelInfrastructureTableName,
} from "../lib/travel-infrastructure/travel-infrastructure-base.js";
import { DEMAND_ANCHORS_SELECT_FIELDS } from "../lib/demand-anchors/airtable-demand-anchors-io.js";
import { TRAVEL_INFRASTRUCTURE_SELECT_FIELDS } from "../lib/travel-infrastructure/airtable-travel-infrastructure-io.js";
import {
  buildSubmarketBackfillPatch,
  summarizeSubmarketBackfill,
} from "../lib/radar-submarket-backfill.js";
import {
  fetchAirtableTableFieldNameSet,
  filterFieldsToAirtableSchema,
} from "../lib/third-party-operator-basics-airtable-column-aliases.js";

const APPLY = process.argv.includes("--apply");
const FORCE = process.argv.includes("--force");
const VERBOSE = process.argv.includes("--verbose");
const NO_CITY_INFER = process.argv.includes("--no-infer-from-city");
const DRY = !APPLY;

const countryIdx = process.argv.indexOf("--country");
const COUNTRY = countryIdx >= 0 ? process.argv[countryIdx + 1] : "";
if (!COUNTRY) {
  console.error('Usage: --country "Dominican Republic" [--table demand-anchors|travel-infrastructure|all] [--apply]');
  process.exit(1);
}

const TABLE_ARG = (() => {
  const idx = process.argv.indexOf("--table");
  if (idx < 0) return "all";
  const v = process.argv[idx + 1];
  if (v === "demand-anchors" || v === "travel-infrastructure" || v === "all") return v;
  throw new Error('--table must be "demand-anchors", "travel-infrastructure", or "all"');
})();

const LIMIT = (() => {
  const idx = process.argv.indexOf("--limit");
  if (idx < 0) return null;
  const n = Number(process.argv[idx + 1]);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : null;
})();

const AIRTABLE_BATCH_DELAY_MS = Number(process.env.AIRTABLE_WRITE_DELAY_MS) || 220;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function printSummary(label, summary) {
  console.log(`\n=== ${label} ===`);
  console.log("Records scanned:", summary.totalScanned);
  console.log("Records to update:", summary.needingUpdate);
  console.log("Skipped (already populated non-Other):", summary.skippedAlreadyPopulated);
  console.log("Skipped (no Submarket: prefix in Notes):", summary.skippedNoNotesPrefix);
  console.log("Skipped (country mismatch):", summary.skippedCountryMismatch);
  console.log("Skipped (unchanged):", summary.skippedUnchanged);
  console.log("Would remain Other / empty:", summary.stillOtherAfter);
  if (Object.keys(summary.bySubmarket).length) {
    console.log("By target submarket:");
    for (const [k, v] of Object.entries(summary.bySubmarket).sort((a, b) => b[1] - a[1])) {
      console.log(`  ${k}: ${v}`);
    }
  }
  if (summary.samples.length) {
    console.log("Samples:");
    for (const s of summary.samples) {
      console.log(`  ${s.id} | ${s.name} | ${s.previous || "(empty)"} → ${s.target}`);
    }
  }
  if (summary.missingSubmarketAfter.length) {
    console.log("Missing submarket note (up to 20):");
    for (const m of summary.missingSubmarketAfter) {
      console.log(`  ${m.id} | ${m.name} | current: ${m.current} | notes: ${m.notesPreview || "(empty)"}`);
    }
  }
}

async function backfillTable({ label, cfg, tableName, nameField, notesField, submarketField, countryField, cityField, selectFields, countryFilter }) {
  const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);
  if (schema && !schema.has(submarketField)) {
    console.warn(`WARN: ${submarketField} not on ${tableName}`);
  }

  const requestedFields = schema ? selectFields.filter((name) => schema.has(name)) : selectFields;
  const formula = `{${countryField}} = '${countryFilter.replace(/'/g, "\\'")}'`;
  const records = await cfg.base(tableName).select({ fields: requestedFields, filterByFormula: formula }).all();
  const slice = LIMIT ? records.slice(0, LIMIT) : records;
  const results = slice.map((rec) =>
    buildSubmarketBackfillPatch(rec, {
      notesField,
      submarketField,
      countryField,
      cityField,
      nameField,
      countryFilter,
      force: FORCE,
      inferFromCity: !NO_CITY_INFER,
    })
  );
  const summary = summarizeSubmarketBackfill(results);
  printSummary(label, summary);

  if (VERBOSE) {
    for (const r of results.filter((x) => x.needsUpdate)) {
      console.log(JSON.stringify(r, null, 2));
    }
  }

  if (DRY) return { summary, updated: 0, failed: 0 };

  let updated = 0;
  let failed = 0;
  for (const r of results) {
    if (!r.needsUpdate) continue;
    const patch = filterFieldsToAirtableSchema(r.patch, schema);
    if (!Object.keys(patch).length) continue;
    try {
      await cfg.base(tableName).update(r.recordId, patch, { typecast: true });
      updated += 1;
      if (VERBOSE) console.log("UPDATED", r.recordId, r.name, r.target);
    } catch (err) {
      failed += 1;
      console.error("FAIL", r.recordId, r.name, err?.message || err);
    }
    await sleep(AIRTABLE_BATCH_DELAY_MS);
  }
  console.log(`${label} apply: updated=${updated} failed=${failed}`);
  return { summary, updated, failed };
}

async function main() {
  console.log(DRY ? "=== DRY RUN ===" : "=== APPLY ===");
  console.log("Country:", COUNTRY);
  console.log("Table:", TABLE_ARG);
  if (LIMIT) console.log("Limit:", LIMIT);

  const totals = { demandUpdated: 0, demandFailed: 0, travelUpdated: 0, travelFailed: 0 };

  if (TABLE_ARG === "demand-anchors" || TABLE_ARG === "all") {
    const cfg = getDemandAnchorsAirtableConfig();
    if (!cfg) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");
    const tableName = await resolveDemandAnchorsTableName(cfg.baseId, cfg.apiKey);
    const r = await backfillTable({
      label: "Demand Anchors",
      cfg,
      tableName,
      nameField: DA_F.name,
      notesField: DA_F.notes,
      submarketField: DA_F.submarket,
      countryField: DA_F.country,
      cityField: DA_F.city,
      selectFields: DEMAND_ANCHORS_SELECT_FIELDS,
      countryFilter: COUNTRY,
    });
    totals.demandUpdated = r.updated || 0;
    totals.demandFailed = r.failed || 0;
  }

  if (TABLE_ARG === "travel-infrastructure" || TABLE_ARG === "all") {
    const cfg = getTravelInfrastructureAirtableConfig();
    if (!cfg) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");
    const tableName = await resolveTravelInfrastructureTableName(cfg.baseId, cfg.apiKey);
    const r = await backfillTable({
      label: "Travel Infrastructure",
      cfg,
      tableName,
      nameField: TI_F.name,
      notesField: TI_F.notes,
      submarketField: TI_F.submarket,
      countryField: TI_F.country,
      cityField: TI_F.city,
      selectFields: TRAVEL_INFRASTRUCTURE_SELECT_FIELDS,
      countryFilter: COUNTRY,
    });
    totals.travelUpdated = r.updated || 0;
    totals.travelFailed = r.failed || 0;
  }

  if (DRY) console.log("\nNo writes performed. Re-run with --apply after submarket options exist in Airtable.");
  else console.log("\nApply totals:", totals);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
