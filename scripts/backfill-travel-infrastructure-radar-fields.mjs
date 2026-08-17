#!/usr/bin/env node
/**
 * Backfill Travel Infrastructure Data radar extension fields from legacy columns.
 *
 *   node scripts/backfill-travel-infrastructure-radar-fields.mjs           # dry run (default)
 *   node scripts/backfill-travel-infrastructure-radar-fields.mjs --apply
 *   node scripts/backfill-travel-infrastructure-radar-fields.mjs --apply --force
 *   node scripts/backfill-travel-infrastructure-radar-fields.mjs --limit 50 --type Airport --verbose
 */
import "../load-env.js";
import {
  TRAVEL_INFRASTRUCTURE_FIELDS as F,
} from "../lib/travel-infrastructure/airtable-travel-infrastructure-fields.js";
import {
  getTravelInfrastructureAirtableConfig,
  resolveTravelInfrastructureTableName,
} from "../lib/travel-infrastructure/travel-infrastructure-base.js";
import { TRAVEL_INFRASTRUCTURE_SELECT_FIELDS } from "../lib/travel-infrastructure/airtable-travel-infrastructure-io.js";
import {
  buildBackfillPatch,
  summarizeBackfillResults,
  sanitizeBackfillPatchForSchema,
} from "../lib/travel-infrastructure/backfill-radar-fields.js";
import {
  fetchAirtableTableFieldNameSet,
  filterFieldsToAirtableSchema,
} from "../lib/third-party-operator-basics-airtable-column-aliases.js";

const APPLY = process.argv.includes("--apply");
const FORCE = process.argv.includes("--force");
const VERBOSE = process.argv.includes("--verbose");
const DRY = !APPLY;

function parseArg(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx < 0) return null;
  return process.argv[idx + 1] || null;
}

const LIMIT = (() => {
  const raw = parseArg("--limit");
  if (!raw) return null;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : null;
})();

const TYPE_FILTER = parseArg("--type");
const COUNTRY_FILTER = parseArg("--country");

const AIRTABLE_BATCH_DELAY_MS = Number(process.env.AIRTABLE_WRITE_DELAY_MS) || 220;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function buildFilterFormula() {
  const parts = [];
  if (TYPE_FILTER) {
    const safe = TYPE_FILTER.replace(/'/g, "\\'");
    parts.push(`OR({${F.type}} = '${safe}', {${F.pointType}} = '${safe}')`);
  }
  if (COUNTRY_FILTER) {
    const safe = COUNTRY_FILTER.replace(/'/g, "\\'");
    parts.push(`{${F.country}} = '${safe}'`);
  }
  if (!parts.length) return null;
  return parts.length === 1 ? parts[0] : `AND(${parts.join(", ")})`;
}

function printDryRunReport(summary) {
  console.log("\n=== DRY RUN REPORT ===");
  console.log("Total records scanned:", summary.totalScanned);
  console.log("Records needing update:", summary.needingUpdate);
  console.log("Records skipped (already populated, no changes):", summary.skippedAlreadyPopulated);
  console.log("Records missing valid coordinates:", summary.missingCoordinates);
  console.log("\nUpdates by Point Type:");
  for (const [k, v] of Object.entries(summary.byPointType).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${k}: ${v}`);
  }
  console.log("\nUpdates by Country (top 15):");
  const countries = Object.entries(summary.byCountry).sort((a, b) => b[1] - a[1]).slice(0, 15);
  for (const [k, v] of countries) {
    console.log(`  ${k}: ${v}`);
  }
  console.log("\nSample changes (up to 10):");
  for (const s of summary.samples) {
    console.log(
      `  ${s.id} | ${s.name} | ${s.pointType} | ${s.country} | fields: ${s.patchKeys.join(", ")}`
    );
  }
}

function printApplyReport(report) {
  console.log("\n=== APPLY REPORT ===");
  console.log("Records updated:", report.updated);
  console.log("Records failed:", report.failed);
  console.log("Records skipped (no patch):", report.skippedNoPatch);
  if (Object.keys(report.errorsByType).length) {
    console.log("\nErrors by type:");
    for (const [k, v] of Object.entries(report.errorsByType)) {
      console.log(`  ${k}: ${v}`);
    }
  }
  console.log(
    "\nRate limit: writes spaced at",
    AIRTABLE_BATCH_DELAY_MS,
    "ms. Re-run script to continue if interrupted (idempotent without --force)."
  );
}

async function main() {
  const cfg = getTravelInfrastructureAirtableConfig();
  if (!cfg) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  const tableName = await resolveTravelInfrastructureTableName(cfg.baseId, cfg.apiKey);
  const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);
  const requestedFields = schema
    ? TRAVEL_INFRASTRUCTURE_SELECT_FIELDS.filter((name) => schema.has(name))
    : TRAVEL_INFRASTRUCTURE_SELECT_FIELDS;

  console.log(DRY ? "=== DRY RUN (default) ===" : "=== APPLY ===");
  console.log("Table:", tableName);
  console.log("Force overwrite:", FORCE);
  if (TYPE_FILTER) console.log("Type filter:", TYPE_FILTER);
  if (COUNTRY_FILTER) console.log("Country filter:", COUNTRY_FILTER);
  if (LIMIT) console.log("Limit:", LIMIT);

  const selectOptions = { fields: requestedFields };
  const formula = buildFilterFormula();
  if (formula) selectOptions.filterByFormula = formula;

  const records = await cfg.base(tableName).select(selectOptions).all();
  const slice = LIMIT ? records.slice(0, LIMIT) : records;

  const results = slice.map((rec) => buildBackfillPatch(rec, { force: FORCE }));
  const summary = summarizeBackfillResults(results);
  printDryRunReport(summary);

  if (VERBOSE) {
    console.log("\n--- Verbose record patches ---");
    for (const r of results.filter((x) => x.needsUpdate)) {
      console.log(JSON.stringify({ id: r.recordId, name: r.name, patch: r.patch }, null, 2));
    }
  }

  if (DRY) {
    console.log("\nNo writes performed. Re-run with --apply to update Airtable.");
    return;
  }

  const applyReport = {
    updated: 0,
    failed: 0,
    skippedNoPatch: 0,
    errorsByType: {},
  };

  for (const r of results) {
    if (!r.needsUpdate) {
      applyReport.skippedNoPatch += 1;
      continue;
    }

    let safePatch = sanitizeBackfillPatchForSchema(r.patch, schema);
    safePatch = filterFieldsToAirtableSchema(safePatch, schema);
    if (!Object.keys(safePatch).length) {
      applyReport.skippedNoPatch += 1;
      continue;
    }

    try {
      await cfg.base(tableName).update(r.recordId, safePatch, { typecast: true });
      applyReport.updated += 1;
      if (VERBOSE) {
        console.log("UPDATED", r.recordId, r.name, Object.keys(safePatch).join(", "));
      }
    } catch (err) {
      applyReport.failed += 1;
      const errType = err?.error || err?.statusCode || err?.message || "unknown";
      const key = String(errType).slice(0, 80);
      applyReport.errorsByType[key] = (applyReport.errorsByType[key] || 0) + 1;
      console.error("FAIL", r.recordId, r.name, err?.message || err);
    }

    await sleep(AIRTABLE_BATCH_DELAY_MS);
  }

  printApplyReport(applyReport);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
