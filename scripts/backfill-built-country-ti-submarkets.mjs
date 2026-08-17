#!/usr/bin/env node
/**
 * Built-country Travel Infrastructure submarket inference backfill.
 *
 *   node scripts/backfill-built-country-ti-submarkets.mjs --built-countries
 *   node scripts/backfill-built-country-ti-submarkets.mjs --built-countries --min-confidence High --apply
 */
import "../load-env.js";
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { TRAVEL_INFRASTRUCTURE_FIELDS as F } from "../lib/travel-infrastructure/airtable-travel-infrastructure-fields.js";
import {
  getTravelInfrastructureAirtableConfig,
  resolveTravelInfrastructureTableName,
} from "../lib/travel-infrastructure/travel-infrastructure-base.js";
import { TRAVEL_INFRASTRUCTURE_SELECT_FIELDS } from "../lib/travel-infrastructure/airtable-travel-infrastructure-io.js";
import {
  fetchAirtableTableFieldNameSet,
  filterFieldsToAirtableSchema,
} from "../lib/third-party-operator-basics-airtable-column-aliases.js";
import {
  planBuiltCountryTiSubmarketInference,
  INFERENCE_NOTE_TAG,
  BUILT_RADAR_COUNTRIES,
} from "../lib/radar-buildout/travel-infrastructure-submarket-inference.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const APPLY = process.argv.includes("--apply");
const FORCE = process.argv.includes("--force");
const VERBOSE = process.argv.includes("--verbose");
const DRY = !APPLY;

const countryIdx = process.argv.indexOf("--country");
const countriesIdx = process.argv.indexOf("--countries");
const builtCountries = process.argv.includes("--built-countries");

const COUNTRIES = (() => {
  if (countryIdx >= 0) return [process.argv[countryIdx + 1]];
  if (countriesIdx >= 0) {
    return process.argv[countriesIdx + 1]
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }
  if (builtCountries) return [...BUILT_RADAR_COUNTRIES];
  return [...BUILT_RADAR_COUNTRIES];
})();

const LIMIT = (() => {
  const idx = process.argv.indexOf("--limit");
  if (idx < 0) return null;
  const n = Number(process.argv[idx + 1]);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : null;
})();

const MIN_CONFIDENCE = (() => {
  const idx = process.argv.indexOf("--min-confidence");
  if (idx < 0) return "High";
  const v = process.argv[idx + 1];
  if (v === "High" || v === "Medium" || v === "Low") return v;
  throw new Error("--min-confidence must be High, Medium, or Low");
})();

const OUTPUT = (() => {
  const idx = process.argv.indexOf("--output");
  return idx >= 0
    ? process.argv[idx + 1]
    : "data/built-country-ti-submarket-inference-report.json";
})();

const REVIEW_OUTPUT = "data/built-country-ti-submarket-review-list.json";
const AIRTABLE_BATCH_DELAY_MS = Number(process.env.AIRTABLE_WRITE_DELAY_MS) || 220;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function strVal(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  return String(v).trim();
}

function mapRecord(rec) {
  const fields = rec.fields || {};
  return {
    id: rec.id,
    name: strVal(fields[F.name]),
    city: strVal(fields[F.city]),
    country: strVal(fields[F.country]),
    region: strVal(fields[F.region]),
    submarket: strVal(fields[F.submarket]),
    notes: strVal(fields[F.notes]),
    pointType: strVal(fields[F.pointType] || fields[F.type]),
    type: strVal(fields[F.type]),
    iataCode: strVal(fields[F.iataCode]),
    airportType: strVal(fields[F.airportType]),
    infrastructureRole: strVal(fields[F.infrastructureRole]),
    latitude: fields[F.lat],
    longitude: fields[F.lng],
  };
}

function buildPatch(record, inference, existingNotes) {
  const patch = { [F.submarket]: inference.inferredSubmarket };
  let notes = existingNotes;
  if (inference.proposedNotesAppend && !notes.includes(INFERENCE_NOTE_TAG)) {
    notes = notes ? `${notes} ${inference.proposedNotesAppend}` : inference.proposedNotesAppend;
    patch[F.notes] = notes;
  }
  return patch;
}

function summarizeRemainingOther(results) {
  const byCountry = {};
  for (const r of results) {
    const cur = r.currentSubmarket;
    const after = r.shouldUpdate ? r.inferredSubmarket : cur;
    const isOtherOrEmpty = !after || after === "Other";
    if (isOtherOrEmpty) {
      byCountry[r.country] = (byCountry[r.country] || 0) + 1;
    }
  }
  return byCountry;
}

async function main() {
  const cfg = getTravelInfrastructureAirtableConfig();
  if (!cfg) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  const tableName = await resolveTravelInfrastructureTableName(cfg.baseId, cfg.apiKey);
  const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);
  const requestedFields = schema
    ? TRAVEL_INFRASTRUCTURE_SELECT_FIELDS.filter((name) => schema.has(name))
    : TRAVEL_INFRASTRUCTURE_SELECT_FIELDS;

  console.log(DRY ? "=== DRY RUN ===" : "=== APPLY ===");
  console.log("Countries:", COUNTRIES.join(", "));
  console.log("Min confidence:", MIN_CONFIDENCE);
  if (LIMIT) console.log("Limit:", LIMIT);

  const countryFormula =
    COUNTRIES.length === 1
      ? `{${F.country}} = '${COUNTRIES[0].replace(/'/g, "\\'")}'`
      : `OR(${COUNTRIES.map((c) => `{${F.country}} = '${c.replace(/'/g, "\\'")}'`).join(",")})`;

  const records = await cfg
    .base(tableName)
    .select({ fields: requestedFields, filterByFormula: countryFormula })
    .all();

  const mapped = (LIMIT ? records.slice(0, LIMIT) : records).map(mapRecord);
  const plan = planBuiltCountryTiSubmarketInference(mapped, {
    countries: COUNTRIES,
    minConfidence: MIN_CONFIDENCE,
    force: FORCE,
  });

  const reviewList = plan.review.map((r) => ({
    recordId: r.recordId,
    name: r.name,
    country: r.country,
    city: r.city,
    currentSubmarket: r.currentSubmarket || "(empty)",
    reason: r.reason,
    recommendedAction:
      r.confidence === "No Match"
        ? "manual_submarket_assignment_from_official_source"
        : "review_low_confidence_before_apply",
  }));

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY ? "dry-run" : "apply",
    countries: COUNTRIES,
    minConfidence: MIN_CONFIDENCE,
    force: FORCE,
    summary: {
      recordsScanned: plan.scanned,
      recordsEligible: plan.eligible,
      recordsProposedForUpdate: plan.proposedUpdates,
      skippedAlreadyPopulated: plan.skippedAlreadyPopulated,
      skippedNoMatch: plan.skippedNoMatch,
      skippedLowConfidence: plan.skippedLowConfidence,
      proposedByCountry: plan.proposedByCountry,
      proposedBySubmarket: plan.proposedBySubmarket,
      reviewListCount: reviewList.length,
    },
    proposed: plan.proposed.map((r) => ({
      recordId: r.recordId,
      name: r.name,
      country: r.country,
      city: r.city,
      currentSubmarket: r.currentSubmarket,
      inferredSubmarket: r.inferredSubmarket,
      confidence: r.confidence,
      reason: r.reason,
    })),
    apply: null,
  };

  console.log("\n=== Inference summary ===");
  console.log("Records scanned:", plan.scanned);
  console.log("Records eligible (empty/Other):", plan.eligible);
  console.log("Proposed updates:", plan.proposedUpdates);
  console.log("Skipped (already populated non-Other):", plan.skippedAlreadyPopulated);
  console.log("Skipped (no match):", plan.skippedNoMatch);
  console.log("Skipped (below min confidence):", plan.skippedLowConfidence);
  console.log("Review list:", reviewList.length);

  if (Object.keys(plan.proposedByCountry).length) {
    console.log("\nProposed by country:");
    for (const [k, v] of Object.entries(plan.proposedByCountry).sort((a, b) => b[1] - a[1])) {
      console.log(`  ${k}: ${v}`);
    }
  }
  if (Object.keys(plan.proposedBySubmarket).length) {
    console.log("\nProposed by submarket:");
    for (const [k, v] of Object.entries(plan.proposedBySubmarket).sort((a, b) => b[1] - a[1])) {
      console.log(`  ${k}: ${v}`);
    }
  }

  if (VERBOSE) {
    for (const p of plan.proposed.slice(0, 30)) {
      console.log(
        `  ${p.recordId} | ${p.name} | ${p.currentSubmarket || "(empty)"} → ${p.inferredSubmarket} [${p.confidence}]`
      );
    }
  }

  let updated = 0;
  let failed = 0;
  const errors = [];

  if (APPLY) {
    for (const inference of plan.proposed) {
      const rec = records.find((r) => r.id === inference.recordId);
      const existingNotes = strVal(rec?.fields?.[F.notes]);
      const patch = filterFieldsToAirtableSchema(
        buildPatch(inference, inference, existingNotes),
        schema
      );
      if (!Object.keys(patch).length) continue;
      try {
        await cfg.base(tableName).update(inference.recordId, patch, { typecast: true });
        updated += 1;
        if (VERBOSE) {
          console.log("UPDATED", inference.recordId, inference.name, "→", inference.inferredSubmarket);
        }
      } catch (err) {
        failed += 1;
        errors.push({
          recordId: inference.recordId,
          name: inference.name,
          message: err?.message || String(err),
        });
        console.error("FAIL", inference.recordId, inference.name, err?.message || err);
      }
      await sleep(AIRTABLE_BATCH_DELAY_MS);
    }

    const afterPlan = planBuiltCountryTiSubmarketInference(
      mapped.map((r) => {
        const applied = plan.proposed.find((p) => p.recordId === r.id && p.shouldUpdate);
        return applied
          ? { ...r, submarket: applied.inferredSubmarket }
          : r;
      }),
      { countries: COUNTRIES, minConfidence: MIN_CONFIDENCE, force: FORCE }
    );

    report.apply = {
      recordsUpdated: updated,
      recordsSkipped: plan.proposedUpdates - updated - failed,
      recordsFailed: failed,
      errors,
      remainingOtherOrEmptyByCountry: summarizeRemainingOther(afterPlan.results),
    };

    console.log("\n=== Apply results ===");
    console.log("Updated:", updated);
    console.log("Failed:", failed);
    console.log("Remaining Other/empty by country:", report.apply.remainingOtherOrEmptyByCountry);
  } else {
    report.remainingOtherOrEmptyByCountry = summarizeRemainingOther(plan.results);
    console.log("\nRemaining Other/empty by country (if applied):", report.remainingOtherOrEmptyByCountry);
    console.log("\nNo writes performed. Re-run with --apply to update Airtable.");
  }

  writeFileSync(join(root, OUTPUT), JSON.stringify(report, null, 2) + "\n");
  writeFileSync(join(root, REVIEW_OUTPUT), JSON.stringify(reviewList, null, 2) + "\n");
  console.log("\nWritten:", OUTPUT);
  console.log("Written:", REVIEW_OUTPUT);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
