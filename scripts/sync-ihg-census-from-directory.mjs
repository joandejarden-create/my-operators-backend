#!/usr/bin/env node
/**
 * Sync Hotel Census Website + Property ID for CALA IHG rows from official directory.
 *
 * Fill-blank only. Dry-run by default.
 *
 *   node scripts/extract-ihg-cala-directory.mjs
 *   node scripts/sync-ihg-census-from-directory.mjs
 *   node scripts/sync-ihg-census-from-directory.mjs --apply
 */
import "../load-env.js";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import {
  planIhgCensusDirectoryMatch,
  validateIhgCensusApplyRow,
} from "../lib/hotel-census/plan-ihg-census-directory-match.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const EXTRACT_JSON = join(REPORTS, "ihg-cala-directory-extract.json");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    dryRun: !args.includes("--apply"),
    // Conservative default: medium+ only (low goes to steward CSV, not auto-apply)
    minScore: Number(args.find((a) => a.startsWith("--min-score="))?.split("=")[1] || 68),
    minNameSim: Number(args.find((a) => a.startsWith("--min-name-sim="))?.split("=")[1] || 0.6),
    minConfidence: args.find((a) => a.startsWith("--min-confidence="))?.split("=")[1] || "medium",
    extractPath: args.find((a) => a.startsWith("--extract="))?.split("=")[1] || EXTRACT_JSON,
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const opts = parseArgs();
  mkdirSync(REPORTS, { recursive: true });

  if (!existsSync(opts.extractPath)) {
    throw new Error(
      `Missing extract ${opts.extractPath}. Run: node scripts/extract-ihg-cala-directory.mjs`
    );
  }

  const extract = JSON.parse(readFileSync(opts.extractPath, "utf8"));
  const directoryRows = Array.isArray(extract.propertyRows) ? extract.propertyRows : [];
  console.log("=== IHG census directory match (CALA) ===\n");
  console.log("Directory hotels:", directoryRows.length);
  console.log("minScore:", opts.minScore, "minNameSim:", opts.minNameSim, "minConfidence:", opts.minConfidence);

  const plan = await planIhgCensusDirectoryMatch({
    directoryRows,
    minScore: opts.minScore,
    minNameSim: opts.minNameSim,
    minConfidence: opts.minConfidence,
    calaOnly: true,
  });

  const planPath = join(REPORTS, "ihg-census-directory-match-plan.json");
  writeFileSync(planPath, JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2));

  const stewardPath = join(REPORTS, "ihg-census-steward-review.csv");
  writeFileSync(
    stewardPath,
    [
      "censusRecordId,censusName,censusCountry,propertyId,propertyUrl,matchScore,matchConfidence,nameSim",
      ...plan.stewardReview.map((r) =>
        [
          r.censusRecordId,
          r.censusName,
          r.censusCountry,
          r.propertyId,
          r.propertyUrl,
          r.matchScore,
          r.matchConfidence,
          r.nameSim,
        ]
          .map(csvEscape)
          .join(",")
      ),
    ].join("\n")
  );

  const unmatchedPath = join(REPORTS, "ihg-census-unmatched-steward.csv");
  const unmatched = plan.skipped.filter((s) => s.reason === "no_directory_match");
  writeFileSync(
    unmatchedPath,
    [
      "censusRecordId,censusName,censusCountry,reason",
      ...unmatched.map((r) =>
        [r.censusRecordId, r.censusName, r.censusCountry, r.reason].map(csvEscape).join(",")
      ),
    ].join("\n")
  );

  console.log("\nCensus IHG (all):", plan.censusRowsTotalIhg);
  console.log("Census CALA scanned:", plan.censusRowsScanned);
  console.log("Ready to apply:", plan.readyToApply);
  console.log("Steward (low confidence):", plan.stewardReview.length);
  console.log("Unmatched census:", unmatched.length);
  console.log("Unmatched directory:", plan.unmatchedDirectoryCount);
  console.log("Plan:", planPath);

  const validated = [];
  const validationFailed = [];
  for (const row of plan.planRows) {
    const v = validateIhgCensusApplyRow(row);
    if (v.pass) validated.push(row);
    else validationFailed.push({ ...row, validationErrors: v.errors });
  }
  if (validationFailed.length) {
    console.log("Validation failed:", validationFailed.length);
    writeFileSync(
      join(REPORTS, "ihg-census-validation-failed.json"),
      JSON.stringify(validationFailed, null, 2)
    );
  }

  console.log("\nMode:", opts.dryRun ? "DRY-RUN (no writes)" : "APPLY");
  console.log("Validated rows:", validated.length);

  if (opts.dryRun) {
    writeFileSync(
      join(REPORTS, "ihg-census-apply-dry-run.json"),
      JSON.stringify(
        {
          generatedAt: new Date().toISOString(),
          mode: "dry-run",
          fieldMapping: plan.fieldMapping,
          wouldUpdate: validated.length,
          sample: validated.slice(0, 15).map((r) => ({
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
    console.log("Dry-run preview: reports/ihg-census-apply-dry-run.json");
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
      for (const b of batch) {
        log.push({ ...b, error: String(err?.message || err) });
      }
    }
    batch = [];
  }

  for (const row of validated) {
    log.push({
      censusRecordId: row.censusRecordId,
      censusName: row.censusName,
      propertyId: row.propertyId,
      propertyUrl: row.propertyUrl,
      matchConfidence: row.matchConfidence,
      matchScore: row.matchScore,
      applyFields: row.applyFields,
    });
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= BATCH) await flush();
  }
  await flush();

  const applyLogPath = join(REPORTS, "ihg-census-enrichment-apply-log.json");
  writeFileSync(
    applyLogPath,
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
    join(REPORTS, "ihg-census-enrichment-apply-log.csv"),
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
  console.log("Apply log:", applyLogPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
