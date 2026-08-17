#!/usr/bin/env node
/**
 * Wave 2 IHG: fill-blank Website + Property ID for Kimpton Hotels + Hotel Indigo.
 * Uses existing IHG directory extract + match plan; filters apply to Wave 2 affiliations.
 *
 *   node scripts/run-ihg-wave2-census-enrichment.mjs
 *   node scripts/run-ihg-wave2-census-enrichment.mjs --apply
 */
import "../load-env.js";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import {
  planIhgCensusDirectoryMatch,
  validateIhgCensusApplyRow,
  MAP_IHG_CENSUS_BACKFILL,
} from "../lib/hotel-census/plan-ihg-census-directory-match.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const EXTRACT_JSON = join(REPORTS, "ihg-cala-directory-extract.json");
const WAVE2_AFFILIATIONS = new Set(["Kimpton Hotels", "Hotel Indigo"]);
const BATCH = 10;

function parseArgs() {
  return {
    apply: process.argv.includes("--apply"),
    extractPath:
      process.argv.find((a) => a.startsWith("--extract="))?.split("=")[1] || EXTRACT_JSON,
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
    throw new Error(`Missing extract ${opts.extractPath}. Run extract-ihg-cala-directory.mjs first.`);
  }

  const extract = JSON.parse(readFileSync(opts.extractPath, "utf8"));
  const directoryRows = Array.isArray(extract.propertyRows) ? extract.propertyRows : [];
  console.log("=== IHG Wave 2 (Kimpton + Hotel Indigo) ===\n");
  console.log("Directory hotels:", directoryRows.length);

  const plan = await planIhgCensusDirectoryMatch({
    directoryRows,
    calaOnly: true,
    minScore: 68,
    minNameSim: 0.6,
    minConfidence: "medium",
  });

  const affField = CENSUS_FIELDS.affiliation;
  const wave2Rows = plan.planRows.filter((r) => {
    // plan rows may not include affiliation — load from census via record later
    return true;
  });

  // Attach affiliation from Airtable for filter
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const idSet = new Set(plan.planRows.map((r) => r.censusRecordId));
  const affById = new Map();
  if (idSet.size) {
    const formula = `OR(${[...WAVE2_AFFILIATIONS]
      .map((a) => `{${affField}}="${a}"`)
      .join(",")})`;
    const recs = await base(HOTEL_CENSUS_TABLE)
      .select({ fields: [affField, "name"], filterByFormula: formula, pageSize: 100 })
      .all();
    for (const r of recs) affById.set(r.id, String(r.fields?.[affField] || "").trim());
  }

  const filtered = plan.planRows.filter((r) => WAVE2_AFFILIATIONS.has(affById.get(r.censusRecordId)));
  for (const r of filtered) r.affiliation = affById.get(r.censusRecordId);

  const steward = [
    ...plan.stewardReview.filter((r) => WAVE2_AFFILIATIONS.has(affById.get(r.censusRecordId))),
    ...plan.skipped.filter(
      (r) =>
        WAVE2_AFFILIATIONS.has(affById.get(r.censusRecordId)) &&
        (r.reason === "no_directory_match" || r.reason === "below_apply_gate_steward_only")
    ),
  ];

  // Also list all Wave2 census blanks even if not in plan
  const allWave2 = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: ["name", affField, "country", "Website", "Property ID"],
      filterByFormula: `OR(${[...WAVE2_AFFILIATIONS]
        .map((a) => `{${affField}}="${a}"`)
        .join(",")})`,
    })
    .all();

  const validated = [];
  const validationFailed = [];
  for (const row of filtered) {
    const v = validateIhgCensusApplyRow(row);
    if (v.pass) validated.push(row);
    else validationFailed.push({ ...row, validationErrors: v.errors });
  }

  writeFileSync(
    join(REPORTS, "ihg-wave2-census-enrichment-plan.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        affiliations: [...WAVE2_AFFILIATIONS],
        fieldMapping: MAP_IHG_CENSUS_BACKFILL,
        censusWave2Total: allWave2.length,
        readyToApply: validated.length,
        planRows: validated,
        stewardReview: steward,
        validationFailed,
      },
      null,
      2
    )
  );
  writeFileSync(
    join(REPORTS, "ihg-wave2-steward-review.csv"),
    [
      "censusRecordId,censusName,affiliation,country,reason,propertyUrl",
      ...steward.map((r) =>
        [
          r.censusRecordId,
          r.censusName,
          affById.get(r.censusRecordId) || r.affiliation,
          r.censusCountry,
          r.reason,
          r.propertyUrl,
        ]
          .map(csvEscape)
          .join(",")
      ),
    ].join("\n")
  );

  console.log("Wave2 census rows:", allWave2.length);
  console.log("Ready to apply:", validated.length);
  console.log("Steward:", steward.length);
  for (const r of validated.slice(0, 20)) {
    console.log(
      `  ${r.affiliation || ""} | ${r.censusName} → ${Object.keys(r.applyFields).join("|")}`
    );
  }

  if (!opts.apply) {
    writeFileSync(
      join(REPORTS, "ihg-wave2-apply-dry-run.json"),
      JSON.stringify(
        {
          generatedAt: new Date().toISOString(),
          mode: "dry-run",
          wouldUpdate: validated.length,
          sample: validated,
        },
        null,
        2
      )
    );
    console.log("\nDRY-RUN — re-run with --apply after review.");
    return;
  }

  let updated = 0;
  let errors = 0;
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
    }
    batch = [];
  }
  for (const row of validated) {
    log.push(row);
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= BATCH) await flush();
  }
  await flush();
  writeFileSync(
    join(REPORTS, "ihg-wave2-enrichment-apply-log.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, errors, rows: log }, null, 2)
  );
  console.log("\nUpdated:", updated, "Errors:", errors);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
