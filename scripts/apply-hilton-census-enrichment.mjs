/**
 * Apply Hilton directory enrichment plan to Hotel Census (fill-blank-only by default).
 *
 *   node scripts/apply-hilton-census-enrichment.mjs --input reports/hilton-census-enrichment-plan-curio-collection-by-hilton.json --dry-run
 *   node scripts/apply-hilton-census-enrichment.mjs --input reports/hilton-census-enrichment-plan-curio-collection-by-hilton.json
 *   node scripts/apply-hilton-census-enrichment.mjs --input ... --force
 */
import "../load-env.js";
import { readFileSync, mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import {
  validateEnrichmentPlanRow,
  MAP_DIRECTORY_ENRICHMENT,
} from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { getGovernanceFieldAvailability } from "../lib/hotel-census/census-governance.js";
import {
  CENSUS_AMENITIES_TEXT_FIELD,
  CENSUS_AMENITY_YN_COLUMNS,
} from "../lib/hilton-amenity-map.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  return {
    input: get("--input"),
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    minConfidence: get("--min-confidence") || "medium",
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

const CONF_RANK = { high: 3, medium: 2, low: 1, none: 0 };

async function probeBrandPropertyCodeField(base) {
  const field = MAP_DIRECTORY_ENRICHMENT.brandPropertyCode;
  try {
    await base(HOTEL_CENSUS_TABLE).select({ fields: [field], maxRecords: 1 }).firstPage();
    return true;
  } catch (err) {
    const msg = err?.message || String(err);
    if (/unknown field|not found|invalid/i.test(msg)) return false;
    throw err;
  }
}

/** @returns {Set<string>} */
async function probeCensusAmenityFields(base) {
  const present = new Set();
  for (const field of [CENSUS_AMENITIES_TEXT_FIELD, ...CENSUS_AMENITY_YN_COLUMNS]) {
    try {
      await base(HOTEL_CENSUS_TABLE).select({ fields: [field], maxRecords: 1 }).firstPage();
      present.add(field);
    } catch (err) {
      const msg = err?.message || String(err);
      if (!/unknown field|not found|invalid/i.test(msg)) throw err;
    }
  }
  return present;
}

function filterApplyFieldsToPresentColumns(applyFields, presentAmenityFields) {
  const out = { ...applyFields };
  for (const field of [CENSUS_AMENITIES_TEXT_FIELD, ...CENSUS_AMENITY_YN_COLUMNS]) {
    if (field in out && !presentAmenityFields.has(field)) delete out[field];
  }
  return out;
}

async function main() {
  const { input, dryRun, force, minConfidence } = parseArgs();
  if (!input) {
    throw new Error("Usage: --input reports/hilton-census-enrichment-plan-....json [--dry-run] [--force]");
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT || process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  const plan = JSON.parse(readFileSync(input, "utf8"));
  const planRows = (plan.planRows || []).filter((r) => r.status === "ready");
  const minRank = CONF_RANK[minConfidence] ?? CONF_RANK.medium;

  console.log(`=== Apply Hilton Census Enrichment (${dryRun ? "DRY RUN" : "LIVE"}) ===`);
  console.log(`Plan: ${input}`);
  console.log(`Brand: ${plan.brand}`);
  console.log(`Ready rows: ${planRows.length}`);
  console.log(`Min confidence: ${minConfidence}\n`);

  const base = new Airtable({ apiKey }).base(baseId);
  const governance = await getGovernanceFieldAvailability(base);
  const brandPropertyCodeFieldExists = await probeBrandPropertyCodeField(base);
  const presentAmenityFields = await probeCensusAmenityFields(base);

  if (!brandPropertyCodeFieldExists) {
    console.warn(
      `Note: "${MAP_DIRECTORY_ENRICHMENT.brandPropertyCode}" column not in base — skipping property code writes.`
    );
  }
  if (!governance.dataConfidence) {
    console.warn(`Note: Data Confidence column not present — will omit from patch.`);
  }
  console.log(
    `Amenity columns present: ${[...presentAmenityFields].join(", ") || "(none)"}\n`
  );

  const logPath = join(
    dirname(input).startsWith(join(__dirname, ".."))
      ? dirname(input)
      : join(__dirname, "..", "reports"),
    `hilton-census-enrichment-apply-log-${Date.now()}.csv`
  );

  const logRows = [];
  let updated = 0;
  let skipped = 0;
  let errors = 0;

  for (const row of planRows) {
    const confRank = CONF_RANK[row.matchConfidence] ?? 0;
    if (confRank < minRank) {
      skipped++;
      logRows.push({ ...row, applyStatus: "skipped_low_confidence" });
      continue;
    }

    let applyFields = filterApplyFieldsToPresentColumns(
      { ...(row.applyFields || {}) },
      presentAmenityFields
    );
    if (!governance.dataConfidence) {
      delete applyFields[MAP_DIRECTORY_ENRICHMENT.dataConfidence];
    }
    if (!brandPropertyCodeFieldExists) {
      delete applyFields[MAP_DIRECTORY_ENRICHMENT.brandPropertyCode];
    }

    const validation = validateEnrichmentPlanRow({
      ...row,
      applyFields,
    });
    if (!validation.pass) {
      skipped++;
      console.warn(`  SKIP ${row.censusName}: ${validation.errors.join("; ")}`);
      logRows.push({ ...row, applyStatus: `skipped: ${validation.errors.join("; ")}` });
      continue;
    }

    if (dryRun) {
      updated++;
      const preview = Object.entries(applyFields)
        .map(([k, v]) => `${k}=${JSON.stringify(v)}`)
        .join(", ");
      console.log(`  [dry-run] ${row.censusName}: ${preview}`);
      logRows.push({ ...row, applyStatus: "dry_run", appliedFields: applyFields });
      continue;
    }

    try {
      const rec = await base(HOTEL_CENSUS_TABLE).find(row.censusRecordId);
      const current = rec.fields || {};

      if (!force && plan.scope !== "all_hilton_brands") {
        const affiliation = String(current[MAP_DIRECTORY_ENRICHMENT.affiliation] || "");
        const matchers = plan.alias?.affiliationMatchers || [];
        if (matchers.length && !matchers.includes(affiliation)) {
          skipped++;
          console.warn(`  SKIP ${row.censusName}: affiliation mismatch "${affiliation}"`);
          logRows.push({ ...row, applyStatus: "skipped_affiliation_mismatch" });
          continue;
        }
      }

      await base(HOTEL_CENSUS_TABLE).update(
        [{ id: row.censusRecordId, fields: applyFields }],
        { typecast: true }
      );
      updated++;
      console.log(`  UPDATED ${row.censusName} (${Object.keys(applyFields).length} fields)`);
      logRows.push({ ...row, applyStatus: "ok", appliedFields: applyFields });
    } catch (err) {
      errors++;
      console.error(`  FAILED ${row.censusName}:`, err.message);
      logRows.push({ ...row, applyStatus: `error: ${err.message}` });
    }
  }

  mkdirSync(dirname(logPath), { recursive: true });
  const headers = [
    "applyStatus",
    "censusRecordId",
    "censusName",
    "directoryBrandPropertyCode",
    "matchConfidence",
    "appliedFieldCount",
  ];
  writeFileSync(
    logPath,
    `${headers.join(",")}\n${logRows
      .map((r) =>
        [
          r.applyStatus,
          r.censusRecordId,
          r.censusName,
          r.directoryBrandPropertyCode,
          r.matchConfidence,
          Object.keys(r.appliedFields || r.applyFields || {}).length,
        ]
          .map(csvEscape)
          .join(",")
      )
      .join("\n")}\n`,
    "utf8"
  );

  console.log("\nDone.");
  console.log(`  Updated: ${updated}${dryRun ? " (dry-run)" : ""}`);
  console.log(`  Skipped: ${skipped}`);
  console.log(`  Errors: ${errors}`);
  console.log(`  Log: ${logPath}`);

  if (errors) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
