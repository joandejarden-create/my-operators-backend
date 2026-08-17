#!/usr/bin/env node
/**
 * Apply Hilton description plan to Hotel Census (fill-blank Description field).
 *
 *   node scripts/apply-hilton-census-descriptions.mjs --input reports/hilton-census-descriptions-plan-curio-collection-by-hilton.json
 *   node scripts/apply-hilton-census-descriptions.mjs --input ... --dry-run
 */
import "../load-env.js";
import { readFileSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import {
  CENSUS_DESCRIPTION_FIELD,
  probeCensusDescriptionFields,
} from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { probeHiltonBackfillFields } from "../lib/hotel-census/hilton-census-field-backfill-contract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const BATCH = 10;

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
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const { input, dryRun, force } = parseArgs();
  if (!input) throw new Error("Usage: --input reports/hilton-census-descriptions-plan-....json");

  const plan = JSON.parse(readFileSync(input, "utf8"));
  const rows = plan.planRows || plan;
  if (!Array.isArray(rows)) throw new Error("Invalid plan JSON");

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  const base = new Airtable({ apiKey }).base(baseId);
  const { writable: backfillWritable } = await probeHiltonBackfillFields(base);
  const descriptionFields = await probeCensusDescriptionFields(base);
  const presentFields = [...new Set([...backfillWritable, ...descriptionFields])];

  if (!presentFields.includes(CENSUS_DESCRIPTION_FIELD)) {
    throw new Error(
      `Hotel Census is missing "${CENSUS_DESCRIPTION_FIELD}" column. Add multilineText field in Airtable or set AIRTABLE_CENSUS_DESCRIPTION_FIELD. Plan CSV still has descriptions for review.`
    );
  }

  console.log(`=== Apply Hilton descriptions (${dryRun ? "DRY RUN" : "LIVE"}) ===`);
  console.log("Description field:", CENSUS_DESCRIPTION_FIELD);
  console.log("Writable backfill fields:", presentFields.join(", "));

  const ready = rows.filter((r) => r.status === "ready" && r.censusRecordId && r.applyFields);
  if (!force) {
    for (const r of ready) {
      const filtered = {};
      for (const [k, v] of Object.entries(r.applyFields)) {
        if (presentFields.includes(k)) filtered[k] = v;
      }
      r.applyFields = filtered;
    }
  }

  let updated = 0;
  let errors = 0;
  const log = [];
  let batch = [];

  for (const row of ready) {
    if (!Object.keys(row.applyFields).length) continue;

    log.push({
      action: dryRun ? "would_update" : "updated",
      recordId: row.censusRecordId,
      ctyhocn: row.ctyhocn,
      name: row.censusName,
      fields: Object.keys(row.applyFields).join("; "),
    });

    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= BATCH) {
      if (!dryRun) {
        try {
          await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
          updated += batch.length;
        } catch (err) {
          errors += batch.length;
          console.error("Batch failed:", err?.message || err);
        }
      } else updated += batch.length;
      batch = [];
    }
  }

  if (batch.length) {
    if (!dryRun) {
      try {
        await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
        updated += batch.length;
      } catch (err) {
        errors += batch.length;
        console.error("Batch failed:", err?.message || err);
      }
    } else updated += batch.length;
  }

  const reportPath = join(__dirname, "..", "reports", "hilton-census-descriptions-apply-log.csv");
  mkdirSync(dirname(reportPath), { recursive: true });
  writeFileSync(
    reportPath,
    `action,recordId,ctyhocn,name,fields\n${log.map((r) => [r.action, r.recordId, r.ctyhocn, csvEscape(r.name), r.fields].join(",")).join("\n")}\n`
  );

  console.log("\nReady rows:", ready.length);
  console.log("Updated:", updated, dryRun ? "(dry-run)" : "");
  console.log("Errors:", errors);
  console.log("Log:", reportPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
