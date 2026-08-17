#!/usr/bin/env node
/**
 * Audit Hilton Hotel Census status (Open vs Pipeline) against hilton.com GraphQL.
 *
 *   node scripts/audit-hilton-census-status.mjs
 *   node scripts/audit-hilton-census-status.mjs --apply
 *   node scripts/audit-hilton-census-status.mjs --apply --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  auditHiltonCensusStatus,
  buildStatusCorrectionFields,
} from "../lib/hotel-census/audit-hilton-census-status.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  return {
    apply: args.includes("--apply"),
    dryRun: args.includes("--dry-run"),
    input: get("--input"),
    fetchDelayMs: (() => {
      const i = args.indexOf("--fetch-delay-ms");
      return i >= 0 ? Number(args[i + 1] || 250) : 250;
    })(),
    onlyVerdict: (() => {
      const i = args.indexOf("--only-verdict");
      return i >= 0 ? args[i + 1] : null;
    })(),
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const opts = parseArgs();
  console.log("=== Audit Hilton Census Status (Open vs Pipeline) ===\n");

  let audit;
  if (opts.input) {
    const { readFileSync } = await import("node:fs");
    audit = JSON.parse(readFileSync(opts.input, "utf8"));
    if (audit.auditRows) {
      // full report file
    } else {
      throw new Error("Invalid audit JSON — expected auditRows");
    }
    console.log("(Loaded from", opts.input + ")");
  } else {
    audit = await auditHiltonCensusStatus({
      fetchDelayMs: opts.fetchDelayMs,
      onProgress: (msg) => console.log(" ", msg),
    });
  }

  let rows = audit.auditRows;
  if (opts.onlyVerdict) {
    rows = rows.filter((r) => r.verdict === opts.onlyVerdict);
  }

  mkdirSync(REPORTS, { recursive: true });
  const jsonPath = join(REPORTS, "hilton-census-status-audit.json");
  const csvPath = join(REPORTS, "hilton-census-status-audit.csv");

  writeFileSync(
    jsonPath,
    JSON.stringify({ generatedAt: new Date().toISOString(), ...audit }, null, 2)
  );

  const headers = [
    "verdict",
    "censusRecordId",
    "censusName",
    "hiltonName",
    "ctyhocn",
    "censusStatus",
    "hiltonStatus",
    "hiltonOpenDate",
    "city",
    "country",
    "affiliation",
  ];
  const csvLines = [
    headers.join(","),
    ...audit.auditRows.map((r) =>
      [
        r.verdict,
        r.censusRecordId,
        csvEscape(r.censusName),
        csvEscape(r.hiltonName || ""),
        r.ctyhocn || "",
        r.censusStatus || "",
        r.hiltonStatus || "",
        r.hiltonOpenDate || "",
        csvEscape(r.city),
        csvEscape(r.country),
        csvEscape(r.affiliation),
      ].join(",")
    ),
  ];
  writeFileSync(csvPath, csvLines.join("\n"));

  console.log("\n=== Summary ===");
  for (const [k, v] of Object.entries(audit.summary)) {
    console.log(`  ${k}: ${v}`);
  }
  console.log("\nReports:", jsonPath, csvPath);

  const corrections = audit.auditRows.filter((r) =>
    ["mismatch_census_pipeline_hilton_open", "mismatch_census_open_hilton_pipeline", "census_status_blank"].includes(
      r.verdict
    )
  );

  if (corrections.length) {
    console.log("\n=== Suggested corrections ===");
    for (const r of corrections.slice(0, 20)) {
      console.log(
        `  ${r.verdict}: ${r.censusName} | census=${r.censusStatus || "(blank)"} → hilton=${r.hiltonStatus}`
      );
    }
    if (corrections.length > 20) console.log(`  ... and ${corrections.length - 20} more`);
  }

  if (!opts.apply) {
    console.log("\nRun with --apply to write status corrections to Airtable (or --apply --dry-run).");
    return;
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  const base = new Airtable({ apiKey }).base(baseId);
  let updated = 0;
  let batch = [];

  for (const row of corrections) {
    const fields = buildStatusCorrectionFields(row);
    if (!Object.keys(fields).length) continue;
    batch.push({ id: row.censusRecordId, fields });
    if (batch.length >= BATCH) {
      if (!opts.dryRun) await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
      batch = [];
    }
  }
  if (batch.length) {
    if (!opts.dryRun) await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
  }

  console.log(`\n${opts.dryRun ? "Would update" : "Updated"}: ${updated} records`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
