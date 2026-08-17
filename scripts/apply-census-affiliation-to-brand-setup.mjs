#!/usr/bin/env node
/**
 * Apply Hotel Census Affiliation rewrites so values match Brand Setup Active/Live Brand Name.
 *
 * Reads plan from reports/census-affiliation-vs-brand-setup-ready.csv
 * (regenerate via audit-census-affiliation-vs-brand-setup.mjs).
 *
 *   node scripts/apply-census-affiliation-to-brand-setup.mjs
 *   node scripts/apply-census-affiliation-to-brand-setup.mjs --apply
 */
import "../load-env.js";
import { readFileSync, existsSync, appendFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { auditCensusAffiliationVsBrandSetup } from "./audit-census-affiliation-vs-brand-setup.mjs";

const APPLY = process.argv.includes("--apply");
const SKIP_AUDIT = process.argv.includes("--skip-audit");
const READY_CSV = join("reports", "census-affiliation-vs-brand-setup-ready.csv");
const LOG_CSV = join("reports", "census-affiliation-vs-brand-setup-applies.csv");
const SUMMARY_JSON = join("reports", "census-affiliation-vs-brand-setup-apply-summary.json");

const MAP = {
  affiliation: CENSUS_FIELDS.affiliation,
};

function parseCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  if (!lines.length) return [];
  const headers = splitCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cols = splitCsvLine(line);
    /** @type {Record<string, string>} */
    const row = {};
    headers.forEach((h, i) => {
      row[h] = cols[i] ?? "";
    });
    return row;
  });
}

function splitCsvLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (inQ && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else inQ = !inQ;
    } else if (c === "," && !inQ) {
      out.push(cur);
      cur = "";
    } else cur += c;
  }
  out.push(cur);
  return out;
}

function validateRow(row) {
  /** @type {string[]} */
  const failed = [];
  if (!row.censusRecordId) failed.push("missing_record_id");
  if (!row.fromAffiliation) failed.push("missing_from");
  if (!row.toAffiliation) failed.push("missing_to");
  if (row.fromAffiliation === row.toAffiliation) failed.push("noop");
  return {
    pass: failed.length === 0,
    failed,
    fieldMapping: MAP,
    sanitizedPayloadPreview: { [MAP.affiliation]: row.toAffiliation },
  };
}

async function main() {
  if (!SKIP_AUDIT) {
    console.log("Refreshing audit…");
    await auditCensusAffiliationVsBrandSetup();
  }
  if (!existsSync(READY_CSV)) {
    throw new Error(`Missing ${READY_CSV}`);
  }

  const rows = parseCsv(readFileSync(READY_CSV, "utf8"));
  console.log(`\nReady rows: ${rows.length}`);
  console.log(`Mode: ${APPLY ? "APPLY" : "dry-run"}`);
  console.log(`Field map: ${MAP.affiliation} ← Brand Setup Brand Name`);

  /** @type {Map<string, number>} */
  const pairs = new Map();
  for (const r of rows) {
    const k = `${r.fromAffiliation} → ${r.toAffiliation}`;
    pairs.set(k, (pairs.get(k) || 0) + 1);
  }
  for (const [k, n] of [...pairs.entries()].sort((a, b) => b[1] - a[1])) {
    console.log(`  ${n}\t${k}`);
  }

  if (!APPLY) {
    console.log("\nDry-run only — pass --apply to write Airtable (typecast:true).");
    writeFileSync(
      SUMMARY_JSON,
      JSON.stringify(
        {
          generatedAt: new Date().toISOString(),
          mode: "dry-run",
          ready: rows.length,
          pairs: Object.fromEntries(pairs),
        },
        null,
        2
      )
    );
    return;
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  if (!existsSync(LOG_CSV)) {
    appendFileSync(
      LOG_CSV,
      "ts,censusRecordId,fromAffiliation,toAffiliation,reason,ok,error\n"
    );
  }

  let applied = 0;
  let failed = 0;
  /** @type {object[]} */
  const errors = [];

  for (const row of rows) {
    const validation = validateRow(row);
    if (!validation.pass) {
      failed++;
      errors.push({ row, failed: validation.failed });
      continue;
    }
    try {
      // Confirm current Affiliation still matches planned from-value (avoid stale plan)
      const current = await base(HOTEL_CENSUS_TABLE).find(row.censusRecordId);
      const currentAff = String(current.fields[MAP.affiliation] || "").trim();
      if (currentAff !== row.fromAffiliation) {
        console.log(
          `Skip stale ${row.censusRecordId}: current="${currentAff}" expected from="${row.fromAffiliation}"`
        );
        failed++;
        appendFileSync(
          LOG_CSV,
          `${new Date().toISOString()},${row.censusRecordId},"${row.fromAffiliation}","${row.toAffiliation}",${row.reason},0,stale_affiliation\n`
        );
        continue;
      }
      await base(HOTEL_CENSUS_TABLE).update(
        row.censusRecordId,
        { [MAP.affiliation]: row.toAffiliation },
        { typecast: true }
      );
      applied++;
      console.log(
        `Updated ${row.censusRecordId}: ${row.fromAffiliation} → ${row.toAffiliation}`
      );
      appendFileSync(
        LOG_CSV,
        `${new Date().toISOString()},${row.censusRecordId},"${row.fromAffiliation}","${row.toAffiliation}",${row.reason},1,\n`
      );
    } catch (err) {
      failed++;
      const msg = String(err?.message || err).replace(/"/g, "'");
      console.error(`FAIL ${row.censusRecordId}`, msg);
      errors.push({ row, error: msg });
      appendFileSync(
        LOG_CSV,
        `${new Date().toISOString()},${row.censusRecordId},"${row.fromAffiliation}","${row.toAffiliation}",${row.reason},0,"${msg}"\n`
      );
    }
  }

  const summary = {
    generatedAt: new Date().toISOString(),
    mode: "apply",
    ready: rows.length,
    applied,
    failed,
    pairs: Object.fromEntries(pairs),
    fieldMapping: MAP,
    errors: errors.slice(0, 20),
  };
  writeFileSync(SUMMARY_JSON, JSON.stringify(summary, null, 2));
  console.log(`\nApplied: ${applied}  Failed: ${failed}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
