#!/usr/bin/env node
/**
 * Derive missing affiliation dates from the paired field (fill-blank only):
 * - Year Affiliated from Open Date
 * - Open Date from Year & Month Affiliated / Affiliated Month when Open Date is blank
 */
import "../load-env.js";
import Airtable from "airtable";
import { mkdirSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import {
  CENSUS_YEAR_AFFILIATED_FIELD,
  yearFromDate,
} from "../lib/hotel-census/hilton-census-field-backfill-contract.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const BATCH = 10;

function openDateFromAffiliation(fields) {
  let year = Number(fields[CENSUS_YEAR_AFFILIATED_FIELD]);
  let month = Number(fields["Affiliated Month"]);

  const yearAffRaw = fields[CENSUS_YEAR_AFFILIATED_FIELD];
  if (Number.isFinite(year) && year >= 100001 && year <= 999912) {
    const s = String(Math.trunc(year));
    year = Number(s.slice(0, 4));
    month = Number(s.slice(4, 6)) || month;
  }

  const yearMonth = String(fields["Year & Month Affiliated"] || "").trim();
  if ((!Number.isFinite(year) || year < 1800) && /^\d{4}/.test(yearMonth)) {
    year = Number(yearMonth.slice(0, 4));
    if (!Number.isFinite(month) || month < 1 || month > 12) {
      const tail = yearMonth.slice(4).replace(/\D/g, "");
      if (tail.length >= 2) month = Number(tail.slice(-2));
    }
  }

  if (!Number.isFinite(year) || year < 1800 || year > 2200) return null;
  const mm = Number.isFinite(month) && month >= 1 && month <= 12 ? String(month).padStart(2, "0") : "01";
  return `${year}-${mm}-01`;
}

function normalizedYearAffiliated(fields) {
  const raw = fields[CENSUS_YEAR_AFFILIATED_FIELD];
  const n = Number(raw);
  if (Number.isFinite(n) && n >= 100001 && n <= 999912) {
    return Number(String(Math.trunc(n)).slice(0, 4));
  }
  if (Number.isFinite(n) && n >= 1800 && n <= 2200) return n;
  const openYear = yearFromDate(fields["Open Date"]);
  return openYear;
}

async function main() {
  const apply = process.argv.includes("--apply");
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const recs = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "Open Date",
        CENSUS_YEAR_AFFILIATED_FIELD,
        "Affiliated Month",
        "Year & Month Affiliated",
        CENSUS_FIELDS.name,
        CENSUS_FIELDS.parentCompany,
      ],
      filterByFormula: `FIND("Hilton", {${CENSUS_FIELDS.parentCompany}})`,
      pageSize: 100,
    })
    .all();

  const patches = [];
  for (const r of recs) {
    const f = r.fields || {};
    const patch = {};

    if (isBlankCensusValue(f[CENSUS_YEAR_AFFILIATED_FIELD])) {
      const year = yearFromDate(f["Open Date"]);
      if (year) patch[CENSUS_YEAR_AFFILIATED_FIELD] = year;
    } else {
      const normalized = normalizedYearAffiliated(f);
      const current = Number(f[CENSUS_YEAR_AFFILIATED_FIELD]);
      if (normalized && normalized !== current) patch[CENSUS_YEAR_AFFILIATED_FIELD] = normalized;
    }

    if (isBlankCensusValue(f["Open Date"])) {
      const openDate = openDateFromAffiliation(f);
      if (openDate) patch["Open Date"] = openDate;
    }

    if (Object.keys(patch).length) {
      patches.push({ id: r.id, name: f[CENSUS_FIELDS.name], fields: patch });
    }
  }

  console.log("Affiliation date patches ready:", patches.length);
  if (!apply) {
    console.log("Run with --apply to write.");
    return;
  }

  let updated = 0;
  let batch = [];
  for (const p of patches) {
    batch.push({ id: p.id, fields: p.fields });
    if (batch.length >= BATCH) {
      await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
      batch = [];
    }
  }
  if (batch.length) {
    await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
  }

  const logPath = join(__dirname, "..", "reports", "hilton-affiliation-date-derive-apply-log.csv");
  mkdirSync(dirname(logPath), { recursive: true });
  writeFileSync(
    logPath,
    `recordId,name,fields\n${patches.map((p) => `${p.id},"${p.name}",${Object.keys(p.fields).join(";")}`).join("\n")}\n`
  );
  console.log("Updated:", updated);
  console.log("Log:", logPath);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
