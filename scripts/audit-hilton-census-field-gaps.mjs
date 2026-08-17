#!/usr/bin/env node
/** Export Hilton census rows still missing target fields for steward review. */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { auditHiltonCensusFieldBlanks } from "../lib/hotel-census/plan-hilton-census-field-backfill.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { resolveCensusCtyhocn } from "../lib/hotel-census/plan-hilton-census-descriptions.js";
import { mapCensusRowForDirectoryMatch } from "../lib/hotel-census/match-brand-directory-to-census.js";
import { MAP_HILTON_CENSUS_FIELD_BACKFILL, CENSUS_FORMULA_AFFILIATION_FIELDS } from "../lib/hotel-census/hilton-census-field-backfill-contract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const TARGETS = [
  MAP_HILTON_CENSUS_FIELD_BACKFILL.hotelDescription,
  MAP_HILTON_CENSUS_FIELD_BACKFILL.amenities,
  MAP_HILTON_CENSUS_FIELD_BACKFILL.website,
  MAP_HILTON_CENSUS_FIELD_BACKFILL.openDate,
  MAP_HILTON_CENSUS_FIELD_BACKFILL.yearAffiliated,
];

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const audit = await auditHiltonCensusFieldBlanks();
  const base = getPlatformBase();
  const fields = [
    CENSUS_FIELDS.name,
    CENSUS_FIELDS.city,
    CENSUS_FIELDS.country,
    CENSUS_FIELDS.affiliation,
    CENSUS_FIELDS.status,
    CENSUS_FIELDS.parentCompany,
    ...TARGETS,
    ...CENSUS_FORMULA_AFFILIATION_FIELDS,
    "Website",
  ];
  const recs = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [...new Set(fields)],
      filterByFormula: `FIND("Hilton", {${CENSUS_FIELDS.parentCompany}})`,
      pageSize: 100,
    })
    .all();

  const rows = [];
  for (const rec of recs) {
    const f = rec.fields || {};
    const blankFields = TARGETS.filter((col) => isBlankCensusValue(f[col]));
    if (!blankFields.length) continue;
    const censusRow = mapCensusRowForDirectoryMatch(rec);
    const ctyhocn = resolveCensusCtyhocn(censusRow) || "";
    rows.push({
      recordId: rec.id,
      name: f[CENSUS_FIELDS.name] || "",
      city: f[CENSUS_FIELDS.city] || "",
      country: f[CENSUS_FIELDS.country] || "",
      affiliation: f[CENSUS_FIELDS.affiliation] || "",
      status: Array.isArray(f[CENSUS_FIELDS.status]) ? f[CENSUS_FIELDS.status].join(", ") : f[CENSUS_FIELDS.status] || "",
      ctyhocn,
      blankFields: blankFields.join("; "),
      hasHiltonCode: ctyhocn ? "yes" : "no",
      yearMonthAffiliated: f["Year & Month Affiliated"] || "",
      affiliatedMonth: f["Affiliated Month"] || "",
    });
  }

  const reportPath = join(__dirname, "..", "reports", "hilton-census-field-gaps-steward.csv");
  mkdirSync(dirname(reportPath), { recursive: true });
  const header =
    "recordId,name,city,country,affiliation,status,ctyhocn,hasHiltonCode,blankFields,yearMonthAffiliated,affiliatedMonth";
  writeFileSync(
    reportPath,
    `${header}\n${rows
      .map((r) =>
        [
          r.recordId,
          csvEscape(r.name),
          csvEscape(r.city),
          csvEscape(r.country),
          csvEscape(r.affiliation),
          csvEscape(r.status),
          r.ctyhocn,
          r.hasHiltonCode,
          csvEscape(r.blankFields),
          csvEscape(r.yearMonthAffiliated),
          csvEscape(r.affiliatedMonth),
        ].join(",")
      )
      .join("\n")}\n`
  );

  const withCode = rows.filter((r) => r.ctyhocn).length;
  const withoutCode = rows.length - withCode;

  console.log("=== Hilton census field gaps (steward report) ===\n");
  console.log("Total Hilton rows:", audit.total);
  console.log("Rows with any target blank:", rows.length);
  console.log("  With Hilton ctyhocn:", withCode);
  console.log("  Without ctyhocn (manual / pipeline):", withoutCode);
  console.log("\nBlank counts:");
  for (const [k, v] of Object.entries(audit.blankCounts)) {
    console.log(`  ${k}: ${v}`);
  }
  console.log("\nFormula fields (auto when Open Date + Year Affiliated set):");
  console.log(" ", CENSUS_FORMULA_AFFILIATION_FIELDS.join(", "));
  console.log("\nReport:", reportPath);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
