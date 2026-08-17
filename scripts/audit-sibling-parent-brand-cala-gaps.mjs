#!/usr/bin/env node
/**
 * Audit CALA Hotel Census sibling brands under Hilton / Accor / Marriott / IHG
 * that are NOT in Active Brand Setup — candidates for parent-API enrichment.
 *
 *   node scripts/audit-sibling-parent-brand-cala-gaps.mjs
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const OUT_CSV = "reports/sibling-parent-brand-cala-gaps.csv";
const OUT_JSON = "reports/sibling-parent-brand-cala-gaps.json";
const OUT_MD = "reports/sibling-parent-brand-cala-gaps.md";

const PARENT_MATCHERS = [
  { key: "Hilton", re: /hilton/i, access: "graphql_directory" },
  { key: "Accor", re: /accor/i, access: "catalog_jsonld" },
  { key: "Marriott", re: /marriott/i, access: "bv_sitemap_partial" },
  { key: "IHG", re: /intercontinental|ihg/i, access: "directory_partial" },
];

function blank(v) {
  return isBlankCensusValue(v);
}

async function main() {
  mkdirSync("reports", { recursive: true });
  const apiKey = process.env.AIRTABLE_API_KEY;
  const mvp = new Airtable({ apiKey }).base(process.env.AIRTABLE_BASE_ID);
  const plat = new Airtable({ apiKey }).base(process.env.AIRTABLE_BASE_ID_ALT);

  const activeBrands = new Set(
    (
      await mvp("Brand Setup - Brand Basics")
        .select({
          fields: ["Brand Name"],
          filterByFormula: BRAND_STATUS_ACTIVE_FORMULA,
        })
        .all()
    )
      .map((r) => String(r.fields["Brand Name"] || "").trim())
      .filter(Boolean)
  );

  const records = await plat(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.parentCompany,
        CENSUS_FIELDS.country,
        "Website",
        CENSUS_PROPERTY_ID_FIELD,
        CENSUS_AMENITIES_TEXT_FIELD,
        CENSUS_DESCRIPTION_FIELD,
      ],
      pageSize: 100,
    })
    .all();

  /** @type {Record<string, object>} */
  const byKey = {};

  for (const rec of records) {
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
    const parent = String(rec.fields[CENSUS_FIELDS.parentCompany] || "").trim();
    const aff = String(rec.fields[CENSUS_FIELDS.affiliation] || "").trim();
    if (!aff) continue;

    const parentHit = PARENT_MATCHERS.find((p) => p.re.test(parent) || p.re.test(aff));
    if (!parentHit) continue;
    if (activeBrands.has(aff)) continue; // siblings only

    const key = `${parentHit.key}||${aff}`;
    if (!byKey[key]) {
      byKey[key] = {
        parentFamily: parentHit.key,
        accessPath: parentHit.access,
        affiliation: aff,
        parentCompany: parent,
        cala: 0,
        blankWebsite: 0,
        blankPropertyId: 0,
        blankAmenities: 0,
        blankDescription: 0,
        withPidBlankDesc: 0,
        withPidBlankAmen: 0,
        withWebsiteBlankAmen: 0,
      };
    }
    const row = byKey[key];
    row.cala++;
    const hasWeb = !blank(rec.fields.Website);
    const hasPid = !blank(rec.fields[CENSUS_PROPERTY_ID_FIELD]);
    if (!hasWeb) row.blankWebsite++;
    if (!hasPid) row.blankPropertyId++;
    if (blank(rec.fields[CENSUS_AMENITIES_TEXT_FIELD])) {
      row.blankAmenities++;
      if (hasPid) row.withPidBlankAmen++;
      if (hasWeb) row.withWebsiteBlankAmen++;
    }
    if (blank(rec.fields[CENSUS_DESCRIPTION_FIELD])) {
      row.blankDescription++;
      if (hasPid) row.withPidBlankDesc++;
    }
  }

  const rows = Object.values(byKey)
    .map((r) => ({
      ...r,
      opportunityScore:
        r.blankWebsite * 3 +
        r.blankPropertyId * 3 +
        r.withPidBlankDesc * 4 +
        r.withPidBlankAmen * 4 +
        r.withWebsiteBlankAmen * 2 +
        (r.blankDescription - r.withPidBlankDesc) +
        (r.blankAmenities - r.withPidBlankAmen),
    }))
    .sort(
      (a, b) =>
        b.opportunityScore - a.opportunityScore ||
        a.parentFamily.localeCompare(b.parentFamily) ||
        a.affiliation.localeCompare(b.affiliation)
    );

  writeCsv(OUT_CSV, rows);
  writeFileSync(
    OUT_JSON,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        activeBrandsExcluded: [...activeBrands].sort(),
        affiliationCount: rows.length,
        totalCalaRows: rows.reduce((s, r) => s + r.cala, 0),
        rows,
      },
      null,
      2
    )
  );

  const byParent = {};
  for (const r of rows) {
    if (!byParent[r.parentFamily]) byParent[r.parentFamily] = [];
    byParent[r.parentFamily].push(r);
  }

  const md = [
    "# Sibling parent-brand CALA gaps (non-Active)",
    "",
    `**Generated:** ${new Date().toISOString().slice(0, 10)}`,
    `**Active brands excluded:** ${activeBrands.size}`,
    `**Sibling affiliations with CALA rows:** ${rows.length}`,
    "",
    "## Top opportunities (by score)",
    "",
    "| Score | Parent | Affiliation | CALA | Blank Web | Blank PID | PID→blank Desc | PID→blank Amen |",
    "|------:|--------|-------------|-----:|----------:|----------:|---------------:|---------------:|",
    ...rows.slice(0, 40).map(
      (r) =>
        `| ${r.opportunityScore} | ${r.parentFamily} | ${r.affiliation} | ${r.cala} | ${r.blankWebsite} | ${r.blankPropertyId} | ${r.withPidBlankDesc} | ${r.withPidBlankAmen} |`
    ),
    "",
    "## By parent family",
    "",
  ];
  for (const [fam, list] of Object.entries(byParent).sort()) {
    md.push(`### ${fam} (${list.length} affiliations)`);
    md.push("");
    for (const r of list.slice(0, 15)) {
      md.push(
        `- **${r.affiliation}** — cala ${r.cala}; blank W/P/A/D ${r.blankWebsite}/${r.blankPropertyId}/${r.blankAmenities}/${r.blankDescription}; PID→desc ${r.withPidBlankDesc}, PID→amen ${r.withPidBlankAmen}`
      );
    }
    if (list.length > 15) md.push(`- … +${list.length - 15} more`);
    md.push("");
  }
  md.push(`CSV: \`${OUT_CSV}\``);
  writeFileSync(OUT_MD, md.join("\n"));

  console.log(`Sibling affiliations: ${rows.length}`);
  console.log("Top 15:");
  for (const r of rows.slice(0, 15)) {
    console.log(
      `  [${r.opportunityScore}] ${r.parentFamily} | ${r.affiliation} | cala=${r.cala} blankW=${r.blankWebsite} pidDesc=${r.withPidBlankDesc} pidAmen=${r.withPidBlankAmen}`
    );
  }
  console.log("MD:", OUT_MD);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
