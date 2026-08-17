#!/usr/bin/env node
/**
 * Wave 7: export steward worklist for Active Brand Setup × CALA Hotel Census
 * residual blanks (Website / Property ID / Amenities / Hotel Description).
 *
 * Prioritizes rows that already have an official Website (browser-save path)
 * vs Pipeline/no-URL steward (directory research only).
 *
 *   node scripts/export-active-brand-cala-steward-worklist.mjs
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const OUT_CSV = join("reports", "active-brand-cala-steward-worklist-wave7.csv");
const OUT_JSON = join("reports", "active-brand-cala-steward-worklist-wave7.json");
const OUT_MD = join("reports", "active-brand-cala-steward-worklist-wave7.md");

function blankFields(fields) {
  /** @type {string[]} */
  const out = [];
  if (isBlankCensusValue(fields.Website)) out.push("Website");
  if (isBlankCensusValue(fields[CENSUS_PROPERTY_ID_FIELD])) out.push("Property ID");
  if (isBlankCensusValue(fields[CENSUS_AMENITIES_TEXT_FIELD])) out.push("Amenities");
  if (isBlankCensusValue(fields[CENSUS_DESCRIPTION_FIELD])) out.push("Hotel Description");
  return out;
}

function stewardPath(row) {
  const blanks = row.blankFields;
  const hasUrl = Boolean(row.website);
  if (!hasUrl && (blanks.includes("Website") || blanks.includes("Property ID"))) {
    return "directory_research";
  }
  if (hasUrl && (blanks.includes("Amenities") || blanks.includes("Hotel Description"))) {
    return "browser_save_property_page";
  }
  if (hasUrl && blanks.includes("Property ID")) {
    return "extract_id_from_url";
  }
  return "steward_review";
}

function priority(row) {
  // Lower = higher priority
  const path = row.stewardPath;
  if (path === "browser_save_property_page") return 1;
  if (path === "extract_id_from_url") return 2;
  if (path === "directory_research") return 3;
  return 4;
}

async function main() {
  mkdirSync("reports", { recursive: true });
  const apiKey = process.env.AIRTABLE_API_KEY;
  const mvp = new Airtable({ apiKey }).base(process.env.AIRTABLE_BASE_ID);
  const plat = new Airtable({ apiKey }).base(process.env.AIRTABLE_BASE_ID_ALT);

  const brands = (
    await mvp("Brand Setup - Brand Basics")
      .select({
        fields: ["Brand Name", "Parent Company"],
        filterByFormula: BRAND_STATUS_ACTIVE_FORMULA,
        pageSize: 100,
      })
      .all()
  )
    .map((r) => ({
      id: r.id,
      name: String(r.fields["Brand Name"] || "").trim(),
      parent: String(r.fields["Parent Company"] || "").trim(),
    }))
    .filter((b) => b.name);
  const want = new Map(brands.map((b) => [b.name, b]));

  const records = await plat(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.country,
        CENSUS_FIELDS.city,
        "Website",
        CENSUS_PROPERTY_ID_FIELD,
        CENSUS_AMENITIES_TEXT_FIELD,
        CENSUS_DESCRIPTION_FIELD,
      ],
      pageSize: 100,
    })
    .all();

  /** @type {object[]} */
  const rows = [];
  for (const rec of records) {
    const aff = String(rec.fields[CENSUS_FIELDS.affiliation] || "").trim();
    const brand = want.get(aff);
    if (!brand) continue;
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
    const blanks = blankFields(rec.fields);
    if (!blanks.length) continue;
    const website = String(rec.fields.Website || "").trim();
    const row = {
      censusRecordId: rec.id,
      brandName: brand.name,
      parentCompany: brand.parent,
      censusName: rec.fields.name,
      country: rec.fields[CENSUS_FIELDS.country],
      city: rec.fields[CENSUS_FIELDS.city] || "",
      website,
      propertyId: String(rec.fields[CENSUS_PROPERTY_ID_FIELD] || "").trim(),
      blankFields: blanks,
      blankFieldsJoined: blanks.join("; "),
    };
    row.stewardPath = stewardPath(row);
    row.priority = priority(row);
    row.instruction =
      row.stewardPath === "browser_save_property_page"
        ? "Open Website in browser → save complete HTML → extract Amenities/Description from official page only (no invent)."
        : row.stewardPath === "extract_id_from_url"
          ? "Confirm Property ID from official URL/path (slug or brand code); fill-blank only."
          : row.stewardPath === "directory_research"
            ? "Find official open listing on brand directory; do not invent Pipeline IDs or URLs."
            : "Review residual blank fields against official brand sources.";
    rows.push(row);
  }

  rows.sort((a, b) => a.priority - b.priority || a.brandName.localeCompare(b.brandName) || String(a.censusName).localeCompare(String(b.censusName)));

  const byPath = {};
  const byBrand = {};
  for (const r of rows) {
    byPath[r.stewardPath] = (byPath[r.stewardPath] || 0) + 1;
    byBrand[r.brandName] = (byBrand[r.brandName] || 0) + 1;
  }

  writeCsv(
    OUT_CSV,
    rows.map((r) => ({
      priority: r.priority,
      stewardPath: r.stewardPath,
      brandName: r.brandName,
      parentCompany: r.parentCompany,
      censusRecordId: r.censusRecordId,
      censusName: r.censusName,
      country: r.country,
      city: r.city,
      website: r.website,
      propertyId: r.propertyId,
      blankFieldsJoined: r.blankFieldsJoined,
      instruction: r.instruction,
    }))
  );

  writeFileSync(
    OUT_JSON,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        totalResidualRows: rows.length,
        byStewardPath: byPath,
        byBrand,
        rows,
      },
      null,
      2
    )
  );

  const browserSave = rows.filter((r) => r.stewardPath === "browser_save_property_page");
  const md = [
    "# Active Brand CALA — Wave 7 steward worklist",
    "",
    `**Generated:** ${new Date().toISOString().slice(0, 10)}`,
    `**Residual census rows with ≥1 blank field:** ${rows.length}`,
    "",
    "## By steward path",
    "",
    ...Object.entries(byPath).map(([k, v]) => `- **${k}:** ${v}`),
    "",
    "## Browser-save priority (have Website; need Amenities and/or Description)",
    "",
    `Count: **${browserSave.length}**`,
    "",
    "| Brand | Hotel | Country | Blanks | Website |",
    "|-------|-------|---------|--------|---------|",
    ...browserSave.slice(0, 80).map(
      (r) =>
        `| ${r.brandName} | ${String(r.censusName).replace(/\|/g, "/")} | ${r.country} | ${r.blankFieldsJoined} | ${r.website} |`
    ),
    browserSave.length > 80 ? `\n… +${browserSave.length - 80} more in CSV.\n` : "",
    "",
    "## Files",
    "",
    `- CSV: \`${OUT_CSV}\``,
    `- JSON: \`${OUT_JSON}\``,
    "",
    "## How to apply after steward save",
    "",
    "- Choice: save as `reports/choice-amenity-html/{propertyId}.html` then `node scripts/backfill-choice-wave4-from-html.mjs --apply`",
    "- Other brands: provide official HTML/JSON export; use brand-specific backfill scripts (fill-blank only)",
    "",
  ].join("\n");
  writeFileSync(OUT_MD, md);

  console.log("Residual rows:", rows.length);
  console.log("By path:", byPath);
  console.log("CSV:", OUT_CSV);
  console.log("MD:", OUT_MD);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
