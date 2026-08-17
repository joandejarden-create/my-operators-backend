#!/usr/bin/env node
/**
 * Wave 4 Choice content: fill-blank Amenities + Hotel Description from
 * steward-saved choicehotels.com HTML under reports/choice-amenity-html/.
 *
 *   node scripts/backfill-choice-wave4-from-html.mjs
 *   node scripts/backfill-choice-wave4-from-html.mjs --apply
 *   node scripts/backfill-choice-wave4-from-html.mjs --affiliation="Ascend Hotel Collection"
 */
import "../load-env.js";
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { parseChoiceAmenitiesFromHtml } from "../lib/choice-hotel-content-fetch.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";

const APPLY = process.argv.includes("--apply");
const HTML_DIR = "reports/choice-amenity-html";
const affArg =
  process.argv.find((a) => a.startsWith("--affiliation="))?.split("=")[1] || "";

const DEFAULT_AFFS = [
  "Ascend Hotel Collection",
  "Comfort Inn & Suites",
  "Quality Inn",
  "Radisson by Choice",
  "Radisson Blu by Choice",
  "Radisson Individuals by Choice",
  "Radisson RED by Choice",
  "Country Inn & Suites by Choice",
];

function extractMetaDescription(html) {
  const m = String(html || "").match(
    /<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i
  );
  if (!m) {
    const m2 = String(html || "").match(
      /content=["']([^"']+)["'][^>]+name=["']description["']/i
    );
    return m2 ? m2[1].replace(/\s+/g, " ").trim() : "";
  }
  return m[1].replace(/\s+/g, " ").trim();
}

function isUsableChoiceDescription(text) {
  const t = String(text || "").trim();
  if (t.length < 80) return false;
  // Reject catalog/loyalty/booking shells — keep location/amenity-forward copy only
  if (/^choice hotels/i.test(t)) return false;
  if (/choice privileges/i.test(t)) return false;
  if (/earn (and|&) use/i.test(t)) return false;
  if (/^book (direct|online|your|now|a stay)/i.test(t) && !/\b(near|located|offers|features|wi-?fi|breakfast|pool)\b/i.test(t)) {
    return false;
  }
  if (/find a hotel|official site of choice/i.test(t) && t.length < 120) return false;
  // Prefer copy that mentions place or on-property features
  if (!/\b(hotel|resort|inn|located|near|in |offers|features|wi-?fi|breakfast|pool|beach|restaurant)\b/i.test(t)) {
    return false;
  }
  return true;
}

async function main() {
  mkdirSync("reports", { recursive: true });
  if (!existsSync(HTML_DIR)) throw new Error(`Missing ${HTML_DIR}`);

  const affs = affArg ? [affArg] : DEFAULT_AFFS;
  const htmlByPid = new Map();
  for (const f of readdirSync(HTML_DIR)) {
    if (!f.endsWith(".html") || f.startsWith("_")) continue;
    htmlByPid.set(f.replace(/\.html$/i, "").toLowerCase(), join(HTML_DIR, f));
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const formula = `OR(${affs.map((a) => `{${CENSUS_FIELDS.affiliation}}="${a}"`).join(",")})`;
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.country,
        "Website",
        CENSUS_PROPERTY_ID_FIELD,
        CENSUS_AMENITIES_TEXT_FIELD,
        CENSUS_DESCRIPTION_FIELD,
      ],
      filterByFormula: formula,
      pageSize: 100,
    })
    .all();

  const planRows = [];
  const skipped = [];

  for (const rec of records) {
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
    const pid = String(rec.fields[CENSUS_PROPERTY_ID_FIELD] || "")
      .trim()
      .toLowerCase();
    if (!pid || !htmlByPid.has(pid)) {
      skipped.push({
        id: rec.id,
        name: rec.fields.name,
        reason: "no_saved_html",
        pid: pid || null,
      });
      continue;
    }

    const needAmen = isBlankCensusValue(rec.fields[CENSUS_AMENITIES_TEXT_FIELD]);
    const needDesc = isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD]);
    if (!needAmen && !needDesc) {
      skipped.push({ id: rec.id, reason: "fields_present" });
      continue;
    }

    const html = readFileSync(htmlByPid.get(pid), "utf8");
    /** @type {Record<string, string>} */
    const applyFields = {};
    const notes = [];

    if (needAmen) {
      const parsed = parseChoiceAmenitiesFromHtml(html);
      if (parsed.hasAmenityMarkers && parsed.amenities?.length >= 1 && parsed.amenitiesText) {
        applyFields[CENSUS_AMENITIES_TEXT_FIELD] = parsed.amenitiesText;
        notes.push(`amenities:${parsed.amenities.length}`);
      } else {
        notes.push("amenities_parse_failed");
      }
    }

    if (needDesc) {
      const desc = extractMetaDescription(html);
      if (isUsableChoiceDescription(desc)) {
        applyFields[CENSUS_DESCRIPTION_FIELD] = desc;
        notes.push("description:meta");
      } else {
        notes.push("description_unusable");
      }
    }

    if (!Object.keys(applyFields).length) {
      skipped.push({
        id: rec.id,
        name: rec.fields.name,
        pid,
        reason: "nothing_extractable",
        notes,
      });
      continue;
    }

    planRows.push({
      censusRecordId: rec.id,
      censusName: rec.fields.name,
      affiliation: rec.fields[CENSUS_FIELDS.affiliation],
      propertyId: pid.toUpperCase(),
      htmlFile: htmlByPid.get(pid),
      notes,
      applyFields,
    });
  }

  const out = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    affiliations: affs,
    htmlFilesIndexed: htmlByPid.size,
    readyToApply: planRows.length,
    planRows,
    skippedSummary: {
      no_saved_html: skipped.filter((s) => s.reason === "no_saved_html").length,
      fields_present: skipped.filter((s) => s.reason === "fields_present").length,
      nothing_extractable: skipped.filter((s) => s.reason === "nothing_extractable").length,
    },
    skipped,
  };
  writeFileSync("reports/choice-wave4-from-html-plan.json", JSON.stringify(out, null, 2));
  console.log(
    `Ready: ${planRows.length} (amenity fills: ${
      planRows.filter((r) => r.applyFields[CENSUS_AMENITIES_TEXT_FIELD]).length
    }, desc fills: ${
      planRows.filter((r) => r.applyFields[CENSUS_DESCRIPTION_FIELD]).length
    })`
  );
  for (const r of planRows.slice(0, 20)) {
    console.log(`  ${r.affiliation} | ${r.censusName} | ${r.propertyId} | ${r.notes.join(",")}`);
  }
  if (planRows.length > 20) console.log(`  … +${planRows.length - 20} more`);

  if (!APPLY) {
    console.log("DRY-RUN — pass --apply to write");
    return;
  }

  let updated = 0;
  let batch = [];
  async function flush() {
    if (!batch.length) return;
    await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
    batch = [];
  }
  for (const row of planRows) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= 10) await flush();
  }
  await flush();
  writeFileSync(
    "reports/choice-wave4-from-html-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, planRows }, null, 2)
  );
  console.log("Updated:", updated);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
