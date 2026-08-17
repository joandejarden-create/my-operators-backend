#!/usr/bin/env node
/**
 * Wave 6: fill-blank Amenities for Design Hotels from designhotels.com JSON-LD.
 * Also try Tribute blank amenities via Marriott subpage chips when Website present.
 *
 *   node scripts/backfill-design-tribute-wave6-amenities.mjs
 *   node scripts/backfill-design-tribute-wave6-amenities.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import { fetchMarriottSubpageContent } from "../lib/marriott-subpage-content-fetch.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";

const APPLY = process.argv.includes("--apply");
const UA = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html",
};

function extractDesignAmenities(html) {
  /** @type {string[]} */
  const labels = [];
  const seen = new Set();
  const push = (raw) => {
    const s = String(raw || "")
      .replace(/\s+/g, " ")
      .trim();
    if (!s || s.length < 2 || s.length > 100) return;
    if (/^home$|^hotels$/i.test(s)) return;
    const key = s.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    labels.push(s);
  };
  for (const m of String(html || "").matchAll(
    /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
  )) {
    try {
      const json = JSON.parse(m[1]);
      const arr = Array.isArray(json) ? json : [json];
      for (const o of arr) {
        const types = Array.isArray(o["@type"]) ? o["@type"] : [o["@type"]];
        if (!types.some((t) => /Hotel|Lodging|Resort/i.test(String(t)))) continue;
        const feats = Array.isArray(o.amenityFeature)
          ? o.amenityFeature
          : o.amenityFeature
            ? [o.amenityFeature]
            : [];
        for (const f of feats) if (f?.name) push(f.name);
      }
    } catch {
      /* skip */
    }
  }
  return labels.sort((a, b) => a.localeCompare(b));
}

async function main() {
  mkdirSync("reports", { recursive: true });
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const formula = `OR({${CENSUS_FIELDS.affiliation}}="Design Hotels",{${CENSUS_FIELDS.affiliation}}="Tribute Portfolio")`;
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.country,
        "Website",
        CENSUS_PROPERTY_ID_FIELD,
        CENSUS_AMENITIES_TEXT_FIELD,
      ],
      filterByFormula: formula,
      pageSize: 100,
    })
    .all();

  const planRows = [];
  const skipped = [];

  for (const rec of records) {
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
    if (!isBlankCensusValue(rec.fields[CENSUS_AMENITIES_TEXT_FIELD])) {
      skipped.push({ id: rec.id, reason: "present" });
      continue;
    }
    const website = String(rec.fields.Website || "").trim();
    const aff = rec.fields[CENSUS_FIELDS.affiliation];
    const marsha = String(rec.fields[CENSUS_PROPERTY_ID_FIELD] || "")
      .trim()
      .toUpperCase();

    try {
      if (aff === "Design Hotels" && /designhotels\.com/i.test(website)) {
        const res = await fetch(website, { headers: UA, redirect: "follow" });
        const html = await res.text();
        const labels = extractDesignAmenities(html);
        if (!labels.length) {
          skipped.push({ id: rec.id, name: rec.fields.name, reason: "no_design_amenities" });
          continue;
        }
        planRows.push({
          censusRecordId: rec.id,
          censusName: rec.fields.name,
          affiliation: aff,
          source: "designhotels_jsonld",
          applyFields: { [CENSUS_AMENITIES_TEXT_FIELD]: labels.join("; ") },
        });
        continue;
      }

      if (aff === "Tribute Portfolio" && /marriott\.com/i.test(website)) {
        const content = await fetchMarriottSubpageContent(website, { marshaCode: marsha });
        if (!content?.amenitiesText) {
          skipped.push({
            id: rec.id,
            name: rec.fields.name,
            reason: "marriott_subpage_empty",
            errors: content?.parseErrors || [],
          });
          continue;
        }
        planRows.push({
          censusRecordId: rec.id,
          censusName: rec.fields.name,
          affiliation: aff,
          source: content.source || "marriott_subpages",
          applyFields: { [CENSUS_AMENITIES_TEXT_FIELD]: content.amenitiesText },
        });
        continue;
      }

      skipped.push({ id: rec.id, name: rec.fields.name, aff, reason: "no_usable_website" });
    } catch (err) {
      skipped.push({
        id: rec.id,
        name: rec.fields.name,
        reason: "error",
        error: String(err?.message || err),
      });
    }
  }

  writeFileSync(
    "reports/design-tribute-wave6-amenities-plan.json",
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: APPLY ? "apply" : "dry-run",
        readyToApply: planRows.length,
        planRows,
        skipped,
      },
      null,
      2
    )
  );
  console.log("Ready:", planRows.length);
  for (const r of planRows) console.log(" ", r.affiliation, "|", r.censusName, "|", r.source);

  if (!APPLY) {
    console.log("DRY-RUN");
    return;
  }
  for (const row of planRows) {
    await base(HOTEL_CENSUS_TABLE).update([{ id: row.censusRecordId, fields: row.applyFields }], {
      typecast: true,
    });
  }
  console.log("Updated:", planRows.length);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
