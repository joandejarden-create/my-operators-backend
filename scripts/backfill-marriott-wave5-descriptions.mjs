#!/usr/bin/env node
/**
 * Wave 5: fill-blank Hotel Description (+ Amenities if blank) for Autograph /
 * Tribute / Design Hotels rows that already have marriott.com Website,
 * using Marriott overview fetch (puppeteer fallback).
 *
 *   node scripts/backfill-marriott-wave5-descriptions.mjs
 *   node scripts/backfill-marriott-wave5-descriptions.mjs --apply
 *   node scripts/backfill-marriott-wave5-descriptions.mjs --limit=15
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import {
  fetchMarriottHotelContent,
  marriottOverviewUrlFromWebsite,
} from "../lib/marriott-hotel-content-fetch.js";
import { fetchMarriottSubpageContent } from "../lib/marriott-subpage-content-fetch.js";

const APPLY = process.argv.includes("--apply");
const AFFS = ["Autograph Collection", "Tribute Portfolio", "Design Hotels"];
const limitArg = process.argv.find((a) => a.startsWith("--limit="));
const LIMIT = limitArg ? Number(limitArg.split("=")[1]) : 0;
const DELAY = 1500;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  mkdirSync("reports", { recursive: true });
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const formula = `OR(${AFFS.map((a) => `{${CENSUS_FIELDS.affiliation}}="${a}"`).join(",")})`;
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.country,
        "Website",
        CENSUS_PROPERTY_ID_FIELD,
        CENSUS_DESCRIPTION_FIELD,
        CENSUS_AMENITIES_TEXT_FIELD,
      ],
      filterByFormula: formula,
      pageSize: 100,
    })
    .all();

  const candidates = [];
  for (const rec of records) {
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
    const needDesc = isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD]);
    const needAmen = isBlankCensusValue(rec.fields[CENSUS_AMENITIES_TEXT_FIELD]);
    if (!needDesc && !needAmen) continue;
    const website = String(rec.fields.Website || "").trim();
    if (!website || !/marriott\.com/i.test(website)) continue;
    candidates.push(rec);
  }
  const work = LIMIT > 0 ? candidates.slice(0, LIMIT) : candidates;
  console.log(`Candidates with marriott.com Website + blank content: ${candidates.length}; working: ${work.length}`);

  const planRows = [];
  const skipped = [];
  let i = 0;
  for (const rec of work) {
    i++;
    const website = String(rec.fields.Website || "").trim();
    const marsha = String(rec.fields[CENSUS_PROPERTY_ID_FIELD] || "")
      .trim()
      .toUpperCase();
    console.log(` [${i}/${work.length}] ${rec.fields.name} ${marsha || "?"}`);
    try {
      let content = await fetchMarriottHotelContent(website, {
        marshaCode: marsha,
        usePuppeteer: true,
        fallbackPuppeteer: true,
      });
      if (!content.description && !content.amenitiesText) {
        // Subpage / Bonvoy fallback when overview is blocked or URL shape odd
        const sub = await fetchMarriottSubpageContent(website, { marshaCode: marsha });
        if (sub?.description || sub?.amenitiesText) {
          content = {
            ...content,
            description: sub.description || content.description,
            amenitiesText: sub.amenitiesText || content.amenitiesText,
            amenities: sub.amenities || content.amenities,
            source: sub.source || content.source,
            accessDenied: false,
          };
        }
      }
      await sleep(DELAY);

      /** @type {Record<string, string>} */
      const applyFields = {};
      if (
        isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD]) &&
        content.description &&
        content.description.length >= 40
      ) {
        applyFields[CENSUS_DESCRIPTION_FIELD] = content.description;
      }
      if (
        isBlankCensusValue(rec.fields[CENSUS_AMENITIES_TEXT_FIELD]) &&
        content.amenitiesText
      ) {
        applyFields[CENSUS_AMENITIES_TEXT_FIELD] = content.amenitiesText;
      }
      if (!Object.keys(applyFields).length) {
        skipped.push({
          id: rec.id,
          name: rec.fields.name,
          marsha,
          reason: content.accessDenied ? "access_denied" : "empty_content",
          overviewUrl: marriottOverviewUrlFromWebsite(website),
          errors: content.errors || [],
        });
        continue;
      }
      planRows.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        affiliation: rec.fields[CENSUS_FIELDS.affiliation],
        marsha,
        website,
        source: content.source,
        applyFields,
      });
    } catch (err) {
      skipped.push({
        id: rec.id,
        name: rec.fields.name,
        reason: "fetch_error",
        error: String(err?.message || err),
      });
    }
  }

  writeFileSync(
    "reports/marriott-wave5-descriptions-plan.json",
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
  console.log("\nReady:", planRows.length, "Skipped:", skipped.length);
  for (const r of planRows) {
    console.log(
      " ",
      r.affiliation,
      "|",
      r.censusName,
      "|",
      Object.keys(r.applyFields).join("+")
    );
  }

  if (!APPLY) {
    console.log("DRY-RUN");
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
    "reports/marriott-wave5-descriptions-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, planRows }, null, 2)
  );
  console.log("Updated:", updated);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
