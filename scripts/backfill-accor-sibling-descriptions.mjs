#!/usr/bin/env node
/**
 * Sibling Accor: fill-blank Hotel Description (+ Amenities if blank) for CALA
 * Accor parent rows that are NOT Active soft brands, from official Accor URLs.
 *
 *   node scripts/backfill-accor-sibling-descriptions.mjs
 *   node scripts/backfill-accor-sibling-descriptions.mjs --apply --limit=80
 *   node scripts/backfill-accor-sibling-descriptions.mjs --affiliations="ibis|Mercure|Novotel"
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import { ACCOR_FETCH_HEADERS } from "../lib/accor-brand-directory-extract.js";
import { fetchAccorHotelAmenities } from "../lib/accor-hotel-content-fetch.js";

const APPLY = process.argv.includes("--apply");
const limitArg = process.argv.find((a) => a.startsWith("--limit="));
const LIMIT = limitArg ? Number(limitArg.split("=")[1]) : 100;
const affArg = process.argv.find((a) => a.startsWith("--affiliations="))?.split("=")[1];
const AFF_FILTER = affArg
  ? affArg.split("|").map((s) => s.trim()).filter(Boolean)
  : null;
const DELAY = 250;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function extractJsonLdDescription(html) {
  for (const m of String(html || "").matchAll(
    /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
  )) {
    try {
      const json = JSON.parse(m[1]);
      const arr = Array.isArray(json) ? json : [json];
      for (const o of arr) {
        const types = Array.isArray(o["@type"]) ? o["@type"] : [o["@type"]];
        if (!types.some((t) => /Hotel|LodgingBusiness|Resort/i.test(String(t)))) continue;
        const d = String(o.description || "")
          .replace(/\s+/g, " ")
          .trim();
        if (d.length >= 60) return d;
      }
    } catch {
      /* skip */
    }
  }
  const meta = String(html || "").match(
    /<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i
  );
  if (meta) {
    const d = meta[1].replace(/\s+/g, " ").trim();
    if (d.length >= 60 && !/^all\s*-\s*accor/i.test(d)) return d;
  }
  return "";
}

async function main() {
  mkdirSync("reports", { recursive: true });
  const apiKey = process.env.AIRTABLE_API_KEY;
  const mvp = new Airtable({ apiKey }).base(process.env.AIRTABLE_BASE_ID);
  const plat = new Airtable({ apiKey }).base(process.env.AIRTABLE_BASE_ID_ALT);

  const active = new Set(
    (
      await mvp("Brand Setup - Brand Basics")
        .select({ fields: ["Brand Name"], filterByFormula: BRAND_STATUS_ACTIVE_FORMULA })
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
        CENSUS_DESCRIPTION_FIELD,
        CENSUS_AMENITIES_TEXT_FIELD,
      ],
      filterByFormula: `FIND("Accor", {${CENSUS_FIELDS.parentCompany}})`,
      pageSize: 100,
    })
    .all();

  /** @type {import('airtable').Record[]} */
  const candidates = [];
  for (const rec of records) {
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
    const aff = String(rec.fields[CENSUS_FIELDS.affiliation] || "").trim();
    if (!aff || active.has(aff)) continue;
    if (AFF_FILTER && !AFF_FILTER.some((a) => a.toLowerCase() === aff.toLowerCase())) continue;
    const website = String(rec.fields.Website || "").trim();
    if (!website || !/accor\.com/i.test(website)) continue;
    const needDesc = isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD]);
    const needAmen = isBlankCensusValue(rec.fields[CENSUS_AMENITIES_TEXT_FIELD]);
    if (!needDesc && !needAmen) continue;
    candidates.push(rec);
  }

  const work = candidates.slice(0, LIMIT);
  console.log(`Accor sibling candidates: ${candidates.length}; working: ${work.length}`);

  const planRows = [];
  const skipped = [];
  let n = 0;
  for (const rec of work) {
    n++;
    const website = String(rec.fields.Website || "").trim();
    console.log(` [${n}/${work.length}] ${rec.fields[CENSUS_FIELDS.affiliation]} | ${rec.fields.name}`);
    try {
      /** @type {Record<string, string>} */
      const applyFields = {};
      if (isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD])) {
        const res = await fetch(website, { headers: ACCOR_FETCH_HEADERS, redirect: "follow" });
        const html = await res.text();
        const desc = extractJsonLdDescription(html);
        if (desc) applyFields[CENSUS_DESCRIPTION_FIELD] = desc;
        else skipped.push({ id: rec.id, name: rec.fields.name, reason: "empty_desc", status: res.status });
      }
      if (isBlankCensusValue(rec.fields[CENSUS_AMENITIES_TEXT_FIELD])) {
        const am = await fetchAccorHotelAmenities(website);
        if (am.ok && am.amenitiesText) applyFields[CENSUS_AMENITIES_TEXT_FIELD] = am.amenitiesText;
      }
      await sleep(DELAY);
      if (!Object.keys(applyFields).length) {
        if (!skipped.some((s) => s.id === rec.id)) {
          skipped.push({ id: rec.id, name: rec.fields.name, reason: "nothing_extractable" });
        }
        continue;
      }
      planRows.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        affiliation: rec.fields[CENSUS_FIELDS.affiliation],
        website,
        applyFields,
      });
    } catch (err) {
      skipped.push({
        id: rec.id,
        name: rec.fields.name,
        reason: "error",
        error: String(err?.message || err),
      });
    }
  }

  const byAff = {};
  for (const r of planRows) byAff[r.affiliation] = (byAff[r.affiliation] || 0) + 1;

  writeFileSync(
    "reports/accor-sibling-descriptions-plan.json",
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: APPLY ? "apply" : "dry-run",
        limit: LIMIT,
        readyToApply: planRows.length,
        byAffiliation: byAff,
        planRows,
        skipped,
      },
      null,
      2
    )
  );
  console.log("\nReady:", planRows.length, byAff);

  if (!APPLY) {
    console.log("DRY-RUN");
    return;
  }
  let updated = 0;
  let batch = [];
  async function flush() {
    if (!batch.length) return;
    await plat(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
    batch = [];
  }
  for (const row of planRows) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= 10) await flush();
  }
  await flush();
  writeFileSync(
    "reports/accor-sibling-descriptions-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, byAff, planRows }, null, 2)
  );
  console.log("Updated:", updated);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
