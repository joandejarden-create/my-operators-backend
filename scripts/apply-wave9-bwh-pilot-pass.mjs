#!/usr/bin/env node
/**
 * Apply the Wave 9 BWH puppeteer pass (Monterrey Premier 70262) fill-blank only.
 *
 *   node scripts/apply-wave9-bwh-pilot-pass.mjs
 *   node scripts/apply-wave9-bwh-pilot-pass.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import puppeteer from "puppeteer";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const APPLY = process.argv.includes("--apply");
const PID = "70262";
const URL =
  "https://www.bestwestern.com/en_US/book/hotels-in-apodaca/best-western-premier-monterrey-aeropuerto/propertyCode.70262.html";
const PROFILE = join("data", "wave9-chrome-profile");

function extractMetaDescription(html) {
  const m = String(html || "").match(
    /<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i
  );
  return m ? m[1].replace(/\s+/g, " ").trim() : "";
}

function extractBwhLabelsFromHtml(html) {
  /** @type {string[]} */
  const labels = [];
  const seen = new Set();
  const push = (raw) => {
    const s = String(raw || "")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    if (!s || s.length < 3 || s.length > 80) return;
    if (/best western|book now|sign in|^menu$/i.test(s)) return;
    const key = s.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    labels.push(s);
  };
  for (const m of String(html || "").matchAll(/"amenity(?:Name|Label|Description)"\s*:\s*"([^"]+)"/gi)) {
    push(m[1]);
  }
  for (const m of String(html || "").matchAll(
    /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
  )) {
    try {
      const json = JSON.parse(m[1]);
      const arr = Array.isArray(json) ? json : [json];
      for (const o of arr) {
        const feats = Array.isArray(o?.amenityFeature)
          ? o.amenityFeature
          : o?.amenityFeature
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
  const browser = await puppeteer.launch({
    headless: "new",
    channel: "chrome",
    userDataDir: PROFILE,
    args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
  });
  let html = "";
  try {
    const page = await browser.newPage();
    await page.goto(URL, { waitUntil: "networkidle2", timeout: 120000 });
    await new Promise((r) => setTimeout(r, 3000));
    html = await page.content();
  } finally {
    await browser.close();
  }

  const desc = extractMetaDescription(html);
  const amenities = extractBwhLabelsFromHtml(html);
  console.log("desc", desc.length, desc.slice(0, 120));
  console.log("amenities", amenities.length, amenities.slice(0, 8));

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        CENSUS_PROPERTY_ID_FIELD,
        CENSUS_AMENITIES_TEXT_FIELD,
        CENSUS_DESCRIPTION_FIELD,
      ],
      filterByFormula: `{${CENSUS_PROPERTY_ID_FIELD}}="${PID}"`,
      maxRecords: 5,
    })
    .all();

  if (!records.length) throw new Error(`No census row for Property ID ${PID}`);
  const rec = records[0];
  /** @type {Record<string, string>} */
  const applyFields = {};
  if (isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD]) && desc.length >= 40) {
    applyFields[CENSUS_DESCRIPTION_FIELD] = desc;
  }
  if (isBlankCensusValue(rec.fields[CENSUS_AMENITIES_TEXT_FIELD]) && amenities.length >= 3) {
    applyFields[CENSUS_AMENITIES_TEXT_FIELD] = amenities.join("; ");
  }

  const plan = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    censusRecordId: rec.id,
    censusName: rec.fields.name,
    propertyId: PID,
    applyFields,
  };
  writeFileSync("reports/wave9-bwh-pilot-apply-plan.json", JSON.stringify(plan, null, 2));
  console.log("Would apply fields:", Object.keys(applyFields));

  if (!APPLY) {
    console.log("DRY-RUN — pass --apply to write");
    return;
  }
  if (!Object.keys(applyFields).length) {
    console.log("Nothing to apply");
    return;
  }
  await base(HOTEL_CENSUS_TABLE).update([{ id: rec.id, fields: applyFields }], { typecast: true });
  writeFileSync(
    "reports/wave9-bwh-pilot-apply-log.json",
    JSON.stringify({ ...plan, updated: true }, null, 2)
  );
  console.log("Updated", rec.id);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
