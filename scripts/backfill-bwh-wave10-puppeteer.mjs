#!/usr/bin/env node
/**
 * Wave 10: fill-blank Amenities + Hotel Description for BW Premier / Signature
 * rows with bestwestern.com Website via Chrome-channel puppeteer.
 *
 *   node scripts/backfill-bwh-wave10-puppeteer.mjs
 *   node scripts/backfill-bwh-wave10-puppeteer.mjs --apply
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
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";

const APPLY = process.argv.includes("--apply");
const AFFS = ["BW Premier Collection", "BW Signature Collection"];
const PROFILE = join("data", "wave9-chrome-profile");
const DELAY = 2000;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isBlocked(html) {
  return /access denied|attention required|captcha|cf-challenge|robot check|perimeterx/i.test(
    String(html || "").slice(0, 8000)
  );
}

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

async function fetchPage(browser, url) {
  const page = await browser.newPage();
  try {
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, "webdriver", { get: () => false });
    });
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    );
    const resp = await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
    await sleep(3000);
    const html = await page.content();
    return { status: resp?.status() || 0, html, blocked: isBlocked(html), htmlLen: html.length };
  } finally {
    await page.close();
  }
}

async function main() {
  mkdirSync("reports", { recursive: true });
  mkdirSync(PROFILE, { recursive: true });

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
        CENSUS_AMENITIES_TEXT_FIELD,
        CENSUS_DESCRIPTION_FIELD,
      ],
      filterByFormula: formula,
      pageSize: 100,
    })
    .all();

  const candidates = records.filter((rec) => {
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) return false;
    const need =
      isBlankCensusValue(rec.fields[CENSUS_AMENITIES_TEXT_FIELD]) ||
      isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD]);
    if (!need) return false;
    return /bestwestern\.com/i.test(String(rec.fields.Website || ""));
  });

  console.log(`BWH candidates with bestwestern.com + blank content: ${candidates.length}`);

  const browser = await puppeteer.launch({
    headless: "new",
    channel: "chrome",
    userDataDir: PROFILE,
    args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
  });

  const planRows = [];
  const skipped = [];
  try {
    let n = 0;
    for (const rec of candidates) {
      n++;
      const website = String(rec.fields.Website || "").trim();
      const pid = String(rec.fields[CENSUS_PROPERTY_ID_FIELD] || "").trim();
      console.log(` [${n}/${candidates.length}] ${rec.fields.name} ${pid}`);
      try {
        const got = await fetchPage(browser, website);
        await sleep(DELAY);
        if (got.blocked || got.htmlLen < 5000) {
          skipped.push({
            id: rec.id,
            name: rec.fields.name,
            pid,
            reason: got.blocked ? "blocked" : "html_too_small",
            status: got.status,
          });
          continue;
        }
        const desc = extractMetaDescription(got.html);
        const amenities = extractBwhLabelsFromHtml(got.html);
        /** @type {Record<string, string>} */
        const applyFields = {};
        if (
          isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD]) &&
          desc.length >= 40
        ) {
          applyFields[CENSUS_DESCRIPTION_FIELD] = desc;
        }
        if (
          isBlankCensusValue(rec.fields[CENSUS_AMENITIES_TEXT_FIELD]) &&
          amenities.length >= 3
        ) {
          applyFields[CENSUS_AMENITIES_TEXT_FIELD] = amenities.join("; ");
        }
        if (!Object.keys(applyFields).length) {
          skipped.push({
            id: rec.id,
            name: rec.fields.name,
            pid,
            reason: "empty_extract",
            descLen: desc.length,
            amenityCount: amenities.length,
          });
          continue;
        }
        planRows.push({
          censusRecordId: rec.id,
          censusName: rec.fields.name,
          affiliation: rec.fields[CENSUS_FIELDS.affiliation],
          propertyId: pid,
          amenityCount: amenities.length,
          descriptionLen: desc.length,
          applyFields,
        });
        console.log(`  ready fields=${Object.keys(applyFields).join("+")} amen=${amenities.length}`);
      } catch (err) {
        skipped.push({
          id: rec.id,
          name: rec.fields.name,
          reason: "error",
          error: String(err?.message || err),
        });
      }
    }
  } finally {
    await browser.close();
  }

  writeFileSync(
    "reports/bwh-wave10-puppeteer-plan.json",
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
  console.log(`\nReady: ${planRows.length} Skipped: ${skipped.length}`);

  if (!APPLY) {
    console.log("DRY-RUN — pass --apply to write");
    return;
  }
  for (const row of planRows) {
    await base(HOTEL_CENSUS_TABLE).update([{ id: row.censusRecordId, fields: row.applyFields }], {
      typecast: true,
    });
  }
  writeFileSync(
    "reports/bwh-wave10-puppeteer-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated: planRows.length, planRows }, null, 2)
  );
  console.log("Updated:", planRows.length);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
