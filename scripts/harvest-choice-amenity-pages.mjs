#!/usr/bin/env node
/**
 * Harvest Choice property page HTML for amenities (Akamai — use visible Chrome + profile).
 * Solve any bot challenge once; cookies persist in data/choice-browser-profile.
 *
 *   node scripts/harvest-choice-amenity-pages.mjs --limit 5
 *   node scripts/harvest-choice-amenity-pages.mjs --property-id mx043 mx092
 *   node scripts/harvest-choice-amenity-pages.mjs --limit 10 --apply-import
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import puppeteer from "puppeteer";
import Airtable from "airtable";
import {
  parseChoiceAmenitiesFromHtml,
  choicePropertyIdFromUrl,
} from "../lib/choice-hotel-content-fetch.js";
import { buildChoiceRegionalPageForCountry } from "../lib/choice-regional-directory-extract.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, "..", "reports", "choice-amenity-html");
const PROFILE_DIR = join(__dirname, "..", "data", "choice-browser-profile");

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const eq = args.find((a) => a.startsWith(`${flag}=`));
    if (eq) return eq.slice(flag.length + 1);
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  const pidIdx = args.indexOf("--property-id");
  const propertyIds =
    pidIdx >= 0
      ? args.slice(pidIdx + 1).filter((a) => !a.startsWith("--"))
      : [];
  return {
    limit: Number(get("--limit") || 0),
    waitMs: Number(get("--wait-ms") || 6000),
    applyImport: args.includes("--apply-import"),
    headless: args.includes("--headless"),
    propertyIds,
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isBlockedHtml(html) {
  return /access denied|robot check|captcha/i.test(html) || html.length < 5000;
}

async function fetchPropertyHtml(page, propertyUrl, country, waitMs) {
  const regional = buildChoiceRegionalPageForCountry(country)?.url;
  if (regional) {
    await page.goto(regional, { waitUntil: "networkidle2", timeout: 120000 });
    await sleep(1500);
  }

  await page.goto(propertyUrl, { waitUntil: "domcontentloaded", timeout: 120000 });

  for (let attempt = 0; attempt < 8; attempt++) {
    const html = await page.content();
    const parsed = parseChoiceAmenitiesFromHtml(html);
    if (parsed.amenities.length) return { html, parsed };
    if (!isBlockedHtml(html) && html.length > 30000) {
      return { html, parsed };
    }
    await sleep(Math.max(waitMs, 4000));
  }

  const html = await page.content();
  return { html, parsed: parseChoiceAmenitiesFromHtml(html) };
}

async function loadTargets(limit, propertyIds) {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const recs = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        "Website",
        "Property ID",
        CENSUS_AMENITIES_TEXT_FIELD,
        CENSUS_FIELDS.country,
        CENSUS_FIELDS.parentCompany,
      ],
      filterByFormula: `FIND("Choice", {${CENSUS_FIELDS.parentCompany}})`,
    })
    .all();

  let targets = recs
    .filter((r) => {
      const url = String(r.fields?.Website || "").trim();
      return (
        url &&
        /choicehotels\.com/i.test(url) &&
        isBlankCensusValue(r.fields?.[CENSUS_AMENITIES_TEXT_FIELD])
      );
    })
    .map((r) => {
      const pid =
        String(r.fields["Property ID"] || choicePropertyIdFromUrl(r.fields.Website) || "")
          .trim()
          .toLowerCase() || r.id;
      return {
        recordId: r.id,
        name: r.fields.name,
        propertyId: pid,
        propertyUrl: String(r.fields.Website).trim(),
        country: r.fields[CENSUS_FIELDS.country] || "",
      };
    });

  if (propertyIds.length) {
    const want = new Set(propertyIds.map((p) => p.toLowerCase()));
    targets = targets.filter((t) => want.has(t.propertyId));
  }

  return limit > 0 ? targets.slice(0, limit) : targets;
}

async function applyAmenities(rows) {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  let applied = 0;
  for (const row of rows) {
    if (!row.amenitiesText || !row.recordId) continue;
    await base(HOTEL_CENSUS_TABLE).update(row.recordId, {
      [CENSUS_AMENITIES_TEXT_FIELD]: row.amenitiesText,
    });
    applied++;
    console.log("  Applied", row.propertyId, row.amenitiesText.slice(0, 60) + "…");
  }
  return applied;
}

async function main() {
  const opts = parseArgs();
  mkdirSync(OUT_DIR, { recursive: true });
  mkdirSync(PROFILE_DIR, { recursive: true });

  const targets = await loadTargets(opts.limit, opts.propertyIds);
  console.log("Harvest targets:", targets.length);
  if (!targets.length) {
    console.log("Nothing to harvest.");
    return;
  }

  console.log(
    "Opening Chrome (headed). If Akamai challenges appear, complete them in the browser window."
  );

  const browser = await puppeteer.launch({
    headless: opts.headless ? "new" : false,
    userDataDir: PROFILE_DIR,
    channel: "chrome",
    args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
  });

  const page = await browser.newPage();
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => false });
  });

  /** @type {object[]} */
  const harvested = [];
  /** @type {object[]} */
  const failed = [];

  for (let i = 0; i < targets.length; i++) {
    const t = targets[i];
    const outPath = join(OUT_DIR, `${t.propertyId}.html`);

    if (existsSync(outPath)) {
      const cachedHtml = readFileSync(outPath, "utf8");
      const cached = parseChoiceAmenitiesFromHtml(cachedHtml);
      if (!isBlockedHtml(cachedHtml) && cached.amenities.length) {
        console.log(`[${i + 1}/${targets.length}] skip cached ${t.propertyId}`);
        harvested.push({ ...t, amenitiesText: cached.amenitiesText, cached: true });
        continue;
      }
    }

    console.log(`[${i + 1}/${targets.length}] ${t.propertyId} ${t.name}`);
    try {
      const { html, parsed } = await fetchPropertyHtml(page, t.propertyUrl, t.country, opts.waitMs);
      if (isBlockedHtml(html)) {
        const reason = "access_denied";
        failed.push({ ...t, reason, htmlLength: html.length });
        console.log("  FAIL", reason, "len", html.length, "(not saved — blocked shell)");
      } else {
        writeFileSync(outPath, html);
        if (parsed.amenities.length) {
          harvested.push({ ...t, amenitiesText: parsed.amenitiesText, amenityCount: parsed.amenities.length });
          console.log("  OK amenities:", parsed.amenities.length);
        } else {
          failed.push({ ...t, reason: "no_amenities_parsed", htmlLength: html.length });
          console.log("  FAIL no_amenities_parsed len", html.length);
        }
      }
    } catch (err) {
      failed.push({ ...t, reason: err?.message || String(err) });
      console.log("  ERR", failed.at(-1).reason);
    }
    await sleep(500);
  }

  await browser.close();

  writeFileSync(
    join(__dirname, "..", "reports", "choice-amenity-harvest-summary.json"),
    JSON.stringify(
      { harvested: harvested.length, failed: failed.length, failedSample: failed.slice(0, 15) },
      null,
      2
    )
  );

  console.log("\nHarvested:", harvested.length, "Failed:", failed.length, "Dir:", OUT_DIR);

  if (opts.applyImport && harvested.length) {
    const n = await applyAmenities(harvested);
    console.log("Applied to Airtable:", n);
  } else if (harvested.length) {
    console.log("Run: node scripts/apply-choice-amenities-from-html.mjs --apply");
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
