#!/usr/bin/env node
/**
 * Harvest marriott.com overview HTML for amenities parsing.
 * Overview is Akamai-protected — use visible Chrome; solve challenge once if prompted.
 *
 *   node scripts/harvest-marriott-overview-pages.mjs --limit=5
 *   node scripts/harvest-marriott-overview-pages.mjs --apply-import
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import puppeteer from "puppeteer";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import {
  marriottOverviewUrlFromWebsite,
  parseMarriottOverviewHtml,
} from "../lib/marriott-hotel-content-fetch.js";
import { marshaFromMarriottWebsite } from "../lib/marriott-brand-directory-extract.js";
import { planMarriottCensusContentBackfill } from "../lib/hotel-census/plan-marriott-census-content-backfill.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const BATCH = 10;

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, "..", "data", "marriott-overview-harvest");
const PROFILE_DIR = join(__dirname, "..", "data", "marriott-browser-profile");

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const eq = args.find((a) => a.startsWith(`${flag}=`));
    if (eq) return eq.slice(flag.length + 1);
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  return {
    limit: Number(get("--limit") || 0),
    waitMs: Number(get("--wait-ms") || 8000),
    applyImport: args.includes("--apply-import"),
    headless: args.includes("--headless"),
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchOverviewHtml(page, overviewUrl, slug, waitMs) {
  const exp = overviewUrl.replace(/\/overview\/?$/i, "/experiences/");
  await page.goto("https://www.marriott.com/default.mi", {
    waitUntil: "domcontentloaded",
    timeout: 120000,
  });
  await page.goto(exp, { waitUntil: "networkidle2", timeout: 120000 });
  await sleep(2000);
  await page.goto(overviewUrl, { waitUntil: "domcontentloaded", timeout: 120000 });

  for (let attempt = 0; attempt < 6; attempt++) {
    const html = await page.content();
    const parsed = parseMarriottOverviewHtml(html);
    if (parsed.description || parsed.amenities.length) return { html, parsed };
    if (!/access denied|sec-if-cpt|akamai/i.test(html) && html.length > 50000) {
      return { html, parsed };
    }
    await sleep(Math.max(waitMs, 5000));
  }
  return { html: await page.content(), parsed: parseMarriottOverviewHtml(await page.content()) };
}

async function applyContentRows(contentRows, dryRun = false) {
  const plan = await planMarriottCensusContentBackfill({ contentRows });
  console.log("Import ready:", plan.readyToApply, "Skipped:", plan.skipped.length);
  if (!plan.planRows.length || dryRun) return plan;

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  let updated = 0;
  let batch = [];
  for (const row of plan.planRows) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= BATCH) {
      await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
      batch = [];
    }
  }
  if (batch.length) {
    await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
  }
  console.log("Applied overview harvest updates:", updated);
  return plan;
}

async function loadTargets(limit) {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const recs = await base(HOTEL_CENSUS_TABLE)
    .select({
      filterByFormula: `FIND("Marriott", {${CENSUS_FIELDS.parentCompany}})`,
      fields: [
        CENSUS_FIELDS.name,
        MAP_DIRECTORY_ENRICHMENT.website,
        CENSUS_PROPERTY_ID_FIELD,
        CENSUS_AMENITIES_TEXT_FIELD,
        CENSUS_DESCRIPTION_FIELD,
      ],
      pageSize: 100,
    })
    .all();

  const targets = [];
  for (const rec of recs) {
    const f = rec.fields || {};
    if (
      !isBlankCensusValue(f[CENSUS_AMENITIES_TEXT_FIELD]) &&
      !isBlankCensusValue(f[CENSUS_DESCRIPTION_FIELD])
    ) {
      continue;
    }
    const website = String(f[MAP_DIRECTORY_ENRICHMENT.website] || "").trim();
    const marsha =
      String(f[CENSUS_PROPERTY_ID_FIELD] || "").trim().toUpperCase() ||
      marshaFromMarriottWebsite(website);
    const overviewUrl = marriottOverviewUrlFromWebsite(website);
    if (!overviewUrl || !marsha) continue;
    targets.push({ recordId: rec.id, name: f[CENSUS_FIELDS.name], marsha, overviewUrl });
  }
  return limit > 0 ? targets.slice(0, limit) : targets;
}

async function main() {
  const opts = parseArgs();
  mkdirSync(OUT_DIR, { recursive: true });
  mkdirSync(PROFILE_DIR, { recursive: true });

  const targets = await loadTargets(opts.limit);
  console.log("Harvest targets:", targets.length);

  const browser = await puppeteer.launch({
    headless: opts.headless ? "new" : false,
    userDataDir: PROFILE_DIR,
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
    const outPath = join(OUT_DIR, `${t.marsha}.html`);
    if (existsSync(outPath)) {
      const cached = parseMarriottOverviewHtml(readFileSync(outPath, "utf8"));
      if (!/access denied/i.test(readFileSync(outPath, "utf8")) && (cached.amenities.length || cached.description)) {
        console.log(`[${i + 1}/${targets.length}] skip cached ${t.marsha}`);
        harvested.push({ ...t, ...cached, cached: true });
        continue;
      }
    }

    const slug = t.overviewUrl.match(/\/hotels\/([a-z0-9-]+)\//i)?.[1] || "";
    console.log(`[${i + 1}/${targets.length}] ${t.marsha} ${t.name}`);
    try {
      const { html, parsed } = await fetchOverviewHtml(page, t.overviewUrl, slug, opts.waitMs);
      writeFileSync(outPath, html);
      if (parsed.amenities.length || parsed.description) {
        harvested.push({ ...t, amenitiesText: parsed.amenitiesText, description: parsed.description });
        console.log("  OK desc", parsed.description.length, "amen", parsed.amenities.length);
      } else {
        failed.push({ ...t, reason: /access denied/i.test(html) ? "access_denied" : "no_content" });
        console.log("  FAIL", failed.at(-1).reason);
      }
    } catch (err) {
      failed.push({ ...t, reason: err?.message || String(err) });
      console.log("  ERR", failed.at(-1).reason);
    }
    await sleep(500);
  }

  await browser.close();

  writeFileSync(
    join(__dirname, "..", "reports", "marriott-overview-harvest-summary.json"),
    JSON.stringify({ harvested: harvested.length, failed: failed.length, failedSample: failed.slice(0, 20) }, null, 2)
  );

  console.log("\nHarvested:", harvested.length, "Failed:", failed.length, "Dir:", OUT_DIR);

  if (opts.applyImport && harvested.length) {
    const contentRows = harvested
      .filter((h) => h.amenitiesText || h.description)
      .map((h) => ({
        marshaCode: h.marsha,
        description: h.description || "",
        amenitiesText: h.amenitiesText || "",
        website: h.overviewUrl,
      }));
    await applyContentRows(contentRows);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
