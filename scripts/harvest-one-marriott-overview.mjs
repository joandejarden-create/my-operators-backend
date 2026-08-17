#!/usr/bin/env node
/**
 * Harvest a single marriott overview page → parse → optional apply.
 *   node scripts/harvest-one-marriott-overview.mjs --url="https://.../overview/" --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import puppeteer from "puppeteer";
import Airtable from "airtable";
import {
  marriottOverviewUrlFromWebsite,
  parseMarriottOverviewHtml,
} from "../lib/marriott-hotel-content-fetch.js";
import { marshaFromMarriottWebsite } from "../lib/marriott-brand-directory-extract.js";
import { fetchMarriottBazaarvoiceProduct } from "../lib/marriott-bazaarvoice-content-fetch.js";
import { planMarriottCensusContentBackfill } from "../lib/hotel-census/plan-marriott-census-content-backfill.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, "..", "data", "marriott-overview-harvest");

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const eq = args.find((a) => a.startsWith(`${flag}=`));
    if (eq) return eq.slice(flag.length + 1);
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  return {
    url: get("--url"),
    apply: args.includes("--apply"),
    waitMs: Number(get("--wait-ms") || 15000),
    headless: !args.includes("--visible"),
  };
}

async function fetchOverviewHtml(url, waitMs, headless) {
  const browser = await puppeteer.launch({
    headless: headless ? "new" : false,
    args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
  });
  try {
    const page = await browser.newPage();
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, "webdriver", { get: () => false });
    });
    await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
    await new Promise((r) => setTimeout(r, waitMs));
    return { html: await page.content(), title: await page.title() };
  } finally {
    await browser.close();
  }
}

async function main() {
  const opts = parseArgs();
  const overviewUrl = marriottOverviewUrlFromWebsite(opts.url || "");
  if (!overviewUrl) {
    console.error("Provide --url with a marriott.com /hotels/.../overview/ link");
    process.exit(1);
  }

  const marsha = marshaFromMarriottWebsite(overviewUrl);
  console.log("MARSHA:", marsha);
  console.log("URL:", overviewUrl);

  mkdirSync(OUT_DIR, { recursive: true });
  let parsed = { description: "", amenities: [], amenitiesText: "", parseErrors: [] };

  console.log("\nFetching overview (puppeteer)…");
  const { html, title } = await fetchOverviewHtml(overviewUrl, opts.waitMs, opts.headless);
  writeFileSync(join(OUT_DIR, `${marsha}.html`), html);
  console.log("Page title:", title);
  parsed = parseMarriottOverviewHtml(html);

  if (!parsed.description && !parsed.amenities.length) {
    console.log("Overview blocked or empty — trying Bazaarvoice for description…");
    const bv = await fetchMarriottBazaarvoiceProduct(marsha);
    if (bv?.description) parsed.description = bv.description;
  }

  console.log("\n--- Parsed ---");
  console.log("Description:", parsed.description || "(empty)");
  console.log("Amenities:", parsed.amenities.length ? parsed.amenities.join(", ") : "(empty)");

  if (!parsed.description && !parsed.amenitiesText) {
    console.error("\nNo content extracted. Try --visible and pass Akamai in the browser window.");
    process.exit(1);
  }

  const contentRows = [
    {
      marshaCode: marsha,
      description: parsed.description,
      amenitiesText: parsed.amenitiesText,
      website: overviewUrl,
      sourceFile: join(OUT_DIR, `${marsha}.html`),
    },
  ];

  const plan = await planMarriottCensusContentBackfill({ contentRows });
  console.log("\nCensus match ready:", plan.readyToApply);
  if (plan.planRows[0]) {
    console.log("Hotel:", plan.planRows[0].censusName);
    console.log("Fields:", Object.keys(plan.planRows[0].applyFields).join(", "));
  } else if (plan.skipped[0]) {
    console.log("Skipped:", plan.skipped[0]);
  }

  if (!opts.apply) {
    console.log("\nRun with --apply to write to Hotel Census.");
    return;
  }

  if (!plan.planRows.length) {
    console.error("No census row to update.");
    process.exit(1);
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  await base(HOTEL_CENSUS_TABLE).update(
    [{ id: plan.planRows[0].censusRecordId, fields: plan.planRows[0].applyFields }],
    { typecast: true }
  );
  console.log("\nUpdated census record:", plan.planRows[0].censusRecordId);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
