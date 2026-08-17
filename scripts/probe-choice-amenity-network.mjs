#!/usr/bin/env node
/**
 * Probe Choice property page network + navigation paths (profile browser).
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import puppeteer from "puppeteer";
import { parseChoiceAmenitiesFromHtml } from "../lib/choice-hotel-content-fetch.js";
import { buildChoiceRegionalPageForCountry } from "../lib/choice-regional-directory-extract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROFILE_DIR = join(__dirname, "..", "data", "choice-browser-profile");
const OUT = join(__dirname, "..", "reports", "choice-amenity-network-probe.json");

const PROPERTY_ID = (process.argv[2] || "mx043").toLowerCase();
const regional = buildChoiceRegionalPageForCountry("Mexico")?.url;

mkdirSync(join(__dirname, "..", "reports"), { recursive: true });

/** @type {object[]} */
const responses = [];

const browser = await puppeteer.launch({
  headless: false,
  userDataDir: PROFILE_DIR,
  channel: "chrome",
  args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
});

const page = await browser.newPage();
await page.evaluateOnNewDocument(() => {
  Object.defineProperty(navigator, "webdriver", { get: () => false });
});

page.on("response", async (res) => {
  const url = res.url();
  if (!/choicehotels|akamai|sbsd/i.test(url)) return;
  const ct = res.headers()["content-type"] || "";
  let sample = "";
  try {
    if (/json|javascript|html|text/i.test(ct) && res.status() === 200) {
      const buf = await res.buffer();
      if (buf.length < 80000) sample = buf.toString("utf8").slice(0, 2000);
    }
  } catch {
    /* body already consumed */
  }
  responses.push({
    status: res.status(),
    url: url.slice(0, 300),
    ct,
    sampleHasAmenity: /amenity/i.test(sample),
    sampleLen: sample.length,
    sample: sample.slice(0, 500),
  });
});

console.log("1) Regional:", regional);
await page.goto(regional, { waitUntil: "networkidle2", timeout: 120000 });
await new Promise((r) => setTimeout(r, 3000));

const hotelLink = await page.evaluate((pid) => {
  const re = new RegExp(`/${pid}(?:["/?#]|$)`, "i");
  for (const a of document.querySelectorAll("a[href]")) {
    if (re.test(a.href)) return a.href;
  }
  return "";
}, PROPERTY_ID);

console.log("2) Hotel link from regional:", hotelLink || "(not found)");

let directHtml = "";
if (hotelLink) {
  await page.goto(hotelLink, { waitUntil: "networkidle2", timeout: 120000 });
  await new Promise((r) => setTimeout(r, 8000));
  directHtml = await page.content();
} else {
  const direct = `https://www.choicehotels.com/chihuahua/chihuahua/quality-inn-hotels/${PROPERTY_ID}`;
  await page.goto(direct, { waitUntil: "networkidle2", timeout: 120000 });
  await new Promise((r) => setTimeout(r, 8000));
  directHtml = await page.content();
}

const parsed = parseChoiceAmenitiesFromHtml(directHtml);
const result = {
  propertyId: PROPERTY_ID,
  hotelLink,
  htmlLength: directHtml.length,
  blocked: /access denied/i.test(directHtml),
  amenityCount: parsed.amenities.length,
  amenities: parsed.amenities.slice(0, 15),
  interestingResponses: responses.filter(
    (r) => r.sampleHasAmenity || /hotel|property|amenit|content/i.test(r.url)
  ),
  allResponseCount: responses.length,
};

writeFileSync(OUT, JSON.stringify(result, null, 2));
console.log(JSON.stringify(result, null, 2));
await browser.close();
