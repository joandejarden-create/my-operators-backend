#!/usr/bin/env node
/**
 * Test overview harvest with Akamai challenge wait + warm session.
 */
import puppeteer from "puppeteer";
import { parseMarriottOverviewHtml } from "../lib/marriott-hotel-content-fetch.js";

const SLUG = process.argv[2] || "pujac-ac-hotel-punta-cana";
const MARSHA = process.argv[3] || "PUJAC";

async function fetchOverview(page, slug) {
  const exp = `https://www.marriott.com/en-us/hotels/${slug}/experiences/`;
  const ov = `https://www.marriott.com/en-us/hotels/${slug}/overview/`;

  await page.goto("https://www.marriott.com/default.mi", { waitUntil: "domcontentloaded", timeout: 120000 });
  await page.goto(exp, { waitUntil: "networkidle2", timeout: 120000 });
  await new Promise((r) => setTimeout(r, 3000));

  await page.goto(ov, { waitUntil: "domcontentloaded", timeout: 120000 });

  for (let wait = 0; wait < 6; wait++) {
    const html = await page.content();
    const parsed = parseMarriottOverviewHtml(html);
    const title = await page.title();
    console.log(`  try ${wait + 1}: title=${title.slice(0, 40)} len=${html.length} desc=${parsed.description.length} amen=${parsed.amenities.length}`);
    if (parsed.description || parsed.amenities.length) return { html, parsed, title };
    if (!/access denied|akamai|sec-if-cpt/i.test(html) && html.length > 50000) {
      return { html, parsed, title };
    }
    await new Promise((r) => setTimeout(r, 8000));
  }
  const html = await page.content();
  return { html, parsed: parseMarriottOverviewHtml(html), title: await page.title() };
}

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
});
const page = await browser.newPage();
await page.evaluateOnNewDocument(() => {
  Object.defineProperty(navigator, "webdriver", { get: () => false });
});
await page.setUserAgent(
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
);

console.log("Testing", MARSHA, SLUG);
const { parsed, title } = await fetchOverview(page, SLUG);
console.log("FINAL", title, "desc", parsed.description.length, "amen", parsed.amenities.length);
if (parsed.amenities.length) console.log(parsed.amenities.slice(0, 8).join("; "));
if (parsed.description) console.log(parsed.description.slice(0, 120));

await browser.close();
