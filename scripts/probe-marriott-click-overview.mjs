#!/usr/bin/env node
import puppeteer from "puppeteer";
import { parseMarriottOverviewHtml } from "../lib/marriott-hotel-content-fetch.js";

const exp = "https://www.marriott.com/en-us/hotels/poplc-the-ocean-club-a-luxury-collection-resort-costa-norte/experiences/";

const browser = await puppeteer.launch({
  headless: false,
  args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
});
const page = await browser.newPage();
await page.evaluateOnNewDocument(() => {
  Object.defineProperty(navigator, "webdriver", { get: () => false });
});

await page.goto(exp, { waitUntil: "networkidle2", timeout: 120000 });
console.log("on experiences", await page.title());

const overviewHref = await page.$eval(
  'a[href*="/overview/"]',
  (a) => a.href
).catch(() => null);
console.log("overview link", overviewHref);

if (overviewHref) {
  await Promise.all([
    page.waitForNavigation({ waitUntil: "networkidle2", timeout: 120000 }),
    page.click('a[href*="/overview/"]'),
  ]).catch((e) => console.log("nav err", e.message));
  await new Promise((r) => setTimeout(r, 5000));
  const html = await page.content();
  const parsed = parseMarriottOverviewHtml(html);
  console.log("after click title", await page.title());
  console.log("denied", /access denied/i.test(html), "desc", parsed.description.length, "amen", parsed.amenities.length);
  if (parsed.description) console.log(parsed.description.slice(0, 150));
}

await browser.close();
