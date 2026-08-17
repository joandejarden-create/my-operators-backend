#!/usr/bin/env node
import { writeFileSync } from "node:fs";
import puppeteer from "puppeteer";

const url =
  "https://www.marriott.com/en-us/hotels/poplc-the-ocean-club-a-luxury-collection-resort-costa-norte/overview/";

const browser = await puppeteer.launch({
  headless: false,
  args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
});
const page = await browser.newPage();
await page.evaluateOnNewDocument(() => {
  Object.defineProperty(navigator, "webdriver", { get: () => false });
});
await page.setUserAgent(
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
);
await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
await new Promise((r) => setTimeout(r, 5000));
const html = await page.content();
const title = await page.title();
writeFileSync("reports/marriott-poplc-overview-puppeteer.html", html);
console.log("title", title, "len", html.length);
console.log("denied", /access denied/i.test(html));
console.log("overview", /15-minute drive/i.test(html));
console.log("amenities", /Free high-speed internet/i.test(html));
await browser.close();
