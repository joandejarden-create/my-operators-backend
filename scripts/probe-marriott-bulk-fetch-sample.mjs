#!/usr/bin/env node
import puppeteer from "puppeteer";
import { parseMarriottOverviewHtml } from "../lib/marriott-hotel-content-fetch.js";

const samples = [
  "https://www.marriott.com/en-us/hotels/sdqjw-jw-marriott-hotel-santo-domingo/overview/",
  "https://www.marriott.com/en-us/hotels/midcy-courtyard-merida-downtown/overview/",
  "https://www.marriott.com/en-us/hotels/poplc-the-ocean-club-a-luxury-collection-resort-costa-norte/overview/",
];

const browser = await puppeteer.launch({
  headless: "new",
  args: [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
  ],
});

const page = await browser.newPage();
await page.evaluateOnNewDocument(() => {
  Object.defineProperty(navigator, "webdriver", { get: () => false });
});
await page.setUserAgent(
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
);

for (const url of samples) {
  await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
  await new Promise((r) => setTimeout(r, 3000));
  const html = await page.content();
  const parsed = parseMarriottOverviewHtml(html);
  console.log(
    url.split("/hotels/")[1]?.slice(0, 40),
    "denied",
    /access denied/i.test(html),
    "desc",
    parsed.description.length,
    "amenities",
    parsed.amenities.length
  );
}

await browser.close();
