#!/usr/bin/env node
import puppeteer from "puppeteer";
import { homedir } from "node:os";
import { join } from "node:path";
import { parseMarriottOverviewHtml } from "../lib/marriott-hotel-content-fetch.js";

const url =
  "https://www.marriott.com/en-us/hotels/poplc-the-ocean-club-a-luxury-collection-resort-costa-norte/overview/";

const userDataDir = join(
  homedir(),
  "AppData",
  "Local",
  "Google",
  "Chrome",
  "User Data"
);

const attempts = [
  { label: "default puppeteer", opts: { headless: "new" } },
  {
    label: "chrome channel",
    opts: { headless: "new", channel: "chrome" },
  },
];

for (const { label, opts } of attempts) {
  console.log("\n===", label, "===");
  let browser;
  try {
    browser = await puppeteer.launch({
      ...opts,
      args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
    });
    const page = await browser.newPage();
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, "webdriver", { get: () => false });
    });
    await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
    await new Promise((r) => setTimeout(r, 4000));
    const html = await page.content();
    const parsed = parseMarriottOverviewHtml(html);
    console.log("title", await page.title());
    console.log("denied", /access denied/i.test(html), "desc", parsed.description.length, "amen", parsed.amenities.length);
    if (parsed.description) console.log("desc preview", parsed.description.slice(0, 120));
  } catch (e) {
    console.log("ERR", e.message);
  } finally {
    if (browser) await browser.close();
  }
}

// Legacy .mi endpoint
console.log("\n=== hotelinformation POPLC.mi ===");
const r = await fetch("https://www.marriott.com/hotelinformation/POPLC.mi", {
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    Accept: "text/html",
  },
  redirect: "follow",
});
const html = await r.text();
const parsed = parseMarriottOverviewHtml(html);
console.log("status", r.status, "len", html.length, "desc", parsed.description.length);
for (const pat of ["shortDescription", "hotelDescription", "overviewText", "15-minute"]) {
  console.log(pat, html.includes(pat));
}
