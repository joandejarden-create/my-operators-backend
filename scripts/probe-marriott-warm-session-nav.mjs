#!/usr/bin/env node
import puppeteer from "puppeteer";
import { parseMarriottOverviewHtml } from "../lib/marriott-hotel-content-fetch.js";

const SLUG = "poplc-the-ocean-club-a-luxury-collection-resort-costa-norte";
const exp = `https://www.marriott.com/en-us/hotels/${SLUG}/experiences/`;
const ov = `https://www.marriott.com/en-us/hotels/${SLUG}/overview/`;

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
});
const page = await browser.newPage();
await page.evaluateOnNewDocument(() => {
  Object.defineProperty(navigator, "webdriver", { get: () => false });
});

await page.goto("https://www.marriott.com/default.mi", { waitUntil: "networkidle2", timeout: 120000 });
await page.goto(exp, { waitUntil: "networkidle2", timeout: 120000 });
console.log("experiences title", await page.title());

await page.goto(ov, { waitUntil: "networkidle2", timeout: 120000 });
const html = await page.content();
const parsed = parseMarriottOverviewHtml(html);
console.log("overview title", await page.title());
console.log("denied", /access denied/i.test(html), "desc", parsed.description.length, "amen", parsed.amenities.length);
if (parsed.description) console.log(parsed.description.slice(0, 150));

await browser.close();
