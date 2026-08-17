#!/usr/bin/env node
import puppeteer from "puppeteer";
import { parseMarriottOverviewHtml } from "../lib/marriott-hotel-content-fetch.js";

const exp = "https://www.marriott.com/en-us/hotels/pujac-ac-hotel-punta-cana/experiences/";
const ov = "https://www.marriott.com/en-us/hotels/pujac-ac-hotel-punta-cana/overview/";

const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
const page = await browser.newPage();
await page.goto(exp, { waitUntil: "networkidle2", timeout: 120000 });

const inPageFetch = await page.evaluate(async (url) => {
  const r = await fetch(url, { credentials: "include" });
  const html = await r.text();
  return { status: r.status, html, len: html.length };
}, ov);

console.log("in-page fetch", inPageFetch.status, inPageFetch.len, /access denied/i.test(inPageFetch.html));

const parsed = parseMarriottOverviewHtml(inPageFetch.html);
console.log("desc", parsed.description.length, "amen", parsed.amenities.length);
if (parsed.description) console.log(parsed.description.slice(0, 200));
if (parsed.amenities.length) console.log(parsed.amenities.slice(0, 10).join(", "));

await browser.close();
