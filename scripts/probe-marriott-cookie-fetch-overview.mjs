#!/usr/bin/env node
import puppeteer from "puppeteer";
import { parseMarriottOverviewHtml } from "../lib/marriott-hotel-content-fetch.js";

const exp = "https://www.marriott.com/en-us/hotels/pujac-ac-hotel-punta-cana/experiences/";
const ov = "https://www.marriott.com/en-us/hotels/pujac-ac-hotel-punta-cana/overview/";

const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
const page = await browser.newPage();
await page.goto(exp, { waitUntil: "networkidle2", timeout: 120000 });
await new Promise((r) => setTimeout(r, 5000));

const cookies = await page.cookies();
const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join("; ");
console.log("cookies", cookies.length);

const res = await fetch(ov, {
  headers: {
    Cookie: cookieHeader,
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    Accept: "text/html,application/xhtml+xml",
    Referer: exp,
  },
});
const html = await res.text();
const parsed = parseMarriottOverviewHtml(html);
console.log("fetch status", res.status, "len", html.length, "denied", /access denied/i.test(html));
console.log("desc", parsed.description.length, "amen", parsed.amenities.length);
if (parsed.amenities.length) console.log(parsed.amenities.join(", "));

await browser.close();
