#!/usr/bin/env node
import puppeteer from "puppeteer";
import { writeFileSync } from "node:fs";
import { parseMarriottOverviewHtml } from "../lib/marriott-hotel-content-fetch.js";

const exp = "https://www.marriott.com/en-us/hotels/pujac-ac-hotel-punta-cana/experiences/";
const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
});
const page = await browser.newPage();
await page.evaluateOnNewDocument(() => {
  Object.defineProperty(navigator, "webdriver", { get: () => false });
});

await page.goto(exp, { waitUntil: "networkidle2", timeout: 120000 });
await new Promise((r) => setTimeout(r, 5000));
console.log("experiences title:", await page.title());

const overviewHref = await page.evaluate(() => {
  const links = [...document.querySelectorAll("a[href*='/overview']")];
  return links.map((a) => a.href).find((h) => /\/overview\/?$/i.test(h)) || links[0]?.href || "";
});
console.log("overview link:", overviewHref);

if (overviewHref) {
  await page.evaluate((href) => {
    window.location.href = href;
  }, overviewHref);
  await page.waitForNavigation({ waitUntil: "networkidle2", timeout: 120000 });
} else {
  await page.goto(
    "https://www.marriott.com/en-us/hotels/pujac-ac-hotel-punta-cana/overview/",
    { waitUntil: "networkidle2", timeout: 120000 }
  );
}

await new Promise((r) => setTimeout(r, 8000));
const html = await page.content();
writeFileSync("data/marriott-overview-harvest/PUJAC.html", html);
const parsed = parseMarriottOverviewHtml(html);
console.log("overview title:", await page.title());
console.log("denied", /access denied/i.test(html), "desc", parsed.description.length, "amen", parsed.amenities.length);
if (parsed.description) console.log("desc preview:", parsed.description.slice(0, 180));
if (parsed.amenities.length) console.log("amenities:", parsed.amenities.join(", "));

await browser.close();
