#!/usr/bin/env node
import puppeteer from "puppeteer";
import { parseNextDataFromHtml } from "../lib/marriott-brand-directory-extract.js";

const urls = [
  "https://www.marriott.com/en-us/hotels/pujac-ac-hotel-punta-cana/experiences/",
  "https://www.marriott.com/en-us/hotels/pujac-ac-hotel-punta-cana/rooms/",
  "https://www.marriott.com/en-us/hotels/pujac-ac-hotel-punta-cana/events/",
];

function walkFind(obj, pred, depth = 0, out = []) {
  if (!obj || depth > 18) return out;
  if (pred(obj)) out.push(obj);
  if (Array.isArray(obj)) {
    for (const x of obj) walkFind(x, pred, depth + 1, out);
  } else if (typeof obj === "object") {
    for (const v of Object.values(obj)) walkFind(v, pred, depth + 1, out);
  }
  return out;
}

const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
const page = await browser.newPage();
for (const url of urls) {
  await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
  const html = await page.content();
  const nd = parseNextDataFromHtml(html);
  const pp = nd?.props?.pageProps || {};
  const str = JSON.stringify(pp);
  console.log("\n", url.split("/").slice(-2, -1)[0], "html", html.length, "next", str.length);
  const amenityHits = walkFind(pp, (o) => Array.isArray(o?.amenities) && o.amenities.length >= 3);
  console.log("amenity array hits", amenityHits.length);
  for (const hit of amenityHits.slice(0, 2)) {
    console.log(" sample", hit.amenities.slice(0, 8));
  }
  const overviewHits = walkFind(
    pp,
    (o) => typeof o?.overview === "string" && o.overview.length > 80
  );
  console.log("overview hits", overviewHits.length, overviewHits[0]?.overview?.slice(0, 120));
}
await browser.close();
