#!/usr/bin/env node
import puppeteer from "puppeteer";
import { load as loadCheerio } from "cheerio";
import { writeFileSync } from "node:fs";

const url = "https://www.marriott.com/en-us/hotels/pujac-ac-hotel-punta-cana/rooms/";
const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
const page = await browser.newPage();
await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
const html = await page.content();
const $ = loadCheerio(html);
$("script[type='application/ld+json']").each((_, el) => {
  const id = $(el).attr("id") || "";
  if (!/unap-schema|hotel/i.test(id)) return;
  const data = JSON.parse($(el).html() || "{}");
  writeFileSync("reports/pujac-rooms-hotel-jsonld.json", JSON.stringify(data, null, 2));
  console.log("keys", Object.keys(data));
  console.log("description", data.description?.slice?.(0, 300));
  console.log("amenityFeature", data.amenityFeature?.length);
  console.log("containsPlace", data.containsPlace?.length);
});
await browser.close();
