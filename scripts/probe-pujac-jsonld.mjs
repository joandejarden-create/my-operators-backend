#!/usr/bin/env node
import puppeteer from "puppeteer";
import { load as loadCheerio } from "cheerio";

const pages = [
  "experiences",
  "rooms",
  "dining",
  "events",
];

const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
const page = await browser.newPage();

for (const sub of pages) {
  const url = `https://www.marriott.com/en-us/hotels/pujac-ac-hotel-punta-cana/${sub}/`;
  await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
  await new Promise((r) => setTimeout(r, 3000));
  const html = await page.content();
  const $ = loadCheerio(html);
  console.log("\n===", sub, "===");
  $("script[type='application/ld+json']").each((_, el) => {
    const id = $(el).attr("id") || "(no id)";
    try {
      const data = JSON.parse($(el).html() || "{}");
      const type = data["@type"];
      console.log("schema", id, type);
      if (data.description) console.log(" description:", String(data.description).slice(0, 200));
      if (Array.isArray(data.amenityFeature)) {
        const names = data.amenityFeature
          .filter((a) => a?.value === "true" || a?.value === true)
          .map((a) => a.name)
          .filter(Boolean);
        console.log(" amenityFeature count:", names.length);
        console.log(" sample:", names.slice(0, 15).join(", "));
      }
    } catch (err) {
      console.log(" schema parse fail", id, err.message);
    }
  });
}

await browser.close();
