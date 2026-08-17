#!/usr/bin/env node
import puppeteer from "puppeteer";
import { load } from "cheerio";
import { writeFileSync } from "node:fs";

const pages = ["experiences", "dining", "events", "rooms"];
const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
const page = await browser.newPage();
const all = [];

for (const sub of pages) {
  const url = `https://www.marriott.com/en-us/hotels/pujac-ac-hotel-punta-cana/${sub}/`;
  await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
  const html = await page.content();
  const $ = load(html);
  $("script[type='application/ld+json']").each((_, el) => {
    const id = $(el).attr("id") || "";
    try {
      const data = JSON.parse($(el).html() || "{}");
      if (data["@type"] === "FAQPage") {
        for (const item of data.mainEntity || []) {
          all.push({
            page: sub,
            q: item.name,
            a: item.acceptedAnswer?.text,
          });
        }
      }
      if (data["@type"] === "Hotel") {
        all.push({
          page: sub,
          hotel: true,
          containsPlace: (data.containsPlace || []).map((p) => `${p["@type"]}:${p.name}`),
        });
      }
    } catch {
      /* ignore */
    }
  });
}

writeFileSync("reports/pujac-subpage-faq.json", JSON.stringify(all, null, 2));
console.log(JSON.stringify(all, null, 2));
await browser.close();
