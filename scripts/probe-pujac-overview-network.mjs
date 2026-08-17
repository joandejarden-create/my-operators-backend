#!/usr/bin/env node
import puppeteer from "puppeteer";
import { writeFileSync } from "node:fs";

const ov = "https://www.marriott.com/en-us/hotels/pujac-ac-hotel-punta-cana/overview/";
const hits = [];
const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
const page = await browser.newPage();
page.on("response", async (res) => {
  const url = res.url();
  if (!/json|graphql|hws|content|dam|overview|amenit/i.test(url)) return;
  try {
    const text = await res.text();
    if (text.length > 100 && text.length < 600000 && /amenit|overview|Connect to the city|high-speed|PUJAC/i.test(text)) {
      hits.push({ url, status: res.status(), body: text.slice(0, 15000) });
    }
  } catch {
    /* ignore */
  }
});
await page.goto(ov, { waitUntil: "networkidle2", timeout: 120000 });
await new Promise((r) => setTimeout(r, 8000));
writeFileSync("reports/pujac-overview-network.json", JSON.stringify(hits, null, 2));
console.log("title", await page.title(), "hits", hits.length);
for (const h of hits) console.log(h.status, h.url.slice(0, 160));
await browser.close();
