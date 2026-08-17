#!/usr/bin/env node
import puppeteer from "puppeteer";
import { writeFileSync } from "node:fs";

const SLUG = "poplc-the-ocean-club-a-luxury-collection-resort-costa-norte";
const rooms = `https://www.marriott.com/en-us/hotels/${SLUG}/rooms/`;

const hits = [];
const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
const page = await browser.newPage();
page.on("response", async (res) => {
  const url = res.url();
  if (!/json|graphql|hws|property|amenit|content/i.test(url)) return;
  try {
    const text = await res.text();
    if (/amenit|overview|high-speed|description/i.test(text) && text.length < 500000) {
      hits.push({ url, status: res.status(), body: text.slice(0, 8000) });
    }
  } catch {
    /* ignore */
  }
});
await page.goto(rooms, { waitUntil: "networkidle2", timeout: 120000 });
await new Promise((r) => setTimeout(r, 4000));
writeFileSync("reports/marriott-poplc-rooms-network.json", JSON.stringify(hits, null, 2));
console.log("hits", hits.length);
for (const h of hits) console.log(h.status, h.url.slice(0, 140));
await browser.close();
