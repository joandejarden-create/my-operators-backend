#!/usr/bin/env node
import puppeteer from "puppeteer";
import { writeFileSync } from "node:fs";

const SLUG = "poplc-the-ocean-club-a-luxury-collection-resort-costa-norte";
const exp = `https://www.marriott.com/en-us/hotels/${SLUG}/experiences/`;

const hits = [];
const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
const page = await browser.newPage();

page.on("response", async (res) => {
  const url = res.url();
  const ct = res.headers()["content-type"] || "";
  if (!/json|graphql|text\/plain/i.test(ct) && !/query|graph|api|hws|property/i.test(url)) return;
  try {
    const text = await res.text();
    if (/15-minute|Puerto Plata|Free high-speed|overview|amenit/i.test(text)) {
      hits.push({ url, status: res.status(), preview: text.slice(0, 400) });
    }
  } catch {
    /* ignore */
  }
});

await page.goto(exp, { waitUntil: "networkidle2", timeout: 120000 });
await new Promise((r) => setTimeout(r, 5000));
console.log("hits", hits.length);
for (const h of hits.slice(0, 15)) {
  console.log("\n", h.status, h.url.slice(0, 120));
  console.log(h.preview.replace(/\s+/g, " "));
}
writeFileSync("reports/marriott-poplc-network-hits.json", JSON.stringify(hits, null, 2));
await browser.close();
