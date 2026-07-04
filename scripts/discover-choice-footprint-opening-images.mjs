/**
 * Discover hoteldam hero URLs for footprint.openings property pages (probe + optional Puppeteer).
 *
 *   node scripts/discover-choice-footprint-opening-images.mjs
 *   node scripts/discover-choice-footprint-opening-images.mjs --puppeteer
 */
import puppeteer from "puppeteer";
import { resolveProfileForAirtableName } from "./lib/choice-chi-brand-resolve.mjs";
import { buildCalaOpeningsForProfile } from "./lib/choice-cala-openings-from-census.mjs";
import { resolveFootprintOpeningImageUrl } from "./lib/choice-footprint-opening-image-map.mjs";

const BASICS_CHI = [
  "Ascend Hotel Collection",
  "Cambria Hotels",
  "Clarion",
  "Clarion Pointe",
  "Comfort Inn & Suites",
  "Country Inn & Suites by Radisson",
  "Econo Lodge",
  "Everhome Suites",
  "MainStay Suites",
  "Park Inn by Choice",
  "Park Plaza by Choice",
  "Quality Inn",
  "Radisson Blu by Choice",
  "Radisson by Choice",
  "Radisson Collection by Choice",
  "Radisson Individuals by Choice",
  "Radisson Inn & Suites",
  "Radisson RED by Choice",
  "Rodeway Inn",
  "Sleep Inn",
  "Suburban Studios",
  "WoodSpring Suites",
];

function extractUrl(body) {
  const m = String(body || "").match(/(https:\/\/www\.choicehotels\.com\/[^\s)\]]+)/i);
  return m ? m[1].trim() : "";
}

function propertyId(pageUrl) {
  const parts = String(pageUrl).replace(/\/$/, "").split("/");
  return (parts[parts.length - 1] || "").toLowerCase();
}

function probeCandidates(pid) {
  const cc = pid.slice(0, 2);
  const P = pid.toUpperCase();
  const sizes = ["1280", "2048", "480"];
  const names = [
    `${P}ExteriorTemp01_1.jpg`,
    `${P}ExteriorTemp1.jpg`,
    `${P}Exterior01_1.jpg`,
    `${P}Exterior1_1.jpg`,
    `${P}Exterior1.jpg`,
    `Exterior1.JPG`,
    `${pid}exterior2_1.jpg`,
    `${P}Hexterior01_1.jpeg`,
    `${P}AerialTemp1_1.jpg`,
  ];
  const exts = [".jpg", ".jpeg", ".JPG", ".JPEG"];
  const out = [];
  for (const size of sizes) {
    for (const name of names) {
      const base = `https://www.choicehotels.com/hoteldam/${cc}/${pid}/images/${size}/${name}`;
      out.push(base);
      if (!/\.(jpe?g|png)$/i.test(name)) {
        for (const e of exts) {
          if (!name.endsWith(e)) out.push(base.replace(/\.[^.]+$/, e));
        }
      }
    }
  }
  return [...new Set(out)];
}

async function headOk(url) {
  try {
    const res = await fetch(url, { method: "HEAD", redirect: "follow" });
    if (res.ok) return true;
    if (res.status === 405 || res.status === 403) {
      const get = await fetch(url, { method: "GET", redirect: "follow" });
      return get.ok && (get.headers.get("content-type") || "").includes("image");
    }
  } catch {
    /* ignore */
  }
  return false;
}

async function probeImage(pageUrl) {
  const pid = propertyId(pageUrl);
  for (const candidate of probeCandidates(pid)) {
    if (await headOk(candidate)) return candidate;
  }
  return "";
}

async function scrapeImage(page, pageUrl) {
  await page.goto(pageUrl, { waitUntil: "networkidle2", timeout: 90000 });
  await new Promise((r) => setTimeout(r, 3500));
  const html = await page.content();
  const found = [
    ...html.matchAll(
      /https:\/\/www\.choicehotels\.com\/hoteldam\/[^"'\\s]+?\.(?:jpg|jpeg|png|webp)/gi
    ),
  ].map((m) => m[0]);
  const ext = found.filter((u) => /exterior|aerial|hero|facade/i.test(u));
  const pick = ext[0] || found.find((u) => /1280|2048/.test(u)) || found[0];
  return pick || "";
}

function collectGaps() {
  const gaps = [];
  for (const brand of BASICS_CHI) {
    const profile = resolveProfileForAirtableName(brand).name;
    const cards = buildCalaOpeningsForProfile(profile);
    for (const c of cards) {
      const url = extractUrl(c.body);
      if (!url || resolveFootprintOpeningImageUrl(url)) continue;
      gaps.push({ brand, title: c.title, url });
    }
  }
  return gaps;
}

async function main() {
  const usePuppeteer = process.argv.includes("--puppeteer");
  const gaps = collectGaps();
  console.log(`Gaps: ${gaps.length}`);

  let browser;
  let page;
  if (usePuppeteer) {
    browser = await puppeteer.launch({ headless: "new" });
    page = await browser.newPage();
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
    );
  }

  const discovered = {};
  for (const g of gaps) {
    let imageUrl = await probeImage(g.url);
    if (!imageUrl && page) {
      try {
        imageUrl = await scrapeImage(page, g.url);
      } catch (err) {
        console.log(`scrape fail ${g.url}: ${err.message}`);
      }
    }
    if (imageUrl) {
      discovered[g.url] = imageUrl;
      console.log(`OK ${propertyId(g.url)} -> ${imageUrl}`);
    } else {
      console.log(`MISS ${g.url}`);
    }
  }

  if (browser) await browser.close();

  console.log("\n// Paste into EXTRA_BY_PROPERTY_PAGE or case-study-image-url-map:\n");
  console.log(JSON.stringify(discovered, null, 2));
  console.log(`\nResolved ${Object.keys(discovered).length}/${gaps.length}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
