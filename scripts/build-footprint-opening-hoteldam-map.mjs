/**
 * Build fixtures/choice-footprint-opening-hoteldam-map.json from CALA opening URLs + hoteldam probe.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { resolveProfileForAirtableName } from "./lib/choice-chi-brand-resolve.mjs";
import { buildCalaOpeningsForProfile } from "./lib/choice-cala-openings-from-census.mjs";
import { resolveFootprintOpeningImageUrl } from "./lib/choice-footprint-opening-image-map.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.resolve(__dirname, "../fixtures/choice-footprint-opening-hoteldam-map.json");

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
  return m ? m[1].trim().replace(/[.,;]+$/, "") : "";
}

function propertyId(pageUrl) {
  const parts = String(pageUrl).replace(/\/$/, "").split("/");
  return (parts[parts.length - 1] || "").toLowerCase();
}

function candidates(pid) {
  const cc = pid.slice(0, 2);
  const P = pid.toUpperCase();
  const p = pid.toLowerCase();
  const sizes = ["1280", "2048", "480"];
  const names = [
    `${P}ExteriorTemp01_1.jpg`,
    `${P}ExteriorTemp1.jpg`,
    `${P}Exterior01_1.jpg`,
    `${P}Exterior1_1.jpg`,
    `${P}Exterior1.jpg`,
    `${P}Hexterior01_1.jpeg`,
    `${P}TerraceTemp001_1.jpg`,
    `${P}PoolCourtyard4_1.JPG`,
    `${P}AerialTemp1_1.jpg`,
    `Exterior1.JPG`,
    `${p}exterior2_1.jpg`,
    `${P}Exterior5_1.JPG`,
  ];
  const out = [];
  for (const size of sizes) {
    for (const name of names) {
      out.push(`https://www.choicehotels.com/hoteldam/${cc}/${pid}/images/${size}/${name}`);
    }
  }
  return out;
}

async function firstOk(url) {
  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: AbortSignal.timeout(12000),
    });
    if (!res.ok) return "";
    const ct = res.headers.get("content-type") || "";
    return ct.includes("image") ? url : "";
  } catch {
    return "";
  }
}

async function probe(pid) {
  for (const url of candidates(pid)) {
    const ok = await firstOk(url);
    if (ok) return ok;
  }
  return "";
}

function collectPageUrls() {
  const urls = [];
  for (const brand of BASICS_CHI) {
    const profile = resolveProfileForAirtableName(brand).name;
    for (const c of buildCalaOpeningsForProfile(profile)) {
      const url = extractUrl(c.body);
      if (url) urls.push(url);
    }
  }
  return [...new Set(urls)];
}

const existing = fs.existsSync(OUT)
  ? JSON.parse(fs.readFileSync(OUT, "utf8"))
  : {};

const pageUrls = collectPageUrls();
let probed = 0;
let added = 0;

for (const pageUrl of pageUrls) {
  if (resolveFootprintOpeningImageUrl(pageUrl) || existing[pageUrl]) continue;
  const pid = propertyId(pageUrl);
  if (!pid || pid.length < 4) continue;
  const imageUrl = await probe(pid);
  probed += 1;
  if (imageUrl) {
    existing[pageUrl] = imageUrl;
    added += 1;
    console.log(`+ ${pid}`);
  } else {
    console.log(`? ${pid} ${pageUrl}`);
  }
}

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(existing, null, 2) + "\n");
console.log(`\nWrote ${OUT}: ${Object.keys(existing).length} entries (+${added} new, probed ${probed})`);
