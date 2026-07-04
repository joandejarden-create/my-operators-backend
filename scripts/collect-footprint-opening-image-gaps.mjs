/**
 * List footprint.openings property URLs missing hoteldam image mappings.
 */
import { listBrandsWithCuratedMomentum } from "./lib/choice-chi-footprint-momentum-curated.mjs";
import { resolveProfileForAirtableName } from "./lib/choice-chi-brand-resolve.mjs";
import { buildCalaOpeningsForProfile } from "./lib/choice-cala-openings-from-census.mjs";
import { resolveFootprintOpeningImageUrl } from "./lib/choice-footprint-opening-image-map.mjs";

function extractUrl(body) {
  const m = String(body || "").match(/(https:\/\/www\.choicehotels\.com\/[^\s)\]]+)/i);
  return m ? m[1].trim() : "";
}

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

const missing = [];
for (const brand of BASICS_CHI) {
  const profile = resolveProfileForAirtableName(brand).name;
  const cards = buildCalaOpeningsForProfile(profile);
  for (const c of cards) {
    const url = extractUrl(c.body);
    if (!url) continue;
    if (!resolveFootprintOpeningImageUrl(url)) {
      missing.push({ brand, title: c.title, url });
    }
  }
}

console.log(`Missing mappings: ${missing.length}`);
for (const m of missing) {
  console.log(`\n${m.brand}\n  ${m.title}\n  ${m.url}`);
}

void listBrandsWithCuratedMomentum;
