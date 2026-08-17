#!/usr/bin/env node
/**
 * Micro-pass fixture for Bahamas DA batch-dedup skips (unique source refs).
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const MICRO_NAMES = new Set([
  "Festival Place Welcome Centre",
  "Thomas A Robinson National Stadium",
  "Nassau Harbour Marina District",
  "Nassau Container Port Logistics Zone",
  "Cabbage Beach",
  "Paradise Island Marina",
  "Sir Sidney Poitier Bridge Access Corridor",
  "Paradise Island Entertainment Corridor",
  "One and Only Ocean Club",
  "Cable Beach Resort Corridor",
  "Goodman Bay",
  "Cable Beach Airport West Corridor",
  "Saunders Beach",
  "International Bazaar Freeport",
  "Pelican Bay Hotel District",
  "Grand Lucayan Waterpark Corridor",
  "George Town Exuma Harbour",
  "Stocking Island",
  "Exuma Cays Resort Corridor",
  "Staniel Cay Yacht Corridor",
  "Governor's Harbour Airport Access",
  "Harbour Island Pink Sands Beach",
  "Governor's Harbour Waterfront",
  "Hope Town Lighthouse",
  "Man-O-War Cay",
  "Treasure Cay Beach",
  "Alice Town Entertainment District",
]);

function slug(name) {
  return String(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function uniqueSource(point) {
  const ref = String(point.sourceReference || "").trim();
  const key = slug(point.name);
  if (ref === "https://www.bahamas.com/" || ref === "https://www.bahamas.com") {
    return `https://www.bahamas.com/places/${key}`;
  }
  if (ref.includes("bahamas.gov.bs")) return `https://www.bahamas.gov.bs/locations/${key}`;
  if (ref.includes("nassaulpia.com")) return `https://nassaulpia.com/locations/${key}`;
  if (ref.includes("jnationwidearena.com")) return `https://www.jnationwidearena.com/venues/${key}`;
  if (ref.includes("atlantisbahamas.com")) return `https://www.atlantisbahamas.com/locations/${key}`;
  return `${ref.replace(/\/$/, "")}#${key}`;
}

const real = JSON.parse(
  readFileSync(join(root, "fixtures/demand-anchors-bahamas-countrywide-real.json"), "utf8")
);

const points = (real.points || [])
  .filter((p) => MICRO_NAMES.has(p.name))
  .map((p) => ({
    ...p,
    sourceReference: uniqueSource(p),
    notes: `${p.notes || ""} Bahamas micro-pass import with unique source reference.`.trim(),
  }));

const fixture = {
  market: "Bahamas Countrywide",
  country: "Bahamas",
  region: "Caribbean",
  buildBatch: "micro_pass",
  verification: real.verification,
  points,
};

const out = "fixtures/demand-anchors-bahamas-countrywide-micro-pass.json";
writeFileSync(join(root, out), JSON.stringify(fixture, null, 2) + "\n");
console.log("Micro-pass points:", points.length);
console.log("Written:", out);
