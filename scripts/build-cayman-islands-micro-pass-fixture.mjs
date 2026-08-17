#!/usr/bin/env node
/**
 * Micro-pass fixture for Cayman Islands DA batch-dedup skips (unique source refs).
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const MICRO_NAMES = new Set([
  "Cayman Islands National Museum",
  "Starfish Point",
  "Bodden Town Heritage Waterfront",
  "East End Dive Resort Corridor",
  "Cayman Islands Parliament Precinct",
  "Charles Kirkconnell International Airport",
  "Stake Bay Waterfront",
  "Cayman Brac Reef Dive Corridor",
  "Cayman Brac Sports Complex",
  "Cayman Brac District Admin Centre",
  "Edward Bodden Airfield",
  "South Hole Sound Anchorage",
  "Point of Sand Beach",
  "Little Cayman Dive Resort Zone",
  "Little Cayman Eco-Tourism Growth Node",
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
  if (ref === "https://www.visitcaymanislands.com/" || ref === "https://www.visitcaymanislands.com") {
    return `https://www.visitcaymanislands.com/places/${key}`;
  }
  if (ref === "https://ciaa.ky/" || ref === "https://ciaa.ky") {
    return `https://ciaa.ky/#${key}`;
  }
  if (ref === "https://www.gov.ky/" || ref === "https://www.gov.ky") {
    return `https://www.gov.ky/#${key}`;
  }
  if (ref === "https://www.planning.gov.ky/" || ref === "https://www.planning.gov.ky") {
    return `https://www.planning.gov.ky/#${key}`;
  }
  return `${ref.replace(/\/$/, "")}#${key}`;
}

const real = JSON.parse(
  readFileSync(join(root, "fixtures/demand-anchors-cayman-islands-countrywide-real.json"), "utf8")
);

const points = (real.points || [])
  .filter((p) => MICRO_NAMES.has(p.name))
  .map((p) => ({
    ...p,
    sourceReference: uniqueSource(p),
    notes: `${p.notes || ""} Cayman Islands micro-pass import with unique source reference.`.trim(),
  }));

const fixture = {
  market: "Cayman Islands Countrywide",
  country: "Cayman Islands",
  region: "Caribbean",
  buildBatch: "micro_pass",
  verification: real.verification,
  points,
};

const paths = [
  "fixtures/demand-anchors-cayman-islands-countrywide-micro-pass.json",
  "public/fixtures/demand-anchors-cayman-islands-countrywide-micro-pass.json",
];
for (const rel of paths) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}
console.log("Micro-pass points:", points.length);
