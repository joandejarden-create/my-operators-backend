#!/usr/bin/env node
/**
 * Micro-pass fixture for Barbados DA batch-dedup skips (unique source refs).
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const MICRO_NAMES = new Set([
  "National Heroes Square",
  "Pelican Craft Centre",
  "Cheapside Market District",
  "Bridgetown Waterfront Boardwalk",
  "Holetown Heritage District",
  "Speightstown Waterfront",
  "Mullins Beach",
  "Paynes Bay Resort Strip",
  "St Lawrence Gap Entertainment District",
  "Dover Beach",
  "Rockley Beach",
  "Worthing Main Road Commercial Corridor",
  "Hastings Boardwalk",
  "Graeme Hall Nature Sanctuary Area",
  "Oistins Fish Market and Bay Gardens",
  "South Point Lighthouse Area",
  "Maxwell Beach Resort Corridor",
  "South Coast Redevelopment Growth Node",
  "Airport Access Commercial Zone",
  "Barbados Cruise Homeport Logistics",
  "Flower Forest Botanical Gardens",
  "Bathsheba Surf and East Coast Lookout",
  "Crane Beach Resort Area",
  "Scotland District Nature Corridor",
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
  if (ref === "https://www.visitbarbados.org/" || ref === "https://www.visitbarbados.org") {
    return `https://www.visitbarbados.org/places/${key}`;
  }
  if (ref === "https://www.investbarbados.org/" || ref === "https://www.investbarbados.org") {
    return `https://www.investbarbados.org/#${key}`;
  }
  return `${ref.replace(/\/$/, "")}#${key}`;
}

const real = JSON.parse(
  readFileSync(join(root, "fixtures/demand-anchors-barbados-countrywide-real.json"), "utf8")
);

const points = (real.points || [])
  .filter((p) => MICRO_NAMES.has(p.name))
  .map((p) => ({
    ...p,
    sourceReference: uniqueSource(p),
    notes: `${p.notes || ""} Barbados micro-pass import with unique source reference.`.trim(),
  }));

const fixture = {
  market: "Barbados Countrywide",
  country: "Barbados",
  region: "Caribbean",
  buildBatch: "micro_pass",
  verification: real.verification,
  points,
};

const paths = [
  "fixtures/demand-anchors-barbados-countrywide-micro-pass.json",
  "public/fixtures/demand-anchors-barbados-countrywide-micro-pass.json",
];
for (const rel of paths) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}
console.log("Micro-pass points:", points.length);
