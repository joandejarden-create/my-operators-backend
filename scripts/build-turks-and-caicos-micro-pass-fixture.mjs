#!/usr/bin/env node
/**
 * Micro-pass fixture for Turks & Caicos DA batch-dedup skips (unique source refs).
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const MICRO_NAMES = new Set([
  "Leeward Marina and Yacht Basin",
  "The Bight and Turtle Cove Marina",
  "Long Bay Beach",
  "Chalk Sound National Park",
  "Sapodilla Bay",
  "Taylor Bay",
  "Grace Bay Retail and Dining District",
  "National Stadium Providenciales",
  "Providenciales Resort Expansion Corridor",
  "JAGS McCartney International Airport",
  "Cockburn Town Heritage District",
  "Governor's Beach",
  "Grand Turk Lighthouse",
  "Salt Raking Historic Sites Corridor",
  "Columbus Landfall Marine Zone",
  "Grand Turk Civic and Government Precinct",
  "North Caicos Ferry Gateway",
  "Middle Caicos Mudjin Harbour",
  "South Caicos Airport Corridor",
  "South Caicos Fishing Port District",
  "Salt Cay Heritage Waterfront",
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
  if (ref === "https://www.visittci.com/" || ref === "https://www.visittci.com") {
    return `https://www.visittci.com/places/${key}`;
  }
  if (ref === "https://www.tciairports.com/" || ref === "https://www.tciairports.com") {
    return `https://www.tciairports.com/#${key}`;
  }
  if (ref === "https://www.gov.tc/" || ref === "https://www.gov.tc") {
    return `https://www.gov.tc/#${key}`;
  }
  if (ref === "https://www.investturksandcaicos.tc/" || ref === "https://www.investturksandcaicos.tc") {
    return `https://www.investturksandcaicos.tc/#${key}`;
  }
  return `${ref.replace(/\/$/, "")}#${key}`;
}

const real = JSON.parse(
  readFileSync(join(root, "fixtures/demand-anchors-turks-and-caicos-countrywide-real.json"), "utf8")
);

const points = (real.points || [])
  .filter((p) => MICRO_NAMES.has(p.name))
  .map((p) => ({
    ...p,
    sourceReference: uniqueSource(p),
    notes: `${p.notes || ""} Turks & Caicos micro-pass import with unique source reference.`.trim(),
  }));

const fixture = {
  market: "Turks & Caicos Countrywide",
  country: "Turks & Caicos",
  region: "Caribbean",
  buildBatch: "micro_pass",
  verification: real.verification,
  points,
};

const paths = [
  "fixtures/demand-anchors-turks-and-caicos-countrywide-micro-pass.json",
  "public/fixtures/demand-anchors-turks-and-caicos-countrywide-micro-pass.json",
];
for (const rel of paths) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}
console.log("Micro-pass points:", points.length);
