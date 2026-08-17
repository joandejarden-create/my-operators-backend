#!/usr/bin/env node
/**
 * Micro-pass fixture for Curaçao DA batch-dedup skips (unique source refs).
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const MICRO_NAMES = new Set([
  "Handelskade Waterfront Promenade",
  "Punda Shopping and Heritage Corridor",
  "Mambo Beach Boulevard",
  "Jan Thiel Beach Resort Corridor",
  "Jan Thiel Lagoon Marina",
  "Blue Bay Beach",
  "Piscadera Bay Snorkel Area",
  "Piscadera Beach Resort Corridor",
  "Curaçao International Airport Hotel Corridor",
  "Airport to Willemstad Transit Corridor",
  "Spanish Water Lagoon",
  "Caracasbaai Bay",
  "Spanish Water Marina",
  "Playa Kalki Westpunt",
  "Grote Knip Kenepa Beach",
  "Westpunt Coastal Resort Growth Corridor",
  "Bullenbaai Port Logistics Zone",
  "Isla Refinery Industrial Corridor",
  "Curaçao Ports Authority Willemstad",
  "Mega Pier Cruise Terminal",
  "Curaçao Cruise Terminal Mathey Wharf",
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
  if (ref === "https://www.curacao.com/" || ref === "https://www.curacao.com") {
    return `https://www.curacao.com/places/${key}`;
  }
  if (ref.includes("curacao.com/en/discover")) {
    return `${ref.replace(/\/$/, "")}#${key}`;
  }
  if (ref === "https://www.curacao-ports.com/") {
    return `https://www.curacao-ports.com/#${key}`;
  }
  return `${ref.replace(/\/$/, "")}#${key}`;
}

const real = JSON.parse(
  readFileSync(join(root, "fixtures/demand-anchors-curacao-countrywide-real.json"), "utf8")
);

const points = (real.points || [])
  .filter((p) => MICRO_NAMES.has(p.name))
  .map((p) => ({
    ...p,
    sourceReference: uniqueSource(p),
    notes: `${p.notes || ""} Curaçao micro-pass import with unique source reference.`.trim(),
  }));

const fixture = {
  market: "Curaçao Countrywide",
  country: "Curaçao",
  region: "Caribbean",
  buildBatch: "micro_pass",
  verification: real.verification,
  points,
};

const out = "fixtures/demand-anchors-curacao-countrywide-micro-pass.json";
writeFileSync(join(root, out), JSON.stringify(fixture, null, 2) + "\n");
console.log("Micro-pass points:", points.length);
console.log("Written:", out);
