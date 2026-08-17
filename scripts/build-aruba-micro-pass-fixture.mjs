#!/usr/bin/env node
/**
 * Micro-pass fixture for Aruba DA batch-dedup skips (unique source refs).
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const MICRO_NAMES = new Set([
  "Palm Beach Casino Entertainment Corridor",
  "Divi Phoenix Palm Beach Zone",
  "Holiday Inn Resort Aruba Beach Zone",
  "Palm Beach Water Sports Corridor",
  "Eagle Beach Low-Rise Resort Corridor",
  "Manchebo Beach",
  "Amsterdam Manor Beach Zone",
  "Eagle Beach Public Beach Access",
  "Eagle Beach Events and Festival Zone",
  "Wilhelmina Park",
  "Parliament of Aruba",
  "Noord Commercial Corridor",
  "Boca Catalina Snorkel Beach",
  "Noord Hotel Growth Corridor",
  "San Nicolas Art and Culture District",
  "Baby Beach",
  "Rodgers Beach",
  "Seroe Colorado Growth Corridor",
  "Arashi Beach",
  "Malmok Beach",
  "Arashi Reef Snorkel Corridor",
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
  if (ref === "https://www.aruba.com/" || ref === "https://www.aruba.com") {
    return `https://www.aruba.com/places/${key}`;
  }
  if (ref.includes("aruba.com/us/explore")) {
    return `${ref.replace(/\/$/, "")}#${key}`;
  }
  return `${ref.replace(/\/$/, "")}#${key}`;
}

const real = JSON.parse(
  readFileSync(join(root, "fixtures/demand-anchors-aruba-countrywide-real.json"), "utf8")
);

const points = (real.points || [])
  .filter((p) => MICRO_NAMES.has(p.name))
  .map((p) => ({
    ...p,
    sourceReference: uniqueSource(p),
    notes: `${p.notes || ""} Aruba micro-pass import with unique source reference.`.trim(),
  }));

const fixture = {
  market: "Aruba Countrywide",
  country: "Aruba",
  region: "Caribbean",
  buildBatch: "micro_pass",
  verification: real.verification,
  points,
};

const out = "fixtures/demand-anchors-aruba-countrywide-micro-pass.json";
writeFileSync(join(root, out), JSON.stringify(fixture, null, 2) + "\n");
console.log("Micro-pass points:", points.length);
console.log("Written:", out);
