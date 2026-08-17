#!/usr/bin/env node
/**
 * Micro-pass fixture for Jamaica DA batch-dedup skips (unique source refs).
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const MICRO_NAMES = new Set([
  "Montego Bay Free Zone",
  "White River Valley",
  "St. Ann's Bay Hospital",
  "Negril Cliffs",
  "Long Bay Beach",
  "Negril Town Centre",
  "National Stadium",
  "Kingston Freezone",
  "Downtown Kingston Business District",
  "Victoria Pier",
  "Port of Falmouth",
  "Falmouth Town Centre",
  "Luminous Lagoon",
  "Falmouth Growth Corridor",
  "Blue Lagoon",
  "Navy Island",
  "Boston Jerk Centre",
  "Black River Safari",
  "Lovers Leap",
  "Mandeville Business Corridor",
  "Bamboo Avenue",
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
  if (ref === "https://www.visitjamaica.com/" || ref === "https://www.visitjamaica.com") {
    return `https://www.visitjamaica.com/places/${key}`;
  }
  if (ref.includes("moh.gov.jm")) return `https://www.moh.gov.jm/hospitals/${key}`;
  if (ref.includes("portjam.com")) return `https://www.portjam.com/locations/${key}`;
  if (ref.includes("jamaicafreezones.com")) return `https://www.jamaicafreezones.com/zones/${key}`;
  if (ref.includes("jnationwidearena.com")) return `https://www.jnationwidearena.com/venues/${key}`;
  return `${ref.replace(/\/$/, "")}#${key}`;
}

const real = JSON.parse(
  readFileSync(join(root, "fixtures/demand-anchors-jamaica-countrywide-real.json"), "utf8")
);

const points = (real.points || [])
  .filter((p) => MICRO_NAMES.has(p.name))
  .map((p) => ({
    ...p,
    sourceReference: uniqueSource(p),
    notes: `${p.notes || ""} Jamaica micro-pass import with unique source reference.`.trim(),
  }));

const fixture = {
  market: "Jamaica Countrywide",
  country: "Jamaica",
  region: "Caribbean",
  buildBatch: "micro_pass",
  verification: real.verification,
  points,
};

const out = "fixtures/demand-anchors-jamaica-countrywide-micro-pass.json";
writeFileSync(join(root, out), JSON.stringify(fixture, null, 2) + "\n");
console.log("Micro-pass points:", points.length);
console.log("Written:", out);
