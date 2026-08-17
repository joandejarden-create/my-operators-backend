#!/usr/bin/env node
/**
 * Micro-pass fixture for Saint Kitts and Nevis DA batch-dedup skips (unique source refs).
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const MICRO_NAMES = new Set([]);

function slug(name) {
  return String(name).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function uniqueSource(point) {
  const ref = String(point.sourceReference || "").trim();
  const key = slug(point.name);
  const rules = [
    {
        "base": [
            "https://www.stkittstourism.kn/",
            "https://www.stkittstourism.kn"
        ],
        "template": "https://www.stkittstourism.kn/places/{key}"
    }
];
  for (const rule of rules) {
    const bases = Array.isArray(rule.base) ? rule.base : [rule.base];
    if (bases.some((b) => ref === b || ref === b.replace(/\/$/, ""))) {
      return rule.template.replace("{key}", key);
    }
  }
  return `${ref.replace(/\/$/, "")}#${key}`;
}

const real = JSON.parse(
  readFileSync(join(root, "fixtures/demand-anchors-saint-kitts-and-nevis-countrywide-real.json"), "utf8")
);

const points = (real.points || [])
  .filter((p) => MICRO_NAMES.has(p.name))
  .map((p) => ({
    ...p,
    sourceReference: uniqueSource(p),
    notes: `${p.notes || ""} Saint Kitts and Nevis micro-pass import with unique source reference.`.trim(),
  }));

const fixture = {
  market: "Saint Kitts and Nevis Countrywide",
  country: "Saint Kitts and Nevis",
  region: "Caribbean",
  buildBatch: "micro_pass",
  verification: real.verification,
  points,
};

for (const rel of [
  "fixtures/demand-anchors-saint-kitts-and-nevis-countrywide-micro-pass.json",
  "public/fixtures/demand-anchors-saint-kitts-and-nevis-countrywide-micro-pass.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}
console.log("Micro-pass points:", points.length);
