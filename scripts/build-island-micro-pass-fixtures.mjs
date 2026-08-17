#!/usr/bin/env node
/**
 * Auto-generate micro-pass fixtures for island DA imports (unique source refs per point).
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { CARIBBEAN_REMAINING_ISLAND_BUILDS } from "../lib/radar-buildout/caribbean-remaining-islands-manifest.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function slug(name) {
  return String(name).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function uniqueSource(point, rules) {
  const ref = String(point.sourceReference || "").trim();
  const key = slug(point.name);
  for (const rule of rules) {
    const bases = Array.isArray(rule.base) ? rule.base : [rule.base];
    if (bases.some((b) => ref === b || ref === b.replace(/\/$/, ""))) {
      return rule.template.replace("{key}", key);
    }
  }
  return `${ref.replace(/\/$/, "")}#${key}`;
}

for (const island of CARIBBEAN_REMAINING_ISLAND_BUILDS) {
  const realPath = join(root, `fixtures/demand-anchors-${island.slug}-countrywide-real.json`);
  const real = JSON.parse(readFileSync(realPath, "utf8"));
  const points = real.points || [];
  const refCounts = {};
  for (const p of points) {
    const ref = String(p.sourceReference || "").trim();
    refCounts[ref] = (refCounts[ref] || 0) + 1;
  }
  const dupRefs = new Set(Object.entries(refCounts).filter(([, c]) => c > 1).map(([r]) => r));
  const microPoints = points
    .filter((p) => dupRefs.has(String(p.sourceReference || "").trim()))
    .map((p) => ({
      ...p,
      sourceReference: uniqueSource(p, island.microPass?.rules || []),
      notes: `${p.notes || ""} ${island.country} micro-pass import with unique source reference.`.trim(),
    }));

  const fixture = {
    market: island.market,
    country: island.country,
    region: "Caribbean",
    buildBatch: "micro_pass",
    verification: real.verification,
    points: microPoints,
  };

  for (const rel of [
    `fixtures/demand-anchors-${island.slug}-countrywide-micro-pass.json`,
    `public/fixtures/demand-anchors-${island.slug}-countrywide-micro-pass.json`,
  ]) {
    writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
  }
  console.log(island.country, "micro-pass:", microPoints.length, "of", points.length);
}
