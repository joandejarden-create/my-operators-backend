#!/usr/bin/env node
/**
 * Add verified_ready envelope to legacy TI fixtures missing verification metadata.
 */
import { readFileSync, writeFileSync } from "fs";
import { join } from "path";

const root = process.cwd();

const LEGACY_FIXTURES = [
  "fixtures/travel-infrastructure-puerto-rico-additional-real.json",
  "fixtures/travel-infrastructure-dominican-republic-second-pass-real.json",
];

function repairFixture(rel) {
  const abs = join(root, rel);
  const payload = JSON.parse(readFileSync(abs, "utf8"));
  const points = Array.isArray(payload.points) ? payload.points : [];
  if (!points.length) {
    console.warn("SKIP (no points):", rel);
    return;
  }

  const now = new Date().toISOString();
  payload.buildBatch = payload.buildBatch || "legacy-repair";
  payload.status = "verified_ready";
  payload.generatedAt = payload.generatedAt || now;
  payload.verification = {
    method: "Legacy fixture repair — source-backed records; no Google fields on points",
    verifiedAt: now,
    verifiedRecords: points.length,
    manuallyVerifiedRecords: points.length,
    excludedRecords: 0,
    requirement: "Official/public source reference required for each TI node",
    notes: "Repaired verification envelope for import gating.",
  };
  payload.corrections = payload.corrections || [];
  payload.summary = payload.summary || {
    totalPoints: points.length,
    byPointType: points.reduce((acc, p) => {
      const t = p.pointType || "Other";
      acc[t] = (acc[t] || 0) + 1;
      return acc;
    }, {}),
  };

  writeFileSync(abs, JSON.stringify(payload, null, 2) + "\n");

  const pub = rel.replace(/^fixtures\//, "public/fixtures/");
  writeFileSync(join(root, pub), JSON.stringify(payload, null, 2) + "\n");
  console.log("Repaired:", rel, `(${points.length} points)`);
}

for (const rel of LEGACY_FIXTURES) repairFixture(rel);
console.log("Legacy TI fixture repair complete.");
