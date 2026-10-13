#!/usr/bin/env node
/**
 * Gate: Full ADP vs Lite Leak Audit comparison.
 * npm run test:adp-leak-audit-full-vs-lite-comparison-v1
 */
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import {
  runFullVsLiteComparison,
  assertComparisonBundleShape,
  COMPARISON_REPORT_DIR,
  COMPARISON_HOTELS,
} from "../lib/ai-demand-positioning/leak-audit/full-vs-lite-comparison-v1.js";

const root = process.cwd();
const outDir = join(root, COMPARISON_REPORT_DIR);
const result = runFullVsLiteComparison({ repoRoot: root, outDir });

assert.equal(result.ok, true);
assert.equal(result.productionAdpWrites, 0);
assert.equal(result.fingerprintsUnchanged, true);
assert.equal(result.isolationOk, true);
assert.ok(result.hotelsCompared >= 3);

const shape = assertComparisonBundleShape(result);
assert.equal(shape.ok, true, shape.failures.join(","));

const requiredFiles = [
  "README.md",
  "comparison-summary.md",
  "comparison-results.json",
  "cambridge-beaches-comparison.md",
  "now-now-noho-comparison.md",
  "hotel-phillips-comparison.md",
];
for (const f of requiredFiles) {
  assert.equal(existsSync(join(outDir, f)), true, `missing ${f}`);
}

const summary = readFileSync(join(outDir, "comparison-summary.md"), "utf8");
assert.match(summary, /Recommended default free-audit scope/);
assert.match(summary, /Provider failure rule/);
assert.match(summary, /Safe for free prospect use/);

const json = JSON.parse(readFileSync(join(outDir, "comparison-results.json"), "utf8"));
assert.equal(json.hotels.length, COMPARISON_HOTELS.length);

for (const hotel of json.hotels) {
  assert.ok(hotel.reliability.directionalReliabilityScore >= 0);
  assert.ok(hotel.reliability.directionalReliabilityScore <= 100);
  assert.ok(["Green", "Yellow", "Orange", "Red"].includes(hotel.reliability.band));
  assert.ok(hotel.full.aiConsideration != null || hotel.full.aiConsideration === 0);
  assert.ok(hotel.lite.scenariosMonitored);
  assert.ok(Array.isArray(hotel.lite.competitors));
  assert.ok(hotel.lite.evidence);
  assert.ok(Array.isArray(hotel.lite.actions));
  assert.ok(hotel.scopeComparison.liteV1.scope === "15 × 4");
  assert.ok(hotel.scopeComparison.liteV15.scope === "20 × 4");
  assert.ok(hotel.providerFailure.scope === "15 × 3");
  // Client-facing hotel objects must not embed production property IDs
  const blob = JSON.stringify(hotel);
  assert.equal(/\badp_[a-z0-9_]+/i.test(blob), false, `property id leaked in ${hotel.slug}`);
  assert.equal(/fullPromptText|_internalFullPromptText/i.test(blob), false);
}

for (const name of [
  "cambridge-beaches-comparison.md",
  "now-now-noho-comparison.md",
  "hotel-phillips-comparison.md",
]) {
  const md = readFileSync(join(outDir, name), "utf8");
  assert.match(md, /## Verdict/);
  assert.match(md, /directional|Score:/i);
  assert.match(md, /Side-by-Side Metrics/);
  assert.match(md, /Competitor Comparison/);
  assert.match(md, /Evidence Comparison/);
  assert.match(md, /Action Item Comparison/);
  assert.equal(/\badp_[a-z0-9_]+/i.test(md), false);
  assert.equal(/\brec[a-z0-9]{14}\b/i.test(md), false);
  assert.equal(/INTERNAL —/i.test(md), false);
}

// Published ADP still readable
for (const meta of COMPARISON_HOTELS) {
  const fixtureDir = join(
    root,
    "fixtures/ai-demand-positioning/published",
    meta.propertyId
  );
  assert.equal(existsSync(fixtureDir), true, `missing fixture dir ${meta.propertyId}`);
}

console.log(
  JSON.stringify(
    {
      pass: true,
      gate: "ADP_LEAK_AUDIT_FULL_VS_LITE_COMPARISON_V1",
      hotelsCompared: result.hotelsCompared,
      averageScore: result.averageScore,
      portfolioSafeForProspectUse: result.portfolioSafeForProspectUse,
      recommendedDefaultScope: result.recommendedDefaultScope,
      fingerprintsUnchanged: true,
      productionAdpWrites: 0,
    },
    null,
    2
  )
);
