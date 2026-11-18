#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getCanonicalFourteenSegmentIds } from "../lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-traversal-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-fourteen-segment-discovery-commercial-benchmark-v1",
);

const REQUIRED = [
  "FOUNDER_EXECUTIVE_SUMMARY.md",
  "FOURTEEN_SEGMENT_SCORECARD.md",
  "DISCOVERY_GAPS_AND_IMPROVEMENTS.md",
  "CONTACT_AND_COMMERCIAL_QUALIFICATION.md",
  "OPPORTUNITY_DRAWER_QUALITY.md",
  "EXA_PAID_BENCHMARK_AUTHORIZATION.md",
  "RESIDUAL_SOURCE_SUMMARY_DRIFT.md",
  "TEST_RESULTS.md",
  "FOUNDER_NEXT_DECISIONS.md",
  "BENCHMARK_ACCEPTANCE.json",
  "PRODUCTION_BASELINE.json",
  "SEGMENT_SCORECARD.json",
];

for (const name of REQUIRED) {
  assert.ok(fs.existsSync(path.join(OUT, name)), `missing ${name}`);
}

const acceptance = JSON.parse(
  fs.readFileSync(path.join(OUT, "BENCHMARK_ACCEPTANCE.json"), "utf8"),
);
const baseline = JSON.parse(
  fs.readFileSync(path.join(OUT, "PRODUCTION_BASELINE.json"), "utf8"),
);
const scorecard = JSON.parse(
  fs.readFileSync(path.join(OUT, "SEGMENT_SCORECARD.json"), "utf8"),
);

assert.equal(getCanonicalFourteenSegmentIds().length, 14);
assert.equal((scorecard.scorecard || []).length, 14);
assert.equal(acceptance.segments, 14);
assert.equal(acceptance.missionsOk, true);
assert.equal(acceptance.productionWrites, 0);
assert.equal(acceptance.paidSpendUsd, 0);
assert.equal(acceptance.pinnacleOk, true);
assert.equal(acceptance.pdaOk, true);
assert.ok(baseline.bethesdaLiveCount >= 87);
assert.ok(baseline.portfolioLiveCount >= 687);
assert.equal(baseline.productionWritesThisRun, 0);

console.log(
  JSON.stringify(
    {
      ok: true,
      bethesda: baseline.bethesdaLiveCount,
      portfolio: baseline.portfolioLiveCount,
      segments: 14,
      productionWrites: 0,
      paidSpendUsd: 0,
    },
    null,
    2,
  ),
);
