#!/usr/bin/env node
/**
 * test:adp-toronto-forensic-reconciliation-v1
 * Reads forensic summary artifact produced by adp-toronto-measurement-forensic-v1.
 */
import assert from "assert";
import { existsSync, readFileSync } from "fs";
import { join } from "path";
import { spawnSync } from "child_process";

const ROOT = process.cwd();
const summaryPath = join(
  ROOT,
  "reports/adp/radisson-blu-toronto-measurement-forensic-v1/_forensic-summary.json"
);

if (!existsSync(summaryPath)) {
  const run = spawnSync(process.execPath, ["scripts/adp-toronto-measurement-forensic-v1.mjs"], {
    cwd: ROOT,
    encoding: "utf8",
  });
  assert.equal(run.status, 0, run.stderr || run.stdout);
}

const summary = JSON.parse(readFileSync(summaryPath, "utf8"));
assert.equal(summary.ok, true);
assert.equal(summary.observationIntegrity.observations, 252);
assert.equal(summary.observationIntegrity.uniquePairs, 252);
assert.equal(summary.observationIntegrity.duplicates, 0);
assert.equal(summary.observationIntegrity.missingPairs, 0);
assert.equal(summary.observationIntegrity.real, 252);
assert.equal(summary.observationIntegrity.synthetic, 0);
assert.equal(summary.observationIntegrity.crossHotel, 0);

assert.equal(summary.metrics.consideration.num, 73);
assert.equal(summary.metrics.consideration.den, 220);
assert.equal(summary.metrics.consideration.recomputed, 33.2);
assert.equal(summary.metrics.scenarioPresence.recomputed, 38.1);
assert.equal(summary.metrics.numberOne.recomputed, 44.7);
assert.equal(summary.metrics.top3.recomputed, 85.1);
assert.equal(summary.metrics.realityCoverage.recomputed, 19);
assert.equal(summary.metrics.realityGap.recomputed, 66.7);
assert.deepEqual(summary.calculationMismatch, []);
assert.equal(summary.businessMissing.total, 16);
assert.equal(summary.businessMissing.fromCore, true);
assert.equal(summary.periodFamily.loyaltyIsolated, true);

const required = [
  "PIPELINE_TRACE.md",
  "OBSERVATION_MATRIX.csv",
  "OBSERVATION_INTEGRITY.csv",
  "PROPERTY_EVIDENCE_REGISTER.csv",
  "CORE_METRIC_RECONCILIATION.csv",
  "TERRITORY_RECONCILIATION.csv",
  "PROVIDER_RECONCILIATION.csv",
  "REALITY_GAP_RECONCILIATION.csv",
  "DISPLACEMENT_RECONCILIATION.csv",
  "CITATION_RECONCILIATION.csv",
  "LOYALTY_CORE_ISOLATION.md",
  "REPORT_PERIOD_INTEGRITY.csv",
  "DRAWER_PERIOD_INTEGRITY.csv",
  "CONTROL_HOTEL_COMPARISON.csv",
  "BROWSER_QA.md",
  "REGRESSION_QA.md",
  "CALCULATION_MISMATCH.md",
  "FOUNDER_REPORT.md",
];
for (const f of required) {
  assert.ok(
    existsSync(join(ROOT, "reports/adp/radisson-blu-toronto-measurement-forensic-v1", f)),
    `missing ${f}`
  );
}

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:adp-toronto-forensic-reconciliation-v1",
      mismatches: summary.calculationMismatch,
    },
    null,
    2
  )
);
