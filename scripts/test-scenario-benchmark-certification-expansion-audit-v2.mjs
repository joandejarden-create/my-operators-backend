#!/usr/bin/env node
/**
 * Scenario benchmark certification expansion audit V2 tests.
 * No provider calls. No UI changes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "url";
import {
  runScenarioBenchmarkCertificationExpansionAudit,
  PRIMARY_PATHS,
  FROZEN_CERTIFIED,
} from "../lib/ai-visibility/competitive-moat/scenario-benchmark-certification-expansion-audit.js";
import { PRIMARY_OPERATOR_COUNT } from "../lib/ai-visibility/operator-intelligence/universe.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nBrand AI Scenario Certification Expansion Audit V2\n");

const report = runScenarioBenchmarkCertificationExpansionAudit({ writeReport: true });

await test("certified 3 rows frozen", () => {
  assert.equal(report.freeze.STOP, false);
  assert.equal(report.freeze.AUTOGRAPH_SOFT_BRAND.DIFF, 0);
  assert.equal(report.freeze.AUTOGRAPH_SOFT_BRAND.index, 103);
  assert.equal(report.freeze.TAPESTRY_SOFT_BRAND.DIFF, 0);
  assert.equal(report.freeze.TAPESTRY_SOFT_BRAND.index, 103);
  assert.equal(report.freeze.ASCEND_SOFT_COLLECTION.DIFF, 0);
  assert.equal(report.freeze.ASCEND_SOFT_COLLECTION.index, 67);
  assert.equal(report.freeze.CERTIFIED_UI_DIFF, 0);
  assert.equal(FROZEN_CERTIFIED.length, 3);
});

await test("every non-certified row gets one primary path", () => {
  assert.equal(report.audit.ROWS_AUDITED, 42);
  for (const row of report.rows) {
    assert.ok(PRIMARY_PATHS.includes(row.PRIMARY_CERTIFICATION_PATH), row.SUBJECT);
    assert.equal(
      PRIMARY_PATHS.filter((p) => p === row.PRIMARY_CERTIFICATION_PATH).length,
      1
    );
  }
  const sum = Object.values(report.primaryPathBreakdown).reduce((a, b) => a + b, 0);
  assert.equal(sum, 42);
});

await test("near/medium/far classification", () => {
  assert.equal(
    report.audit.NEAR + report.audit.MEDIUM + report.audit.FAR,
    report.audit.ROWS_AUDITED
  );
  for (const row of report.rows) {
    assert.ok(["NEAR", "MEDIUM", "FAR"].includes(row.DISTANCE), row.SUBJECT);
  }
});

await test("soft-brand expansion audit", () => {
  assert.ok(report.softBrand.CURIO);
  assert.ok(report.softBrand.TRIBUTE);
  assert.ok(report.softBrand.VIGNETTE);
  assert.equal(report.softBrand.CURIO.PROVIDER_BLOCKER, "CONFLICT");
  assert.equal(report.softBrand.TRIBUTE.PROVIDER_BLOCKER, "CONFLICT");
  assert.equal(report.softBrand.VIGNETTE.PROVIDER_BLOCKER, "CONFLICT");
  assert.equal(report.softBrand.CURIO.CAN_NORMAL_LONGITUDINAL_WAVE_HELP, "YES");
  assert.equal(report.softBrand.CURIO.SPECIAL_WAVE_REQUIRED, "NO");
});

await test("conversion blocker audit", () => {
  assert.equal(report.conversion.AUTOGRAPH.INDEX, 164);
  assert.equal(report.conversion.AUTOGRAPH.STABILITY, "FRAGILE");
  assert.equal(report.conversion.SPECIAL_WAVE_REQUIRED, "NO");
  assert.equal(report.conversion.NORMAL_LONGITUDINAL_WAVE_CAN_HELP, "YES");
  assert.match(report.conversion.AUTOGRAPH.DETAIL, /Do not drop/);
});

await test("lifestyle segmentation audit", () => {
  assert.equal(report.lifestyle.RECOMMENDED_ACTION, "SPLIT_SCENARIO");
  assert.equal(report.lifestyle.BIMODALITY_CAUSE, "MIXED");
  const moxy = report.lifestyle.ADDITIONAL_CORE_CANDIDATES.find((c) => c.brand === "Moxy Hotels");
  assert.equal(moxy.COMMERCIAL_CORE_FIT, "LOW");
  const red = report.lifestyle.ADDITIONAL_CORE_CANDIDATES.find((c) => c.brand === "Radisson RED");
  assert.match(red.note, /SECONDARY/);
});

await test("longitudinal reuse checked before special-wave recommendation", () => {
  assert.equal(report.focusedSpecialWave.REQUIRED, "NO");
  assert.equal(report.focusedSpecialWave.CALLS, 0);
  const nearConflict = report.rows.filter(
    (r) =>
      r.PROVIDER_DIRECTION === "CONFLICT" &&
      r.DISTANCE === "NEAR"
  );
  for (const row of nearConflict) {
    assert.equal(row.SPECIAL_WAVE_REQUIRED, "NO");
    assert.equal(row.CAN_NORMAL_LONGITUDINAL_WAVE_ADVANCE_CERTIFICATION, "YES");
  }
});

await test("offline re-extraction checked before provider-call recommendation", () => {
  assert.equal(report.independentConversion.SPECIAL_WAVE_REQUIRED, "NO");
  assert.equal(report.independentConversion.OPTION_A_2_PROVIDER.EXECUTED, "NO");
  assert.equal(report.independentConversion.OPTION_B_4_PROVIDER.EXECUTED, "NO");
  assert.ok(report.independentConversion.OPTION_A_2_PROVIDER.CALLS > 0);
});

await test("shared provider cost not multiplied by brands", () => {
  const a = report.independentConversion.OPTION_A_2_PROVIDER;
  assert.equal(a.CALLS, a.PROMPTS * a.PROVIDERS.length);
  const b = report.independentConversion.OPTION_B_4_PROVIDER;
  assert.equal(b.CALLS, b.PROMPTS * b.PROVIDERS.length);
  assert.ok(b.CALLS < b.PROMPTS * b.PROVIDERS.length * 6);
});

await test("target 6/8/12 roadmap consistency", () => {
  assert.equal(report.expansionRoadmap.target6.ROWS.length, 3);
  assert.equal(report.expansionRoadmap.target6.SPECIAL_CALLS, 0);
  assert.equal(report.expansionRoadmap.target8.ROWS.length, 5);
  assert.equal(report.expansionRoadmap.target8.SPECIAL_CALLS, 0);
  assert.equal(report.expansionRoadmap.target12.FEASIBLE, "PARTIAL");
  assert.equal(report.bestNext5.length, 5);
  assert.equal(report.second5.length, 5);
  assert.ok(report.bestNext5[0].SUBJECT.includes("Curio"));
});

await test("no provider calls", () => {
  assert.equal(report.providerCalls, 0);
  assert.equal(report.spend, 0);
  assert.equal(report.focusedSpecialWave.EXECUTED, "NO");
});

await test("no UI changes", () => {
  assert.equal(report.uiChanges, 0);
  assert.equal(report.NEW_ROWS_RENDERED, 0);
  assert.equal(report.LIVE_CERTIFIED_VALUES_ONLY, "UNCHANGED");
  assert.equal(report.regression.BRAND_UI_DIFF, 0);
  const html = fs.readFileSync(path.join(ROOT, "public/ai-visibility-brand.html"), "utf8");
  assert.match(html, /Competitive \/ Peer Analysis/);
  assert.doesNotMatch(html, /Overall AI Presence Index/i);
});

await test("Brand regression", () => {
  assert.equal(report.regression.CERTIFIED_BENCHMARK_ROWS_DIFF, 0);
  assert.equal(report.regression.BRAND_PRESENCE_DIFF, 0);
  assert.equal(report.regression.BRAND_LONGITUDINAL_DATA_DIFF, 0);
  assert.equal(report.current.PRODUCTION_VALIDATED, 3);
  assert.equal(report.HEADLINE_INDEX, "DEFERRED");
});

await test("Operator regression", () => {
  assert.equal(report.regression.OPERATOR_DIFF, 0);
  assert.equal(report.regression.operatorCount, PRIMARY_OPERATOR_COUNT);
});

await test("distribution and residences deferred", () => {
  assert.equal(report.distribution.PROMPT_GAP, "YES");
  assert.equal(report.distribution.FUTURE_PHASE, "BRAND_DISTRIBUTION_OWNER_INTENT_PROMPT_DESIGN");
  assert.equal(report.brandedResidences.STATUS, "REDESIGN_REQUIRED");
});

console.log(`\n${passed} passed, ${failed} failed\n`);
process.exit(failed ? 1 : 0);
