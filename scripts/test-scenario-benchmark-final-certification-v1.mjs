#!/usr/bin/env node
/**
 * Final scenario index certification + controlled UI activation tests.
 * No provider calls.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "url";
import {
  runScenarioBenchmarkFinalCertification,
  FINAL_CERTIFICATION_SUBJECTS,
  resetFinalCertificationCache,
} from "../lib/ai-visibility/competitive-moat/scenario-benchmark-final-certification.js";
import {
  buildOwnerIntentBenchmarksForBrand,
  enrichQuestionsMissingWithCompetitiveContext,
  getCertifiedExecutiveBenchmarkContext,
  auditCustomerBenchmarkPayload,
  positionCopy,
} from "../lib/ai-visibility/competitive-moat/scenario-benchmark-customer-service.js";
import { IDS, SCENARIO_IDS } from "../lib/ai-visibility/competitive-moat/benchmark-brand-ids.js";
import { auditFutureCustomerPayload } from "../lib/ai-visibility/competitive-moat/scenario-benchmark-tab-integration.js";
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

console.log("\nBrand AI Scenario Index Final Certification V1\n");

resetFinalCertificationCache();
const report = runScenarioBenchmarkFinalCertification({ writeReport: true });

await test("three-candidate independent recomputation", () => {
  assert.equal(report.candidates.length, 3);
  assert.equal(report.MATERIAL_MISMATCH, 0);
  for (const c of report.candidates) {
    assert.ok(["EXACT_MATCH", "ROUNDING_ONLY"].includes(c.INDEX_MATCH), c.SUBJECT);
    assert.equal(c.gates.coreOnlyPass, true);
    assert.ok(c.MEASURED_CORE_PEERS >= 3);
    assert.ok(c.COMMON_GRAINS.MIN >= 8);
    assert.ok(c.PROVIDERS.length >= 2);
    assert.equal(c.DENOMINATOR_SAFE, "YES");
    assert.equal(c.SEMANTIC_CLAIM_SAFE, "YES");
    assert.notEqual(c.STABILITY, "FRAGILE");
    assert.notEqual(c.PROVIDER_DIRECTION, "CONFLICT");
  }
});

await test("expected indices", () => {
  const autograph = report.candidates.find((c) => c.subjectId === IDS.AUTOGRAPH);
  const tapestry = report.candidates.find((c) => c.subjectId === IDS.TAPESTRY);
  const ascend = report.candidates.find((c) => c.subjectId === IDS.ASCEND);
  assert.equal(autograph.INDEX, 103);
  assert.equal(tapestry.INDEX, 103);
  assert.equal(ascend.INDEX, 67);
});

await test("production validated count", () => {
  assert.ok(report.certificationCounts.PRODUCTION_VALIDATED >= 3);
  assert.equal(report.SCENARIO_BENCHMARK_UI, "LIVE_CERTIFIED_VALUES_ONLY");
});

await test("customer owner-intent payload certified only", () => {
  const block = buildOwnerIntentBenchmarksForBrand(IDS.AUTOGRAPH, { allProvidersMode: true });
  assert.ok(block.ownerIntentBenchmarks.length >= 1);
  const row = block.ownerIntentBenchmarks[0];
  assert.equal(row.indexValue, 103);
  assert.ok(Math.abs(row.relativeGapPct - 3) <= 0.5);
  assert.ok(row.selectedCorePeers.length <= 3);
  assert.ok(!("peerPresenceValues" in row));
  const limited = buildOwnerIntentBenchmarksForBrand(IDS.INDIGO, { allProvidersMode: true });
  for (const r of limited.ownerIntentBenchmarks) {
    assert.equal(r.indexValue, null);
  }
});

await test("provider-specific does not inherit all-providers certified index", () => {
  const openai = buildOwnerIntentBenchmarksForBrand(IDS.AUTOGRAPH, {
    allProvidersMode: false,
    provider: "openai",
  });
  const soft = openai.ownerIntentBenchmarks.find((r) => r.intentLabel === "Soft Brand Affiliation");
  assert.notEqual(soft?.indexValue, 103);
  const all = buildOwnerIntentBenchmarksForBrand(IDS.AUTOGRAPH, { allProvidersMode: true });
  const allSoft = all.ownerIntentBenchmarks.find((r) => r.intentLabel === "Soft Brand Affiliation");
  assert.equal(allSoft?.indexValue, 103);
});

await test("position copy arithmetic", () => {
  assert.equal(positionCopy(103, 3), "3% above benchmark");
  assert.equal(positionCopy(67, -33), "33% below benchmark");
});

await test("questions missing competitive context additive", () => {
  const rows = enrichQuestionsMissingWithCompetitiveContext(
    [
      {
        QUESTION: "Test",
        PROMPT_FAMILY: "Collection / Soft Brand",
        promptId: "p1",
        SUBJECT_PRESENCE: "MISSING_ACROSS_ALL_PROVIDERS",
        PROVIDERS_MISSING: ["openai", "gemini"],
        PEERS_PRESENT: [],
      },
    ],
    { subjectBrandId: IDS.AUTOGRAPH, observations: [], brandNamesById: {} }
  );
  assert.ok(Array.isArray(rows[0].corePeersPresent));
  assert.ok(["PRIORITY", "REVIEW", "MONITOR"].includes(rows[0].priority));
});

await test("executive benchmark context", () => {
  const ctx = getCertifiedExecutiveBenchmarkContext(IDS.ASCEND);
  assert.equal(ctx.indexValue, 67);
  assert.ok(ctx.positionCopy.includes("below"));
});

await test("customer payload leak audit", () => {
  const block = buildOwnerIntentBenchmarksForBrand(IDS.TAPESTRY, { allProvidersMode: true });
  const audit = auditCustomerBenchmarkPayload(block);
  assert.equal(audit.ok, true, audit.violations?.join("; "));
});

await test("UI files reuse existing sections", () => {
  const html = fs.readFileSync(path.join(ROOT, "public/ai-visibility-brand.html"), "utf8");
  const js = fs.readFileSync(path.join(ROOT, "public/js/ai-visibility/ai-visibility-brand.js"), "utf8");
  assert.match(html, /Competitive \/ Peer Analysis/);
  assert.match(html, /Questions Missing Watchlist/);
  assert.match(js, /AI Presence by Owner Intent/);
  assert.match(js, /renderOwnerIntentBenchmarks/);
  assert.doesNotMatch(html, /Overall AI Presence Index/i);
});

await test("operator freeze", () => {
  assert.equal(report.regression.OPERATOR_DIFF, 0);
  assert.equal(report.regression.operatorCount, PRIMARY_OPERATOR_COUNT);
});

await test("no provider calls", () => {
  assert.equal(report.providerCalls, 0);
  assert.equal(report.spend, 0);
});

console.log(`\n${passed} passed, ${failed} failed\n`);
process.exit(failed ? 1 : 0);
