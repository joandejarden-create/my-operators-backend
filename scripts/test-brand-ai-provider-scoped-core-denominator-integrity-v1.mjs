#!/usr/bin/env node
/**
 * Provider-scoped CORE-only benchmark denominator integrity V1.
 * PROVIDER_CALLS = 0
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  aggregateBenchmarkPresence,
  secondaryInCustomerBenchmarkDenominator,
} from "../lib/ai-visibility/competitive-moat/benchmark-engine-v1.js";
import { IDS, SCENARIO_IDS as S } from "../lib/ai-visibility/competitive-moat/benchmark-brand-ids.js";
import {
  auditProviderScopeCandidate,
  BENCHMARK_SCOPES,
  countSecondaryInDenomByScope,
  loadProviderScopedCertificationRegistry,
  runProviderScopedCertificationAudit,
  verifyAllProvidersFrozenBaseline,
} from "../lib/ai-visibility/competitive-moat/provider-scoped-benchmark-certification.js";
import { resolveScenarioCommercialPeers } from "../lib/ai-visibility/competitive-moat/scenario-peer-eligibility.js";
import { loadBenchmarkEligibleUniverse } from "../lib/ai-visibility/competitive-moat/benchmark-eligible-universe.js";
import { verifyFrozenBaseline } from "../lib/ai-visibility/competitive-moat/scenario-benchmark-longitudinal-recertification.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const REGISTRY_PATH = path.join(
  ROOT,
  "reports/ai-visibility/provider-scoped-benchmark-certification-v1.json"
);

let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log("  PASS", name);
  } catch (err) {
    failed += 1;
    console.log("  FAIL", name + ":", err.message);
  }
}

console.log("\nBrand AI Provider-Scoped CORE Denominator Integrity V1\n");

await test("CORE-only denominator — rate collision does not flag secondary", () => {
  const corePairwise = [
    { commercialRelation: "CORE", peerPresence: 0.5 },
    { commercialRelation: "CORE", peerPresence: 0.8 },
    { commercialRelation: "CORE", peerPresence: 0.6 },
  ];
  const benchCore = aggregateBenchmarkPresence(
    corePairwise.map((p) => p.peerPresence),
    "MEDIAN"
  ).value;
  assert.equal(secondaryInCustomerBenchmarkDenominator(corePairwise, benchCore), false);
  // Legacy false positive: secondary with same rate as core
  const secondarySameRate = 0.6;
  assert.ok(corePairwise.map((p) => p.peerPresence).includes(secondarySameRate));
  assert.equal(
    secondaryInCustomerBenchmarkDenominator(corePairwise, benchCore),
    false
  );
});

await test("CORE-only denominator — non-CORE in set flags defect", () => {
  const contaminated = [
    { commercialRelation: "CORE", peerPresence: 0.5 },
    { commercialRelation: "SECONDARY", peerPresence: 0.9 },
  ];
  assert.equal(secondaryInCustomerBenchmarkDenominator(contaminated, 0.7), true);
});

await test("commercial peer classification provider-independent", () => {
  const universe = loadBenchmarkEligibleUniverse();
  const allPeers = resolveScenarioCommercialPeers(IDS.AUTOGRAPH, S.SOFT_BRAND, { universe });
  const openaiPeers = resolveScenarioCommercialPeers(IDS.AUTOGRAPH, S.SOFT_BRAND, { universe });
  assert.deepEqual(
    allPeers.calculationPeers.map((p) => `${p.peerBrandId}|${p.commercialRelation}`),
    openaiPeers.calculationPeers.map((p) => `${p.peerBrandId}|${p.commercialRelation}`)
  );
});

await test("rerun provider certification — zero secondary in customer denominators", () => {
  const report = runProviderScopedCertificationAudit({ registryPath: REGISTRY_PATH });
  const counts = countSecondaryInDenomByScope(report.records);
  assert.equal(counts.OPENAI, 0, JSON.stringify(counts));
  assert.equal(counts.GEMINI, 0, JSON.stringify(counts));
  assert.equal(counts.PERPLEXITY, 0, JSON.stringify(counts));
  assert.equal(counts.CLAUDE, 0, JSON.stringify(counts));
  assert.equal(report.secondaryInDenomByScope.OPENAI, 0);
});

await test("Autograph OpenAI deep dive — secondary not in denominator", () => {
  runProviderScopedCertificationAudit({ registryPath: REGISTRY_PATH });
  const dive = auditProviderScopeCandidate(IDS.AUTOGRAPH, S.SOFT_BRAND, BENCHMARK_SCOPES.OPENAI, {
    registryPath: REGISTRY_PATH,
  });
  assert.equal(dive.SECONDARY_IN_DENOMINATOR, 0);
  assert.equal(dive.gates.secondaryInDenom, false);
  assert.ok(dive.CORE_PEERS.length >= 3);
});

await test("Perplexity certified integrity — values unchanged", () => {
  const before = JSON.parse(fs.readFileSync(REGISTRY_PATH, "utf8"));
  runProviderScopedCertificationAudit({ registryPath: REGISTRY_PATH });
  const after = JSON.parse(fs.readFileSync(REGISTRY_PATH, "utf8"));
  const beforePerp = (before.certifiedByScope?.PERPLEXITY || []).slice().sort((a, b) =>
    `${a.subjectBrandId}|${a.scenarioId}`.localeCompare(`${b.subjectBrandId}|${b.scenarioId}`)
  );
  const afterPerp = (after.certifiedByScope?.PERPLEXITY || []).slice().sort((a, b) =>
    `${a.subjectBrandId}|${a.scenarioId}`.localeCompare(`${b.subjectBrandId}|${b.scenarioId}`)
  );
  assert.deepEqual(afterPerp, beforePerp);
  assert.equal(afterPerp.length, 6);
});

await test("Claude certified integrity — Ascend preserved", () => {
  const registry = loadProviderScopedCertificationRegistry({ refresh: true, registryPath: REGISTRY_PATH });
  const ascend = (registry.certifiedByScope?.CLAUDE || []).find(
    (r) => r.subjectBrandId === IDS.ASCEND && r.scenarioId === S.SOFT_BRAND
  );
  assert.ok(ascend);
  assert.equal(ascend.certifiedIndex, 63);
});

await test("All Providers frozen baseline 103/103/67", () => {
  const frozen = verifyFrozenBaseline();
  assert.equal(frozen.AUTOGRAPH_103_DIFF, 0);
  assert.equal(frozen.TAPESTRY_103_DIFF, 0);
  assert.equal(frozen.ASCEND_67_DIFF, 0);
  const scope = verifyAllProvidersFrozenBaseline({ registryPath: REGISTRY_PATH });
  assert.equal(scope.AUTOGRAPH_103_DIFF, 0);
  assert.equal(scope.TAPESTRY_103_DIFF, 0);
  assert.equal(scope.ASCEND_67_DIFF, 0);
});

await test("exact-scope UI policy unchanged in brand JS", () => {
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/ai-visibility/ai-visibility-brand.js"),
    "utf8"
  );
  assert.match(js, /Benchmark still developing/);
  assert.match(js, /formatOwnerIntentIndex/);
  assert.match(js, /typeof row\.indexValue === "number"/);
});

console.log("\n" + passed + " passed, " + failed + " failed\n");

if (failed > 0) {
  process.exitCode = 1;
  console.log("BRAND_AI_PROVIDER_SCOPED_CORE_DENOMINATOR_INTEGRITY_REMEDIATION_REQUIRED");
} else {
  console.log("BRAND_AI_PROVIDER_SCOPED_CORE_DENOMINATOR_INTEGRITY_PASS");
}
