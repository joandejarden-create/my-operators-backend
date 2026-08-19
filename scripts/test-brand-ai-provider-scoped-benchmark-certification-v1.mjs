#!/usr/bin/env node
/**
 * Brand AI provider-scoped Owner Intent presence + certification V1
 * PROVIDER_CALLS = 0
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { IDS, SCENARIO_IDS as S } from "../lib/ai-visibility/competitive-moat/benchmark-brand-ids.js";
import {
  auditObservationScenarioMapping,
  enrichObservationForScenarioResolution,
} from "../lib/ai-visibility/competitive-moat/prompt-scenario-bridge.js";
import {
  BENCHMARK_SCOPES,
  benchmarkScopeFromProvider,
  lookupScopeCertification,
  runProviderScopedCertificationAudit,
  verifyAllProvidersFrozenBaseline,
  PROVIDER_SPECIFIC_CERTIFICATION_GATES,
} from "../lib/ai-visibility/competitive-moat/provider-scoped-benchmark-certification.js";
import {
  buildOwnerIntentBenchmarksForBrand,
  computeScenarioSubjectPresence,
} from "../lib/ai-visibility/competitive-moat/scenario-benchmark-customer-service.js";
import { buildPromptMetadataById } from "../lib/ai-visibility/associations/prompt-metadata-lookup.js";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import { findMatchingSummaries } from "../lib/ai-visibility/brand-read-service.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const BRAND_JS = fs.readFileSync(
  path.join(ROOT, "public/js/ai-visibility/ai-visibility-brand.js"),
  "utf8"
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

console.log("\nBrand AI Provider-Scoped Benchmark Certification V1\n");

runProviderScopedCertificationAudit();

await test("governed prompt metadata precedes display labels", () => {
  const map = buildPromptMetadataById();
  const enriched = enrichObservationForScenarioResolution(
    {
      promptId: "p_global_collection_affiliation_v1",
      promptFamily: "Collection / Soft Brand",
      intentTerritory: "Collection / Soft Brand",
    },
    map
  );
  assert.equal(enriched.promptFamily, "showcase_collection_soft_affiliation");
});

await test("provider observation mapping — openai zero unmapped", async () => {
  const store = createBrandAiVisibilityReadStore();
  const geo = { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" };
  const summary = (await findMatchingSummaries(store, geo, "openai", { language: "en" }))[0];
  assert.ok(summary, "expected openai summary");
  const { observations } = await loadObservationsFromBatchSummary(store, summary, {
    language: "en",
  });
  const audit = auditObservationScenarioMapping(observations);
  assert.equal(audit.unmapped, 0, JSON.stringify(audit.unmappedSamples));
  assert.ok(audit.mapped >= 60);
});

await test("autograph openai soft brand presence populated", async () => {
  const store = createBrandAiVisibilityReadStore();
  const geo = { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" };
  const summary = (await findMatchingSummaries(store, geo, "openai", { language: "en" }))[0];
  const { observations } = await loadObservationsFromBatchSummary(store, summary, {
    language: "en",
  });
  const presence = computeScenarioSubjectPresence(IDS.AUTOGRAPH, S.SOFT_BRAND, observations);
  assert.equal(presence, 1);
});

await test("all providers autograph soft brand index 103 unchanged", () => {
  const all = buildOwnerIntentBenchmarksForBrand(IDS.AUTOGRAPH, {
    allProvidersMode: true,
    provider: "openai",
    benchmarkScope: BENCHMARK_SCOPES.ALL_PROVIDERS,
  });
  const soft = all.ownerIntentBenchmarks.find((r) => r.intentLabel === "Soft Brand Affiliation");
  assert.equal(soft?.indexValue, 103);
  assert.equal(soft?.subjectPresence, 1);
});

await test("openai exact-scope certified index independent of all providers 103", async () => {
  runProviderScopedCertificationAudit();
  const store = createBrandAiVisibilityReadStore();
  const geo = { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" };
  const summary = (await findMatchingSummaries(store, geo, "openai", { language: "en" }))[0];
  const { observations } = await loadObservationsFromBatchSummary(store, summary, {
    language: "en",
  });
  const openai = buildOwnerIntentBenchmarksForBrand(IDS.AUTOGRAPH, {
    allProvidersMode: false,
    provider: "openai",
    observations,
  });
  const soft = openai.ownerIntentBenchmarks.find((r) => r.intentLabel === "Soft Brand Affiliation");
  assert.notEqual(soft?.indexValue, 103);
  assert.equal(soft?.subjectPresence, 1);
  assert.equal(soft?.indexValue, 100);
  assert.equal(soft?.benchmarkStatus, "CERTIFIED");
});

await test("exact scope certified renders perplexity index for autograph soft brand", async () => {
  const store = createBrandAiVisibilityReadStore();
  const geo = { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" };
  const summary = (await findMatchingSummaries(store, geo, "perplexity", { language: "en" }))[0];
  const { observations } = await loadObservationsFromBatchSummary(store, summary, {
    language: "en",
  });
  const block = buildOwnerIntentBenchmarksForBrand(IDS.AUTOGRAPH, {
    allProvidersMode: false,
    provider: "perplexity",
    observations,
  });
  const soft = block.ownerIntentBenchmarks.find((r) => r.intentLabel === "Soft Brand Affiliation");
  const cert = lookupScopeCertification(IDS.AUTOGRAPH, S.SOFT_BRAND, BENCHMARK_SCOPES.PERPLEXITY);
  if (cert?.certificationStatus === "PRODUCTION_VALIDATED") {
    assert.equal(soft?.indexValue, cert.certifiedIndex);
    assert.ok(soft?.relativeGapPct != null);
  }
});

await test("benchmark scope mapping", () => {
  assert.equal(benchmarkScopeFromProvider("openai", false), BENCHMARK_SCOPES.OPENAI);
  assert.equal(benchmarkScopeFromProvider("openai", true), BENCHMARK_SCOPES.ALL_PROVIDERS);
});

await test("frozen all providers baseline", () => {
  const frozen = verifyAllProvidersFrozenBaseline();
  assert.equal(frozen.ok, true);
  assert.equal(frozen.AUTOGRAPH_103_DIFF, 0);
  assert.equal(frozen.TAPESTRY_103_DIFF, 0);
  assert.equal(frozen.ASCEND_67_DIFF, 0);
});

await test("certification contract explicit thresholds", () => {
  assert.equal(PROVIDER_SPECIFIC_CERTIFICATION_GATES.MIN_CORE_PEERS, 3);
  assert.equal(PROVIDER_SPECIFIC_CERTIFICATION_GATES.COMMON_GRAIN_MIN, 8);
  assert.equal(PROVIDER_SPECIFIC_CERTIFICATION_GATES.MULTI_PROVIDER_REQUIRED, false);
});

await test("UI contract — exact scope policy", () => {
  assert.match(BRAND_JS, /Benchmark still developing/);
  assert.match(BRAND_JS, /selected AI provider scope/);
});

console.log("\n" + passed + " passed, " + failed + " failed\n");

if (failed === 0) {
  console.log("BRAND_AI_PROVIDER_SCOPED_BENCHMARK_CERTIFICATION_PASS");
  process.exit(0);
}
console.log("BRAND_AI_PROVIDER_SCOPED_BENCHMARK_CERTIFICATION_REMEDIATION_REQUIRED");
process.exit(1);
