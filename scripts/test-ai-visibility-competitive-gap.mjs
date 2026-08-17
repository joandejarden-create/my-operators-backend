#!/usr/bin/env node
/**
 * P0C — Competitive Gap Engine tests.
 */
import assert from "node:assert/strict";
import { createHash } from "crypto";
import {
  isAssociationAttributeProductionEligible,
  PRODUCTION_ELIGIBLE_ASSOCIATION_ATTRIBUTES,
} from "../lib/ai-visibility/gaps/association-eligibility.js";
import {
  classifyGapPersistence,
  classifyGapPriority,
  classifyTrendStatus,
} from "../lib/ai-visibility/gaps/gap-priority.js";
import { buildGapId, dedupeGaps } from "../lib/ai-visibility/gaps/gap-identity.js";
import {
  detectPeerPresentBrandMissingGaps,
  aggregatePersistentScenarioGaps,
  detectValidatedAssociationGaps,
  detectCompetitiveGapsForBrand,
  runCompetitiveGapEngine,
  buildExecutiveGapHighlights,
} from "../lib/ai-visibility/gaps/competitive-gap-engine.js";
import { buildTruthLayerHook } from "../lib/ai-visibility/gaps/truth-layer-hook.js";
import { observationsFromEvidence } from "../lib/ai-visibility/gaps/evidence-observations.js";
import { computeAiPresenceRate } from "../lib/ai-visibility/metrics.js";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import { auditAssociationEvidenceCorpus } from "../lib/ai-visibility/associations/evidence-corpus-audit.js";

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

async function asyncTest(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

function obs(overrides = {}) {
  return {
    success: true,
    presentEntityIds: overrides.present || [],
    promptId: overrides.promptId || "p_test",
    provider: overrides.provider || "openai",
    language: overrides.language || "en",
    geographyKey: overrides.geography || "CALA",
    commercialRegion: "CALA",
    scenarioId: overrides.scenarioId || "scenario_independent_uu_conversion_v1",
    scenarioStatus: "MAPPED",
    intentFamily: "Conversion",
    ownerPriority: "Conversion Suitability",
    commercialPriority: overrides.commercialPriority || "CRITICAL",
    evidenceId: overrides.evidenceId || `ev_${createHash("sha256").update(JSON.stringify(overrides)).digest("hex").slice(0, 8)}`,
    periodKey: overrides.periodKey || "run_1",
    citations: overrides.citations || [],
    ...overrides,
  };
}

console.log("\nP0C — Competitive Gap Engine Tests\n");

test("association eligibility — DISTRIBUTION allowed", () => {
  assert.equal(isAssociationAttributeProductionEligible("DISTRIBUTION"), true);
});

test("association eligibility — OWNER_FLEXIBILITY blocked", () => {
  assert.equal(isAssociationAttributeProductionEligible("OWNER_FLEXIBILITY"), false);
});

test("commercial priority matrix — Critical Strongly Repeated → HIGH_PRIORITY", () => {
  assert.equal(classifyGapPriority("CRITICAL", "STRONGLY_REPEATED"), "HIGH_PRIORITY");
});

test("commercial priority matrix — Investigation Single → null", () => {
  assert.equal(classifyGapPriority("INVESTIGATION", "SINGLE"), null);
});

test("persistence — strongly repeated", () => {
  assert.equal(
    classifyGapPersistence({ observationCount: 10, providers: ["a", "b", "c"], periods: ["p1", "p2"], variants: ["v1", "v2"] }),
    "STRONGLY_REPEATED"
  );
});

test("peer present subject missing — detects gap", () => {
  const observations = [
    obs({ present: ["receQkxgjlezsc1xg"], promptId: "p1", evidenceId: "ev1" }),
    obs({ present: ["recEJCTDj1zrsjPM6"], promptId: "p2", evidenceId: "ev2" }),
  ];
  const gaps = detectPeerPresentBrandMissingGaps(observations, {
    subjectBrandId: "recEJCTDj1zrsjPM6",
    peerEntityIds: ["recEJCTDj1zrsjPM6", "receQkxgjlezsc1xg"],
  });
  assert.equal(gaps.length, 1);
  assert.equal(gaps[0].peerBrandIds[0], "receQkxgjlezsc1xg");
});

test("subject present — no peer-present gap", () => {
  const observations = [obs({ present: ["recEJCTDj1zrsjPM6", "receQkxgjlezsc1xg"] })];
  const gaps = detectPeerPresentBrandMissingGaps(observations, {
    subjectBrandId: "recEJCTDj1zrsjPM6",
    peerEntityIds: ["recEJCTDj1zrsjPM6", "receQkxgjlezsc1xg"],
  });
  assert.equal(gaps.length, 0);
});

test("no peer present — no competitive gap", () => {
  const observations = [obs({ present: [] })];
  const gaps = detectPeerPresentBrandMissingGaps(observations, {
    subjectBrandId: "recEJCTDj1zrsjPM6",
    peerEntityIds: ["recEJCTDj1zrsjPM6", "receQkxgjlezsc1xg"],
  });
  assert.equal(gaps.length, 0);
});

test("persistent scenario gap — aggregates A-class", () => {
  const raw = [
    {
      gapClass: "PEER_PRESENT_BRAND_MISSING",
      subjectBrandId: "recEJCTDj1zrsjPM6",
      peerBrandIds: ["receQkxgjlezsc1xg"],
      scenarioId: "scenario_independent_uu_conversion_v1",
      geography: "CALA",
      language: "en",
      commercialPriority: "CRITICAL",
      evidenceIds: ["ev1"],
      promptId: "p1",
      provider: "openai",
      periods: ["run1"],
      peerSetId: "peers_uu_collection_lifestyle_owner_decision_v2",
    },
    {
      gapClass: "PEER_PRESENT_BRAND_MISSING",
      subjectBrandId: "recEJCTDj1zrsjPM6",
      peerBrandIds: ["receQkxgjlezsc1xg"],
      scenarioId: "scenario_independent_uu_conversion_v1",
      geography: "CALA",
      language: "en",
      commercialPriority: "CRITICAL",
      evidenceIds: ["ev2"],
      promptId: "p2",
      provider: "claude",
      periods: ["run2"],
      peerSetId: "peers_uu_collection_lifestyle_owner_decision_v2",
    },
  ];
  const persistent = aggregatePersistentScenarioGaps(raw, { subjectBrandId: "recEJCTDj1zrsjPM6" });
  assert.equal(persistent.length, 1);
  assert.ok(["EMERGING", "REPEATED", "STRONGLY_REPEATED"].includes(persistent[0].persistence));
});

test("variant dedupe — same gapId merges evidence", () => {
  const base = {
    gapClass: "PEER_PRESENT_BRAND_MISSING",
    subjectBrandId: "recEJCTDj1zrsjPM6",
    peerBrandIds: ["receQkxgjlezsc1xg"],
    scenarioId: "scenario_independent_uu_conversion_v1",
    geography: "CALA",
    language: "en",
    peerSetId: "peers_uu_collection_lifestyle_owner_decision_v2",
    evidenceIds: ["ev1"],
    providers: ["openai"],
    promptIds: ["p1"],
    observationCount: 1,
    commercialPriority: "CRITICAL",
    classification: "MONITOR",
  };
  const id = buildGapId(base);
  const merged = dedupeGaps([
    { ...base, gapId: id },
    { ...base, gapId: id, evidenceIds: ["ev2"], providers: ["claude"], promptIds: ["p2"], observationCount: 1 },
  ]);
  assert.equal(merged.length, 1);
  assert.equal(merged[0].evidenceIds.length, 2);
});

test("language isolation — EN observations only", () => {
  const evidence = [
    { evidenceId: "ev_en", promptId: "p1", provider: "openai", language: "en", commercialRegion: "CALA", payload: { mentions: [], citations: [] } },
    { evidenceId: "ev_es", promptId: "p2", provider: "openai", language: "es", commercialRegion: "CALA", payload: { mentions: [], citations: [] } },
  ];
  const en = observationsFromEvidence(evidence, { geography: "CALA", language: "en" });
  assert.ok(en.every((o) => o.language === "en"));
});

test("geography isolation — CALA filter", () => {
  const evidence = [
    { evidenceId: "ev1", promptId: "p1", provider: "openai", language: "en", commercialRegion: "CALA", payload: { mentions: [], citations: [] } },
    { evidenceId: "ev2", promptId: "p2", provider: "openai", language: "en", commercialRegion: "Europe", payload: { mentions: [], citations: [] } },
  ];
  const cala = observationsFromEvidence(evidence, { geography: "CALA", language: "en" });
  assert.equal(cala.length, 1);
});

test("insufficient history — single period", () => {
  assert.equal(classifyTrendStatus(1), "INSUFFICIENT_HISTORY");
});

test("association gap — blocks OWNER_FLEXIBILITY", () => {
  const result = detectValidatedAssociationGaps([], {
    subjectBrandId: "recEJCTDj1zrsjPM6",
    peerEntityIds: ["receQkxgjlezsc1xg"],
    attributeId: "OWNER_FLEXIBILITY",
    geography: "CALA",
    language: "en",
  });
  assert.equal(result.blocked, true);
});

test("research-only association cannot affect client classification", () => {
  const engine = runCompetitiveGapEngine({
    observations: [obs({ present: ["receQkxgjlezsc1xg"] })],
    evidence: [],
    brandIds: ["recEJCTDj1zrsjPM6"],
    brandNamesById: { recEJCTDj1zrsjPM6: "Autograph Collection", receQkxgjlezsc1xg: "Curio Collection" },
    geography: "CALA",
    language: "en",
  });
  assert.equal(engine.clientVisibleResearchAssociationGaps, 0);
  assert.ok(engine.researchAssociationGapsCreated >= 0);
});

test("deterministic gap ID — stable", () => {
  const a = buildGapId({ gapClass: "PEER_PRESENT_BRAND_MISSING", subjectBrandId: "x", scenarioId: "s", geography: "CALA", language: "en" });
  const b = buildGapId({ gapClass: "PEER_PRESENT_BRAND_MISSING", subjectBrandId: "x", scenarioId: "s", geography: "CALA", language: "en" });
  assert.equal(a, b);
});

test("truth layer hook — NOT_EVALUATED", () => {
  const hook = buildTruthLayerHook({ gapClass: "AI_PERCEPTION_VS_DEALALITY_FACT_GAP" });
  assert.equal(hook.truthComparisonStatus, "NOT_EVALUATED");
});

test("executive highlights — production qualified only", () => {
  const gaps = [
    { gapClass: "PERSISTENT_SCENARIO_GAP", subjectBrandId: "recEJCTDj1zrsjPM6", peerBrandIds: ["receQkxgjlezsc1xg"], scenarioId: "scenario_independent_uu_conversion_v1", geography: "CALA", language: "en", commercialPriority: "CRITICAL", classification: "HIGH_PRIORITY", persistence: "STRONGLY_REPEATED", observationCount: 5, lifecycleStatus: "ACTIVE" },
  ];
  const h = buildExecutiveGapHighlights(gaps, { recEJCTDj1zrsjPM6: "Autograph Collection", receQkxgjlezsc1xg: "Curio Collection" });
  assert.ok(h.LARGEST_COMPETITIVE_GAP);
  assert.equal(h.productionQualified, true);
});

await asyncTest("full engine — runs on existing corpus without provider calls", async () => {
  const corpus = await auditAssociationEvidenceCorpus();
  const observations = observationsFromEvidence(corpus.evidence, { geography: "CALA", language: "en" });
  const engine = runCompetitiveGapEngine({
    observations,
    evidence: corpus.evidence,
    brandIds: ["recEJCTDj1zrsjPM6"],
    brandNamesById: { recEJCTDj1zrsjPM6: "Autograph Collection" },
    geography: "CALA",
    language: "en",
  });
  assert.equal(engine.NEW_PROVIDER_CALLS, 0);
  assert.ok(Array.isArray(engine.gaps));
});

await asyncTest("certified metrics — Presence unchanged", async () => {
  const corpus = await auditAssociationEvidenceCorpus();
  const observations = observationsFromEvidence(corpus.evidence, { geography: "CALA", language: "en" });
  const before = computeAiPresenceRate(observations, "recEJCTDj1zrsjPM6");
  const engine = runCompetitiveGapEngine({ observations, evidence: corpus.evidence, brandIds: ["recEJCTDj1zrsjPM6"] });
  const after = computeAiPresenceRate(observations, "recEJCTDj1zrsjPM6");
  assert.equal(after.value, before.value);
  assert.equal(after.denominator, before.denominator);
  assert.ok(engine.gaps.length >= 0);
});

test("production eligible attributes — DISTRIBUTION only", () => {
  assert.deepEqual([...PRODUCTION_ELIGIBLE_ASSOCIATION_ATTRIBUTES], ["DISTRIBUTION"]);
});

console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}`);
if (failed > 0) {
  console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0C_REMEDIATION_REQUIRED (tests failed)");
  process.exit(1);
}
console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0C_PASS (tests)");
process.exit(0);
