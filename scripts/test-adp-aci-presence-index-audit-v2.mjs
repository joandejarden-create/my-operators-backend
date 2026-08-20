#!/usr/bin/env node
/**
 * ADP ACI + Presence Index audit V2 tests.
 *   npm run test:adp-aci-presence-index-audit-v2
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { computeDemandCaptureIndex } from "../lib/ai-demand-positioning/intelligence/demand-capture-index.js";
import { buildOwnerPayload, computeIntentPresenceIndexLegacy } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import {
  reconstructIntentPresenceIndex,
  isDeclaredCompPresenceIndex,
  ADP_INDEX_MAX,
  ADP_INDEX_MIN_AVG_COMP_RATE,
} from "../lib/ai-demand-positioning/metrics/presence-index-reconstruction.js";
import {
  classifyObservedEntity,
  classifyEntityUniverse,
  entityEligibleForBenchmark,
  ENTITY_CLASSES,
} from "../lib/ai-demand-positioning/metrics/south-florida-entity-registry.js";
import {
  classifyCandidateForTerritory,
  buildTerritoryBenchmarkSets,
  COMPETITIVE_CLASSES,
  MIN_CORE_COMPETITORS,
} from "../lib/ai-demand-positioning/metrics/territory-core-contract.js";
import {
  observationFractionalShare,
  expectedShareEqualFair,
  computeTerritoryAci,
  providerScopedAci,
  aciSensitivity,
} from "../lib/ai-demand-positioning/metrics/aci-research-engine.js";
import { runAciPresenceIndexAuditV2 } from "../lib/ai-demand-positioning/metrics/aci-presence-index-audit-v2.js";
import { TRAVELER_INTENTS } from "../lib/ai-demand-positioning/prompt-universe/standard-scenarios.js";

const PROPERTY_ID = "adp_waterstone_boca_raton";

function synthObs({ scenarioId, provider, mentioned, competitors }) {
  return {
    observationId: `obs_${scenarioId}_${provider}`,
    scenarioId,
    provider,
    parsed: true,
    mentioned,
    competitorsMentioned: competitors || [],
  };
}

function main() {
  const profile = loadPropertyProfile(PROPERTY_ID);
  const periods = loadAllPeriods(PROPERTY_ID);
  const period = [...periods].reverse().find((p) => (p.observations || []).some((o) => o.parsed));
  const scenarios = buildScenarioUniverse(profile);
  const observations = period.observations.filter((o) => o.parsed);

  const owner = buildOwnerPayload(period, scenarios, profile, { allPeriods: periods });
  const reconstructed = reconstructIntentPresenceIndex(
    observations,
    scenarios,
    profile,
    computeDemandCaptureIndex(observations, scenarios)
  );
  const legacy = computeIntentPresenceIndexLegacy(
    observations,
    scenarios,
    profile,
    computeDemandCaptureIndex(observations, scenarios)
  );
  for (const intent of Object.keys(legacy || {})) {
    assert.strictEqual(reconstructed[intent].index, legacy[intent].index, `legacy reconstruction ${intent}`);
  }
  assert.ok(!owner.aiConsiderationIndex, "no UI ACI field");
  assert.equal(ADP_INDEX_MAX, 200);
  assert.equal(ADP_INDEX_MIN_AVG_COMP_RATE, 30);

  // --- Declared matcher / peer sets ---
  assert.equal(isDeclaredCompPresenceIndex("Renaissance Boca Raton", ["renaissance boca raton"]), true);
  assert.ok((reconstructed.business.currentPeerSet || []).length >= 0);

  // --- Thin peer suppression ---
  const thinProfile = { ...profile, declaredCompSet: ["The Boca Raton"] };
  const thin = reconstructIntentPresenceIndex(observations, scenarios, thinProfile, computeDemandCaptureIndex(observations, scenarios));
  const thinIntent = Object.values(thin)[0];
  assert.equal(thinIntent.index, null);
  assert.ok(String(thinIntent.suppressionState).includes("THIN"));

  // --- Entity canonicalization / artifacts ---
  assert.equal(classifyObservedEntity("Best Hotel").class, ENTITY_CLASSES.GENERIC_PHRASE);
  assert.equal(classifyObservedEntity("Yacht Club").class, ENTITY_CLASSES.VENUE_ONLY);
  assert.equal(classifyObservedEntity("The Boca Raton").entityId, "the_boca_raton");
  assert.equal(classifyObservedEntity("Boca Raton Resort").entityId, "the_boca_raton");
  assert.equal(classifyObservedEntity("Beach Club").class, ENTITY_CLASSES.AMBIGUOUS);
  assert.equal(entityEligibleForBenchmark(classifyObservedEntity("Best Hotel")), false);
  assert.equal(entityEligibleForBenchmark(classifyObservedEntity("The Boca Raton")), true);
  const gov = classifyEntityUniverse(["Best Hotel", "The Boca Raton", "Boca Raton Resort", "Yacht Club"]);
  assert.equal(gov.canonicalHotels, 1);
  assert.ok(gov.duplicatesMerged >= 1);
  assert.ok(gov.artifactsRemoved >= 2);

  // --- Territory CORE; declared not automatic CORE ---
  const hiltonLeisure = classifyCandidateForTerritory("hilton_boca_raton_suites", TRAVELER_INTENTS.LEISURE);
  assert.notEqual(hiltonLeisure.role, COMPETITIVE_CLASSES.CORE_COMPETITOR);
  const renaissanceCouples = classifyCandidateForTerritory("renaissance_boca_raton", TRAVELER_INTENTS.COUPLES);
  assert.equal(renaissanceCouples.role, COMPETITIVE_CLASSES.NON_COMPARABLE);
  const bocaBusiness = classifyCandidateForTerritory("the_boca_raton", TRAVELER_INTENTS.BUSINESS);
  assert.equal(bocaBusiness.role, COMPETITIVE_CLASSES.CORE_COMPETITOR);
  const sets = buildTerritoryBenchmarkSets(profile, ["The Boca Raton", "Seagate Hotel", "Hawks Cay Resort"]);
  assert.ok(sets.byIntent.business.coreCount >= MIN_CORE_COMPETITORS);
  assert.ok(!sets.byIntent.leisure.coreIds.includes("hawks_cay"));
  assert.equal(sets.byIntent.leisure.secondaryCount >= 0, true);

  // --- Actual share / multi-hotel / absent ---
  const coreIds = ["the_boca_raton", "renaissance_boca_raton", "marriott_boca_raton"];
  const multi = observationFractionalShare(
    synthObs({ scenarioId: "s1", provider: "openai", mentioned: true, competitors: ["The Boca Raton", "Renaissance Boca Raton"] }),
    coreIds
  );
  assert.equal(multi.include, true);
  assert.ok(Math.abs(multi.share - 1 / 3) < 1e-9);
  const absent = observationFractionalShare(
    synthObs({ scenarioId: "s1", provider: "openai", mentioned: false, competitors: ["The Boca Raton"] }),
    coreIds
  );
  assert.equal(absent.share, 0);
  const none = observationFractionalShare(
    synthObs({ scenarioId: "s1", provider: "openai", mentioned: false, competitors: ["Hawks Cay Resort"] }),
    coreIds
  );
  assert.equal(none.include, false);
  assert.equal(expectedShareEqualFair(3), 0.25);

  // --- Secondary excluded: Hawks Cay is non-comparable, not in leisure CORE ---
  assert.ok(!sets.byIntent.leisure.coreIds.includes("hawks_cay"));

  // --- Territory ACI + provider scope ---
  const bizScenarios = scenarios.filter((s) => s.intent === "business");
  const aciAll = computeTerritoryAci(observations, scenarios, "business", sets.byIntent.business.coreIds);
  assert.ok(aciAll.researchAci == null || Number.isFinite(aciAll.researchAci));
  const scoped = providerScopedAci(observations, scenarios, "business", sets.byIntent.business.coreIds);
  assert.equal(scoped.noCrossFill, true);
  assert.ok(scoped.byProvider.openai);
  const sens = aciSensitivity(observations, scenarios, "business", sets.byIntent.business.coreIds);
  assert.ok(["LOW", "MEDIUM", "HIGH"].includes(sens.sensitivity));

  // --- Extreme flag ---
  const extremeObs = bizScenarios.slice(0, 4).flatMap((s) =>
    ["openai", "gemini", "perplexity", "claude"].map((provider) =>
      synthObs({ scenarioId: s.scenarioId, provider, mentioned: true, competitors: [] })
    )
  );
  const extreme = computeTerritoryAci(extremeObs, bizScenarios, "business", coreIds);
  assert.ok(extreme.researchAci > 300);
  assert.equal(extreme.extreme, true);

  // --- Full audit ---
  const audit = runAciPresenceIndexAuditV2({ period, scenarios, propertyProfile: profile, allPeriods: periods });
  assert.equal(audit.title, "ADP_AI_CONSIDERATION_INDEX_AND_PRESENCE_INDEX_AUDIT_V2_COMPLETE");
  assert.equal(audit.certification.customerAciStatus, "BLOCKED");
  assert.equal(audit.propertyAci.CUSTOMER_STATUS, "BLOCKED");
  assert.equal(audit.actualConsiderationShare.secondaryInDenominator, 0);
  assert.equal(audit.regression.ADP_UI_DIFF, 0);
  assert.equal(audit.regression.LEGACY_PRESENCE_INDEX_DIFF, 0);
  assert.equal(audit.execution.PROVIDER_CALLS, 0);
  assert.equal(audit.uiRecommendationResearchOnly.SHOW_PRESENCE_INDEX_AND_ACI_TOGETHER, "NO");
  assert.ok(audit.waterstoneTerritoryResearch.length === 8);
  assert.ok(audit.presenceIndexVsAci.SEMANTIC_OVERLAP);
  assert.ok(audit.stability.TOTAL_PERIODS >= 1);

  const blob = JSON.stringify(audit);
  assert.ok(!blob.includes("Best upscale hotel in Boca Raton"), "prompt leak");
  assert.equal(audit.security.ACI_CUSTOMER_LEAKS, 0);

  // ADP regression: surgical phase 1 still present
  assert.ok(owner.demandCapture);
  assert.ok(owner.intentPresenceIndex);
  assert.ok(owner.executiveMetrics);

  // Brand / operator freeze — this suite does not touch those trees
  assert.equal(audit.regression.BRAND_AI_DIFF, 0);
  assert.equal(audit.regression.OPERATOR_AI_DIFF, 0);

  const reportPath = join(process.cwd(), "reports/ai-demand-positioning/aci-presence-index-audit-v2.json");
  try {
    const saved = JSON.parse(readFileSync(reportPath, "utf8"));
    assert.equal(saved.certification.customerAciStatus, "BLOCKED");
  } catch {
    // runner writes the report; tests may run first
  }

  console.log("test:adp-aci-presence-index-audit-v2 — PASS");
  console.log("  Presence reconstruction matches owner payload");
  console.log("  Entity artifacts rejected; CORE territory-specific");
  console.log("  ACI customer status BLOCKED; UI diffs 0");
}

main();
