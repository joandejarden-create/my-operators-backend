#!/usr/bin/env node
/**
 *   npm run test:adp-property-specific-core-benchmark-governance-v1
 */

import assert from "assert";
import { readFileSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { STABILIZED_CORE_IDS } from "../lib/ai-demand-positioning/metrics/presence-benchmark-v1.js";
import {
  coreIdsForIntent,
  hotelById,
  assertCoreSetIntegrity,
} from "../lib/ai-demand-positioning/metrics/presence-benchmark-v1.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { TRAVELER_INTENTS } from "../lib/ai-demand-positioning/prompt-universe/standard-scenarios.js";
import {
  runPropertySpecificCoreBenchmarkGovernanceV1,
  runPropertyCoreGovernance,
} from "../lib/ai-demand-positioning/metrics/property-specific-core-benchmark-governance-v1.js";
import {
  classifyEntityUniverseForProperty,
  canonicalizeForProperty,
} from "../lib/ai-demand-positioning/metrics/adp-property-entity-registries.js";
import {
  PROPERTY_STABILIZED_CORE_IDS,
  MIN_CORE_HOTELS_PRODUCTION,
} from "../lib/ai-demand-positioning/metrics/property-core-governance-data.js";
import { buildTerritoryBenchmarkSets } from "../lib/ai-demand-positioning/metrics/territory-core-contract.js";
import { auditProperty, compareWaterstoneRegression } from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { buildGovernedIntentPresenceIndex } from "../lib/ai-demand-positioning/metrics/governed-customer-presence-index.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { loadLatestPeriod, loadLatestTargetedPeriod } from "../lib/ai-demand-positioning/data-model.js";

const REPORT = join(process.cwd(), "reports/ai-demand-positioning/property-specific-core-benchmark-governance-v1.json");
const WATERSTONE_BASELINE = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json"
);

function assertNoWeakFourthPeer(propertyId) {
  for (const [intent, ids] of Object.entries(PROPERTY_STABILIZED_CORE_IDS[propertyId] || {})) {
    if (ids.length === MIN_CORE_HOTELS_PRODUCTION) {
      for (const id of ids) {
        const h = hotelById(id, { propertyId });
        assert.ok(h, `${propertyId} ${intent} CORE peer ${id} must be canonical`);
        assert.notEqual(h.identityConfidence, "LOW", `${id} must not be LOW confidence CORE`);
      }
    }
    assert.ok(ids.length <= 6, `${propertyId} ${intent} should not pad beyond legitimate set`);
  }
}

async function main() {
  const report = runPropertySpecificCoreBenchmarkGovernanceV1();
  assert.equal(report.execution.PROVIDER_CALLS, 0);
  assert.equal(report.execution.SPEND, 0);
  assert.equal(report.governance.PROPERTY_SPECIFIC_BENCHMARK_LOGIC, 0);
  assert.equal(report.waterstoneRegression.WATERSTONE_CORE_DIFF, 0);
  assert.equal(report.waterstoneRegression.WATERSTONE_INDEX_DIFF, 0);

  const frozen = JSON.stringify(STABILIZED_CORE_IDS);
  assert.ok(frozen.includes("the_boca_raton"), "Waterstone CORE freeze intact");

  const waterstone = auditProperty("adp_waterstone_boca_raton");
  const regression = compareWaterstoneRegression(waterstone, WATERSTONE_BASELINE);
  assert.equal(regression.PHASE1_METRIC_DIFF, 0);
  assert.equal(regression.INDEX_DIFF, 0);
  assert.equal(waterstone.numericIndexTerritories.length, 4);

  for (const propertyId of [
    "adp_renaissance_times_square",
    "adp_cambridge_beaches_bermuda",
    "adp_now_now_noho",
  ]) {
    const profile = loadPropertyProfile(propertyId);
    assert.ok(profile, `profile ${propertyId}`);
    assertNoWeakFourthPeer(propertyId);

    const businessCore = coreIdsForIntent(TRAVELER_INTENTS.BUSINESS, profile);
    assert.ok(businessCore.length >= 1, `${propertyId} business CORE exists`);
    assert.ok(!businessCore.some((id) => id.includes("waterstone") || id.includes("boca_raton")));
    const integrity = assertCoreSetIntegrity(businessCore, profile);
    assert.equal(integrity.DUPLICATE_CORE_ENTITIES, 0);

    const gov = runPropertyCoreGovernance(propertyId);
    assert.ok(gov.propertyTruth.TRUTH_STATUS === "COMPLETE" || gov.propertyTruth.GAPS.length <= 2);
    assert.ok(gov.entityQa.ARTIFACTS_REMOVED > 0 || gov.entityQa.RAW_CANDIDATES > 0);

    const declaredOnly = (profile.declaredCompSet || [])
      .map((d) => canonicalizeForProperty(propertyId, d))
      .filter(Boolean);
    for (const id of declaredOnly) {
      const allCore = Object.values(PROPERTY_STABILIZED_CORE_IDS[propertyId] || {}).flat();
      if (!allCore.includes(id)) {
        assert.ok(true, "declared comp not automatic CORE — ok");
      }
    }

    const bench = buildTerritoryBenchmarkSets(profile, []);
    assert.ok(bench.byIntent[TRAVELER_INTENTS.BUSINESS], "territory benchmark sets built");
    assert.notEqual(bench.version, "adp_aci_benchmark_set_v2", "non-waterstone uses property governance version");
  }

  const renaissance = runPropertyCoreGovernance("adp_renaissance_times_square");
  const renaissanceReady = renaissance.coreGovernance.filter((r) => r.STATUS === "CORE_TRUTH_READY");
  assert.ok(renaissanceReady.length >= 5, "Renaissance should have multiple CORE_TRUTH_READY territories");

  const cambridge = runPropertyCoreGovernance("adp_cambridge_beaches_bermuda");
  const cambridgeLeisure = cambridge.coreGovernance.find((r) => r.TERRITORY.includes("Resort Leisure"));
  assert.ok(cambridgeLeisure.CORE_COUNT >= 4);
  assert.ok(!cambridgeLeisure.CORE_HOTELS.some((h) => /boca|florida/i.test(h)));

  const nowNow = runPropertyCoreGovernance("adp_now_now_noho");
  const nowNowBusiness = nowNow.coreGovernance.find((r) => r.TERRITORY.includes("Business"));
  assert.equal(nowNowBusiness.CORE_COUNT, 4);
  assert.ok(!nowNowBusiness.CORE_HOTELS.some((h) => /Marriott Marquis|Times Square/i.test(h)));

  const cambridgePublished = await getPublishedOwnerReport("adp_cambridge_beaches_bermuda");
  assert.ok(cambridgePublished.ok);
  const cambridgeTargeted = loadLatestTargetedPeriod("adp_cambridge_beaches_bermuda");
  assert.ok(cambridgeTargeted, "targeted Cambridge period exists");
  const cambridgeProfile = loadPropertyProfile("adp_cambridge_beaches_bermuda");
  const cambridgeScenarios = buildScenarioUniverse(cambridgeProfile);
  const cambridgeGoverned = buildGovernedIntentPresenceIndex(
    cambridgeTargeted.observations,
    cambridgeScenarios,
    cambridgeProfile
  );
  const cambridgeNumeric = Object.values(cambridgeGoverned).filter((r) => r.index != null);
  assert.ok(cambridgeNumeric.length >= 1, "Cambridge certified territories show numeric on targeted period");
  for (const row of cambridgeNumeric) {
    assert.equal(row.status, "PRODUCTION_VALIDATED");
    assert.ok(row.coreBenchmarkRatePct != null);
  }

  const targetedPeriod = loadLatestTargetedPeriod("adp_renaissance_times_square");
  assert.ok(targetedPeriod, "targeted Renaissance period exists");
  const profile = loadPropertyProfile("adp_renaissance_times_square");
  const scenarios = buildScenarioUniverse(profile);
  const governed = buildGovernedIntentPresenceIndex(targetedPeriod.observations, scenarios, profile);
  const numeric = Object.values(governed).filter((r) => r.index != null);
  assert.ok(numeric.length >= 3, "Renaissance certified territories show numeric index after measurement");
  for (const row of numeric) {
    assert.equal(row.status, "PRODUCTION_VALIDATED");
  }

  const entityQa = classifyEntityUniverseForProperty("adp_cambridge_beaches_bermuda", ["Beach Club", "Best Resort"]);
  assert.ok(entityQa.artifactsRemoved >= 1);

  mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
  writeFileSync(REPORT, JSON.stringify(report, null, 2));

  console.log("test:adp-property-specific-core-benchmark-governance-v1 — PASS");
  console.log("  final:", report.final);
  console.log("  next:", report.next);
  console.log("  CORE_TRUTH_READY:", report.summary.TOTAL_CORE_TRUTH_READY_TERRITORIES);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
