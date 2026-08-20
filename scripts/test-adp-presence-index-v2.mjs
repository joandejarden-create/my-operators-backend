#!/usr/bin/env node
/**
 *   npm run test:adp-presence-index-v2
 */

import assert from "assert";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload, computeIntentPresenceIndexLegacy } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { TRAVELER_INTENTS } from "../lib/ai-demand-positioning/prompt-universe/standard-scenarios.js";
import {
  computeScopePresenceRates,
  presenceIndexFromRates,
  computePresenceIndexV2ForIntent,
  peerAppearsInObservation,
} from "../lib/ai-demand-positioning/metrics/presence-index-v2.js";
import {
  coreIdsForIntent,
  STABILIZED_CORE_IDS,
  MIN_CORE_PEERS_PRODUCTION,
  assertCoreSetIntegrity,
} from "../lib/ai-demand-positioning/metrics/presence-benchmark-v1.js";
import { classifyCandidateForTerritory, COMPETITIVE_CLASSES } from "../lib/ai-demand-positioning/metrics/territory-core-contract.js";
import { runPresenceIndexV2Audit } from "../lib/ai-demand-positioning/metrics/presence-index-v2-audit.js";
import { reconstructIntentPresenceIndex } from "../lib/ai-demand-positioning/metrics/presence-index-reconstruction.js";
import { computeDemandCaptureIndex } from "../lib/ai-demand-positioning/intelligence/demand-capture-index.js";

const PROPERTY_ID = "adp_waterstone_boca_raton";

function obs({ scenarioId, provider, mentioned, competitors }) {
  return {
    observationId: `o_${scenarioId}_${provider}`,
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

  // zero-presence CORE peer included
  const coreIds = ["the_boca_raton", "renaissance_boca_raton", "marriott_boca_raton"];
  const rates = computeScopePresenceRates(
    [
      obs({ scenarioId: "a", provider: "openai", mentioned: true, competitors: ["The Boca Raton"] }),
      obs({ scenarioId: "b", provider: "openai", mentioned: false, competitors: ["The Boca Raton"] }),
    ],
    coreIds
  );
  assert.equal(rates.peerRates.find((p) => p.entityId === "marriott_boca_raton").rate, 0);
  assert.ok(rates.zeroPresencePeers.includes("marriott_boca_raton"));
  assert.ok(rates.coreBenchmarkMean < 1);

  // secondary excluded: hawks_cay not in leisure CORE
  assert.ok(!coreIdsForIntent("leisure").includes("hawks_cay"));
  assert.ok(!coreIdsForIntent("leisure").includes("colony_hotel_delray"));

  // declared not automatic CORE
  assert.equal(
    classifyCandidateForTerritory("hilton_boca_raton_suites", TRAVELER_INTENTS.COUPLES).role,
    COMPETITIVE_CLASSES.NON_COMPARABLE
  );
  assert.ok(!coreIdsForIntent("couples").includes("hilton_boca_raton_suites"));
  assert.ok(!coreIdsForIntent("leisure").includes("hilton_boca_raton_suites"));

  // no score cap
  const uncapped = presenceIndexFromRates(0.9, 0.2);
  assert.ok(uncapped.index > 200);
  assert.equal(uncapped.cap, "NONE");

  // zero benchmark
  const zeroBench = presenceIndexFromRates(0.5, 0);
  assert.equal(zeroBench.status, "BENCHMARK_NOT_ESTABLISHED");
  assert.equal(zeroBench.index, null);

  // provider-specific + missing ≠ zero
  const biz = scenarios.filter((s) => s.intent === "business").slice(0, 6);
  const mixed = [];
  for (const s of biz) {
    mixed.push(obs({ scenarioId: s.scenarioId, provider: "openai", mentioned: true, competitors: ["The Boca Raton"] }));
    mixed.push(obs({ scenarioId: s.scenarioId, provider: "gemini", mentioned: false, competitors: ["Renaissance Boca Raton"] }));
  }
  const v2 = computePresenceIndexV2ForIntent(mixed, scenarios, "business", { coreIds });
  assert.equal(v2.byProvider.openai.included, true);
  assert.equal(v2.byProvider.gemini.included, true);
  assert.equal(v2.byProvider.claude.included, false);
  assert.equal(v2.byProvider.claude.missingNotZero, true);
  assert.equal(v2.byProvider.claude.index, null);
  assert.ok(v2.allProviders.includedProviders.includes("openai"));
  assert.ok(!v2.allProviders.includedProviders.includes("claude"));
  assert.equal(v2.allProviders.method, "A_EQUAL_MEAN_OF_PROVIDER_RATES_THEN_INDEX");

  // thin CORE adventure
  assert.equal(STABILIZED_CORE_IDS.adventure.length, 3);
  assert.ok(STABILIZED_CORE_IDS.adventure.length < MIN_CORE_PEERS_PRODUCTION);

  // integrity
  for (const intent of Object.keys(STABILIZED_CORE_IDS)) {
    const integ = assertCoreSetIntegrity(STABILIZED_CORE_IDS[intent]);
    assert.equal(integ.DUPLICATE_CORE_ENTITIES, 0);
    assert.equal(integ.GENERIC_CORE_ENTITIES, 0);
    assert.equal(integ.BRAND_ONLY_CORE_ENTITIES, 0);
  }

  const owner = buildOwnerPayload(period, scenarios, profile, { allPeriods: periods });
  const live = reconstructIntentPresenceIndex(
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
  for (const intent of Object.keys(legacy)) {
    assert.strictEqual(legacy[intent].index, live[intent].index, `legacy rollback mismatch ${intent}`);
  }
  assert.ok(owner.intentPresenceIndex);
  assert.ok(!owner.presenceIndexV2);
  assert.ok(!owner.aiConsiderationIndex);
  for (const row of Object.values(owner.intentPresenceIndex)) {
    if (row.index != null) {
      assert.ok(row.subjectRatePct != null, "numeric index requires subject rate");
      assert.ok(row.coreBenchmarkRatePct != null, "numeric index requires CORE benchmark");
    }
  }

  const audit = runPresenceIndexV2Audit({ period, scenarios, propertyProfile: profile, allPeriods: periods });
  assert.equal(audit.regression.ADP_UI_DIFF, 0);
  assert.equal(audit.regression.LIVE_PRESENCE_INDEX_DIFF, 0);
  assert.equal(audit.regression.PHASE1_METRIC_DIFF, 0);
  assert.equal(audit.execution.PROVIDER_CALLS, 0);
  assert.equal(audit.presenceIndexV2Contract.SCORE_CAP, "NONE");
  assert.equal(audit.presenceIndexV2Contract.ZERO_PRESENCE_CORE_PEERS_INCLUDED, "YES");
  assert.equal(audit.aciProgress.customerAciStatus, "BLOCKED");
  assert.ok(audit.thinBenchmarks.WELLNESS);
  assert.ok(audit.thinBenchmarks.ADVENTURE);
  assert.equal(audit.longitudinal.TOTAL_PERIODS, periods.length);
  assert.ok(JSON.stringify(audit).includes("adp_presence_benchmark_v1"));
  assert.ok(!JSON.stringify(audit).includes("Best upscale hotel in Boca Raton"));

  // peer appear helper
  assert.equal(
    peerAppearsInObservation({ competitorsMentioned: ["The Boca Raton"] }, "the_boca_raton"),
    true
  );

  console.log("test:adp-presence-index-v2 — PASS");
}

main();
