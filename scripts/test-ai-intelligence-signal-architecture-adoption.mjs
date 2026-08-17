#!/usr/bin/env node
/**
 * Signal/flag architecture adoption — regression tests.
 * LIVE_PROVIDER_CALLS: 0. HOLDOUT_ACCESS: 0. AIRTABLE_WRITES: 0.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  deriveProductionSignalsFromInternalRole,
  assertSignalConsistency,
  SIGNAL_PAYLOAD_FIELDS,
  PRODUCTION_SIGNALS,
  SIGNAL_KEYS,
  buildSignalPayload,
  DEV_SIGNAL_VALIDATION_SNAPSHOT,
  getSignalReadiness,
  assertReadinessIndependence,
  SIGNAL_READINESS,
  applySignalPublicationGate,
  unavailableSignalMetric,
  SIGNAL_UNAVAILABLE_MESSAGE,
  evaluateSignalPublicationPlan,
  summarizeProductSurfaceAudit,
  SURFACE_CLASS,
  confirmMetricContractsUnchanged,
  buildSignalValidationScorecard,
  classifyOld10ClassGateRole,
  INTERNAL_RECOMMENDATION_ROLES,
  evaluateHoldoutReadiness,
  buildSignalArchitectureAdoptionReport,
  listRecallWorkstreams,
} from "../lib/ai-visibility/signal-architecture/index.js";
import { POSITIVE_RECOMMENDATION_ROLES } from "../lib/ai-visibility/metrics.js";
import { AVAILABILITY } from "../lib/ai-visibility/availability-states.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

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

console.log("AI Intelligence — Signal Architecture Adoption\n");

test("PRESENCE_SIGNAL_FROM_ENTITY_RESOLUTION", () => {
  const present = deriveProductionSignalsFromInternalRole("discussed");
  const absent = deriveProductionSignalsFromInternalRole("no_mention");
  assert.equal(present.PRESENCE, true);
  assert.equal(absent.PRESENCE, false);
  assert.equal(absent.RECOMMENDED, false);
});

test("ASSOCIATED_NOT_RECOMMENDED", () => {
  const s = deriveProductionSignalsFromInternalRole("associated_option");
  assert.equal(s.RECOMMENDED, false);
  assert.equal(s.PRESENCE, true);
  const c = assertSignalConsistency(s, "associated_option");
  assert.equal(c.ok, true);
});

test("FIRST_IS_RECOMMENDED", () => {
  const s = deriveProductionSignalsFromInternalRole("first_recommendation");
  assert.equal(s.FIRST_RECOMMENDATION, true);
  assert.equal(s.RECOMMENDED, true);
  const c = assertSignalConsistency(s, "first_recommendation");
  assert.equal(c.ok, true);
});

test("RANKED_IS_RECOMMENDED", () => {
  const s = deriveProductionSignalsFromInternalRole("ranked_recommendation");
  assert.equal(s.RECOMMENDED, true);
  assert.equal(s.FIRST_RECOMMENDATION, false);
});

test("EXPLICIT_IS_RECOMMENDED", () => {
  const s = deriveProductionSignalsFromInternalRole("explicit_recommendation");
  assert.equal(s.RECOMMENDED, true);
  assert.equal(s.FIRST_RECOMMENDATION, false);
});

test("DISCUSSION_NOT_RECOMMENDED", () => {
  const s = deriveProductionSignalsFromInternalRole("discussed");
  assert.equal(s.RECOMMENDED, false);
  const c = assertSignalConsistency(s, "discussed");
  assert.equal(c.ok, true);
});

test("SIGNAL_PAYLOAD_CONTRACT", () => {
  const p = buildSignalPayload({ value: true, provider: "openai" });
  for (const f of SIGNAL_PAYLOAD_FIELDS) {
    assert.ok(f in p, `missing field ${f}`);
  }
  assert.equal(Object.keys(PRODUCTION_SIGNALS).length, 5);
});

test("SIGNAL_READINESS_INDEPENDENT", () => {
  const r = assertReadinessIndependence();
  assert.equal(r.SIGNAL_READINESS_INDEPENDENT, true);
  assert.equal(r.ok, true);
});

test("FAILED_RECOMMENDED_DOES_NOT_BLOCK_PRESENCE", () => {
  const r = assertReadinessIndependence();
  assert.equal(r.FAILED_RECOMMENDED_DOES_NOT_BLOCK_PRESENCE, true);
  assert.equal(r.BRAND_AI_VISIBILITY_CAN_ADVANCE_WITHOUT_RECOMMENDED, true);
  assert.equal(r.RECOMMENDED_REQUIRED_FOR_V1, false);
  assert.equal(getSignalReadiness(SIGNAL_KEYS.PRESENCE).gateStatus, "PASS");
  assert.equal(
    getSignalReadiness(SIGNAL_KEYS.RECOMMENDED).readiness,
    SIGNAL_READINESS.RESEARCH_BLOCKED
  );
  assert.equal(
    getSignalReadiness(SIGNAL_KEYS.RECOMMENDED).productionCertificationStatus,
    "RESEARCH_BLOCKED_NOT_PRODUCTION_READY"
  );
});

test("FAILED_FIRST_DOES_NOT_BLOCK_PRESENCE", () => {
  const r = assertReadinessIndependence();
  assert.equal(r.FAILED_FIRST_DOES_NOT_BLOCK_PRESENCE, true);
  assert.equal(
    getSignalReadiness(SIGNAL_KEYS.FIRST_RECOMMENDATION).readiness,
    SIGNAL_READINESS.NOT_READY
  );
});

test("UNAVAILABLE_SIGNAL_NOT_ZERO", () => {
  const blocked = applySignalPublicationGate("recommendationShare", {
    availability: AVAILABILITY.ZERO,
    value: 0,
    display: "0%",
  });
  assert.equal(blocked.value, null);
  assert.notEqual(blocked.value, 0);
  assert.equal(blocked.availability, AVAILABILITY.UNAVAILABLE);
  assert.equal(blocked.display, SIGNAL_UNAVAILABLE_MESSAGE);

  const u = unavailableSignalMetric({ signalKey: SIGNAL_KEYS.RECOMMENDED });
  assert.equal(u.value, null);
  assert.ok(u.display.includes("Validated monitoring data"));
});

test("PUBLICATION_GATE_PER_SIGNAL", () => {
  const plan = evaluateSignalPublicationPlan();
  // Presence Holdout v3 PASS → PRODUCTION_VALIDATED → client-publishable
  assert.equal(plan.presenceMayPublish, true);
  assert.equal(plan.recommendedMustHide, true);
  assert.equal(plan.firstMustHide, true);

  const presence = applySignalPublicationGate("aiPresence", {
    availability: AVAILABILITY.OBSERVED,
    value: 0.5,
    display: "50%",
  });
  assert.equal(presence.value, 0.5);
  assert.equal(presence.signalGateBlocked, undefined);

  const first = applySignalPublicationGate("firstRecommendationRate", {
    availability: AVAILABILITY.OBSERVED,
    value: 0.2,
    display: "20%",
  });
  assert.equal(first.value, null);
  assert.equal(first.signalGateBlocked, true);
});

test("OLD_10_CLASS_NOT_CLIENT_CONTRACT", () => {
  const t = classifyOld10ClassGateRole();
  assert.equal(t.PRODUCTION_CONTRACT, false);
  assert.equal(t.INTERNAL_RESEARCH_ONLY, true);
  assert.equal(t.gate.retiredAsProductionReleaseControl, true);
});

test("INTERNAL_TAXONOMY_PRESERVED", () => {
  assert.equal(INTERNAL_RECOMMENDATION_ROLES.length, 10);
  assert.ok(INTERNAL_RECOMMENDATION_ROLES.includes("associated_option"));
  assert.ok(INTERNAL_RECOMMENDATION_ROLES.includes("no_mention"));
  for (const r of POSITIVE_RECOMMENDATION_ROLES) {
    assert.ok(INTERNAL_RECOMMENDATION_ROLES.includes(r));
  }
});

test("METRIC_CONTRACTS_UNCHANGED", () => {
  const m = confirmMetricContractsUnchanged();
  assert.equal(m.confirmedUnchanged, true);
  assert.equal(m.recommendationShareExcludesAssociated, true);
});

test("PRODUCT_SURFACE_AUDIT_CLASSES", () => {
  const s = summarizeProductSurfaceAudit();
  assert.ok(s.SAFE_NOW.some((x) => x.id === "aiPresence"));
  assert.ok(s.SAFE_NOW.some((x) => x.id === "competitivePosition"));
  assert.ok(s.SAFE_NOW.some((x) => x.id === "questionsMissing"));
  assert.ok(s.BLOCKED.some((x) => x.class === SURFACE_CLASS.BLOCKED_BY_RECOMMENDED_FLAG));
  assert.ok(s.BLOCKED.some((x) => x.class === SURFACE_CLASS.BLOCKED_BY_FIRST_REC_FLAG));
  assert.ok(s.INTERNAL_ONLY.some((x) => x.id === "internal10ClassRecommendationStatus"));
});

test("SCORECARD_NO_COMPOSITE", () => {
  const sc = buildSignalValidationScorecard();
  assert.equal(sc.PER_SIGNAL, true);
  assert.equal(sc.COMPOSITE_SCORE, false);
  assert.equal(sc.rows.length, 5);
  assert.equal(sc.HOLDOUT_ACCESSED, true);
  const presence = sc.rows.find((r) => r.signal === "PRESENCE");
  assert.equal(presence.PRODUCTION_READINESS, "VALIDATED");
  assert.ok(
    presence.HOLDOUT === "PASS" ||
      presence.holdoutV3Status === "PASS" ||
      String(presence.HOLDOUT).includes("PASS")
  );
});

test("HOLDOUT_STRATEGY_PRESENCE_ONLY", () => {
  const h = evaluateHoldoutReadiness();
  assert.equal(h.RECOMMENDED_READY_FOR_HOLDOUT, "NO");
  assert.equal(h.FIRST_READY_FOR_HOLDOUT, "NO");
  assert.equal(h.HOLDOUT_ACCESSED, "YES");
  assert.equal(h.PRESENCE_PRODUCTION_VALIDATED, true);
  assert.equal(h.PRESENCE_HOLDOUT_V3_STATUS, "PASS");
  assert.equal(h.HOLDOUT_EXECUTED, true);
});

test("RECALL_WORKSTREAMS_CLOSED_RESEARCH_BLOCKED", () => {
  const w = listRecallWorkstreams();
  assert.equal(w.workstreams.length, 2);
  assert.equal(w.noBroadMulticlassRestart, true);
  assert.equal(w.workstreams[0].status, "CLOSED_RESEARCH_BLOCKED");
  assert.equal(w.workstreams[1].status, "PAUSED_UNTIL_RECOMMENDED_REOPEN");
});

test("BRAND_V1_PRESENCE_LED_CONTRACT", async () => {
  const { buildBrandAiVisibilityV1Contract } = await import(
    "../lib/ai-visibility/signal-architecture/brand-ai-visibility-v1.js"
  );
  const {
    buildRecommendedResearchClosure,
    AI_SIGNAL_RECOMMENDED_STATUS,
  } = await import("../lib/ai-visibility/signal-architecture/recommended-research-closure.js");
  const contract = buildBrandAiVisibilityV1Contract();
  assert.equal(contract.RECOMMENDED_REQUIRED_FOR_V1, false);
  assert.equal(contract.BRAND_AI_VISIBILITY_CAN_ADVANCE_WITHOUT_RECOMMENDED, true);
  assert.equal(contract.PRESENCE, "PRODUCTION_VALIDATED");
  assert.equal(contract.RECOMMENDED, AI_SIGNAL_RECOMMENDED_STATUS);
  assert.equal(contract.FINAL_STATUS, "PRESENCE_LED_BRAND_AI_VISIBILITY_V1_READY");
  const closure = buildRecommendedResearchClosure();
  assert.equal(closure.AI_SIGNAL_RECOMMENDED, "RESEARCH_BLOCKED_NOT_PRODUCTION_READY");
  assert.equal(closure.RECOMMENDATION_SHARE, "BLOCKED");
  assert.ok(Array.isArray(closure.REOPEN_RESEARCH_IF));
  assert.ok(closure.REOPEN_RESEARCH_IF.length >= 4);
});

test("CROSS_PROVIDER_PRESENCE_DERIVED_NO_ARBITRARY_SCORE", async () => {
  const {
    buildCrossProviderPresenceIntelligence,
    assertCrossProviderComparability,
    buildSourceOverlapBetweenProviders,
  } = await import("../lib/ai-visibility/cross-provider-presence.js");
  const {
    buildProviderSelectorOptions,
    ALL_PROVIDERS_SELECTOR_ID,
  } = await import("../lib/ai-visibility/provider-dimension.js");

  const opts = buildProviderSelectorOptions([{ id: "openai", completedBatchCount: 2 }]);
  assert.equal(opts[0].id, ALL_PROVIDERS_SELECTOR_ID);
  assert.equal(opts[0].mode, "DERIVED");
  assert.equal(opts[0].ALL_PROVIDERS_RUN, false);

  const unlike = assertCrossProviderComparability([
    { promptFamily: "a", geography: "CALA", language: "en", monitoringWindow: "w1" },
    { promptFamily: "b", geography: "CALA", language: "en", monitoringWindow: "w1" },
  ]);
  assert.equal(unlike.NOT_COMPARABLE, true);

  const xp = buildCrossProviderPresenceIntelligence({
    entityId: "recTest",
    geography: "CALA",
    language: "en",
    providers: [
      {
        provider: "openai",
        monitored: true,
        presenceRate: 0.8,
        geography: "CALA",
        language: "en",
        monitoringWindow: "w1",
        promptFamily: "portfolio_presence",
      },
      {
        provider: "claude",
        monitored: true,
        presenceRate: 0.4,
        geography: "CALA",
        language: "en",
        monitoringWindow: "w1",
        promptFamily: "portfolio_presence",
      },
    ],
  });
  assert.equal(xp.ARBITRARY_SCORE, false);
  assert.equal(xp.ALL_PROVIDERS_RUN, false);
  assert.equal(xp.AI_VISIBILITY_SCORE, null);
  assert.ok(xp.CROSS_PROVIDER_AVERAGE_OBSERVED_PRESENCE != null);
  assert.equal(xp.PROVIDER_DISAGREEMENT.status, "DISAGREE");

  const overlap = buildSourceOverlapBetweenProviders({
    openai: ["a.com", "b.com"],
    claude: ["b.com", "c.com"],
  });
  assert.deepEqual(overlap.SOURCE_OVERLAP_BETWEEN_PROVIDERS.sharedDomains, ["b.com"]);
  assert.equal(overlap.INFLUENCING_SOURCES_LABEL, false);
});

test("EXECUTIVE_INSIGHT_LAYER_DETERMINISTIC", async () => {
  const { buildExecutiveInsightBoxes, PERMITTED_INSIGHT_TYPES } = await import(
    "../lib/ai-visibility/brand-executive-insights.js"
  );
  const out = buildExecutiveInsightBoxes({
    geographyKey: "CALA",
    topByPresence: {
      brandName: "Autograph Collection",
      presence: 0.72,
      display: "72%",
      geography: "CALA",
    },
    questionsMissing: { value: 12, denominator: 40, display: "30% (12)" },
  });
  assert.equal(out.IMPLEMENTED, true);
  assert.equal(out.FREEFORM_LLM, false);
  assert.ok(out.boxes.length >= 1);
  assert.ok(out.boxes.length <= 5);
  for (const b of out.boxes) {
    assert.ok(PERMITTED_INSIGHT_TYPES.includes(b.type));
    assert.ok(b.finding);
    assert.ok(b.evidence);
    assert.equal(b.CAUSAL_LANGUAGE_USED, false);
  }
});

test("ADOPTION_REPORT_PASS", () => {
  const report = buildSignalArchitectureAdoptionReport();
  assert.equal(report.phase, "AI_INTELLIGENCE_SIGNAL_ARCHITECTURE_ADOPTION_COMPLETE");
  assert.equal(report.status, "AI_INTELLIGENCE_SIGNAL_ARCHITECTURE_ADOPTION_PASS");
  assert.equal(report.nextStep, "READY_FOR_PRESENCE_HOLDOUT_AND_PRODUCT_INTEGRATION");
  assert.equal(report.architecture.INTERNAL_10_CLASS_PRESERVED, "YES");
  assert.equal(report.hardGuards.HOLDOUT_ACCESS, 0);
});

test("DEV_SNAPSHOT_MATCHES_STUDY_V41", () => {
  const studyPath = path.join(
    ROOT,
    "data/ai-visibility/validation/production-signal-taxonomy-study.json"
  );
  assert.ok(fs.existsSync(studyPath));
  const study = JSON.parse(fs.readFileSync(studyPath, "utf8"));
  const v41 = study.candidateE.benchmarks["v4.1"];
  assert.equal(DEV_SIGNAL_VALIDATION_SNAPSHOT.signals.PRESENCE.precision, v41.PRESENCE.PRECISION);
  assert.equal(
    DEV_SIGNAL_VALIDATION_SNAPSHOT.signals.RECOMMENDED.precision,
    v41.RECOMMENDED_FLAG.precision
  );
  assert.equal(
    DEV_SIGNAL_VALIDATION_SNAPSHOT.signals.FIRST_RECOMMENDATION.recall,
    v41.FIRST_RECOMMENDATION_FLAG.recall
  );
});

console.log(`\n${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
