/**
 * Unit tests: ADP status dimensions + provider completeness policy.
 */
import assert from "assert";
import {
  PROPERTY_QA_STATUS,
  PERIOD_CERTIFICATION_STATUS,
  CURRENT_PERIOD_CLASS,
  PUBLISH_ELIGIBILITY,
  isCertificationEraPeriod,
  mapEngineToPropertyQaStatus,
  resolvePeriodCertificationStatus,
  resolveCurrentPeriodClass,
  resolvePublishEligibility,
} from "../lib/ai-demand-positioning/certification/adp-status-dimensions-v1.js";
import {
  evaluateProviderCompletenessGate,
  maxAllowedFailures,
  ADP_PROVIDER_COMPLETENESS_POLICY_NAME,
} from "../lib/ai-demand-positioning/certification/adp-provider-completeness-policy-v1.js";
import { PROVIDERS } from "../lib/ai-demand-positioning/data-model.js";

function makeObs(scenarioCount, providerOverrides = {}) {
  const obs = [];
  for (const p of PROVIDERS) {
    const n = providerOverrides[p]?.successful ?? scenarioCount;
    const timeouts = providerOverrides[p]?.timedOut ?? 0;
    for (let i = 0; i < n; i++) {
      obs.push({
        scenarioId: `s${i}`,
        provider: p,
        mentioned: false,
        parsed: true,
      });
    }
    for (let i = 0; i < timeouts; i++) {
      obs.push({
        scenarioId: `s${n + i}`,
        provider: p,
        error: "timeout",
        timeout: true,
      });
    }
  }
  return { observations: obs, scenarioCount };
}

// --- Status dimensions ---
assert.strictEqual(
  mapEngineToPropertyQaStatus("CERTIFIED", [], []),
  PROPERTY_QA_STATUS.PASS
);
assert.strictEqual(
  mapEngineToPropertyQaStatus("QA_REVIEW_REQUIRED", [], [{ code: "X" }]),
  PROPERTY_QA_STATUS.REVIEW
);
assert.strictEqual(
  mapEngineToPropertyQaStatus("QA_FAILED", [{ code: "X" }], []),
  PROPERTY_QA_STATUS.FAIL
);

assert.strictEqual(
  isCertificationEraPeriod({ globalCertificationEngineVersion: "adp_global_certification_engine_v1" }),
  true
);
assert.strictEqual(isCertificationEraPeriod({ certificationStatus: "CERTIFIED" }), false);

const legacyPass = resolvePeriodCertificationStatus({
  period: { periodId: "p1", certificationStatus: "CERTIFIED" },
  publishedManifest: { latestPeriodId: "p1", certificationStatus: "CERTIFIED" },
  engineWouldCertify: "CERTIFIED",
  propertyQaStatus: PROPERTY_QA_STATUS.PASS,
});
assert.strictEqual(legacyPass, PERIOD_CERTIFICATION_STATUS.LEGACY_QA_PASS);

const eraCert = resolvePeriodCertificationStatus({
  period: {
    periodId: "p2",
    certificationStatus: "CERTIFIED",
    certified: true,
    globalCertificationEngineVersion: "adp_global_certification_engine_v1",
  },
  publishedManifest: {
    latestPeriodId: "p2",
    certificationStatus: "CERTIFIED",
    globalCertificationEngineVersion: "adp_global_certification_engine_v1",
  },
  engineWouldCertify: "CERTIFIED",
  propertyQaStatus: PROPERTY_QA_STATUS.PASS,
});
assert.strictEqual(eraCert, PERIOD_CERTIFICATION_STATUS.CERTIFIED);

assert.strictEqual(
  resolveCurrentPeriodClass(
    { globalCertificationEngineVersion: "adp_global_certification_engine_v1" },
    { latestPeriodId: "p2", globalCertificationEngineVersion: "adp_global_certification_engine_v1" }
  ),
  CURRENT_PERIOD_CLASS.CERTIFICATION_ERA_OFFICIAL
);

process.env.ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH = "1";
assert.strictEqual(
  resolvePublishEligibility({
    currentPeriodClass: CURRENT_PERIOD_CLASS.CERTIFICATION_ERA_OFFICIAL,
    periodCertificationStatus: PERIOD_CERTIFICATION_STATUS.CERTIFIED,
    officialPeriodPublished: true,
  }),
  PUBLISH_ELIGIBILITY.ALLOWED
);
assert.strictEqual(
  resolvePublishEligibility({
    currentPeriodClass: CURRENT_PERIOD_CLASS.LEGACY_OFFICIAL,
    periodCertificationStatus: PERIOD_CERTIFICATION_STATUS.LEGACY_UNCERTIFIED,
    officialPeriodPublished: true,
  }),
  PUBLISH_ELIGIBILITY.GRANDFATHERED
);
assert.strictEqual(
  resolvePublishEligibility({
    currentPeriodClass: CURRENT_PERIOD_CLASS.CERTIFICATION_ERA_OFFICIAL,
    periodCertificationStatus: PERIOD_CERTIFICATION_STATUS.QA_REVIEW_REQUIRED,
    officialPeriodPublished: false,
  }),
  PUBLISH_ELIGIBILITY.BLOCKED
);

// --- Completeness policy ---
assert.strictEqual(maxAllowedFailures(252), 25);
assert.strictEqual(maxAllowedFailures(63), 6);

const yotelLike = makeObs(63, { claude: { successful: 62, timedOut: 1 } });
const gateOk = evaluateProviderCompletenessGate(yotelLike);
assert.strictEqual(gateOk.policyName, ADP_PROVIDER_COMPLETENESS_POLICY_NAME);
assert.strictEqual(gateOk.expected, 252);
assert.strictEqual(gateOk.successful, 251);
assert.strictEqual(gateOk.timedOut, 1);
assert.strictEqual(gateOk.materialFailure, false);
assert.strictEqual(gateOk.providerImbalance.length, 0);

const badClaude = makeObs(63, { claude: { successful: 10, timedOut: 53 } });
const gateBad = evaluateProviderCompletenessGate(badClaude);
assert.strictEqual(gateBad.materialFailure, true);
assert.ok(gateBad.providerImbalance.some((p) => p.provider === "claude"));

console.log("test-adp-certification-inventory-reconciliation-v1: PASS");
