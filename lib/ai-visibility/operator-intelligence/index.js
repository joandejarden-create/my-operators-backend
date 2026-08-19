import { assertUniverseLock, OPERATOR_AI_UNIVERSE, PRIMARY_OPERATOR_COUNT } from "./universe.js";
import { OPERATOR_AMBIGUITY_LIST } from "./aliases.js";
import { OPERATOR_DECISION_SCENARIOS } from "./scenarios.js";
import { promptLibraryStats } from "./prompts.js";
import { OPERATOR_SIGNAL_PRESENCE } from "./presence.js";
import { scoreOperatorPresenceValidation } from "./holdout.js";
import { costOperatorFoundationWave } from "./cost-model.js";
import { computeOperatorQuestionsMissing, computeOperatorAllProvidersPresence } from "./questions-missing.js";
import { RAW_GAP_TYPES } from "./gaps.js";
import { operatorTruthAudit } from "./truth.js";
import { operatorAssociationStatus } from "./associations.js";
import { OPERATOR_AI_PRODUCT, OPERATOR_REUSE_INVENTORY } from "./product.js";
import { OPERATOR_ELIGIBILITY_VERSION } from "./eligibility.js";

export * from "./universe.js";
export * from "./aliases.js";
export * from "./scenarios.js";
export * from "./prompts.js";
export * from "./presence.js";
export * from "./holdout.js";
export * from "./cost-model.js";
export * from "./eligibility.js";
export * from "./gaps.js";
export * from "./questions-missing.js";
export * from "./truth.js";
export * from "./associations.js";
export * from "./product.js";
export * from "./presence-validation-wave.js";
export * from "./customer-disclosure.js";
export * from "./comparability.js";
export * from "./scenario-production-policy.js";
export * from "./competitive-gap-gold.js";
export * from "./competitive-intelligence.js";
export * from "./comparability-truth.js";
export * from "./competitive-gap-holdout.js";
export * from "./operator-customer-read-service.js";

export function buildOperatorFoundationSnapshot() {
  assertUniverseLock();
  const presence = scoreOperatorPresenceValidation();
  const cost = costOperatorFoundationWave();
  const qmEmpty = computeOperatorQuestionsMissing({
    operatorId: OPERATOR_AI_UNIVERSE[0].canonicalId,
    promptIds: [],
    observations: [],
  });
  const allProvidersEmpty = computeOperatorAllProvidersPresence([]);
  const assoc = operatorAssociationStatus();
  const truth = operatorTruthAudit();

  return {
    product: OPERATOR_AI_PRODUCT,
    universe: OPERATOR_AI_UNIVERSE.map((o) => ({
      founderName: o.founderName,
      canonicalName: o.canonicalName,
      canonicalId: o.canonicalId,
      monitoredScope: o.monitoredScope,
      identityStatus: o.identityStatus,
      measurementEligible: o.measurementEligible ? "YES" : "NO",
      truthCoverage: o.truthCoverage,
      operatorLens: o.operatorLens,
    })),
    primaryOperatorCount: PRIMARY_OPERATOR_COUNT,
    ambiguityList: OPERATOR_AMBIGUITY_LIST,
    reuse: OPERATOR_REUSE_INVENTORY,
    scenarios: OPERATOR_DECISION_SCENARIOS,
    prompts: promptLibraryStats(),
    presence: {
      signal: OPERATOR_SIGNAL_PRESENCE,
      ...presence,
      precision: presence.holdout.precision,
      recall: presence.holdout.recall,
      f1: presence.holdout.f1,
      falsePositives: presence.holdout.fp,
      falseNegatives: presence.holdout.fn,
    },
    cost,
    questionsMissing: { status: qmEmpty.status, denominator: "prompt with >=1 comparable provider observation" },
    allProviders: {
      status: allProvidersEmpty.status,
      missingProviderZero: "NO",
    },
    competitiveGap: {
      rawGapTypes: Object.values(RAW_GAP_TYPES),
      eligibilityLayer: OPERATOR_ELIGIBILITY_VERSION,
      commercialInterpretation: "TRUE_COMPETITIVE_GAP / EXPECTED_POSITIONING_DIFFERENCE / OUT_OF_SCOPE",
      status: "DIAGNOSTIC_ONLY",
      clientPromoted: 0,
    },
    truth,
    associations: assoc,
    sources: {
      citationContractReused: true,
      ownedDomainMap: OPERATOR_AI_UNIVERSE.map((o) => ({
        canonicalId: o.canonicalId,
        domain: o.domain,
        parentDomain: o.parentDomain || null,
      })),
      sourceCausalityRule: "PASS",
    },
    stability: { contractReused: true },
    longitudinal: {
      brandComponentsReused: [
        "measurement period",
        "prompt × provider grain",
        "common cohort",
        "idempotency",
        "cost ledger",
        "quality gate",
      ],
      operatorLongitudinalReady: "PARTIAL",
      scheduler: "OFF",
    },
    guards: {
      CENSUS_READS: 0,
      DATAFORSEO_CALLS: 0,
      RECOMMENDATION_METRICS: 0,
      OWNER_AI_BUILD: 0,
      BRAND_UI_WORK: 0,
      RAW_RESPONSES_IN_AIRTABLE: 0,
      PER_OPERATOR_PROVIDER_EXECUTION: 0,
    },
  };
}
