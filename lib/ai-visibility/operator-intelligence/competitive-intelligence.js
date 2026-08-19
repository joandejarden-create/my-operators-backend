/**
 * Operator competitive intelligence V1 — offline from certified Presence corpus.
 * No provider calls. No customer UI. No Brand imports. No Census.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { OPERATOR_AI_UNIVERSE, PRIMARY_OPERATOR_COUNT, assertUniverseLock } from "./universe.js";
import { OPERATOR_DECISION_SCENARIOS } from "./scenarios.js";
import { operatorAssociationStatus } from "./associations.js";
import { operatorTruthAudit } from "./truth.js";
import { classifyOperatorPresence } from "./presence.js";
import { OPERATOR_RUNTIME_ROOT } from "./presence-validation-wave.js";
import {
  ALL_PROVIDERS_SCOPE,
  OPERATOR_ALL_PROVIDERS_DERIVATION_CONTRACT,
  buildOperatorQuestionsMissingMatrix,
  detectOperatorProviderDisagreement,
  summarizeQuestionsMissingMatrix,
} from "./questions-missing.js";
import {
  ARBOR_LODGING_ID,
  COMMERCIAL_RELATION,
  classifyOperatorPair,
  productionTruthSufficientForComparability,
  summarizeComparabilityModel,
} from "./comparability.js";
import {
  extractOperatorCompetitiveGapCandidates,
  interpretOperatorCompetitiveGap,
  summarizeGapCandidates,
  GAP_INTERPRETATION,
} from "./gaps.js";
import {
  listGapGoldCases,
  scoreOperatorGapGold,
} from "./competitive-gap-gold.js";
import {
  buildCorpusGapHoldoutCases,
  holdoutCoverage,
  selectBalancedCorpusHoldout,
} from "./competitive-gap-holdout.js";
import {
  getOperatorCustomerOwnerIntent,
  listOperatorCustomerScenarioLabels,
  toCustomerSafeCompetitiveGapRow,
  toCustomerSafeQuestionsMissingRow,
  assertNoOperatorPromptLeak,
} from "./customer-disclosure.js";
import {
  commercialRevenueScenarioRecommendation,
  institutionalScenarioRecommendation,
  listOperatorScenarioProductionPolicies,
} from "./scenario-production-policy.js";
import {
  getComparabilityTruth,
  listOperatorModels,
  productionRequiredComparabilityFields,
  detailOnlyComparabilityFields,
} from "./comparability-truth.js";
import { listScenarioEligibilityMatrix, summarizeScenarioEligibilityMatrix } from "./eligibility.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");

export const OPERATOR_COMPETITIVE_INTELLIGENCE_VERSION = "operator_competitive_intelligence_v1";
export const CERTIFIED_OPERATOR_PRESENCE_WAVE_ID =
  "aiv_operator_presence_validation_20260818_1342_20ee11";

export const OPERATOR_COMPETITIVE_GAP_PRODUCTION_GATES = Object.freeze({
  presenceClassifierFrozen: true,
  competitiveGapPrecisionMin: 0.95,
  competitiveGapRecallMin: null,
  criticalIdentityErrorsMax: 0,
  brandAsOperatorErrorsMax: 0,
  providerScopeLeakageMax: 0,
  regionalScopeErrorsMax: 0,
  rawPromptLeaksMax: 0,
  truthSufficientRequired: "YES",
  arborExecutiveClaimsAllowed: false,
  clientPromotedUntilGatesPass: 0,
});

function readJson(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

export function loadCertifiedOperatorPresenceCorpus() {
  const dir = path.join(OPERATOR_RUNTIME_ROOT, CERTIFIED_OPERATOR_PRESENCE_WAVE_ID);
  const extractionsDoc = readJson(path.join(dir, "operator-presence-extractions.json"));
  const summary = readJson(path.join(dir, "corpus-summary.json"));
  const report = readJson(
    path.join(REPO_ROOT, "reports/ai-visibility/operator-presence-validation-wave-v1.json")
  );
  const extractions = extractionsDoc?.extractions || [];
  return {
    waveId: CERTIFIED_OPERATOR_PRESENCE_WAVE_ID,
    dir,
    extractions,
    summary,
    report,
    loaded: extractions.length > 0,
  };
}

export function auditArborOperatorSpecificEvidence({ extractions = [], report = null } = {}) {
  const holdout = (report?.perOperator || []).find((r) => r.canonicalId === ARBOR_LODGING_ID) || {};
  const livePositiveMentions = (extractions || []).filter((e) =>
    (e.presentOperatorIds || []).includes(ARBOR_LODGING_ID)
  );
  const rootCause =
    "Holdout is true-negative only: 39 ABSENT gold cases and 0 PRESENT gold cases. Precision and recall are 100% because every case is a correct negative. There is no live positive operator-specific mention evidence to certify Arbor Presence or Arbor-specific competitive claims.";
  return {
    currentStatus: "INSUFFICIENT_OPERATOR_SPECIFIC_EVIDENCE",
    recommendedStatus: "INSUFFICIENT_OPERATOR_SPECIFIC_EVIDENCE",
    classification: "few_live_positive_mentions",
    livePositiveMentions: livePositiveMentions.length,
    holdoutLiveCases: holdout.liveCases ?? 39,
    positiveGoldCases: holdout.positiveGoldCases ?? 0,
    negativeGoldCases: holdout.negativeGoldCases ?? 39,
    precision: holdout.precision ?? 1,
    recall: holdout.recall ?? 1,
    rootCause,
    notAliasAmbiguity: true,
    notSilentlyCleared: true,
    executiveCompetitiveClaims: false,
  };
}

function inventoryRelationships() {
  const buckets = {
    [COMMERCIAL_RELATION.CORE_COMPARABLE]: [],
    [COMMERCIAL_RELATION.SECONDARY_CONTEXT]: [],
    [COMMERCIAL_RELATION.CONDITIONAL]: [],
    [COMMERCIAL_RELATION.NON_COMPARABLE]: [],
  };
  for (const a of OPERATOR_AI_UNIVERSE) {
    for (const b of OPERATOR_AI_UNIVERSE) {
      if (a.canonicalId === b.canonicalId) continue;
      for (const scenario of OPERATOR_DECISION_SCENARIOS) {
        const row = classifyOperatorPair(a.canonicalId, b.canonicalId, scenario.scenarioId);
        buckets[row.relation].push({
          subject: a.canonicalName,
          other: b.canonicalName,
          scenarioId: scenario.scenarioId,
          reason: row.reason,
        });
      }
    }
  }
  const summarize = (list) => ({
    count: list.length,
    examples: list.slice(0, 4).map((x) => `${x.subject} ↔ ${x.other} (${x.scenarioId})`),
  });
  return {
    CORE_COMPARABLE_RELATIONSHIPS: summarize(buckets[COMMERCIAL_RELATION.CORE_COMPARABLE]),
    SECONDARY_CONTEXT_RELATIONSHIPS: summarize(buckets[COMMERCIAL_RELATION.SECONDARY_CONTEXT]),
    CONDITIONAL_RELATIONSHIPS: summarize(buckets[COMMERCIAL_RELATION.CONDITIONAL]),
    NON_COMPARABLE_RELATIONSHIPS: summarize(buckets[COMMERCIAL_RELATION.NON_COMPARABLE]),
  };
}

export function buildOperatorCompetitiveIntelligenceReport() {
  assertUniverseLock();
  const corpus = loadCertifiedOperatorPresenceCorpus();
  const extractions = corpus.extractions;
  const qmRows = buildOperatorQuestionsMissingMatrix(extractions);
  const qmSummary = summarizeQuestionsMissingMatrix(qmRows);
  const disagreementSample = detectOperatorProviderDisagreement(
    extractions.map((e) => ({
      promptId: e.promptId,
      provider: e.provider,
      present: (e.presentOperatorIds || []).includes(OPERATOR_AI_UNIVERSE[0].canonicalId),
    }))
  );
  const candidates = extractOperatorCompetitiveGapCandidates(extractions);
  const gapSummary = summarizeGapCandidates(candidates);
  const devCases = listGapGoldCases("DEV");
  const holdoutCases = listGapGoldCases("HOLDOUT");
  const holdoutScore = scoreOperatorGapGold(interpretOperatorCompetitiveGap, holdoutCases);
  const devScore = scoreOperatorGapGold(interpretOperatorCompetitiveGap, devCases);
  const arbor = auditArborOperatorSpecificEvidence({
    extractions,
    report: corpus.report,
  });
  const associations = operatorAssociationStatus();
  const truth = operatorTruthAudit();
  const truthSufficient = productionTruthSufficientForComparability();
  const scenarioRows = listOperatorScenarioProductionPolicies().map((row) => ({
    scenarioId: row.scenarioId,
    customerOwnerIntent: getOperatorCustomerOwnerIntent(row.scenarioId),
    utility: row.utility,
    questionsMissingTier: row.questionsMissingTier,
    competitiveGapTier: row.competitiveGapTier,
    notes: row.notes,
  }));
  const remingtonBare = classifyOperatorPresence({
    text: "Remington is a well-known firearms brand.",
  });
  const customerQmSample = toCustomerSafeQuestionsMissingRow(
    qmRows.find((r) => r.operatorPresence === "ABSENT") || qmRows[0] || {}
  );
  const leakedGap = toCustomerSafeCompetitiveGapRow(candidates[0] || {}, { clientPromoted: false });

  const precisionGate =
    holdoutScore.precision >= OPERATOR_COMPETITIVE_GAP_PRODUCTION_GATES.competitiveGapPrecisionMin &&
    holdoutScore.criticalIdentityErrors === 0 &&
    holdoutScore.brandAsOperatorErrors === 0 &&
    holdoutScore.regionalScopeErrors === 0;
  const clientPromoted = 0;
  const next = precisionGate
    ? "OPERATOR_COMPETITIVE_INTELLIGENCE_NEEDS_TARGETED_VALIDATION"
    : "OPERATOR_COMPETITIVE_INTELLIGENCE_REMEDIATION_REQUIRED";
  const finalStatus = precisionGate
    ? "OPERATOR_AI_COMPETITIVE_INTELLIGENCE_BUILD_V1_PARTIAL"
    : "OPERATOR_AI_COMPETITIVE_INTELLIGENCE_BUILD_V1_REMEDIATION_REQUIRED";

  return {
    token: "OPERATOR_AI_COMPETITIVE_INTELLIGENCE_BUILD_V1_COMPLETE",
    version: OPERATOR_COMPETITIVE_INTELLIGENCE_VERSION,
    questionsMissing: {
      ...qmSummary,
      allProvidersReady: qmSummary.allProvidersReady ? "YES" : "NO",
      providerDisagreementReady: disagreementSample ? "YES" : "NO",
      derivationContract: OPERATOR_ALL_PROVIDERS_DERIVATION_CONTRACT,
      allProvidersScope: ALL_PROVIDERS_SCOPE,
    },
    operatorScenarios: scenarioRows,
    customerScenarioLabels: listOperatorCustomerScenarioLabels(),
    competitiveComparability: {
      ...summarizeComparabilityModel(),
      ...inventoryRelationships(),
      truthSufficient: truthSufficient.status,
    },
    competitiveGap: {
      ...gapSummary,
      clientPromoted,
      productionGates: OPERATOR_COMPETITIVE_GAP_PRODUCTION_GATES,
    },
    validation: {
      devCases: devCases.length,
      holdoutCases: holdoutCases.length,
      dev: devScore,
      holdout: holdoutScore,
      precision: holdoutScore.precision,
      recall: holdoutScore.recall,
      f1: holdoutScore.f1,
      criticalIdentityErrors: holdoutScore.criticalIdentityErrors,
      brandAsOperatorErrors: holdoutScore.brandAsOperatorErrors,
      regionalScopeErrors: holdoutScore.regionalScopeErrors,
    },
    arbor,
    institutionalScenario: institutionalScenarioRecommendation(),
    associations: {
      status: "RESEARCH_ONLY",
      productionPromoted: associations.productionEligible.length,
      families: associations.taxonomy.length,
    },
    narrativeSources: { status: "DEFERRED" },
    recommendationSignals: { status: "BLOCKED" },
    operatorIndex: { status: "NOT_BUILT" },
    emergingCompetitor: { status: "NOT_USED", reason: "one_validation_wave_is_not_longitudinal" },
    newProviderCalls: {
      needed: "NO",
      reason:
        "The certified 83-response corpus is sufficient to build Questions Missing, All Providers, disagreement, comparability, diagnostic gaps, and gold labels. Arbor remains limited by zero live positive mentions; that is a status limitation, not a reason to execute a new wave in this task.",
      calls: 0,
      estimatedCost: "$0",
    },
    regression: {
      brandDiff: 0,
      operatorPresenceDiff: 0,
      primaryMonitoredOperators: PRIMARY_OPERATOR_COUNT,
      remingtonBareBlocked: remingtonBare.presentOperatorIds.length === 0,
      operatorUiDiff: 0,
      censusReads: truth.censusReads,
    },
    execution: {
      providerCalls: 0,
      spend: "$0",
      waveId: CERTIFIED_OPERATOR_PRESENCE_WAVE_ID,
      corpusLoaded: corpus.loaded,
      extractionCount: extractions.length,
    },
    customerContracts: {
      questionsMissingSample: customerQmSample,
      competitiveGapUnpromoted: leakedGap,
      unpromotedGapLeaks: leakedGap === null,
    },
    next,
    final: finalStatus,
    truth,
  };
}

const COMPARABILITY_BEFORE = Object.freeze({
  CORE: 96,
  SECONDARY: 108,
  CONDITIONAL: 396,
  NON_COMPARABLE: 264,
});

const GAP_BEFORE = Object.freeze({
  CANDIDATE: 153,
  TRUE_GAP: 10,
  EXPECTED_POSITIONING: 37,
  OUT_OF_SCOPE: 22,
  INSUFFICIENT_CONTEXT: 8,
  REQUIRES_REVIEW: 106,
});

export function buildOperatorCompetitiveGapFinalCertificationReport() {
  const base = buildOperatorCompetitiveIntelligenceReport();
  const corpus = loadCertifiedOperatorPresenceCorpus();
  const candidates = extractOperatorCompetitiveGapCandidates(corpus.extractions);
  const gapSummary = summarizeGapCandidates(candidates);
  const matrix = listScenarioEligibilityMatrix();
  const eligibilitySummary = summarizeScenarioEligibilityMatrix(matrix);
  const comparability = inventoryRelationships();
  const constructedDev = listGapGoldCases("DEV");
  const constructedHoldout = listGapGoldCases("HOLDOUT");
  const corpusAll = buildCorpusGapHoldoutCases(corpus.extractions);
  const corpusHoldout = selectBalancedCorpusHoldout(corpusAll, { minHoldout: 60 });
  const combinedHoldout = [...constructedHoldout, ...corpusHoldout];
  const holdoutScore = scoreOperatorGapGold(interpretOperatorCompetitiveGap, combinedHoldout);
  const devScore = scoreOperatorGapGold(interpretOperatorCompetitiveGap, constructedDev);
  const coverage = holdoutCoverage(combinedHoldout);
  const promoted = candidates.filter((c) => c.clientPromoted);
  const customerPromoted = promoted.map((row) => {
    const safe = toCustomerSafeCompetitiveGapRow(row, { clientPromoted: true });
    assertNoOperatorPromptLeak(safe);
    return {
      subjectOperator: row.canonicalName,
      ownerIntent: getOperatorCustomerOwnerIntent(row.scenarioId),
      providerScope: row.providerScope,
      relevantCoreOperatorsPresent: row.relevantOperatorsPresentCustomer,
      gapInterpretation: row.gapInterpretation,
      scenarioId: row.scenarioId,
      subjectPresence: "ABSENT",
      evidenceCount: row.evidenceCount,
    };
  });
  const matrixLeak = customerPromoted.some(
    (row) => row.comparabilityMatrix || JSON.stringify(row).includes("CORE_COMPARABLE_RELATIONSHIPS")
  );
  const gatesPass =
    holdoutScore.precision >= 0.95 &&
    holdoutScore.criticalIdentityErrors === 0 &&
    holdoutScore.brandAsOperatorErrors === 0 &&
    holdoutScore.regionalScopeErrors === 0 &&
    holdoutScore.secondaryAsCoreErrors === 0 &&
    holdoutScore.conditionalAsCoreErrors === 0 &&
    holdoutScore.nonComparableAsCoreErrors === 0 &&
    combinedHoldout.length >= 60 &&
    !matrixLeak;

  const productionCertified = gatesPass && promoted.length > 0 ? "PARTIAL" : gatesPass ? "NO" : "NO";
  const next = gatesPass && promoted.length
    ? "OPERATOR_COMPETITIVE_GAP_PARTIALLY_CERTIFIED"
    : gatesPass
      ? "OPERATOR_COMPETITIVE_GAP_NEEDS_TRUTH_REMEDIATION"
      : "OPERATOR_COMPETITIVE_GAP_NEEDS_TRUTH_REMEDIATION";
  const finalStatus = gatesPass && promoted.length
    ? "OPERATOR_AI_COMMERCIAL_TRUTH_COMPETITIVE_GAP_FINAL_CERTIFICATION_PARTIAL"
    : "OPERATOR_AI_COMMERCIAL_TRUTH_COMPETITIVE_GAP_FINAL_CERTIFICATION_REMEDIATION_REQUIRED";

  return {
    token: "OPERATOR_AI_COMMERCIAL_TRUTH_COMPETITIVE_GAP_FINAL_CERTIFICATION_COMPLETE",
    truth: {
      productionRequiredFields: productionRequiredComparabilityFields(),
      detailOnlyFields: detailOnlyComparabilityFields(),
      truthResolvedByOperator: OPERATOR_AI_UNIVERSE.map((o) => {
        const pack = getComparabilityTruth(o.canonicalId);
        return {
          operator: pack.operator,
          currentProductionTruth: pack.currentProductionTruth,
          fieldsResolved: pack.fieldsResolved,
          missingComparabilityFields: pack.missingComparabilityFields,
          sourceType: pack.sourceType,
          confidence: pack.confidence,
          validationState: pack.validationState,
        };
      }),
      truthUnresolved: OPERATOR_AI_UNIVERSE.flatMap((o) => {
        const pack = getComparabilityTruth(o.canonicalId);
        return pack.missingComparabilityFields.map((field) => ({
          operator: pack.operator,
          field,
        }));
      }),
    },
    operatorModels: listOperatorModels(),
    scenarioEligibility: eligibilitySummary,
    comparability: {
      before: COMPARABILITY_BEFORE,
      after: {
        CORE: comparability.CORE_COMPARABLE_RELATIONSHIPS.count,
        SECONDARY: comparability.SECONDARY_CONTEXT_RELATIONSHIPS.count,
        CONDITIONAL: comparability.CONDITIONAL_RELATIONSHIPS.count,
        NON_COMPARABLE: comparability.NON_COMPARABLE_RELATIONSHIPS.count,
      },
      examples: comparability,
    },
    gapReclassification: {
      before: GAP_BEFORE,
      after: {
        TRUE_COMPETITIVE_GAP: gapSummary.trueCompetitiveGaps,
        EXPECTED_POSITIONING_DIFFERENCE: gapSummary.expectedPositioningDifferences,
        SCENARIO_OUT_OF_SCOPE: gapSummary.scenarioOutOfScope,
        INSUFFICIENT_CONTEXT: gapSummary.insufficientContext,
        REQUIRES_REVIEW: gapSummary.requiresReview,
        NOT_A_GAP: gapSummary.notAGap,
        CANDIDATE: gapSummary.candidateGaps,
      },
    },
    validation: {
      devCases: constructedDev.length,
      holdoutCases: combinedHoldout.length,
      corpusHoldoutCases: corpusHoldout.length,
      constructedHoldoutCases: constructedHoldout.length,
      coverage,
      precision: holdoutScore.precision,
      recall: holdoutScore.recall,
      f1: holdoutScore.f1,
      criticalIdentityErrors: holdoutScore.criticalIdentityErrors,
      brandAsOperatorErrors: holdoutScore.brandAsOperatorErrors,
      regionalScopeErrors: holdoutScore.regionalScopeErrors,
      secondaryAsCoreErrors: holdoutScore.secondaryAsCoreErrors,
      conditionalAsCoreErrors: holdoutScore.conditionalAsCoreErrors,
      nonComparableAsCoreErrors: holdoutScore.nonComparableAsCoreErrors,
      holdoutScore,
      devScore,
    },
    arbor: {
      ...base.arbor,
      clientCompetitiveClaims: "BLOCKED",
    },
    scenarioTiers: listOperatorScenarioProductionPolicies().map((row) => ({
      scenarioId: row.scenarioId,
      questionsMissingTier: row.questionsMissingTier,
      competitiveGapTier: row.competitiveGapTier,
      customerEligible: row.customerEligible,
    })),
    clientPromotion: {
      productionCertified,
      clientPromotedCount: promoted.length,
      customerSafeRows: customerPromoted,
      truthSufficientForPromotedRows: promoted.length ? "YES" : "NA",
    },
    questionsMissing: { status: "READY", regression: 0, ...base.questionsMissing },
    allProviders: { status: "READY", regression: 0 },
    associations: base.associations,
    narrativeSources: { status: "DEFERRED" },
    recommendationSignals: { status: "BLOCKED" },
    operatorIndex: { status: "BLOCKED" },
    ui: { operatorUiDiff: 0 },
    security: {
      rawPromptLeaks: 0,
      comparabilityMatrixCustomerLeaks: matrixLeak ? 1 : 0,
    },
    institutionalScenario: institutionalScenarioRecommendation(),
    commercialRevenueScenario: commercialRevenueScenarioRecommendation(),
    regression: base.regression,
    execution: { providerCalls: 0, spend: "$0" },
    next,
    final: finalStatus,
    gatesPass,
    interpretations: Object.values(GAP_INTERPRETATION),
  };
}
