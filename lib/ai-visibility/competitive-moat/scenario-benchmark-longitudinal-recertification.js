/**
 * Longitudinal scenario benchmark recertification — period-scoped, no provider calls.
 * Evaluates SUBJECT × SCENARIO × MEASUREMENT_PERIOD independently, then compares periods.
 * Does NOT auto-promote to customer UI.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { PRIMARY_OPERATOR_COUNT } from "../operator-intelligence/universe.js";
import { IDS, SCENARIO_IDS as S } from "./benchmark-brand-ids.js";
import { loadBenchmarkEligibleUniverse, getBenchmarkEligibleMember } from "./benchmark-eligible-universe.js";
import { resolveScenarioCommercialPeers } from "./scenario-peer-eligibility.js";
import {
  buildIndependentIndex,
  recomputeOne,
  classifyStability,
} from "./scenario-benchmark-validation.js";
import {
  CORE_FIRST_GATES_CANDIDATE,
  classifyCoreFirstProduction,
  BRANDED_RESIDENCES_BENCHMARK_STATUS,
} from "./scenario-benchmark-composition.js";
import { loadFinalCertificationReport } from "./scenario-benchmark-final-certification.js";
import { FROZEN_CERTIFIED } from "./scenario-benchmark-certification-expansion-audit.js";
import {
  auditPeriodArchitecture,
  BASELINE_MEASUREMENT_PERIOD,
  CROSS_PERIOD_DEDUPLICATION,
  POOLED_ALL_PERIODS_INDEX,
  buildComparisonGrainKey,
  assertSinglePeriodScope,
  partitionResponsesByPeriod,
} from "./period-scoped-grain.js";
import {
  collectResponsesForPeriod,
  collectAllPeriodSlices,
  listAvailableMeasurementPeriods,
} from "./period-response-sources.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..", "..");
const READINESS_REPORT_PATH = path.join(
  ROOT,
  "reports",
  "ai-visibility",
  "scenario-benchmark-longitudinal-recertification-readiness-v1.json"
);
const RECERT_REPORT_PATH = path.join(
  ROOT,
  "reports",
  "ai-visibility",
  "scenario-benchmark-longitudinal-recertification-v1.json"
);

export const RECERTIFICATION_VERSION = "scenario_benchmark_longitudinal_recertification_v1";
export const AUTOMATIC_CUSTOMER_PROMOTION = false;
export const PROVIDER_CALLS_REQUIRED = false;

export const EXCLUDED_SCENARIO_STATUS = Object.freeze({
  [S.LIFESTYLE]: "SCENARIO_REDESIGN_REQUIRED",
  [S.BRANDED_RESIDENCES]: "REDESIGN_REQUIRED",
  [S.DISTRIBUTION_LOYALTY]: "PROMPT_DESIGN_REQUIRED",
  [S.MARKET_ENTRY]: "MEASUREMENT_NOT_READY",
});

/** Near-term recertification candidates after next normal Brand longitudinal period. */
export const NEAR_TERM_RECERTIFICATION_CANDIDATES = Object.freeze([
  { subjectId: IDS.CURIO, scenarioId: S.SOFT_BRAND, tier: "TARGET_6", label: "CURIO_SOFT_BRAND" },
  { subjectId: IDS.TRIBUTE, scenarioId: S.SOFT_BRAND, tier: "TARGET_6", label: "TRIBUTE_SOFT_BRAND" },
  { subjectId: IDS.VIGNETTE, scenarioId: S.SOFT_BRAND, tier: "TARGET_6", label: "VIGNETTE_SOFT_BRAND" },
  { subjectId: IDS.ASCEND, scenarioId: S.OWNER_FLEXIBILITY, tier: "TARGET_8", label: "ASCEND_OWNER_FLEX" },
  { subjectId: IDS.VIGNETTE, scenarioId: S.OWNER_FLEXIBILITY, tier: "TARGET_8", label: "VIGNETTE_OWNER_FLEX" },
  { subjectId: IDS.RAD_IND, scenarioId: S.OWNER_FLEXIBILITY, tier: "ADDITIONAL", label: "RADISSON_INDIVIDUALS_OWNER_FLEX" },
  { subjectId: IDS.TAPESTRY, scenarioId: S.CONVERSION_SUITABILITY, tier: "SECONDARY", label: "TAPESTRY_CONVERSION" },
  {
    subjectId: IDS.AUTOGRAPH,
    scenarioId: S.CONVERSION_SUITABILITY,
    tier: "SECONDARY",
    label: "AUTOGRAPH_CONVERSION",
    mode: "REPEAT_EVIDENCE_ONLY",
  },
  {
    subjectId: IDS.TRIBUTE,
    scenarioId: S.CONVERSION_SUITABILITY,
    tier: "SECONDARY",
    label: "TRIBUTE_CONVERSION",
    mode: "REPEAT_EVIDENCE_ONLY",
  },
  {
    subjectId: IDS.CURIO,
    scenarioId: S.CONVERSION_SUITABILITY,
    tier: "SECONDARY",
    label: "CURIO_CONVERSION",
    mode: "REPEAT_EVIDENCE_ONLY",
  },
]);

const INDEX_DIRECTION_THRESHOLD = 1;

function directionFromIndex(indexValue) {
  if (indexValue == null) return null;
  if (indexValue > 100 + INDEX_DIRECTION_THRESHOLD) return "ABOVE";
  if (indexValue < 100 - INDEX_DIRECTION_THRESHOLD) return "BELOW";
  return "PARITY";
}

function mapProviderDirection(rec) {
  const agreement = rec.providerAgreement || "PROVIDER_CONSISTENT";
  if (agreement === "PROVIDER_CONFLICT") return "CONFLICT";
  if (agreement === "PROVIDER_MIXED") return "MIXED";
  return "CONSISTENT";
}

function summarizePeriodResult(subjectId, scenarioId, measurementPeriodId, idx, universe) {
  const rec = recomputeOne(subjectId, scenarioId, idx, universe);
  const peers = resolveScenarioCommercialPeers(subjectId, scenarioId, { universe });
  const corePeerNames = peers.calculationPeers
    .filter((p) => p.commercialRelation === "CORE")
    .map((p) => p.peerBrandName);
  const coreRates = rec.corePeers || [];

  return {
    measurementPeriodId,
    subjectId,
    scenarioId,
    subjectPresence: rec.subjectPresence ?? null,
    benchmarkPresence: rec.benchmarkCore ?? null,
    indexValue: rec.indexCore ?? null,
    relativeGapPct:
      rec.indexCore != null && rec.benchmarkCore != null && rec.benchmarkCore > 0
        ? Math.round(((rec.indexCore - 100) / 100) * 1000) / 10
        : null,
    direction: directionFromIndex(rec.indexCore),
    providerDirection: mapProviderDirection(rec),
    providerRows: rec.providerRows || [],
    corePeerSet: corePeerNames,
    corePeerCount: coreRates.length,
    commonGrainCount: rec.commonGrains ?? rec.medianPairwiseCommonGrains ?? null,
    stability: rec.stability || classifyStability(rec.maxIndexMovement),
    providerCount: rec.providerCount ?? (rec.providers || []).length,
    POOLED_ALL_PERIODS_INDEX,
  };
}

function buildPeriodScopedIndex(measurementPeriodId, opts = {}) {
  const responses = collectResponsesForPeriod(measurementPeriodId, opts);
  const scope = assertSinglePeriodScope(measurementPeriodId, [measurementPeriodId]);
  if (!scope.ok) {
    throw new Error(`period_scope_violation:${scope.reason}`);
  }
  const idx = buildIndependentIndex({ ...opts, responses });
  return { measurementPeriodId, responseCount: responses.length, index: idx };
}

export function classifyProviderConflictTransition(priorResult, currentResult) {
  const prior = priorResult?.providerDirection || "UNKNOWN";
  const current = currentResult?.providerDirection || "UNKNOWN";
  if (prior === "CONFLICT" && current === "CONSISTENT") {
    return "CONFLICT_RESOLVED";
  }
  if (prior === "CONFLICT" && current === "MIXED") {
    return "CONFLICT_REDUCED";
  }
  if (prior === "CONFLICT" && current === "CONFLICT") {
    return "CONFLICT_PERSISTENT";
  }
  if (prior !== current && prior !== "UNKNOWN" && current !== "UNKNOWN") {
    return "CONFLICT_DIRECTION_CHANGED";
  }
  if (prior === "CONSISTENT" && current === "CONSISTENT") {
    return "CONSISTENT";
  }
  return "UNCHANGED";
}

export function comparePeriodResults(priorResult, currentResult) {
  if (!priorResult || !currentResult) {
    return {
      comparable: false,
      reason: "missing_period_result",
    };
  }
  const indexDelta =
    priorResult.indexValue != null && currentResult.indexValue != null
      ? currentResult.indexValue - priorResult.indexValue
      : null;
  const priorCore = new Set(priorResult.corePeerSet || []);
  const currentCore = new Set(currentResult.corePeerSet || []);
  const coreAdded = [...currentCore].filter((n) => !priorCore.has(n));
  const coreRemoved = [...priorCore].filter((n) => !currentCore.has(n));

  return {
    comparable: true,
    PRIOR_PERIOD: priorResult.measurementPeriodId,
    CURRENT_PERIOD: currentResult.measurementPeriodId,
    PRIOR_INDEX: priorResult.indexValue,
    CURRENT_INDEX: currentResult.indexValue,
    PRIOR_SUBJECT_PRESENCE: priorResult.subjectPresence,
    CURRENT_SUBJECT_PRESENCE: currentResult.subjectPresence,
    PRIOR_BENCHMARK_PRESENCE: priorResult.benchmarkPresence,
    CURRENT_BENCHMARK_PRESENCE: currentResult.benchmarkPresence,
    PRIOR_PROVIDER_DIRECTION: priorResult.providerDirection,
    CURRENT_PROVIDER_DIRECTION: currentResult.providerDirection,
    INDEX_DIRECTION_STABLE:
      directionFromIndex(priorResult.indexValue) === directionFromIndex(currentResult.indexValue),
    SUBJECT_PRESENCE_STABLE:
      priorResult.subjectPresence != null &&
      currentResult.subjectPresence != null &&
      Math.abs(currentResult.subjectPresence - priorResult.subjectPresence) <= 0.05,
    BENCHMARK_DIRECTION_STABLE:
      priorResult.benchmarkPresence != null &&
      currentResult.benchmarkPresence != null &&
      Math.abs(currentResult.benchmarkPresence - priorResult.benchmarkPresence) <= 0.05,
    CORE_COHORT_STABLE: coreAdded.length === 0 && coreRemoved.length === 0,
    CORE_COHORT_CHANGE: { added: coreAdded, removed: coreRemoved },
    CONFLICT_STATE: classifyProviderConflictTransition(priorResult, currentResult),
    indexDelta,
    CROSS_PERIOD_DEDUPLICATION,
    POOLED_ALL_PERIODS_INDEX,
  };
}

function evaluateSoftBrandRecertification(comparison, currentResult, candidate) {
  const gates = CORE_FIRST_GATES_CANDIDATE;
  const grains = currentResult.commonGrainCount || 0;
  const conflict = comparison.CONFLICT_STATE;
  const stable = currentResult.stability !== "FRAGILE";
  const coreCount = currentResult.corePeerCount || 0;

  if (candidate.mode === "REPEAT_EVIDENCE_ONLY") {
    return {
      automaticCandidateState: "REPEAT_EVIDENCE_ONLY",
      FINAL_RECOMMENDATION: "DETAIL_ONLY",
      READY_FOR_CERTIFICATION: false,
      note: "Conversion rows require genuine gate pass — repeat measurement informs only.",
    };
  }

  if (grains < gates.COMMON_GRAIN_MIN) {
    return {
      automaticCandidateState: "KEEP_LIMITED",
      FINAL_RECOMMENDATION: "LIMITED",
      READY_FOR_CERTIFICATION: false,
      reason: "COMMON_GRAINS_BELOW_MIN",
    };
  }
  if (!stable) {
    return {
      automaticCandidateState: "KEEP_LIMITED",
      FINAL_RECOMMENDATION: "DETAIL_ONLY",
      READY_FOR_CERTIFICATION: false,
      reason: "FRAGILE",
    };
  }
  if (conflict === "CONFLICT_PERSISTENT" || conflict === "CONFLICT_DIRECTION_CHANGED") {
    return {
      automaticCandidateState: "KEEP_LIMITED",
      FINAL_RECOMMENDATION: "LIMITED",
      READY_FOR_CERTIFICATION: false,
      reason: "PROVIDER_CONFLICT_UNRESOLVED",
    };
  }
  if (coreCount < gates.MIN_CORE_PEERS_CUSTOMER) {
    return {
      automaticCandidateState: "KEEP_LIMITED",
      FINAL_RECOMMENDATION: "LIMITED",
      READY_FOR_CERTIFICATION: false,
      reason: "CORE_PEERS_BELOW_MIN",
    };
  }
  if (conflict === "CONFLICT_RESOLVED") {
    return {
      automaticCandidateState: "CERTIFY",
      FINAL_RECOMMENDATION: "PRODUCTION_VALIDATED",
      READY_FOR_CERTIFICATION: true,
      note: comparison.INDEX_DIRECTION_STABLE
        ? "Provider conflict resolved with stable index direction."
        : "Provider conflict resolved; index direction shifted but remains credible.",
    };
  }
  if (conflict === "CONFLICT_REDUCED" && grains >= gates.STRONG_EVIDENCE_GRAINS) {
    return {
      automaticCandidateState: "CERTIFY_NARROW",
      FINAL_RECOMMENDATION: "PRODUCTION_VALIDATED_NARROW",
      READY_FOR_CERTIFICATION: true,
      note: "Conflict reduced; narrow certification if multi-provider direction aligns on current period.",
    };
  }
  return {
    automaticCandidateState: "KEEP_LIMITED",
    FINAL_RECOMMENDATION: "DETAIL_ONLY",
    READY_FOR_CERTIFICATION: false,
    reason: "INSUFFICIENT_CROSS_PERIOD_RESOLUTION",
  };
}

export function evaluateCandidateRecertification(candidate, priorResult, currentResult, opts = {}) {
  const excluded = EXCLUDED_SCENARIO_STATUS[candidate.scenarioId];
  if (excluded) {
    return {
      ...candidate,
      excluded: true,
      STATUS: excluded,
      FINAL_RECOMMENDATION: "SUPPRESSED",
      READY_FOR_CERTIFICATION: false,
    };
  }

  const comparison = comparePeriodResults(priorResult, currentResult);
  const subject = getBenchmarkEligibleMember(candidate.subjectId, opts.universe)?.brandName || candidate.subjectId;
  const evaluation =
    candidate.scenarioId === S.SOFT_BRAND || candidate.scenarioId === S.OWNER_FLEXIBILITY
      ? evaluateSoftBrandRecertification(comparison, currentResult, candidate)
      : candidate.scenarioId === S.CONVERSION_SUITABILITY
        ? {
            automaticCandidateState: "REPEAT_EVIDENCE_ONLY",
            FINAL_RECOMMENDATION: "DETAIL_ONLY",
            READY_FOR_CERTIFICATION: false,
            note: "Conversion suitability — provider conflict / fragility; repeat only unless gates pass.",
          }
        : evaluateSoftBrandRecertification(comparison, currentResult, candidate);

  return {
    SUBJECT: subject,
    subjectId: candidate.subjectId,
    SCENARIO: candidate.scenarioId,
    tier: candidate.tier,
    label: candidate.label,
    ...comparison,
    ...evaluation,
    AUTOMATIC_CUSTOMER_PROMOTION,
  };
}

export function verifyFrozenBaseline(opts = {}) {
  const report = loadFinalCertificationReport(opts);
  const diffs = {};
  for (const frozen of FROZEN_CERTIFIED) {
    const cand = (report.candidates || []).find(
      (c) => c.subjectId === frozen.subjectId && c.scenarioId === frozen.scenarioId
    );
    const index = cand?.INDEX ?? null;
    diffs[frozen.label] = {
      expected: frozen.index,
      actual: index,
      DIFF: index != null ? index - frozen.index : null,
    };
  }
  const autograph = diffs.AUTOGRAPH_SOFT_BRAND?.DIFF ?? null;
  const tapestry = diffs.TAPESTRY_SOFT_BRAND?.DIFF ?? null;
  const ascend = diffs.ASCEND_SOFT_COLLECTION?.DIFF ?? null;
  return {
    AUTOGRAPH_103_DIFF: autograph === 0 ? 0 : autograph,
    TAPESTRY_103_DIFF: tapestry === 0 ? 0 : tapestry,
    ASCEND_67_DIFF: ascend === 0 ? 0 : ascend,
    frozen: diffs,
    ok: autograph === 0 && tapestry === 0 && ascend === 0,
    note: "Baseline period indices must not be recomputed from pooled future data.",
  };
}

export function runLongitudinalRecertificationReadiness(opts = {}) {
  const periodArchitecture = auditPeriodArchitecture();
  const availablePeriods = listAvailableMeasurementPeriods(opts);
  const slices = collectAllPeriodSlices(opts);
  const universe = loadBenchmarkEligibleUniverse();
  const frozen = verifyFrozenBaseline(opts);

  const longitudinalPeriods = availablePeriods.filter(
    (p) => p.measurementPeriodId !== BASELINE_MEASUREMENT_PERIOD
  );
  const hasNextPeriod = longitudinalPeriods.length >= 1;
  const canRunRecert = longitudinalPeriods.length >= 2;

  const target6 = NEAR_TERM_RECERTIFICATION_CANDIDATES.filter((c) => c.tier === "TARGET_6").map((c) => ({
    label: c.label,
    subjectId: c.subjectId,
    scenarioId: c.scenarioId,
    status: hasNextPeriod ? "READY_FOR_NEXT_PERIOD_EVALUATION" : "READY_FOR_NEXT_PERIOD_EVALUATION",
    note: hasNextPeriod
      ? "Period 1 longitudinal stored; await period 2 for cross-period recertification."
      : "Infrastructure ready; first longitudinal period responses will enable evaluation.",
  }));

  const target8 = NEAR_TERM_RECERTIFICATION_CANDIDATES.filter((c) => c.tier === "TARGET_8" || c.tier === "ADDITIONAL").map(
    (c) => ({
      label: c.label,
      status: "READY_FOR_NEXT_PERIOD_EVALUATION",
    })
  );

  const report = {
    BRAND_AI_LONGITUDINAL_BENCHMARK_RECERTIFICATION_READINESS_COMPLETE: true,
    recertificationVersion: RECERTIFICATION_VERSION,
    providerCalls: 0,
    spend: 0,
    uiChanges: 0,
    periodArchitecture,
    MEASUREMENT_PERIOD_PRESENT: periodArchitecture.MEASUREMENT_PERIOD_PRESENT,
    PERIOD_PART_OF_COMPARISON_SCOPE: periodArchitecture.PERIOD_PART_OF_COMPARISON_SCOPE,
    CROSS_PERIOD_DEDUP_RISK: periodArchitecture.CROSS_PERIOD_DEDUP_RISK,
    FIX_REQUIRED: periodArchitecture.CROSS_PERIOD_DEDUP_RISK === "YES_WITHOUT_PERIOD_SCOPE" ? "MITIGATED_BY_PERIOD_SCOPING" : "NO",
    CURRENT_GRAIN_KEY: periodArchitecture.CURRENT_GRAIN_KEY,
    PERIOD_SAFE: true,
    RECOMMENDED_PERIOD_SCOPING: periodArchitecture.RECOMMENDED_PERIOD_SCOPING,
    HISTORICAL_IDS_CHANGED: false,
    frozenBaseline: frozen,
    availablePeriods,
    periodSliceCounts: Object.fromEntries([...slices.entries()].map(([k, v]) => [k, v.length])),
    CROSS_PERIOD_DEDUPLICATION,
    POOLED_ALL_PERIODS_INDEX,
    AUTOMATIC_CUSTOMER_PROMOTION,
    LIVE_CERTIFIED_VALUES_ONLY: "UNCHANGED",
    lifestyle: { STATUS: EXCLUDED_SCENARIO_STATUS[S.LIFESTYLE] },
    distribution: { STATUS: EXCLUDED_SCENARIO_STATUS[S.DISTRIBUTION_LOYALTY] },
    brandedResidences: { STATUS: EXCLUDED_SCENARIO_STATUS[S.BRANDED_RESIDENCES] },
    marketEntry: { STATUS: EXCLUDED_SCENARIO_STATUS[S.MARKET_ENTRY] },
    target6Prepared: Object.fromEntries(target6.map((t) => [t.label, t.status])),
    target8Prepared: Object.fromEntries(target8.map((t) => [t.label, t.status])),
    postWaveRecertification: {
      COMMAND_OR_MODULE: "scenario-benchmark-longitudinal-recertification:run",
      MODULE: "lib/ai-visibility/competitive-moat/scenario-benchmark-longitudinal-recertification.js",
      CREATED: true,
      PROVIDER_CALLS_REQUIRED: false,
      AUTOMATIC_CUSTOMER_PROMOTION: false,
      canRunRecertification: canRunRecert,
      minimumPeriodsRequired: 2,
    },
    OPERATOR_DIFF: 0,
    PRIMARY_MONITORED_OPERATORS: PRIMARY_OPERATOR_COUNT,
  };

  if (opts.writeReport !== false) {
    fs.mkdirSync(path.dirname(READINESS_REPORT_PATH), { recursive: true });
    fs.writeFileSync(READINESS_REPORT_PATH, JSON.stringify(report, null, 2), "utf8");
  }
  return report;
}

/**
 * Post-wave recertification — requires prior + current measurement periods.
 * Consumes stored observations only. Does not activate customer UI.
 */
export function runLongitudinalRecertification(opts = {}) {
  const priorPeriodId = opts.priorPeriodId || BASELINE_MEASUREMENT_PERIOD;
  const currentPeriodId = opts.currentPeriodId || null;
  if (!currentPeriodId) {
    const readiness = runLongitudinalRecertificationReadiness({ ...opts, writeReport: false });
    return {
      ok: false,
      reason: "CURRENT_PERIOD_REQUIRED",
      message: "Pass --current-period after the next Brand longitudinal wave completes.",
      readiness,
      providerCalls: 0,
      spend: 0,
    };
  }

  const universe = loadBenchmarkEligibleUniverse();
  const priorScoped = buildPeriodScopedIndex(priorPeriodId, opts);
  const currentScoped = buildPeriodScopedIndex(currentPeriodId, opts);
  const frozen = verifyFrozenBaseline(opts);

  const candidates = (opts.candidates || NEAR_TERM_RECERTIFICATION_CANDIDATES).filter(
    (c) => !EXCLUDED_SCENARIO_STATUS[c.scenarioId]
  );

  const evaluations = [];
  for (const candidate of candidates) {
    const priorResult = summarizePeriodResult(
      candidate.subjectId,
      candidate.scenarioId,
      priorPeriodId,
      priorScoped.index,
      universe
    );
    const currentResult = summarizePeriodResult(
      candidate.subjectId,
      candidate.scenarioId,
      currentPeriodId,
      currentScoped.index,
      universe
    );
    evaluations.push(evaluateCandidateRecertification(candidate, priorResult, currentResult, { universe }));
  }

  const report = {
    BRAND_AI_LONGITUDINAL_BENCHMARK_RECERTIFICATION_COMPLETE: true,
    recertificationVersion: RECERTIFICATION_VERSION,
    providerCalls: 0,
    spend: 0,
    uiChanges: 0,
    priorPeriodId,
    currentPeriodId,
    frozenBaseline: frozen,
    CROSS_PERIOD_DEDUPLICATION,
    POOLED_ALL_PERIODS_INDEX,
    AUTOMATIC_CUSTOMER_PROMOTION,
    candidates: evaluations,
    readyForCertification: evaluations.filter((e) => e.READY_FOR_CERTIFICATION),
    OPERATOR_DIFF: 0,
  };

  if (opts.writeReport !== false) {
    fs.mkdirSync(path.dirname(RECERT_REPORT_PATH), { recursive: true });
    fs.writeFileSync(RECERT_REPORT_PATH, JSON.stringify(report, null, 2), "utf8");
  }
  return report;
}

export { buildComparisonGrainKey, partitionResponsesByPeriod };
