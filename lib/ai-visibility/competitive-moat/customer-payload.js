/**
 * Customer-safe benchmark payload contract — allowlisted fields only.
 */

import { BENCHMARK_PARITY, INDEX_NAME } from "./benchmark-engine-v1.js";
import { toCustomerObservedCompetitors } from "./observed-competitive-set.js";

export const CUSTOMER_BENCHMARK_PAYLOAD_VERSION = "customer_benchmark_payload_v1";

/** Explicit allowlist for customer benchmark API responses. */
export const CUSTOMER_PAYLOAD_ALLOWLIST = Object.freeze([
  "subjectEntityId",
  "subjectName",
  "entityType",
  "indexName",
  "indexValue",
  "benchmarkParity",
  "relativeGapPct",
  "gapToLeaderIndexPoints",
  "benchmarkLabel",
  "benchmarkSampleBand",
  "benchmarkStatus",
  "topObservedCompetitors",
  "intentStrengths",
  "intentWeaknesses",
  "measurementPeriod",
  "comparisonPeriod",
  "evidenceSummary",
  "provider",
  "accessDepth",
  "payloadVersion",
]);

export const INTERNAL_ONLY_FIELDS = Object.freeze([
  "benchmarkMembers",
  "allCompetitorScores",
  "allCompetitorPresenceRates",
  "rawScore",
  "promptTextFullCorpus",
  "mutationRule",
  "classifierThreshold",
  "normalizationRule",
  "cohortSelectionRules",
  "methodologyWeights",
  "fullObservationLedger",
  "researchRecommendation",
  "benchmarkMatrix",
  "commonCohortKeys",
  "normalizationDiagnostics",
]);

/**
 * Build customer-safe benchmark payload.
 */
export function buildCustomerBenchmarkPayload(opts = {}) {
  const index = opts.indexResult || {};
  const observed = opts.observedCompetitiveSet || {};
  const benchmarkStatus = opts.benchmarkStatus || index.status || "VALID";

  const payload = {
    payloadVersion: CUSTOMER_BENCHMARK_PAYLOAD_VERSION,
    subjectEntityId: opts.subjectEntityId || null,
    subjectName: opts.subjectName || null,
    entityType: opts.entityType || "BRAND",
    indexName: index.indexName || INDEX_NAME,
    indexValue: benchmarkStatus.startsWith("SUPPRESSED") ? null : index.indexValue ?? null,
    benchmarkParity: BENCHMARK_PARITY,
    relativeGapPct: index.relativeGapPct ?? null,
    gapToLeaderIndexPoints: opts.gapToLeader?.gapToLeaderIndexPoints ?? null,
    benchmarkLabel: opts.benchmarkLabel || "Comparable owner-decision peer cohort",
    benchmarkSampleBand: opts.benchmarkSampleBand || sampleBandLabel(opts.benchmarkSampleSize),
    benchmarkStatus,
    topObservedCompetitors: toCustomerObservedCompetitors(observed, opts.competitorLimit ?? 5),
    intentStrengths: opts.intentStrengths || [],
    intentWeaknesses: opts.intentWeaknesses || [],
    measurementPeriod: opts.measurementPeriod || null,
    comparisonPeriod: opts.comparisonPeriod || null,
    evidenceSummary: opts.evidenceSummary || null,
    provider: opts.provider || null,
    accessDepth: opts.accessDepth || "deep",
  };

  if (benchmarkStatus.startsWith("SUPPRESSED") || benchmarkStatus.includes("INSUFFICIENT")) {
    payload.indexDisplay =
      benchmarkStatus === "SUPPRESSED_INSUFFICIENT_DATA"
        ? "Not enough comparable observations yet"
        : null;
  }

  return payload;
}

function sampleBandLabel(size) {
  if (size == null) return null;
  if (size >= 10) return "10+ peers";
  if (size >= 5) return "5–9 peers";
  if (size >= 3) return "3–4 peers";
  return "Limited sample";
}

/**
 * Strip non-allowlisted fields from a payload object.
 */
export function redactToCustomerAllowlist(payload = {}) {
  const out = {};
  for (const key of CUSTOMER_PAYLOAD_ALLOWLIST) {
    if (payload[key] !== undefined) out[key] = payload[key];
  }
  return out;
}
