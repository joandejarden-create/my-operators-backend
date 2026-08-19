/**
 * BENCHMARK_COHORT_VALIDITY_V2 — richer states, no N-only VALID/LIMITED collapse.
 * Thresholds are corpus-informed; do not weaken to maximize coverage.
 */

export const BENCHMARK_COHORT_VALIDITY_VERSION = "benchmark_cohort_validity_v2";

export const BENCHMARK_STATES = Object.freeze([
  "VALID",
  "LIMITED_SAMPLE",
  "LIMITED_CORE_PEERS_MISSING",
  "LIMITED_COMMON_GRAIN_COVERAGE",
  "LIMITED_PROVIDER_COVERAGE",
  "SUPPRESSED_SEMANTIC_MISMATCH",
  "SUPPRESSED_INSUFFICIENT_DATA",
]);

/**
 * Recommended gates after inspecting stored-corpus grain distribution.
 * CORE coverage uses BOTH a ratio and named mandatory peers — a pile of
 * SECONDARY brands cannot replace missing Autograph/Curio-class cores.
 */
export const VALIDITY_GATES_V2 = Object.freeze({
  MIN_TOTAL_PEERS: 3,
  MIN_TOTAL_PEERS_VALID: 5,
  MIN_CORE_PEERS: 2,
  CORE_COVERAGE_REQUIREMENT: "BOTH_RATIO_AND_MANDATORY_NAMED_CORES",
  CORE_COVERAGE_RATIO: 0.5,
  COMMON_GRAIN_REQUIREMENT: 8,
  PROVIDER_REQUIREMENT: "SINGLE_PROVIDER_ALLOWED_INTERNAL_NOT_HEADLINE",
  SEMANTIC_COMPARABILITY: "CORE_OR_SECONDARY_ONLY",
  NO_BROAD_FALLBACK: true,
  UNION_GRAIN_PROHIBITED: true,
});

export function classifyBenchmarkCohortValidityV2({
  totalValidPeers = 0,
  corePeers = 0,
  coreWithData = 0,
  coreCoverage = 0,
  mandatoryCoreMissing = [],
  commonGrains = 0,
  providerClass = "NO_PROVIDER",
  semanticOk = true,
  usedBroaderFallback = false,
  unionGrainUsed = false,
  scenarioHasPrompts = true,
} = {}) {
  if (usedBroaderFallback || unionGrainUsed) {
    return {
      status: "SUPPRESSED_SEMANTIC_MISMATCH",
      reasons: [
        usedBroaderFallback ? "NO_FULL_SET_FALLBACK" : null,
        unionGrainUsed ? "UNION_GRAIN_PROHIBITED" : null,
      ].filter(Boolean),
    };
  }
  if (!semanticOk) {
    return { status: "SUPPRESSED_SEMANTIC_MISMATCH", reasons: ["semantic_comparability_failed"] };
  }
  if (!scenarioHasPrompts || commonGrains <= 0) {
    return { status: "SUPPRESSED_INSUFFICIENT_DATA", reasons: ["no_common_measurement_grains"] };
  }
  if (totalValidPeers < 1 || corePeers < 1) {
    return { status: "SUPPRESSED_INSUFFICIENT_DATA", reasons: ["no_commercial_peers"] };
  }

  const reasons = [];
  if (mandatoryCoreMissing.length) {
    reasons.push("LIMITED_CORE_PEERS_MISSING");
  }
  if (corePeers < VALIDITY_GATES_V2.MIN_CORE_PEERS || coreCoverage < VALIDITY_GATES_V2.CORE_COVERAGE_RATIO) {
    reasons.push("LIMITED_CORE_PEERS_MISSING");
  }
  if (commonGrains < VALIDITY_GATES_V2.COMMON_GRAIN_REQUIREMENT) {
    reasons.push("LIMITED_COMMON_GRAIN_COVERAGE");
  }
  if (totalValidPeers < VALIDITY_GATES_V2.MIN_TOTAL_PEERS) {
    reasons.push("LIMITED_SAMPLE");
  }
  if (coreWithData < 1 && corePeers > 0) {
    reasons.push("LIMITED_CORE_PEERS_MISSING");
  }

  const providerLimited = providerClass === "SINGLE_PROVIDER_ONLY";

  if (!reasons.length && totalValidPeers >= VALIDITY_GATES_V2.MIN_TOTAL_PEERS_VALID) {
    return {
      status: "VALID",
      reasons: providerLimited ? ["SINGLE_PROVIDER_INTERNAL_OK_NOT_HEADLINE"] : [],
    };
  }
  if (!reasons.length && totalValidPeers >= VALIDITY_GATES_V2.MIN_TOTAL_PEERS) {
    return { status: "LIMITED_SAMPLE", reasons: ["below_valid_peer_count"] };
  }
  if (reasons.includes("LIMITED_CORE_PEERS_MISSING")) {
    return { status: "LIMITED_CORE_PEERS_MISSING", reasons };
  }
  if (reasons.includes("LIMITED_COMMON_GRAIN_COVERAGE")) {
    return { status: "LIMITED_COMMON_GRAIN_COVERAGE", reasons };
  }
  if (reasons.includes("LIMITED_SAMPLE")) {
    return { status: "LIMITED_SAMPLE", reasons };
  }
  return { status: "LIMITED_SAMPLE", reasons };
}
