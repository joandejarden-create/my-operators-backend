/**
 * BENCHMARK_ENGINE_V1 — contextual cohort logic for validated Presence only.
 */

import { loadPeerSetConfig, resolvePeerSetMembership, PEER_SET_ID_V2 } from "../peer-sets.js";
import { loadSelectedBrandUniverse } from "../brand-longitudinal/selected-universe.js";
import { PRIMARY_OPERATOR_COUNT } from "../operator-intelligence/universe.js";

export const BENCHMARK_ENGINE_ID = "BENCHMARK_ENGINE_V1";
export const BENCHMARK_ENGINE_VERSION = "benchmark_engine_v1";
export const SUPPORTED_METRIC = "PRESENCE";
export const BENCHMARK_PARITY = 100;

/** Recommended index name — Presence-specific, avoids API/APIx confusion with preference. */
export const INDEX_NAME = "AI Presence Index";
export const INDEX_SHORT_CODE = "APIx";

export const BENCHMARK_AGGREGATION = "MEDIAN";
/** Working candidate only — method certification is deferred until scenario cohorts are certified. */
export const BENCHMARK_AGGREGATION_STATUS = "DEFERRED_UNTIL_COHORT_CERTIFIED";
export const HEADLINE_INDEX_AGGREGATION = "DEFERRED";
export const UNION_GRAIN_BENCHMARK = "PROHIBITED";
export const FULL_SET_FALLBACK = "PROHIBITED";
export const BENCHMARK_AGGREGATION_RATIONALE =
  "Median comparable Presence is the working candidate (robust to outlier peers). " +
  "Mean vs median vs trimmed mean is NOT certified until scenario-specific cohorts pass validity V2.";

export const MIN_SAMPLE_POLICY = Object.freeze({
  VALID_BENCHMARK: { minPeers: 5, label: "VALID_BENCHMARK" },
  LIMITED_BENCHMARK: { minPeers: 3, maxPeers: 4, label: "LIMITED_BENCHMARK" },
  SUPPRESSED: { minPeers: 0, maxPeers: 2, label: "SUPPRESSED_INSUFFICIENT_DATA" },
  ZERO_BENCHMARK: { label: "INDEX_SUPPRESSED_ZERO_BENCHMARK" },
  BROADER_COHORT: { label: "BROADER_COHORT_REQUIRED" },
});

export const COHORT_LOGIC_VERSION = "contextual_cohort_v1";
export const ACCESS_POLICY_VERSION = "competitive_data_access_v1";
export const INDEX_ROUNDING = "nearest_integer";

/**
 * Resolve benchmark cohort members for a brand subject.
 * Never mixes Brand and Operator entities.
 */
export function resolveBrandBenchmarkCohort(subjectBrandId, opts = {}) {
  const peerSetId = opts.peerSetId || PEER_SET_ID_V2;
  const commercialRegion = opts.commercialRegion || "CALA";
  const cfg = loadPeerSetConfig(opts.peerSetConfigPath);
  const membership = resolvePeerSetMembership({ peerSetId, commercialRegion }, cfg);
  if (!membership.ok) {
    return { ok: false, status: "NOT_COMPARABLE", members: [], error: membership.error };
  }
  const members = membership.entityIds.filter((id) => id !== subjectBrandId);
  const set = cfg.peerSets?.find((p) => p.peerSetId === peerSetId);
  const memberDetails = (set?.members || []).filter((m) =>
    membership.entityIds.includes(m.brandId)
  );
  return {
    ok: true,
    entityType: "BRAND",
    peerSetId,
    peerSetVersion: membership.peerSetVersion,
    commercialRegion,
    members,
    memberDetails,
    subjectBrandId,
    cohortLogicVersion: COHORT_LOGIC_VERSION,
  };
}

/**
 * Operator benchmark cohort — separate from Brand. Blocked until Presence validated.
 */
export function resolveOperatorBenchmarkCohort(subjectOperatorId, opts = {}) {
  if (opts.presenceValidated !== true) {
    return {
      ok: false,
      status: "BLOCKED_PENDING_PRESENCE_VALIDATION",
      members: [],
      entityType: "OPERATOR",
      expectedPrimaryCount: PRIMARY_OPERATOR_COUNT,
    };
  }
  const monitoredIds = opts.monitoredOperatorIds || [];
  const members = monitoredIds.filter((id) => id !== subjectOperatorId);
  return {
    ok: true,
    entityType: "OPERATOR",
    members,
    subjectOperatorId,
    cohortLogicVersion: COHORT_LOGIC_VERSION,
  };
}

/**
 * Classify benchmark sample size.
 */
export function classifyBenchmarkSampleSize(peerCount, benchmarkPresence = null) {
  if (benchmarkPresence === 0) {
    return MIN_SAMPLE_POLICY.ZERO_BENCHMARK.label;
  }
  if (peerCount >= MIN_SAMPLE_POLICY.VALID_BENCHMARK.minPeers) {
    return MIN_SAMPLE_POLICY.VALID_BENCHMARK.label;
  }
  if (peerCount >= MIN_SAMPLE_POLICY.LIMITED_BENCHMARK.minPeers) {
    return MIN_SAMPLE_POLICY.LIMITED_BENCHMARK.label;
  }
  return MIN_SAMPLE_POLICY.SUPPRESSED.label;
}

/**
 * Aggregate benchmark Presence from peer rates.
 */
export function aggregateBenchmarkPresence(peerPresenceRates = [], method = BENCHMARK_AGGREGATION) {
  const rates = peerPresenceRates.filter((v) => typeof v === "number" && Number.isFinite(v));
  if (!rates.length) return { value: null, method, sampleSize: 0 };
  const sorted = [...rates].sort((a, b) => a - b);
  let value;
  if (method === "MEAN") {
    value = rates.reduce((s, v) => s + v, 0) / rates.length;
  } else if (method === "TRIMMED_MEAN") {
    const trim = Math.floor(rates.length * 0.1);
    const trimmed = sorted.slice(trim, sorted.length - trim || sorted.length);
    value = trimmed.reduce((s, v) => s + v, 0) / trimmed.length;
  } else {
    const mid = Math.floor(sorted.length / 2);
    value =
      sorted.length % 2 === 0
        ? (sorted[mid - 1] + sorted[mid]) / 2
        : sorted[mid];
  }
  return { value, method, sampleSize: rates.length };
}

/**
 * CORE_BENCHMARK_PLUS_SECONDARY_CONTEXT — customer benchmark denominator is CORE-only.
 * Returns true only when the stored benchmark median structurally includes non-CORE rates.
 * Rate collisions between CORE and SECONDARY peers do NOT count as denominator contamination.
 */
export function secondaryInCustomerBenchmarkDenominator(corePairwise = [], benchmarkCore = null) {
  const coreOnlyRates = corePairwise
    .filter((p) => p.commercialRelation === "CORE" || p.commercialRelation == null)
    .map((p) => p.peerPresence)
    .filter((v) => typeof v === "number");
  const nonCoreInSet = corePairwise.some(
    (p) => p.commercialRelation && p.commercialRelation !== "CORE"
  );
  if (nonCoreInSet) return true;
  if (benchmarkCore == null) return false;
  const coreOnlyMedian = aggregateBenchmarkPresence(coreOnlyRates, "MEDIAN").value;
  return coreOnlyMedian !== benchmarkCore;
}

/**
 * Compute AI Presence Index.
 * INDEX = subject / benchmark × 100
 */
export function computeAiPresenceIndex(subjectPresence, benchmarkPresence) {
  if (benchmarkPresence == null || benchmarkPresence === 0) {
    return {
      ok: false,
      status: MIN_SAMPLE_POLICY.ZERO_BENCHMARK.label,
      indexValue: null,
      benchmarkParity: BENCHMARK_PARITY,
    };
  }
  if (subjectPresence == null || !Number.isFinite(subjectPresence)) {
    return { ok: false, status: "SUBJECT_MISSING", indexValue: null };
  }
  const raw = (subjectPresence / benchmarkPresence) * BENCHMARK_PARITY;
  const indexValue = Math.round(raw);
  const relativeGapPct = Math.round((raw - BENCHMARK_PARITY) * 10) / 10;
  return {
    ok: true,
    status: "VALID",
    indexName: INDEX_NAME,
    indexShortCode: INDEX_SHORT_CODE,
    indexValue,
    benchmarkParity: BENCHMARK_PARITY,
    relativeGapPct,
    subjectPresence,
    benchmarkPresence,
    interpretation:
      indexValue > BENCHMARK_PARITY
        ? `${Math.abs(relativeGapPct)}% above the relevant competitive benchmark`
        : indexValue < BENCHMARK_PARITY
          ? `${Math.abs(relativeGapPct)}% below the relevant competitive benchmark`
          : "At competitive parity",
  };
}

/**
 * Gap to leader within valid cohort.
 */
export function computeGapToLeader(subjectIndex, cohortIndices = []) {
  const valid = cohortIndices.filter(
    (c) => typeof c.indexValue === "number" && Number.isFinite(c.indexValue)
  );
  if (!valid.length) return { ok: false, gapToLeaderIndexPoints: null };
  const leader = valid.reduce((best, c) => (c.indexValue > best.indexValue ? c : best));
  if (leader.indexValue <= subjectIndex) {
    return { ok: true, isLeader: true, gapToLeaderIndexPoints: 0, leaderEntityId: leader.entityId };
  }
  return {
    ok: true,
    isLeader: false,
    gapToLeaderIndexPoints: Math.round(subjectIndex - leader.indexValue),
    leaderEntityId: leader.entityId,
    leaderIndex: leader.indexValue,
  };
}

export function loadBrandSubjectsForAudit() {
  const universe = loadSelectedBrandUniverse();
  return universe.brands.map((b) => ({
    brandId: b.brandId,
    brandName: b.brandName,
    parent: b.parent,
  }));
}
