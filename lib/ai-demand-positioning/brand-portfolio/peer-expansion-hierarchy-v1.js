/**
 * ADP Brand & Portfolio — Peer expansion hierarchy (LOYALTY ECOSYSTEM primary).
 */

export const PEER_EXPANSION_HIERARCHY_V1 = Object.freeze([
  {
    level: 1,
    id: "L1_SAME_ECOSYSTEM_SAME_HOTEL_MARKET",
    rule: "Same loyalty/parent ecosystem + same submarket / hotel market",
  },
  {
    level: 2,
    id: "L2_SAME_ECOSYSTEM_BROADER_DESTINATION",
    rule: "Same loyalty/parent ecosystem + broader legitimate metro/destination (not statewide/national/global)",
  },
  {
    level: 3,
    id: "L3_INSUFFICIENT_PORTFOLIO_PEER_SET",
    rule: "Stop. Do not expand to another region merely to hit threshold.",
  },
]);

/** @deprecated alias — branded hotels no longer use L3 loyalty expansion (loyalty IS primary) */
export const LEGACY_COLLECTION_EXPANSION_NOTE =
  "Collection/hard-brand match is peer metadata only — not an inclusion requirement.";

export const MIN_PEER_THRESHOLDS_V1 = Object.freeze({
  rankUsable: 3,
  benchmarkUsable: 5,
  indexUsable: 5,
  preferred: 5,
  tooThin: 2,
});

export const INSUFFICIENT_PORTFOLIO_PEER_SET = "INSUFFICIENT_PORTFOLIO_PEER_SET";

export const LOYALTY_ECOSYSTEM_PEER_SET_INTEGRITY = "LOYALTY_ECOSYSTEM_PEER_SET_INTEGRITY";
export const LOYALTY_ECOSYSTEM_PROMPT_ALIGNMENT = "LOYALTY_ECOSYSTEM_PROMPT_ALIGNMENT";
export const SUBJECT_VS_ECOSYSTEM_RANKING_INTEGRITY = "SUBJECT_VS_ECOSYSTEM_RANKING_INTEGRITY";
export const LOYALTY_PORTFOLIO_DISPLACEMENT_INTEGRITY = "LOYALTY_PORTFOLIO_DISPLACEMENT_INTEGRITY";

export function evaluatePeerSetAdequacy(peerCountExcludingSubject) {
  const n = Number(peerCountExcludingSubject) || 0;
  if (n <= MIN_PEER_THRESHOLDS_V1.tooThin) {
    return {
      status: INSUFFICIENT_PORTFOLIO_PEER_SET,
      canRank: false,
      canBenchmark: false,
      canIndex: false,
      canShowPresenceAndEvidence: false,
      reason: "peer_count_too_thin",
    };
  }
  if (n < MIN_PEER_THRESHOLDS_V1.rankUsable) {
    return {
      status: INSUFFICIENT_PORTFOLIO_PEER_SET,
      canRank: false,
      canBenchmark: false,
      canIndex: false,
      canShowPresenceAndEvidence: false,
      reason: "below_rank_minimum",
    };
  }
  if (n < MIN_PEER_THRESHOLDS_V1.benchmarkUsable) {
    return {
      status: "RANK_ONLY_SUPPRESS_BENCHMARK_INDEX",
      canRank: true,
      canBenchmark: false,
      canIndex: false,
      canShowPresenceAndEvidence: true,
      reason: "rank_ok_benchmark_requires_5",
      showKpis: ["portfolioAiPresence", "portfolioRank", "numberOneAppearance", "top3Appearance"],
      suppressKpis: ["portfolioBenchmark", "portfolioPresenceIndex"],
    };
  }
  return {
    status: "ADEQUATE",
    canRank: true,
    canBenchmark: true,
    canIndex: true,
    canShowPresenceAndEvidence: true,
    reason: "meets_preferred_threshold",
    showKpis: [
      "portfolioAiPresence",
      "portfolioRank",
      "portfolioBenchmark",
      "portfolioPresenceIndex",
      "numberOneAppearance",
      "top3Appearance",
    ],
    suppressKpis: [],
  };
}
