/**
 * Competitive history / evidence REVIEW anomaly signals (not automatic errors).
 */

export const RANK_HISTORY_ANOMALY_REVIEW_VERSION = "adp_rank_history_anomaly_review_v1";

export function detectRankHistoryAnomalies({ movementPack, thresholdRankJump = 8 } = {}) {
  const reviews = [];
  if (!movementPack?.customerPresentationReady) {
    return { version: RANK_HISTORY_ANOMALY_REVIEW_VERSION, reviews, note: "No comparable PoP movement yet" };
  }

  for (const [scopeKey, scope] of Object.entries(movementPack.byScope || {})) {
    if (!scope.comparable) continue;
    const rows = scope.rows || [];
    const largeMoves = rows.filter(
      (r) => r.rankDelta != null && Math.abs(r.rankDelta) >= thresholdRankJump
    );
    if (largeMoves.length) {
      reviews.push({
        code: "UNUSUALLY_LARGE_RANK_MOVEMENT",
        severity: "REVIEW",
        scopeKey,
        count: largeMoves.length,
        examples: largeMoves.slice(0, 5).map((r) => ({
          entityId: r.entityId,
          priorRank: r.priorRank,
          currentRank: r.currentRank,
          rankDelta: r.rankDelta,
        })),
      });
    }
    const newEntrants = rows.filter((r) => r.state === "NEW_TO_RANKING" && r.currentRank != null && r.currentRank <= 3);
    if (newEntrants.length) {
      reviews.push({
        code: "NEW_ENTRANT_UNEXPECTEDLY_HIGH_RANK",
        severity: "REVIEW",
        scopeKey,
        count: newEntrants.length,
        examples: newEntrants.slice(0, 5),
      });
    }
    const exited = rows.filter((r) => r.state === "EXITED").length;
    const entered = rows.filter((r) =>
      r.state === "NEW_TO_RANKING" || r.state === "RETURNED"
    ).length;
    if (exited + entered >= 8) {
      reviews.push({
        code: "MAJOR_RANKING_RESHUFFLE",
        severity: "REVIEW",
        scopeKey,
        exited,
        entered,
      });
    }
  }

  return { version: RANK_HISTORY_ANOMALY_REVIEW_VERSION, reviews };
}
