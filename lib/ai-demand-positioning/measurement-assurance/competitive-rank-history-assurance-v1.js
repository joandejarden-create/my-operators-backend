/**
 * COMPETITIVE_RANK_HISTORY_INTEGRITY — permanent assurance gate.
 */

import {
  buildFullCompetitiveRankingSnapshot,
  buildCompetitiveMovementPack,
  computeScopeMovement,
  canFinalizeRankSnapshot,
  finalizeRankSnapshot,
  MOVEMENT_STATE,
  DEFECT_INCORRECT_RANK_DELTA,
  DEFECT_FALSE_NEW_TO_RANKING,
  DEFECT_NON_COMPARABLE_PERIOD_MOVEMENT,
  COMPETITIVE_RANK_HISTORY_VERSION,
  selectPriorComparablePeriod,
} from "../competitive-history/rank-history-ledger-v1.js";
import { OVERALL_RANKING_KEY } from "../customer/competitive-ranking-overall-view-v1.js";

export const COMPETITIVE_RANK_HISTORY_INTEGRITY = "COMPETITIVE_RANK_HISTORY_INTEGRITY";

export function runCompetitiveRankHistoryIntegrity({
  period,
  scenarios,
  propertyProfile,
  allPeriods = [],
  historySnapshots = [],
  certificationStatus = "CERTIFIED_WITH_DISCLOSURES",
}) {
  const defects = [];
  const snapshot = buildFullCompetitiveRankingSnapshot({
    period,
    scenarios,
    propertyProfile,
    certificationStatus,
  });

  const overall = snapshot.byScope?.[OVERALL_RANKING_KEY];
  if (!overall?.entities?.length && overall?.comparableN > 0) {
    defects.push({ code: "STALE_COMPETITIVE_HISTORY", detail: "overall entities empty with comparableN>0" });
  }

  // Ranks dense 1..N
  for (const [scopeKey, scope] of Object.entries(snapshot.byScope || {})) {
    const ranks = (scope.entities || []).map((e) => e.rank);
    for (let i = 0; i < ranks.length; i++) {
      if (ranks[i] !== i + 1) {
        defects.push({
          code: DEFECT_INCORRECT_RANK_DELTA,
          detail: `scope ${scopeKey} dense rank broken at index ${i}`,
        });
        break;
      }
    }
    // Entity IDs unique
    const ids = (scope.entities || []).map((e) => e.entityId);
    if (new Set(ids).size !== ids.length) {
      defects.push({ code: "ENTITY_HISTORY_BREAK", detail: `duplicate entityId in ${scopeKey}` });
    }
  }

  // Finalize gate respects provider coverage
  const canFin = canFinalizeRankSnapshot(snapshot);
  let finalized = snapshot;
  if (canFin.ok) {
    finalized = finalizeRankSnapshot(snapshot, { certificationStatus }).snapshot;
  }

  const { priorPeriod } = selectPriorComparablePeriod(period, allPeriods, scenarios);
  const movement = buildCompetitiveMovementPack({
    currentSnapshot: finalized,
    historySnapshots: [...historySnapshots, finalized],
    scenarios,
    currentPeriod: period,
    priorPeriod,
  });

  // Single-period: must not invent ↑/↓/NEW
  if (!priorPeriod) {
    for (const scope of Object.values(movement.byScope || {})) {
      for (const row of scope.rows || []) {
        if (row.rankDelta != null) {
          defects.push({
            code: DEFECT_NON_COMPARABLE_PERIOD_MOVEMENT,
            detail: "rankDelta set without prior comparable period",
          });
        }
        if (
          [MOVEMENT_STATE.MOVED, MOVEMENT_STATE.NEW_TO_RANKING, MOVEMENT_STATE.RETURNED].includes(
            row.state
          ) &&
          !movement.customerPresentationReady
        ) {
          // INITIAL expected
          if (row.state !== MOVEMENT_STATE.INITIAL && scope.comparable === false) {
            defects.push({
              code: DEFECT_FALSE_NEW_TO_RANKING,
              detail: `unexpected state ${row.state} on non-comparable single period`,
            });
          }
        }
      }
    }
  }

  // Arithmetic check when comparable synthetic prior exists in historySnapshots
  if (priorPeriod && movement.comparability?.comparable && finalized.finalized) {
    const priorSnap = historySnapshots.find((h) => h.periodId === priorPeriod.periodId);
    if (priorSnap?.finalized) {
      for (const [scopeKey, scope] of Object.entries(movement.byScope || {})) {
        if (!scope.comparable) continue;
        for (const row of scope.rows || []) {
          if (row.priorRank != null && row.currentRank != null && row.rankDelta != null) {
            const expected = row.priorRank - row.currentRank;
            if (expected !== row.rankDelta) {
              defects.push({
                code: DEFECT_INCORRECT_RANK_DELTA,
                detail: `${scopeKey} ${row.entityId} delta ${row.rankDelta} != ${expected}`,
              });
            }
          }
        }
      }
    }
  }

  // Top-10 must not drive NEW — verify a synthetic deep-rank case via computeScopeMovement
  const synthetic = computeScopeMovement({
    currentScope: {
      entities: [
        { entityId: "hotel_c", displayName: "Hotel C", rank: 7, appearances: 3, aiPresencePct: 10 },
      ],
    },
    priorScope: {
      entities: [
        { entityId: "hotel_c", displayName: "Hotel C", rank: 12, appearances: 1, aiPresencePct: 4 },
      ],
    },
    comparable: true,
    earlierScopes: [],
  });
  const hotelC = synthetic.rows.find((r) => r.entityId === "hotel_c");
  if (!hotelC || hotelC.state !== MOVEMENT_STATE.MOVED || hotelC.rankDelta !== 5) {
    defects.push({
      code: DEFECT_FALSE_NEW_TO_RANKING,
      detail: "Hotel C #12→#7 must be MOVED ↑5, not NEW",
    });
  }

  return {
    gate: COMPETITIVE_RANK_HISTORY_INTEGRITY,
    version: COMPETITIVE_RANK_HISTORY_VERSION,
    status: defects.length ? "FAIL" : "PASS",
    defects,
    canFinalize: canFin.ok,
    finalizeReason: canFin.reason,
    finalized: Boolean(finalized.finalized),
    customerPresentationReady: movement.customerPresentationReady,
    scopeCount: Object.keys(snapshot.byScope || {}).length,
    overallEntityCount: overall?.entities?.length || 0,
    movement,
    snapshotMeta: {
      periodId: snapshot.periodId,
      providerCoverageGate: snapshot.providerCoverageGate?.status,
    },
  };
}
