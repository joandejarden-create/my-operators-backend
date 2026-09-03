/**
 * Build competitive movement pack against a resolved comparison window.
 * Reuses computeScopeMovement — only the baseline period selection changes.
 */

import {
  computeScopeMovement,
  MOVEMENT_STATE,
  COMPETITIVE_RANK_HISTORY_VERSION,
} from "../competitive-history/rank-history-ledger-v1.js";
import { COMPARISON_MODES } from "./comparison-window-v1.js";

/**
 * @param {object} args
 * @param {object} args.currentSnapshot
 * @param {object[]} args.historySnapshots — chronological or unsorted finalized snaps
 * @param {object} args.comparisonResolution — from resolveComparisonWindowV1
 */
export function buildCompetitiveMovementPackForComparison({
  currentSnapshot,
  historySnapshots = [],
  comparisonResolution,
}) {
  const priorPeriodId = comparisonResolution?.comparisonPeriodId || null;
  const priorSnap = priorPeriodId
    ? historySnapshots.find((h) => h.periodId === priorPeriodId) || null
    : null;

  const priorDate = String(priorSnap?.executionDate || priorSnap?.calendarDate || "");
  const earlier = historySnapshots.filter((h) => {
    if (h.periodId === currentSnapshot.periodId) return false;
    if (priorSnap && h.periodId === priorSnap.periodId) return false;
    const d = String(h.executionDate || h.calendarDate || "");
    // RETURNED vs NEW: only history strictly before the selected baseline counts.
    // Appearances between baseline and current must not flip NEW → RETURNED.
    if (priorDate && d >= priorDate) return false;
    return d < String(currentSnapshot.executionDate || currentSnapshot.calendarDate || "");
  });

  const comparable =
    Boolean(priorSnap?.finalized && currentSnapshot.finalized) &&
    Boolean(comparisonResolution?.comparability?.comparable);

  const byScope = {};
  for (const scopeKey of Object.keys(currentSnapshot.byScope || {})) {
    const currentScope = currentSnapshot.byScope[scopeKey];
    const priorScope = priorSnap?.byScope?.[scopeKey] || null;
    const earlierScopes = earlier.map((h) => h.byScope?.[scopeKey]).filter(Boolean);

    if (!comparable || !priorSnap) {
      byScope[scopeKey] = {
        scopeKey,
        comparable: false,
        reason:
          comparisonResolution?.unavailableReason ||
          comparisonResolution?.comparability?.reason ||
          (!priorSnap ? "NO_PRIOR_SNAPSHOT" : "RANK_CHANGE_NOT_COMPARABLE"),
        comparisonMode: comparisonResolution?.comparisonMode || null,
        comparisonPeriodId: priorPeriodId,
        actualComparisonDate: comparisonResolution?.actualComparisonDate || null,
        rows: (currentScope.entities || []).map((e) => ({
          entityId: e.entityId,
          displayName: e.displayName,
          isSubject: e.isSubject,
          aiPresencePct: e.aiPresencePct,
          currentRank: e.rank,
          priorRank: null,
          rankDelta: null,
          state: comparable === false && priorSnap
            ? MOVEMENT_STATE.RANK_CHANGE_NOT_COMPARABLE
            : MOVEMENT_STATE.INITIAL,
          numerator: e.numerator,
          denominator: e.denominator,
        })),
      };
      continue;
    }

    byScope[scopeKey] = {
      scopeKey,
      ...computeScopeMovement({
        currentScope,
        priorScope,
        comparable: true,
        earlierScopes,
      }),
      comparisonMode: comparisonResolution.comparisonMode,
      comparisonPeriodId: priorSnap.periodId,
      actualComparisonDate: comparisonResolution.actualComparisonDate,
      priorPeriodId: priorSnap.periodId,
    };
  }

  return {
    version: COMPETITIVE_RANK_HISTORY_VERSION,
    propertyId: currentSnapshot.propertyId,
    currentPeriodId: currentSnapshot.periodId,
    comparisonMode: comparisonResolution?.comparisonMode || COMPARISON_MODES.PRIOR_RUN,
    comparisonModeLabel: comparisonResolution?.comparisonModeLabel || null,
    deltaColumnLabel: comparisonResolution?.deltaColumnLabel || null,
    comparisonTooltip: comparisonResolution?.comparisonTooltip || null,
    datePairLabel: comparisonResolution?.datePairLabel || null,
    priorPeriodId: priorSnap?.periodId || null,
    comparisonPeriodId: priorSnap?.periodId || null,
    actualComparisonDate: comparisonResolution?.actualComparisonDate || null,
    comparability: comparisonResolution?.comparability || { comparable: false },
    byScope,
    customerPresentationReady: false, // production UI still deferred
    syntheticHarnessOnly: Boolean(currentSnapshot.synthetic === true),
  };
}
