/**
 * Canonical row-level Prior Run comparison resolver (Core + BPP).
 *
 * SAME_ROW_COMPARISON_SAME_CANONICAL_RESOLVER
 * PRIOR_RUN_ROW_IDENTITY_INTEGRITY
 * PRIOR_RUN_DELTA_NULL_ZERO_INTEGRITY
 * RANK_DIRECTION_SEMANTICS_INTEGRITY
 * LONGITUDINAL_ROW_MEMBERSHIP_STATE_INTEGRITY
 *
 * Does not invent values. Callers supply current/prior from certified snapshots.
 * Rank improvement = rank number decreasing (rankDelta = priorRank - currentRank).
 */

export const SAME_ROW_COMPARISON_SAME_CANONICAL_RESOLVER =
  "SAME_ROW_COMPARISON_SAME_CANONICAL_RESOLVER";
export const PRIOR_RUN_ROW_IDENTITY_INTEGRITY = "PRIOR_RUN_ROW_IDENTITY_INTEGRITY";
export const PRIOR_RUN_DELTA_CALCULATION_INTEGRITY = "PRIOR_RUN_DELTA_CALCULATION_INTEGRITY";
export const PRIOR_RUN_DELTA_NULL_ZERO_INTEGRITY = "PRIOR_RUN_DELTA_NULL_ZERO_INTEGRITY";
export const RANK_DIRECTION_SEMANTICS_INTEGRITY = "RANK_DIRECTION_SEMANTICS_INTEGRITY";
export const HISTORICAL_RANK_USES_PERIOD_SPECIFIC_UNIVERSE =
  "HISTORICAL_RANK_USES_PERIOD_SPECIFIC_UNIVERSE";
export const LONGITUDINAL_ROW_MEMBERSHIP_STATE_INTEGRITY =
  "LONGITUDINAL_ROW_MEMBERSHIP_STATE_INTEGRITY";
export const CUSTOMER_ROW_MOVEMENT_PAYLOAD_COMPLETE = "CUSTOMER_ROW_MOVEMENT_PAYLOAD_COMPLETE";
export const LOCAL_PRODUCTION_ROW_MOVEMENT_PARITY = "LOCAL_PRODUCTION_ROW_MOVEMENT_PARITY";

export const ROW_MEMBERSHIP_STATE = Object.freeze({
  SAME: "SAME",
  NEW: "NEW",
  EXITED: "EXITED",
  RETURNED: "RETURNED",
  NOT_COMPARABLE: "NOT_COMPARABLE",
});

export const RANK_DIRECTION = Object.freeze({
  IMPROVED: "IMPROVED",
  DECLINED: "DECLINED",
  UNCHANGED: "UNCHANGED",
  UNAVAILABLE: "UNAVAILABLE",
});

export const DELTA_UNIT = Object.freeze({
  PP: "pp",
  COUNT: "count",
  INDEX: "index",
  RANK: "rank",
  NONE: "none",
});

function isFiniteNumber(n) {
  return n != null && Number.isFinite(Number(n));
}

function round1(n) {
  return Math.round(Number(n) * 10) / 10;
}

/**
 * Format a rate/index-point delta for customer Δ vs Prior Run cells.
 * Rates use percentage points: +3.2 pp (never +6.6%).
 */
export function formatGovernedDeltaDisplay({
  delta,
  deltaUnit,
  membershipState = null,
  suppressed = false,
} = {}) {
  if (suppressed) return "—";
  if (
    membershipState === ROW_MEMBERSHIP_STATE.NEW ||
    membershipState === "NEW_TO_RANKING"
  ) {
    return "NEW";
  }
  if (membershipState === ROW_MEMBERSHIP_STATE.EXITED || membershipState === "EXITED") {
    return "EXITED";
  }
  if (membershipState === ROW_MEMBERSHIP_STATE.RETURNED || membershipState === "RETURNED") {
    if (!isFiniteNumber(delta)) return "RETURNED";
  }
  if (membershipState === ROW_MEMBERSHIP_STATE.NOT_COMPARABLE) return "—";
  if (!isFiniteNumber(delta)) return null; // unavailable — UI should not coerce to 0
  const n = Number(delta);
  if (deltaUnit === DELTA_UNIT.PP) {
    if (Math.abs(n) < 0.05) return "0.0 pp";
    const sign = n > 0 ? "+" : "";
    return `${sign}${round1(n).toFixed(1)} pp`;
  }
  if (deltaUnit === DELTA_UNIT.COUNT || deltaUnit === DELTA_UNIT.INDEX) {
    if (n === 0) return "0";
    const sign = n > 0 ? "+" : "";
    return `${sign}${Math.round(n)}`;
  }
  if (deltaUnit === DELTA_UNIT.RANK) {
    if (n === 0) return "0";
    return n > 0 ? `↑${n}` : `↓${Math.abs(n)}`;
  }
  const sign = n > 0 ? "+" : "";
  return `${sign}${round1(n)}`;
}

/**
 * Rank grammar: #4 → #2 = ↑2 (rankDelta = prior - current = +2 = improved).
 */
export function formatRankWithMovement({
  currentRank,
  priorRank = null,
  rankDelta = null,
  membershipState = null,
} = {}) {
  if (currentRank == null || currentRank === "—") {
    if (membershipState === ROW_MEMBERSHIP_STATE.EXITED || membershipState === "EXITED") {
      return priorRank != null ? `#${priorRank} EXITED` : "EXITED";
    }
    return "—";
  }
  const base = `#${currentRank}`;
  if (
    membershipState === ROW_MEMBERSHIP_STATE.NEW ||
    membershipState === "NEW_TO_RANKING"
  ) {
    return `${base} NEW`;
  }
  if (membershipState === ROW_MEMBERSHIP_STATE.RETURNED || membershipState === "RETURNED") {
    return `${base} RETURNED`;
  }
  const d =
    rankDelta != null
      ? Number(rankDelta)
      : priorRank != null
        ? Number(priorRank) - Number(currentRank)
        : null;
  if (!isFiniteNumber(d) || d === 0) return base;
  return d > 0 ? `${base} ↑${d}` : `${base} ↓${Math.abs(d)}`;
}

export function resolveRankDirection(rankDelta) {
  if (!isFiniteNumber(rankDelta)) return RANK_DIRECTION.UNAVAILABLE;
  const d = Number(rankDelta);
  if (d > 0) return RANK_DIRECTION.IMPROVED;
  if (d < 0) return RANK_DIRECTION.DECLINED;
  return RANK_DIRECTION.UNCHANGED;
}

/**
 * @param {object} input
 * @returns {object} governed comparison for one canonical row
 */
export function resolveRowLevelPriorComparisonV1(input = {}) {
  const {
    measurementFamily = null,
    propertyId = null,
    currentPeriodId = null,
    priorPeriodId = null,
    scopeType = null,
    scopeKey = null,
    canonicalRowId = null,
    metric = null,
    currentValue = null,
    priorValue = null,
    currentRank = null,
    priorRank = null,
    currentExists = currentValue != null || currentRank != null,
    priorExists = priorValue != null || priorRank != null,
    earlierHadEntity = false,
    comparable = true,
    comparabilityReason = null,
    deltaUnit = DELTA_UNIT.PP,
  } = input;

  if (!canonicalRowId) {
    return {
      ok: false,
      reason: "missing_canonical_row_id",
      gate: PRIOR_RUN_ROW_IDENTITY_INTEGRITY,
    };
  }

  if (!comparable || !priorPeriodId) {
    return {
      ok: true,
      measurementFamily,
      propertyId,
      currentPeriodId,
      priorPeriodId,
      scopeType,
      scopeKey,
      canonicalRowId,
      metric,
      currentValue,
      priorValue: null,
      delta: null,
      deltaUnit,
      deltaDisplay: "—",
      direction: null,
      movementState: ROW_MEMBERSHIP_STATE.NOT_COMPARABLE,
      currentRank,
      priorRank: null,
      rankDelta: null,
      rankDirection: RANK_DIRECTION.UNAVAILABLE,
      rankDisplay: formatRankWithMovement({ currentRank }),
      comparabilityStatus: comparabilityReason || "NOT_COMPARABLE",
      resolver: SAME_ROW_COMPARISON_SAME_CANONICAL_RESOLVER,
    };
  }

  let membershipState = ROW_MEMBERSHIP_STATE.SAME;
  if (currentExists && !priorExists) {
    membershipState = earlierHadEntity
      ? ROW_MEMBERSHIP_STATE.RETURNED
      : ROW_MEMBERSHIP_STATE.NEW;
  } else if (!currentExists && priorExists) {
    membershipState = ROW_MEMBERSHIP_STATE.EXITED;
  }

  let delta = null;
  if (
    membershipState === ROW_MEMBERSHIP_STATE.SAME ||
    membershipState === ROW_MEMBERSHIP_STATE.RETURNED
  ) {
    if (isFiniteNumber(currentValue) && isFiniteNumber(priorValue)) {
      delta = round1(Number(currentValue) - Number(priorValue));
    }
  }

  let rankDelta = null;
  if (
    isFiniteNumber(currentRank) &&
    isFiniteNumber(priorRank) &&
    membershipState === ROW_MEMBERSHIP_STATE.SAME
  ) {
    // + = improved (rank number decreased)
    rankDelta = Number(priorRank) - Number(currentRank);
  }

  const rankDirection = resolveRankDirection(rankDelta);
  const deltaDisplay = formatGovernedDeltaDisplay({
    delta,
    deltaUnit,
    membershipState,
  });

  let direction = null;
  if (isFiniteNumber(delta)) {
    if (delta > 0) direction = "UP";
    else if (delta < 0) direction = "DOWN";
    else direction = "FLAT";
  }

  return {
    ok: true,
    measurementFamily,
    propertyId,
    currentPeriodId,
    priorPeriodId,
    scopeType,
    scopeKey,
    canonicalRowId,
    metric,
    currentValue: isFiniteNumber(currentValue) ? Number(currentValue) : currentValue,
    priorValue: isFiniteNumber(priorValue) ? Number(priorValue) : priorValue,
    // Preserve null vs zero: missing prior stays null; computed zero stays 0
    delta,
    deltaUnit,
    deltaDisplay: deltaDisplay == null ? "—" : deltaDisplay,
    direction,
    movementState: membershipState,
    currentRank: isFiniteNumber(currentRank) ? Number(currentRank) : currentRank,
    priorRank: isFiniteNumber(priorRank) ? Number(priorRank) : priorRank,
    rankDelta,
    rankDirection,
    rankDisplay: formatRankWithMovement({
      currentRank,
      priorRank,
      rankDelta,
      membershipState,
    }),
    comparabilityStatus: "COMPARABLE",
    resolver: SAME_ROW_COMPARISON_SAME_CANONICAL_RESOLVER,
  };
}
