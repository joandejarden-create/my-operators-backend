/**
 * Discovery status vocabulary for hotel universe expansion (identity only).
 */

export const DISCOVERY_STATUS = Object.freeze({
  DISCOVERED: "DISCOVERED",
  MATCHED: "MATCHED",
  NEW_HOTEL: "NEW_HOTEL",
  AMBIGUOUS: "AMBIGUOUS",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  REJECTED: "REJECTED",
});

export const COVERAGE_FLAG = Object.freeze({
  COMPLETE: "COMPLETE",
  GOOD: "GOOD",
  PARTIAL: "PARTIAL",
  POOR: "POOR",
  UNKNOWN: "UNKNOWN",
});

/**
 * Map identity-resolve match_status → discovery status.
 */
export function mapIdentityToDiscoveryStatus(resolveResult, opts = {}) {
  const status = String(resolveResult?.match_status || "").toLowerCase();
  const hasMinFields = Boolean(opts.hasMinFields);
  const inferredCity = Boolean(opts.inferredCity);

  if (status === "exact" || status === "strong" || status === "probable") {
    return DISCOVERY_STATUS.MATCHED;
  }
  if (status === "ambiguous") {
    return DISCOVERY_STATUS.AMBIGUOUS;
  }
  if (status === "new") {
    if (!hasMinFields) return DISCOVERY_STATUS.REVIEW_REQUIRED;
    if (inferredCity) return DISCOVERY_STATUS.REVIEW_REQUIRED;
    return DISCOVERY_STATUS.NEW_HOTEL;
  }
  if (status === "insufficient") {
    return DISCOVERY_STATUS.REVIEW_REQUIRED;
  }
  return DISCOVERY_STATUS.DISCOVERED;
}

export function coverageFlagFromPct(pct, confidence) {
  if (pct == null || !Number.isFinite(pct)) return COVERAGE_FLAG.UNKNOWN;
  if (confidence === "low" && pct < 40) return COVERAGE_FLAG.UNKNOWN;
  if (pct >= 95) return COVERAGE_FLAG.COMPLETE;
  if (pct >= 70) return COVERAGE_FLAG.GOOD;
  if (pct >= 35) return COVERAGE_FLAG.PARTIAL;
  return COVERAGE_FLAG.POOR;
}

export function priorityFromFlag(flag, gapCount) {
  if (flag === COVERAGE_FLAG.UNKNOWN && gapCount > 0) return 1;
  if (flag === COVERAGE_FLAG.POOR) return 2;
  if (flag === COVERAGE_FLAG.PARTIAL) return 3;
  if (flag === COVERAGE_FLAG.GOOD) return 4;
  return 5;
}
