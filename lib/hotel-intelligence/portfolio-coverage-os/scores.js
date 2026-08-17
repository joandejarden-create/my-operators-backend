/**
 * Portfolio Coverage OS — dual scoring for CALA completeness + growth.
 * Read-only planning layer on top of the coverage dashboard.
 */

export const PORTFOLIO_OS_VERSION = "cala-portfolio-coverage-os-v1";

export const MATURITY_STATUS = Object.freeze({
  COMPLETE: "COMPLETE",
  NEAR_COMPLETE: "NEAR_COMPLETE",
  ADVANCING: "ADVANCING",
  PARTIAL: "PARTIAL",
  EARLY: "EARLY",
  CRITICAL: "CRITICAL",
  UNKNOWN: "UNKNOWN",
});

export const READINESS = Object.freeze({
  READY: "READY",
  BLOCKED: "BLOCKED",
  LOW_CONFIDENCE: "LOW_CONFIDENCE",
  NO_DISCOVERY_SOURCE: "NO_DISCOVERY_SOURCE",
  REQUIRES_NEW_SOURCE: "REQUIRES_NEW_SOURCE",
});

export const RECOMMENDED_ACTION = Object.freeze({
  HOLD_ENRICHMENT: "HOLD_ENRICHMENT",
  GROWTH_BATCH: "GROWTH_BATCH",
  PORTFOLIO_BATCH: "PORTFOLIO_BATCH",
  SOURCE_EXPANSION: "SOURCE_EXPANSION",
  HBX_UNBLOCK: "HBX_UNBLOCK",
  MONITOR: "MONITOR",
});

/**
 * Maturity status — describes coverage journey, not raw % alone.
 */
export function maturityStatusFromRow(row = {}) {
  const pct = row.coverage_pct;
  const hasEstimate =
    row.estimated_hotel_universe != null && row.estimated_hotel_universe > 0;
  const candidates = row.discovery_candidates_available || 0;
  const census = row.current_dealality_hotels || 0;

  if (!hasEstimate && census === 0) return MATURITY_STATUS.UNKNOWN;
  if (pct == null && !hasEstimate) return MATURITY_STATUS.UNKNOWN;
  if (pct >= 95) return MATURITY_STATUS.COMPLETE;
  if (pct >= 80) return MATURITY_STATUS.NEAR_COMPLETE;
  if (pct >= 50) return MATURITY_STATUS.ADVANCING;
  if (pct >= 20) return MATURITY_STATUS.PARTIAL;
  if (pct > 0 && pct < 20) return MATURITY_STATUS.EARLY;
  if (pct === 0 && (candidates > 0 || hasEstimate)) return MATURITY_STATUS.CRITICAL;
  if (pct === 0) return MATURITY_STATUS.CRITICAL;
  return MATURITY_STATUS.UNKNOWN;
}

/**
 * Portfolio Coverage Score 0–100
 * "How important is this country to complete CALA coverage?"
 * Favors zero/low coverage and under-represented geos — NOT raw hotel count.
 */
export function scorePortfolioCoverage(row = {}, ctx = {}) {
  const pct = row.coverage_pct;
  const census = Number(row.current_dealality_hotels || 0);
  const region = row.region || "";
  const tourism = row.tourism_priority || "C";
  const conf = String(row.estimation_confidence || row.confidence || "medium");
  const maturity = row.maturity_status || maturityStatusFromRow(row);

  // Inverse coverage weight (0% → 40 pts, 100% → 0)
  let score = 0;
  if (pct == null || !Number.isFinite(pct)) {
    score += 35; // unknown but in registry — completeness risk
  } else {
    score += ((100 - Math.min(100, Math.max(0, pct))) / 100) * 40;
  }

  // Zero / unrepresented boost
  if (census === 0) score += 25;
  else if (census > 0 && census <= 5) score += 12;
  else if (pct != null && pct < 20) score += 10;

  // Strategic completeness (tourism priority from registry)
  const tourismPts = { S: 12, A: 9, B: 6, C: 3 }[tourism] ?? 4;
  score += tourismPts;

  // Regional balance: boost regions that are under-covered overall
  const regionHealth = ctx.regionHealth?.[region];
  if (regionHealth != null && regionHealth < 40) score += 10;
  else if (regionHealth != null && regionHealth < 60) score += 5;

  // Discovery confidence (can we act?)
  if (conf === "high") score += 8;
  else if (conf === "medium") score += 5;
  else score += 2;

  // Maturity urgency
  if (maturity === MATURITY_STATUS.CRITICAL) score += 5;
  if (maturity === MATURITY_STATUS.UNKNOWN) score += 4;

  // Soft penalty if already complete
  if (maturity === MATURITY_STATUS.COMPLETE) score = Math.min(score, 15);
  if (maturity === MATURITY_STATUS.NEAR_COMPLETE) score = Math.min(score, 25);

  return Math.round(Math.max(0, Math.min(100, score)) * 10) / 10;
}

/**
 * Growth Score 0–100
 * "How many additional hotels can this country contribute?"
 */
export function scoreGrowth(row = {}) {
  const missing = Number(row.estimated_missing_hotels || 0);
  const candidates = Number(row.discovery_candidates_available || 0);
  const holds = Number(row.estimation_components?.holds || 0);
  const universe = Number(row.estimated_hotel_universe || 0);
  const dup = Number(row.expected_duplicate_risk || 0.05);
  const effort = Number(row.expected_effort || 0.5);

  let score = 0;
  // Missing hotels (cap at 5000)
  score += Math.min(1, missing / 5000) * 40;
  // Discovery stock
  score += Math.min(1, Math.max(candidates, holds) / 4000) * 30;
  // Universe scale
  score += Math.min(1, universe / 5000) * 15;
  // Efficiency: lower dup + lower effort → higher
  score += (1 - Math.min(1, dup / 0.2)) * 8;
  score += (1 - Math.min(1, effort)) * 7;

  if (candidates === 0 && holds === 0) score = Math.min(score, 20);

  return Math.round(Math.max(0, Math.min(100, score)) * 10) / 10;
}

/**
 * Country readiness for discovery action.
 */
export function classifyReadiness(row = {}) {
  const candidates = row.discovery_candidates_available || 0;
  const holds = row.estimation_components?.holds || 0;
  const stock = Math.max(candidates, holds);
  const conf = String(row.estimation_confidence || row.confidence || "low");
  const hbxBlocked = !row.hbx_wave1_searched && stock > 0;
  const maturity = row.maturity_status || maturityStatusFromRow(row);

  if (maturity === MATURITY_STATUS.COMPLETE) {
    return {
      readiness: READINESS.READY,
      note: "Coverage complete — enrichment/monitor only",
    };
  }

  if (stock === 0 && (row.estimated_hotel_universe == null || row.estimated_hotel_universe === 0)) {
    return {
      readiness: READINESS.NO_DISCOVERY_SOURCE,
      note: "No Cvent/HBX/hold stock in project — requires new source",
    };
  }

  if (stock === 0 && (row.estimated_missing_hotels || 0) > 0) {
    return {
      readiness: READINESS.REQUIRES_NEW_SOURCE,
      note: "Gap remains but discovery candidates exhausted",
    };
  }

  if (conf === "low" && stock > 0) {
    return {
      readiness: READINESS.LOW_CONFIDENCE,
      note: "Candidates exist but estimation confidence is low",
    };
  }

  // HBX auth block is a soft block for high-quality shells, not a hard stop for Cvent discovery
  if (hbxBlocked && stock > 0 && conf === "medium") {
    return {
      readiness: READINESS.READY,
      note: "Cvent/hold stock available; HBX unavailable outside Wave1",
      soft_blocker: "HBX_GEOGRAPHY_403",
    };
  }

  if (stock > 0) {
    return {
      readiness: READINESS.READY,
      note: "Known discovery stock available for factory batch",
    };
  }

  return {
    readiness: READINESS.BLOCKED,
    note: "Unable to proceed without source or unlock",
  };
}

export function recommendedAction(row = {}) {
  const maturity = row.maturity_status;
  const readiness = row.readiness;
  const growth = row.growth_score || 0;
  const portfolio = row.portfolio_coverage_score || 0;

  if (maturity === MATURITY_STATUS.COMPLETE || maturity === MATURITY_STATUS.NEAR_COMPLETE) {
    return RECOMMENDED_ACTION.HOLD_ENRICHMENT;
  }
  if (
    readiness === READINESS.NO_DISCOVERY_SOURCE ||
    readiness === READINESS.REQUIRES_NEW_SOURCE
  ) {
    return RECOMMENDED_ACTION.SOURCE_EXPANSION;
  }
  if (readiness === READINESS.BLOCKED) return RECOMMENDED_ACTION.HBX_UNBLOCK;
  if (growth >= 45 && (row.discovery_candidates_available || 0) >= 200) {
    return RECOMMENDED_ACTION.GROWTH_BATCH;
  }
  if (portfolio >= 55 || maturity === MATURITY_STATUS.CRITICAL || maturity === MATURITY_STATUS.EARLY) {
    return RECOMMENDED_ACTION.PORTFOLIO_BATCH;
  }
  if (readiness === READINESS.READY) return RECOMMENDED_ACTION.PORTFOLIO_BATCH;
  return RECOMMENDED_ACTION.MONITOR;
}

/**
 * Region health = average coverage % of countries in region (nulls as 0).
 */
export function computeRegionHealth(rows = []) {
  const buckets = {};
  for (const r of rows) {
    const region = r.region || "Unknown";
    if (!buckets[region]) buckets[region] = { sum: 0, n: 0 };
    buckets[region].sum += r.coverage_pct == null ? 0 : r.coverage_pct;
    buckets[region].n += 1;
  }
  const out = {};
  for (const [region, b] of Object.entries(buckets)) {
    out[region] = b.n ? Math.round((b.sum / b.n) * 10) / 10 : 0;
  }
  return out;
}
