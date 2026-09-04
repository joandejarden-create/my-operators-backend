/**
 * resolveBaiCustomerLongitudinalHistoryV1
 *
 * Canonical customer-facing longitudinal history for published Brand AI.
 * Source of truth: certified current-vs-prior artifact for the published period
 * (same Wave 3 / Prior Run corpus) — NOT federated batch trend summaries alone.
 *
 * CURRENT (published P2): 2026-09-03
 * PRIOR: DEMO_VALIDATION / 2026-08-14
 * PERIOD COUNT: 2
 *
 * Grain:
 * - brand → brandId certified Presence
 * - parent → mean of parent brand certified Presence (portfolio rollup)
 *
 * No provider calls. No publication mutation. Aug 18 excluded.
 */

import {
  BAI_PERIOD_2_CANDIDATE_ID,
  BAI_PERIOD_2_CUSTOMER_CURRENT_DATE,
  BAI_P2_CUSTOMER_PRIOR_PERIOD_ID,
  BAI_P2_CUSTOMER_PRIOR_DATE,
  BAI_CUSTOMER_PUBLISHED_PERIOD_ID,
  BAI_PERIOD_1_INTERNAL_HISTORY_DATE,
  resolveBaiPriorComparablePeriodV1,
  BAI_VIEW_MODE,
} from "./resolve-bai-prior-comparable-period-v1.js";
import {
  loadCertifiedPeriodCompareArtifact,
} from "./bai-wave3-longitudinal-intelligence-v1.js";

export const BAI_ALL_TREND_SURFACES_USE_CANONICAL_HISTORY =
  "BAI_ALL_TREND_SURFACES_USE_CANONICAL_HISTORY";
export const BAI_SAME_LONGITUDINAL_CONCEPT_SAME_SOURCE =
  "BAI_SAME_LONGITUDINAL_CONCEPT_SAME_SOURCE";
export const BAI_CUSTOMER_COMPARABLE_PERIOD_COUNT_INTEGRITY =
  "BAI_CUSTOMER_COMPARABLE_PERIOD_COUNT_INTEGRITY";
export const BAI_LONGITUDINAL_GRAIN_INTEGRITY =
  "BAI_LONGITUDINAL_GRAIN_INTEGRITY";
export const BAI_AI_PRESENCE_OVER_TIME_PERIOD2_READY =
  "BAI_AI_PRESENCE_OVER_TIME_PERIOD2_READY";
export const BAI_BRAND_TRENDS_PERIOD2_READY =
  "BAI_BRAND_TRENDS_PERIOD2_READY";

export const BAI_CUSTOMER_LONGITUDINAL_HISTORY_SOURCE =
  "bai_customer_longitudinal_certified_v1";

function isFiniteNumber(n) {
  return typeof n === "number" && Number.isFinite(n);
}

function round1(n) {
  return Math.round(Number(n) * 10) / 10;
}

function toPresencePct(raw) {
  if (!isFiniteNumber(raw) && raw != null && raw !== "") {
    const n = Number(raw);
    if (!Number.isFinite(n)) return null;
    return n <= 1 ? round1(n * 100) : round1(n);
  }
  if (!isFiniteNumber(raw)) return null;
  return raw <= 1 ? round1(raw * 100) : round1(raw);
}

function presenceToRate(pctOrRate) {
  if (!isFiniteNumber(pctOrRate)) return null;
  const n = Number(pctOrRate);
  // Artifact stores percent (0–100); customer trend UI accepts rate or percent.
  return n > 1 ? n / 100 : n;
}

/**
 * Full cohort customer history object for the published (or candidate) period.
 */
export function resolveBaiCustomerLongitudinalHistoryV1(opts = {}) {
  const periodId =
    opts.periodId ||
    (BAI_CUSTOMER_PUBLISHED_PERIOD_ID === BAI_PERIOD_2_CANDIDATE_ID
      ? BAI_PERIOD_2_CANDIDATE_ID
      : opts.allowUnpromotedCandidate
        ? BAI_PERIOD_2_CANDIDATE_ID
        : BAI_CUSTOMER_PUBLISHED_PERIOD_ID);

  const published =
    BAI_CUSTOMER_PUBLISHED_PERIOD_ID === BAI_PERIOD_2_CANDIDATE_ID;

  // Only serve P2↔Aug14 customer history when P2 is published (or explicit candidate allow).
  if (
    periodId === BAI_PERIOD_2_CANDIDATE_ID &&
    !published &&
    !opts.allowUnpromotedCandidate
  ) {
    return {
      ok: false,
      reason: "period_not_customer_published",
      periodId,
      comparablePeriodCount: 0,
      periods: [],
      brands: {},
      LIVE_PROVIDER_CALLS: 0,
    };
  }

  const periodResolve =
    opts.periodResolve ||
    resolveBaiPriorComparablePeriodV1({
      viewMode: published
        ? BAI_VIEW_MODE.CUSTOMER_PUBLISHED
        : BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW,
      geography: opts.geography || "CALA",
      storeRoot: opts.storeRoot,
    });

  const artifact = loadCertifiedPeriodCompareArtifact({
    periodId,
    storeRoot: opts.storeRoot,
  });
  if (!artifact.ok) {
    return {
      ok: false,
      reason: artifact.reason || "missing_compare_artifact",
      periodId,
      comparablePeriodCount: 0,
      periods: [],
      brands: {},
      LIVE_PROVIDER_CALLS: 0,
    };
  }

  const currentDate =
    periodResolve.currentPeriodDate ||
    artifact.CURRENT_DATE ||
    BAI_PERIOD_2_CUSTOMER_CURRENT_DATE;
  const priorDate =
    periodResolve.priorPeriodDate ||
    artifact.PRIOR_DATE ||
    BAI_P2_CUSTOMER_PRIOR_DATE;
  const currentPeriodId =
    periodResolve.currentPeriodId || periodId || BAI_PERIOD_2_CANDIDATE_ID;
  const priorPeriodId =
    periodResolve.priorPeriodId || BAI_P2_CUSTOMER_PRIOR_PERIOD_ID;

  if (
    String(priorDate).includes(BAI_PERIOD_1_INTERNAL_HISTORY_DATE) ||
    String(priorPeriodId).includes("20260818")
  ) {
    return {
      ok: false,
      reason: "aug18_forbidden_in_customer_history",
      periodId,
      comparablePeriodCount: 0,
      LIVE_PROVIDER_CALLS: 0,
    };
  }

  const brands = {};
  for (const row of artifact.brandCompare || []) {
    const brandId = String(row.brandId || "");
    if (!brandId) continue;
    const currentPresencePct = toPresencePct(row.CURRENT_PRESENCE);
    const priorPresencePct = toPresencePct(row.PRIOR_PRESENCE);
    const currentRate = presenceToRate(row.CURRENT_PRESENCE);
    const priorRate = presenceToRate(row.PRIOR_PRESENCE);
    brands[brandId] = {
      brandId,
      brandName: row.BRAND || null,
      parentCompanyName: row.PARENT || null,
      grain: "brand",
      currentDate,
      priorDate,
      currentPeriodId,
      priorPeriodId,
      currentPresencePct,
      priorPresencePct,
      currentPresenceRate: currentRate,
      priorPresenceRate: priorRate,
      deltaPp:
        isFiniteNumber(currentPresencePct) && isFiniteNumber(priorPresencePct)
          ? round1(currentPresencePct - priorPresencePct)
          : null,
      points: [
        {
          date: priorDate,
          value: priorRate,
          valuePct: priorPresencePct,
          periodId: priorPeriodId,
          role: "prior",
          batchId: `customer_longitudinal_prior_${priorPeriodId}`,
          source: BAI_CUSTOMER_LONGITUDINAL_HISTORY_SOURCE,
          geographyKey: opts.geography || "CALA",
          language: opts.language || "en",
          provider: "all",
          metric: "aiPresenceRate",
          grain: "brand",
        },
        {
          date: currentDate,
          value: currentRate,
          valuePct: currentPresencePct,
          periodId: currentPeriodId,
          role: "current",
          batchId: `customer_longitudinal_current_${currentPeriodId}`,
          source: BAI_CUSTOMER_LONGITUDINAL_HISTORY_SOURCE,
          geographyKey: opts.geography || "CALA",
          language: opts.language || "en",
          provider: "all",
          metric: "aiPresenceRate",
          grain: "brand",
        },
      ].filter((p) => isFiniteNumber(p.value)),
    };
  }

  const periods = [
    {
      periodId: priorPeriodId,
      date: priorDate,
      role: "prior",
      label: priorDate,
    },
    {
      periodId: currentPeriodId,
      date: currentDate,
      role: "current",
      label: currentDate,
    },
  ];

  return {
    ok: true,
    source: BAI_CUSTOMER_LONGITUDINAL_HISTORY_SOURCE,
    gate: BAI_ALL_TREND_SURFACES_USE_CANONICAL_HISTORY,
    periodId: currentPeriodId,
    currentPeriodId,
    currentDate,
    priorPeriodId,
    priorDate,
    comparablePeriodCount: 2,
    periods,
    brands,
    brandIds: Object.keys(brands),
    aug18Excluded: true,
    LIVE_PROVIDER_CALLS: 0,
    LIVE_MUTATION: false,
  };
}

/**
 * Brand-grain customer trend points for getBrandTrendPayload / detail Trends.
 */
export function resolveBaiCustomerBrandHistoryPointsV1(opts = {}) {
  const brandId = String(opts.brandId || "").trim();
  if (!brandId) {
    return { ok: false, reason: "missing_brand_id", points: [] };
  }
  const history = resolveBaiCustomerLongitudinalHistoryV1(opts);
  if (!history.ok) {
    return { ok: false, reason: history.reason, points: [], history };
  }
  const row = history.brands[brandId];
  if (!row || !row.points || row.points.length < 2) {
    return {
      ok: false,
      reason: "brand_not_in_customer_longitudinal_cohort",
      points: [],
      history,
    };
  }
  return {
    ok: true,
    grain: "brand",
    brandId,
    points: row.points,
    comparablePeriodCount: history.comparablePeriodCount,
    currentDate: history.currentDate,
    priorDate: history.priorDate,
    currentPeriodId: history.currentPeriodId,
    priorPeriodId: history.priorPeriodId,
    source: history.source,
    history,
    LIVE_PROVIDER_CALLS: 0,
  };
}

/**
 * Parent-grain rollup points (mean of member brand Presence).
 */
export function resolveBaiCustomerParentHistoryPointsV1(opts = {}) {
  const parentKey = String(opts.parentCompanyKey || opts.parent || "")
    .trim()
    .toLowerCase();
  const history = resolveBaiCustomerLongitudinalHistoryV1(opts);
  if (!history.ok) {
    return { ok: false, reason: history.reason, points: [], history };
  }
  const members = Object.values(history.brands).filter((b) => {
    if (!parentKey) return true;
    return String(b.parentCompanyName || "")
      .toLowerCase()
      .includes(parentKey);
  });
  if (!members.length) {
    return { ok: false, reason: "no_parent_members", points: [], history };
  }
  const priorVals = members
    .map((m) => m.priorPresencePct)
    .filter(isFiniteNumber);
  const currentVals = members
    .map((m) => m.currentPresencePct)
    .filter(isFiniteNumber);
  const priorPct = priorVals.length
    ? round1(priorVals.reduce((a, b) => a + b, 0) / priorVals.length)
    : null;
  const currentPct = currentVals.length
    ? round1(currentVals.reduce((a, b) => a + b, 0) / currentVals.length)
    : null;
  const points = [
    {
      date: history.priorDate,
      value: presenceToRate(priorPct),
      valuePct: priorPct,
      periodId: history.priorPeriodId,
      role: "prior",
      batchId: `customer_longitudinal_prior_parent_${history.priorPeriodId}`,
      source: history.source,
      geographyKey: opts.geography || "CALA",
      language: opts.language || "en",
      provider: "all",
      metric: "aiPresenceRate",
      grain: "parent",
    },
    {
      date: history.currentDate,
      value: presenceToRate(currentPct),
      valuePct: currentPct,
      periodId: history.currentPeriodId,
      role: "current",
      batchId: `customer_longitudinal_current_parent_${history.currentPeriodId}`,
      source: history.source,
      geographyKey: opts.geography || "CALA",
      language: opts.language || "en",
      provider: "all",
      metric: "aiPresenceRate",
      grain: "parent",
    },
  ].filter((p) => isFiniteNumber(p.value));

  return {
    ok: points.length >= 2,
    grain: "parent",
    parentCompanyKey: parentKey || null,
    memberBrandIds: members.map((m) => m.brandId),
    points,
    comparablePeriodCount: 2,
    currentDate: history.currentDate,
    priorDate: history.priorDate,
    currentPresencePct: currentPct,
    priorPresencePct: priorPct,
    source: history.source,
    history,
    LIVE_PROVIDER_CALLS: 0,
  };
}

/**
 * Prefer canonical customer history when published; else leave legacy points.
 */
export function preferBaiCustomerCanonicalTrendPoints(opts = {}) {
  const legacyPoints = Array.isArray(opts.legacyPoints) ? opts.legacyPoints : [];
  const brandId = opts.brandId;
  if (BAI_CUSTOMER_PUBLISHED_PERIOD_ID !== BAI_PERIOD_2_CANDIDATE_ID) {
    return {
      usedCanonical: false,
      points: legacyPoints,
      reason: "customer_not_on_period_2",
    };
  }
  // Customer longitudinal Presence is period-scoped (all-providers grain).
  // Always prefer for entitled cohort brands once P2 is published.
  const canonical = resolveBaiCustomerBrandHistoryPointsV1({
    brandId,
    geography: opts.geography || "CALA",
    language: opts.language || "en",
    storeRoot: opts.storeRoot,
  });
  if (!canonical.ok || canonical.points.length < 2) {
    return {
      usedCanonical: false,
      points: legacyPoints,
      reason: canonical.reason || "canonical_unavailable",
      canonical,
    };
  }
  return {
    usedCanonical: true,
    points: canonical.points,
    reason: "published_p2_canonical_history",
    canonical,
    comparablePeriodCount: 2,
  };
}
