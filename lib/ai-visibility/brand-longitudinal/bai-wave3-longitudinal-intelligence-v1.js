/**
 * BAI Wave 3 — Longitudinal Intelligence V1 (internal candidate / certified corpus).
 *
 * Uses existing certified Period 2 current-vs-prior.json + owner-intent chg helpers.
 * Does not call providers. Does not promote Period 2.
 */

import fs from "fs";
import path from "path";
import {
  formatGovernedDeltaDisplay,
  formatRankWithMovement,
  resolveRankDirection,
  resolveRowLevelPriorComparisonV1,
  DELTA_UNIT,
  ROW_MEMBERSHIP_STATE,
  RANK_DIRECTION,
} from "../../ai-demand-positioning/longitudinal/resolve-row-level-prior-comparison-v1.js";
import {
  buildOwnerIntentChgVsPrior,
  selectCurrentAndPriorPeriods,
  listGovernedMeasurementPeriods,
  toCustomerSafeChgVsPrior,
} from "../competitive-moat/owner-intent-chg-vs-prior.js";
import { SCENARIO_IDS as S } from "../competitive-moat/benchmark-brand-ids.js";
import {
  BAI_PERIOD_2_CANDIDATE_ID,
  BAI_VIEW_MODE,
  resolveBaiPriorComparablePeriodV1,
  BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER,
  BAI_UNPROMOTED_PERIOD_ISOLATION,
} from "./resolve-bai-prior-comparable-period-v1.js";
import {
  BRAND_LONGITUDINAL_STORE_ROOT,
} from "./measurement-period.js";

export const BAI_PRIOR_RUN_DELTA_NULL_ZERO_INTEGRITY =
  "BAI_PRIOR_RUN_DELTA_NULL_ZERO_INTEGRITY";
export const BAI_ABSOLUTE_RELATIVE_PERFORMANCE_SEPARATION =
  "BAI_ABSOLUTE_RELATIVE_PERFORMANCE_SEPARATION";
export const BAI_BRAND_LONGITUDINAL_IDENTITY_INTEGRITY =
  "BAI_BRAND_LONGITUDINAL_IDENTITY_INTEGRITY";
export const BAI_HISTORICAL_RANK_PERIOD_UNIVERSE_INTEGRITY =
  "BAI_HISTORICAL_RANK_PERIOD_UNIVERSE_INTEGRITY";
export const BAI_LONGITUDINAL_MEMBERSHIP_STATE_INTEGRITY =
  "BAI_LONGITUDINAL_MEMBERSHIP_STATE_INTEGRITY";
export const BAI_INTENT_PRIOR_RUN_RECONCILIATION =
  "BAI_INTENT_PRIOR_RUN_RECONCILIATION";
export const BAI_DELTA_DISPLAY_SEMANTICS_INTEGRITY =
  "BAI_DELTA_DISPLAY_SEMANTICS_INTEGRITY";
export const BAI_LONGITUDINAL_NARRATIVE_RECONCILIATION =
  "BAI_LONGITUDINAL_NARRATIVE_RECONCILIATION";
export const BAI_PRIOR_RUN_SAME_CANONICAL_SOURCE =
  "BAI_PRIOR_RUN_SAME_CANONICAL_SOURCE";
export const BAI_WAVE3_NO_CUSTOMER_PUBLICATION_MUTATION =
  "BAI_WAVE3_NO_CUSTOMER_PUBLICATION_MUTATION";

export const PERFORMANCE_DIRECTION = Object.freeze({
  IMPROVED: "IMPROVED",
  DECLINED: "DECLINED",
  STABLE: "STABLE",
  UNAVAILABLE: "UNAVAILABLE",
});

const MARRIOTT_BRAND_IDS = Object.freeze([
  "recEJCTDj1zrsjPM6", // Autograph
  "recCvV0PuZOi8c3hC", // Tribute
  "rec02zPClpWUTCyXM", // Design Hotels
  "recIPuBC50fv13zRR", // Westin
  "rec9aZp7GHtzUEg0c", // AC Hotels
]);

function isFiniteNumber(n) {
  return n != null && Number.isFinite(Number(n));
}

function round1(n) {
  return Math.round(Number(n) * 10) / 10;
}

function toPresencePct(raw) {
  if (!isFiniteNumber(raw)) return null;
  const n = Number(raw);
  // Corpus stores 0–100 percentages (e.g. 74.42).
  return round1(n);
}

function presenceDeltaPp(currentPct, priorPct) {
  if (!isFiniteNumber(currentPct) || !isFiniteNumber(priorPct)) return null;
  return round1(Number(currentPct) - Number(priorPct));
}

function classifyAbsoluteRelative({ brandDeltaPp, peerMeanDeltaPp }) {
  const abs =
    !isFiniteNumber(brandDeltaPp)
      ? PERFORMANCE_DIRECTION.UNAVAILABLE
      : Math.abs(brandDeltaPp) < 0.05
        ? PERFORMANCE_DIRECTION.STABLE
        : brandDeltaPp > 0
          ? PERFORMANCE_DIRECTION.IMPROVED
          : PERFORMANCE_DIRECTION.DECLINED;

  let rel = PERFORMANCE_DIRECTION.UNAVAILABLE;
  if (isFiniteNumber(brandDeltaPp) && isFiniteNumber(peerMeanDeltaPp)) {
    const relativeGap = round1(brandDeltaPp - peerMeanDeltaPp);
    if (Math.abs(relativeGap) < 0.05) rel = PERFORMANCE_DIRECTION.STABLE;
    else if (relativeGap > 0) rel = PERFORMANCE_DIRECTION.IMPROVED;
    else rel = PERFORMANCE_DIRECTION.DECLINED;
  }

  return {
    absolutePerformance: abs,
    relativePerformance: rel,
    brandDeltaPp: isFiniteNumber(brandDeltaPp) ? Number(brandDeltaPp) : null,
    peerMeanDeltaPp: isFiniteNumber(peerMeanDeltaPp)
      ? Number(peerMeanDeltaPp)
      : null,
    relativeGapPp:
      isFiniteNumber(brandDeltaPp) && isFiniteNumber(peerMeanDeltaPp)
        ? round1(brandDeltaPp - peerMeanDeltaPp)
        : null,
  };
}

function absoluteRelativeCopy(sep) {
  if (
    sep.absolutePerformance === PERFORMANCE_DIRECTION.UNAVAILABLE ||
    sep.relativePerformance === PERFORMANCE_DIRECTION.UNAVAILABLE
  ) {
    return "Comparable absolute and relative movement is not available for this brand.";
  }
  const absWord =
    sep.absolutePerformance === PERFORMANCE_DIRECTION.IMPROVED
      ? "improved"
      : sep.absolutePerformance === PERFORMANCE_DIRECTION.DECLINED
        ? "declined"
        : "was stable";
  const relWord =
    sep.relativePerformance === PERFORMANCE_DIRECTION.IMPROVED
      ? "strengthened"
      : sep.relativePerformance === PERFORMANCE_DIRECTION.DECLINED
        ? "weakened"
        : "held steady";
  return `Visibility ${absWord} in absolute terms (${formatGovernedDeltaDisplay({
    delta: sep.brandDeltaPp,
    deltaUnit: DELTA_UNIT.PP,
  })}), while relative position ${relWord} versus peers (${formatGovernedDeltaDisplay({
    delta: sep.relativeGapPp,
    deltaUnit: DELTA_UNIT.PP,
  })} vs peer mean change).`;
}

/**
 * Rank brands within a single period's universe by presence (higher = better = #1).
 * Historical ranks use that period's values only — never re-rank prior with current universe.
 */
export function rankBrandsInPeriodUniverse(rows, presenceKey) {
  const eligible = (rows || [])
    .filter((r) => r && r.brandId && isFiniteNumber(r[presenceKey]))
    .map((r) => ({
      brandId: String(r.brandId),
      presence: Number(r[presenceKey]),
    }))
    .sort((a, b) => {
      if (b.presence !== a.presence) return b.presence - a.presence;
      return a.brandId.localeCompare(b.brandId);
    });
  const rankById = new Map();
  eligible.forEach((row, idx) => {
    rankById.set(row.brandId, idx + 1);
  });
  return {
    universeSize: eligible.length,
    rankById,
    gate: BAI_HISTORICAL_RANK_PERIOD_UNIVERSE_INTEGRITY,
  };
}

export function loadCertifiedPeriodCompareArtifact(opts = {}) {
  const periodId = opts.periodId || BAI_PERIOD_2_CANDIDATE_ID;
  const storeRoot = opts.storeRoot || BRAND_LONGITUDINAL_STORE_ROOT;
  const filePath = path.join(storeRoot, periodId, "current-vs-prior.json");
  if (!fs.existsSync(filePath)) {
    return { ok: false, reason: "missing_current_vs_prior_artifact", periodId, filePath };
  }
  const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return {
    ok: true,
    periodId,
    filePath,
    CURRENT_DATE: raw.CURRENT_DATE || null,
    PRIOR_DATE: raw.PRIOR_DATE || null,
    brandCompare: Array.isArray(raw.brandCompare) ? raw.brandCompare : [],
    COHORT_CHANGED: raw.COHORT_CHANGED || null,
  };
}

function membershipFromSeriesState(row, priorPresence) {
  const state = String(row.SERIES_STATE || "");
  if (state === "NEW" || state === "NEWLY_OBSERVED") return ROW_MEMBERSHIP_STATE.NEW;
  if (state === "EXITED" || state === "NO_LONGER_OBSERVED") {
    return ROW_MEMBERSHIP_STATE.EXITED;
  }
  if (state === "RETURNED") return ROW_MEMBERSHIP_STATE.RETURNED;
  if (!isFiniteNumber(priorPresence) && isFiniteNumber(row.CURRENT_PRESENCE)) {
    return ROW_MEMBERSHIP_STATE.NEW;
  }
  if (isFiniteNumber(priorPresence) && !isFiniteNumber(row.CURRENT_PRESENCE)) {
    return ROW_MEMBERSHIP_STATE.EXITED;
  }
  return ROW_MEMBERSHIP_STATE.SAME;
}

/**
 * Build brand-level Prior Run rows keyed by canonical brandId.
 */
export function buildBaiBrandPriorRunRowsV1(opts = {}) {
  const artifact = opts.artifact || loadCertifiedPeriodCompareArtifact(opts);
  if (!artifact.ok) return { ok: false, ...artifact };

  const periodResolve =
    opts.periodResolve ||
    resolveBaiPriorComparablePeriodV1({
      viewMode: BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA,
      currentPeriodId: artifact.periodId,
    });

  const rows = artifact.brandCompare;
  const currentRanks = rankBrandsInPeriodUniverse(rows, "CURRENT_PRESENCE");
  const priorRanks = rankBrandsInPeriodUniverse(rows, "PRIOR_PRESENCE");

  const parentFilter = opts.parentCompanyName
    ? String(opts.parentCompanyName).toLowerCase()
    : null;
  const brandIdAllow = opts.brandIds
    ? new Set((opts.brandIds || []).map(String))
    : null;

  const filtered = rows.filter((r) => {
    if (brandIdAllow && !brandIdAllow.has(String(r.brandId))) return false;
    if (parentFilter) {
      return String(r.PARENT || "").toLowerCase().includes(parentFilter);
    }
    return true;
  });

  // Peer mean delta uses the full period universe (not only the parent filter)
  // so relative performance is vs the certified monitoring cohort.
  const universeDeltas = rows
    .map((r) =>
      presenceDeltaPp(toPresencePct(r.CURRENT_PRESENCE), toPresencePct(r.PRIOR_PRESENCE))
    )
    .filter(isFiniteNumber);
  const peerMeanDeltaPp = universeDeltas.length
    ? round1(
        universeDeltas.reduce((a, b) => a + b, 0) / universeDeltas.length
      )
    : null;

  const brandRows = filtered.map((r) => {
    const brandId = String(r.brandId || "");
    const currentPresence = toPresencePct(r.CURRENT_PRESENCE);
    const priorPresence = toPresencePct(r.PRIOR_PRESENCE);
    const membershipState = membershipFromSeriesState(r, priorPresence);
    const comparison = resolveRowLevelPriorComparisonV1({
      measurementFamily: "BAI_PRESENCE",
      propertyId: brandId,
      currentPeriodId: periodResolve.currentPeriodId,
      priorPeriodId: periodResolve.priorPeriodId || "DEMO_VALIDATION",
      scopeType: "brand",
      scopeKey: brandId,
      canonicalRowId: brandId,
      metric: "aiPresenceRate",
      currentValue: currentPresence,
      priorValue: priorPresence,
      currentRank: currentRanks.rankById.get(brandId) ?? null,
      priorRank: priorRanks.rankById.get(brandId) ?? null,
      earlierHadEntity: isFiniteNumber(priorPresence),
      comparable:
        membershipState === ROW_MEMBERSHIP_STATE.SAME ||
        membershipState === ROW_MEMBERSHIP_STATE.RETURNED,
      deltaUnit: DELTA_UNIT.PP,
    });

    const sep = classifyAbsoluteRelative({
      brandDeltaPp: comparison.delta,
      peerMeanDeltaPp,
    });

    return {
      brandId,
      brandName: r.BRAND || null,
      parentCompanyName: r.PARENT || null,
      currentPresence,
      priorPresence,
      deltaPp: comparison.delta,
      deltaDisplay: comparison.deltaDisplay,
      direction: comparison.direction,
      currentRank: comparison.currentRank,
      priorRank: comparison.priorRank,
      rankDelta: comparison.rankDelta,
      rankDirection: resolveRankDirection(comparison.rankDelta),
      rankDisplay: formatRankWithMovement({
        currentRank: comparison.currentRank,
        priorRank: comparison.priorRank,
        rankDelta: comparison.rankDelta,
        membershipState,
      }),
      membershipState,
      questionsMissingCurrent: isFiniteNumber(r.CURRENT_QM)
        ? Number(r.CURRENT_QM)
        : null,
      questionsMissingPrior: isFiniteNumber(r.PRIOR_QM)
        ? Number(r.PRIOR_QM)
        : null,
      absoluteRelative: sep,
      absoluteRelativeNarrative: absoluteRelativeCopy(sep),
      comparisonPeriodId: periodResolve.priorPeriodId,
      comparisonPeriodDate: periodResolve.priorPeriodDate || artifact.PRIOR_DATE,
      currentPeriodId: periodResolve.currentPeriodId,
      currentPeriodDate: periodResolve.currentPeriodDate || artifact.CURRENT_DATE,
      gate: BAI_BRAND_LONGITUDINAL_IDENTITY_INTEGRITY,
    };
  });

  return {
    ok: true,
    gate: BAI_PRIOR_RUN_SAME_CANONICAL_SOURCE,
    periodResolve,
    peerMeanDeltaPp,
    currentUniverseSize: currentRanks.universeSize,
    priorUniverseSize: priorRanks.universeSize,
    brands: brandRows,
  };
}

export function buildBaiPortfolioPriorRunSummaryV1(brandBundle) {
  if (!brandBundle?.ok) return { ok: false, reason: "missing_brand_bundle" };
  const brands = brandBundle.brands || [];
  const withDelta = brands.filter((b) => isFiniteNumber(b.deltaPp));
  const improving = withDelta.filter((b) => b.deltaPp > 0.05);
  const declining = withDelta.filter((b) => b.deltaPp < -0.05);
  const stable = withDelta.filter((b) => Math.abs(b.deltaPp) <= 0.05);

  const mean = (arr, key) => {
    const vals = arr.map((b) => b[key]).filter(isFiniteNumber);
    if (!vals.length) return null;
    return round1(vals.reduce((a, b) => a + b, 0) / vals.length);
  };

  const currentPresence = mean(brands, "currentPresence");
  const priorPresence = mean(brands, "priorPresence");
  const portfolioDeltaPp = presenceDeltaPp(currentPresence, priorPresence);
  const sep = classifyAbsoluteRelative({
    brandDeltaPp: portfolioDeltaPp,
    peerMeanDeltaPp: brandBundle.peerMeanDeltaPp,
  });

  const strongestGain = [...withDelta].sort((a, b) => b.deltaPp - a.deltaPp)[0] || null;
  const largestLoss = [...withDelta].sort((a, b) => a.deltaPp - b.deltaPp)[0] || null;

  return {
    ok: true,
    currentPresence,
    priorPresence,
    portfolioDeltaPp,
    portfolioDeltaDisplay: formatGovernedDeltaDisplay({
      delta: portfolioDeltaPp,
      deltaUnit: DELTA_UNIT.PP,
    }),
    brandsImproving: improving.length,
    brandsDeclining: declining.length,
    brandsStable: stable.length,
    strongestPositiveMover: strongestGain
      ? {
          brandId: strongestGain.brandId,
          brandName: strongestGain.brandName,
          deltaPp: strongestGain.deltaPp,
          deltaDisplay: strongestGain.deltaDisplay,
        }
      : null,
    largestVisibilityLoss: largestLoss
      ? {
          brandId: largestLoss.brandId,
          brandName: largestLoss.brandName,
          deltaPp: largestLoss.deltaPp,
          deltaDisplay: largestLoss.deltaDisplay,
        }
      : null,
    absoluteRelative: sep,
    absoluteRelativeNarrative: absoluteRelativeCopy(sep),
  };
}

export function buildBaiExecutiveLongitudinalNarrativeV1({
  portfolio,
  brands,
  periodResolve,
} = {}) {
  if (!portfolio?.ok) {
    return {
      available: false,
      narrative: null,
      gate: BAI_LONGITUDINAL_NARRATIVE_RECONCILIATION,
    };
  }
  const parts = [];
  parts.push(
    `Versus the prior comparable run (${periodResolve?.priorPeriodDate || "prior"}), portfolio AI Presence moved from ${
      portfolio.priorPresence != null ? portfolio.priorPresence.toFixed(1) + "%" : "—"
    } to ${
      portfolio.currentPresence != null ? portfolio.currentPresence.toFixed(1) + "%" : "—"
    } (${portfolio.portfolioDeltaDisplay || "—"}).`
  );
  parts.push(portfolio.absoluteRelativeNarrative);
  if (portfolio.strongestPositiveMover) {
    parts.push(
      `Strongest positive mover: ${portfolio.strongestPositiveMover.brandName} (${portfolio.strongestPositiveMover.deltaDisplay}).`
    );
  }
  if (portfolio.largestVisibilityLoss) {
    parts.push(
      `Largest visibility loss: ${portfolio.largestVisibilityLoss.brandName} (${portfolio.largestVisibilityLoss.deltaDisplay}).`
    );
  }
  const lead = (brands || []).slice().sort((a, b) => {
    const ra = a.currentRank ?? 999;
    const rb = b.currentRank ?? 999;
    return ra - rb;
  })[0];
  if (lead?.rankDisplay) {
    parts.push(
      `Best competitive position now: ${lead.brandName} ${lead.rankDisplay}.`
    );
  }
  parts.push(
    `Next inspection: review brands with the largest absolute decline and any relative weakening versus peers.`
  );

  return {
    available: true,
    title: "Prior Run Position",
    narrative: parts.join(" "),
    gate: BAI_LONGITUDINAL_NARRATIVE_RECONCILIATION,
  };
}

/**
 * Owner Intent prior-run reconciliation for a subject brand (certified scopes only).
 * Customer-safe: no canonical prompt text / prompt IDs in output.
 */
export function buildBaiIntentPriorRunReconciliationV1(opts = {}) {
  const subjectBrandId = opts.subjectBrandId || MARRIOTT_BRAND_IDS[0];
  const geography = opts.geography || "CALA";
  const providerScope = opts.providerScope || "all_providers";
  const periods = listGovernedMeasurementPeriods(opts);
  const selected = selectCurrentAndPriorPeriods({
    periods,
    geography,
    currentPeriodId: opts.currentPeriodId || BAI_PERIOD_2_CANDIDATE_ID,
    anchorToLiveCurrent: true,
  });

  const scenarioIds = opts.scenarioIds || [
    S.SOFT_BRAND,
    S.CONVERSION_SUITABILITY,
    S.OWNER_FLEXIBILITY,
    S.LIFESTYLE,
  ];

  const intents = [];
  for (const scenarioId of scenarioIds) {
    const history = buildOwnerIntentChgVsPrior({
      subjectBrandId,
      scenarioId,
      providerScope,
      geography,
      language: opts.language || "en",
      currentPeriodId: selected.currentPeriod?.measurementPeriodId,
      periods,
    });
    const safe = toCustomerSafeChgVsPrior(history);
    intents.push({
      ownerIntentLabel: scenarioId.replace(/_/g, " "),
      scenarioKey: scenarioId,
      currentIndex: safe.currentIndex ?? null,
      priorIndex: safe.priorIndex ?? null,
      deltaPoints: safe.indexChangePoints ?? null,
      deltaDisplay: safe.chgVsPriorDisplay ?? "Insufficient History",
      comparisonStatus: safe.comparisonStatus,
      // Never expose promptId / prompt text
    });
  }

  const numeric = intents.filter((i) => isFiniteNumber(i.deltaPoints));
  const strongestGain = [...numeric].sort((a, b) => b.deltaPoints - a.deltaPoints)[0] || null;
  const largestLoss = [...numeric].sort((a, b) => a.deltaPoints - b.deltaPoints)[0] || null;

  return {
    ok: true,
    gate: BAI_INTENT_PRIOR_RUN_RECONCILIATION,
    subjectBrandId,
    periodResolve: {
      currentPeriodId: selected.currentPeriod?.measurementPeriodId || null,
      priorPeriodId: selected.priorPeriod?.measurementPeriodId || null,
    },
    intents,
    strongestGain,
    largestLoss,
  };
}

/**
 * Full internal candidate Wave 3 payload for a parent portfolio.
 */
export function buildBaiWave3LongitudinalIntelligenceV1(opts = {}) {
  const viewMode =
    opts.viewMode || BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA;
  if (viewMode !== BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA) {
    return {
      ok: false,
      reason: "customer_published_view_has_no_period2_longitudinal_payload",
      gate: BAI_WAVE3_NO_CUSTOMER_PUBLICATION_MUTATION,
      viewMode,
      longitudinal: null,
    };
  }

  const periodResolve = resolveBaiPriorComparablePeriodV1({
    viewMode,
    currentPeriodId: opts.currentPeriodId || BAI_PERIOD_2_CANDIDATE_ID,
    geography: opts.geography || "CALA",
    storeRoot: opts.storeRoot,
  });

  const brandBundle = buildBaiBrandPriorRunRowsV1({
    ...opts,
    periodResolve,
    parentCompanyName: opts.parentCompanyName || "Marriott",
    brandIds: opts.brandIds || MARRIOTT_BRAND_IDS,
  });
  if (!brandBundle.ok) {
    return { ok: false, periodResolve, ...brandBundle };
  }

  const portfolio = buildBaiPortfolioPriorRunSummaryV1(brandBundle);
  const executiveLongitudinal = buildBaiExecutiveLongitudinalNarrativeV1({
    portfolio,
    brands: brandBundle.brands,
    periodResolve,
  });

  const intentByBrand = {};
  for (const b of brandBundle.brands) {
    intentByBrand[b.brandId] = buildBaiIntentPriorRunReconciliationV1({
      subjectBrandId: b.brandId,
      currentPeriodId: periodResolve.currentPeriodId,
      geography: opts.geography || "CALA",
      storeRoot: opts.storeRoot,
    });
  }

  return {
    ok: true,
    viewMode,
    gates: {
      BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER,
      BAI_UNPROMOTED_PERIOD_ISOLATION,
      BAI_PRIOR_RUN_DELTA_NULL_ZERO_INTEGRITY,
      BAI_ABSOLUTE_RELATIVE_PERFORMANCE_SEPARATION,
      BAI_BRAND_LONGITUDINAL_IDENTITY_INTEGRITY,
      BAI_HISTORICAL_RANK_PERIOD_UNIVERSE_INTEGRITY,
      BAI_LONGITUDINAL_MEMBERSHIP_STATE_INTEGRITY,
      BAI_INTENT_PRIOR_RUN_RECONCILIATION,
      BAI_DELTA_DISPLAY_SEMANTICS_INTEGRITY,
      BAI_LONGITUDINAL_NARRATIVE_RECONCILIATION,
      BAI_PRIOR_RUN_SAME_CANONICAL_SOURCE,
      BAI_WAVE3_NO_CUSTOMER_PUBLICATION_MUTATION,
    },
    periodResolve,
    portfolio,
    brands: brandBundle.brands,
    peerMeanDeltaPp: brandBundle.peerMeanDeltaPp,
    executiveLongitudinal,
    intentByBrand,
    LIVE_PROVIDER_CALLS: 0,
    PERIOD_2_PUBLICATION_STATE: "UNPROMOTED",
  };
}

export const BAI_WAVE3_MARRIOTT_BRAND_IDS = MARRIOTT_BRAND_IDS;
