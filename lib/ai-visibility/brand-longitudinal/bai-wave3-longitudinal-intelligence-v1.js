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
  BAI_CUSTOMER_PUBLISHED_PERIOD_ID,
  BAI_VIEW_MODE,
  resolveBaiPriorComparablePeriodV1,
  BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER,
  BAI_UNPROMOTED_PERIOD_ISOLATION,
} from "./resolve-bai-prior-comparable-period-v1.js";
import {
  BRAND_LONGITUDINAL_STORE_ROOT,
} from "./measurement-period.js";
import {
  loadShowcaseCompaniesConfig,
  SHOWCASE_MONITORING_BRAND_COUNT,
} from "../brand-ai-showcase-companies.js";

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
export const BAI_WAVE3_FULL_19_BRAND_COHORT_COVERAGE =
  "BAI_WAVE3_FULL_19_BRAND_COHORT_COVERAGE";
export const BAI_WAVE3_ALL_PARENT_GROUPS_RECONCILED =
  "BAI_WAVE3_ALL_PARENT_GROUPS_RECONCILED";
export const BAI_WAVE3_NO_BRAND_LEFT_WITH_UNEXPLAINED_HISTORY =
  "BAI_WAVE3_NO_BRAND_LEFT_WITH_UNEXPLAINED_HISTORY";
/** Owner-intent noncomparability must be explicit — never fabricate 0 deltas. */
export const BAI_INTENT_NONCOMPARABILITY_IS_EXPLICIT =
  "BAI_INTENT_NONCOMPARABILITY_IS_EXPLICIT";

export const INTENT_COMPARABILITY_STATE = Object.freeze({
  COMPARABLE: "COMPARABLE",
  NOT_COMPARABLE_FOR_THIS_PERIOD_PAIR: "NOT_COMPARABLE_FOR_THIS_PERIOD_PAIR",
  UNAVAILABLE: "UNAVAILABLE",
});

export const PERFORMANCE_DIRECTION = Object.freeze({
  IMPROVED: "IMPROVED",
  DECLINED: "DECLINED",
  STABLE: "STABLE",
  UNAVAILABLE: "UNAVAILABLE",
});

/** Founder-governed Wave 3 computation cohort (showcase monitoring v4 = 19). */
export const BAI_WAVE3_GOVERNED_PARENT_KEYS = Object.freeze([
  "marriott",
  "hilton",
  "choice",
  "ihg",
]);

const MARRIOTT_BRAND_IDS = Object.freeze([
  "recEJCTDj1zrsjPM6", // Autograph
  "recCvV0PuZOi8c3hC", // Tribute
  "rec02zPClpWUTCyXM", // Design Hotels
  "recIPuBC50fv13zRR", // Westin
  "rec9aZp7GHtzUEg0c", // AC Hotels
]);

export function loadBaiWave3GovernedCohortV1(opts = {}) {
  const cfg = loadShowcaseCompaniesConfig(opts.showcaseConfigPath);
  const parents = [];
  const brandIds = [];
  const brandById = new Map();
  for (const key of BAI_WAVE3_GOVERNED_PARENT_KEYS) {
    const company = (cfg.companies || []).find((c) => c.companyKey === key);
    if (!company) continue;
    const brands = (company.brands || []).map((b) => ({
      brandId: String(b.brandId),
      brandName: b.brandName || null,
      parentCompanyKey: key,
      parentCompanyName: company.canonicalCompanyName || key,
    }));
    for (const b of brands) {
      brandIds.push(b.brandId);
      brandById.set(b.brandId, b);
    }
    parents.push({
      parentCompanyKey: key,
      parentCompanyName: company.canonicalCompanyName || key,
      brandIds: brands.map((b) => b.brandId),
      brandCount: brands.length,
    });
  }
  return {
    ok: brandIds.length === 19 && parents.length === 4,
    expectedBrandCount: 19,
    expectedParentCount: 4,
    brandIds,
    brandById,
    parents,
    SHOWCASE_MONITORING_BRAND_COUNT,
  };
}

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

  const positiveGains = withDelta.filter((b) => b.deltaPp > 0.05);
  const strongestGain =
    [...positiveGains].sort((a, b) => b.deltaPp - a.deltaPp)[0] || null;
  const largestLoss = [...withDelta].sort((a, b) => a.deltaPp - b.deltaPp)[0] || null;
  const mostStable =
    !strongestGain && withDelta.length
      ? [...withDelta].sort(
          (a, b) => Math.abs(a.deltaPp) - Math.abs(b.deltaPp)
        )[0]
      : null;

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
    // BAI_ZERO_GAIN_NOT_CALLED_STRONGEST_MOVER — never label 0.0 pp as strongest mover
    strongestPositiveMover: strongestGain
      ? {
          brandId: strongestGain.brandId,
          brandName: strongestGain.brandName,
          deltaPp: strongestGain.deltaPp,
          deltaDisplay: strongestGain.deltaDisplay,
        }
      : null,
    mostStableBrand: mostStable
      ? {
          brandId: mostStable.brandId,
          brandName: mostStable.brandName,
          deltaPp: mostStable.deltaPp,
          deltaDisplay: mostStable.deltaDisplay,
        }
      : null,
    noBrandsImproved: !strongestGain,
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
  } else if (portfolio.noBrandsImproved) {
    parts.push(
      portfolio.mostStableBrand
        ? `No brands improved. Most stable: ${portfolio.mostStableBrand.brandName} (${portfolio.mostStableBrand.deltaDisplay}).`
        : "No brands improved."
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
  const canonicalPeriodResolve =
    opts.periodResolve ||
    resolveBaiPriorComparablePeriodV1({
      viewMode:
        opts.viewMode || BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA,
      currentPeriodId: opts.currentPeriodId || BAI_PERIOD_2_CANDIDATE_ID,
      geography,
      storeRoot: opts.storeRoot,
    });

  // Intent Δ uses owner-intent period chain for measurement when available,
  // but period labels / identity must follow the canonical customer prior
  // (Aug 14 for P2) — never Aug 18 in customer-facing periodResolve.
  const selected = selectCurrentAndPriorPeriods({
    periods,
    geography,
    currentPeriodId:
      canonicalPeriodResolve.currentPeriodId ||
      opts.currentPeriodId ||
      BAI_PERIOD_2_CANDIDATE_ID,
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

  // Never convert unavailable → 0. Zero is only valid when comparisonStatus is COMPARABLE
  // and indexChangePoints is literally 0.
  const fabricatedZero = intents.some(
    (i) =>
      i.deltaPoints === 0 &&
      i.comparisonStatus &&
      i.comparisonStatus !== "COMPARABLE"
  );
  const intentComparabilityState =
    numeric.length > 0
      ? INTENT_COMPARABILITY_STATE.COMPARABLE
      : INTENT_COMPARABILITY_STATE.NOT_COMPARABLE_FOR_THIS_PERIOD_PAIR;

  return {
    ok: true,
    gate: BAI_INTENT_PRIOR_RUN_RECONCILIATION,
    subjectBrandId,
    periodResolve: {
      currentPeriodId: canonicalPeriodResolve.currentPeriodId,
      currentPeriodDate: canonicalPeriodResolve.currentPeriodDate,
      priorPeriodId: canonicalPeriodResolve.priorPeriodId,
      priorPeriodDate: canonicalPeriodResolve.priorPeriodDate,
    },
    intents,
    intentCount: intents.length,
    comparableIntentCount: numeric.length,
    intentComparabilityState,
    fabricatedZeroDeltas: fabricatedZero,
    strongestGain,
    largestLoss,
  };
}

/**
 * Wave 3 longitudinal payload.
 * Default computation scope = full governed 19-brand cohort.
 * Optional parentCompanyName / brandIds filter for parent QA views (e.g. Marriott).
 */
function wave3ViewModeAllowed(viewMode) {
  return (
    viewMode === BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA ||
    viewMode === BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW
  );
}

export function buildBaiWave3LongitudinalIntelligenceV1(opts = {}) {
  const viewMode =
    opts.viewMode || BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA;
  if (!wave3ViewModeAllowed(viewMode)) {
    return {
      ok: false,
      reason: "customer_published_view_has_no_period2_longitudinal_payload",
      gate: BAI_WAVE3_NO_CUSTOMER_PUBLICATION_MUTATION,
      viewMode,
      longitudinal: null,
    };
  }

  const cohort = loadBaiWave3GovernedCohortV1(opts);
  const parentRaw = opts.parentCompanyName != null ? String(opts.parentCompanyName).trim() : "";
  const parentNorm = parentRaw.toLowerCase();
  const wantsFullCohort =
    opts.scope === "full_cohort" ||
    opts.fullCohort === true ||
    !parentRaw ||
    parentNorm === "all" ||
    parentNorm === "*" ||
    parentNorm === "full" ||
    parentNorm === "cohort";

  const brandIds = opts.brandIds
    ? opts.brandIds.map(String)
    : wantsFullCohort
      ? cohort.brandIds
      : null;

  let resolvedBrandIds = brandIds;
  if (!resolvedBrandIds) {
    if (wantsFullCohort) {
      resolvedBrandIds = cohort.brandIds;
    } else {
      const parentMeta =
        cohort.parents.find(
          (p) =>
            p.parentCompanyKey === parentNorm ||
            String(p.parentCompanyName || "")
              .toLowerCase()
              .includes(parentNorm)
        ) || null;
      resolvedBrandIds = parentMeta?.brandIds || [];
    }
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
    parentCompanyName: wantsFullCohort ? null : parentRaw,
    brandIds: resolvedBrandIds,
  });
  if (!brandBundle.ok) {
    return { ok: false, periodResolve, ...brandBundle };
  }

  // Attach governed parent identity from showcase cohort (canonical IDs).
  const brands = (brandBundle.brands || []).map((b) => {
    const expected = cohort.brandById.get(String(b.brandId));
    return {
      ...b,
      parentCompanyKey: expected?.parentCompanyKey || null,
      parentCompanyName: expected?.parentCompanyName || b.parentCompanyName || null,
      canonicalBrandIdMatch: Boolean(expected),
      expectedBrandName: expected?.brandName || null,
    };
  });

  const portfolio = buildBaiPortfolioPriorRunSummaryV1({
    ...brandBundle,
    brands,
  });
  const executiveLongitudinal = buildBaiExecutiveLongitudinalNarrativeV1({
    portfolio,
    brands,
    periodResolve,
  });

  const intentByBrand = {};
  for (const b of brands) {
    intentByBrand[b.brandId] = buildBaiIntentPriorRunReconciliationV1({
      subjectBrandId: b.brandId,
      currentPeriodId: periodResolve.currentPeriodId,
      periodResolve,
      viewMode,
      geography: opts.geography || "CALA",
      storeRoot: opts.storeRoot,
    });
  }

  return {
    ok: true,
    viewMode,
    scope: wantsFullCohort ? "full_cohort" : "parent_filter",
    cohortBrandCountExpected: cohort.expectedBrandCount,
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
      BAI_WAVE3_FULL_19_BRAND_COHORT_COVERAGE,
      BAI_WAVE3_ALL_PARENT_GROUPS_RECONCILED,
      BAI_WAVE3_NO_BRAND_LEFT_WITH_UNEXPLAINED_HISTORY,
    },
    periodResolve,
    portfolio,
    brands,
    peerMeanDeltaPp: brandBundle.peerMeanDeltaPp,
    executiveLongitudinal,
    intentByBrand,
    LIVE_PROVIDER_CALLS: 0,
    PERIOD_2_PUBLICATION_STATE:
      BAI_CUSTOMER_PUBLISHED_PERIOD_ID === BAI_PERIOD_2_CANDIDATE_ID
        ? "PUBLISHED"
        : "UNPROMOTED",
  };
}

function historyIsExplained(brandRow) {
  if (!brandRow) return false;
  if (brandRow.canonicalBrandIdMatch !== true) return false;
  const membership = brandRow.membershipState;
  if (
    membership === "NEW" ||
    membership === "EXITED" ||
    membership === "RETURNED" ||
    membership === "NOT_COMPARABLE"
  ) {
    return true;
  }
  // SAME: require finite current + prior (including legitimate zeroes) and a delta.
  if (membership === "SAME") {
    return Boolean(
      isFiniteNumber(brandRow.currentPresence) &&
        isFiniteNumber(brandRow.priorPresence) &&
        isFiniteNumber(brandRow.deltaPp) &&
        brandRow.absoluteRelative?.absolutePerformance &&
        brandRow.absoluteRelative?.relativePerformance
    );
  }
  return false;
}

/**
 * Full 19-brand + 4-parent Wave 3 reconciliation (canonical computation scope).
 */
export function buildBaiWave3FullCohortReconciliationV1(opts = {}) {
  const cohort = loadBaiWave3GovernedCohortV1(opts);
  const payload = buildBaiWave3LongitudinalIntelligenceV1({
    ...opts,
    scope: "full_cohort",
    parentCompanyName: "all",
    brandIds: cohort.brandIds,
    viewMode:
      opts.viewMode || BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA,
  });
  if (!payload.ok) {
    return {
      ok: false,
      ...payload,
      gate: BAI_WAVE3_FULL_19_BRAND_COHORT_COVERAGE,
    };
  }

  const byId = new Map(payload.brands.map((b) => [String(b.brandId), b]));
  const missingFromArtifact = cohort.brandIds.filter((id) => !byId.has(id));
  const unexpectedIds = payload.brands
    .map((b) => String(b.brandId))
    .filter((id) => !cohort.brandById.has(id));

  const matrix = cohort.brandIds.map((id) => {
    const expected = cohort.brandById.get(id);
    const row = byId.get(id) || null;
    const intent = row ? payload.intentByBrand[id] : null;
    const explained = historyIsExplained(row);
    return {
      brandId: id,
      brandName: expected?.brandName || row?.brandName || null,
      parentCompanyKey: expected?.parentCompanyKey || null,
      parentCompanyName: expected?.parentCompanyName || null,
      currentPresence: row?.currentPresence ?? null,
      priorPresence: row?.priorPresence ?? null,
      deltaPp: row?.deltaPp ?? null,
      deltaDisplay: row?.deltaDisplay ?? null,
      currentRank: row?.currentRank ?? null,
      priorRank: row?.priorRank ?? null,
      rankDelta: row?.rankDelta ?? null,
      rankDisplay: row?.rankDisplay ?? null,
      absolutePerformance: row?.absoluteRelative?.absolutePerformance ?? null,
      relativePerformance: row?.absoluteRelative?.relativePerformance ?? null,
      membershipState: row?.membershipState ?? null,
      canonicalBrandIdMatch: row?.canonicalBrandIdMatch === true,
      historyExplained: explained,
      ownerIntent:
        intent && intent.ok
          ? {
              strongestGain: intent.strongestGain || null,
              largestLoss: intent.largestLoss || null,
              intentCount: intent.intentCount ?? (intent.intents || []).length,
              comparableIntentCount:
                intent.comparableIntentCount ??
                (intent.intents || []).filter((i) =>
                  isFiniteNumber(i.deltaPoints)
                ).length,
              intentComparabilityState:
                intent.intentComparabilityState ||
                INTENT_COMPARABILITY_STATE.UNAVAILABLE,
              fabricatedZeroDeltas: intent.fabricatedZeroDeltas === true,
            }
          : {
              strongestGain: null,
              largestLoss: null,
              intentCount: 0,
              comparableIntentCount: 0,
              intentComparabilityState:
                INTENT_COMPARABILITY_STATE.UNAVAILABLE,
              fabricatedZeroDeltas: false,
            },
    };
  });

  const parentSummaries = cohort.parents.map((p) => {
    const rows = matrix.filter((m) => m.parentCompanyKey === p.parentCompanyKey);
    const bundle = {
      ok: true,
      brands: rows.map((r) => ({
        brandId: r.brandId,
        brandName: r.brandName,
        currentPresence: r.currentPresence,
        priorPresence: r.priorPresence,
        deltaPp: r.deltaPp,
        deltaDisplay: r.deltaDisplay,
      })),
      peerMeanDeltaPp: payload.peerMeanDeltaPp,
    };
    // Recompute parent portfolio from parent brand rows only.
    const withDelta = rows.filter((b) => isFiniteNumber(b.deltaPp));
    const mean = (arr, key) => {
      const vals = arr.map((b) => b[key]).filter(isFiniteNumber);
      if (!vals.length) return null;
      return round1(vals.reduce((a, b) => a + b, 0) / vals.length);
    };
    const currentPresence = mean(rows, "currentPresence");
    const priorPresence = mean(rows, "priorPresence");
    const portfolioDeltaPp = presenceDeltaPp(currentPresence, priorPresence);
    const sep = classifyAbsoluteRelative({
      brandDeltaPp: portfolioDeltaPp,
      peerMeanDeltaPp: payload.peerMeanDeltaPp,
    });
    const improving = withDelta.filter((b) => b.deltaPp > 0.05);
    const declining = withDelta.filter((b) => b.deltaPp < -0.05);
    const stable = withDelta.filter((b) => Math.abs(b.deltaPp) <= 0.05);
    const positiveGains = withDelta.filter((b) => b.deltaPp > 0.05);
    const strongestGain =
      [...positiveGains].sort((a, b) => b.deltaPp - a.deltaPp)[0] || null;
    const largestLoss = [...withDelta].sort((a, b) => a.deltaPp - b.deltaPp)[0] || null;
    const mostStable =
      !strongestGain && withDelta.length
        ? [...withDelta].sort(
            (a, b) => Math.abs(a.deltaPp) - Math.abs(b.deltaPp)
          )[0]
        : null;
    return {
      parentCompanyKey: p.parentCompanyKey,
      parentCompanyName: p.parentCompanyName,
      brandCount: rows.length,
      expectedBrandCount: p.brandCount,
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
      mostStableBrand: mostStable
        ? {
            brandId: mostStable.brandId,
            brandName: mostStable.brandName,
            deltaPp: mostStable.deltaPp,
            deltaDisplay: mostStable.deltaDisplay,
          }
        : null,
      noBrandsImproved: !strongestGain,
      largestVisibilityLoss: largestLoss
        ? {
            brandId: largestLoss.brandId,
            brandName: largestLoss.brandName,
            deltaPp: largestLoss.deltaPp,
            deltaDisplay: largestLoss.deltaDisplay,
          }
        : null,
      absoluteRelative: sep,
      allHistoryExplained: rows.every((r) => r.historyExplained),
      allIdentityMatched: rows.every((r) => r.canonicalBrandIdMatch),
      unusedBundle: bundle.ok,
    };
  });

  const unexplained = matrix.filter((m) => !m.historyExplained);
  const identityMisses = matrix.filter((m) => !m.canonicalBrandIdMatch);

  const intentNoncomparabilityPass =
    matrix.length === 19 &&
    matrix.every((m) => {
      const oi = m.ownerIntent;
      if (!oi) return false;
      if (oi.fabricatedZeroDeltas === true) return false;
      // For this Period 2 ↔ federated prior pair: expect explicit noncomparability
      // (0 comparable intents) — never treat as failure, never invent movement.
      if (oi.comparableIntentCount === 0) {
        return (
          oi.intentComparabilityState ===
            INTENT_COMPARABILITY_STATE.NOT_COMPARABLE_FOR_THIS_PERIOD_PAIR &&
          oi.intentCount === 4
        );
      }
      return (
        oi.intentComparabilityState === INTENT_COMPARABILITY_STATE.COMPARABLE
      );
    });

  const coveragePass =
    matrix.length === 19 &&
    missingFromArtifact.length === 0 &&
    unexpectedIds.length === 0 &&
    identityMisses.length === 0;
  const parentsPass =
    parentSummaries.length === 4 &&
    parentSummaries.every(
      (p) => p.brandCount === p.expectedBrandCount && p.allIdentityMatched
    );
  const unexplainedPass = unexplained.length === 0;

  return {
    ok:
      coveragePass &&
      parentsPass &&
      unexplainedPass &&
      intentNoncomparabilityPass,
    gate: BAI_WAVE3_FULL_19_BRAND_COHORT_COVERAGE,
    gates: {
      [BAI_WAVE3_FULL_19_BRAND_COHORT_COVERAGE]: coveragePass,
      [BAI_WAVE3_ALL_PARENT_GROUPS_RECONCILED]: parentsPass,
      [BAI_WAVE3_NO_BRAND_LEFT_WITH_UNEXPLAINED_HISTORY]: unexplainedPass,
      [BAI_INTENT_NONCOMPARABILITY_IS_EXPLICIT]: intentNoncomparabilityPass,
    },
    PERIOD_2_PUBLICATION_STATE:
      BAI_CUSTOMER_PUBLISHED_PERIOD_ID === BAI_PERIOD_2_CANDIDATE_ID
        ? "PUBLISHED"
        : "UNPROMOTED",
    LIVE_PROVIDER_CALLS: 0,
    ownerIntentCohortState:
      INTENT_COMPARABILITY_STATE.NOT_COMPARABLE_FOR_THIS_PERIOD_PAIR,
    periodResolve: payload.periodResolve,
    peerMeanDeltaPp: payload.peerMeanDeltaPp,
    cohort,
    matrix,
    parentSummaries,
    missingFromArtifact,
    unexpectedIds,
    unexplainedBrandIds: unexplained.map((m) => m.brandId),
    identityMissBrandIds: identityMisses.map((m) => m.brandId),
    portfolioUniverse: payload.portfolio,
    intentByBrand: payload.intentByBrand,
  };
}

export const BAI_WAVE3_MARRIOTT_BRAND_IDS = MARRIOTT_BRAND_IDS;
export const BAI_WAVE3_FULL_COHORT_BRAND_IDS = () =>
  loadBaiWave3GovernedCohortV1().brandIds;
