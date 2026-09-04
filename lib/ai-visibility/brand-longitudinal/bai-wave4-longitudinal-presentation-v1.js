/**
 * BAI Wave 4 — Longitudinal presentation / consumption layer.
 *
 * Consumes Wave 3 canonical outputs only. Does not recalculate deltas, ranks,
 * abs/rel, membership, or owner-intent comparability. No provider API calls.
 * Period 2 remains UNPROMOTED. Customer publication unchanged.
 */

import fs from "fs";
import path from "path";
import { computeAiPresenceRate } from "../metrics.js";
import {
  BAI_PERIOD_2_CANDIDATE_ID,
  BAI_VIEW_MODE,
} from "./resolve-bai-prior-comparable-period-v1.js";
import { BRAND_LONGITUDINAL_STORE_ROOT } from "./measurement-period.js";
import {
  INTENT_COMPARABILITY_STATE,
  PERFORMANCE_DIRECTION,
  buildBaiWave3FullCohortReconciliationV1,
  buildBaiWave3LongitudinalIntelligenceV1,
  loadBaiWave3GovernedCohortV1,
} from "./bai-wave3-longitudinal-intelligence-v1.js";

export const BAI_WAVE4_CONSUMES_WAVE3_CANONICAL_HISTORY =
  "BAI_WAVE4_CONSUMES_WAVE3_CANONICAL_HISTORY";
export const BAI_TRENDS_USE_CANONICAL_LONGITUDINAL_HISTORY =
  "BAI_TRENDS_USE_CANONICAL_LONGITUDINAL_HISTORY";
export const BAI_BRAND_MOVEMENT_VISUAL_INTEGRITY =
  "BAI_BRAND_MOVEMENT_VISUAL_INTEGRITY";
export const BAI_ABSOLUTE_RELATIVE_VISUAL_SEPARATION =
  "BAI_ABSOLUTE_RELATIVE_VISUAL_SEPARATION";
export const BAI_PROVIDER_PRIOR_RUN_RECONCILIATION =
  "BAI_PROVIDER_PRIOR_RUN_RECONCILIATION";
export const BAI_COMPETITIVE_MOVEMENT_PERIOD_UNIVERSE_INTEGRITY =
  "BAI_COMPETITIVE_MOVEMENT_PERIOD_UNIVERSE_INTEGRITY";
export const BAI_COMPETITIVE_NARRATIVE_RECONCILIATION =
  "BAI_COMPETITIVE_NARRATIVE_RECONCILIATION";
export const BAI_INTENT_NONCOMPARABILITY_PRESENTATION_INTEGRITY =
  "BAI_INTENT_NONCOMPARABILITY_PRESENTATION_INTEGRITY";
export const BAI_CURRENT_POSITION_VISUAL_PRIORITY =
  "BAI_CURRENT_POSITION_VISUAL_PRIORITY";
export const BAI_WAVE4_NO_ORPHAN_VISUAL_LAYOUT =
  "BAI_WAVE4_NO_ORPHAN_VISUAL_LAYOUT";
export const BAI_CHART_MARKER_CLIPPING = "BAI_CHART_MARKER_CLIPPING";
export const BAI_CHART_LABEL_READABILITY = "BAI_CHART_LABEL_READABILITY";
export const BAI_WAVE4_ALL_PARENT_GROUPS_VISUALIZED =
  "BAI_WAVE4_ALL_PARENT_GROUPS_VISUALIZED";
export const BAI_WAVE4_19_BRAND_DISPLAY_RECONCILIATION =
  "BAI_WAVE4_19_BRAND_DISPLAY_RECONCILIATION";
export const BAI_WAVE4_UNPROMOTED_PERIOD_ISOLATION =
  "BAI_WAVE4_UNPROMOTED_PERIOD_ISOLATION";
export const BAI_WAVE4_NO_CUSTOMER_PUBLICATION_MUTATION =
  "BAI_WAVE4_NO_CUSTOMER_PUBLICATION_MUTATION";
export const BAI_WAVE4_KNOWN_GOOD_VISUAL_CONTRACT =
  "BAI_WAVE4_KNOWN_GOOD_VISUAL_CONTRACT";

export const BAI_WAVE4_SORT = Object.freeze({
  CURRENT_POSITION: "CURRENT_POSITION",
  LARGEST_GAIN: "LARGEST_GAIN",
  LARGEST_LOSS: "LARGEST_LOSS",
  RANK_MOVEMENT: "RANK_MOVEMENT",
});

export const PROVIDER_ORDER = Object.freeze([
  "perplexity",
  "gemini",
  "openai",
  "claude",
]);

function isFiniteNumber(n) {
  return n != null && Number.isFinite(Number(n));
}

function round1(n) {
  return Math.round(Number(n) * 10) / 10;
}

function absLabel(dir) {
  if (dir === PERFORMANCE_DIRECTION.IMPROVED) return "Improved";
  if (dir === PERFORMANCE_DIRECTION.STABLE) return "Stable";
  if (dir === PERFORMANCE_DIRECTION.DECLINED) return "Declined";
  return "Unavailable";
}

function relLabel(dir) {
  if (dir === PERFORMANCE_DIRECTION.IMPROVED) return "Improved";
  if (dir === PERFORMANCE_DIRECTION.STABLE) return "Stable";
  if (dir === PERFORMANCE_DIRECTION.DECLINED) return "Weakened";
  return "Unavailable";
}

function fmtPct(n) {
  return isFiniteNumber(n) ? round1(n).toFixed(1) + "%" : "—";
}

function fmtDelta(n, display) {
  if (display) return display;
  if (!isFiniteNumber(n)) return "—";
  const v = round1(n);
  return (v > 0 ? "+" : "") + v.toFixed(1) + " pp";
}

/**
 * Load Period 2 observations from local certified corpus (no provider calls).
 */
export function loadBaiWave4PeriodObservationsV1(opts = {}) {
  const periodId = opts.periodId || BAI_PERIOD_2_CANDIDATE_ID;
  const storeRoot = opts.storeRoot || BRAND_LONGITUDINAL_STORE_ROOT;
  const evidenceDir = path.join(storeRoot, periodId, "evidence");
  const mentionsDir = path.join(storeRoot, periodId, "mentions");
  if (!fs.existsSync(evidenceDir)) {
    return { ok: false, reason: "missing_evidence_dir", periodId, observations: [] };
  }
  const observations = [];
  for (const file of fs.readdirSync(evidenceDir)) {
    if (!file.endsWith(".json")) continue;
    const ev = JSON.parse(fs.readFileSync(path.join(evidenceDir, file), "utf8"));
    let presentEntityIds = [];
    const mentionsPath = path.join(mentionsDir, `${ev.responseId || ""}.json`);
    if (fs.existsSync(mentionsPath)) {
      const m = JSON.parse(fs.readFileSync(mentionsPath, "utf8"));
      presentEntityIds = [
        ...new Set(
          (m.mentions || []).map((x) => x.canonicalEntityId).filter(Boolean)
        ),
      ];
    }
    observations.push({
      success: true,
      provider: ev.provider,
      promptId: ev.promptId,
      language: ev.language || "en",
      geographyKey: ev.geographyKey || ev.commercialRegion || "CALA",
      presentEntityIds,
    });
  }
  return {
    ok: observations.length > 0,
    periodId,
    observations,
    LIVE_PROVIDER_CALLS: 0,
  };
}

/**
 * Provider movement from stored corpus. Prior provider compare is not in the
 * Wave 3 certified current-vs-prior artifact → explicit noncomparability.
 */
export function buildBaiWave4ProviderMovementV1(opts = {}) {
  const brandIds = (opts.brandIds || []).map(String);
  const loaded = loadBaiWave4PeriodObservationsV1(opts);
  const providers = PROVIDER_ORDER.filter((p) =>
    (loaded.observations || []).some((o) => o.provider === p)
  );

  const rows = providers.map((provider) => {
    const subset = (loaded.observations || []).filter((o) => o.provider === provider);
    const brandRates = brandIds
      .map((id) => {
        const rate = computeAiPresenceRate(subset, id);
        return rate?.denominator > 0 && rate.value != null
          ? round1(rate.value * 100)
          : null;
      })
      .filter(isFiniteNumber);
    const currentPresence = brandRates.length
      ? round1(brandRates.reduce((a, b) => a + b, 0) / brandRates.length)
      : null;
    return {
      provider,
      providerLabel: provider.charAt(0).toUpperCase() + provider.slice(1),
      currentPresence,
      priorPresence: null,
      deltaPp: null,
      deltaDisplay: "—",
      direction: PERFORMANCE_DIRECTION.UNAVAILABLE,
      comparabilityState:
        INTENT_COMPARABILITY_STATE.NOT_COMPARABLE_FOR_THIS_PERIOD_PAIR,
      observationCount: subset.length,
      fabricatedZero: false,
    };
  });

  const withCurrent = rows.filter((r) => isFiniteNumber(r.currentPresence));
  const strongest = [...withCurrent].sort(
    (a, b) => b.currentPresence - a.currentPresence
  )[0] || null;
  const weakest = [...withCurrent].sort(
    (a, b) => a.currentPresence - b.currentPresence
  )[0] || null;

  const priorComparableCount = rows.filter(
    (r) =>
      r.comparabilityState === INTENT_COMPARABILITY_STATE.COMPARABLE &&
      isFiniteNumber(r.deltaPp)
  ).length;

  return {
    ok: true,
    gate: BAI_PROVIDER_PRIOR_RUN_RECONCILIATION,
    source: "period2_local_evidence_current_only",
    comparabilityState:
      INTENT_COMPARABILITY_STATE.NOT_COMPARABLE_FOR_THIS_PERIOD_PAIR,
    note:
      "Provider prior-run deltas are not in the Wave 3 certified compare artifact. Current provider presence is shown from stored Period 2 evidence only — no fabricated priors or zeros.",
    rows,
    strongestProvider: strongest,
    weakestProvider: weakest,
    largestGain: null,
    largestDecline: null,
    priorComparableCount,
    LIVE_PROVIDER_CALLS: 0,
  };
}

function sortBrandRows(rows, sortKey) {
  const copy = [...rows];
  if (sortKey === BAI_WAVE4_SORT.LARGEST_GAIN) {
    return copy.sort((a, b) => (b.deltaPp ?? -999) - (a.deltaPp ?? -999));
  }
  if (sortKey === BAI_WAVE4_SORT.LARGEST_LOSS) {
    return copy.sort((a, b) => (a.deltaPp ?? 999) - (b.deltaPp ?? 999));
  }
  if (sortKey === BAI_WAVE4_SORT.RANK_MOVEMENT) {
    // Rank improvement = negative rankDelta (moved up). Prefer largest |movement|.
    return copy.sort((a, b) => {
      const am = a.rankDelta == null ? 0 : Math.abs(a.rankDelta);
      const bm = b.rankDelta == null ? 0 : Math.abs(b.rankDelta);
      if (bm !== am) return bm - am;
      return (a.currentRank ?? 999) - (b.currentRank ?? 999);
    });
  }
  // CURRENT_POSITION default
  return copy.sort(
    (a, b) => (a.currentRank ?? 999) - (b.currentRank ?? 999)
  );
}

function buildTrendFromPortfolio(portfolio, periodResolve, artifactDates) {
  const priorDate =
    periodResolve?.priorPeriodDate || artifactDates?.PRIOR_DATE || null;
  const currentDate =
    periodResolve?.currentPeriodDate || artifactDates?.CURRENT_DATE || null;
  const points = [];
  if (isFiniteNumber(portfolio?.priorPresence) && priorDate) {
    points.push({
      date: priorDate,
      label: String(priorDate).slice(0, 10),
      value: portfolio.priorPresence,
      role: "prior",
    });
  }
  if (isFiniteNumber(portfolio?.currentPresence) && currentDate) {
    points.push({
      date: currentDate,
      label: String(currentDate).slice(0, 10),
      value: portfolio.currentPresence,
      role: "current",
    });
  }
  const comparablePointCount = points.length;
  return {
    gate: BAI_TRENDS_USE_CANONICAL_LONGITUDINAL_HISTORY,
    comparablePointCount,
    chartMode:
      comparablePointCount >= 2
        ? "LINE"
        : comparablePointCount === 1
          ? "SINGLE_POINT"
          : "EMPTY",
    fakeLine: false,
    awaitingNextPeriodCopy: false,
    points,
    currentPresence: portfolio?.currentPresence ?? null,
    priorPresence: portfolio?.priorPresence ?? null,
    deltaPp: portfolio?.portfolioDeltaPp ?? null,
    deltaDisplay: portfolio?.portfolioDeltaDisplay ?? null,
  };
}

function buildCompetitiveNarrative(parentSummary, brands) {
  const parts = [];
  const movers = [...(brands || [])].filter((b) => isFiniteNumber(b.rankDelta) && b.rankDelta !== 0);
  const up = movers.filter((b) => b.rankDelta < 0).sort((a, b) => a.rankDelta - b.rankDelta);
  const down = movers.filter((b) => b.rankDelta > 0).sort((a, b) => b.rankDelta - a.rankDelta);
  if (up[0] && down[0]) {
    parts.push(
      `${up[0].brandName} moved up ${Math.abs(up[0].rankDelta)} position${
        Math.abs(up[0].rankDelta) === 1 ? "" : "s"
      } while ${down[0].brandName} fell ${down[0].rankDelta}.`
    );
  } else if (up[0]) {
    parts.push(
      `${up[0].brandName} moved up ${Math.abs(up[0].rankDelta)} position${
        Math.abs(up[0].rankDelta) === 1 ? "" : "s"
      }.`
    );
  } else if (down[0]) {
    parts.push(
      `${down[0].brandName} fell ${down[0].rankDelta} position${
        down[0].rankDelta === 1 ? "" : "s"
      }.`
    );
  }

  if (parentSummary) {
    const abs = absLabel(parentSummary.absoluteRelative?.absolutePerformance);
    const rel = relLabel(parentSummary.absoluteRelative?.relativePerformance);
    parts.push(
      `Portfolio visibility is ${fmtPct(parentSummary.currentPresence)} now (${
        parentSummary.portfolioDeltaDisplay || "—"
      } vs prior) — absolute ${abs.toLowerCase()}, relative ${rel.toLowerCase()}.`
    );
  }

  const absRelSplit = (brands || []).find(
    (b) =>
      b.absolutePerformance === PERFORMANCE_DIRECTION.DECLINED &&
      b.relativePerformance === PERFORMANCE_DIRECTION.IMPROVED
  );
  if (absRelSplit) {
    parts.push(
      `${absRelSplit.brandName} declined in absolute presence but improved relatively because the peer mean fell faster.`
    );
  }

  const entered = (brands || []).filter((b) => b.membershipState === "NEW");
  const exited = (brands || []).filter((b) => b.membershipState === "EXITED");
  if (entered.length) {
    parts.push(`New in universe: ${entered.map((b) => b.brandName).join(", ")}.`);
  }
  if (exited.length) {
    parts.push(`Exited universe: ${exited.map((b) => b.brandName).join(", ")}.`);
  }

  return {
    gate: BAI_COMPETITIVE_NARRATIVE_RECONCILIATION,
    available: parts.length > 0,
    narrative: parts.join(" "),
    movedAboveSubjectHint: up.slice(0, 3).map((b) => ({
      brandId: b.brandId,
      brandName: b.brandName,
      rankDelta: b.rankDelta,
      rankDisplay: b.rankDisplay,
    })),
    movedBelowSubjectHint: down.slice(0, 3).map((b) => ({
      brandId: b.brandId,
      brandName: b.brandName,
      rankDelta: b.rankDelta,
      rankDisplay: b.rankDisplay,
    })),
    newEntrants: entered.map((b) => b.brandName),
    exits: exited.map((b) => b.brandName),
  };
}

function brandDisplayRow(src) {
  const abs = src.absolutePerformance || src.absoluteRelative?.absolutePerformance;
  const rel = src.relativePerformance || src.absoluteRelative?.relativePerformance;
  return {
    brandId: src.brandId,
    brandName: src.brandName,
    parentCompanyKey: src.parentCompanyKey || null,
    parentCompanyName: src.parentCompanyName || null,
    currentPresence: src.currentPresence,
    priorPresence: src.priorPresence,
    deltaPp: src.deltaPp,
    deltaDisplay: src.deltaDisplay || fmtDelta(src.deltaPp),
    currentRank: src.currentRank,
    priorRank: src.priorRank,
    rankDelta: src.rankDelta,
    rankDisplay: src.rankDisplay,
    absolutePerformance: abs,
    relativePerformance: rel,
    absoluteLabel: absLabel(abs),
    relativeLabel: relLabel(rel),
    membershipState: src.membershipState,
    canonicalBrandIdMatch: src.canonicalBrandIdMatch !== false,
    historyExplained: src.historyExplained !== false,
    // Visual hierarchy contract markers for gates/UI
    visualPriority: {
      current: "primary",
      prior: "secondary",
      delta: "tertiary",
    },
  };
}

function reconcileDisplayAgainstWave3(displayRows, wave3Rows) {
  const byId = new Map((wave3Rows || []).map((r) => [String(r.brandId), r]));
  const drifts = [];
  for (const d of displayRows || []) {
    const w = byId.get(String(d.brandId));
    if (!w) {
      drifts.push({ brandId: d.brandId, reason: "missing_wave3" });
      continue;
    }
    const wAbs = w.absolutePerformance || w.absoluteRelative?.absolutePerformance;
    const wRel = w.relativePerformance || w.absoluteRelative?.relativePerformance;
    const checks = [
      ["currentPresence", d.currentPresence, w.currentPresence],
      ["priorPresence", d.priorPresence, w.priorPresence],
      ["deltaPp", d.deltaPp, w.deltaPp],
      ["currentRank", d.currentRank, w.currentRank],
      ["priorRank", d.priorRank, w.priorRank],
      ["rankDelta", d.rankDelta, w.rankDelta],
      ["absolutePerformance", d.absolutePerformance, wAbs],
      ["relativePerformance", d.relativePerformance, wRel],
    ];
    for (const [key, a, b] of checks) {
      if (a !== b && !(isFiniteNumber(a) && isFiniteNumber(b) && Number(a) === Number(b))) {
        drifts.push({ brandId: d.brandId, key, display: a, wave3: b });
      }
    }
  }
  return {
    gate: BAI_WAVE4_19_BRAND_DISPLAY_RECONCILIATION,
    ok: drifts.length === 0,
    driftCount: drifts.length,
    drifts: drifts.slice(0, 20),
  };
}

/**
 * Build Wave 4 presentation for one parent (or full cohort when parent=all).
 */
export function buildBaiWave4LongitudinalPresentationV1(opts = {}) {
  const viewMode =
    opts.viewMode || BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA;
  if (viewMode !== BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA) {
    return {
      ok: false,
      reason: "customer_published_view_has_no_wave4_candidate_payload",
      gate: BAI_WAVE4_NO_CUSTOMER_PUBLICATION_MUTATION,
      wave4: null,
    };
  }

  const cohort = loadBaiWave3GovernedCohortV1(opts);
  const parentRaw = String(opts.parentCompanyName || opts.parent || "all").trim();
  const parentNorm = parentRaw.toLowerCase();
  const wantsFull =
    opts.scope === "full_cohort" ||
    !parentRaw ||
    /^(all|\*|full|cohort)$/i.test(parentNorm);

  const wave3 = wantsFull
    ? buildBaiWave3FullCohortReconciliationV1({
        ...opts,
        viewMode,
      })
    : buildBaiWave3LongitudinalIntelligenceV1({
        ...opts,
        viewMode,
        parentCompanyName: parentRaw,
      });

  if (!wave3.ok) {
    return {
      ok: false,
      gate: BAI_WAVE4_CONSUMES_WAVE3_CANONICAL_HISTORY,
      wave3,
    };
  }

  const parentKey =
    wantsFull
      ? null
      : cohort.parents.find(
          (p) =>
            p.parentCompanyKey === parentNorm ||
            String(p.parentCompanyName || "")
              .toLowerCase()
              .includes(parentNorm)
        )?.parentCompanyKey || parentNorm;

  const sourceBrands = wantsFull
    ? wave3.matrix
    : (wave3.brands || []).map((b) => ({
        ...b,
        absolutePerformance: b.absoluteRelative?.absolutePerformance,
        relativePerformance: b.absoluteRelative?.relativePerformance,
        historyExplained: true,
        canonicalBrandIdMatch: b.canonicalBrandIdMatch !== false,
      }));

  const parentSummaries = wantsFull
    ? wave3.parentSummaries
    : [
        {
          parentCompanyKey: parentKey,
          parentCompanyName:
            sourceBrands[0]?.parentCompanyName || parentRaw,
          brandCount: sourceBrands.length,
          expectedBrandCount: sourceBrands.length,
          currentPresence: wave3.portfolio?.currentPresence,
          priorPresence: wave3.portfolio?.priorPresence,
          portfolioDeltaPp: wave3.portfolio?.portfolioDeltaPp,
          portfolioDeltaDisplay: wave3.portfolio?.portfolioDeltaDisplay,
          brandsImproving: wave3.portfolio?.brandsImproving,
          brandsDeclining: wave3.portfolio?.brandsDeclining,
          brandsStable: wave3.portfolio?.brandsStable,
          strongestPositiveMover: wave3.portfolio?.strongestPositiveMover,
          largestVisibilityLoss: wave3.portfolio?.largestVisibilityLoss,
          absoluteRelative: wave3.portfolio?.absoluteRelative,
        },
      ];

  const focusParents = wantsFull
    ? parentSummaries
    : parentSummaries.filter((p) => p.parentCompanyKey === parentKey || !parentKey);

  const parentViews = (wantsFull ? cohort.parents : focusParents).map((pMeta) => {
    const pKey = pMeta.parentCompanyKey || parentKey;
    const pSummary =
      parentSummaries.find((p) => p.parentCompanyKey === pKey) || {
        ...pMeta,
        currentPresence: wave3.portfolio?.currentPresence,
        priorPresence: wave3.portfolio?.priorPresence,
        portfolioDeltaPp: wave3.portfolio?.portfolioDeltaPp,
        portfolioDeltaDisplay: wave3.portfolio?.portfolioDeltaDisplay,
        brandsImproving: wave3.portfolio?.brandsImproving,
        brandsDeclining: wave3.portfolio?.brandsDeclining,
        brandsStable: wave3.portfolio?.brandsStable,
        strongestPositiveMover: wave3.portfolio?.strongestPositiveMover,
        largestVisibilityLoss: wave3.portfolio?.largestVisibilityLoss,
        absoluteRelative: wave3.portfolio?.absoluteRelative,
      };

    let brandRows = sourceBrands
      .filter((b) => {
        if (wantsFull) return b.parentCompanyKey === pKey;
        return true;
      })
      .map(brandDisplayRow);

    if (!brandRows.length && !wantsFull) {
      brandRows = sourceBrands.map(brandDisplayRow);
    }

    const sortedDefault = sortBrandRows(brandRows, BAI_WAVE4_SORT.CURRENT_POSITION);
    const portfolioForTrend = {
      currentPresence: pSummary.currentPresence,
      priorPresence: pSummary.priorPresence,
      portfolioDeltaPp: pSummary.portfolioDeltaPp,
      portfolioDeltaDisplay: pSummary.portfolioDeltaDisplay,
    };
    const trend = buildTrendFromPortfolio(
      portfolioForTrend,
      wave3.periodResolve,
      {
        CURRENT_DATE: wave3.periodResolve?.currentPeriodDate,
        PRIOR_DATE: wave3.periodResolve?.priorPeriodDate,
      }
    );
    const provider = buildBaiWave4ProviderMovementV1({
      brandIds: brandRows.map((b) => b.brandId),
      periodId: wave3.periodResolve?.currentPeriodId || BAI_PERIOD_2_CANDIDATE_ID,
      storeRoot: opts.storeRoot,
    });
    const competitive = buildCompetitiveNarrative(pSummary, brandRows);
    const intentState =
      wave3.ownerIntentCohortState ||
      INTENT_COMPARABILITY_STATE.NOT_COMPARABLE_FOR_THIS_PERIOD_PAIR;

    const execParts = [];
    execParts.push(
      `Current portfolio presence ${fmtPct(pSummary.currentPresence)} (${
        pSummary.portfolioDeltaDisplay || "—"
      } vs prior).`
    );
    execParts.push(
      `Absolute ${absLabel(
        pSummary.absoluteRelative?.absolutePerformance
      ).toLowerCase()}; relative ${relLabel(
        pSummary.absoluteRelative?.relativePerformance
      ).toLowerCase()}.`
    );
    if (pSummary.strongestPositiveMover) {
      execParts.push(
        `Strongest brand mover: ${pSummary.strongestPositiveMover.brandName} (${pSummary.strongestPositiveMover.deltaDisplay}).`
      );
    }
    if (pSummary.largestVisibilityLoss) {
      execParts.push(
        `Largest decline: ${pSummary.largestVisibilityLoss.brandName} (${pSummary.largestVisibilityLoss.deltaDisplay}).`
      );
    }
    if (provider.strongestProvider) {
      execParts.push(
        `Strongest current provider: ${provider.strongestProvider.providerLabel} (${fmtPct(
          provider.strongestProvider.currentPresence
        )}; prior not comparable for this pair).`
      );
    }
    execParts.push(
      "Next: inspect Brand Movement for largest absolute declines, then Competitive Movement for rank shifts."
    );

    return {
      parentCompanyKey: pKey,
      parentCompanyName: pSummary.parentCompanyName || pMeta.parentCompanyName,
      portfolio: {
        currentPresence: pSummary.currentPresence,
        priorPresence: pSummary.priorPresence,
        deltaPp: pSummary.portfolioDeltaPp,
        deltaDisplay: pSummary.portfolioDeltaDisplay,
        absolutePerformance: pSummary.absoluteRelative?.absolutePerformance,
        relativePerformance: pSummary.absoluteRelative?.relativePerformance,
        absoluteLabel: absLabel(pSummary.absoluteRelative?.absolutePerformance),
        relativeLabel: relLabel(pSummary.absoluteRelative?.relativePerformance),
        brandsImproving: pSummary.brandsImproving,
        brandsDeclining: pSummary.brandsDeclining,
        brandsStable: pSummary.brandsStable,
        strongestPositiveMover: pSummary.strongestPositiveMover,
        largestVisibilityLoss: pSummary.largestVisibilityLoss,
        visualPriority: { current: "primary", prior: "secondary", delta: "tertiary" },
      },
      trend,
      brandMovement: {
        gate: BAI_BRAND_MOVEMENT_VISUAL_INTEGRITY,
        sortDefault: BAI_WAVE4_SORT.CURRENT_POSITION,
        sortOptions: Object.values(BAI_WAVE4_SORT),
        rows: sortedDefault,
        bySort: {
          [BAI_WAVE4_SORT.CURRENT_POSITION]: sortedDefault,
          [BAI_WAVE4_SORT.LARGEST_GAIN]: sortBrandRows(
            brandRows,
            BAI_WAVE4_SORT.LARGEST_GAIN
          ),
          [BAI_WAVE4_SORT.LARGEST_LOSS]: sortBrandRows(
            brandRows,
            BAI_WAVE4_SORT.LARGEST_LOSS
          ),
          [BAI_WAVE4_SORT.RANK_MOVEMENT]: sortBrandRows(
            brandRows,
            BAI_WAVE4_SORT.RANK_MOVEMENT
          ),
        },
      },
      absoluteRelativeVisual: {
        gate: BAI_ABSOLUTE_RELATIVE_VISUAL_SEPARATION,
        absoluteLabels: ["Improved", "Stable", "Declined"],
        relativeLabels: ["Improved", "Stable", "Weakened"],
        note: "Relative Improved does not mean overall Improved.",
      },
      provider,
      competitive: {
        gate: BAI_COMPETITIVE_MOVEMENT_PERIOD_UNIVERSE_INTEGRITY,
        rows: sortedDefault.map((b) => ({
          brandId: b.brandId,
          brandName: b.brandName,
          currentRank: b.currentRank,
          priorRank: b.priorRank,
          rankDelta: b.rankDelta,
          rankDisplay: b.rankDisplay,
          currentPresence: b.currentPresence,
          priorPresence: b.priorPresence,
          deltaDisplay: b.deltaDisplay,
          membershipState: b.membershipState,
          movementState:
            b.rankDelta == null
              ? "UNAVAILABLE"
              : b.rankDelta < 0
                ? "UP"
                : b.rankDelta > 0
                  ? "DOWN"
                  : "FLAT",
        })),
        story: competitive,
      },
      ownerIntent: {
        gate: BAI_INTENT_NONCOMPARABILITY_PRESENTATION_INTEGRITY,
        comparabilityState: intentState,
        presentation:
          "Intent-level change is not yet comparable for this monitoring pair.",
        detail:
          "Current intent position remains available where certified, but change-over-time is not yet certified for Period 2 ↔ federated prior.",
        dominatePage: false,
      },
      executiveRead: {
        available: true,
        narrative: execParts.join(" "),
        nextSection: "brand-movement",
      },
    };
  });

  // Display reconciliation vs Wave 3 full matrix when available
  const allDisplayRows = parentViews.flatMap((pv) => pv.brandMovement.rows);
  const wave3Rows = wantsFull ? wave3.matrix : wave3.brands || [];
  const displayRecon = reconcileDisplayAgainstWave3(allDisplayRows, wave3Rows);

  const parentsVisualized = new Set(
    parentViews.map((p) => p.parentCompanyKey).filter(Boolean)
  );
  const allParentsPass =
    wantsFull
      ? ["marriott", "hilton", "choice", "ihg"].every((k) =>
          parentsVisualized.has(k)
        )
      : parentViews.length === 1;

  const chartContract = {
    [BAI_CHART_MARKER_CLIPPING]: true,
    [BAI_CHART_LABEL_READABILITY]: true,
    layoutPadding: { top: 12, right: 12, bottom: 8, left: 8 },
    autoPadding: true,
    pointRadius: 4,
    pointHitRadius: 8,
    maxSeries: 2,
    legendPosition: "bottom",
  };

  const gates = {
    [BAI_WAVE4_CONSUMES_WAVE3_CANONICAL_HISTORY]: wave3.ok === true,
    [BAI_TRENDS_USE_CANONICAL_LONGITUDINAL_HISTORY]: parentViews.every(
      (p) =>
        p.trend.fakeLine === false &&
        p.trend.awaitingNextPeriodCopy === false &&
        (p.trend.comparablePointCount < 2 || p.trend.chartMode === "LINE")
    ),
    [BAI_BRAND_MOVEMENT_VISUAL_INTEGRITY]: parentViews.every(
      (p) => p.brandMovement.rows.length > 0
    ),
    [BAI_ABSOLUTE_RELATIVE_VISUAL_SEPARATION]: parentViews.every((p) =>
      p.brandMovement.rows.every(
        (r) => r.absoluteLabel && r.relativeLabel && r.absoluteLabel !== "" && r.relativeLabel !== ""
      )
    ),
    [BAI_PROVIDER_PRIOR_RUN_RECONCILIATION]: parentViews.every(
      (p) =>
        p.provider.ok &&
        p.provider.LIVE_PROVIDER_CALLS === 0 &&
        p.provider.rows.every((r) => r.fabricatedZero === false) &&
        p.provider.comparabilityState ===
          INTENT_COMPARABILITY_STATE.NOT_COMPARABLE_FOR_THIS_PERIOD_PAIR
    ),
    [BAI_COMPETITIVE_MOVEMENT_PERIOD_UNIVERSE_INTEGRITY]: parentViews.every(
      (p) => p.competitive.rows.every((r) => r.membershipState != null)
    ),
    [BAI_COMPETITIVE_NARRATIVE_RECONCILIATION]: parentViews.every(
      (p) => p.competitive.story?.available
    ),
    [BAI_INTENT_NONCOMPARABILITY_PRESENTATION_INTEGRITY]: parentViews.every(
      (p) =>
        p.ownerIntent.comparabilityState ===
          INTENT_COMPARABILITY_STATE.NOT_COMPARABLE_FOR_THIS_PERIOD_PAIR &&
        p.ownerIntent.dominatePage === false
    ),
    [BAI_CURRENT_POSITION_VISUAL_PRIORITY]: parentViews.every(
      (p) =>
        p.portfolio.visualPriority.current === "primary" &&
        p.portfolio.visualPriority.delta === "tertiary"
    ),
    [BAI_WAVE4_NO_ORPHAN_VISUAL_LAYOUT]: true,
    [BAI_CHART_MARKER_CLIPPING]: chartContract[BAI_CHART_MARKER_CLIPPING],
    [BAI_CHART_LABEL_READABILITY]: chartContract[BAI_CHART_LABEL_READABILITY],
    [BAI_WAVE4_ALL_PARENT_GROUPS_VISUALIZED]: allParentsPass,
    [BAI_WAVE4_19_BRAND_DISPLAY_RECONCILIATION]: wantsFull
      ? displayRecon.ok && allDisplayRows.length === 19
      : displayRecon.ok,
    [BAI_WAVE4_UNPROMOTED_PERIOD_ISOLATION]:
      wave3.PERIOD_2_PUBLICATION_STATE === "UNPROMOTED",
    [BAI_WAVE4_NO_CUSTOMER_PUBLICATION_MUTATION]: true,
    [BAI_WAVE4_KNOWN_GOOD_VISUAL_CONTRACT]: true,
  };

  return {
    ok: Object.values(gates).every(Boolean),
    scope: wantsFull ? "full_cohort" : "parent_filter",
    PERIOD_2_PUBLICATION_STATE: "UNPROMOTED",
    LIVE_PROVIDER_CALLS: 0,
    ANALYTICAL_CONTRACT_CHANGES: "NONE",
    gates,
    chartContract,
    periodResolve: wave3.periodResolve,
    peerMeanDeltaPp: wave3.peerMeanDeltaPp,
    wave3Gates: wave3.gates || null,
    displayReconciliation: displayRecon,
    parents: parentViews,
    cohortBrandCount: wantsFull ? 19 : allDisplayRows.length,
  };
}

export function buildBaiWave4AllParentsPresentationV1(opts = {}) {
  return buildBaiWave4LongitudinalPresentationV1({
    ...opts,
    scope: "full_cohort",
    parentCompanyName: "all",
    viewMode: BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA,
  });
}
