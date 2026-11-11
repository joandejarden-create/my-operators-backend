/**
 * Rank subject + peers using each hotel's own Loyalty STANDARD-28 corpus.
 * Comparable measurement universe: identical scenario schedule × providers,
 * independently measured Portfolio AI Presence per hotel.
 */

import {
  loadAllPeriods,
  isLoyaltyMeasurementPeriod,
  loadPropertyProfile,
} from "../data-model.js";
import { getBrandPortfolioPeerSet } from "./brand-portfolio-peer-set-v1.js";
import { computeSubjectMeasuredBppMetrics } from "./bpp-subject-measured-partial-v1.js";
import { applyBppCustomerPresentationV2 } from "./bpp-customer-presentation-v2.js";
import { saveLoyaltyBppStagedPack } from "./loyalty-bpp-staged-pack-store-v1.js";
import {
  saveStagedAdpReport,
  loadStagedOwnerPayload,
} from "../review/adp-staged-report-store-v1.js";
import { ensureDraftPeerProfileForLoyalty } from "../orchestration/adp-greenfield-live-monitoring-v1.js";

export const BPP_COMPARABLE_LOYALTY_COHORT_RANK_V1 =
  "BPP_COMPARABLE_LOYALTY_COHORT_RANK_V1";

function pct(rate) {
  if (rate == null || Number.isNaN(rate)) return "—";
  return `${(Math.round(rate * 1000) / 10).toFixed(1)}%`;
}

function latestLoyalty(propertyId) {
  const periods = (loadAllPeriods(propertyId) || []).filter((p) =>
    isLoyaltyMeasurementPeriod(p)
  );
  return periods.sort((a, b) => String(b.periodId).localeCompare(String(a.periodId)))[0] || null;
}

/**
 * Independently measure each cohort member from its own Loyalty archive.
 */
export function computeComparableLoyaltyCohortRanking(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  const peerSet = getBrandPortfolioPeerSet(propertyId);
  if (!profile || !peerSet) {
    return { ok: false, error: "missing_profile_or_peer_set", propertyId };
  }

  const subjectLoy = latestLoyalty(propertyId);
  if (!subjectLoy?.observations?.length) {
    return { ok: false, error: "subject_loyalty_missing", propertyId };
  }
  const subjectMetrics = computeSubjectMeasuredBppMetrics(profile, subjectLoy);
  if (!subjectMetrics.ok) {
    return { ok: false, error: "subject_metrics_failed", detail: subjectMetrics };
  }

  const rows = [
    {
      canonicalEntityId: propertyId,
      name: profile.name,
      brand: profile.brand || null,
      isSubject: true,
      presenceRate: subjectMetrics.portfolioAiPresence,
      obsHits: subjectMetrics.presentObservations,
      obsDenom: subjectMetrics.totalObservations,
      numberOneRate: subjectMetrics.numberOneRate,
      top3Rate: subjectMetrics.top3Rate,
      periodId: subjectLoy.periodId,
    },
  ];

  const peerFailures = [];
  for (const peer of peerSet.included || []) {
    const peerId = peer.canonicalEntityId || peer.propertyId;
    let peerProfile = loadPropertyProfile(peerId);
    if (!peerProfile) {
      peerProfile = ensureDraftPeerProfileForLoyalty(peer, { peerOf: propertyId });
    }
    const peerLoy = latestLoyalty(peerId);
    if (!peerLoy?.observations?.length) {
      peerFailures.push({ peerId, error: "PEER_LOYALTY_CORPUS_MISSING" });
      continue;
    }
    if (peerLoy.observations.length < 112 * 0.9) {
      peerFailures.push({
        peerId,
        error: "PEER_LOYALTY_INCOMPLETE",
        observations: peerLoy.observations.length,
      });
      continue;
    }
    const m = computeSubjectMeasuredBppMetrics(peerProfile, peerLoy);
    if (!m.ok) {
      peerFailures.push({ peerId, error: "PEER_METRICS_FAILED" });
      continue;
    }
    rows.push({
      canonicalEntityId: peerId,
      name: peer.peerHotel || peerProfile.name,
      brand: peer.brand || peerProfile.brand || null,
      isSubject: false,
      presenceRate: m.portfolioAiPresence,
      obsHits: m.presentObservations,
      obsDenom: m.totalObservations,
      numberOneRate: m.numberOneRate,
      top3Rate: m.top3Rate,
      periodId: peerLoy.periodId,
    });
  }

  if (peerFailures.length) {
    return {
      ok: false,
      error: "PEER_CORPORA_INCOMPLETE",
      peerFailures,
      partialRows: rows,
    };
  }

  // Rank by presence descending; ties share min rank then dense-label by sort order
  const sorted = [...rows].sort((a, b) => {
    const d = (b.presenceRate || 0) - (a.presenceRate || 0);
    if (Math.abs(d) > 1e-12) return d;
    if (a.isSubject !== b.isSubject) return a.isSubject ? -1 : 1;
    return String(a.name).localeCompare(String(b.name));
  });
  let rank = 0;
  let prev = null;
  for (let i = 0; i < sorted.length; i++) {
    const rate = sorted[i].presenceRate || 0;
    if (prev == null || Math.abs(rate - prev) > 1e-12) {
      rank = i + 1;
      prev = rate;
    }
    sorted[i].rank = rank;
    sorted[i].tieState =
      sorted.filter((x) => Math.abs((x.presenceRate || 0) - rate) <= 1e-12).length > 1
        ? "TIE"
        : null;
  }

  const subjectRow = sorted.find((r) => r.isSubject);
  const cohortSize = sorted.length;

  return {
    ok: true,
    contract: BPP_COMPARABLE_LOYALTY_COHORT_RANK_V1,
    propertyId,
    peerSetId: peerSet.peerSetId,
    lensId: peerSet.lensId,
    subjectPeriodId: subjectLoy.periodId,
    cohortSize,
    subjectMetrics,
    subjectRank: subjectRow?.rank ?? null,
    rankingRows: sorted,
    methodology:
      "Each hotel ranked by Portfolio AI Presence on its own Loyalty STANDARD-28 × 4-provider corpus (PROVIDER_OBSERVATION grain).",
  };
}

export function buildComparableCohortBppPayload(propertyId) {
  const ranked = computeComparableLoyaltyCohortRanking(propertyId);
  if (!ranked.ok) return ranked;

  const subject = ranked.rankingRows.find((r) => r.isSubject);
  const kpis = [
    {
      id: "portfolioAiPresence",
      label: "Portfolio AI Presence",
      value: pct(ranked.subjectMetrics.portfolioAiPresence),
      valueRaw: ranked.subjectMetrics.portfolioAiPresence,
      meta: `${ranked.subjectMetrics.presentObservations} of ${ranked.subjectMetrics.totalObservations} AI observations`,
      grain: "PROVIDER_OBSERVATION",
      available: true,
      presentationSlot: "PRIMARY",
    },
    {
      id: "portfolioRank",
      label: "Portfolio Rank",
      value: `#${subject.rank} of ${ranked.cohortSize}`,
      valueRaw: subject.rank,
      meta: `Among ${ranked.cohortSize} relevant ${ranked.lensId || "loyalty"} hotels`,
      grain: "PROVIDER_OBSERVATION",
      available: true,
      presentationSlot: "PRIMARY",
    },
    {
      id: "numberOneAppearance",
      label: "#1 Appearance",
      value: pct(ranked.subjectMetrics.numberOneRate),
      valueRaw: ranked.subjectMetrics.numberOneRate,
      meta: `${ranked.subjectMetrics.numberOneCount} of ${ranked.subjectMetrics.eligibleScenarios} scenarios`,
      grain: "UNIQUE_SCENARIO",
      available: true,
      presentationSlot: "PRIMARY",
    },
    {
      id: "top3Appearance",
      label: "Top-3 Appearance",
      value: pct(ranked.subjectMetrics.top3Rate),
      valueRaw: ranked.subjectMetrics.top3Rate,
      meta: `${ranked.subjectMetrics.top3Count} of ${ranked.subjectMetrics.eligibleScenarios} scenarios`,
      grain: "UNIQUE_SCENARIO",
      available: true,
      presentationSlot: "PRIMARY",
    },
  ];

  const ranking = {
    rows: ranked.rankingRows.map((r) => ({
      rank: r.rank,
      rankLabel: `#${r.rank}`,
      name: r.name,
      brand: r.brand,
      isSubject: r.isSubject,
      presenceDisplay: pct(r.presenceRate),
      presencePct: Math.round((r.presenceRate || 0) * 1000) / 10,
      obsHits: r.obsHits,
      obsDenom: r.obsDenom,
      deltaDisplay: "—",
      displacementDisplay: "—",
      sharedDisplay: "—",
      canonicalEntityId: r.canonicalEntityId,
      periodId: r.periodId,
      tieState: r.tieState,
    })),
    hasPriorPeriod: false,
    firstPeriodDeltaLabel: "—",
  };

  let payload = {
    status: "READY",
    bppCustomerState: "BPP_COMPARISON_CERTIFIED",
    bppMeasurementState: "BPP_COMPARISON_CERTIFIED",
    // Founder staged delivery — not customer publication. SPA readiness must treat
    // BPP_COMPARISON_CERTIFIED as paintable without CUSTOMER_READY assurance.
    assuranceStatus: "STAGED_COMPARISON_CERTIFIED",
    showAnalyticalScaffolding: true,
    measurementFamily: "LOYALTY_BPP",
    contract: BPP_COMPARABLE_LOYALTY_COHORT_RANK_V1,
    peerSetId: ranked.peerSetId,
    lensId: ranked.lensId,
    methodologyNote: ranked.methodology,
    kpis,
    ranking,
    customerPublished: false,
    stagedReviewOnly: true,
  };
  payload = applyBppCustomerPresentationV2(payload);
  return {
    ok: true,
    ...ranked,
    payload,
  };
}

/**
 * Merge comparable-cohort BPP into Core staged owner report (new immutable version).
 */
export function mergeComparableCohortBppIntoStagedOwnerReport(propertyId, { cwd = process.cwd() } = {}) {
  const built = buildComparableCohortBppPayload(propertyId);
  if (!built.ok) return built;

  const pack = saveLoyaltyBppStagedPack({
    propertyId,
    periodId: built.subjectPeriodId,
    payload: built.payload,
    certification: {
      status: "STAGED_LOYALTY_BPP_READY",
      measurementFamily: "LOYALTY_BPP",
      readyRank: true,
      subjectMetricsCertified: true,
      rankingCertified: true,
      peerSetId: built.peerSetId,
      contract: BPP_COMPARABLE_LOYALTY_COHORT_RANK_V1,
      subjectRank: built.subjectRank,
      cohortSize: built.cohortSize,
    },
    pipeline: {
      version: BPP_COMPARABLE_LOYALTY_COHORT_RANK_V1,
      stagesCompleted: [
        "PEER_CORPORA_COMPLETE",
        "COMPARABLE_UNIVERSE_RANKED",
        "BPP_PACK_READY",
      ],
    },
    cwd,
  });

  const coreStaged = loadStagedOwnerPayload(propertyId, cwd);
  if (!coreStaged?.ok || !coreStaged.payload) {
    return {
      ok: false,
      error: "core_staged_missing",
      built,
      pack,
    };
  }

  const priorVersionId = coreStaged.versionId;
  const {
    brandPortfolioPosition: _drop,
    _adpReportVersion,
    _adpStagedVersionId,
    _adpArtifactHash,
    ...rest
  } = coreStaged.payload;

  // Promote Reality Gap primary surface to approved V2 when weighted score is present
  // (legacy greenfield staging sometimes left methodologyId on V1 while gapScore was binary).
  if (rest.realityGap && Number.isFinite(Number(rest.realityGap.gapScoreWeightedV2))) {
    const v2 = Number(rest.realityGap.gapScoreWeightedV2);
    rest.realityGap = {
      ...rest.realityGap,
      historicalImmutableMethodologyId:
        rest.realityGap.historicalImmutableMethodologyId ||
        rest.realityGap.methodologyId ||
        "RG_BINARY_GAP_V1",
      methodologyId: "RG_WEIGHTED_PARTIAL_V2",
      methodologyLabel: rest.realityGap.methodologyLabel || "Weighted partial recognition (V2)",
      gapScore: v2,
      display: `${v2}%`,
      gapScoreWeightedV2: v2,
      gapScoreBinaryV1:
        rest.realityGap.gapScoreBinaryV1 ?? rest.realityGap.gapScore ?? null,
    };
  }

  const merged = {
    ...rest,
    brandPortfolioPosition: {
      ...built.payload,
      customerPublished: false,
      stagedReviewOnly: true,
      _bppPackSource: "loyalty_bpp_comparable_cohort_rank",
      _bppLoyaltyStagedVersionId: pack?.versionId || null,
      bppMeasurementState: "BPP_COMPARISON_CERTIFIED",
    },
    _adpBppSelection: {
      reportVersion: "STAGED",
      bppSource: "loyalty_bpp_comparable_cohort_rank",
      loyaltyPeriodId: built.subjectPeriodId,
      subjectMetricsCertified: true,
      rankingCertified: true,
      subjectRank: built.subjectRank,
      cohortSize: built.cohortSize,
      historicalPriorStagedVersionId: priorVersionId,
      selectionPolicy: "COMPARABLE_OWN_CORPUS_PRESENCE_RANK",
    },
  };

  const staged = saveStagedAdpReport({
    propertyId,
    periodId: rest.period?.periodId || built.subjectPeriodId,
    runId: `bpp_cohort_rank_${built.subjectPeriodId}`,
    workflowVersion: BPP_COMPARABLE_LOYALTY_COHORT_RANK_V1,
    payload: merged,
    certification: {
      status: "STAGED_FOUNDER_REVIEW_BPP_COMPARISON_CERTIFIED",
      bppMeasurementState: "BPP_COMPARISON_CERTIFIED",
      publicationAuthorized: false,
      priorStagedVersionId: priorVersionId,
    },
  });

  return {
    ok: true,
    propertyId,
    subjectRank: built.subjectRank,
    cohortSize: built.cohortSize,
    rankingRows: built.rankingRows,
    kpis: built.payload.kpis,
    stagedVersionId: staged?.versionId || null,
    priorStagedVersionId: priorVersionId,
    pack,
    methodology: built.methodology,
  };
}
