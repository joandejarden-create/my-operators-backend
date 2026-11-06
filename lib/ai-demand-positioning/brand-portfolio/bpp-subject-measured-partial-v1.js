/**
 * Subject-measured BPP partial packs when Loyalty STANDARD-28 observations exist
 * but a certified peer set is not yet available for ranking.
 *
 * Never invents peers or ranks. Preserves Bethesda four-KPI shell.
 */

import { detectPropertyMention } from "../execution/response-parser.js";
import { loadPropertyProfile, loadAllPeriods } from "../data-model.js";
import { resolvePortfolioMapping } from "./brand-portfolio-affiliation-mapping-v1.js";
import { applyBppCustomerPresentationV2 } from "./bpp-customer-presentation-v2.js";
import { saveLoyaltyBppStagedPack } from "./loyalty-bpp-staged-pack-store-v1.js";
import { attachBppMethodologyDisclosure } from "./bpp-measurement-comparability-v1.js";
import {
  loadStagedOwnerPayload,
  saveStagedAdpReport,
} from "../review/adp-staged-report-store-v1.js";

export const BPP_SUBJECT_MEASURED_PARTIAL_V1 = "BPP_SUBJECT_MEASURED_PARTIAL_V1";
export const BPP_CUSTOMER_STATE_SUBJECT_MEASURED_RANK_BLOCKED =
  "BPP_SUBJECT_MEASURED_RANK_BLOCKED";

function formatPct(rate) {
  if (rate == null || !Number.isFinite(Number(rate))) return "—";
  return `${(Math.round(Number(rate) * 1000) / 10).toFixed(1)}%`;
}

function latestLoyaltyPeriod(propertyId) {
  const periods = loadAllPeriods(propertyId) || [];
  const loyalty = periods
    .filter((p) => String(p.measurementFamily || "").includes("LOYALTY"))
    .sort((a, b) => String(b.periodId).localeCompare(String(a.periodId)));
  return loyalty[0] || null;
}

/**
 * Resolve ordinal position for a Loyalty observation.
 * Prefer archived `position`, then parser position from raw text (Bethesda grain).
 */
export function resolveSubjectObservationPosition(obs, profile) {
  const archived =
    obs?.position ??
    obs?.subjectRank ??
    obs?.parsed?.subjectRank ??
    obs?.parsed?.rank ??
    obs?.parsed?.position ??
    null;
  if (Number.isFinite(Number(archived)) && Number(archived) > 0) {
    return Number(archived);
  }
  const text = obs?.rawResponse || obs?.parsed?.rawResponse || "";
  if (!text || !profile) return null;
  const det = detectPropertyMention(text, profile);
  if (det?.mentioned && Number.isFinite(Number(det.position)) && Number(det.position) > 0) {
    return Number(det.position);
  }
  return null;
}

/**
 * Compute subject-only presence / #1 / top-3 from Loyalty observations.
 * Presence = PROVIDER_OBSERVATION grain; #1 / Top-3 = UNIQUE_SCENARIO grain
 * (best ordinal across providers). Top-3 matches Bethesda first-cycle:
 * present + unknown ordinal counts as top-3 (presence without order proof).
 */
export function computeSubjectMeasuredBppMetrics(profile, period) {
  const observations = (period?.observations || []).filter(
    (o) => o && (o.rawResponse || o.parsed || o.mentioned != null)
  );
  if (!observations.length || !profile) {
    return { ok: false, error: "no_observations" };
  }

  let present = 0;
  let rankedObs = 0;
  const byScenario = new Map();
  for (const obs of observations) {
    const text = obs.rawResponse || obs.parsed?.rawResponse || "";
    const det = text ? detectPropertyMention(text, profile) : { mentioned: false };
    const mentioned =
      det.mentioned === true ||
      obs.mentioned === true ||
      String(obs.mentioned).toLowerCase() === "true";
    if (mentioned) present += 1;
    const sid = obs.scenarioId || obs.scenario?.id || "unknown";
    if (!byScenario.has(sid)) {
      byScenario.set(sid, { present: false, bestRank: null });
    }
    const row = byScenario.get(sid);
    if (mentioned) {
      row.present = true;
      const rank = resolveSubjectObservationPosition(obs, profile);
      // Number(null) === 0 — never treat null/undefined as ordinal 0.
      if (rank != null && Number.isFinite(Number(rank)) && Number(rank) > 0) {
        rankedObs += 1;
        const r = Number(rank);
        if (row.bestRank == null || r < row.bestRank) row.bestRank = r;
      }
    }
  }

  const total = observations.length;
  const presence = total ? present / total : null;
  const scenarios = [...byScenario.values()];
  const eligible = scenarios.length;
  const numberOne = scenarios.filter((s) => s.present && s.bestRank === 1).length;
  const top3 = scenarios.filter((s) => {
    if (!s.present) return false;
    if (s.bestRank == null) return true;
    return s.bestRank <= 3;
  }).length;
  const rankedScenarios = scenarios.filter((s) => s.present && s.bestRank != null).length;

  return {
    ok: true,
    totalObservations: total,
    presentObservations: present,
    rankedObservations: rankedObs,
    portfolioAiPresence: presence,
    eligibleScenarios: eligible,
    rankedScenarios,
    numberOneRate: eligible ? numberOne / eligible : null,
    numberOneCount: numberOne,
    top3Rate: eligible ? top3 / eligible : null,
    top3Count: top3,
    grain: {
      portfolioAiPresence: "PROVIDER_OBSERVATION",
      numberOneAppearance: "UNIQUE_SCENARIO",
      top3Appearance: "UNIQUE_SCENARIO",
    },
  };
}

/**
 * Build Bethesda-shaped BPP payload with measured subject KPIs and blocked rank.
 */
export function buildSubjectMeasuredPartialBppPayload(propertyId, { period = null } = {}) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) return { ok: false, error: "missing_profile" };
  const loyPeriod = period || latestLoyaltyPeriod(propertyId);
  if (!loyPeriod?.observations?.length) {
    return { ok: false, error: "no_loyalty_period" };
  }

  const metrics = computeSubjectMeasuredBppMetrics(profile, loyPeriod);
  if (!metrics.ok) return metrics;

  const mapping = resolvePortfolioMapping(propertyId);
  const lens =
    (mapping?.lenses || []).find((l) => l.lensId === mapping.defaultLensId) ||
    (mapping?.lenses || [])[0] ||
    null;

  const kpis = [
    {
      id: "portfolioAiPresence",
      label: "Portfolio AI Presence",
      value: formatPct(metrics.portfolioAiPresence),
      valueRaw: metrics.portfolioAiPresence,
      meta: `${metrics.presentObservations} of ${metrics.totalObservations} AI observations`,
      grain: "PROVIDER_OBSERVATION",
      available: metrics.portfolioAiPresence != null,
    },
    {
      id: "portfolioRank",
      label: "Portfolio Rank",
      value: "—",
      valueRaw: null,
      meta: "Peer roster approved; peer AI measurements pending",
      grain: "PROVIDER_OBSERVATION",
      available: false,
      blockedReason: "PEER_MEASUREMENT_INCOMPLETE",
      tooltip:
        "Subject Loyalty KPIs are certified. Portfolio rank requires comparable peer Loyalty measurements for the approved peer roster — peer membership alone is not enough.",
    },
    {
      id: "numberOneAppearance",
      label: "#1 Appearance",
      value: formatPct(metrics.numberOneRate),
      valueRaw: metrics.numberOneRate,
      meta: `${metrics.numberOneCount} of ${metrics.eligibleScenarios} scenarios`,
      grain: "UNIQUE_SCENARIO",
      available: metrics.numberOneRate != null,
    },
    {
      id: "top3Appearance",
      label: "Top-3 Appearance",
      value: formatPct(metrics.top3Rate),
      valueRaw: metrics.top3Rate,
      meta: `${metrics.top3Count} of ${metrics.eligibleScenarios} scenarios`,
      grain: "UNIQUE_SCENARIO",
      available: metrics.top3Rate != null,
    },
  ];

  const payload = {
    ok: true,
    sectionId: "brand-portfolio-position",
    contractVersion: "ADP_BRAND_PORTFOLIO_POSITION_CONTRACT_V1",
    sectionVisible: true,
    title: "Brand & Portfolio Position",
    subtitle:
      "How this hotel performs within its brand, collection, portfolio, or loyalty ecosystem.",
    status: "READY",
    renderState: "READY",
    assuranceStatus: "CUSTOMER_READY",
    showAnalyticalScaffolding: true,
    bppCustomerState: BPP_CUSTOMER_STATE_SUBJECT_MEASURED_RANK_BLOCKED,
    customerReadyClass: BPP_CUSTOMER_STATE_SUBJECT_MEASURED_RANK_BLOCKED,
    partialCertification: {
      subjectMetricsCertified: true,
      rankingCertified: false,
      blocker: "PEER_SET_NOT_CERTIFIED",
      loyaltyPeriodId: loyPeriod.periodId,
      contract: BPP_SUBJECT_MEASURED_PARTIAL_V1,
    },
    // Customer BPP must not emit a supplemental status panel. Rank unavailability
    // lives only on the Portfolio Rank KPI meta/tooltip (Bethesda shell).
    customerState: null,
    emptyMessage: null,
    adminDiagnostics: {
      statusLabel: "SUBJECT_MEASURED_RANK_BLOCKED",
      blocker: "PEER_SET_NOT_CERTIFIED",
      detail:
        "Loyalty STANDARD-28 subject metrics certified; peer cohort not certified for ranking.",
      customerFacing: false,
    },
    lens: lens
      ? {
          lensId: lens.lensId,
          label: lens.label || lens.lensLabel,
          portfolioType: lens.portfolioType || mapping?.portfolioType,
          status: "DEFAULT",
        }
      : null,
    kpis,
    ranking: {
      // Do not paint a one-row subject-only table as a competitive overview.
      rows: [],
      rankBlocked: true,
      rankBlocker: "PEER_SET_NOT_CERTIFIED",
      emptyState:
        "Peer roster may be approved for research, but comparable peer AI measurements are still pending. Subject Loyalty KPIs above remain valid.",
      denominator: null,
      peerMembershipStatus: "APPROVED_OR_PENDING_MEASUREMENT",
      peerMeasurementStatus: "INCOMPLETE",
      rankComputationStatus: "BLOCKED_PENDING_PEER_CORPORA",
    },
    evidenceLinks: {
      positive: { available: true, label: "View Positive Evidence" },
      missing: { available: true, label: "View Missing Evidence" },
      displacement: {
        available: false,
        label: "View Portfolio Displacement",
        reason: "Requires measured peer Loyalty corpora",
      },
    },
    periodId: loyPeriod.periodId,
    measurement: {
      periodId: loyPeriod.periodId,
      measurementFamily: "LOYALTY_BPP",
      observationCount: metrics.totalObservations,
    },
    loyaltyMeasurementComplete: true,
    customerPublished: false,
    stagedReviewOnly: true,
    _bppPackSource: "loyalty_bpp_subject_measured_partial",
  };

  let presented = applyBppCustomerPresentationV2(payload);
  presented = attachBppMethodologyDisclosure(presented) || presented;
  // Keep partial state after presentation normalize
  presented.bppCustomerState = BPP_CUSTOMER_STATE_SUBJECT_MEASURED_RANK_BLOCKED;
  presented.partialCertification = payload.partialCertification;
  presented.loyaltyMeasurementComplete = true;
  presented.customerState = null;
  presented.adminDiagnostics = payload.adminDiagnostics;
  presented.ranking = payload.ranking;
  // Forbid supplemental status-panel surface on customer packs.
  presented.forbidCustomerStatusPanel = true;

  return {
    ok: true,
    propertyId,
    periodId: loyPeriod.periodId,
    metrics,
    payload: presented,
  };
}

export function saveSubjectMeasuredPartialBppPack(propertyId, cwd = process.cwd()) {
  const built = buildSubjectMeasuredPartialBppPayload(propertyId);
  if (!built.ok) return built;
  const saved = saveLoyaltyBppStagedPack({
    propertyId,
    periodId: built.periodId,
    payload: built.payload,
    certification: {
      status: "STAGED_LOYALTY_BPP_SUBJECT_MEASURED",
      measurementFamily: "LOYALTY_BPP",
      readyRank: false,
      subjectMetricsCertified: true,
      rankingCertified: false,
      blocker: "PEER_SET_NOT_CERTIFIED",
      contract: BPP_SUBJECT_MEASURED_PARTIAL_V1,
    },
    pipeline: { version: BPP_SUBJECT_MEASURED_PARTIAL_V1 },
    cwd,
  });
  return { ...built, pack: saved };
}

/**
 * Overlay subject-measured BPP onto the Core owner staged report (new immutable version).
 * Preserves prior staged versions as historical. Does not fabricate ranks.
 */
export function mergeSubjectMeasuredPartialIntoStagedOwnerReport(
  propertyId,
  { periodId = null, cwd = process.cwd(), writeStagedAdpReport = true } = {}
) {
  const built = buildSubjectMeasuredPartialBppPayload(propertyId, {
    period: periodId
      ? (loadAllPeriods(propertyId) || []).find((p) => p.periodId === periodId) || null
      : null,
  });
  if (!built.ok) return built;

  const pack = saveSubjectMeasuredPartialBppPack(propertyId, cwd);
  if (!writeStagedAdpReport) {
    return {
      ok: true,
      propertyId,
      metrics: built.metrics,
      payload: built.payload,
      pack: pack.pack,
      stagedVersionId: null,
    };
  }

  const coreStaged = loadStagedOwnerPayload(propertyId, cwd);
  if (!coreStaged?.ok || !coreStaged.payload) {
    return {
      ok: false,
      error: "core_staged_missing",
      metrics: built.metrics,
      payload: built.payload,
      pack: pack.pack,
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

  const merged = {
    ...rest,
    brandPortfolioPosition: {
      ...built.payload,
      customerPublished: false,
      stagedReviewOnly: true,
      _bppPackSource: "loyalty_bpp_subject_measured_partial",
      _bppLoyaltyStagedVersionId: pack.pack?.versionId || null,
      bppMeasurementState: "BPP_SUBJECT_MEASURED",
    },
    _adpBppSelection: {
      reportVersion: "STAGED",
      bppSource: "loyalty_bpp_subject_measured_partial",
      loyaltyPeriodId: built.periodId,
      subjectMetricsCertified: true,
      rankingCertified: false,
      historicalPriorStagedVersionId: priorVersionId,
      selectionPolicy: "SUBJECT_KPIS_INDEPENDENT_OF_PEER_RANK",
    },
    measurementFamilyNote:
      "Core ADP owner sections preserved; BPP subject KPIs from LOYALTY_BPP — portfolio rank blocked pending peer corpora",
  };

  const staged = saveStagedAdpReport({
    propertyId,
    periodId: coreStaged.periodId || built.periodId || periodId,
    runId: `bpp_subject_partial_${propertyId}`,
    workflowVersion: BPP_SUBJECT_MEASURED_PARTIAL_V1,
    payload: merged,
    certification: {
      status: "STAGED_FOUNDER_REVIEW_BPP_SUBJECT_MEASURED",
      publicationAuthorized: false,
      subjectMetricsCertified: true,
      rankingCertified: false,
      bppMeasurementState: "BPP_SUBJECT_MEASURED",
      priorStagedVersionId: priorVersionId,
      contract: BPP_SUBJECT_MEASURED_PARTIAL_V1,
    },
    source: "bpp_subject_measured_partial_v1",
    note: "Subject Loyalty KPIs populated; peer rank unavailable — prior staged version preserved as historical",
    customerFacing: true,
    cwd,
  });

  return {
    ok: staged?.ok === true,
    propertyId,
    metrics: built.metrics,
    payload: built.payload,
    pack: pack.pack,
    staged,
    stagedVersionId: staged?.versionId || null,
    priorStagedVersionId: priorVersionId,
    artifactHash: staged?.artifactHash || null,
  };
}
