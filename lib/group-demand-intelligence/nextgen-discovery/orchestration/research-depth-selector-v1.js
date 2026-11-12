/**
 * L1 / L2 / L3 research depth selector for hunter discoveries.
 *
 * L1 — discovery + basic screening (default; always free path)
 * L2 — targeted evidence-gap research (paid only when authorized + gaps justify)
 * L3 — deep commercial qualification research (paid; founder-authorized only)
 *
 * Never auto-spend. Prefer skip when existing evidence is sufficient.
 */

import {
  evaluateDeepResearchEligibility,
  ELIGIBILITY_DECISION,
  RESEARCH_DEPTH_TIER,
} from "../../research/deep-research-eligibility-v1.js";
import { DISCOVERY_EVIDENCE_CLASS } from "../shared/discovery-evidence-class-v1.js";

export const RESEARCH_DEPTH_SELECTOR_V1 = "gdi-nextgen-research-depth-selector-v1";

export const HUNTER_RESEARCH_LEVEL = Object.freeze({
  L1_DISCOVERY_SCREEN: "L1_DISCOVERY_SCREEN",
  L2_EVIDENCE_GAP: "L2_EVIDENCE_GAP",
  L3_DEEP_QUALIFICATION: "L3_DEEP_QUALIFICATION",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function commercialPrioritiesBoost(commercialPriorities = null, row = {}) {
  if (!commercialPriorities || typeof commercialPriorities !== "object") return 0;
  const blob = JSON.stringify(commercialPriorities).toLowerCase();
  const hay = [
    row.demandTrigger,
    row.activityTitle,
    row.pursuitThesis,
    row.geographicScope,
  ]
    .map(text)
    .join(" ")
    .toLowerCase();
  let boost = 0;
  if (/government|procurement|nih|federal/.test(hay) && /government|public/.test(blob)) {
    boost += 0.12;
  }
  if (/sport|tournament|soccer|housing/.test(hay) && /sport|weekend|housing/.test(blob)) {
    boost += 0.12;
  }
  if (/university|alumni|campus|education/.test(hay) && /education|university|alumni/.test(blob)) {
    boost += 0.1;
  }
  if (/project|crew|mobiliz|construction/.test(hay) && /project|extended|crew/.test(blob)) {
    boost += 0.1;
  }
  if (/meeting|kickoff|training|offsite/.test(hay) && /meeting|corporate|incentive/.test(blob)) {
    boost += 0.1;
  }
  return Math.min(0.25, boost);
}

function hotelFeedbackPenalty(hotelFeedback = null) {
  if (!hotelFeedback || typeof hotelFeedback !== "object") return 0;
  const status = text(
    hotelFeedback.commercialValidation ||
      hotelFeedback.status ||
      hotelFeedback.disposition
  ).toUpperCase();
  if (/REJECT|NOT_RELEVANT|FALSE_POSITIVE/.test(status)) return 0.4;
  if (/MONITOR|WATCH|LOW/.test(status)) return 0.15;
  if (/USEFUL|PURSUE|HIGH|VALIDATED/.test(status)) return -0.1;
  return 0;
}

/**
 * Decide research level for one normalized discovery.
 * Paid L2/L3 never execute here — only planned when authorized flags are present.
 */
export function selectResearchDepthForDiscovery(row = {}, ctx = {}) {
  const paidAuthorized = ctx.paidAuthorized === true && Number(ctx.budgetUsd) > 0;
  const gaps = [
    ...(row.unresolvedEvidenceGaps || []),
    ...(row.uncertainties || []),
  ].map(text).filter(Boolean);

  const evidenceStrong =
    (row.evidenceUrls || []).length > 0 &&
    row.hasRealisticHotelThesis === true &&
    (row.discoveryEvidenceClass === DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY ||
      row.discoveryEvidenceClass === DISCOVERY_EVIDENCE_CLASS.LIVE_PUBLIC_DISCOVERY ||
      row.discoveryEvidenceClass === DISCOVERY_EVIDENCE_CLASS.LIVE_PAID_DISCOVERY);

  const demandPotential =
    Number(row.hotelDemandRelevance || 0) +
    commercialPrioritiesBoost(ctx.commercialPriorities, row) -
    hotelFeedbackPenalty(ctx.hotelFeedback);

  const opportunityProxy = {
    organizationName: row.organizationName,
    title: row.activityTitle,
    hotelOpportunityThesis: row.pursuitThesis,
    evidence: (row.evidenceUrls || []).map((url) => ({ url })),
    unresolvedQuestions: gaps,
    contactEmail: row.contactEmail,
    namedStakeholders: row.contactName
      ? [{ name: row.contactName, role: row.contactRole }]
      : [],
  };
  const eligibility = evaluateDeepResearchEligibility(opportunityProxy, {
    budgetRemainingUsd: paidAuthorized ? Number(ctx.budgetUsd) : 0,
    providerAvailable: paidAuthorized,
  });

  // Sufficient evidence → stay L1 (skip paid).
  if (
    evidenceStrong &&
    gaps.length <= 2 &&
    demandPotential >= 0.55 &&
    row.hasRealisticHotelThesis
  ) {
    return {
      level: HUNTER_RESEARCH_LEVEL.L1_DISCOVERY_SCREEN,
      reason: "existing_evidence_sufficient_skip_paid",
      skipPaidRetrieval: true,
      plannedPaidProviders: [],
      unresolvedGaps: gaps,
      estimatedCostUsd: 0,
      eligibility,
      commercialValueHint:
        demandPotential >= 0.75 ? "HIGH" : demandPotential >= 0.55 ? "MEDIUM" : "LOW",
    };
  }

  // High-impact unresolved gaps → plan L2 only when paid authorized.
  const highImpactGap = gaps.some((g) =>
    /named_buyer|intermediary|future_activity|sourcing|hotel_specific/i.test(g)
  );
  if (highImpactGap && demandPotential >= 0.55) {
    if (!paidAuthorized) {
      return {
        level: HUNTER_RESEARCH_LEVEL.L1_DISCOVERY_SCREEN,
        reason: "l2_evidence_gaps_present_but_paid_not_authorized",
        skipPaidRetrieval: true,
        plannedPaidProviders: ["exa"],
        unresolvedGaps: gaps,
        estimatedCostUsd: 0,
        eligibility,
        deferredPaidPilot: true,
        commercialValueHint: demandPotential >= 0.75 ? "HIGH" : "MEDIUM",
      };
    }
    return {
      level: HUNTER_RESEARCH_LEVEL.L2_EVIDENCE_GAP,
      reason: "targeted_evidence_gap_research_authorized",
      skipPaidRetrieval: false,
      plannedPaidProviders: ["exa"],
      unresolvedGaps: gaps,
      estimatedCostUsd: Number(ctx.l2EstimateUsd ?? 1.5),
      eligibility,
      commercialValueHint: demandPotential >= 0.75 ? "HIGH" : "MEDIUM",
    };
  }

  // Deep qualification — only with authorization + strong demand + eligibility.
  if (
    paidAuthorized &&
    demandPotential >= 0.7 &&
    (eligibility.decision === ELIGIBILITY_DECISION.ELIGIBLE_TIER_2 ||
      eligibility.decision === ELIGIBILITY_DECISION.ELIGIBLE_TIER_3)
  ) {
    return {
      level: HUNTER_RESEARCH_LEVEL.L3_DEEP_QUALIFICATION,
      reason: "deep_qualification_authorized",
      skipPaidRetrieval: false,
      plannedPaidProviders: ["exa", "apify"].filter(Boolean),
      unresolvedGaps: gaps,
      estimatedCostUsd: Number(ctx.l3EstimateUsd ?? 4),
      eligibility,
      commercialValueHint: "HIGH",
      researchDepthTier: RESEARCH_DEPTH_TIER,
    };
  }

  return {
    level: HUNTER_RESEARCH_LEVEL.L1_DISCOVERY_SCREEN,
    reason: "default_screen_only",
    skipPaidRetrieval: true,
    plannedPaidProviders: [],
    unresolvedGaps: gaps,
    estimatedCostUsd: 0,
    eligibility,
    commercialValueHint: demandPotential >= 0.55 ? "MEDIUM" : "LOW",
  };
}

export function selectResearchDepthBatch(discoveries = [], ctx = {}) {
  const decisions = (discoveries || []).map((row) => ({
    discoveryKey: row.dedupeKey || row.organizationName,
    organizationName: row.organizationName,
    activityTitle: row.activityTitle,
    ...selectResearchDepthForDiscovery(row, ctx),
  }));
  return {
    schema: RESEARCH_DEPTH_SELECTOR_V1,
    paidAuthorized: ctx.paidAuthorized === true,
    budgetUsd: Number(ctx.budgetUsd) || 0,
    decisions,
    summary: {
      L1: decisions.filter((d) => d.level === HUNTER_RESEARCH_LEVEL.L1_DISCOVERY_SCREEN)
        .length,
      L2: decisions.filter((d) => d.level === HUNTER_RESEARCH_LEVEL.L2_EVIDENCE_GAP)
        .length,
      L3: decisions.filter(
        (d) => d.level === HUNTER_RESEARCH_LEVEL.L3_DEEP_QUALIFICATION
      ).length,
      skipPaid: decisions.filter((d) => d.skipPaidRetrieval).length,
      deferredPaidPilot: decisions.filter((d) => d.deferredPaidPilot).length,
      plannedSpendUsd: decisions.reduce(
        (s, d) => s + (Number(d.estimatedCostUsd) || 0),
        0
      ),
    },
  };
}
