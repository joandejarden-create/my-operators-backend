/**
 * GDI 2.0 Phase 2B — adaptive buyer-recovery planner (offline recommendations only).
 *
 * Reuses Scaling buyer strategy ladder + shared eligibility gaps.
 * Does NOT execute paid tools, ORS loops, or provider calls.
 */

import {
  evaluateSharedCommercialEligibility,
  ELIGIBILITY_REASON,
} from "./commercial-eligibility-contract-v1.js";
import {
  BUYER_RESEARCH_STRATEGY_SEQUENCE,
  selectNextBuyerResearchStrategy,
  enrichResearchContinuationWithBuyerStrategy,
} from "./scaling-buyer-research-strategies-v1.js";
import { classifyResearchContinuationDisposition } from "./research-continuation-disposition-v1.js";

export const BUYER_RECOVERY_PLANNER_V1 = "gdi-buyer-recovery-planner-v1";
/** Phase 2C ranking/calibration bump — still offline-only. */
export const BUYER_RECOVERY_PLANNER_CALIBRATION = "phase2c-v1";

export const BUYER_RECOVERY_RECOMMENDATION = Object.freeze({
  CONTINUE_RESEARCH: "CONTINUE_RESEARCH",
  SCHEDULE_REFRESH: "SCHEDULE_REFRESH",
  REQUEST_HUMAN_REVIEW: "REQUEST_HUMAN_REVIEW",
  STOP_WITH_EVIDENCE: "STOP_WITH_EVIDENCE",
  ALREADY_SALES_READY: "ALREADY_SALES_READY",
  COMMERCIALLY_INELIGIBLE: "COMMERCIALLY_INELIGIBLE",
});

/** Extra pathways beyond Scaling ladder — planning only. */
export const EXTENDED_BUYER_RECOVERY_PATHWAYS = Object.freeze([
  {
    id: "EVENT_ORGANIZER_STAFF_DIRECTORY",
    label: "Event / congress organizer staff directory + meetings contacts",
    appliesWhen: ["associations_events", "missing_buyer", "missing_responsibility"],
  },
  {
    id: "PROJECT_SUPPLIER_OR_GC_CONTACTS",
    label: "Project owner / general contractor / deployment PM pathways",
    appliesWhen: ["scaling_companies", "project", "missing_buyer"],
  },
  {
    id: "LOCAL_LANGUAGE_OFFICIAL_PAGES",
    label: "Local-language official about/team/contact pages",
    appliesWhen: ["international", "missing_buyer", "missing_responsibility"],
  },
  {
    id: "CHAPTER_OR_AFFILIATE_LEADERSHIP",
    label: "Chapter / affiliate / local hosting committee leadership",
    appliesWhen: ["associations_events", "missing_buyer"],
  },
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function missingEvidenceKinds(shared = {}) {
  const kinds = [];
  for (const g of shared.gaps || []) {
    if (g === ELIGIBILITY_REASON.MISSING_NAMED_RELEVANT_BUYER) kinds.push("missing_buyer");
    if (g === ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_UNKNOWN) kinds.push("missing_responsibility");
    if (g === ELIGIBILITY_REASON.BUYER_AFFILIATION_MISMATCH) kinds.push("affiliation_uncertain");
    if (g === ELIGIBILITY_REASON.MISSING_CONTACT_ROUTE) kinds.push("missing_route");
    if (g === ELIGIBILITY_REASON.MISSING_HOTEL_DEMAND_THESIS) kinds.push("missing_demand");
    if (g === ELIGIBILITY_REASON.SOURCES_INSUFFICIENT) kinds.push("missing_sources");
    if (g === ELIGIBILITY_REASON.MISSING_CREDIBLE_ORG_ACTIVITY) kinds.push("missing_activity");
  }
  return [...new Set(kinds)];
}

function estimateWorthContinuing({ shared, attemptedIds, budgetExhausted, strategiesExhausted }) {
  if (shared.salesReady) {
    return { worthwhile: false, reason: "already_sales_ready_under_shared_contract" };
  }
  if (shared.status === "COMMERCIALLY_INELIGIBLE") {
    return { worthwhile: false, reason: "commercially_ineligible" };
  }
  const hasDemandOrActivity =
    shared.gates?.realisticHotelDemand === true ||
    shared.gates?.credibleOrganizationActivity === true;
  if (!hasDemandOrActivity) {
    return {
      worthwhile: false,
      reason: "insufficient_demand_or_activity_foundation",
    };
  }
  if (budgetExhausted && strategiesExhausted) {
    return {
      worthwhile: true,
      reason: "schedule_refresh_after_budget_pause",
      mode: "schedule",
    };
  }
  if (strategiesExhausted) {
    return {
      worthwhile: true,
      reason: "public_strategies_exhausted_propose_authorized_paid_later",
      mode: "request_review",
    };
  }
  if (attemptedIds.length >= BUYER_RESEARCH_STRATEGY_SEQUENCE.length) {
    return {
      worthwhile: true,
      reason: "ladder_complete_consider_extended_pathways_or_review",
      mode: "request_review",
    };
  }
  return { worthwhile: true, reason: "recoverable_buyer_gaps", mode: "continue" };
}

/** Gap-aware information-gain scores for ladder strategies (higher = try sooner). */
const STRATEGY_GAIN_BY_GAP = Object.freeze({
  missing_buyer: {
    OFFICIAL_LEADERSHIP_PAGES: 9,
    WORKPLACE_FACILITIES_REAL_ESTATE: 8,
    PEOPLE_HR_TRAINING: 7,
    REGIONAL_OPS_EXPANSION: 7,
    MEETINGS_INCENTIVES_SALES_OPS: 6,
    PROCUREMENT_ADMIN_EA: 5,
    PRESS_NAMED_THEN_VERIFY_AFFILIATION: 3,
    PUBLIC_PROFESSIONAL_PROFILES: 4,
    ORG_ROUTE_ASK_FOR_NAMED: 2,
  },
  missing_responsibility: {
    OFFICIAL_LEADERSHIP_PAGES: 8,
    WORKPLACE_FACILITIES_REAL_ESTATE: 9,
    PEOPLE_HR_TRAINING: 7,
    MEETINGS_INCENTIVES_SALES_OPS: 6,
    PUBLIC_PROFESSIONAL_PROFILES: 7,
    PRESS_NAMED_THEN_VERIFY_AFFILIATION: 2,
    ORG_ROUTE_ASK_FOR_NAMED: 1,
  },
  missing_route: {
    ORG_ROUTE_ASK_FOR_NAMED: 10,
    OFFICIAL_LEADERSHIP_PAGES: 6,
    PUBLIC_PROFESSIONAL_PROFILES: 5,
    PROCUREMENT_ADMIN_EA: 4,
  },
  missing_demand: {
    MEETINGS_INCENTIVES_SALES_OPS: 5,
    OFFICIAL_LEADERSHIP_PAGES: 3,
  },
});

const LOW_INFORMATION_STRATEGY_IDS = new Set([
  "PRESS_NAMED_THEN_VERIFY_AFFILIATION",
]);

function scoreApproach(approach, missing = [], attemptedIds = []) {
  let score = 5;
  for (const gap of missing) {
    const table = STRATEGY_GAIN_BY_GAP[gap];
    if (table && table[approach.id] != null) score += table[approach.id];
  }
  if (approach.source === "extended_pathway") score += 4;
  if (LOW_INFORMATION_STRATEGY_IDS.has(approach.id) && missing.includes("missing_buyer")) {
    score -= 3;
  }
  if (attemptedIds.includes(approach.id)) score -= 100;
  // Prefer genuinely different query families
  if (approach.queryHints?.length) score += Math.min(2, approach.queryHints.length / 3);
  return score;
}

function detectLowInformationRepeats(attempts = []) {
  const byId = new Map();
  for (const a of attempts) {
    const id = text(a?.strategyId || a?.id);
    if (!id) continue;
    const info = a?.informationFound === true;
    const entry = byId.get(id) || { id, attempts: 0, informationFound: 0 };
    entry.attempts += 1;
    if (info) entry.informationFound += 1;
    byId.set(id, entry);
  }
  return [...byId.values()]
    .filter((e) => e.attempts >= 2 && e.informationFound === 0)
    .map((e) => ({
      strategyId: e.id,
      attempts: e.attempts,
      note: "Repeated with no information found — deprioritize or stop this strategy family.",
    }));
}

/**
 * Plan next buyer-recovery approaches offline.
 * @param {object} opportunity
 * @param {{
 *   hotel?: object,
 *   attempts?: Array<{ strategyId?: string }>,
 *   authorizedBudgetExhausted?: boolean,
 *   providerFailed?: boolean,
 *   paidResearchAuthorized?: boolean,
 * }} [opts]
 */
export function planBuyerRecovery(opportunity = {}, opts = {}) {
  const shared = evaluateSharedCommercialEligibility(opportunity, {
    hotel: opts.hotel,
  });
  const continuation = classifyResearchContinuationDisposition({
    opportunity,
    hotel: opts.hotel || {},
    providerFailed: opts.providerFailed === true,
    authorizedBudgetExhausted: opts.authorizedBudgetExhausted === true,
    contactNotYetVerified: shared.gaps?.includes(ELIGIBILITY_REASON.MISSING_NAMED_RELEVANT_BUYER),
  });

  const attempts = Array.isArray(opts.attempts)
    ? opts.attempts
    : Array.isArray(opportunity.buyerResearchAttempts)
      ? opportunity.buyerResearchAttempts
      : [];
  const attemptedIds = attempts
    .map((a) => text(a?.strategyId || a?.id || ""))
    .filter(Boolean);

  const ladder = selectNextBuyerResearchStrategy({
    gaps: shared.gaps || [],
    attempts,
    attributionInvalid: (shared.gaps || []).includes("INVALID_BUYER_CONTACT_ATTRIBUTION"),
  });

  const missing = missingEvidenceKinds(shared);
  const segmentFamily = shared.segmentFamily || "generic";
  const extended = EXTENDED_BUYER_RECOVERY_PATHWAYS.filter((p) => {
    if (attemptedIds.includes(p.id)) return false;
    return p.appliesWhen.some(
      (tag) =>
        tag === segmentFamily ||
        missing.includes(tag) ||
        (tag === "international" && /[áéíóúñü]|mexico|españa|france|brasil/i.test(
          `${opportunity.organizationName || ""} ${opportunity.eventName || ""} ${opportunity.market || ""}`,
        )) ||
        (tag === "project" &&
          /project|mobilization|construction|extended stay/i.test(
            `${opportunity.potentialHotelBusiness || ""} ${opportunity.demandMechanism || ""}`,
          )),
    );
  });

  const lowInfoRepeats = detectLowInformationRepeats(attempts);
  const lowInfoIds = new Set(lowInfoRepeats.map((r) => r.strategyId));

  const prioritizedApproaches = [];
  if (ladder.nextStrategy && !lowInfoIds.has(ladder.nextStrategy.id)) {
    prioritizedApproaches.push({
      ...ladder.nextStrategy,
      source: "scaling_buyer_ladder",
      paid: false,
    });
  }
  for (const s of ladder.remaining) {
    if (lowInfoIds.has(s.id)) continue;
    prioritizedApproaches.push({ ...s, source: "scaling_buyer_ladder", paid: false });
  }
  for (const p of extended) {
    prioritizedApproaches.push({ ...p, source: "extended_pathway", paid: false });
  }

  // Deduplicate, score by expected information gain for current gaps, take top approaches
  const seen = new Set();
  const uniqueApproaches = prioritizedApproaches
    .filter((a) => {
      const id = text(a.id);
      if (!id || seen.has(id)) return false;
      seen.add(id);
      return true;
    })
    .map((a) => ({
      ...a,
      expectedInformationGain: scoreApproach(a, missing, attemptedIds),
      lowInformationRisk: LOW_INFORMATION_STRATEGY_IDS.has(a.id),
    }))
    .sort((a, b) => b.expectedInformationGain - a.expectedInformationGain)
    .slice(0, 8);

  const worth = estimateWorthContinuing({
    shared,
    attemptedIds,
    budgetExhausted: opts.authorizedBudgetExhausted === true,
    strategiesExhausted: ladder.exhausted === true,
  });

  let recommendation = BUYER_RECOVERY_RECOMMENDATION.CONTINUE_RESEARCH;
  if (shared.salesReady) {
    recommendation = BUYER_RECOVERY_RECOMMENDATION.ALREADY_SALES_READY;
  } else if (shared.status === "COMMERCIALLY_INELIGIBLE") {
    recommendation = BUYER_RECOVERY_RECOMMENDATION.COMMERCIALLY_INELIGIBLE;
  } else if (worth.mode === "schedule") {
    recommendation = BUYER_RECOVERY_RECOMMENDATION.SCHEDULE_REFRESH;
  } else if (worth.mode === "request_review" || !worth.worthwhile) {
    recommendation =
      worth.reason === "insufficient_demand_or_activity_foundation"
        ? BUYER_RECOVERY_RECOMMENDATION.STOP_WITH_EVIDENCE
        : BUYER_RECOVERY_RECOMMENDATION.REQUEST_HUMAN_REVIEW;
  } else if (ladder.exhausted && opts.paidResearchAuthorized !== true) {
    recommendation = BUYER_RECOVERY_RECOMMENDATION.SCHEDULE_REFRESH;
  }

  const enriched = enrichResearchContinuationWithBuyerStrategy(
    {
      gaps: shared.gaps,
      buyerResearchAttempts: attempts,
      preserveOpportunity: true,
    },
    { gaps: shared.gaps, attempts },
  );

  // Stopping logic: if only low-info strategies remain and foundation weak → stop/review
  if (
    !shared.salesReady &&
    uniqueApproaches.length === 0 &&
    lowInfoRepeats.length > 0 &&
    recommendation === BUYER_RECOVERY_RECOMMENDATION.CONTINUE_RESEARCH
  ) {
    recommendation = BUYER_RECOVERY_RECOMMENDATION.REQUEST_HUMAN_REVIEW;
  }

  return {
    version: BUYER_RECOVERY_PLANNER_V1,
    calibration: BUYER_RECOVERY_PLANNER_CALIBRATION,
    offlineOnly: true,
    doesNotExecutePaidTools: true,
    doesNotStartAutonomousLoops: true,
    secondNamedContactRequiredForPublication: false,
    opportunityRef: {
      id: opportunity.id || opportunity.opportunityId || null,
      organizationName: text(opportunity.organizationName) || null,
      hotelId: opportunity.hotelId || null,
      segmentFamily,
    },
    missingEvidence: missing,
    sharedEligibilityStatus: shared.status,
    sharedGaps: shared.gaps || [],
    strategiesAlreadyAttempted: attemptedIds,
    lowInformationRepeats: lowInfoRepeats,
    prioritizedApproaches: uniqueApproaches,
    nextApproach: uniqueApproaches[0] || null,
    strategyNovelty:
      uniqueApproaches.length > 0
        ? "has_untried_or_high_gain_approaches"
        : attemptedIds.length
          ? "no_novel_public_approaches_remaining"
          : "no_attempts_yet",
    worthContinuing: worth,
    recommendation,
    continuationDisposition: {
      disposition: continuation.disposition,
      commerciallyExhausted: continuation.commerciallyExhausted,
      preserveOpportunity: continuation.preserveOpportunity,
      nextActions: continuation.nextActions,
    },
    buyerResearchContinuation: enriched,
    costLimits: {
      paidResearchAuthorized: opts.paidResearchAuthorized === true,
      authorizedBudgetExhausted: opts.authorizedBudgetExhausted === true,
      note: "Planner never spends; paid follow-ups require separate founder authorization.",
    },
    note:
      "Offline buyer-recovery plan (Phase 2C gap-aware ranking). Execute only under authorized research modes; do not invent contacts.",
  };
}
