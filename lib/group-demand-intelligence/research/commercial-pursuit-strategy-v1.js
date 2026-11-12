/**
 * Commercial pursuit strategies for GDI deep-research prioritization V1.
 *
 * Extends existing eligibility + fair scheduler — does not replace discovery,
 * ORS, or commercial qualification. Does not invent events or spend.
 *
 * Core commercial rules:
 * - Unconfirmed hotel selection is NOT automatically negative.
 * - Competitor-hosted current edition is NOT permanent disqualification.
 * - CURRENT_HOST_COMMITTED ≠ FUTURE_CYCLE_POTENTIALLY_CONTESTABLE.
 * - No RFP / room-night count required for Account Development research.
 */

export const COMMERCIAL_PURSUIT_STRATEGY_V1 =
  "gdi-commercial-pursuit-strategy-v1";

/** Pursuit strategies across all fourteen demand segments. */
export const COMMERCIAL_PURSUIT_STRATEGY = Object.freeze({
  OPEN_HOTEL_SOURCING: "OPEN_HOTEL_SOURCING",
  COMPETITOR_DISPLACEMENT: "COMPETITOR_DISPLACEMENT",
  FUTURE_CYCLE_DEVELOPMENT: "FUTURE_CYCLE_DEVELOPMENT",
  OVERFLOW_CAPTURE: "OVERFLOW_CAPTURE",
  MEETING_AND_CATERING_DISPLACEMENT: "MEETING_AND_CATERING_DISPLACEMENT",
  CORPORATE_AND_PROJECT_DEMAND: "CORPORATE_AND_PROJECT_DEMAND",
  PARTNERSHIP_AND_REFERRAL_DEVELOPMENT: "PARTNERSHIP_AND_REFERRAL_DEVELOPMENT",
  COMPETITOR_DISRUPTION_OR_UNMET_NEEDS: "COMPETITOR_DISRUPTION_OR_UNMET_NEEDS",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  NOT_PURSUABLE: "NOT_PURSUABLE",
});

/**
 * Hotel / venue sourcing status — independent of commercial maturity.
 * Do not collapse these into HIGH/MEDIUM/WATCH priority labels.
 */
export const PURSUIT_SOURCING_STATUS = Object.freeze({
  OPEN_UNCONFIRMED: "OPEN_UNCONFIRMED",
  CURRENT_HOST_COMMITTED: "CURRENT_HOST_COMMITTED",
  FUTURE_CYCLE_POTENTIALLY_CONTESTABLE: "FUTURE_CYCLE_POTENTIALLY_CONTESTABLE",
  OVERFLOW_POSSIBLE: "OVERFLOW_POSSIBLE",
  PARTNERSHIP_PIPELINE: "PARTNERSHIP_PIPELINE",
  UNKNOWN: "UNKNOWN",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

export const SCREEN_DECISION = Object.freeze({
  ADVANCE_TO_DEEP_RESEARCH: "ADVANCE_TO_DEEP_RESEARCH",
  SCREEN_ONLY: "SCREEN_ONLY",
  REJECT_IRRELEVANT: "REJECT_IRRELEVANT",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function blob(opportunity = {}) {
  return [
    opportunity.hotelOpportunityThesis,
    opportunity.whyThisHotel,
    opportunity.whyMyHotel,
    opportunity.potentialHotelBusiness,
    opportunity.proposedHotelUse,
    opportunity.title,
    opportunity.programmeName,
    opportunity.organizationName,
    opportunity.internalDisposition,
    opportunity.disposition,
    opportunity.geoFit?.geoFitClass,
    opportunity.hotelFit,
    opportunity.venueSourcingStatus,
    opportunity.roomDemandStatus,
    opportunity.sourcingStatus,
    opportunity.opportunityType,
    opportunity.demandType,
    opportunity.researchSegment,
    JSON.stringify(opportunity.evidence || []).slice(0, 800),
  ]
    .map(text)
    .filter(Boolean)
    .join(" | ");
}

function hasEvidence(opportunity = {}) {
  const urls = [
    ...(opportunity.seedUrls || []),
    ...(opportunity.evidence || []).map((e) => e?.url),
    ...(opportunity.acceptedClaims || []).map((e) => e?.url),
  ].filter((u) => /^https?:\/\//i.test(String(u || "")));
  return urls.length > 0 || text(opportunity.hotelOpportunityThesis).length > 40;
}

function hotelFitPlausible(opportunity = {}) {
  const fit = String(
    opportunity.geoFit?.geoFitClass || opportunity.hotelFit || ""
  );
  if (/MISMATCH|ELSEWHERE_ONLY|OUT_OF_MARKET|NO_FIT/i.test(fit)) return false;
  const thesis = text(opportunity.hotelOpportunityThesis || opportunity.whyThisHotel);
  if (/out of market|no bethesda fit|cannot compete|wrong geography/i.test(thesis)) {
    return false;
  }
  return true;
}

function isPlannerPartnershipSegment(segmentId = "") {
  return /preferred_lodging|professional_event_planners|travel_trade/i.test(
    String(segmentId)
  );
}

function isCorporateProjectSegment(segmentId = "") {
  return /corporate_accounts_scaling|projects_infrastructure|relocation_extended|corporate_meetings/i.test(
    String(segmentId)
  );
}

function isLocalCateringSegment(segmentId = "") {
  return /local_catering|social_special|corporate_celebrations/i.test(
    String(segmentId)
  );
}

/**
 * Low-cost commercial screen — first stage before deep-research spend.
 */
export function lowCostCommercialScreen(opportunity = {}, context = {}) {
  const segmentId = String(
    opportunity.researchSegment || opportunity.segmentId || context.segmentId || ""
  );
  const b = blob(opportunity);
  const disposition = String(
    opportunity.internalDisposition || opportunity.disposition || ""
  ).toUpperCase();
  const geo = String(opportunity.geoFit?.geoFitClass || "");
  const evidenceOk = hasEvidence(opportunity);
  const fitOk = hotelFitPlausible(opportunity);
  const org = text(opportunity.organizationName);
  const reasons = [];

  if (!org) {
    return {
      version: COMMERCIAL_PURSUIT_STRATEGY_V1,
      decision: SCREEN_DECISION.REJECT_IRRELEVANT,
      strategy: COMMERCIAL_PURSUIT_STRATEGY.NOT_PURSUABLE,
      sourcingStatus: PURSUIT_SOURCING_STATUS.NOT_APPLICABLE,
      reasons: ["missing_organization_identity"],
      materialGaps: ["organization_identity"],
    };
  }

  if (!evidenceOk && text(opportunity.hotelOpportunityThesis).length < 40) {
    return {
      version: COMMERCIAL_PURSUIT_STRATEGY_V1,
      decision: SCREEN_DECISION.RESEARCH_CONTINUATION,
      strategy: COMMERCIAL_PURSUIT_STRATEGY.RESEARCH_CONTINUATION,
      sourcingStatus: PURSUIT_SOURCING_STATUS.UNKNOWN,
      reasons: ["insufficient_evidence_for_demand_thesis"],
      materialGaps: ["source_evidence", "demand_thesis"],
    };
  }

  if (!fitOk && /MISMATCH|OUT_OF_MARKET/i.test(geo + b)) {
    return {
      version: COMMERCIAL_PURSUIT_STRATEGY_V1,
      decision: SCREEN_DECISION.REJECT_IRRELEVANT,
      strategy: COMMERCIAL_PURSUIT_STRATEGY.NOT_PURSUABLE,
      sourcingStatus: PURSUIT_SOURCING_STATUS.NOT_APPLICABLE,
      reasons: ["hotel_catchment_mismatch_no_viable_bethesda_fit"],
      materialGaps: [],
    };
  }

  // --- strategy classification ---
  let strategy = COMMERCIAL_PURSUIT_STRATEGY.RESEARCH_CONTINUATION;
  let sourcingStatus = PURSUIT_SOURCING_STATUS.UNKNOWN;
  let futureCycleOpportunity = false;
  let currentOrHistoricalHostHotel = null;

  // Prefer explicit named host fields; regex is a fallback only.
  if (text(opportunity.namedHostHotel || opportunity.currentOrHistoricalHostHotel)) {
    currentOrHistoricalHostHotel = text(
      opportunity.namedHostHotel || opportunity.currentOrHistoricalHostHotel
    );
  } else {
    const hostMatch = b.match(
      /(?:hosted at|host hotel|official hotel|room block at)\s+(?:the\s+)?([A-Z][A-Za-z0-9 &.'-]{2,50}(?:Hotel|Marriott|Hilton|Hyatt|Westin|Sheraton|Renaissance|Embassy Suites|Residence Inn|Courtyard|Omni|Kimpton|Shoreham)(?:\s+[A-Z][A-Za-z]+){0,3})/
    );
    if (hostMatch) currentOrHistoricalHostHotel = text(hostMatch[1]);
  }

  const competitorHost =
    /NAMED_COMPETITOR_HOST|COMPETITOR_HOST|competitor hotel|hosted elsewhere|another hotel/i.test(
      `${geo} ${b} ${disposition}`
    ) || Boolean(currentOrHistoricalHostHotel);

  // Unknown selection is NOT a negative — but absence alone is not OPEN_HOTEL_SOURCING.
  // Require an affirmative open/pre-sourcing signal (or PRE_SOURCING disposition).
  const openSourcing =
    /OPEN_UNCONFIRMED|PRE_SOURCING|CURRENTLY_SOURCING|REGISTRATION_OPEN_HOTEL_UNANNOUNCED|sourcing (not|still) (yet )?(confirmed|announced|selected)|hotel (not|still) (yet )?(selected|confirmed|announced)|no (confirmed|announced) (host|hotel|room block)|hotel selection (open|pending|TBD)/i.test(
      `${b} ${disposition} ${opportunity.sourcingStatus || ""} ${opportunity.venueSourcingStatus || ""} ${opportunity.opportunityType || ""}`
    ) ||
    disposition.includes("PRE_SOURCING") ||
    String(opportunity.sourcingStatus || "").toUpperCase() === "CURRENTLY_SOURCING" ||
    String(opportunity.venueSourcingStatus || "").toUpperCase() ===
      "REGISTRATION_OPEN_HOTEL_UNANNOUNCED";

  const overflow =
    /OVERFLOW|split hotel|housing (bureau|partner)|room.?block overflow|additional rooms|stay.?to.?play/i.test(
      b
    ) || /OVERFLOW/i.test(String(opportunity.opportunityType || ""));

  // "annual" / "recurring" alone ≠ contestable future cycle (avoids collapsing
  // CURRENT_HOST_COMMITTED into FUTURE_CYCLE_POTENTIALLY_CONTESTABLE).
  const futureCycle =
    /FUTURE_CYCLE|COMPETITIVE_FUTURE|next (year|edition|cycle)|rebid window|renewal window|rotates venues|future.?cycle|biennial forum|destination (and hotel )?sourcing not yet/i.test(
      `${b} ${disposition}`
    ) ||
    disposition.includes("COMPETITIVE_FUTURE") ||
    opportunity.futureCycleSupported === true ||
    opportunity.futureCycleOpportunity === true;

  const disruption =
    /renovation|capacity constraint|meeting.?space (limit|shortage)|closed for renovation|displacement/i.test(
      b
    );

  const partnership =
    isPlannerPartnershipSegment(segmentId) ||
    /DMC|destination management|event planner|preferred lodging|referral|wedding venue|private venue/i.test(
      b
    );

  const localCateringDisplacement =
    isLocalCateringSegment(segmentId) &&
    /banquet|luncheon|awards dinner|chapter (meeting|dinner)|reception|day meeting|catering/i.test(
      b
    );

  const corporateProject =
    isCorporateProjectSegment(segmentId) ||
    /training programme|relocation|extended stay|project team|visiting (team|crew)/i.test(
      b
    );

  // Segment-specific and hotel-selection signals precede bare "recurring" future-cycle.
  if (disruption && competitorHost) {
    strategy = COMMERCIAL_PURSUIT_STRATEGY.COMPETITOR_DISRUPTION_OR_UNMET_NEEDS;
    sourcingStatus = PURSUIT_SOURCING_STATUS.FUTURE_CYCLE_POTENTIALLY_CONTESTABLE;
    futureCycleOpportunity = true;
    reasons.push("verified_or_stated_competitor_disruption_signal");
  } else if (overflow) {
    strategy = COMMERCIAL_PURSUIT_STRATEGY.OVERFLOW_CAPTURE;
    sourcingStatus = PURSUIT_SOURCING_STATUS.OVERFLOW_POSSIBLE;
    reasons.push("overflow_or_split_housing_signal");
  } else if (competitorHost && futureCycle) {
    strategy = COMMERCIAL_PURSUIT_STRATEGY.COMPETITOR_DISPLACEMENT;
    sourcingStatus = PURSUIT_SOURCING_STATUS.FUTURE_CYCLE_POTENTIALLY_CONTESTABLE;
    futureCycleOpportunity = true;
    reasons.push("competitor_host_with_future_cycle_contestable_signal");
  } else if (competitorHost && !futureCycle && !openSourcing) {
    strategy = COMMERCIAL_PURSUIT_STRATEGY.COMPETITOR_DISPLACEMENT;
    sourcingStatus = PURSUIT_SOURCING_STATUS.CURRENT_HOST_COMMITTED;
    // Still may warrant research for future cycle — not auto-reject
    reasons.push("competitor_host_current_edition_committed_assess_future_cycle");
    futureCycleOpportunity = false;
  } else if (openSourcing && !competitorHost) {
    strategy = COMMERCIAL_PURSUIT_STRATEGY.OPEN_HOTEL_SOURCING;
    sourcingStatus = PURSUIT_SOURCING_STATUS.OPEN_UNCONFIRMED;
    reasons.push("open_or_unconfirmed_hotel_selection");
  } else if (localCateringDisplacement) {
    strategy = COMMERCIAL_PURSUIT_STRATEGY.MEETING_AND_CATERING_DISPLACEMENT;
    sourcingStatus = competitorHost
      ? PURSUIT_SOURCING_STATUS.CURRENT_HOST_COMMITTED
      : PURSUIT_SOURCING_STATUS.OPEN_UNCONFIRMED;
    reasons.push("local_meeting_or_catering_displacement_signal");
  } else if (partnership) {
    strategy = COMMERCIAL_PURSUIT_STRATEGY.PARTNERSHIP_AND_REFERRAL_DEVELOPMENT;
    sourcingStatus = PURSUIT_SOURCING_STATUS.PARTNERSHIP_PIPELINE;
    reasons.push("planner_venue_or_referral_relationship_signal");
  } else if (corporateProject) {
    strategy = COMMERCIAL_PURSUIT_STRATEGY.CORPORATE_AND_PROJECT_DEMAND;
    sourcingStatus = openSourcing
      ? PURSUIT_SOURCING_STATUS.OPEN_UNCONFIRMED
      : PURSUIT_SOURCING_STATUS.UNKNOWN;
    reasons.push("corporate_training_relocation_or_project_demand_signal");
  } else if (futureCycle && !competitorHost) {
    strategy = COMMERCIAL_PURSUIT_STRATEGY.FUTURE_CYCLE_DEVELOPMENT;
    sourcingStatus = PURSUIT_SOURCING_STATUS.FUTURE_CYCLE_POTENTIALLY_CONTESTABLE;
    futureCycleOpportunity = true;
    reasons.push("recurring_or_future_edition_without_announced_sourcing");
  } else if (disposition.includes("EXCLUDE") && !fitOk) {
    strategy = COMMERCIAL_PURSUIT_STRATEGY.NOT_PURSUABLE;
    sourcingStatus = PURSUIT_SOURCING_STATUS.NOT_APPLICABLE;
    reasons.push("excluded_without_viable_hotel_fit");
  } else {
    strategy = COMMERCIAL_PURSUIT_STRATEGY.RESEARCH_CONTINUATION;
    sourcingStatus = PURSUIT_SOURCING_STATUS.UNKNOWN;
    reasons.push("identity_present_but_pursuit_path_unresolved");
  }

  // Competitor-hosted with NO Bethesda fit → reject
  if (
    competitorHost &&
    !fitOk &&
    strategy !== COMMERCIAL_PURSUIT_STRATEGY.OVERFLOW_CAPTURE
  ) {
    return {
      version: COMMERCIAL_PURSUIT_STRATEGY_V1,
      decision: SCREEN_DECISION.REJECT_IRRELEVANT,
      strategy: COMMERCIAL_PURSUIT_STRATEGY.NOT_PURSUABLE,
      sourcingStatus: PURSUIT_SOURCING_STATUS.CURRENT_HOST_COMMITTED,
      futureCycleOpportunity: false,
      currentOrHistoricalHostHotel,
      reasons: [
        "competitor_hosted_without_viable_bethesda_fit_or_overflow_path",
      ],
      materialGaps: [],
    };
  }

  const materialGaps = [];
  if (!opportunity.primaryContactName && !(opportunity.namedStakeholders || []).length) {
    materialGaps.push("named_buyer_or_intermediary");
  }
  if (sourcingStatus === PURSUIT_SOURCING_STATUS.UNKNOWN) {
    materialGaps.push("hotel_selection_status");
  }
  if (
    sourcingStatus === PURSUIT_SOURCING_STATUS.CURRENT_HOST_COMMITTED &&
    !futureCycleOpportunity
  ) {
    materialGaps.push("future_cycle_rebid_or_rotation_evidence");
  }
  if (competitorHost && !currentOrHistoricalHostHotel) {
    materialGaps.push("named_current_or_historical_host_hotel");
  }

  let decision = SCREEN_DECISION.SCREEN_ONLY;
  if (strategy === COMMERCIAL_PURSUIT_STRATEGY.NOT_PURSUABLE) {
    decision = SCREEN_DECISION.REJECT_IRRELEVANT;
  } else if (strategy === COMMERCIAL_PURSUIT_STRATEGY.RESEARCH_CONTINUATION) {
    decision = SCREEN_DECISION.RESEARCH_CONTINUATION;
  } else if (
    strategy === COMMERCIAL_PURSUIT_STRATEGY.OPEN_HOTEL_SOURCING ||
    strategy === COMMERCIAL_PURSUIT_STRATEGY.OVERFLOW_CAPTURE ||
    strategy === COMMERCIAL_PURSUIT_STRATEGY.COMPETITOR_DISRUPTION_OR_UNMET_NEEDS ||
    (strategy === COMMERCIAL_PURSUIT_STRATEGY.COMPETITOR_DISPLACEMENT &&
      sourcingStatus ===
        PURSUIT_SOURCING_STATUS.FUTURE_CYCLE_POTENTIALLY_CONTESTABLE) ||
    strategy === COMMERCIAL_PURSUIT_STRATEGY.FUTURE_CYCLE_DEVELOPMENT ||
    strategy === COMMERCIAL_PURSUIT_STRATEGY.PARTNERSHIP_AND_REFERRAL_DEVELOPMENT ||
    strategy === COMMERCIAL_PURSUIT_STRATEGY.MEETING_AND_CATERING_DISPLACEMENT ||
    strategy === COMMERCIAL_PURSUIT_STRATEGY.CORPORATE_AND_PROJECT_DEMAND
  ) {
    decision = SCREEN_DECISION.ADVANCE_TO_DEEP_RESEARCH;
  } else if (
    strategy === COMMERCIAL_PURSUIT_STRATEGY.COMPETITOR_DISPLACEMENT &&
    sourcingStatus === PURSUIT_SOURCING_STATUS.CURRENT_HOST_COMMITTED
  ) {
    // Deep research justified to resolve future-cycle uncertainty — not auto EXCLUDE
    decision = SCREEN_DECISION.ADVANCE_TO_DEEP_RESEARCH;
    reasons.push("deep_research_to_resolve_future_cycle_contestability");
  }

  return {
    version: COMMERCIAL_PURSUIT_STRATEGY_V1,
    decision,
    strategy,
    sourcingStatus,
    futureCycleOpportunity,
    currentOrHistoricalHostHotel,
    segmentId: segmentId || null,
    reasons,
    materialGaps,
    requiresActiveRfp: false,
    unknownHotelSelectionIsNegative: false,
    competitorHostIsPermanentDisqualification: false,
  };
}

/**
 * Score deep-research investment after low-cost screening.
 * Higher = more valuable to spend remaining budget resolving uncertainty.
 */
export function scoreDeepResearchInvestment(opportunity = {}, context = {}) {
  const screen =
    context.screen || lowCostCommercialScreen(opportunity, context);
  let score = 0;
  const notes = [];

  if (screen.decision === SCREEN_DECISION.REJECT_IRRELEVANT) {
    return { score: -100, notes: ["reject_irrelevant"], screen };
  }
  if (screen.decision === SCREEN_DECISION.RESEARCH_CONTINUATION) {
    score -= 10;
    notes.push("speculative_continuation");
  }
  if (screen.decision === SCREEN_DECISION.ADVANCE_TO_DEEP_RESEARCH) {
    score += 40;
    notes.push("advance_to_deep");
  }

  switch (screen.strategy) {
    case COMMERCIAL_PURSUIT_STRATEGY.OPEN_HOTEL_SOURCING:
      score += 35;
      notes.push("open_hotel_sourcing");
      break;
    case COMMERCIAL_PURSUIT_STRATEGY.OVERFLOW_CAPTURE:
      score += 30;
      notes.push("overflow_capture");
      break;
    case COMMERCIAL_PURSUIT_STRATEGY.COMPETITOR_DISRUPTION_OR_UNMET_NEEDS:
      score += 32;
      notes.push("competitor_disruption");
      break;
    case COMMERCIAL_PURSUIT_STRATEGY.COMPETITOR_DISPLACEMENT:
      score +=
        screen.sourcingStatus ===
        PURSUIT_SOURCING_STATUS.FUTURE_CYCLE_POTENTIALLY_CONTESTABLE
          ? 28
          : 18;
      notes.push("competitor_displacement");
      break;
    case COMMERCIAL_PURSUIT_STRATEGY.FUTURE_CYCLE_DEVELOPMENT:
      score += 26;
      notes.push("future_cycle_development");
      break;
    case COMMERCIAL_PURSUIT_STRATEGY.PARTNERSHIP_AND_REFERRAL_DEVELOPMENT:
      score += 24;
      notes.push("partnership_referral");
      break;
    case COMMERCIAL_PURSUIT_STRATEGY.MEETING_AND_CATERING_DISPLACEMENT:
      score += 22;
      notes.push("meeting_catering_displacement");
      break;
    case COMMERCIAL_PURSUIT_STRATEGY.CORPORATE_AND_PROJECT_DEMAND:
      score += 25;
      notes.push("corporate_project_demand");
      break;
    default:
      score += 0;
  }

  // Value of resolving uncertainty (buyer / sourcing gaps)
  score += Math.min(20, (screen.materialGaps || []).length * 6);
  if ((screen.materialGaps || []).includes("named_buyer_or_intermediary")) {
    notes.push("buyer_gap_valuable_to_resolve");
  }

  // Hotel fit + revenue
  if (hotelFitPlausible(opportunity)) score += 12;
  const thesis = text(opportunity.hotelOpportunityThesis);
  if (/guestroom|room block|housing|overflow|catering|banquet|meeting/i.test(thesis)) {
    score += 10;
  }

  // Recurrence / lifetime value
  if (/annual|recurring|every year|biennial/i.test(blob(opportunity))) {
    score += 12;
    notes.push("recurring_lifetime_value");
  }

  // Timing / need-date
  if (context.needDateMatch === true || opportunity.needDateMatch === true) {
    score += 15;
    notes.push("need_date_match");
  }

  // Sales feedback
  const fb = applyFeedbackDelta(context.hotelFeedback || opportunity.hotelSalesFeedback);
  score += fb.delta;
  notes.push(...fb.notes);
  if (fb.suppress) {
    score -= 50;
    notes.push("feedback_suppress");
  }

  // Do not reward "already complete info" over contestable demand
  if (
    (opportunity.namedStakeholders || []).length &&
    screen.sourcingStatus === PURSUIT_SOURCING_STATUS.CURRENT_HOST_COMMITTED &&
    !screen.futureCycleOpportunity
  ) {
    score -= 8;
    notes.push("info_rich_but_current_host_committed");
  }

  // Exclude pure noise EXCLUDE without pursuit path
  const disp = String(opportunity.internalDisposition || "").toUpperCase();
  if (
    disp.includes("EXCLUDE") &&
    screen.strategy === COMMERCIAL_PURSUIT_STRATEGY.RESEARCH_CONTINUATION
  ) {
    score -= 25;
    notes.push("exclude_without_pursuit_path");
  }

  // Research cost awareness (caller may pass remaining budget)
  const remaining = Number(context.remainingBudgetUsd ?? 1);
  if (remaining < 0.15 && score < 50) {
    score -= 15;
    notes.push("low_remaining_budget_deprioritize_marginal");
  }

  return {
    version: COMMERCIAL_PURSUIT_STRATEGY_V1,
    score,
    notes,
    screen,
  };
}

function applyFeedbackDelta(feedbackItem) {
  if (!feedbackItem) return { delta: 0, suppress: false, notes: [] };
  const label = text(
    feedbackItem.status || feedbackItem.label || feedbackItem.rationale || feedbackItem
  );
  if (/not.?relevant|organization.?not.?responsible/i.test(label)) {
    return { delta: -40, suppress: true, notes: ["feedback_not_relevant"] };
  }
  if (/already.?(an )?existing.?client|already.?client/i.test(label)) {
    return { delta: -20, suppress: true, notes: ["feedback_existing_client"] };
  }
  if (/worth.?pursu|buyer.?interest|expressed.?interest/i.test(label)) {
    return { delta: 20, suppress: false, notes: ["feedback_pursue"] };
  }
  if (/no.?current.?demand|follow.?up.?in.?a.?future|committed/i.test(label)) {
    return { delta: -5, suppress: false, notes: ["feedback_future_or_committed"] };
  }
  return { delta: 0, suppress: false, notes: [] };
}

/**
 * Attach pursuit classification onto an opportunity (non-destructive).
 */
export function enrichOpportunityWithPursuitStrategy(opportunity = {}, context = {}) {
  const screen = lowCostCommercialScreen(opportunity, context);
  const investment = scoreDeepResearchInvestment(opportunity, {
    ...context,
    screen,
  });
  return {
    ...opportunity,
    commercialPursuitStrategy: screen.strategy,
    pursuitSourcingStatus: screen.sourcingStatus,
    futureCycleOpportunity: screen.futureCycleOpportunity === true,
    currentOrHistoricalHostHotel: screen.currentOrHistoricalHostHotel || null,
    pursuitScreenDecision: screen.decision,
    pursuitMaterialGaps: screen.materialGaps || [],
    deepResearchInvestmentScore: investment.score,
    pursuitReasons: screen.reasons || [],
    requiresActiveRfp: false,
  };
}

export default {
  COMMERCIAL_PURSUIT_STRATEGY_V1,
  COMMERCIAL_PURSUIT_STRATEGY,
  PURSUIT_SOURCING_STATUS,
  SCREEN_DECISION,
  lowCostCommercialScreen,
  scoreDeepResearchInvestment,
  enrichOpportunityWithPursuitStrategy,
};
