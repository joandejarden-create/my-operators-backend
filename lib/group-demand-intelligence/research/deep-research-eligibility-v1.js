/**
 * Deep-research eligibility + research-depth tiers for existing GDI opportunities.
 * Does not replace discovery or commercial qualification.
 */

import { INTERNAL_DISPOSITION, REVENUE_TYPE } from "./commercial-qualification-v3.js";

export const DEEP_RESEARCH_ELIGIBILITY_V1 = "gdi-deep-research-eligibility-v1";

export const RESEARCH_DEPTH_TIER = Object.freeze({
  TIER_1_INITIAL_SCREENING: "TIER_1_INITIAL_SCREENING",
  TIER_2_AUTONOMOUS_DEEP_RESEARCH: "TIER_2_AUTONOMOUS_DEEP_RESEARCH",
  TIER_3_TARGETED_SPECIALIST_ENRICHMENT: "TIER_3_TARGETED_SPECIALIST_ENRICHMENT",
});

export const ELIGIBILITY_DECISION = Object.freeze({
  ELIGIBLE_TIER_2: "ELIGIBLE_TIER_2",
  ELIGIBLE_TIER_3: "ELIGIBLE_TIER_3",
  SCREEN_ONLY_TIER_1: "SCREEN_ONLY_TIER_1",
  NOT_ELIGIBLE: "NOT_ELIGIBLE",
  DEFER_BUDGET_OR_PROVIDER: "DEFER_BUDGET_OR_PROVIDER",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hasNamedBuyer(opportunity = {}) {
  const people = [
    ...(opportunity.namedStakeholders || []),
    ...(opportunity.namedStakeholdersFound || []),
  ];
  return people.some((p) => text(p?.name || p?.personName));
}

function contactComplete(opportunity = {}) {
  const email = opportunity.contactEmail || opportunity.contact?.businessEmail;
  if (!email) return false;
  if (hasNamedBuyer(opportunity) && opportunity.contact?.personName) return true;
  // Functional org contact counts as usable but incomplete for buyer intelligence
  return Boolean(email);
}

function hotelRelevance(opportunity = {}) {
  const thesis = text(opportunity.hotelOpportunityThesis || opportunity.whyMyHotel || "");
  const fit = opportunity.geoFit?.geoFitClass || opportunity.hotelFit || null;
  if (/STRONG|plausible|overflow|catchment|metro|incentive|housing/i.test(thesis)) return "HIGH";
  if (fit && !/MISMATCH|ELSEWHERE/i.test(String(fit))) return "MEDIUM";
  if (thesis.length > 40) return "MEDIUM";
  return "LOW";
}

function demandActivityVerified(opportunity = {}) {
  const room = String(opportunity.roomDemandStatus || "");
  const venue = String(opportunity.venueSourcingStatus || "");
  const type = String(opportunity.opportunityType || opportunity.demandType || "");
  if (/VERIFIED|HOUSING_PROGRAM|PRIMARY_VENUE|OVERFLOW/i.test(`${room} ${venue} ${type}`)) return true;
  if ((opportunity.evidence || opportunity.acceptedClaims || []).length >= 2) return true;
  if (text(opportunity.hotelOpportunityThesis).length > 80) return true;
  return false;
}

function revenuePotential(opportunity = {}) {
  const types = opportunity.revenueTypes || opportunity.relevantRevenue || [];
  if (types.includes?.(REVENUE_TYPE.GUESTROOMS) || /GUESTROOM|HOUSING|OVERFLOW|ROOM/i.test(JSON.stringify(types))) {
    return ["GUESTROOMS"];
  }
  if (/meeting|catering|incentive|housing|overflow/i.test(text(opportunity.hotelOpportunityThesis))) {
    return ["GUESTROOMS", "MEETING_SPACE"];
  }
  return ["GUESTROOMS"];
}

function unresolvedMaterial(opportunity = {}) {
  const qs = [
    ...(opportunity.unresolvedQuestions || []),
    ...(opportunity.openMaterialQuestions || []),
  ].map(text).filter(Boolean);
  const gaps = [];
  if (!hasNamedBuyer(opportunity)) {
    gaps.push("Who influences hotel sourcing / contracting / program accommodation?");
  }
  if (!/independent|corroborat/i.test(JSON.stringify(opportunity.evidence || []))) {
    gaps.push("Can independent sources substantiate commercial activity?");
  }
  if (opportunity.opportunityType === "OVERFLOW_HOUSING" || /FUTURE_CYCLE|COMPETITIVE/i.test(String(opportunity.internalDisposition || ""))) {
    gaps.push("What is the next-cycle / overflow sourcing status?");
  }
  return [...new Set([...qs, ...gaps])];
}

function depthAlreadyAchieved(opportunity = {}) {
  const investigated = (opportunity.investigatedUrls || []).length;
  const related = Number(opportunity.relatedEntityLeadsExecuted || 0);
  const deep = Boolean(opportunity.deepResearchJobId || opportunity.internalDeepResearchEnrichment);
  if (deep && hasNamedBuyer(opportunity) && related >= 1) return "DEEP";
  if (investigated >= 5 || related >= 1) return "PARTIAL";
  if (investigated >= 1 || (opportunity.evidence || []).length >= 2) return "SCREENED";
  return "SHALLOW";
}

/**
 * Commercially intelligent eligibility for autonomous long-form research.
 */
export function evaluateDeepResearchEligibility(opportunity = {}, context = {}) {
  const relevance = hotelRelevance(opportunity);
  const demandOk = demandActivityVerified(opportunity);
  const revenue = revenuePotential(opportunity);
  const stage = opportunity.internalDisposition
    || opportunity.commercialStage
    || opportunity.disposition
    || opportunity.opportunityQualification
    || "UNKNOWN";
  const unresolved = unresolvedMaterial(opportunity);
  const depth = depthAlreadyAchieved(opportunity);
  const contactOk = contactComplete(opportunity);
  const namedBuyer = hasNamedBuyer(opportunity);
  const remainingBudgetUsd = Number(context.remainingBudgetUsd ?? context.perOpportunityCeilingUsd ?? 0.5);
  const providersAvailable = context.providersAvailable !== false;
  const weakGeneric = relevance === "LOW" && !demandOk && unresolved.length === 0;
  const feedback = context.hotelFeedback || opportunity.hotelSalesFeedback || null;

  let decision = ELIGIBILITY_DECISION.NOT_ELIGIBLE;
  let tier = RESEARCH_DEPTH_TIER.TIER_1_INITIAL_SCREENING;
  const reasons = [];

  if (feedback && /not.?relevant|already.?client|already.?existing.?client/i.test(text(feedback.status || feedback.label || feedback))) {
    decision = ELIGIBILITY_DECISION.NOT_ELIGIBLE;
    reasons.push("hotel_feedback_suppresses_deep_research");
  } else if (weakGeneric) {
    decision = ELIGIBILITY_DECISION.NOT_ELIGIBLE;
    reasons.push("weak_or_generic_organization_low_hotel_relevance");
  } else if (!providersAvailable || remainingBudgetUsd <= 0) {
    decision = ELIGIBILITY_DECISION.DEFER_BUDGET_OR_PROVIDER;
    tier = RESEARCH_DEPTH_TIER.TIER_1_INITIAL_SCREENING;
    reasons.push("budget_or_provider_unavailable");
  } else if (
    depth === "DEEP"
    && namedBuyer
    && unresolved.length <= 1
  ) {
    decision = ELIGIBILITY_DECISION.SCREEN_ONLY_TIER_1;
    reasons.push("already_deep_with_buyer_intelligence");
  } else if (
    namedBuyer
    && contactOk
    && unresolved.some((q) => /sourc|cycle|independent|program/i.test(q))
    && remainingBudgetUsd >= 0.05
  ) {
    decision = ELIGIBILITY_DECISION.ELIGIBLE_TIER_3;
    tier = RESEARCH_DEPTH_TIER.TIER_3_TARGETED_SPECIALIST_ENRICHMENT;
    reasons.push("targeted_unresolved_buying_or_sourcing_question");
  } else if (
    (relevance === "HIGH" || relevance === "MEDIUM")
    && demandOk
    && unresolved.length > 0
    && depth !== "DEEP"
  ) {
    decision = ELIGIBILITY_DECISION.ELIGIBLE_TIER_2;
    tier = RESEARCH_DEPTH_TIER.TIER_2_AUTONOMOUS_DEEP_RESEARCH;
    reasons.push("promising_opportunity_material_unresolved_questions");
    if (!namedBuyer) reasons.push("buyer_intelligence_gap");
  } else if (demandOk && relevance !== "LOW") {
    decision = ELIGIBILITY_DECISION.SCREEN_ONLY_TIER_1;
    reasons.push("screen_reuse_existing_evidence_first");
  } else {
    reasons.push("does_not_meet_deep_research_bar");
  }

  // Early-stage account development / pre-sourcing does NOT require active RFP
  const requiresActiveRfp = false;

  return {
    version: DEEP_RESEARCH_ELIGIBILITY_V1,
    decision,
    tier,
    eligible: decision === ELIGIBILITY_DECISION.ELIGIBLE_TIER_2
      || decision === ELIGIBILITY_DECISION.ELIGIBLE_TIER_3,
    hotelRelevance: relevance,
    demandActivityVerified: demandOk,
    revenuePotential: revenue,
    commercialStage: stage,
    unresolvedQuestions: unresolved,
    researchDepthAchieved: depth,
    contactIntelligenceComplete: contactOk && namedBuyer,
    contactUsableOrgRoute: contactOk,
    namedBuyerEstablished: namedBuyer,
    expectedValueOfInvestigation: decision.startsWith("ELIGIBLE") ? "MATERIAL" : "LOW",
    previouslyAttemptedResearch: Boolean(opportunity.deepResearchJobId || (opportunity.investigatedUrls || []).length),
    remainingBudgetUsd,
    providersAvailable,
    requiresActiveRfp,
    reasons,
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    hotelId: opportunity.hotelId || null,
  };
}

export function tierBudgetDefaults(tier) {
  switch (tier) {
    case RESEARCH_DEPTH_TIER.TIER_3_TARGETED_SPECIALIST_ENRICHMENT:
      return { maxIterations: 3, maxLeadFollowUps: 2, suggestedCeilingUsd: 0.35 };
    case RESEARCH_DEPTH_TIER.TIER_2_AUTONOMOUS_DEEP_RESEARCH:
      return { maxIterations: 5, maxLeadFollowUps: 3, suggestedCeilingUsd: 0.5 };
    default:
      return { maxIterations: 1, maxLeadFollowUps: 0, suggestedCeilingUsd: 0 };
  }
}

/**
 * Map hotel sales feedback into research priority adjustments (does not treat feedback as public fact).
 */
export function applySalesFeedbackToResearchPriority(opportunity = {}, feedbackItem = null) {
  if (!feedbackItem) {
    return { priorityDelta: 0, suppressDeepResearch: false, refreshContact: false, notes: [] };
  }
  const label = text(feedbackItem.status || feedbackItem.label || feedbackItem.rationale || feedbackItem);
  const notes = [];
  let priorityDelta = 0;
  let suppressDeepResearch = false;
  let refreshContact = false;

  if (/not.?relevant/i.test(label)) {
    suppressDeepResearch = true;
    priorityDelta = -5;
    notes.push("feedback_not_relevant");
  } else if (/already.?(an )?existing.?client|already.?client/i.test(label)) {
    suppressDeepResearch = true;
    priorityDelta = -3;
    notes.push("feedback_existing_client");
  } else if (/contact.?incorrect|contact.?updated/i.test(label)) {
    refreshContact = true;
    priorityDelta = 2;
    notes.push("feedback_contact_refresh");
  } else if (/worth.?pursu|buyer.?interest|expressed.?interest|sales.?team.?contacted/i.test(label)) {
    priorityDelta = 3;
    notes.push("feedback_pursue");
  } else if (/no.?current.?demand|follow.?up.?in.?a.?future|committed|opportunity.?already.?committed/i.test(label)) {
    priorityDelta = -1;
    notes.push("feedback_future_or_committed");
  } else if (/organization.?not.?responsible/i.test(label)) {
    suppressDeepResearch = true;
    notes.push("feedback_org_not_sourcing");
  }

  return {
    priorityDelta,
    suppressDeepResearch,
    refreshContact,
    notes,
    feedbackLabel: label || null,
    opportunityId: opportunity.id || opportunity.opportunityId || null,
  };
}

export function buildDeepResearchWeeklyHookDesign() {
  return {
    version: DEEP_RESEARCH_ELIGIBILITY_V1,
    featureFlag: "GDI_WEEKLY_DEEP_RESEARCH_ENABLED",
    defaultEnabled: false,
    productionWeeklyJobSwitched: false,
    insertionPoint: "runWeeklyRefreshCycle after buildWeeklyWorkQueues — CONTACT_ENRICHMENT / named_buyer_enrichment_pending / high-priority feedback followup",
    secondaryInsertionPoint: "post promoteQualifiedGdiOpportunity in weekly discovery v1.2 when named buyer missing",
    eligibility: "evaluateDeepResearchEligibility",
    entrypoint: "runDeepResearchOnExistingGdiOpportunity → matureOpportunity",
    paidRequiresSeparateAuthorization: true,
    surfePdlParallel: "disabled_without_separate_go",
  };
}
