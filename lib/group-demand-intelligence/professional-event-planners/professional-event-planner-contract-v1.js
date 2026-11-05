/**
 * Professional Event Planner Partnership — Segment 8 subtype contract.
 * Demand segment remains social_special_events; subtype is not a 13th/14th segment.
 * Complementary to PREFERRED_LODGING_PARTNERSHIP (venues vs planners/intermediaries).
 */
export const PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP_V1 =
  "gdi-professional-event-planner-partnership-v1";

export const PROFESSIONAL_EVENT_PLANNER_SUBTYPE =
  "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP";

export const PROFESSIONAL_EVENT_PLANNER_SEGMENT_ID = "social_special_events";

export const PROFESSIONAL_EVENT_PLANNER_CLASSIFICATION_LINE =
  "Professional Event Planner Partnership · Social Groups & Special Events";

/** Evidence-grounded partnership-potential tiers — never popularity rankings. */
export const PLANNER_PARTNERSHIP_POTENTIAL = Object.freeze({
  ESTABLISHED_EVENT_PRODUCER: "ESTABLISHED_EVENT_PRODUCER",
  RELEVANT_SPECIALIST: "RELEVANT_SPECIALIST",
  EMERGING_RESEARCH_CONTINUATION: "EMERGING_RESEARCH_CONTINUATION",
  NOT_QUALIFIED: "NOT_QUALIFIED",
  UNKNOWN: "UNKNOWN",
});

export const PLANNER_PARTNERSHIP_POTENTIAL_LABEL = Object.freeze({
  [PLANNER_PARTNERSHIP_POTENTIAL.ESTABLISHED_EVENT_PRODUCER]:
    "Established Event Producer",
  [PLANNER_PARTNERSHIP_POTENTIAL.RELEVANT_SPECIALIST]: "Relevant Specialist",
  [PLANNER_PARTNERSHIP_POTENTIAL.EMERGING_RESEARCH_CONTINUATION]:
    "Emerging / Research Continuation",
  [PLANNER_PARTNERSHIP_POTENTIAL.NOT_QUALIFIED]: "Not qualified",
  [PLANNER_PARTNERSHIP_POTENTIAL.UNKNOWN]: "Partnership potential unknown",
});

/**
 * Hotel partnership lifecycle — reflect hotel-team actions or verified evidence only.
 * Research must never auto-mark Partnership Confirmed.
 */
export const PLANNER_PARTNERSHIP_STATUS = Object.freeze({
  NOT_CONTACTED: "NOT_CONTACTED",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  OUTREACH_PLANNED: "OUTREACH_PLANNED",
  CONTACTED: "CONTACTED",
  RELATIONSHIP_ESTABLISHED: "RELATIONSHIP_ESTABLISHED",
  PREFERRED_PARTNER_DISCUSSION: "PREFERRED_PARTNER_DISCUSSION",
  PARTNERSHIP_CONFIRMED: "PARTNERSHIP_CONFIRMED",
  REFERRAL_RECEIVED: "REFERRAL_RECEIVED",
  BUSINESS_GENERATED: "BUSINESS_GENERATED",
  NOT_RELEVANT: "NOT_RELEVANT",
  RELATIONSHIP_INACTIVE: "RELATIONSHIP_INACTIVE",
});

export const PLANNER_PARTNERSHIP_STATUS_LABEL = Object.freeze({
  [PLANNER_PARTNERSHIP_STATUS.NOT_CONTACTED]: "Not Contacted",
  [PLANNER_PARTNERSHIP_STATUS.RESEARCH_CONTINUATION]: "Research Continuation",
  [PLANNER_PARTNERSHIP_STATUS.OUTREACH_PLANNED]: "Outreach Planned",
  [PLANNER_PARTNERSHIP_STATUS.CONTACTED]: "Contacted",
  [PLANNER_PARTNERSHIP_STATUS.RELATIONSHIP_ESTABLISHED]:
    "Relationship Established",
  [PLANNER_PARTNERSHIP_STATUS.PREFERRED_PARTNER_DISCUSSION]:
    "Preferred Partner Discussion",
  [PLANNER_PARTNERSHIP_STATUS.PARTNERSHIP_CONFIRMED]: "Partnership Confirmed",
  [PLANNER_PARTNERSHIP_STATUS.REFERRAL_RECEIVED]: "Referral Received",
  [PLANNER_PARTNERSHIP_STATUS.BUSINESS_GENERATED]: "Business Generated",
  [PLANNER_PARTNERSHIP_STATUS.NOT_RELEVANT]: "Not Relevant",
  [PLANNER_PARTNERSHIP_STATUS.RELATIONSHIP_INACTIVE]: "Relationship Inactive",
});

/** Credible hotel revenue mechanisms — at least one required; none universally mandatory. */
export const PLANNER_HOTEL_REVENUE_MECHANISM = Object.freeze({
  GROUP_GUESTROOMS: "GROUP_GUESTROOMS",
  WEDDING_ROOM_BLOCKS: "WEDDING_ROOM_BLOCKS",
  BANQUET_CATERING: "BANQUET_CATERING",
  MEETING_EVENT_SPACE: "MEETING_EVENT_SPACE",
  PRIVATE_DINING: "PRIVATE_DINING",
  RESORT_CELEBRATIONS: "RESORT_CELEBRATIONS",
  INCENTIVE_GROUPS: "INCENTIVE_GROUPS",
  OVERNIGHT_REFERRALS: "OVERNIGHT_REFERRALS",
  RECURRING_PROGRAMMES: "RECURRING_PROGRAMMES",
});

export const PLANNER_HOTEL_REVENUE_MECHANISM_LABEL = Object.freeze({
  [PLANNER_HOTEL_REVENUE_MECHANISM.GROUP_GUESTROOMS]: "Group guestrooms",
  [PLANNER_HOTEL_REVENUE_MECHANISM.WEDDING_ROOM_BLOCKS]: "Wedding room blocks",
  [PLANNER_HOTEL_REVENUE_MECHANISM.BANQUET_CATERING]: "Banquets / catering",
  [PLANNER_HOTEL_REVENUE_MECHANISM.MEETING_EVENT_SPACE]: "Meeting / event space",
  [PLANNER_HOTEL_REVENUE_MECHANISM.PRIVATE_DINING]: "Private dining",
  [PLANNER_HOTEL_REVENUE_MECHANISM.RESORT_CELEBRATIONS]: "Resort celebrations",
  [PLANNER_HOTEL_REVENUE_MECHANISM.INCENTIVE_GROUPS]: "Incentive groups",
  [PLANNER_HOTEL_REVENUE_MECHANISM.OVERNIGHT_REFERRALS]:
    "Overnight accommodation referrals",
  [PLANNER_HOTEL_REVENUE_MECHANISM.RECURRING_PROGRAMMES]:
    "Recurring event programmes",
});

/** Vendor classes that are NOT automatic hotel demand producers. */
export const PLANNER_EXCLUDED_VENDOR_CLASSES = Object.freeze([
  "decorator",
  "florist",
  "photographer",
  "equipment_supplier",
  "entertainment_vendor",
  "dj",
  "caterer_only_no_planning",
]);

export function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizePlannerPartnershipStatus(raw) {
  const s = String(raw || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "_");
  if (PLANNER_PARTNERSHIP_STATUS[s]) return s;
  if (s === "WATCHLIST" || s === "RESEARCH") {
    return PLANNER_PARTNERSHIP_STATUS.RESEARCH_CONTINUATION;
  }
  return PLANNER_PARTNERSHIP_STATUS.NOT_CONTACTED;
}

/**
 * Detect Professional Event Planner Partnership opportunities.
 */
export function isProfessionalEventPlannerOpportunity(opp = {}) {
  const subtype = text(
    opp.demandOpportunitySubtype ||
      opp.opportunitySubtype ||
      opp.professionalEventPlannerSubtype,
  );
  if (subtype === PROFESSIONAL_EVENT_PLANNER_SUBTYPE) return true;
  if (opp.professionalEventPlannerPartnershipV1 === true) return true;
  if (opp.plannerIntelligence?.version) return true;
  const type = text(
    opp.opportunityType || opp.demandSignalType || opp.detailLayout,
  );
  if (
    type === PROFESSIONAL_EVENT_PLANNER_SUBTYPE ||
    /professional.?event.?planner/i.test(type)
  ) {
    return true;
  }
  if (
    /Professional Event Planner Partnership/i.test(
      text(opp.title || opp.opportunityName || opp.classificationLine),
    )
  ) {
    return true;
  }
  return false;
}
