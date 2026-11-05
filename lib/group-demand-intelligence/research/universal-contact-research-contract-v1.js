/**
 * Universal GDI contact research contract V1.
 *
 * One shared standard for all fourteen PowerShell FullE2E workflows.
 * Segment-specific buyer-role guidance is additive — never a parallel engine.
 *
 * CONTACT_FOUND_IN_RESEARCH ≠ CONTACT_COMMERCIALLY_RELEVANT
 * ≠ CONTRACTING_AUTHORITY_VERIFIED.
 *
 * Zero-paid contract module — does not call providers.
 */

export const UNIVERSAL_CONTACT_RESEARCH_CONTRACT_V1 =
  "gdi-universal-contact-research-contract-v1";

export const CONTACT_EVIDENCE_CLASS = Object.freeze({
  CONTACT_FOUND_IN_RESEARCH: "CONTACT_FOUND_IN_RESEARCH",
  CONTACT_COMMERCIALLY_RELEVANT: "CONTACT_COMMERCIALLY_RELEVANT",
  CONTRACTING_AUTHORITY_VERIFIED: "CONTRACTING_AUTHORITY_VERIFIED",
  ORGANIZATION_ROUTE_ONLY: "ORGANIZATION_ROUTE_ONLY",
  NO_NAMED_CONTACT: "NO_NAMED_CONTACT",
});

export const CONTACT_COMMERCIAL_DISPOSITION = Object.freeze({
  SALES_READY: "SALES_READY",
  RESEARCH_CONTINUATION_REQUIRED: "RESEARCH_CONTINUATION_REQUIRED",
  WATCH: "WATCH",
  NOT_CUSTOMER_READY: "NOT_CUSTOMER_READY",
});

/** Segment-specific research targets (roles to investigate — not authority claims). */
export const SEGMENT_BUYER_ROLE_TARGETS = Object.freeze({
  associations_conferences_events: [
    "Event director",
    "Meetings director",
    "Conference manager",
    "Convention services / housing coordinator",
    "Association operations leader",
  ],
  corporate_accounts_scaling: [
    "Travel manager",
    "Workplace manager",
    "Meetings and events manager",
    "Procurement / sourcing lead",
    "People / HR mobility coordinator",
  ],
  corporate_meetings_incentives: [
    "Incentive programme owner",
    "Corporate events manager",
    "Sales operations / event coordinator",
    "Meeting planner",
    "Travel programme owner",
  ],
  government_public_sector: [
    "Programme coordinator",
    "Procurement officer",
    "Agency travel / meetings coordinator",
    "Administrative or events manager",
  ],
  sports_entertainment: [
    "Tournament director",
    "Team travel coordinator",
    "Event operations manager",
    "Housing coordinator",
  ],
  education_scientific: [
    "Programme administrator",
    "Conference coordinator",
    "Academic events manager",
    "Continuing education director",
  ],
  projects_infrastructure: [
    "Project administrator",
    "Mobilization coordinator",
    "Travel / accommodation manager",
    "Procurement lead",
  ],
  social_special_events: [
    "Wedding coordinator",
    "Event director",
    "Venue manager",
    "Group accommodation coordinator",
  ],
  local_catering_day_meetings: [
    "Event coordinator",
    "Office manager",
    "Facilities manager",
    "Executive assistant with event responsibilities",
    "Catering purchaser",
  ],
  corporate_celebrations_private_events: [
    "Corporate events manager",
    "HR / People events coordinator",
    "Internal communications / events lead",
  ],
  travel_trade_group_intermediaries: [
    "Group contracting manager",
    "DMC owner",
    "Product manager",
    "Group operations director",
  ],
  relocation_extended_stay: [
    "Global mobility manager",
    "Relocation coordinator",
    "HR operations lead",
    "Travel / accommodation manager",
  ],
  preferred_lodging_partnerships: [
    "Venue owner",
    "Venue general manager",
    "Wedding director",
    "Event sales director",
  ],
  professional_event_planners: [
    "Owner / founder",
    "Lead planner",
    "Director of events",
    "Group accommodation coordinator",
  ],
});

export const UNIVERSAL_CONTACT_RESEARCH_RULES = Object.freeze([
  "Minimum: one verified named commercially relevant person (buyer, operational coordinator, influencer, or credible referral).",
  "Preferred: two distinct named contacts — primary commercial route + alternative / referral.",
  "Ideal: verify hotel-sourcing or contracting decision-maker when evidence supports it.",
  "Investigate hotel sourcing / purchasing responsibility explicitly; label unknowns.",
  "Distinguish buyer vs influencer vs referral; never treat speakers, presenters, or unrelated executives as buyers solely because they have a name.",
  "Preserve LinkedIn / profile URLs and public email/phone only when verified — never invent channels.",
  "Retain source URLs supporting contact relevance (claim → source).",
  "ORS must reject unrelated contacts; CIB must preserve all valid CONTACT_RELEVANT findings.",
  "A corporate inbox or contact form is an organizational route — not a named buyer.",
  "Do not fabricate a second contact. Record unresolved contact-research questions honestly.",
]);

/**
 * Prompt / ORS block injected into every workflow deep-research context.
 */
export function buildUniversalContactResearchPromptBlock({
  segmentId = null,
  segmentLabel = null,
} = {}) {
  const roles =
    (segmentId && SEGMENT_BUYER_ROLE_TARGETS[segmentId]) ||
    SEGMENT_BUYER_ROLE_TARGETS.corporate_accounts_scaling;
  const label = segmentLabel || segmentId || "this demand segment";
  return [
    `## Universal contact research standard (${UNIVERSAL_CONTACT_RESEARCH_CONTRACT_V1})`,
    ...UNIVERSAL_CONTACT_RESEARCH_RULES.map((r, i) => `${i + 1}. ${r}`),
    `Segment (${label}) — investigate these role targets when relevant (not authority claims):`,
    ...roles.map((r) => `- ${r}`),
    "Output named contacts as buyerCandidates / namedContacts with: fullName|name, title, organization, whyRelevant, sourceUrl, linkedinUrl (nullable), relevanceClass, contractingAuthorityVerified (boolean).",
    "Classify each person: CONTACT_FOUND_IN_RESEARCH → then CONTACT_COMMERCIALLY_RELEVANT only with evidence → CONTRACTING_AUTHORITY_VERIFIED only with explicit sourcing authority evidence.",
  ].join("\n");
}

export function listFourteenWorkflowSegmentIds() {
  return Object.keys(SEGMENT_BUYER_ROLE_TARGETS);
}

export function assertWorkflowContactContractCoverage(segmentIds = []) {
  const required = listFourteenWorkflowSegmentIds();
  const have = new Set((segmentIds || []).map(String));
  const missing = required.filter((id) => !have.has(id));
  return {
    version: UNIVERSAL_CONTACT_RESEARCH_CONTRACT_V1,
    requiredCount: required.length,
    coveredCount: required.length - missing.length,
    missing,
    ok: missing.length === 0,
  };
}
