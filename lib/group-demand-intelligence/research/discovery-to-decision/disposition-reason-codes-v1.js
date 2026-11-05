/**
 * GDI V37 — standardized disposition reason codes (machine + human).
 * Does not change commercial qualification standards.
 */

export const DISPOSITION_REASON_CODE = Object.freeze({
  DUPLICATE_EXISTING_OPPORTUNITY: "DUPLICATE_EXISTING_OPPORTUNITY",
  DUPLICATE_SAME_EVENT_FAMILY: "DUPLICATE_SAME_EVENT_FAMILY",
  DUPLICATE_SAME_ORG_CROSS_PASS: "DUPLICATE_SAME_ORG_CROSS_PASS",
  HISTORICAL_EVENT: "HISTORICAL_EVENT",
  NO_FUTURE_EDITION_VERIFIED: "NO_FUTURE_EDITION_VERIFIED",
  OUTSIDE_HOTEL_CATCHMENT: "OUTSIDE_HOTEL_CATCHMENT",
  HOTEL_PRODUCT_MISMATCH: "HOTEL_PRODUCT_MISMATCH",
  NO_REALISTIC_LODGING_DEMAND: "NO_REALISTIC_LODGING_DEMAND",
  NO_REALISTIC_CATERING_DEMAND: "NO_REALISTIC_CATERING_DEMAND",
  CONTACT_NOT_VERIFIED: "CONTACT_NOT_VERIFIED",
  CONTACT_COMMERCIALLY_IRRELEVANT: "CONTACT_COMMERCIALLY_IRRELEVANT",
  DEMAND_MECHANISM_UNSUPPORTED: "DEMAND_MECHANISM_UNSUPPORTED",
  ALREADY_CONTRACTED_OR_HOSTED: "ALREADY_CONTRACTED_OR_HOSTED",
  COMPETITOR_DEMAND_NOT_TRANSFERABLE: "COMPETITOR_DEMAND_NOT_TRANSFERABLE",
  CONSTRUCTION_WORKFORCE_MISMATCH: "CONSTRUCTION_WORKFORCE_MISMATCH",
  RESEARCH_BUDGET_LIMIT: "RESEARCH_BUDGET_LIMIT",
  RESEARCH_ALLOCATION_CAP: "RESEARCH_ALLOCATION_CAP",
  DEEP_RESEARCH_NOT_STARTED: "DEEP_RESEARCH_NOT_STARTED",
  RESEARCH_INCOMPLETE: "RESEARCH_INCOMPLETE",
  PROVIDER_TIMEOUT: "PROVIDER_TIMEOUT",
  PARSING_FAILURE: "PARSING_FAILURE",
  IDENTITY_MISSING_ORGANIZATION: "IDENTITY_MISSING_ORGANIZATION",
  SCREENED_OUT_COMMERCIAL: "SCREENED_OUT_COMMERCIAL",
  HARD_BLOCK_ACCEPTANCE: "HARD_BLOCK_ACCEPTANCE",
  RESEARCH_CONTINUATION_GAPS: "RESEARCH_CONTINUATION_GAPS",
  WATCH_INSUFFICIENT_EVIDENCE: "WATCH_INSUFFICIENT_EVIDENCE",
  REJECTED_COMMERCIAL: "REJECTED_COMMERCIAL",
  AWAITING_FOUNDER_PROMOTION: "AWAITING_FOUNDER_PROMOTION",
  CUSTOMER_FACING_APPROVED: "CUSTOMER_FACING_APPROVED",
  PROVENANCE_UNAVAILABLE: "PROVENANCE_UNAVAILABLE",
  STAGED_INTERNAL_ONLY: "STAGED_INTERNAL_ONLY",
});

const META = Object.freeze({
  [DISPOSITION_REASON_CODE.DUPLICATE_EXISTING_OPPORTUNITY]: {
    label: "Duplicate of an existing opportunity",
    explanation:
      "This discovery resolves to an opportunity already present in inventory under another ID.",
  },
  [DISPOSITION_REASON_CODE.DUPLICATE_SAME_EVENT_FAMILY]: {
    label: "Duplicate within the same event/account family",
    explanation:
      "Another candidate covers the same organization/programme family; this row was collapsed.",
  },
  [DISPOSITION_REASON_CODE.DUPLICATE_SAME_ORG_CROSS_PASS]: {
    label: "Duplicate organization across discovery passes",
    explanation:
      "The same organization name was already retained from an earlier discovery pass.",
  },
  [DISPOSITION_REASON_CODE.HISTORICAL_EVENT]: {
    label: "Historical event without transferable future demand",
    explanation:
      "Evidence describes a past edition without a verified future edition or open sourcing window.",
  },
  [DISPOSITION_REASON_CODE.NO_FUTURE_EDITION_VERIFIED]: {
    label: "No future edition verified",
    explanation:
      "Public sources do not confirm a next edition, dates, or procurement opening.",
  },
  [DISPOSITION_REASON_CODE.OUTSIDE_HOTEL_CATCHMENT]: {
    label: "Outside hotel catchment",
    explanation:
      "Geography or venue placement is outside realistic lodging catchment for this hotel.",
  },
  [DISPOSITION_REASON_CODE.HOTEL_PRODUCT_MISMATCH]: {
    label: "Hotel product mismatch",
    explanation:
      "Demand type does not match this hotel’s product (rooms, meeting, catering, or positioning).",
  },
  [DISPOSITION_REASON_CODE.NO_REALISTIC_LODGING_DEMAND]: {
    label: "No realistic lodging demand",
    explanation:
      "No credible room-night, block, or overnight mechanism was established.",
  },
  [DISPOSITION_REASON_CODE.NO_REALISTIC_CATERING_DEMAND]: {
    label: "No realistic catering demand",
    explanation:
      "No credible banquet/F&B procurement mechanism was established for this hotel.",
  },
  [DISPOSITION_REASON_CODE.CONTACT_NOT_VERIFIED]: {
    label: "Named contact not verified",
    explanation:
      "A commercially relevant named person was not verified from durable public evidence.",
  },
  [DISPOSITION_REASON_CODE.CONTACT_COMMERCIALLY_IRRELEVANT]: {
    label: "Contact commercially irrelevant",
    explanation:
      "A named person exists but is not a buyer, planner, or contracting route for this hotel.",
  },
  [DISPOSITION_REASON_CODE.DEMAND_MECHANISM_UNSUPPORTED]: {
    label: "Demand mechanism unsupported",
    explanation:
      "The activity does not establish a hotel-sellable demand mechanism under GDI gates.",
  },
  [DISPOSITION_REASON_CODE.ALREADY_CONTRACTED_OR_HOSTED]: {
    label: "Already contracted or hosted elsewhere",
    explanation:
      "Evidence shows an incumbent venue/host with no transferable overflow or switch signal.",
  },
  [DISPOSITION_REASON_CODE.COMPETITOR_DEMAND_NOT_TRANSFERABLE]: {
    label: "Competitor demand not transferable",
    explanation:
      "Demand is locked to a competitor venue without a realistic transfer path.",
  },
  [DISPOSITION_REASON_CODE.CONSTRUCTION_WORKFORCE_MISMATCH]: {
    label: "Construction / workforce mismatch",
    explanation:
      "Workforce or construction lodging thesis does not fit this hotel’s product.",
  },
  [DISPOSITION_REASON_CODE.RESEARCH_BUDGET_LIMIT]: {
    label: "Stopped by research budget limit",
    explanation:
      "Candidate remained undeepened or incomplete because the authorized spend ceiling was reached.",
  },
  [DISPOSITION_REASON_CODE.RESEARCH_ALLOCATION_CAP]: {
    label: "Deferred by deep-research allocation cap",
    explanation:
      "Eligible for deep research but not selected under workflow/global adaptive caps.",
  },
  [DISPOSITION_REASON_CODE.DEEP_RESEARCH_NOT_STARTED]: {
    label: "Deep research not started",
    explanation:
      "Discovered and retained, but no deep-research run was executed for this candidate.",
  },
  [DISPOSITION_REASON_CODE.RESEARCH_INCOMPLETE]: {
    label: "Research incomplete",
    explanation:
      "Deep research started or was attempted but commercial evidence remains incomplete.",
  },
  [DISPOSITION_REASON_CODE.PROVIDER_TIMEOUT]: {
    label: "Provider timeout / failure",
    explanation:
      "Exa (or related provider) timed out or failed before a durable candidate outcome.",
  },
  [DISPOSITION_REASON_CODE.PARSING_FAILURE]: {
    label: "Parsing failure",
    explanation:
      "Provider returned content that could not be parsed into a structured candidate.",
  },
  [DISPOSITION_REASON_CODE.IDENTITY_MISSING_ORGANIZATION]: {
    label: "Missing organization identity",
    explanation:
      "Structured hit lacked a resolvable organization name and was dropped at extraction.",
  },
  [DISPOSITION_REASON_CODE.SCREENED_OUT_COMMERCIAL]: {
    label: "Screened out commercially",
    explanation:
      "Failed commercial screening before deep research (identity, segment, or demand fit).",
  },
  [DISPOSITION_REASON_CODE.HARD_BLOCK_ACCEPTANCE]: {
    label: "Hard-blocked at acceptance",
    explanation:
      "Fourteen-workflow acceptance hard block prevented staging as Sales Ready.",
  },
  [DISPOSITION_REASON_CODE.RESEARCH_CONTINUATION_GAPS]: {
    label: "Research Continuation — evidence gaps",
    explanation:
      "Deep-researched but independently classified Research Continuation, not Sales Ready.",
  },
  [DISPOSITION_REASON_CODE.WATCH_INSUFFICIENT_EVIDENCE]: {
    label: "Watch — insufficient evidence",
    explanation:
      "Deep-researched but independently classified Watch; not customer-ready.",
  },
  [DISPOSITION_REASON_CODE.REJECTED_COMMERCIAL]: {
    label: "Commercially rejected after research",
    explanation:
      "After research (or hard screening), demand was rejected — not merely undeepened.",
  },
  [DISPOSITION_REASON_CODE.AWAITING_FOUNDER_PROMOTION]: {
    label: "Awaiting founder promotion",
    explanation:
      "Independently accepted Sales Ready and staged internally; not yet founder-promoted to customer-facing.",
  },
  [DISPOSITION_REASON_CODE.CUSTOMER_FACING_APPROVED]: {
    label: "Customer-facing approved",
    explanation:
      "Founder-approved selective promotion; visible on the customer-facing GDI dashboard.",
  },
  [DISPOSITION_REASON_CODE.PROVENANCE_UNAVAILABLE]: {
    label: "Historical provenance unavailable",
    explanation:
      "Existing artifacts do not retain the original provider envelope needed for full reconciliation.",
  },
  [DISPOSITION_REASON_CODE.STAGED_INTERNAL_ONLY]: {
    label: "Staged internal only",
    explanation:
      "Opportunity exists in internal staging inventory but is not customer-facing.",
  },
});

export function dispositionReasonMeta(code) {
  const c = String(code || "").trim();
  return (
    META[c] || {
      label: c || "Unknown disposition",
      explanation: "No standardized explanation is registered for this code.",
    }
  );
}

/**
 * Build a full disposition object (never "Rejected" alone).
 */
export function buildDisposition({
  code,
  explanation = null,
  evidence = [],
  decidedAt = null,
  stage = null,
  nextAction = null,
  supportingOpportunityId = null,
} = {}) {
  const meta = dispositionReasonMeta(code);
  return {
    code: code || null,
    label: meta.label,
    explanation: explanation || meta.explanation,
    evidence: Array.isArray(evidence) ? evidence.filter(Boolean) : [],
    decidedAt: decidedAt || new Date().toISOString(),
    stage: stage || null,
    nextAction: nextAction || null,
    supportingOpportunityId: supportingOpportunityId || null,
  };
}
