/**
 * Business Impact — definition, qualified actions, metrics, attribution (Phase 3C.1).
 * No inferred actions from AI Presence or citations.
 */

export const BUSINESS_IMPACT_VERSION = "ai_visibility_business_impact_v1";

export const BUSINESS_IMPACT_DEFINITION = Object.freeze({
  question:
    "Are AI-originated visits producing owner/developer actions relevant to hotel development?",
  NOT_ECOMMERCE: true,
  INFERRED_FROM_AI_PRESENCE: false,
  INFERRED_FROM_CITATIONS: false,
});

export const QUALIFIED_ACTION_TAXONOMY = Object.freeze([
  { id: "development_inquiry_submitted", label: "Development inquiry submitted", qualified: true },
  { id: "franchise_inquiry", label: "Franchise inquiry", qualified: true },
  { id: "owner_contact", label: "Owner contact", qualified: true },
  { id: "download_development_materials", label: "Download development materials", qualified: true },
  { id: "request_meeting", label: "Request meeting", qualified: true },
  { id: "qualified_lead", label: "Qualified lead", qualified: true },
  { id: "project_submission", label: "Project submission", qualified: true },
]);

export const BUSINESS_IMPACT_METRICS_V1 = Object.freeze([
  { id: "qualified_development_actions_from_ai", label: "Qualified Development Actions From AI" },
  { id: "development_inquiry_conversion_rate_from_ai", label: "Development Inquiry Conversion Rate From AI" },
  { id: "brand_development_page_engagement_from_ai", label: "Brand Development Page Engagement From AI" },
]);

export const ATTRIBUTION_STATE = Object.freeze({
  DIRECT_AI_REFERRAL_ACTION: "DIRECT_AI_REFERRAL_ACTION",
  AI_IN_PATH: "AI_IN_PATH",
  ATTRIBUTION_UNKNOWN: "ATTRIBUTION_UNKNOWN",
});

export const ATTRIBUTION_METHODOLOGY = Object.freeze({
  RULE: "Do not claim AI caused a lead because an AI referral occurred earlier unless session path data supports it",
  DEFAULT_V1: ATTRIBUTION_STATE.DIRECT_AI_REFERRAL_ACTION,
  AI_ASSISTED_LABEL: "Use only when attribution model explicitly supports assisted path",
});

/**
 * Business impact event governance contract.
 */
export function buildBusinessImpactEventContract(event = {}) {
  return {
    eventId: event.eventId || null,
    eventName: event.eventName || null,
    definition: event.definition || null,
    version: event.version || "1",
    sourceSystem: event.sourceSystem || null,
    qualified: event.qualified === true,
    ownerDevelopmentRelevance: event.ownerDevelopmentRelevance !== false,
    effectiveFrom: event.effectiveFrom || null,
  };
}

/**
 * Validate event is qualified before counting as business impact.
 */
export function isQualifiedBusinessImpactEvent(event) {
  const c = buildBusinessImpactEventContract(event);
  return c.qualified && c.ownerDevelopmentRelevance && Boolean(c.eventId);
}

export const CRM_DEPENDENCY = Object.freeze({
  STATUS: "FUTURE",
  FIELDS: ["qualified_development_lead", "owner_company", "project", "brand", "source", "timestamp", "status"],
});

export const READERSHIP_ENRICHMENT = Object.freeze({
  STATUS: "DEFERRED",
  REASON: "Not required for Discoverability / Referral / Business Impact v1",
});
