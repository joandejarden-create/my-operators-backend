/**
 * AI Visibility Audit & Action Plan — PDF payload contract (reusable onboarding).
 * Visual family: dealality-report-system-v1 (same as AI Demand Performance Review).
 * Hotel-specific content belongs in payload JSON — not in the HTML template.
 */

export const AI_VISIBILITY_AUDIT_PRODUCT_TITLE = "AI Visibility Audit & Action Plan";

export const AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V1 =
  "AI_VISIBILITY_AUDIT_ACTION_PLAN_PDF_V1";

export const AI_VISIBILITY_AUDIT_REPORT_FAMILY = "dealality-report-system-v1";

export const AI_VISIBILITY_AUDIT_RENDERER = "AdpAiVisibilityAuditPdfV1";

/** Claim / finding classification for customer-safe language */
export const AUDIT_CLAIM_TYPES = Object.freeze({
  VERIFIED_ISSUE: "VERIFIED_ISSUE",
  RECOMMENDED_INVESTIGATION: "RECOMMENDED_INVESTIGATION",
  OPTIONAL_COMMERCIAL_OPPORTUNITY: "OPTIONAL_COMMERCIAL_OPPORTUNITY",
  REQUIRES_HOTEL_CONFIRMATION: "REQUIRES_HOTEL_CONFIRMATION",
});

/** Customer-facing priority labels (ADP-style plain English) */
export const AUDIT_ACTION_PRIORITIES = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  OPTIONAL: "OPTIONAL",
});

export const AUDIT_IMPLEMENTATION_STATUS = Object.freeze({
  NOT_STARTED: "NOT_STARTED",
  AWAITING_HOTEL_DECISION: "AWAITING_HOTEL_DECISION",
  IN_PROGRESS: "IN_PROGRESS",
  COMPLETED: "COMPLETED",
  VERIFIED: "VERIFIED",
});

export const AUDIT_EFFORT_LEVELS = Object.freeze({
  LOW: "LOW",
  MEDIUM: "MEDIUM",
  HIGH: "HIGH",
});

/**
 * Required fields on each action (onboarding + monthly bridge).
 * Business explanation first; technical details nested under technicalDetails.
 */
export const AUDIT_ACTION_REQUIRED_FIELDS = Object.freeze([
  "actionId",
  "headline",
  "priority",
  "priorityWhy",
  "whatWeFound",
  "whyItMatters",
  "recommendedSteps",
  "accountableOwner",
  "supportingParties",
  "suggestedTimeframe",
  "completionCriteria",
  "requiresHotelApproval",
  "hotelApprovalNotes",
  "supportingEvidence",
  "technicalDetails",
  "implementationStatus",
  "claimType",
]);

/** Measurable completion stages for onboarding actions */
export const AUDIT_COMPLETION_CRITERIA_KEYS = Object.freeze([
  "investigationCompleted",
  "correctionSubmitted",
  "correctionVerifiedLive",
  "noChangeRequired",
]);

/**
 * @param {object} audit
 */
export function assertAuditPdfPayload(audit) {
  const errors = [];
  if (!audit || typeof audit !== "object") {
    return { ok: false, errors: ["MISSING_PAYLOAD"] };
  }
  if (!audit.property?.name) errors.push("MISSING_PROPERTY_NAME");
  if (!audit.property?.propertyId) errors.push("MISSING_PROPERTY_ID");
  if (!audit.reporting?.reportMonthLabel) errors.push("MISSING_REPORT_MONTH");
  if (!Array.isArray(audit.actions) || audit.actions.length < 1) {
    errors.push("MISSING_ACTIONS");
  }
  for (const a of audit.actions || []) {
    for (const f of AUDIT_ACTION_REQUIRED_FIELDS) {
      if (a[f] === undefined || a[f] === null) {
        errors.push(`${a.actionId || "?"}: missing ${f}`);
      }
    }
    if (!Array.isArray(a.recommendedSteps) || !a.recommendedSteps.length) {
      errors.push(`${a.actionId || "?"}: recommendedSteps_empty`);
    }
    const cc = a.completionCriteria;
    if (!cc || typeof cc !== "object") {
      errors.push(`${a.actionId || "?"}: completionCriteria_missing`);
    } else {
      for (const k of AUDIT_COMPLETION_CRITERIA_KEYS) {
        if (cc[k] === undefined || cc[k] === null || String(cc[k]).trim() === "") {
          errors.push(`${a.actionId || "?"}: completionCriteria.${k}`);
        }
      }
    }
    const td = a.technicalDetails;
    if (!td || typeof td !== "object") {
      errors.push(`${a.actionId || "?"}: technicalDetails_missing`);
    } else {
      for (const k of ["urls", "changesOrInvestigation", "evidence", "verificationMethod"]) {
        if (td[k] === undefined || td[k] === null) {
          errors.push(`${a.actionId || "?"}: technicalDetails.${k}`);
        }
      }
    }
  }
  const ex = audit.executiveSummary || {};
  if (!ex.headline) errors.push("MISSING_EXEC_HEADLINE");
  if (!ex.lead) errors.push("MISSING_EXEC_LEAD");
  if (!Array.isArray(ex.themes) || ex.themes.length < 1) {
    errors.push("MISSING_EXEC_THEMES");
  }
  if (!Array.isArray(ex.priorities) || ex.priorities.length < 1) {
    errors.push("MISSING_EXEC_PRIORITIES");
  }
  if (!ex.successLooksLike) errors.push("MISSING_EXEC_SUCCESS");
  if (!ex.closing) errors.push("MISSING_EXEC_CLOSING");
  if (!Array.isArray(audit.actionOverview) || audit.actionOverview.length < 1) {
    errors.push("MISSING_ACTION_OVERVIEW");
  }
  return { ok: errors.length === 0, errors, templateId: AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V1 };
}
