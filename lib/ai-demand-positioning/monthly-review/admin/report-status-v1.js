/**
 * ADP_MONTHLY_REVIEW_REPORT_STATUS_V1
 *
 * Monthly Review publication status is separate from ADP measurement certification.
 * The review is derived from certified data; these statuses govern report artifacts only.
 */

export const ADP_MONTHLY_REVIEW_REPORT_STATUS_V1 =
  "ADP_MONTHLY_REVIEW_REPORT_STATUS_V1";

/** Publication / approval lifecycle (not ADP certification). */
export const REVIEW_STATUS = Object.freeze({
  DRAFT: "DRAFT",
  READY_FOR_REVIEW: "READY_FOR_REVIEW",
  APPROVED: "APPROVED",
  CLIENT_ISSUED: "CLIENT_ISSUED",
  SUPERSEDED: "SUPERSEDED",
  ARCHIVED: "ARCHIVED",
});

/** Operational generation outcome (orthogonal to reviewStatus). */
export const GENERATION_STATUS = Object.freeze({
  SUCCESS: "SUCCESS",
  FAILED: "GENERATION_FAILED",
  IN_PROGRESS: "IN_PROGRESS",
  PENDING_PDF: "PENDING_PDF",
});

export const PDF_STATUS = Object.freeze({
  READY: "READY",
  MISSING: "MISSING",
  FAILED: "FAILED",
  PENDING: "PENDING",
});

export const GENERATION_REASON = Object.freeze({
  GOLDEN_REGISTER: "GOLDEN_REGISTER",
  REGENERATE_SAME_PERIODS: "REGENERATE_SAME_PERIODS",
  GENERATE_NEW_DRAFT: "GENERATE_NEW_DRAFT",
  MIGRATION: "MIGRATION",
});

export const REVIEW_BUILDER_VERSION = "ADP_MONTHLY_REVIEW_BUILDER_V1";
export const ACTION_LIBRARY_VERSION = "ADP_ACTION_PATTERN_LIBRARY_V1";
export const REVIEW_SCHEMA_VERSION = "ADP_MONTHLY_EXECUTIVE_REVIEW_V1";

export const ARCHIVE_INDEX_SCHEMA = "ADP_MONTHLY_REVIEW_ARCHIVE_INDEX_V1";
export const ACTION_FOUNDER_FEEDBACK_SCHEMA =
  "ADP_ACTION_PATTERN_FOUNDER_FEEDBACK_V1";
export const REPORT_SECTION_FEEDBACK_SCHEMA =
  "ADP_MONTHLY_REVIEW_SECTION_FEEDBACK_V1";

export const REPORT_FEEDBACK_SECTIONS = Object.freeze([
  "Executive Assessment",
  "Material Movements",
  "Evidence",
  "Actions",
  "PDF Layout",
  "Meeting Mode",
]);

export const ACTION_FEEDBACK_RATINGS = Object.freeze({
  KEEP: "KEEP",
  NEEDS_IMPROVEMENT: "NEEDS_IMPROVEMENT",
  REJECT: "REJECT",
});

export const REPORT_FEEDBACK_RATINGS = Object.freeze({
  PASS: "PASS",
  NEEDS_WORK: "NEEDS_WORK",
});

export function isTerminalImmutableReviewStatus(status) {
  return status === REVIEW_STATUS.CLIENT_ISSUED;
}

export function canMutateReviewArtifacts(meta) {
  if (!meta) return false;
  if (isTerminalImmutableReviewStatus(meta.reviewStatus)) return false;
  if (meta.clientIssuedAt) return false;
  return true;
}

export function labelReviewStatus(status) {
  const map = {
    DRAFT: "Draft",
    READY_FOR_REVIEW: "Ready for Review",
    APPROVED: "Approved",
    CLIENT_ISSUED: "Client Issued",
    SUPERSEDED: "Superseded",
    ARCHIVED: "Archived",
  };
  return map[status] || status;
}

export function labelGenerationStatus(status) {
  const map = {
    SUCCESS: "Success",
    GENERATION_FAILED: "Generation Failed",
    IN_PROGRESS: "In Progress",
    PENDING_PDF: "Pending Pdf",
  };
  return map[status] || status;
}
