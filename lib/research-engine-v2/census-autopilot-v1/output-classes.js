/**
 * Map completeness + field outcomes → Autopilot output class.
 */

import { OUTPUT_CLASS, FIELD_RESOLUTION_STATUS } from "./constants.js";

/**
 * Primary census data output class.
 * Image rights are tracked separately — do not collapse all hotels into
 * SOURCE RIGHTS REVIEW just because imagery rights are Unknown.
 *
 * @param {object} record
 * @param {object} fieldResult
 * @param {object} completeness
 * @param {object} [extras]
 */
export function classifyOutput(record, fieldResult, completeness, extras = {}) {
  if (extras.reference_challenge_only) {
    return OUTPUT_CLASS.REFERENCE_CHALLENGE;
  }
  if (
    record.reconstruction_status === "Hold — Evidence Conflict" ||
    (fieldResult.fields || []).some((f) => f.resolution_status === FIELD_RESOLUTION_STATUS.CONFLICTING_EVIDENCE)
  ) {
    return OUTPUT_CLASS.HOLD_CONFLICTING;
  }
  // Factual data rights block (not image rights) — e.g. source-rights registry Unknown for production display of scraped text
  if (extras.factual_source_rights_blocked) {
    return OUTPUT_CLASS.SOURCE_RIGHTS_REVIEW_REQUIRED;
  }
  if (extras.first_party_required) {
    return OUTPUT_CLASS.FIRST_PARTY_VALIDATION_REQUIRED;
  }

  const deep = (fieldResult.escalations || []).length;
  const material = completeness.material_completeness;
  const core = completeness.core_completeness;
  const hard = (completeness.hard_gate_failures || []).length;

  if (hard && material < 40) return OUTPUT_CLASS.DEEP_RESEARCH_REQUIRED;
  if (deep >= 3 && material < 55) return OUTPUT_CLASS.DEEP_RESEARCH_REQUIRED;

  if (core >= 90 && material >= 70 && !hard) {
    return OUTPUT_CLASS.VERIFIED_PRODUCTION_CANDIDATE;
  }
  if (core >= 90 && material >= 50) {
    return OUTPUT_CLASS.VERIFIED_MATERIAL_REMEDIATION;
  }
  if (core >= 80) {
    return OUTPUT_CLASS.PARTIAL_NONCRITICAL;
  }
  return OUTPUT_CLASS.DEEP_RESEARCH_REQUIRED;
}

/**
 * @param {object} imageIntegrity
 * @returns {boolean}
 */
export function imageRightsReviewRequired(imageIntegrity) {
  const status = String(imageIntegrity?.rights_status || "");
  return /review required|unknown/i.test(status);
}
