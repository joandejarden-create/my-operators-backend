/**
 * Research Engine V2 — contradiction-first claim model.
 * Proposed corrections only; never writes Airtable.
 */

export const CLAIM_TYPES = Object.freeze([
  "HOTEL_EXISTS",
  "CURRENT_BRAND",
  "CURRENT_PARENT",
  "OPERATING_STATUS",
  "PIPELINE_STATUS",
  "OPENING_STATUS",
  "REFLAG_STATUS",
  "CURRENT_OPERATOR",
]);

export const CLAIM_STATUSES = Object.freeze([
  "Confirmed",
  "Contradicted",
  "Superseded",
  "Unverified",
  "Conflicting Evidence",
  "Unknown",
]);

export const RECOMMENDED_ACTIONS = Object.freeze([
  "No Change",
  "Review",
  "Proposed Update",
  "Proposed Reflag",
  "Proposed Status Change",
  "Proposed Parent Correction",
  "Proposed Operator Correction",
  "Insufficient Evidence",
]);

/**
 * @param {object} partial
 */
export function createClaim(partial = {}) {
  const now = new Date().toISOString();
  return {
    claimType: partial.claimType || "Unknown",
    hotelId: partial.hotelId || "",
    hotelName: partial.hotelName || "",
    currentDealalityValue: partial.currentDealalityValue ?? null,
    independentlyObservedValue: partial.independentlyObservedValue ?? null,
    claimStatus: partial.claimStatus || "Unknown",
    evidenceSource: partial.evidenceSource || null,
    sourceType: partial.sourceType || null,
    sourceDate: partial.sourceDate || null,
    eventDate: partial.eventDate || null,
    evidenceRetrievalDate: partial.evidenceRetrievalDate || now,
    dealalityLastVerified: partial.dealalityLastVerified || null,
    confidence: partial.confidence ?? null,
    contradictionFound: Boolean(partial.contradictionFound),
    proposedCorrection: partial.proposedCorrection ?? null,
    notes: partial.notes || "",
    supportQueries: partial.supportQueries || [],
    contradictionQueries: partial.contradictionQueries || [],
  };
}

/**
 * @param {object} partial
 */
export function createProposedCorrection(partial = {}) {
  return {
    hotel_id: partial.hotel_id || "",
    hotel_name: partial.hotel_name || "",
    field: partial.field || "",
    current_value: partial.current_value ?? null,
    observed_value: partial.observed_value ?? null,
    classification: partial.classification || "Unknown",
    evidence: partial.evidence || [],
    confidence: partial.confidence ?? null,
    reason: partial.reason || "",
    recommended_action: partial.recommended_action || "Insufficient Evidence",
  };
}

/**
 * Normalize operating/pipeline status labels.
 * @param {unknown} raw
 */
export function normalizeOperatingStatus(raw) {
  const val = Array.isArray(raw) ? raw[0] : raw;
  const s = String(val || "").trim();
  if (!s) return "";
  if (/^open$/i.test(s) || /^operating$/i.test(s) || /^bookable$/i.test(s)) return "Open";
  if (/^pipeline$/i.test(s) || /^coming soon$/i.test(s) || /^under development$/i.test(s)) {
    return "Pipeline";
  }
  if (/^closed$/i.test(s) || /^permanently closed$/i.test(s)) return "Closed";
  return s;
}
