/**
 * Fail-closed gate: commercial decision windows may only use validated hotel-contract dates.
 * Opening dates, loan maturities, unresolved schedule columns, and unattributed portfolio
 * language must never drive franchise/renewal opportunity calculations.
 */

import {
  AUDITED_DATE_MEANING,
  DECISION_WINDOW_ELIGIBILITY,
  auditContractDateObservation,
  EVIDENCE_CONFIDENCE,
} from "./contract-date-meaning-audit-v1.js";

export const CONTRACT_DATE_SEMANTIC_GATE_V1 = "contract-date-semantic-gate-v1";

const ALLOWED_FOR_WINDOWS = new Set([
  AUDITED_DATE_MEANING.FRANCHISE_AGREEMENT_EXPIRATION,
  AUDITED_DATE_MEANING.MANAGEMENT_AGREEMENT_EXPIRATION,
  AUDITED_DATE_MEANING.LEASE_OR_LICENSE_EXPIRATION,
  AUDITED_DATE_MEANING.RENEWAL_OR_EXTENSION_EVENT,
]);

/**
 * @param {object} opts
 * @param {object} [opts.audit] — result of auditContractDateObservation
 * @param {object} [opts.observation] — raw observation if audit not precomputed
 * @param {boolean} [opts.hotelAttributed=true] — date must apply to this hotel
 * @param {boolean} [opts.portfolioLevelOnly=false] — portfolio language without hotel attribution
 */
export function assertDateEligibleForDecisionWindow(opts = {}) {
  const audit =
    opts.audit ||
    (opts.observation ? auditContractDateObservation(opts.observation) : null);
  if (!audit) {
    return {
      ok: false,
      code: "DATE_AUDIT_REQUIRED",
      decisionWindowEligibility: DECISION_WINDOW_ELIGIBILITY.QUARANTINED,
      reason: "No date-meaning audit provided — fail closed",
    };
  }

  if (opts.portfolioLevelOnly || audit.portfolioLevelOnly) {
    return {
      ok: false,
      code: "PORTFOLIO_LANGUAGE_NOT_HOTEL_ATTRIBUTED",
      decisionWindowEligibility: DECISION_WINDOW_ELIGIBILITY.INELIGIBLE,
      reason:
        "Portfolio-level renewal/term language must not be attributed to every hotel automatically",
      audit,
    };
  }

  if (opts.hotelAttributed === false) {
    return {
      ok: false,
      code: "HOTEL_ATTRIBUTION_REQUIRED",
      decisionWindowEligibility: DECISION_WINDOW_ELIGIBILITY.QUARANTINED,
      reason: "Source evidence must show the date applies to this particular hotel/agreement",
      audit,
    };
  }

  if (
    audit.auditedMeaning === AUDITED_DATE_MEANING.HOTEL_OPENING_OR_YEAR_BUILT ||
    audit.auditedMeaning === AUDITED_DATE_MEANING.LOAN_MATURITY
  ) {
    return {
      ok: false,
      code: "NON_CONTRACT_DATE_MEANING",
      decisionWindowEligibility: DECISION_WINDOW_ELIGIBILITY.INELIGIBLE,
      reason: `Cannot calculate commercial contract windows from ${audit.auditedMeaning}`,
      audit,
    };
  }

  if (audit.auditedMeaning === AUDITED_DATE_MEANING.UNRESOLVED_DATE_MEANING) {
    return {
      ok: false,
      code: "UNRESOLVED_DATE_MEANING",
      decisionWindowEligibility: DECISION_WINDOW_ELIGIBILITY.QUARANTINED,
      reason: "Uncertain date meaning — fail closed",
      audit,
    };
  }

  if (!ALLOWED_FOR_WINDOWS.has(audit.auditedMeaning)) {
    return {
      ok: false,
      code: "DATE_MEANING_NOT_ELIGIBLE",
      decisionWindowEligibility: DECISION_WINDOW_ELIGIBILITY.INELIGIBLE,
      reason: `Audited meaning ${audit.auditedMeaning} is not eligible for decision windows`,
      audit,
    };
  }

  if (
    audit.confidence !== EVIDENCE_CONFIDENCE.HIGH &&
    audit.confidence !== EVIDENCE_CONFIDENCE.MEDIUM
  ) {
    return {
      ok: false,
      code: "INSUFFICIENT_DATE_CONFIDENCE",
      decisionWindowEligibility: DECISION_WINDOW_ELIGIBILITY.QUARANTINED,
      reason: "Date meaning confidence too low — fail closed",
      audit,
    };
  }

  if (audit.decisionWindowEligibility !== DECISION_WINDOW_ELIGIBILITY.ELIGIBLE) {
    return {
      ok: false,
      code: "AUDIT_MARKED_INELIGIBLE",
      decisionWindowEligibility: audit.decisionWindowEligibility,
      reason: "Audit did not mark date eligible for decision windows",
      audit,
    };
  }

  return {
    ok: true,
    code: "DATE_SEMANTIC_OK",
    decisionWindowEligibility: DECISION_WINDOW_ELIGIBILITY.ELIGIBLE,
    audit,
  };
}

export default { assertDateEligibleForDecisionWindow };
