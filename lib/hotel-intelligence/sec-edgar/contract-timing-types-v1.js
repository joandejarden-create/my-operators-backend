/**
 * Hotel Contract Timing Intelligence — date-meaning vocabulary.
 * Never conflate mortgage/loan maturity with franchise or management expiration.
 */

export const CONTRACT_TIMING_MODULE_ID = "HOTEL_CONTRACT_TIMING_INTELLIGENCE_V1";
export const CONTRACT_TIMING_MODULE_VERSION = "1.0.0";

/** What an extracted date actually means. */
export const DATE_MEANING = Object.freeze({
  FRANCHISE_EXPIRATION: "FRANCHISE_EXPIRATION",
  MANAGEMENT_EXPIRATION: "MANAGEMENT_EXPIRATION",
  LICENSE_EXPIRATION: "LICENSE_EXPIRATION",
  /**
   * CMBS combined column: "Expiration/Termination of Related License/ Franchise
   * Agreement, Operating Agreement or Management Agreement".
   * Hotel-level contract date is verified; specific agreement type is unresolved.
   */
  COMBINED_LICENSE_FRANCHISE_OPERATING_OR_MANAGEMENT:
    "COMBINED_LICENSE_FRANCHISE_OPERATING_OR_MANAGEMENT",
  TERMINATION_RIGHT_DATE: "TERMINATION_RIGHT_DATE",
  RENEWAL_OPTION_EXPIRATION: "RENEWAL_OPTION_EXPIRATION",
  EXTENSION_OPTION_DEADLINE: "EXTENSION_OPTION_DEADLINE",
  AGREEMENT_EFFECTIVE_DATE: "AGREEMENT_EFFECTIVE_DATE",
  AGREEMENT_SIGNED_DATE: "AGREEMENT_SIGNED_DATE",
  LOAN_MATURITY_DATE: "LOAN_MATURITY_DATE",
  /** Present in source but meaning ambiguous — never promote as hotel contract timing. */
  AMBIGUOUS_DATE: "AMBIGUOUS_DATE",
});

/** Source families for coverage accounting. */
export const SOURCE_FAMILY = Object.freeze({
  CMBS_424B: "CMBS_424B",
  HOTEL_REIT_SCHEDULE: "HOTEL_REIT_SCHEDULE",
  MANAGEMENT_EXHIBIT_EX10: "MANAGEMENT_EXHIBIT_EX10",
  PUBLIC_FINANCIALS_NON_SEC: "PUBLIC_FINANCIALS_NON_SEC",
  FDD: "FDD",
  OTHER_SEC: "OTHER_SEC",
});

export const HOTEL_CONTRACT_DATE_MEANINGS = Object.freeze([
  DATE_MEANING.FRANCHISE_EXPIRATION,
  DATE_MEANING.MANAGEMENT_EXPIRATION,
  DATE_MEANING.LICENSE_EXPIRATION,
  DATE_MEANING.COMBINED_LICENSE_FRANCHISE_OPERATING_OR_MANAGEMENT,
  DATE_MEANING.TERMINATION_RIGHT_DATE,
  DATE_MEANING.RENEWAL_OPTION_EXPIRATION,
  DATE_MEANING.EXTENSION_OPTION_DEADLINE,
  DATE_MEANING.AGREEMENT_EFFECTIVE_DATE,
  DATE_MEANING.AGREEMENT_SIGNED_DATE,
]);

export function isHotelContractDateMeaning(meaning) {
  return HOTEL_CONTRACT_DATE_MEANINGS.includes(String(meaning || ""));
}

export function isLoanMaturityMeaning(meaning) {
  return String(meaning || "") === DATE_MEANING.LOAN_MATURITY_DATE;
}

/**
 * Horizon buckets from a reference "now" (ISO date or Date).
 * Returns which of 6/12/24/36 month windows the date falls into (inclusive).
 */
export function classifyDateHorizon(isoOrYear, now = new Date()) {
  const ref = now instanceof Date ? now : new Date(now);
  let target;
  if (typeof isoOrYear === "number" || /^\d{4}$/.test(String(isoOrYear))) {
    target = new Date(Date.UTC(Number(isoOrYear), 11, 31));
  } else {
    target = new Date(isoOrYear);
  }
  if (Number.isNaN(target.getTime())) {
    return { ok: false, error: "INVALID_DATE", buckets: [] };
  }
  const ms = target.getTime() - ref.getTime();
  const days = ms / (24 * 60 * 60 * 1000);
  const buckets = [];
  if (days < 0) {
    return {
      ok: true,
      daysAhead: days,
      historical: true,
      buckets: [],
      note: "Date is in the past — requires revalidation for current applicability",
    };
  }
  if (days <= 182.5) buckets.push(6);
  if (days <= 365) buckets.push(12);
  if (days <= 730) buckets.push(24);
  if (days <= 1095) buckets.push(36);
  return { ok: true, daysAhead: days, historical: false, buckets };
}

export const CONTRACT_TIMING_GUARANTEES = Object.freeze([
  "Mortgage/loan maturity is never treated as franchise or management expiration",
  "Portfolio-level date rows are rejected — hotel identity required",
  "Conditional renewal / termination-right language is preserved as DATE_MEANING, not silent expiration",
  "CUSTOMER_BLOCKED on all research-only persistence",
  "No automatic HPC hotel creation",
  "Registrant / CMBS issuer is not auto-classified as legal owner",
]);
