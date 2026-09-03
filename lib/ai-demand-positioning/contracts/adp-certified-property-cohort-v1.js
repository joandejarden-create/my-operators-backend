/**
 * Canonical five-property Existing Hotel ADP certification cohort.
 * Single source of truth — do not omit Hotel Phillips.
 */

export const ADP_CERTIFIED_PROPERTY_COHORT_VERSION = "ADP_CERTIFIED_PROPERTY_COHORT_V1";

/** @type {readonly string[]} */
export const ADP_CERTIFIED_PROPERTY_IDS = Object.freeze([
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_hotel_phillips_kansas_city",
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
]);

export const CORE_CERTIFIED_PROPERTY_COHORT_INTEGRITY = "CORE_CERTIFIED_PROPERTY_COHORT_INTEGRITY";

/**
 * Validate cohort integrity against an arbitrary property list.
 */
export function assertCertifiedPropertyCohortIntegrity(propertyIds) {
  const expected = [...ADP_CERTIFIED_PROPERTY_IDS];
  const actual = [...(propertyIds || [])].sort();
  const expSorted = [...expected].sort();
  const missing = expected.filter((id) => !actual.includes(id));
  const extra = actual.filter((id) => !expSorted.includes(id));
  const pass = missing.length === 0 && extra.length === 0 && actual.length === expected.length;
  return {
    gate: CORE_CERTIFIED_PROPERTY_COHORT_INTEGRITY,
    pass,
    expectedCount: expected.length,
    actualCount: actual.length,
    missing,
    extra,
  };
}
