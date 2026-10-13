/**
 * Permanent global ADP regression-control set (Parts 27–28, 36–39).
 * Controls are for framework portability — not hotel-specific certification logic.
 */

export const ADP_REGRESSION_CONTROL_SET_VERSION = "adp_regression_control_set_v1";

export const ADP_REGRESSION_TEST_TYPES = Object.freeze([
  "IDENTITY",
  "SCENARIO_COUNT",
  "SCENARIO_IDS",
  "PROVIDER_COMPLETENESS",
  "RAW_METRIC_RECOMPUTE",
  "SOURCE_ATTRIBUTION",
  "OWNED_DOMAIN",
  "DENOMINATOR_GRAIN",
  "REPORT_PERIOD_ID",
  "COMPARABILITY",
]);

/**
 * Minimum permanent control hotels + portability archetypes.
 */
export const ADP_REGRESSION_CONTROL_HOTELS_V1 = Object.freeze([
  {
    propertyId: "adp_hilton_times_square",
    role: "CONTROL_PRIMARY",
    archetype: "urban_full_service_branded",
    notes: "Identity alias + scenario mismatch + competitor source lessons",
  },
  {
    propertyId: "adp_renaissance_times_square",
    role: "CONTROL_PEER",
    archetype: "urban_full_service_branded",
    notes: "Matched-control peer; prop_rts_* scenario tracking",
  },
  {
    propertyId: "adp_bethesda_marriott",
    role: "CONTROL_BASELINE",
    archetype: "suburban_upper_upscale",
    notes: "Stable certified baseline regression",
  },
  {
    propertyId: "adp_spice_island_beach_resort",
    role: "PORTABILITY",
    archetype: "resort",
    notes: "Resort portability",
  },
  {
    propertyId: "adp_yotel_geneva_lake",
    role: "PORTABILITY",
    archetype: "select_service_european",
    notes: "Select-service + European",
  },
  {
    propertyId: "adp_w_rome",
    role: "PORTABILITY",
    archetype: "european_lifestyle",
    notes: "European lifestyle brand",
  },
  {
    propertyId: "adp_jw_marriott_santo_domingo",
    role: "PORTABILITY",
    archetype: "cala",
    notes: "CALA portability",
  },
  {
    propertyId: "adp_now_now_noho",
    role: "PORTABILITY",
    archetype: "independent_non_major_chain",
    notes: "Independent / non-major-chain portability",
  },
]);

export function listRegressionControlPropertyIds() {
  return ADP_REGRESSION_CONTROL_HOTELS_V1.map((h) => h.propertyId);
}

export function getRegressionControlHotel(propertyId) {
  return ADP_REGRESSION_CONTROL_HOTELS_V1.find((h) => h.propertyId === propertyId) || null;
}
