/**
 * Census Truth Layer placeholder — P0D-B only. Never reads Census in P0D-A.
 */

export const CENSUS_TRUTH_LAYER_STATUS = "DEFERRED_INCOMPLETE_CENSUS";

export const CENSUS_FUTURE_DIMENSIONS = Object.freeze([
  "COUNTRY_PRESENCE",
  "GEOGRAPHIC_FOOTPRINT",
  "OPEN_HOTEL_COUNT",
  "PIPELINE",
]);

/**
 * @returns {object}
 */
export function getCensusTruthLayerStatus() {
  return {
    status: CENSUS_TRUTH_LAYER_STATUS,
    CENSUS_READS_FOR_TRUTH_COMPARISON: 0,
    CENSUS_TRUTH_COMPARISONS: 0,
    deferredDimensions: CENSUS_FUTURE_DIMENSIONS,
    note: "Hotel Property Census incomplete — Census-backed Truth Layer deferred to P0D-B.",
  };
}

/**
 * Attempting Census truth comparison always returns deferred.
 */
export function compareCensusTruth() {
  return {
    comparisonStatus: "NOT_EVALUATED",
    eligibilityStatus: "DEFERRED",
    censusStatus: CENSUS_TRUTH_LAYER_STATUS,
    reason: "DEFERRED_INCOMPLETE_CENSUS",
  };
}
