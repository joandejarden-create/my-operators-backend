/**
 * Compatibility re-export — prefer adp-measurement-coverage-policy-v1.js.
 */

import {
  DEFAULT_COVERAGE_LIMITS,
  resolveMeasurementCoverageLimits,
  assessMinimumAttributeCoverage,
} from "./adp-measurement-coverage-policy-v1.js";

export {
  ADP_MEASUREMENT_COVERAGE_POLICY_V1 as ADP_MINIMUM_ATTRIBUTE_COVERAGE_V1,
  DEFAULT_COVERAGE_LIMITS,
  assessMinimumAttributeCoverage,
  assessMeasurementCoveragePolicy,
  resolveMeasurementCoverageLimits,
} from "./adp-measurement-coverage-policy-v1.js";

/** Founder working minimum (unique eligible concepts). */
export const DEFAULT_MINIMUM_ELIGIBLE_ATTRIBUTES = DEFAULT_COVERAGE_LIMITS.minimum;

export function resolveMinimumEligibleAttributeThreshold(opts = {}) {
  const limits = resolveMeasurementCoverageLimits({
    minimum: opts.minimumEligible,
  });
  return {
    contractVersion: "ADP_MINIMUM_ATTRIBUTE_COVERAGE_V1",
    minimumEligible: limits.minimum,
    source: opts.minimumEligible != null ? "OVERRIDE" : "DEFAULT_FOUNDER_WORKING",
    programId: opts.programId || null,
    hotelType: opts.hotelType || null,
    productionBlockingGateActive: false,
    note: limits.note,
  };
}
