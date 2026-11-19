/**
 * ADP measurement coverage policy — min / preferred / max unique eligible concepts.
 * Diagnostic-only in Phase 1C / PR-2 (no production research blocking).
 */

export const ADP_MEASUREMENT_COVERAGE_POLICY_V1 = "ADP_MEASUREMENT_COVERAGE_POLICY_V1";

export const MEASUREMENT_COVERAGE_STATUS = Object.freeze({
  BELOW_MINIMUM_ATTRIBUTE_COVERAGE: "BELOW_MINIMUM_ATTRIBUTE_COVERAGE",
  MEASUREMENT_COVERAGE_COMPLIANT: "MEASUREMENT_COVERAGE_COMPLIANT",
  ABOVE_MAXIMUM_ATTRIBUTE_COVERAGE: "ABOVE_MAXIMUM_ATTRIBUTE_COVERAGE",
});

/** Founder-approved working limits (unique eligible approved concepts). */
export const DEFAULT_COVERAGE_LIMITS = Object.freeze({
  minimum: 12,
  preferredMin: 15,
  preferredMax: 18,
  maximum: 20,
});

/**
 * @param {object} [opts]
 */
export function resolveMeasurementCoverageLimits(opts = {}) {
  const d = DEFAULT_COVERAGE_LIMITS;
  const minimum = Number.isFinite(Number(opts.minimum)) ? Math.floor(Number(opts.minimum)) : d.minimum;
  const preferredMin = Number.isFinite(Number(opts.preferredMin))
    ? Math.floor(Number(opts.preferredMin))
    : d.preferredMin;
  const preferredMax = Number.isFinite(Number(opts.preferredMax))
    ? Math.floor(Number(opts.preferredMax))
    : d.preferredMax;
  const maximum = Number.isFinite(Number(opts.maximum)) ? Math.floor(Number(opts.maximum)) : d.maximum;
  return {
    contractVersion: ADP_MEASUREMENT_COVERAGE_POLICY_V1,
    minimum,
    preferredMin,
    preferredMax,
    maximum,
    productionBlockingGateActive: false,
    note:
      "Diagnostic only in PR-2. Counts unique eligible measurable concepts — not raw synonym keys. Research-blocking requires founder approval.",
  };
}

/**
 * @param {number} uniqueEligibleConceptCount
 * @param {object} [opts]
 */
export function assessMeasurementCoveragePolicy(uniqueEligibleConceptCount, opts = {}) {
  const limits = resolveMeasurementCoverageLimits(opts);
  const count = Math.max(0, Number(uniqueEligibleConceptCount) || 0);
  const shortfall = Math.max(0, limits.minimum - count);
  const surplus = Math.max(0, count - limits.maximum);
  const inPreferredRange = count >= limits.preferredMin && count <= limits.preferredMax;

  let status = MEASUREMENT_COVERAGE_STATUS.MEASUREMENT_COVERAGE_COMPLIANT;
  let flag = null;
  if (count < limits.minimum) {
    status = MEASUREMENT_COVERAGE_STATUS.BELOW_MINIMUM_ATTRIBUTE_COVERAGE;
    flag = status;
  } else if (count > limits.maximum) {
    status = MEASUREMENT_COVERAGE_STATUS.ABOVE_MAXIMUM_ATTRIBUTE_COVERAGE;
    flag = status;
  }

  return {
    ...limits,
    uniqueEligibleConceptCount: count,
    shortfall,
    surplus,
    inPreferredRange,
    status,
    flag,
    meetsMinimum: count >= limits.minimum,
    withinMaximum: count <= limits.maximum,
    governedExceptionEligible: count < limits.minimum,
    governedExceptionNote:
      count < limits.minimum
        ? "Properties genuinely unable to support 12 distinct relevant attributes may request a steward exception — do not invent attributes."
        : null,
  };
}

/** Backward-compatible wrapper used by Phase 1B bridge. */
export function assessMinimumAttributeCoverage(eligibleCount, opts = {}) {
  const policy = assessMeasurementCoveragePolicy(eligibleCount, {
    minimum: opts.minimumEligible,
  });
  return {
    contractVersion: policy.contractVersion,
    minimumEligible: policy.minimum,
    source: opts.minimumEligible != null ? "OVERRIDE" : "DEFAULT_FOUNDER_WORKING",
    programId: opts.programId || null,
    hotelType: opts.hotelType || null,
    productionBlockingGateActive: false,
    note: policy.note,
    eligibleCount: policy.uniqueEligibleConceptCount,
    shortfall: policy.shortfall,
    meetsMinimum: policy.meetsMinimum,
    flag: policy.meetsMinimum ? null : "BELOW_MINIMUM_ATTRIBUTE_COVERAGE",
  };
}
