/**
 * ADP minimum measurable attribute coverage — Phase 1B diagnostic.
 * Configurable threshold (founder working value: 12). Not a production research gate.
 */

export const ADP_MINIMUM_ATTRIBUTE_COVERAGE_V1 = "ADP_MINIMUM_ATTRIBUTE_COVERAGE_V1";

/** Founder-directed working threshold for new ADP research configurations. */
export const DEFAULT_MINIMUM_ELIGIBLE_ATTRIBUTES = 12;

/**
 * @param {object} [opts]
 * @param {number} [opts.minimumEligible]
 * @param {string} [opts.programId]
 * @param {string} [opts.hotelType]
 */
export function resolveMinimumEligibleAttributeThreshold(opts = {}) {
  const configured = Number(opts.minimumEligible);
  const minimumEligible =
    Number.isFinite(configured) && configured > 0
      ? Math.floor(configured)
      : DEFAULT_MINIMUM_ELIGIBLE_ATTRIBUTES;
  return {
    contractVersion: ADP_MINIMUM_ATTRIBUTE_COVERAGE_V1,
    minimumEligible,
    source: Number.isFinite(configured) && configured > 0 ? "OVERRIDE" : "DEFAULT_FOUNDER_WORKING",
    programId: opts.programId || null,
    hotelType: opts.hotelType || null,
    productionBlockingGateActive: false,
    note:
      "Diagnostic only in Phase 1B. Future fail-closed research gate requires separate founder approval.",
  };
}

/**
 * @param {number} eligibleCount
 * @param {object} [opts]
 */
export function assessMinimumAttributeCoverage(eligibleCount, opts = {}) {
  const threshold = resolveMinimumEligibleAttributeThreshold(opts);
  const eligible = Math.max(0, Number(eligibleCount) || 0);
  const shortfall = Math.max(0, threshold.minimumEligible - eligible);
  const meetsMinimum = eligible >= threshold.minimumEligible;
  return {
    ...threshold,
    eligibleCount: eligible,
    shortfall,
    meetsMinimum,
    flag: meetsMinimum ? null : "BELOW_MINIMUM_ATTRIBUTE_COVERAGE",
  };
}
