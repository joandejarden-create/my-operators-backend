/**
 * Emerging Competitor rules — validated Presence / gap + real elapsed periods only.
 */

export const EMERGING_COMPETITOR_VERSION = "emerging_competitor_v1";

export const EMERGING_RULE = Object.freeze({
  minPeriodsForEmerging: 3,
  minRecurrenceThreshold: 2,
  minAppearanceIncrease: 1,
  syntheticHistoryAllowed: false,
  twoPointOverstatementBlocked: true,
  periods: {
    1: "NOT_ELIGIBLE",
    2: "CURRENT_VS_PRIOR_ONLY",
    3: "EARLY_EMERGING_POSSIBLE",
    4: "STRONGER_LONGITUDINAL",
  },
});

/**
 * Evaluate emerging competitor candidacy from period-over-period presence counts.
 */
export function evaluateEmergingCompetitor(opts = {}) {
  const periods = opts.periods || [];
  const periodCount = periods.length;

  if (periodCount < EMERGING_RULE.minPeriodsForEmerging) {
    return {
      eligible: false,
      status: periodCount <= 1 ? "NOT_ELIGIBLE" : "CURRENT_VS_PRIOR_ONLY",
      emergingCandidate: false,
      reason: `Requires ${EMERGING_RULE.minPeriodsForEmerging}+ real elapsed periods`,
    };
  }

  const appearances = periods.map((p) => p.appearanceCount || 0);
  const increases = [];
  for (let i = 1; i < appearances.length; i++) {
    if (appearances[i] > appearances[i - 1]) increases.push(i);
  }

  const recurrenceMet = increases.length >= EMERGING_RULE.minRecurrenceThreshold;
  const latestVsFirst = appearances[appearances.length - 1] - appearances[0];

  if (recurrenceMet && latestVsFirst >= EMERGING_RULE.minAppearanceIncrease) {
    return {
      eligible: true,
      status: periodCount >= 4 ? "STRONGER_LONGITUDINAL" : "EARLY_EMERGING",
      emergingCandidate: true,
      appearanceSeries: appearances,
      increaseCount: increases.length,
    };
  }

  return {
    eligible: false,
    status: "INSUFFICIENT_RECURRENCE",
    emergingCandidate: false,
    appearanceSeries: appearances,
  };
}

/**
 * Client copy gate — only show Emerging Competitor when longitudinal gate satisfied.
 */
export function canShowEmergingCompetitorLabel(evaluation) {
  return evaluation?.emergingCandidate === true && evaluation?.eligible === true;
}
