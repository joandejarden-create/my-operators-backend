/**
 * Early Signal production configuration (V1.2 + Pre-Opening Capture V1).
 * Enabled families include previously disabled land/capital/project/open-decision packs
 * after query hygiene. Weak individual queries stay out of the catalogs.
 */

export const EARLY_SIGNAL_PRODUCTION_FAMILIES = [
  "planning",
  "earlyDevelopment",
  "mixedUse",
  "adaptiveReuse",
  "landSite",
  "capitalFormation",
  "projectFormation",
  "openDecision",
];

/** Reserved for queries proven too noisy to schedule (empty after V1 enablement). */
export const EARLY_SIGNAL_DISABLED_FAMILIES = [];

export const EARLY_SIGNAL_EXPERIMENTAL_STATUS = "EXPERIMENTAL_DISABLED";

/** Google News recency for production discovery (7–14 days). */
export const EARLY_SIGNAL_PRODUCTION_WHEN =
  process.env.MARKET_ALERTS_EARLY_SIGNALS_WHEN || "14d";

export const EARLY_SIGNAL_PRODUCTION_PER_QUERY = Math.min(
  Math.max(parseInt(process.env.MARKET_ALERTS_EARLY_SIGNALS_PER_QUERY || "25", 10) || 25, 1),
  50
);

export const EARLY_SIGNAL_PRODUCTION_INSERT_LIMIT = Math.min(
  Math.max(parseInt(process.env.MARKET_ALERTS_EARLY_SIGNALS_LIMIT || "50", 10) || 50, 1),
  100
);

export const EARLY_SIGNAL_PRODUCTION_MAX_AGE_DAYS = Math.min(
  Math.max(parseInt(process.env.MARKET_ALERTS_EARLY_SIGNALS_MAX_AGE_DAYS || "14", 10) || 14, 7),
  30
);

export function isEarlySignalProductionEnabled() {
  return process.env.MARKET_ALERTS_EARLY_SIGNALS_ENABLED === "true";
}

export function isProductionEarlySignalFamily(family) {
  return EARLY_SIGNAL_PRODUCTION_FAMILIES.includes(family);
}

/** Weekly production cadence — default 7 days (10,080 minutes). */
export const EARLY_SIGNAL_PRODUCTION_INTERVAL_MINUTES = Math.max(
  parseInt(process.env.MARKET_ALERTS_EARLY_SIGNALS_INTERVAL_MINUTES || "10080", 10) || 10080,
  60
);
