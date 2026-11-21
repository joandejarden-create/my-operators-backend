/**
 * HIF V6 — Five-country concentration strategy (founder 2026-11-21).
 * Opportunities first; ownership enrichment second.
 * Ambitions are planning guides — never evidence thresholds.
 */

export const HIF_V6_COUNTRY_CONCENTRATION = "hif-v6-country-concentration-v6";

export const PRIORITY_COUNTRIES_V6 = Object.freeze([
  "US",
  "MX",
  "BR",
  "GB",
  "CA",
]);

/** Provisional planning ambitions — not quotas. */
export const COUNTRY_PROSPECT_AMBITIONS_V6 = Object.freeze({
  US: { min: 300, max: 400, label: "United States" },
  MX: { min: 150, max: 250, label: "Mexico" },
  BR: { min: 150, max: 250, label: "Brazil" },
  GB: { min: 150, max: 250, label: "United Kingdom" },
  CA: { min: 100, max: 150, label: "Canada" },
});

export const COUNTRY_CONCENTRATION_RULES_V6 = Object.freeze({
  geographicEligibilityExclusions: "NONE",
  priorityIsConcentrationNotRestriction: true,
  promoteOtherCountriesWhenYieldHigher: true,
  ownershipRequiredForDiscovery: false,
  historicalNotAutomaticallyActive: true,
  listsOnly: Object.freeze([
    "BRAND_CONVERSION_TARGETS",
    "MANAGEMENT_CONTRACT_TARGETS",
  ]),
  combinedPlanningAmbition: Object.freeze({ min: 850, max: 1300 }),
  globalMilestone: 1000,
});

export function emptyCountryScorecardRow(countryCode) {
  const ambition = COUNTRY_PROSPECT_AMBITIONS_V6[countryCode] || null;
  return {
    countryCode,
    countryLabel: ambition?.label || countryCode,
    planningAmbitionMin: ambition?.min ?? null,
    planningAmbitionMax: ambition?.max ?? null,
    distinctHotelsResearched: 0,
    distinctEvidenceBackedProspects: 0,
    newlyVerifiedFranchiseExpirations: 0,
    newlyVerifiedManagementExpirations: 0,
    contractNoticesAndTerminations: 0,
    brandConversionAndOperatorChangeSignals: 0,
    verifiedContractualResearchOpportunities: 0,
    evidenceBackedCommercialProspectsUndated: 0,
    outreachReady: 0,
    historicalClosedSuperseded: 0,
    holdResearchCandidates: 0,
    duplicatesRemoved: 0,
    invalidOrQuarantined: 0,
    researchCostUsd: 0,
    yieldPer1000HotelsResearched: null,
    progressTowardAmbitionMinPct: null,
    sourceFamilies: {},
    trackAContribution: 0,
    trackBContribution: 0,
  };
}

export function computeYieldPer1000(prospects, hotelsResearched) {
  const h = Number(hotelsResearched) || 0;
  const p = Number(prospects) || 0;
  if (h <= 0) return null;
  return Math.round((1000 * p) / h * 10) / 10;
}

export function progressTowardAmbition(prospects, countryCode) {
  const a = COUNTRY_PROSPECT_AMBITIONS_V6[countryCode];
  if (!a) return null;
  const n = Number(prospects) || 0;
  return {
    current: n,
    ambitionMin: a.min,
    ambitionMax: a.max,
    pctOfMin: Math.round((1000 * n) / a.min) / 10,
    remainingToMin: Math.max(0, a.min - n),
  };
}
