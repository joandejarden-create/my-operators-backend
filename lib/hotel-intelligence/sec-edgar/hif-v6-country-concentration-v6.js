/**
 * HIF V6 — Five-country concentration strategy (founder 2026-11-21).
 * Mirror of Track A contract for Track B workspace isolation.
 */

export const HIF_V6_COUNTRY_CONCENTRATION = "hif-v6-country-concentration-v6";

export const PRIORITY_COUNTRIES_V6 = Object.freeze(["US", "MX", "BR", "GB", "CA"]);

export const COUNTRY_PROSPECT_AMBITIONS_V6 = Object.freeze({
  US: { min: 300, max: 400, label: "United States" },
  MX: { min: 150, max: 250, label: "Mexico" },
  BR: { min: 150, max: 250, label: "Brazil" },
  GB: { min: 150, max: 250, label: "United Kingdom" },
  CA: { min: 100, max: 150, label: "Canada" },
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
    brandConversionAndOperatorChangeSignals: 0,
    verifiedContractualResearchOpportunities: 0,
    evidenceBackedCommercialProspectsUndated: 0,
    historicalClosedSuperseded: 0,
    holdResearchCandidates: 0,
    duplicatesRemoved: 0,
    invalidOrQuarantined: 0,
    researchCostUsd: 0,
    portalsRetrievedOk: 0,
    filingsOrDatasetsProbed: 0,
    trackAContribution: 0,
    trackBContribution: 0,
    sourceFamilies: {},
  };
}
