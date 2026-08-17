/**
 * Conversion Financing Package — sharing and provider visibility helpers.
 */
import {
  DEFAULT_CAPITAL_SHARING_STATUS,
  PROVIDER_VISIBLE_SHARING_STATUSES,
} from "./capital-setup/conversion-financing-package-options.js";

export function isProviderVisibleSharingStatus(status) {
  const s = String(status || "").trim();
  return PROVIDER_VISIBLE_SHARING_STATUSES.has(s);
}

export function defaultSharingStatus() {
  return DEFAULT_CAPITAL_SHARING_STATUS;
}

/**
 * Build anonymized Hotel Capital Opportunity view for capital providers.
 * Never includes property name, exact address, or owner-identifying fields.
 */
export function buildHotelCapitalOpportunityView({ inputs, snapshot, sharingStatus }) {
  if (!isProviderVisibleSharingStatus(sharingStatus)) {
    return null;
  }
  const s = snapshot || {};
  const i = inputs || {};
  const opp = s.opportunitySummary || {};
  return {
    objectType: "Hotel Capital Opportunity",
    sharingStatus,
    financingType: i.capitalNeedType || null,
    amountRange: i.capitalAmountRange || i.capitalAmount || null,
    currency: i.capitalCurrency || null,
    market: opp.marketRegion || opp.country || null,
    country: opp.country || null,
    assetType: opp.hotelType || null,
    keys: opp.keysRange || null,
    currentStatus: i.capitalAssetStatus || null,
    useOfProceeds: i.useOfProceeds || [],
    capitalUnlock: i.capitalUnlock || [],
    brandOperatorContext: s.brandOperatorContext?.summary || null,
    timing: i.capitalTiming || null,
    documentsAvailable: i.supportingDocumentsAvailable || [],
    riskFlags: s.riskFlagsAndOpenQuestions?.items || [],
    providerCategoryFit: s.capitalProviderReviewLens?.categories || [],
    sharingNote:
      "Anonymized summary only. Property-identifying details and documents require separate owner approval.",
  };
}
