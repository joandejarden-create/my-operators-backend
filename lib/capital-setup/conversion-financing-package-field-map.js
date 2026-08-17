/**
 * Conversion Financing Package — central Airtable field map.
 * Table: Capital Setup - Deal Financing Needs (child of Deals via Related Deal).
 */
import { TABLE_FINANCING_NEEDS } from "./airtable-capital-setup-fields.js";

export const CFP_TABLE = TABLE_FINANCING_NEEDS;

/** Reused Capital Setup fields (existing on Deal Financing Needs). */
export const map_cfp_reused = {
  financingNeedName: "Financing Need Name",
  relatedDeal: "Related Deal",
  financingNeedStatus: "Financing Need Status",
  financingTypeNeeded: "Financing Type Needed",
  loanAmountRequested: "Loan Amount Requested",
  useOfProceedsLegacy: "Use of Proceeds",
  timingNeed: "Timing Need",
  assetStatusLegacy: "Asset Status",
  brandStatusLegacy: "Brand Status",
  operatorStatusLegacy: "Operator Status",
  financialsAvailableLegacy: "Financials Available",
  financingReadinessNotes: "Financing Readiness Notes",
  missingInformation: "Missing Information",
  internalNotes: "Internal Notes",
};

/** MVP extension fields (created by ensure-conversion-financing-package-schema.mjs). */
export const map_cfp = {
  ...map_cfp_reused,
  capitalNeedType: "Capital Need Type",
  capitalCurrency: "Capital Currency",
  capitalAmountRange: "Capital Amount Range",
  capitalUseOfProceeds: "Capital Use of Proceeds",
  capitalUnlock: "Capital Unlock",
  capitalAssetStatus: "Capital Asset Status",
  capitalBrandStatus: "Capital Brand Status",
  capitalOperatorStatus: "Capital Operator Status",
  targetBrandPath: "Target Brand Path",
  pipCapexEstimate: "PIP / Capex Estimate",
  pipCapexEstimateStatus: "PIP / Capex Estimate Status",
  existingDebtStatus: "Existing Debt Status",
  ownerEquityContributionStatus: "Owner Equity Contribution Status",
  capitalTiming: "Capital Timing",
  capitalSharingPreference: "Capital Sharing Preference",
  capitalSharingStatus: "Capital Sharing Status",
  supportingDocumentsAvailable: "Supporting Documents Available",
  capitalFinancialsAvailability: "Capital Financials Availability",
  capitalPackageInputsJson: "Capital Package Inputs JSON",
  capitalPackageSnapshotJson: "Capital Package Snapshot JSON",
  capitalPackageNarrative: "Capital Package Narrative",
  capitalRiskFlags: "Capital Risk Flags",
  capitalExecutionDependencies: "Capital Execution Dependencies",
  capitalProviderCategoryFit: "Capital Provider Category Fit",
  capitalLastGeneratedAt: "Capital Last Generated At",
};

export const CFP_FORM_FIELD_KEYS = [
  "capitalNeedType",
  "capitalAmount",
  "capitalAmountRange",
  "capitalCurrency",
  "useOfProceeds",
  "capitalUnlock",
  "capitalAssetStatus",
  "capitalBrandStatus",
  "targetBrandPath",
  "pipCapexEstimate",
  "pipCapexEstimateStatus",
  "capitalOperatorStatus",
  "capitalFinancialsAvailability",
  "existingDebtStatus",
  "ownerEquityContributionStatus",
  "capitalTiming",
  "capitalSharingPreference",
  "supportingDocumentsAvailable",
];
