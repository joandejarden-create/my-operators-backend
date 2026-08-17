/**
 * Conversion Financing Package — Airtable field specs for Deal Financing Needs extensions.
 */
import * as OPT from "./conversion-financing-package-options.js";
import { map_cfp } from "./conversion-financing-package-field-map.js";

function singleSelect(name, choices) {
  return { name, type: "singleSelect", options: { choices: choices.map((c) => ({ name: c })) } };
}

function multiSelect(name, choices) {
  return { name, type: "multipleSelects", options: { choices: choices.map((c) => ({ name: c })) } };
}

function currencyField(name) {
  return { name, type: "currency", options: { precision: 0, symbol: "USD" } };
}

function dateField(name) {
  return { name, type: "dateTime", options: { dateFormat: { name: "iso" }, timeFormat: { name: "24hour" }, timeZone: "utc" } };
}

/** Fields to ensure on Capital Setup - Deal Financing Needs (idempotent). */
export function buildConversionFinancingPackageFieldSpecs() {
  const F = map_cfp;
  return [
    singleSelect(F.capitalNeedType, OPT.CAPITAL_NEED_TYPE_OPTIONS),
    singleSelect(F.capitalCurrency, OPT.CAPITAL_CURRENCY_OPTIONS),
    { name: F.capitalAmountRange, type: "singleLineText" },
    multiSelect(F.capitalUseOfProceeds, OPT.CAPITAL_USE_OF_PROCEEDS_OPTIONS),
    multiSelect(F.capitalUnlock, OPT.CAPITAL_UNLOCK_OPTIONS),
    singleSelect(F.capitalAssetStatus, OPT.CAPITAL_ASSET_STATUS_OPTIONS),
    singleSelect(F.capitalBrandStatus, OPT.CAPITAL_BRAND_STATUS_OPTIONS),
    singleSelect(F.capitalOperatorStatus, OPT.CAPITAL_OPERATOR_STATUS_OPTIONS),
    { name: F.targetBrandPath, type: "singleLineText" },
    currencyField(F.pipCapexEstimate),
    singleSelect(F.pipCapexEstimateStatus, OPT.CAPITAL_PIP_ESTIMATE_STATUS_OPTIONS),
    singleSelect(F.existingDebtStatus, OPT.CAPITAL_DEBT_STATUS_OPTIONS),
    singleSelect(F.ownerEquityContributionStatus, OPT.CAPITAL_EQUITY_CONTRIBUTION_OPTIONS),
    singleSelect(F.capitalTiming, OPT.CAPITAL_TIMING_OPTIONS),
    singleSelect(F.capitalSharingPreference, OPT.CAPITAL_SHARING_PREFERENCE_OPTIONS),
    singleSelect(F.capitalSharingStatus, OPT.CAPITAL_SHARING_STATUS_OPTIONS),
    multiSelect(F.supportingDocumentsAvailable, OPT.CAPITAL_SUPPORTING_DOCUMENTS_OPTIONS),
    singleSelect(F.capitalFinancialsAvailability, OPT.CAPITAL_FINANCIALS_AVAILABILITY_OPTIONS),
    { name: F.capitalPackageInputsJson, type: "multilineText" },
    { name: F.capitalPackageSnapshotJson, type: "multilineText" },
    { name: F.capitalPackageNarrative, type: "multilineText" },
    { name: F.capitalRiskFlags, type: "multilineText" },
    { name: F.capitalExecutionDependencies, type: "multilineText" },
    { name: F.capitalProviderCategoryFit, type: "multilineText" },
    dateField(F.capitalLastGeneratedAt),
  ];
}
