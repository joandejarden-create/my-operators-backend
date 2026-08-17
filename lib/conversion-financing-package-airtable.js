/**
 * Conversion Financing Package — Airtable persistence helpers.
 */
import Airtable from "airtable";
import { map_cfp, CFP_TABLE } from "./capital-setup/conversion-financing-package-field-map.js";
import { DEFAULT_CAPITAL_SHARING_STATUS } from "./capital-setup/conversion-financing-package-options.js";

function baseClient() {
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
}

function asText(v) {
  return v == null ? "" : String(v).trim();
}

function parseJsonField(value) {
  if (!value) return null;
  if (typeof value === "object") return value;
  if (typeof value !== "string") return null;
  try {
    return JSON.parse(value);
  } catch (err) {
    console.warn("[conversion-financing-package] JSON parse failed:", err.message);
    return null;
  }
}

function parseAmountNumber(value) {
  const raw = asText(value).replace(/[$,]/g, "");
  if (!raw) return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

export function mapFinancingNeedRecord(record) {
  const f = record?.fields || {};
  const inputs =
    parseJsonField(f[map_cfp.capitalPackageInputsJson]) ||
  {
      capitalNeedType: asText(f[map_cfp.capitalNeedType]) ||
        (Array.isArray(f[map_cfp.financingTypeNeeded]) ? f[map_cfp.financingTypeNeeded][0] : asText(f[map_cfp.financingTypeNeeded])),
      capitalAmount: f[map_cfp.loanAmountRequested],
      capitalAmountRange: asText(f[map_cfp.capitalAmountRange]),
      capitalCurrency: asText(f[map_cfp.capitalCurrency]),
      useOfProceeds: f[map_cfp.capitalUseOfProceeds] || f[map_cfp.useOfProceedsLegacy] || [],
      capitalUnlock: f[map_cfp.capitalUnlock] || [],
      capitalAssetStatus: asText(f[map_cfp.capitalAssetStatus] || f[map_cfp.assetStatusLegacy]),
      capitalBrandStatus: asText(f[map_cfp.capitalBrandStatus] || f[map_cfp.brandStatusLegacy]),
      targetBrandPath: asText(f[map_cfp.targetBrandPath]),
      pipCapexEstimate: asText(f[map_cfp.pipCapexEstimate]),
      pipCapexEstimateStatus: asText(f[map_cfp.pipCapexEstimateStatus]),
      capitalOperatorStatus: asText(f[map_cfp.capitalOperatorStatus] || f[map_cfp.operatorStatusLegacy]),
      capitalFinancialsAvailability: asText(
        f[map_cfp.capitalFinancialsAvailability] || f[map_cfp.financialsAvailableLegacy]
      ),
      existingDebtStatus: asText(f[map_cfp.existingDebtStatus]),
      ownerEquityContributionStatus: asText(f[map_cfp.ownerEquityContributionStatus]),
      capitalTiming: asText(f[map_cfp.capitalTiming] || f[map_cfp.timingNeed]),
      capitalSharingPreference: asText(f[map_cfp.capitalSharingPreference]),
      capitalSharingStatus: asText(f[map_cfp.capitalSharingStatus]) || DEFAULT_CAPITAL_SHARING_STATUS,
      supportingDocumentsAvailable: f[map_cfp.supportingDocumentsAvailable] || [],
    };

  return {
    recordId: record.id,
    financingNeedName: asText(f[map_cfp.financingNeedName]),
    inputs,
    snapshot: parseJsonField(f[map_cfp.capitalPackageSnapshotJson]),
    narrative: asText(f[map_cfp.capitalPackageNarrative]),
    sharingStatus: asText(f[map_cfp.capitalSharingStatus]) || DEFAULT_CAPITAL_SHARING_STATUS,
    sharingPreference: asText(f[map_cfp.capitalSharingPreference]),
    lastGeneratedAt: asText(f[map_cfp.capitalLastGeneratedAt]),
    financingNeedStatus: asText(f[map_cfp.financingNeedStatus]),
  };
}

export function buildAirtableFieldsFromInputs(dealId, dealName, inputs) {
  const F = map_cfp;
  const amountNum = parseAmountNumber(inputs.capitalAmount);
  const pipNum = parseAmountNumber(inputs.pipCapexEstimate);
  const name = `${dealName || "Deal"} — Conversion Financing Package`.slice(0, 120);

  const fields = {
    [F.financingNeedName]: name,
    [F.relatedDeal]: [dealId],
    [F.financingNeedStatus]: "Draft",
    [F.capitalSharingStatus]: inputs.capitalSharingStatus || DEFAULT_CAPITAL_SHARING_STATUS,
    [F.capitalPackageInputsJson]: JSON.stringify(inputs),
  };

  if (inputs.capitalNeedType) fields[F.capitalNeedType] = inputs.capitalNeedType;
  if (amountNum != null) fields[F.loanAmountRequested] = amountNum;
  if (inputs.capitalAmountRange) fields[F.capitalAmountRange] = inputs.capitalAmountRange;
  if (inputs.capitalCurrency) fields[F.capitalCurrency] = inputs.capitalCurrency;
  if (inputs.useOfProceeds?.length) fields[F.capitalUseOfProceeds] = inputs.useOfProceeds;
  if (inputs.capitalUnlock?.length) fields[F.capitalUnlock] = inputs.capitalUnlock;
  if (inputs.capitalAssetStatus) fields[F.capitalAssetStatus] = inputs.capitalAssetStatus;
  if (inputs.capitalBrandStatus) fields[F.capitalBrandStatus] = inputs.capitalBrandStatus;
  if (inputs.targetBrandPath) fields[F.targetBrandPath] = inputs.targetBrandPath;
  if (pipNum != null) fields[F.pipCapexEstimate] = pipNum;
  if (inputs.pipCapexEstimateStatus) fields[F.pipCapexEstimateStatus] = inputs.pipCapexEstimateStatus;
  if (inputs.capitalOperatorStatus) fields[F.capitalOperatorStatus] = inputs.capitalOperatorStatus;
  if (inputs.capitalFinancialsAvailability) {
    fields[F.capitalFinancialsAvailability] = inputs.capitalFinancialsAvailability;
  }
  if (inputs.existingDebtStatus) fields[F.existingDebtStatus] = inputs.existingDebtStatus;
  if (inputs.ownerEquityContributionStatus) {
    fields[F.ownerEquityContributionStatus] = inputs.ownerEquityContributionStatus;
  }
  if (inputs.capitalTiming) fields[F.capitalTiming] = inputs.capitalTiming;
  if (inputs.capitalSharingPreference) fields[F.capitalSharingPreference] = inputs.capitalSharingPreference;
  if (inputs.supportingDocumentsAvailable?.length) {
    fields[F.supportingDocumentsAvailable] = inputs.supportingDocumentsAvailable;
  }
  return fields;
}

export function buildAirtableFieldsFromPackageResult(inputs, result) {
  const F = map_cfp;
  return {
    [F.capitalPackageSnapshotJson]: JSON.stringify(result.snapshot),
    [F.capitalPackageNarrative]: result.narrative,
    [F.capitalRiskFlags]: result.derived.riskFlagsText,
    [F.capitalExecutionDependencies]: result.derived.executionDependenciesText,
    [F.capitalProviderCategoryFit]: result.derived.providerCategoryFitText,
    [F.capitalLastGeneratedAt]: new Date().toISOString(),
    [F.capitalPackageInputsJson]: JSON.stringify(inputs),
    [F.financingNeedStatus]: "Needs Review",
  };
}

export async function findFinancingNeedForDeal(dealId) {
  const formula = `{${map_cfp.relatedDeal}} = "${dealId}"`;
  const rows = await baseClient()(CFP_TABLE)
    .select({ filterByFormula: formula, maxRecords: 5, sort: [{ field: map_cfp.capitalLastGeneratedAt, direction: "desc" }] })
    .all();
  return rows[0] || null;
}

export async function upsertFinancingNeedForDeal(dealId, dealName, inputs) {
  const existing = await findFinancingNeedForDeal(dealId);
  const fields = buildAirtableFieldsFromInputs(dealId, dealName, inputs);
  if (existing) {
    const updated = await baseClient()(CFP_TABLE).update(existing.id, fields, { typecast: true });
    return updated;
  }
  const created = await baseClient()(CFP_TABLE).create(fields, { typecast: true });
  return created;
}

export async function updateFinancingNeedPackage(recordId, inputs, result) {
  const fields = {
    ...buildAirtableFieldsFromPackageResult(inputs, result),
  };
  return baseClient()(CFP_TABLE).update(recordId, fields, { typecast: true });
}

export async function updateFinancingNeedSharing(recordId, { sharingStatus, sharingPreference }) {
  const fields = {};
  if (sharingStatus) fields[map_cfp.capitalSharingStatus] = sharingStatus;
  if (sharingPreference) fields[map_cfp.capitalSharingPreference] = sharingPreference;
  return baseClient()(CFP_TABLE).update(recordId, fields, { typecast: true });
}

export async function getFinancingNeedById(recordId) {
  return baseClient()(CFP_TABLE).find(recordId);
}
