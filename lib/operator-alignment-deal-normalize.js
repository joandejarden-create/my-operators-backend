/**
 * Phase 5E — Normalize deal inputs for Operator Alignment scoring.
 * Structured SI/Deals fields first; legacy MP/SI fields as fallback only.
 */

import {
  OAS_DEAL_SI_FIELD_NAMES,
  OAS_DEAL_DEALS_FIELD_NAMES,
  OAS_DEAL_MP_FIELD_NAMES,
} from "./operator-alignment-field-options.js";

function toStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && Number.isFinite(v)) return String(v);
  if (Array.isArray(v)) return v.map((x) => toStr(x)).filter(Boolean).join(", ");
  if (typeof v === "object" && v && typeof v.name === "string") return String(v.name).trim();
  return String(v).trim();
}

function toList(v) {
  if (v == null) return [];
  if (Array.isArray(v)) return v.map((x) => toStr(x)).filter(Boolean);
  const s = toStr(v);
  if (!s) return [];
  return s.split(/\s*,\s*/).map((x) => x.trim()).filter(Boolean);
}

function locValue(locationData, airtableKey, normalizedKey) {
  if (!locationData || typeof locationData !== "object") return "";
  return locationData[airtableKey] ?? locationData[normalizedKey] ?? "";
}

function pickWithSource(siData, dealFields, siKey, dealKeys, sources, outKey) {
  const si = siData || {};
  const deals = dealFields || {};
  const siTitle = OAS_DEAL_SI_FIELD_NAMES[siKey];
  if (siTitle && toList(si[siTitle]).length) {
    sources[outKey] = "structured_si";
    return toList(si[siTitle]);
  }
  if (siTitle && toStr(si[siTitle])) {
    sources[outKey] = "structured_si";
    return [toStr(si[siTitle])];
  }
  for (const k of dealKeys || []) {
    const v = deals[k];
    if (toList(v).length) {
      sources[outKey] = "structured_deals";
      return toList(v);
    }
    if (toStr(v)) {
      sources[outKey] = "structured_deals";
      return [toStr(v)];
    }
  }
  return [];
}

function pickScalarWithSource(siData, dealFields, siKey, dealKeys, legacyKeys, sources, outKey) {
  const si = siData || {};
  const deals = dealFields || {};
  const siTitle = OAS_DEAL_SI_FIELD_NAMES[siKey];
  if (siTitle && toStr(si[siTitle])) {
    sources[outKey] = "structured_si";
    return toStr(si[siTitle]);
  }
  for (const k of dealKeys || []) {
    if (toStr(deals[k])) {
      sources[outKey] = "structured_deals";
      return toStr(deals[k]);
    }
  }
  for (const k of legacyKeys || []) {
    if (toStr((si || {})[k])) {
      sources[outKey] = "legacy_si";
      return toStr(si[k]);
    }
    if (toStr((deals || {})[k])) {
      sources[outKey] = "legacy_deals";
      return toStr(deals[k]);
    }
  }
  return "";
}

/**
 * @param {object} dealFields
 * @param {object|null} locationData
 * @param {object|null} mpData
 * @param {object|null} siData
 * @returns {object} normalized deal inputs + fieldSources map
 */
export function normalizeOperatorAlignmentDealInputs(dealFields, locationData, mpData, siData) {
  const sources = {};
  const si = siData || {};
  const mp = mpData || {};
  const deals = dealFields || {};

  const dealCountry = toStr(
    locValue(locationData, "Country", "country") || deals.Country || deals.country
  );
  const dealCity = toStr(
    locValue(locationData, "City", "city") ||
      locValue(locationData, "Market", "market") ||
      deals.City ||
      deals.city
  );
  const dealScale = toStr(
    locValue(locationData, "Hotel Chain Scale", "hotelChainScale") || deals["Hotel Chain Scale"]
  );
  const dealProjectType = toStr(deals["Project Type"]);
  const dealBuildingType = toStr(
    locValue(locationData, "Building Type", "buildingType") || deals["Building Type"]
  );
  const dealStage = toStr(
    deals["Stage of Development"] || locValue(locationData, "Stage of Development", "stageOfDevelopment")
  );
  const assetStatus = toStr(deals["Asset Status"] || deals.assetStatus);

  const brandAgreementStructure = pickScalarWithSource(
    si,
    deals,
    "brandAgreementStructure",
    ["Brand Agreement Structure", "brandAgreementStructure"],
    [],
    sources,
    "brandAgreementStructure"
  );
  const operatingModel = pickScalarWithSource(
    si,
    deals,
    "dealOperatingModel",
    ["Operating Model", "operatingModel"],
    ["Preferred Future Operating Model", "Current Operating Model"],
    sources,
    "operatingModel"
  );

  const preferredManagementStructure = pickWithSource(
    si,
    deals,
    "preferredManagementStructure",
    ["Preferred Management Structure", "preferredManagementStructure"],
    sources,
    "preferredManagementStructure"
  );

  const preferredOperatorManagementStructure = toList(
    mp[OAS_DEAL_MP_FIELD_NAMES.preferredOperatorManagementStructure]
  );
  if (preferredOperatorManagementStructure.length) {
    sources.preferredOperatorManagementStructure = "structured_mp";
  }

  const operatorStructureIntent = pickScalarWithSource(
    si,
    deals,
    "operatorStructureIntent",
    [],
    [],
    sources,
    "operatorStructureIntent"
  );

  const operatorScope = pickWithSource(
    si,
    deals,
    "operatorScope",
    ["Operator Scope", "operatorScope"],
    sources,
    "operatorScope"
  );

  const requiredOperatorServices = pickWithSource(
    si,
    deals,
    "requiredOperatorServices",
    ["Required Operator Services"],
    sources,
    "requiredOperatorServices"
  );
  const mustHaveOperatorServices = pickWithSource(
    si,
    deals,
    "mustHaveOperatorServices",
    ["Must-Have Operator Services"],
    sources,
    "mustHaveOperatorServices"
  );
  const niceToHaveOperatorServices = pickWithSource(
    si,
    deals,
    "niceToHaveOperatorServices",
    ["Nice-to-Have Operator Services"],
    sources,
    "niceToHaveOperatorServices"
  );

  let legacyMustHaves = toList(
    si["Must-Haves From Brand/Operator"] || si["Must-Haves From Brand or Operator"]
  );
  if (legacyMustHaves.length) sources.legacyMustHaves = "legacy_si";

  const marketPresenceRequirement = pickScalarWithSource(
    si,
    deals,
    "marketPresenceRequirement",
    ["Market Presence Requirement"],
    [],
    sources,
    "marketPresenceRequirement"
  );

  const preOpeningSupportNeeded = pickScalarWithSource(
    si,
    deals,
    "preOpeningSupportNeeded",
    ["Pre-Opening Support Needed"],
    [],
    sources,
    "preOpeningSupportNeeded"
  );

  const ownerReportingExpectations = pickScalarWithSource(
    si,
    deals,
    "ownerReportingExpectations",
    ["Owner Reporting Expectations"],
    ["Owner Reporting Frequency", "Preferred Reporting Frequency", "Owner Reporting Cadence"],
    sources,
    "ownerReportingExpectations"
  );

  const ownerControlPreference = pickScalarWithSource(
    si,
    deals,
    "ownerControlPreference",
    ["Owner Control Preference"],
    [],
    sources,
    "ownerControlPreference"
  );

  const commercialPriority = pickWithSource(
    si,
    deals,
    "commercialPriority",
    ["Commercial Priority"],
    sources,
    "commercialPriority"
  );

  const fbComplexity =
    toStr(deals[OAS_DEAL_DEALS_FIELD_NAMES.fbComplexity]) ||
    toStr(deals["F&B Complexity"]) ||
    "";
  if (fbComplexity) sources.fbComplexity = "structured_deals";

  const openingTimeline =
    toStr(deals[OAS_DEAL_DEALS_FIELD_NAMES.openingTimeline]) ||
    toStr(deals["Opening Timeline"]) ||
    "";
  if (openingTimeline) sources.openingTimeline = "structured_deals";

  const legacyDealStructure = toStr(mp["Preferred Deal Structure"]);
  if (legacyDealStructure && !sources.brandAgreementStructure) {
    sources.legacyDealStructure = "legacy_mp";
  }

  const dealPreferredBrands = toList(si["Preferred Brands"]);
  const dealBreakers = toList(si["Top 3 Deal Breakers"]);

  return {
    dealCountry,
    dealCity,
    dealScale,
    dealProjectType,
    dealBuildingType,
    dealStage,
    assetStatus,
    brandAgreementStructure,
    operatingModel,
    preferredManagementStructure,
    preferredOperatorManagementStructure,
    operatorStructureIntent,
    operatorScope,
    requiredOperatorServices,
    mustHaveOperatorServices,
    niceToHaveOperatorServices,
    marketPresenceRequirement,
    preOpeningSupportNeeded,
    ownerReportingExpectations,
    ownerControlPreference,
    commercialPriority,
    fbComplexity,
    openingTimeline,
    legacyDealStructure,
    legacyMustHaves,
    dealPreferredBrands,
    dealBreakers,
    dealRoy: toStr(mp["Royalty Fee Expectations"]),
    dealMktFee: toStr(mp["Marketing Fee Expectations"]),
    dealLoyaltyFee: toStr(mp["Loyalty Fee Expectations"]),
    fieldSources: sources,
    hasStructuredStructure: Boolean(
      brandAgreementStructure ||
        operatingModel ||
        preferredManagementStructure.length ||
        preferredOperatorManagementStructure.length ||
        operatorStructureIntent ||
        operatorScope.length
    ),
    hasStructuredServices: Boolean(
      mustHaveOperatorServices.length ||
        requiredOperatorServices.length ||
        operatorScope.length
    ),
  };
}
