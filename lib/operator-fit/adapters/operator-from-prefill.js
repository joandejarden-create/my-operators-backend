/**
 * Adapt Operator Setup prefill / synthetic profile → Operator Fit operator domain model.
 */

import {
  fieldAbsent,
  fieldPresent,
  fieldUnknown,
  isKnownPositive,
  listValue,
} from "./field-state.js";
import { mapOperatingStructureList } from "../structure-mapping.js";
import {
  CANDIDATE_TYPE,
  EVIDENCE_CLASSES,
  TABLE_STAKES_CAPABILITY_TOKENS,
} from "../config.js";

function toList(v) {
  if (v == null) return [];
  if (Array.isArray(v)) return v.map((x) => String(x).trim()).filter(Boolean);
  const s = String(v || "").trim();
  if (!s) return [];
  return s.split(/\s*,\s*/).map((x) => x.trim()).filter(Boolean);
}

function wrapList(keys, op, source) {
  for (const k of keys) {
    const list = toList(op[k]);
    if (list.length) return fieldPresent(list, { source, field: k });
  }
  return fieldUnknown({ source, fieldsTried: keys });
}

function wrapScalar(keys, op, source) {
  for (const k of keys) {
    if (op[k] != null && String(op[k]).trim()) {
      return fieldPresent(String(op[k]).trim(), { source, field: k });
    }
  }
  return fieldUnknown({ source, fieldsTried: keys });
}

function normTok(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

export function isTableStakesToken(label) {
  const n = normTok(label);
  return TABLE_STAKES_CAPABILITY_TOKENS.some((t) => n === t || n.includes(t) || t.includes(n));
}

/**
 * Classify offered services into table-stakes vs differentiators.
 */
export function classifyServices(serviceLabels) {
  const tableStakes = [];
  const differentiators = [];
  for (const s of serviceLabels || []) {
    if (isTableStakesToken(s)) tableStakes.push(s);
    else differentiators.push(s);
  }
  return { tableStakes, differentiators };
}

/**
 * @param {object} prefill - Operator Setup prefill or synthetic fixture
 * @param {{ operatorId?: string, companyName?: string, candidateType?: string, brandManagedMeta?: object }} opts
 */
export function adaptOperatorFromPrefill(prefill = {}, opts = {}) {
  const op = prefill || {};
  const companyName =
    opts.companyName ||
    op.companyName ||
    op.company_name ||
    "Unknown operator";

  const countries = wrapList(["activeCountries"], op, "platform");
  const markets = wrapList(["activeMarkets", "specificMarkets", "regionsSupported"], op, "platform");
  const presence = wrapList(["marketPresenceType"], op, "platform");
  const marketPresenceRecords = Array.isArray(op.marketPresence)
    ? op.marketPresence
    : Array.isArray(op.marketPresenceRecords)
      ? op.marketPresenceRecords
      : [];
  const scales = wrapList(
    ["chainScalesSupported", "chainScale", "chainScalesYouSupport", "chain_scales"],
    op,
    "profile"
  );
  const assets = wrapList(
    ["bestFitAssetTypes", "propertyTypes", "propertyTypesManaged", "hotel_types", "asset_classes"],
    op,
    "commercial"
  );
  const situations = wrapList(
    ["operatingSituations", "projectStages", "bf_selected_situation_types", "newBuildOpeningExperience"],
    op,
    "commercial"
  );
  const structures = wrapList(
    ["managementStructuresSupported", "bestFitDealStructures", "typicalAssignmentTypes"],
    op,
    "commercial"
  );
  const brands = wrapList(["brands", "brandsManaged", "brands_managed"], op, "profile");
  const brandFamilies = wrapList(["Brand Families Operated", "brandFamiliesOperated"], op, "profile");
  const services = wrapList(
    ["offeredServices", "primaryServices", "additionalServices"],
    op,
    "governance"
  );
  const reportingLevel = wrapScalar(["ownerReportingLevel", "ownerReportingCadence"], op, "governance");
  const lessIdeal = wrapScalar(["lessIdealSituations", "less_proven_areas", "bf_not_ideal_for"], op, "commercial");
  const fee = wrapScalar(
    ["feeStructureSummary", "operatorFeeApproach", "dealTermsSummary"],
    op,
    "commercial"
  );
  const preOpening = wrapScalar(["preOpeningSupportCapability"], op, "commercial");
  const newBuild = wrapScalar(["newBuildOpeningExperience"], op, "commercial");
  const conversion = wrapScalar(["conversionReflagExperience", "Conversion / Reflag Experience"], op, "commercial");
  const dataConfidence = wrapScalar(["dataConfidenceLevel", "Data Confidence Level"], op, "master");
  const sourceType = wrapScalar(["sourceType", "Source Type"], op, "master");

  const serviceLists = classifyServices(listValue(services));
  const structureKeys = mapOperatingStructureList(listValue(structures));

  const comparables = Array.isArray(op.comparables) ? op.comparables : [];
  const sources = Array.isArray(op.sources) ? op.sources : [];
  const evidenceClasses = Array.isArray(op.evidenceClasses) ? op.evidenceClasses : [];

  if (!evidenceClasses.length) {
    if (comparables.some((c) => c && c.verified)) {
      evidenceClasses.push(EVIDENCE_CLASSES.VERIFIED_PROJECT);
    } else if (comparables.some((c) => c && c.referenced)) {
      evidenceClasses.push(EVIDENCE_CLASSES.INDEPENDENT_REFERENCED);
    } else if (comparables.length) {
      evidenceClasses.push(EVIDENCE_CLASSES.DETAILED_OPERATOR_PROVIDED);
    } else if (isKnownPositive(scales) || isKnownPositive(countries)) {
      evidenceClasses.push(EVIDENCE_CLASSES.PORTFOLIO_LEVEL);
    } else if (isKnownPositive(services)) {
      evidenceClasses.push(EVIDENCE_CLASSES.GENERAL_CLAIM);
    } else {
      evidenceClasses.push(EVIDENCE_CLASSES.UNKNOWN);
    }
  }

  const activeStatus = wrapScalar(["submission_status", "submissionStatus", "activeStatus"], op, "master");
  const isActive =
    !isKnownPositive(activeStatus) ||
    /active/i.test(String(activeStatus.value || ""));

  return {
    operatorId: opts.operatorId || op.operatorId || op.id || null,
    identity: fieldPresent({ name: companyName, parentCompany: op.parentCompany || null }),
    candidateType: opts.candidateType || CANDIDATE_TYPE.THIRD_PARTY_OPERATOR,
    brandManagedMeta: opts.brandManagedMeta || null,
    activeStatus: isActive
      ? fieldPresent("Active", { source: "master" })
      : fieldPresent(String(activeStatus.value || "Inactive"), { source: "master" }),
    geography: {
      countries,
      markets,
      presence,
      marketPresence: marketPresenceRecords,
      presenceRecords: marketPresenceRecords,
    },
    operatingStructures: structures.state === "unknown"
      ? structures
      : fieldPresent(listValue(structures), {
          source: "commercial",
          canonicalKeys: structureKeys,
        }),
    chainScales: scales,
    assetExperience: assets,
    developmentExperience: situations,
    brandsOperated: brands,
    brandFamilies,
    specialistExperience: {
      preOpening,
      newBuild,
      conversion,
      differentiators: serviceLists.differentiators.length
        ? fieldPresent(serviceLists.differentiators, { source: "governance" })
        : fieldUnknown({ source: "governance" }),
      tableStakesClaimed: serviceLists.tableStakes.length
        ? fieldPresent(serviceLists.tableStakes, { source: "governance", scoring: "no_positive_points" })
        : fieldAbsent({ source: "governance" }),
    },
    comparables: comparables.length
      ? fieldPresent(comparables, { source: "case_studies_or_fixture" })
      : fieldUnknown({ source: "case_studies" }),
    commercial: {
      feeEconomics: fee,
      differentiators: serviceLists.differentiators,
    },
    ownershipGovernance: {
      reportingLevel,
      ownerRelationsNarrative: wrapScalar(
        ["ownerCommunicationStyle", "operatingCollaborationMode"],
        op,
        "governance"
      ),
    },
    regionalResources: wrapList(["regionalTeam", "regionalResources"], op, "platform"),
    capacity: wrapScalar(["capacityNotes", "concurrentOpenings"], op, "platform"),
    risksAndConcerns: lessIdeal,
    sources: sources.length ? fieldPresent(sources, { source: "pi" }) : fieldUnknown({ source: "pi" }),
    evidenceClasses: fieldPresent(evidenceClasses, { source: "derived" }),
    dataConfidenceMeta: dataConfidence,
    sourceTypeMeta: sourceType,
    _prefillKeys: Object.keys(op),
  };
}

/**
 * Brand-managed candidate — distinct type, not a fake third-party profile.
 */
export function adaptBrandManagedCandidate({
  brandId,
  brandName,
  offersBrandManagement,
  offersBrandManagementConfirmed = false,
  markets = [],
  scales = [],
  structures = ["Brand-managed"],
  evidenceClasses = [EVIDENCE_CLASSES.PORTFOLIO_LEVEL],
  sources = [],
}) {
  const confirmed = Boolean(offersBrandManagementConfirmed || offersBrandManagement);
  return adaptOperatorFromPrefill(
    {
      companyName: `${brandName} Brand Management`,
      activeCountries: markets,
      activeMarkets: markets,
      marketPresenceType: markets.length ? ["Active operations"] : [],
      chainScalesSupported: scales,
      managementStructuresSupported: structures,
      brands: [brandName],
      offeredServices: [],
      evidenceClasses,
      sources,
      submission_status: "Active",
    },
    {
      operatorId: brandId ? `brand-managed:${brandId}` : `brand-managed:${brandName}`,
      candidateType: CANDIDATE_TYPE.BRAND_MANAGED,
      brandManagedMeta: {
        brandId: brandId || null,
        brandName,
        // Strategic preference alone must not confirm (founder enrichment 2.3)
        offersBrandManagement: Boolean(offersBrandManagement),
        offersBrandManagementConfirmed: Boolean(offersBrandManagementConfirmed),
        offersBrandManagementVerified: Boolean(offersBrandManagementConfirmed),
      },
      companyName: `${brandName} Brand Management`,
    }
  );
}
