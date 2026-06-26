/**
 * Capital Provider Explorer — tab readiness scoring and general readiness document templates.
 */
import { CAPITAL_PROVIDER_FIELDS as F } from "./airtable-capital-setup-fields.js";

export const TAB_KEYS = [
  "overview",
  "lendingFocus",
  "dealCriteria",
  "requiredInfo",
  "process",
  "contactPathway",
  "sources",
  "internalNotes",
];

export const READINESS_LEVELS = ["Strong", "Adequate", "Thin", "Not Enough Data"];

const LEVEL_RANK = {
  Strong: 4,
  Adequate: 3,
  Thin: 2,
  "Not Enough Data": 1,
};

export const GENERAL_READINESS_INTERNAL_NOTE =
  "General readiness checklist item added to support Financing Hub MVP. Not provider-specific.";

export const GENERAL_READINESS_DESCRIPTION =
  "General hotel financing readiness item. Not confirmed as a provider-specific requirement unless supported by source references.";

/** @type {Array<{ documentName: string; category: string; requiredLevel: string; dealTypes: string[]; sortOrder: number }>} */
export const GENERAL_FINANCING_READINESS_DOCS = [
  { documentName: "Executive Summary", category: "Financing Request", requiredLevel: "Usually Required", dealTypes: ["All Deal Types"], sortOrder: 1 },
  { documentName: "Financing Request", category: "Financing Request", requiredLevel: "Usually Required", dealTypes: ["All Deal Types"], sortOrder: 2 },
  { documentName: "Sources & Uses", category: "Financial Statements", requiredLevel: "Usually Required", dealTypes: ["All Deal Types"], sortOrder: 3 },
  { documentName: "Property Financials", category: "Financial Statements", requiredLevel: "Usually Required", dealTypes: ["All Deal Types"], sortOrder: 4 },
  { documentName: "Sponsor Background", category: "Sponsor / Ownership", requiredLevel: "Usually Required", dealTypes: ["All Deal Types"], sortOrder: 5 },
  { documentName: "Existing Debt Information", category: "Financial Statements", requiredLevel: "Helpful", dealTypes: ["Refinance", "Acquisition"], sortOrder: 6 },
  { documentName: "STR Report / Market Data", category: "Market / STR", requiredLevel: "Helpful", dealTypes: ["All Deal Types"], sortOrder: 7 },
  { documentName: "Brand Information", category: "Brand / Operator", requiredLevel: "Helpful", dealTypes: ["All Deal Types"], sortOrder: 8 },
  { documentName: "Operator Information", category: "Brand / Operator", requiredLevel: "Helpful", dealTypes: ["All Deal Types"], sortOrder: 9 },
  { documentName: "Capex / PIP Budget", category: "Development / Construction", requiredLevel: "Helpful", dealTypes: ["Renovation / PIP"], sortOrder: 10 },
  { documentName: "Construction Budget", category: "Development / Construction", requiredLevel: "Helpful", dealTypes: ["Construction"], sortOrder: 11 },
  { documentName: "Business Plan", category: "Financing Request", requiredLevel: "Helpful", dealTypes: ["All Deal Types"], sortOrder: 12 },
  { documentName: "Appraisal", category: "Appraisal / Valuation", requiredLevel: "Case-by-Case", dealTypes: ["Acquisition", "Refinance"], sortOrder: 13 },
  { documentName: "Legal / Ownership Documents", category: "Legal / Ownership", requiredLevel: "Usually Required", dealTypes: ["All Deal Types"], sortOrder: 14 },
];

export const OWNER_FACING_BACKFILL_FIELD_KEYS = [
  "shortDescription",
  "institutionOverview",
  "hotelLendingFocus",
  "geographicCoverage",
  "preferredMarkets",
  "typicalDealTypes",
  "loanProductsOffered",
  "preferredAssetTypes",
  "projectStageAppetite",
  "brandPreference",
  "operatorPreference",
  "sponsorPreference",
  "contactPathway",
  "requiredInformationSummary",
  "processOverview",
  "ownerFacingNotes",
  "sourceConfidence",
  "lastVerifiedDate",
];

export function isPopulated(value) {
  if (value === undefined || value === null) return false;
  if (typeof value === "string") return Boolean(value.trim());
  if (typeof value === "number") return Number.isFinite(value);
  if (typeof value === "boolean") return value;
  if (Array.isArray(value)) return value.length > 0;
  return true;
}

export function fieldPopulated(fields, airtableFieldName) {
  return isPopulated(fields?.[airtableFieldName]);
}

export function countPopulated(fields, airtableFieldNames) {
  return (airtableFieldNames || []).filter((name) => fieldPopulated(fields, name)).length;
}

export function scoreFromCount(count, strongMin, adequateMin, thinMin) {
  if (count >= strongMin) return "Strong";
  if (count >= adequateMin) return "Adequate";
  if (count >= thinMin) return "Thin";
  return "Not Enough Data";
}

export function isGeneralReadinessDocument(record, documentFieldMap) {
  const desc = String(record.fields?.[documentFieldMap.description] || "");
  const internal = String(record.fields?.[documentFieldMap.internalNotes] || "");
  const reqName = String(record.fields?.[documentFieldMap.reqName] || "");
  return (
    reqName.startsWith("General Financing Readiness —") ||
    desc.includes(GENERAL_READINESS_DESCRIPTION) ||
    internal.includes(GENERAL_READINESS_INTERNAL_NOTE)
  );
}

export function isGenericProcessCopy(fields) {
  const process = String(fields?.[F.processOverview] || "").trim();
  if (!process) return false;
  const hasSupport =
    fieldPopulated(fields, F.requiredInformationSummary) ||
    fieldPopulated(fields, F.ownerFacingNotes);
  if (hasSupport) return false;
  const genericPatterns = [
    /^contact (the )?institution/i,
    /^visit (the )?official website/i,
    /^reach out to your local/i,
    /^business banking and local branch/i,
  ];
  return genericPatterns.some((re) => re.test(process)) || process.length < 80;
}

export function hasPublicContactPathway(fields, contacts) {
  const pathway = String(fields?.[F.contactPathway] || "");
  if (/direct contact/i.test(pathway)) return true;
  return (contacts || []).some((c) => {
    const email = c.fields?.Email || c.fields?.["Email"];
    const internalOnly = c.fields?.["Internal Only"];
    return isPopulated(email) && internalOnly !== true;
  });
}

export function criteriaHasSupportedDetail(criteriaRecords, criteriaFieldMap) {
  return (criteriaRecords || []).some((rec) => {
    const f = rec.fields || {};
    const hasSummary = isPopulated(f[criteriaFieldMap.ownerSummary]);
    const hasProduct = isPopulated(f[criteriaFieldMap.loanProduct]);
    const hasDealTypes = isPopulated(f[criteriaFieldMap.dealTypes]);
    const hasSize =
      isPopulated(f[criteriaFieldMap.minLoan]) ||
      isPopulated(f[criteriaFieldMap.maxLoan]) ||
      isPopulated(f[criteriaFieldMap.termRange]);
    return hasSummary || hasProduct || hasDealTypes || hasSize;
  });
}

/**
 * @param {object} ctx
 * @param {object} ctx.providerFields — Capital Provider Airtable fields
 * @param {object[]} ctx.criteriaRecords
 * @param {object[]} ctx.documentRecords
 * @param {object[]} ctx.sourceRecords
 * @param {object[]} ctx.contactRecords
 * @param {object} ctx.documentFieldMap
 * @param {object} ctx.criteriaFieldMap
 */
export function scoreProviderTabs(ctx) {
  const pf = ctx.providerFields || {};
  const criteria = ctx.criteriaRecords || [];
  const documents = ctx.documentRecords || [];
  const sources = ctx.sourceRecords || [];
  const contacts = ctx.contactRecords || [];
  const DF = ctx.documentFieldMap;
  const CF = ctx.criteriaFieldMap;

  const overviewCount = countPopulated(pf, [
    F.shortDescription,
    F.institutionOverview,
    F.institutionType,
    F.primaryRegion,
    F.geographicCoverage,
    F.hotelLendingFocus,
    F.sourceConfidence,
  ]);

  const lendingCount = countPopulated(pf, [
    F.hotelLendingFocus,
    F.geographicCoverage,
    F.preferredMarkets,
    F.primaryRegion,
    F.preferredAssetTypes,
    F.typicalDealTypes,
    F.projectStageAppetite,
  ]);

  const hasLoanProducts = fieldPopulated(pf, F.loanProductsOffered);
  const hasDealTypes = fieldPopulated(pf, F.typicalDealTypes);
  const criteriaCount = criteria.length;
  const criteriaDetail = criteriaHasSupportedDetail(criteria, CF);

  let dealCriteria = "Not Enough Data";
  if (
    criteriaCount >= 2 &&
    hasLoanProducts &&
    hasDealTypes &&
    criteriaDetail
  ) {
    dealCriteria = "Strong";
  } else if (criteriaCount >= 1 && hasLoanProducts) {
    dealCriteria = "Adequate";
  } else if (hasLoanProducts) {
    dealCriteria = "Thin";
  }

  const docCount = documents.length;
  const hasRequiredSummary = fieldPopulated(pf, F.requiredInformationSummary);
  let requiredInfo = "Not Enough Data";
  if (docCount >= 4) requiredInfo = "Strong";
  else if (docCount >= 2) requiredInfo = "Adequate";
  else if (hasRequiredSummary) requiredInfo = "Thin";

  const processCount = countPopulated(pf, [
    F.processOverview,
    F.contactPathway,
    F.requiredInformationSummary,
    F.ownerFacingNotes,
  ]);
  let process = scoreFromCount(processCount, 4, 2, 1);
  if (process === "Adequate" && isGenericProcessCopy(pf) && processCount <= 2) {
    process = "Thin";
  }
  if (!fieldPopulated(pf, F.processOverview) && processCount === 0) {
    process = "Not Enough Data";
  }

  const hasPathway = fieldPopulated(pf, F.contactPathway);
  const hasOwnerNotes = fieldPopulated(pf, F.ownerFacingNotes);
  const publicContact = hasPublicContactPathway(pf, contacts);
  let contactPathway = "Not Enough Data";
  if (hasPathway && hasOwnerNotes && publicContact) contactPathway = "Strong";
  else if (hasPathway) contactPathway = "Adequate";
  else if (hasOwnerNotes) contactPathway = "Thin";

  const sourceCount = sources.length;
  const sourcesTab = scoreFromCount(sourceCount, 3, 2, 1);

  const hasInternalNotes = fieldPopulated(pf, F.internalNotes);
  const hasVerificationNotes =
    String(pf[F.sourceConfidence] || "").toLowerCase().includes("verification") ||
    String(pf[F.internalNotes] || "").toLowerCase().includes("warning") ||
    String(pf[F.internalNotes] || "").toLowerCase().includes("not a published");
  const hasRelationship =
    fieldPopulated(pf, F.relationshipSensitivity) ||
    fieldPopulated(pf, F.internalRelationshipOwner);

  let internalNotes = "Not Enough Data";
  if (hasInternalNotes && (hasVerificationNotes || hasRelationship)) internalNotes = "Strong";
  else if (hasInternalNotes || hasVerificationNotes) internalNotes = "Adequate";
  else if (hasInternalNotes) internalNotes = "Thin";

  return {
    overview: scoreFromCount(overviewCount, 6, 4, 2),
    lendingFocus: scoreFromCount(lendingCount, 5, 4, 2),
    dealCriteria,
    requiredInfo,
    process,
    contactPathway,
    sources: sourcesTab,
    internalNotes,
  };
}

export function overallReadinessFromTabs(tabReadiness) {
  const ownerTabs = [
    tabReadiness.overview,
    tabReadiness.lendingFocus,
    tabReadiness.dealCriteria,
    tabReadiness.requiredInfo,
    tabReadiness.process,
    tabReadiness.contactPathway,
    tabReadiness.sources,
  ];
  const avg =
    ownerTabs.reduce((sum, level) => sum + (LEVEL_RANK[level] || 1), 0) / ownerTabs.length;
  if (avg >= 3.4) return "Strong";
  if (avg >= 2.6) return "Adequate";
  if (avg >= 1.8) return "Thin";
  return "Not Enough Data";
}

export function recommendedUiTreatment(tabReadiness) {
  const ownerTabs = [
    tabReadiness.overview,
    tabReadiness.lendingFocus,
    tabReadiness.dealCriteria,
    tabReadiness.requiredInfo,
    tabReadiness.process,
    tabReadiness.contactPathway,
    tabReadiness.sources,
  ];
  const adequateOrStrong = ownerTabs.filter(
    (t) => t === "Strong" || t === "Adequate"
  ).length;
  if (adequateOrStrong >= 5) return "Full Tabs";
  if (adequateOrStrong >= 3) return "Consolidated Tabs";
  return "Summary Only";
}

export function listPopulatedProviderFields(fields) {
  return Object.values(F).filter((name) => fieldPopulated(fields, name));
}

export function listMissingBackfillFields(fields) {
  return OWNER_FACING_BACKFILL_FIELD_KEYS.filter((key) => {
    const airtableName = F[key];
    return airtableName && !fieldPopulated(fields, airtableName);
  }).map((key) => F[key]);
}

/**
 * @param {object[]} providerReports — each with tabReadiness
 */
export function buildGlobalTabRecommendation(providerReports) {
  const ownerTabKeys = [
    "overview",
    "lendingFocus",
    "dealCriteria",
    "requiredInfo",
    "process",
    "contactPathway",
    "sources",
  ];

  let optionACount = 0;
  for (const p of providerReports) {
    const tabs = p.tabReadiness || {};
    const adequateOrStrong = ownerTabKeys.filter(
      (k) => tabs[k] === "Strong" || tabs[k] === "Adequate"
    ).length;
    if (adequateOrStrong >= 5) optionACount += 1;
  }

  const thinOrLess = providerReports.filter(
    (p) => p.overallReadiness === "Thin" || p.overallReadiness === "Not Enough Data"
  ).length;
  const majorityThin = thinOrLess > providerReports.length / 2;

  if (majorityThin) {
    return {
      recommendedMvpTabStructure: ["Summary Card Only"],
      reasoning:
        "Most providers are Thin or Not Enough Data across owner-facing tabs. A summary-card MVP avoids multiple empty tabs.",
      tabsToCombine: ownerTabKeys,
      tabsToKeepSeparate: [],
      option: "C",
    };
  }

  if (optionACount >= 8) {
    return {
      recommendedMvpTabStructure: [
        "Overview",
        "Lending Focus",
        "Deal Criteria",
        "Required Info",
        "Process",
        "Contact Pathway",
        "Sources",
      ],
      reasoning:
        "At least 8 providers have Adequate or Strong content in 5+ owner-facing tabs. Full tab structure is supportable.",
      tabsToCombine: [],
      tabsToKeepSeparate: [
        "Overview",
        "Lending Focus",
        "Deal Criteria",
        "Required Info",
        "Process",
        "Contact Pathway",
        "Sources",
      ],
      option: "A",
    };
  }

  return {
    recommendedMvpTabStructure: [
      "Overview",
      "Financing Focus",
      "Readiness & Process",
      "Sources",
    ],
    reasoning:
      "Content is uneven across providers. Consolidated MVP tabs avoid several thin owner-facing sections while keeping Explorer useful.",
    tabsToCombine: [
      "Lending Focus + Deal Criteria → Financing Focus",
      "Required Info + Process + Contact Pathway → Readiness & Process",
    ],
    tabsToKeepSeparate: ["Overview", "Sources", "Internal Notes (admin only)"],
    option: "B",
  };
}

export function buildGeneralReadinessDocumentRow(providerName, template, providerId) {
  const reqName = `General Financing Readiness — ${template.documentName}`;
  return {
    documentRequirementName: reqName,
    documentName: template.documentName,
    category: template.category,
    requiredLevel: template.requiredLevel,
    dealTypes: template.dealTypes,
    description: GENERAL_READINESS_DESCRIPTION,
    ownerInstructions:
      "Prepare this item as part of general hotel financing readiness. Confirm provider-specific requirements with the lender.",
    visibility: "Owner Visible",
    sortOrder: template.sortOrder,
    internalNotes: GENERAL_READINESS_INTERNAL_NOTE,
    providerId,
    providerName,
  };
}
