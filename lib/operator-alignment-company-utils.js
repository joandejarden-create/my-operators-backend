/**
 * Operator Alignment Snapshot — company-level (Operating Companies for Consideration).
 * Wraps scoreOperatorMatchForDeal without modifying scoring weights/logic.
 */

import { scoreOperatorMatchForDeal } from "../api/my-deals.js";
import { buildOperatorNarrativePack } from "./operator-alignment-company-narratives.js";
import { buildOperatorAlignmentExecutiveSummary } from "./operator-alignment-executive-summary.js";
import { normalizeOperatorAlignmentDealInputs } from "./operator-alignment-deal-normalize.js";
import { buildOperatingPathDisplayLabel } from "./operator-alignment-operating-path-label.js";
import {
  buildDealContextFromMerged,
  mergeDealFieldsForAlignment,
  METHODOLOGY_NOTE,
  OAS_FEATURE_NAME,
} from "./operator-alignment-profile-utils.js";
import { formatListValue } from "../api/lib/third-party-operator-value-utils.js";
import {
  NEW_BASE_MASTER_TABLE,
  NEW_BASE_PROFILE_TABLE,
  NEW_BASE_PLATFORM_TABLE,
  NEW_BASE_COMMERCIAL_TABLE,
  NEW_BASE_GOVERNANCE_TABLE,
  fetchAllRecordsRest,
  buildPrefillObjectFromNewBaseRows,
  loadBrandNameByIdMap,
} from "../api/lib/operator-setup-new-base-read.js";

export const OAS_MODE_COMPANIES = "companies";
export const OAS_SECTION_COMPANIES = "Operating Companies for Consideration";

const ALIGNMENT_BANDS = [
  "Strong Alignment Signals",
  "Moderate Alignment Signals",
  "Conditional Alignment Signals",
  "Limited Alignment Signals",
  "Insufficient Data",
];

const COMPANY_METHODOLOGY_NOTE =
  "Company-level alignment in this snapshot is based on available Operator Setup profile data and does not indicate endorsement, availability, approval, or commercial terms.";

function toStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && Number.isFinite(v)) return String(v);
  if (Array.isArray(v)) return v.map((x) => toStr(x)).filter(Boolean).join(", ");
  return String(v).trim();
}

function toList(v) {
  if (v == null) return [];
  if (Array.isArray(v)) return v.map((x) => toStr(x)).filter(Boolean);
  const s = toStr(v);
  if (!s) return [];
  return s.split(/\s*,\s*/).map((x) => x.trim()).filter(Boolean);
}

function isExplorerTestOperatorName(name) {
  const n = String(name || "").toLowerCase();
  return (
    n.includes("e2e shadow") ||
    n.includes("e2e validation") ||
    n.includes("shadowval") ||
    n.includes("shadow test") ||
    n.includes("gold test") ||
    n.includes("shadow-validation") ||
    n.includes("example-operator-e2e")
  );
}

function isActiveSubmissionStatus(status) {
  return String(status || "").trim().toLowerCase() === "active";
}

function mapFirstLinkedByMasterLocal(rows) {
  const m = new Map();
  for (const r of rows || []) {
    const op = r.fields && r.fields.Operator;
    const mid = Array.isArray(op) && op[0] ? op[0] : null;
    if (mid && !m.has(mid)) m.set(mid, r);
  }
  return m;
}

/**
 * Load Active Operator Setup masters with linked rows for scoring (live Airtable only).
 * @returns {Promise<{ candidates: object[], airtableConfigured: boolean }>}
 */
export async function loadActiveOperatorCandidatesForAlignment() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    return { candidates: [], airtableConfigured: false };
  }

  const hideTest = String(process.env.OPERATOR_EXPLORER_HIDE_TEST_RECORDS || "").trim() === "1";

  const [masterRecords, profileRows, platformRows, commercialRows, governanceRows] = await Promise.all([
    fetchAllRecordsRest(NEW_BASE_MASTER_TABLE).catch(() => []),
    fetchAllRecordsRest(NEW_BASE_PROFILE_TABLE).catch(() => []),
    fetchAllRecordsRest(NEW_BASE_PLATFORM_TABLE).catch(() => []),
    fetchAllRecordsRest(NEW_BASE_COMMERCIAL_TABLE).catch(() => []),
    fetchAllRecordsRest(NEW_BASE_GOVERNANCE_TABLE).catch(() => []),
  ]);

  const profileByMaster = mapFirstLinkedByMasterLocal(profileRows);
  const platformByMaster = mapFirstLinkedByMasterLocal(platformRows);
  const commercialByMaster = mapFirstLinkedByMasterLocal(commercialRows);
  const governanceByMaster = mapFirstLinkedByMasterLocal(governanceRows);

  const candidates = [];
  for (const master of masterRecords || []) {
    if (!master.id || !String(master.id).startsWith("rec")) continue;
    const mf = master.fields || {};
    const status = formatListValue(mf.submission_status);
    if (!isActiveSubmissionStatus(status)) continue;

    const profile = profileByMaster.get(master.id) || null;
    const platform = platformByMaster.get(master.id) || null;
    const commercial = commercialByMaster.get(master.id) || null;
    const governance = governanceByMaster.get(master.id) || null;

    const companyName =
      formatListValue(mf.company_name || (profile && profile.fields && profile.fields.company_name)) || "";
    if (!companyName || companyName === "—") continue;
    if (hideTest && isExplorerTestOperatorName(companyName)) continue;

    candidates.push({
      operatorId: master.id,
      master,
      profile,
      platform,
      commercial,
      governance,
      companyName,
      dealStatus: status,
      headquarters: profile ? formatListValue(profile.fields.headquarters) : "",
      primaryServiceModel: profile ? formatListValue(profile.fields.primaryServiceModel) : "",
      regionsSupported: platform
        ? toList(platform.fields.regionsSupported || platform.fields.topMarkets)
        : [],
      chainScales: platform ? toList(platform.fields.chainScale) : [],
    });
  }

  return { candidates, airtableConfigured: true };
}

/**
 * @param {object} prefill
 * @param {{ profile: boolean, platform: boolean, commercial: boolean, governance: boolean }} links
 */
export function assessOperatorDataCompleteness(prefill, links = {}) {
  const gaps = [];
  const op = prefill || {};
  const markets = toList(
    op.activeCountries ||
      op.activeMarkets ||
      op.specificMarkets ||
      op.market_fit ||
      op.topMarkets ||
      op.regionsSupported ||
      op.bestFitGeographies
  );
  const scales = toList(op.chainScalesSupported || op.chainScale || op.chainScalesYouSupport || op.chain_scales);
  const services = toList(
    op.offeredServices || op.primaryServices || op.primary_services || op.serviceModels || op.service_models
  );
  const structures = toList(
    op.managementStructuresSupported || op.bestFitDealStructures || op.typicalAssignmentTypes || op.service_models
  );
  const company = toStr(op.companyName || op.company_name);

  if (!company) gaps.push("Company name is not available on the Operator Setup profile.");
  if (!links.profile) gaps.push("Operator Setup profile record is not linked.");
  if (!links.platform) gaps.push("Operator Setup platform & markets record is not linked.");
  if (markets.length === 0) gaps.push("Supported markets or regions are not documented.");
  if (scales.length === 0) gaps.push("Supported chain scales are not documented.");
  if (services.length === 0 && structures.length === 0) {
    gaps.push("Service models or management structures are not documented.");
  }

  let score = 0;
  if (company) score += 1;
  if (links.profile) score += 1;
  if (links.platform) score += 1;
  if (markets.length > 0) score += 1;
  if (scales.length > 0) score += 1;
  if (services.length > 0 || structures.length > 0) score += 1;
  if (links.commercial) score += 0.5;
  if (links.governance) score += 0.5;

  const maxScore = 7;
  const ratio = score / maxScore;

  let level = "insufficient";
  if (ratio >= 0.72 && markets.length > 0 && scales.length > 0) level = "sufficient";
  else if (ratio >= 0.45 && company) level = "partial";

  return {
    level,
    ratio: Math.round(ratio * 100) / 100,
    gaps,
    scorable: level === "sufficient" || (level === "partial" && markets.length > 0 && company),
  };
}

/**
 * @param {number|null} score
 * @param {{ scorable: boolean, level: string }} completeness
 */
export function scoreToCompanyAlignmentBand(score, completeness) {
  if (!completeness.scorable) return "Insufficient Data";
  if (score == null || Number.isNaN(Number(score))) return "Insufficient Data";
  const s = Number(score);
  if (s >= 80) return "Strong Alignment Signals";
  if (s >= 65) return "Moderate Alignment Signals";
  if (s >= 50) return "Conditional Alignment Signals";
  if (s >= 35) return "Limited Alignment Signals";
  return "Insufficient Data";
}

function bandSortRank(band) {
  const idx = ALIGNMENT_BANDS.indexOf(band);
  return idx >= 0 ? idx : 99;
}

/**
 * @param {Record<string, object>} breakdownDetails
 * @returns {string[]}
 */
export function alignmentSignalsFromBreakdown(breakdownDetails) {
  const signals = [];
  for (const [key, f] of Object.entries(breakdownDetails || {})) {
    if (!f) continue;
    if (f.missingDataClass === "needs_validation" && f.rationale) {
      signals.push(String(f.rationale));
      continue;
    }
    if (f.score === "—" || f.score == null) {
      if (f.rationale) signals.push(String(f.rationale));
      continue;
    }
    const num = Number(f.score);
    if (Number.isNaN(num)) continue;
    if (f.rationale && num >= 50) {
      signals.push(String(f.rationale));
      continue;
    }
    if (key === "negativeFitPenalty" && num < 50) {
      signals.push("Potential offsetting signal: deal breakers may overlap with operator less-ideal situations.");
      continue;
    }
    if (num >= 75) {
      signals.push("Potential alignment on " + String(f.label || key).toLowerCase() + " based on available inputs.");
    } else if (num >= 50) {
      signals.push("Conditional alignment signal on " + String(f.label || key).toLowerCase() + " — validation may be needed.");
    } else if (num < 50) {
      signals.push("Limited alignment signal on " + String(f.label || key).toLowerCase() + " with current inputs.");
    }
  }
  const deduped = [];
  const seen = new Set();
  for (const s of signals) {
    const t = String(s || "").trim();
    if (!t || seen.has(t)) continue;
    seen.add(t);
    deduped.push(t);
  }
  return deduped.slice(0, 6);
}

/**
 * @param {Record<string, object>} breakdownDetails
 * @returns {string[]}
 */
export function reviewConsiderationsFromBreakdown(breakdownDetails) {
  const out = [];
  for (const f of Object.values(breakdownDetails || {})) {
    if (!f || !f.note) continue;
    if (f.score === "—" || f.score == null) {
      out.push(String(f.label || "Factor") + ": data may be needed on both sides — " + String(f.note));
    } else if (Number(f.score) < 55) {
      out.push("Review " + String(f.label || "factor").toLowerCase() + " before advancing — " + String(f.note));
    }
  }
  return out.slice(0, 4);
}

function countScoredFactors(breakdownDetails) {
  let n = 0;
  for (const f of Object.values(breakdownDetails || {})) {
    if (f && f.score != null && f.score !== "—" && !Number.isNaN(Number(f.score))) n += 1;
  }
  return n;
}

/**
 * @param {object} candidate
 * @param {object} dealFields
 * @param {object|null} locationData
 * @param {object|null} mpData
 * @param {object|null} siData
 * @param {Map|null} brandNameById
 */
export function buildCompanyAlignmentResult(candidate, dealFields, locationData, mpData, siData, brandNameById) {
  const prefill = buildPrefillObjectFromNewBaseRows(
    candidate.master,
    candidate.profile,
    candidate.platform,
    candidate.commercial,
    candidate.governance
  );

  const links = {
    profile: Boolean(candidate.profile),
    platform: Boolean(candidate.platform),
    commercial: Boolean(candidate.commercial),
    governance: Boolean(candidate.governance),
  };

  const completeness = assessOperatorDataCompleteness(prefill, links);
  const parentCompany = toStr(
    prefill.parentCompany || prefill.parent_company || (candidate.master.fields || {}).parent_company
  );
  const serviceModels = toList(
    prefill.serviceModelsSupported ||
      prefill.serviceModels ||
      prefill.service_models ||
      prefill.typicalAssignmentTypes ||
      candidate.primaryServiceModel
  );
  const chainScales = toList(
    prefill.chainScalesSupported || prefill.chainScale || prefill.chainScalesYouSupport || candidate.chainScales
  );
  const countriesMarkets = toList(
    [].concat(
      toList(prefill.activeCountries),
      toList(prefill.activeMarkets),
      toList(prefill.specificMarkets),
      toList(prefill.regionsSupported),
      toList(prefill.topMarkets),
      toList(candidate.regionsSupported)
    )
  );

  if (!completeness.scorable) {
    return {
      operatorId: candidate.operatorId,
      operatorName: candidate.companyName,
      parentCompany: parentCompany || null,
      countriesMarkets,
      serviceModels,
      chainScales,
      alignmentBand: "Insufficient Data",
      alignmentScoreOptional: null,
      alignmentSignals: [],
      reviewConsiderations: [
        "Operator Setup profile may need additional markets, chain scale, or service model detail before company-level alignment can be assessed.",
      ],
      questionsToClarify: [
        "Which markets and chain scales should be confirmed on the Operator Setup profile?",
        "Are service models and deal structures documented for this operator?",
      ],
      dataGaps: completeness.gaps,
      dataCompleteness: completeness.level,
      sourceStatus: "incomplete",
      explanation:
        "Available Operator Setup data for this company is not complete enough to support a scored company-level alignment view.",
      suggestedWorkflowAction:
        "Request updated operator profile fields in Operator Setup before comparing this company side-by-side with the deal.",
      _sortScore: -1,
      _bandRank: 99,
    };
  }

  const { score, breakdownDetails } = scoreOperatorMatchForDeal(
    dealFields,
    locationData || {},
    mpData || {},
    siData || {},
    prefill,
    brandNameById
  );

  const scoredFactors = countScoredFactors(breakdownDetails);
  const band = scoreToCompanyAlignmentBand(score, completeness);
  const showNumeric =
    scoredFactors >= 3 && completeness.level === "sufficient" && band !== "Insufficient Data";

  const narrativePack = buildOperatorNarrativePack({
    companyName: candidate.companyName,
    operatorId: candidate.operatorId,
    prefill,
    masterFields: candidate.master?.fields || {},
    breakdownDetails,
    dealFields,
    locationData,
    mpData,
    siData,
    alignmentBand: band,
  });

  let alignmentSignals = narrativePack.alignmentSignals.length
    ? narrativePack.alignmentSignals
    : alignmentSignalsFromBreakdown(breakdownDetails);
  if (!alignmentSignals.length) {
    alignmentSignals.push(
      "Company-level alignment signals are limited with the current deal and operator profile inputs."
    );
  }

  let reviewConsiderations = narrativePack.reviewConsiderations.length
    ? narrativePack.reviewConsiderations
    : reviewConsiderationsFromBreakdown(breakdownDetails);
  if (!reviewConsiderations.length) {
    reviewConsiderations.push(
      "Validate geography, chain scale, and service scope with the operator before external sharing."
    );
  }

  const questionsToClarify = narrativePack.ownerQuestions.length
    ? narrativePack.ownerQuestions.slice(0, 4)
    : [];
  if (!questionsToClarify.length) {
    if (completeness.gaps.length) {
      questionsToClarify.push("Which missing Operator Setup fields should be completed first?");
    }
    questionsToClarify.push("Does this operator actively pursue deals with this structure and market profile?");
  }

  let sourceStatus = "live";
  if (completeness.level === "partial") sourceStatus = "needs review";
  if (band === "Insufficient Data") sourceStatus = "incomplete";

  return {
    operatorId: candidate.operatorId,
    operatorName: candidate.companyName,
    companyName: candidate.companyName,
    parentCompany: parentCompany || null,
    countriesMarkets,
    serviceModels,
    chainScales,
    alignmentBand: band,
    alignmentScoreOptional: showNumeric ? Math.round(Number(score) * 10) / 10 : null,
    alignmentSignals,
    reviewConsiderations,
    questionsToClarify,
    dataGaps: completeness.gaps,
    dataCompleteness: completeness.level,
    sourceStatus,
    explanation:
      "Alignment band is derived from weighted comparison of deal intake fields and this operator's Operator Setup profile. It does not indicate endorsement or commercial terms.",
    suggestedWorkflowAction:
      "Compare side-by-side with deal intake and clarify open items before any outreach or term discussions.",
    narrativePack,
    ownerFacingRationale: narrativePack.ownerFacingRationale,
    whatSupportsReview: narrativePack.whatSupportsReview,
    whatNeedsValidation: narrativePack.whatNeedsValidation,
    whatCouldWeakenAlignment: narrativePack.whatCouldWeakenAlignment,
    ownerQuestions: narrativePack.ownerQuestions,
    keyConsideration: narrativePack.keyConsideration,
    reviewStatusLabel: narrativePack.reviewStatusLabel,
    factorsReviewed: narrativePack.factorsReviewed,
    operatorStructuredProfile: narrativePack.operatorStructuredProfile,
    dataConfidenceLevel:
      narrativePack.operatorStructuredProfile?.dataConfidence || null,
    _sortScore: showNumeric ? Number(score) : 0,
    _bandRank: bandSortRank(band),
  };
}

function stripInternalCompanyFields(row) {
  const out = { ...row };
  delete out._sortScore;
  delete out._bandRank;
  return out;
}

/**
 * @param {string} dealId
 * @param {{ dealFields: object, locationData: object|null, mpData: object|null, siData: object|null }} scoringCtx
 */
export async function buildOperatorAlignmentCompaniesSnapshot(dealId, scoringCtx) {
  const minScorable = Number(process.env.OPERATOR_ALIGNMENT_MIN_SCORABLE_OPERATORS || 3);
  const maxCompanies = Number(process.env.OPERATOR_ALIGNMENT_MAX_COMPANIES || 25);
  const minRanked = Number.isFinite(minScorable) && minScorable > 0 ? minScorable : 3;
  const maxRank = Number.isFinite(maxCompanies) && maxCompanies > 0 ? maxCompanies : 25;

  const merged = mergeDealFieldsForAlignment(
    scoringCtx.dealFields,
    scoringCtx.locationData,
    scoringCtx.siData,
    scoringCtx.mpData
  );
  const dealContext = buildDealContextFromMerged(merged);

  const { candidates, airtableConfigured } = await loadActiveOperatorCandidatesForAlignment();
  const brandNameById = await loadBrandNameByIdMap().catch(() => new Map());

  const summary = {
    activeOperatorRecords: candidates.length,
    scorableOperators: 0,
    rankedCompanies: 0,
    airtableConfigured,
    minScorableRequired: minRanked,
  };

  if (!airtableConfigured) {
    return {
      dealId,
      generatedAt: new Date().toISOString(),
      featureName: OAS_FEATURE_NAME,
      mode: OAS_MODE_COMPANIES,
      sectionName: OAS_SECTION_COMPANIES,
      companiesAvailable: false,
      gatingReason:
        "Company-level operator alignment requires Airtable Operator Setup data. Configure AIRTABLE_BASE_ID and AIRTABLE_API_KEY to enable this section.",
      methodologyNote: COMPANY_METHODOLOGY_NOTE,
      dealContext,
      companiesForConsideration: [],
      dataCompletenessSummary: summary,
      dataGaps: ["Operator Setup connection is not configured in this environment."],
      suggestedWorkflowActions: [
        "Complete Operator Setup profiles for relevant management companies, then reopen this snapshot.",
      ],
    };
  }

  const built = [];
  for (const c of candidates.slice(0, maxRank * 2)) {
    const row = buildCompanyAlignmentResult(
      c,
      scoringCtx.dealFields,
      scoringCtx.locationData,
      scoringCtx.mpData,
      scoringCtx.siData,
      brandNameById
    );
    if (row.dataCompleteness !== "insufficient" && row.sourceStatus !== "incomplete") {
      summary.scorableOperators += 1;
    }
    built.push(row);
  }

  built.sort((a, b) => {
    if (a._bandRank !== b._bandRank) return a._bandRank - b._bandRank;
    return (b._sortScore || 0) - (a._sortScore || 0);
  });

  const ranked = built
    .filter((r) => r.alignmentBand !== "Insufficient Data" && r.sourceStatus === "live")
    .slice(0, maxRank)
    .map(stripInternalCompanyFields);

  summary.rankedCompanies = ranked.length;

  const companiesAvailable = summary.scorableOperators >= minRanked && ranked.length > 0;

  const dataGaps = [];
  const actions = [];
  if (!companiesAvailable) {
    dataGaps.push(
      "Fewer than " +
        minRanked +
        " active Operator Setup profiles have enough structured data for company-level alignment on this deal."
    );
    actions.push(
      "Request updated Operator Setup profiles (markets, chain scale, services) before comparing specific operating companies."
    );
  } else if (ranked.length < built.filter((r) => r._bandRank < 4).length) {
    dataGaps.push(
      "Some active operators were excluded because profile data was incomplete or alignment band was Insufficient Data."
    );
  }

  let gatingReason = null;
  if (!companiesAvailable) {
    gatingReason =
      summary.activeOperatorRecords < minRanked
        ? "Company-level operator alignment requires more active Operator Setup profiles before specific operating companies can be shown for this deal."
        : "Some operating companies may be relevant, but available profile data is not yet complete enough to support company-level alignment.";
  }

  const tableShownLimit = Number(process.env.OAS_TABLE_COMPANY_LIMIT || 8);
  const executiveSummary = buildOperatorAlignmentExecutiveSummary({
    dealContext,
    dealFields: scoringCtx.dealFields,
    locationData: scoringCtx.locationData,
    mpData: scoringCtx.mpData,
    siData: scoringCtx.siData,
    companiesAvailable,
    companiesForConsideration: companiesAvailable ? ranked : [],
    tableShownLimit: Number.isFinite(tableShownLimit) ? tableShownLimit : 8,
    profilePathwayCount: 0,
    activeOperatorRecords: summary.activeOperatorRecords,
  });

  const normalizedDeal = normalizeOperatorAlignmentDealInputs(
    scoringCtx.dealFields,
    scoringCtx.locationData,
    scoringCtx.mpData,
    scoringCtx.siData
  );

  return {
    dealId,
    generatedAt: new Date().toISOString(),
    featureName: OAS_FEATURE_NAME,
    mode: OAS_MODE_COMPANIES,
    sectionName: OAS_SECTION_COMPANIES,
    companiesAvailable,
    gatingReason,
    methodologyNote: COMPANY_METHODOLOGY_NOTE,
    dealContext,
    operatingPathLabel: buildOperatingPathDisplayLabel(normalizedDeal),
    companiesForConsideration: companiesAvailable ? ranked : [],
    operatorAlignmentExecutiveSummary: executiveSummary,
    operatorAlignmentSummaryParagraphs: executiveSummary.operatorAlignmentSummaryParagraphs,
    dataCompletenessSummary: summary,
    dataGaps,
    suggestedWorkflowActions: actions,
  };
}

export { ALIGNMENT_BANDS, COMPANY_METHODOLOGY_NOTE };
