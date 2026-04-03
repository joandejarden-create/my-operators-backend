import { airtableBasicsFieldsToPrefill, applyFootprintFieldsToPrefill } from "./third-party-operator-basics-to-prefill.js";
import { applyNewTwoPrefillFromSplitTables } from "./third-party-operator-new-two-fields.js";
import { formatListValue, parseMultiValue } from "./third-party-operator-value-utils.js";
import { applyOperatorServiceGranularPrefill } from "./operator-setup-service-granular-fields.js";
import {
  normalizeCaseStudySituationForForm,
  normalizeOperatorSetupSelectPrefill,
} from "./third-party-operator-select-prefill-normalize.js";
import { NEW_BASE_GOVERNANCE_TABLE } from "./operator-setup-new-base-read.js";

export { formatListValue, parseMultiValue, safeParseJsonArray } from "./third-party-operator-value-utils.js";

const TABLE_NAME = process.env.AIRTABLE_THIRD_PARTY_OPERATORS_TABLE || "3rd Party Operator - Basics";
const FOOTPRINT_TABLE =
  process.env.AIRTABLE_THIRD_PARTY_OPERATOR_FOOTPRINT_TABLE || "3rd Party Operator - Footprint";
const BRAND_BASICS_TABLE =
  process.env.AIRTABLE_BRAND_SETUP_BASICS_TABLE || "Brand Setup - Brand Basics";
const CASE_STUDIES_TABLE =
  process.env.AIRTABLE_THIRD_PARTY_OPERATOR_CASE_STUDIES_TABLE || "3rd Party Operator - Case Studies";
const OWNER_DILIGENCE_QA_TABLE =
  process.env.AIRTABLE_THIRD_PARTY_OPERATOR_OWNER_DILIGENCE_QA_TABLE || "3rd Party Operator - Owner Diligence QA";
const DEAL_TERMS_TABLE =
  process.env.AIRTABLE_THIRD_PARTY_OPERATOR_DEAL_TERMS_TABLE || "3rd Party Operator - Deal Terms & Fees";
const REPRESENTATIVE_PROPERTIES_TABLE =
  process.env.AIRTABLE_THIRD_PARTY_OPERATOR_REPRESENTATIVE_PROPERTIES_TABLE ||
  "3rd Party Operator - Representative Properties";
const LEADERSHIP_TEAM_TABLE =
  process.env.AIRTABLE_THIRD_PARTY_OPERATOR_LEADERSHIP_TEAM_TABLE ||
  "3rd Party Operator - Leadership Team";

function extractLeadingNumber(val) {
  const s = formatListValue(val);
  if (!s) return "";
  const m = String(s).match(/-?\d*\.?\d+/);
  return m ? m[0] : "";
}

function extractIntegerString(val) {
  const s = formatListValue(val);
  if (!s) return "";
  const m = String(s).match(/-?\d+/);
  return m ? m[0] : "";
}

function normalizeEsgReporting(raw) {
  const esgRaw = formatListValue(raw);
  if (!esgRaw) return "";
  const normalized = String(esgRaw).trim();
  if (normalized === "true" || normalized === "Yes") return "Yes - Annual";
  if (normalized === "false" || normalized === "No") return "No";
  return normalized;
}

function isLikelyAirtableRecordId(s) {
  return typeof s === "string" && /^rec[a-zA-Z0-9]{14,}$/.test(s.trim());
}

function normalizeKey(k) {
  return String(k || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function readFieldByNormalizedName(fields, candidates) {
  const source = fields || {};
  for (const [k, v] of Object.entries(source)) {
    if (candidates.has(normalizeKey(k))) return v;
  }
  return undefined;
}

function toEpochMs(createdTime) {
  if (!createdTime) return 0;
  const ms = Date.parse(String(createdTime));
  return Number.isFinite(ms) ? ms : 0;
}

/** Brand Basics record id → Brand Name (for linked "Brands Managed" on operator rows). */
function buildBrandNameByIdFromBasicsRecords(records) {
  const m = new Map();
  for (const rec of records || []) {
    const nm = formatListValue((rec.fields || {})["Brand Name"]);
    if (rec.id && nm) m.set(rec.id, nm);
  }
  return m;
}

function buildBrandLogoByNameFromBasicsRecords(records) {
  const m = new Map();
  for (const rec of records || []) {
    const f = rec.fields || {};
    const nm = formatListValue(f["Brand Name"]).trim();
    if (!nm) continue;
    const logoField = f["Brand Logo"] || f["Logo"] || f["Company Logo"] || null;
    const logoUrl =
      Array.isArray(logoField) && logoField[0] && logoField[0].url
        ? String(logoField[0].url).trim()
        : "";
    if (logoUrl) m.set(nm.toLowerCase(), logoUrl);
  }
  return m;
}

/**
 * Intake multi-select options use brand *names*; Airtable often stores linked record IDs.
 */
export function resolvePrefillBrandsToNames(prefill, brandNameById) {
  const raw = prefill && prefill.brands;
  if (raw == null) return;
  const arr = Array.isArray(raw) ? raw : parseMultiValue(String(raw));
  const out = [];
  const seen = new Set();
  for (const item of arr) {
    const s = formatListValue(item).trim();
    if (!s) continue;
    const label = isLikelyAirtableRecordId(s) ? brandNameById.get(s) || s : s;
    if (!label || seen.has(label)) continue;
    seen.add(label);
    out.push(label);
  }
  if (out.length) prefill.brands = out;
}

export async function fetchAllRecordsFromAirtable(tableName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    const err = new Error("Airtable not configured");
    err.statusCode = 503;
    throw err;
  }

  const tableSegment = encodeURIComponent(tableName);
  const allRecords = [];
  let offset = null;

  do {
    let url = `https://api.airtable.com/v0/${baseId}/${tableSegment}?pageSize=100`;
    if (offset) url += "&offset=" + encodeURIComponent(offset);
    const pageRes = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const pageData = await pageRes.json().catch(() => ({}));
    if (!pageRes.ok || pageData.error) {
      const err = new Error(
        (pageData.error && pageData.error.message) || pageRes.statusText || "Airtable API error"
      );
      err.statusCode = pageRes.status;
      throw err;
    }
    allRecords.push(...(pageData.records || []));
    offset = pageData.offset || null;
  } while (offset);

  return allRecords;
}

export async function fetchThirdPartyOperatorPrefillContext() {
  const [
    caseStudyRecords,
    ownerQaRecords,
    dealTermsRecords,
    footprintRecords,
    perfOpsRecords,
    servicesRecords,
    idealRecords,
    ownerRelRecords,
    representativePropertiesRecords,
    leadershipTeamRecords,
    brandBasicsRecords,
    governanceRecords,
  ] = await Promise.all([
    fetchAllRecordsFromAirtable(CASE_STUDIES_TABLE).catch(() => []),
    fetchAllRecordsFromAirtable(OWNER_DILIGENCE_QA_TABLE).catch(() => []),
    fetchAllRecordsFromAirtable(DEAL_TERMS_TABLE).catch(() => []),
    fetchAllRecordsFromAirtable(FOOTPRINT_TABLE).catch(() => []),
    fetchAllRecordsFromAirtable("3rd Party Operator - Performance & Operations").catch(() => []),
    fetchAllRecordsFromAirtable("3rd Party Operator - Service Offerings").catch(() => []),
    fetchAllRecordsFromAirtable("3rd Party Operator - Ideal Projects & Deal Fit").catch(() => []),
    fetchAllRecordsFromAirtable("3rd Party Operator - Owner Relations & Communication").catch(() => []),
    fetchAllRecordsFromAirtable(REPRESENTATIVE_PROPERTIES_TABLE).catch(() => []),
    fetchAllRecordsFromAirtable(LEADERSHIP_TEAM_TABLE).catch(() => []),
    fetchAllRecordsFromAirtable(BRAND_BASICS_TABLE).catch(() => []),
    fetchAllRecordsFromAirtable(NEW_BASE_GOVERNANCE_TABLE).catch(() => []),
  ]);
  return {
    caseStudyRecords,
    ownerQaRecords,
    dealTermsRecords,
    footprintRecords,
    perfOpsRecords,
    servicesRecords,
    idealRecords,
    ownerRelRecords,
    representativePropertiesRecords,
    leadershipTeamRecords,
    brandBasicsRecords,
    governanceRecords,
  };
}

/** @param {{ id: string, fields?: object }} operatorRecord Basics row */
export function buildThirdPartyOperatorPrefillFromContext(operatorRecord, ctx) {
  const recordId = operatorRecord.id;
  const f = operatorRecord.fields || {};
  const companyName = formatListValue(f["Company Name"]).trim();

  const {
    caseStudyRecords,
    ownerQaRecords,
    dealTermsRecords,
    footprintRecords,
    perfOpsRecords,
    servicesRecords,
    idealRecords,
    ownerRelRecords,
    brandBasicsRecords,
    governanceRecords,
  } = ctx;
  const brandNameById = buildBrandNameByIdFromBasicsRecords(brandBasicsRecords);
  const brandLogoByName = buildBrandLogoByNameFromBasicsRecords(brandBasicsRecords);

  const caseStudies = caseStudyRecords
    .filter((r) => formatListValue((r.fields || {})["Operator Record ID"]) === recordId)
    .map((r) => {
      const row = r.fields || {};
      return {
        property_name: formatListValue(row["Property Name"]),
        hotel_type: formatListValue(row["Hotel Type"]),
        region: formatListValue(row["Region"]),
        branded_independent: formatListValue(row["Branded / Independent"]),
        situation: normalizeCaseStudySituationForForm(formatListValue(row["Situation"])),
        services: formatListValue(row["Services"]),
        outcome: formatListValue(row["Outcome"]),
        owner_relevance: formatListValue(row["Owner Relevance"]),
        image_url: formatListValue(row["Image URL"]),
      };
    });

  const ownerDiligenceQa = ownerQaRecords
    .filter((r) => formatListValue((r.fields || {})["Operator Record ID"]) === recordId)
    .map((r) => {
      const row = r.fields || {};
      return {
        category: formatListValue(row["Category"]),
        question: formatListValue(row["Question"]),
        answer: formatListValue(row["Answer"]),
      };
    });

  const byLink = (rows) => {
    let best = null;
    let bestScore = -1;
    let bestCreatedMs = -1;

    for (const r of rows || []) {
      const rf = r.fields || {};
      const links = readFieldByNormalizedName(rf, new Set(["operator (basics link)", "operator basics link"]));
      const hasDirectLink =
        (Array.isArray(links) && links.includes(recordId)) ||
        (typeof links === "string" && links.trim() === recordId);

      // Backward compatibility for split-table rows that store direct record id.
      const opRecordId = formatListValue(
        readFieldByNormalizedName(rf, new Set(["operator record id", "operatorrecordid"]))
      ).trim();
      const hasRecordId = !!opRecordId && opRecordId === recordId;

      // Last-resort match for older data: same operator company label.
      const opName = formatListValue(readFieldByNormalizedName(rf, new Set(["operator", "company name"]))).trim();
      const hasCompanyMatch = !!companyName && !!opName && opName.toLowerCase() === companyName.toLowerCase();

      // Prefer deterministic IDs. Company name is fallback only.
      let score = 0;
      if (hasDirectLink) score = 100;
      else if (hasRecordId) score = 90;
      else if (hasCompanyMatch) score = 10;
      if (score <= 0) continue;

      const createdMs = toEpochMs(r.createdTime);
      if (score > bestScore || (score === bestScore && createdMs > bestCreatedMs)) {
        best = r;
        bestScore = score;
        bestCreatedMs = createdMs;
      }
    }

    return best || null;
  };

  const footprint = byLink(footprintRecords);
  const dealTerms = byLink(dealTermsRecords);
  const perfOps = byLink(perfOpsRecords);
  const services = byLink(servicesRecords);
  const ideal = byLink(idealRecords);
  const ownerRel = byLink(ownerRelRecords);
  const governance = byLink(governanceRecords || []);

  const ff = (footprint && footprint.fields) || {};
  const dtf = (dealTerms && dealTerms.fields) || {};
  const pf = (perfOps && perfOps.fields) || {};
  const sf = (services && services.fields) || {};
  const gf = (governance && governance.fields) || {};
  const ifields = (ideal && ideal.fields) || {};
  const of = (ownerRel && ownerRel.fields) || {};

  const prefill = { ...airtableBasicsFieldsToPrefill(f) };
  // Overlay split-table Deal Terms first, then Footprint and other sections.
  applyFootprintFieldsToPrefill(prefill, dtf);
  applyFootprintFieldsToPrefill(prefill, ff);
  prefill.esgReporting = normalizeEsgReporting(prefill.esgReporting ?? f["ESG Reporting"]);

  const str = (v) => formatListValue(v);

  prefill.contactName = str(of["Contact Name"]) || prefill.contactName || "";
  prefill.contactEmail = str(of["Primary Contact Email"]) || prefill.contactEmail || "";
  prefill.contactPhone = str(of["Primary Contact Phone"]) || prefill.contactPhone || "";
  prefill.preferredContactMethod = str(of["Preferred Contact Method"]) || prefill.preferredContactMethod || "";
  prefill.ownerInvolvement = str(of["Owner Involvement Level"]) || prefill.ownerInvolvement || "";

  prefill.communicationStyle = str(of["Owner Communication Style"]) || prefill.communicationStyle || "";
  prefill.operatingCollaborationMode =
    str(of["Operating Collaboration Mode"]) || prefill.operatingCollaborationMode || "";
  prefill.ownerResponseTime =
    str(of["Typical Response Time for Owner Inquiries"]) || prefill.ownerResponseTime || "";
  prefill.decisionMaking = str(of["Decision-Making Process"]) || prefill.decisionMaking || "";
  prefill.disputeResolution = str(of["Dispute Resolution Approach"]) || prefill.disputeResolution || "";
  prefill.concernResolutionTime =
    str(of["Average Time to Resolve Owner Concerns"]) || prefill.concernResolutionTime || "";
  prefill.ownerAdvisoryBoard = str(of["Owner Advisory Board"]) || prefill.ownerAdvisoryBoard || "";
  prefill.ownerEducation = str(of["Owner Education/Training Provided"]) || prefill.ownerEducation || "";
  const ownerRefAvail = of["Owner References Available"];
  if (ownerRefAvail != null && String(ownerRefAvail).trim() !== "" && !prefill.ownerReferences) {
    prefill.ownerReferences = String(ownerRefAvail).trim();
  }
  prefill.testimonialLinks = str(of["Testimonial Links"]) || prefill.testimonialLinks || "";
  prefill.industryRecognition = str(of["Industry Recognition"]) || prefill.industryRecognition || "";
  const ownerNps = of["Owner Satisfaction Score (NPS)"];
  if (ownerNps != null && String(ownerNps).trim() !== "" && !prefill.ownerSatisfactionScore) {
    prefill.ownerSatisfactionScore = String(ownerNps).trim();
  }
  prefill.lenderReferences = str(of["Lender References Available"]) || prefill.lenderReferences || "";
  prefill.majorLenders = str(of["Major Lenders Worked With"]) || prefill.majorLenders || "";
  prefill.specializations = str(of["Specializations"]) || prefill.specializations || "";
  prefill.testimonials = str(of["Key Owner Success Stories"]) || prefill.testimonials || "";
  prefill.ownerPortalFeatures = str(of["Owner Portal Features"]) || prefill.ownerPortalFeatures || "";
  prefill.additionalNotes = str(of["Additional Notes"]) || prefill.additionalNotes || "";

  // New Basics fields for Operator Explorer normalization
  const bestFitAssetTypes = parseMultiValue(f["Best Fit Asset Types"]);
  if (bestFitAssetTypes.length) prefill.bestFitAssetTypes = bestFitAssetTypes;
  const bestFitGeographies = parseMultiValue(f["Best Fit Geographies"]);
  if (bestFitGeographies.length) prefill.bestFitGeographies = bestFitGeographies;
  const bestFitOwnerTypes = parseMultiValue(f["Best Fit Owner Types"]);
  if (bestFitOwnerTypes.length) prefill.bestFitOwnerTypes = bestFitOwnerTypes;
  const bestFitDealStructures = parseMultiValue(f["Best Fit Deal Structures"]);
  if (bestFitDealStructures.length) prefill.bestFitDealStructures = bestFitDealStructures;
  const typicalAssignmentTypes = parseMultiValue(f["Typical Assignment Types"]);
  if (typicalAssignmentTypes.length) prefill.typicalAssignmentTypes = typicalAssignmentTypes;
  prefill.lessIdealSituations = str(f["Less Ideal Situations"]) || prefill.lessIdealSituations || "";
  prefill.ownerValueProposition = str(f["Owner Value Proposition"]) || prefill.ownerValueProposition || "";
  prefill.ownerReportingCadence = str(f["Owner Reporting Cadence"]) || prefill.ownerReportingCadence || "";
  if (f["KPI Dashboard Provided"] != null && String(f["KPI Dashboard Provided"]).trim() !== "") {
    prefill.kpiDashboardProvided = !!f["KPI Dashboard Provided"];
  }
  prefill.budgetForecastReportingDiscipline =
    str(f["Budget / Forecast Reporting Discipline"]) || prefill.budgetForecastReportingDiscipline || "";
  prefill.capitalPlanningSupport = str(f["Capital Planning Support"]) || prefill.capitalPlanningSupport || "";
  prefill.franchiseCompatibleExperience =
    str(f["Franchise-Compatible Experience"]) || prefill.franchiseCompatibleExperience || "";
  prefill.softBrandExperience = str(f["Soft Brand Experience"]) || prefill.softBrandExperience || "";
  prefill.independentCollectionExperience =
    str(f["Independent Collection Experience"]) || prefill.independentCollectionExperience || "";
  prefill.brandStandardsFlexibility = str(f["Brand Standards Flexibility"]) || prefill.brandStandardsFlexibility || "";

  prefill.totalProperties =
    str(pf["Total Properties Managed"]) || str(f["Total Properties Managed"]) || prefill.totalProperties || "";
  prefill.totalRooms = str(pf["Total Rooms Managed"]) || str(f["Total Rooms Managed"]) || prefill.totalRooms || "";
  prefill.portfolioMetricsAsOf = str(pf["Portfolio Metrics As of Date"]) || prefill.portfolioMetricsAsOf || "";

  const revparPf = str(pf["Average RevPAR Improvement"]);
  prefill.revparImprovement = revparPf || prefill.revparImprovement || "";
  prefill.occupancyImprovement = str(pf["Average Occupancy Improvement"]) || prefill.occupancyImprovement || "";
  prefill.noiImprovement = str(pf["Average NOI Improvement"]) || prefill.noiImprovement || "";
  prefill.ownerRetention = str(pf["Owner Retention Rate"]) || prefill.ownerRetention || "";
  prefill.renewalRate = str(pf["Average Contract Renewal Rate"]) || prefill.renewalRate || "";

  const pgPf = extractLeadingNumber(pf["Portfolio Growth Rate"]);
  prefill.portfolioGrowthRate = pgPf || prefill.portfolioGrowthRate || "";

  const turnPf = extractIntegerString(pf["Properties Turned Around"]);
  prefill.turnaroundCount = turnPf || prefill.turnaroundCount || "";

  prefill.stabilizationTime = str(pf["Time to Stabilization"]) || prefill.stabilizationTime || "";
  prefill.reportingFrequency =
    str(pf["Financial Reporting Frequency"]) || str(f["Reporting Frequency"]) || prefill.reportingFrequency || "";

  const reportTypesPf = parseMultiValue(pf["Report Types Provided"]);
  if (reportTypesPf.length) prefill.reportTypes = reportTypesPf;

  prefill.budgetProcess = str(pf["Budget Process"]) || prefill.budgetProcess || "";
  prefill.capexPlanning = str(pf["Capital Expenditure Planning"]) || prefill.capexPlanning || "";
  prefill.performanceReviews = str(pf["Performance Review Meetings"]) || prefill.performanceReviews || "";
  prefill.primaryPMS = str(pf["Primary PMS System"]) || prefill.primaryPMS || "";
  prefill.revenueManagementSystem = str(pf["Revenue Management System"]) || prefill.revenueManagementSystem || "";
  prefill.accountingSystem = str(pf["Accounting System"]) || prefill.accountingSystem || "";
  prefill.guestCommunication = str(pf["Guest Communication Platform"]) || prefill.guestCommunication || "";
  prefill.mobileCheckin = str(pf["Mobile Check-in Capability"]) || prefill.mobileCheckin || "";
  prefill.ownerPortal = str(pf["Owner Portal"]) || prefill.ownerPortal || "";
  prefill.analyticsPlatform = str(pf["Data Analytics Platform"]) || prefill.analyticsPlatform || "";
  prefill.apiIntegrations = str(pf["API Integrations"]) || prefill.apiIntegrations || "";
  prefill.internalControlsSummary = str(pf["Internal Controls Summary"]) || prefill.internalControlsSummary || "";
  prefill.escalationProtocol = str(pf["Escalation Protocol"]) || prefill.escalationProtocol || "";
  prefill.complianceReviewProcess = str(pf["Compliance Review Process"]) || prefill.complianceReviewProcess || "";
  prefill.auditQaProcess = str(pf["Audit / QA Process"]) || prefill.auditQaProcess || "";
  prefill.incidentReportingProcess = str(pf["Incident Reporting Process"]) || prefill.incidentReportingProcess || "";

  applyOperatorServiceGranularPrefill({ ...sf, ...gf }, prefill);
  prefill.serviceDifferentiators = str(sf["Service Offering Summary"]) || prefill.serviceDifferentiators || "";
  prefill.procurementSupport = str(sf["Procurement Support"]) || prefill.procurementSupport || "";
  prefill.trainingPlatformSopSupport =
    str(sf["Training Platform / SOP Support"]) || prefill.trainingPlatformSopSupport || "";
  prefill.transitionTaskforce = str(sf["Transition Taskforce"]) || prefill.transitionTaskforce || "";

  // New Footprint fields used by New Overview markets section
  const countriesServed = parseMultiValue(ff["Countries Served"]);
  if (countriesServed.length) prefill.countriesServed = countriesServed;
  prefill.citiesServed = str(ff["Cities Served"]) || prefill.citiesServed || "";
  const priorityCountries = parseMultiValue(ff["Priority Countries"]);
  if (priorityCountries.length) prefill.priorityCountries = priorityCountries;
  prefill.priorityCities = str(ff["Priority Cities"]) || prefill.priorityCities || "";
  prefill.resortMarketExperience = str(ff["Resort Market Experience"]) || prefill.resortMarketExperience || "";
  prefill.urbanMarketExperience = str(ff["Urban Market Experience"]) || prefill.urbanMarketExperience || "";
  prefill.crossBorderOperatingExperience =
    str(ff["Cross-Border Operating Experience"]) || prefill.crossBorderOperatingExperience || "";
  prefill.marketEntryExperience = str(ff["Market Entry Experience"]) || prefill.marketEntryExperience || "";

  const ipt = parseMultiValue(ifields["Acceptable Project Types"]);
  if (ipt.length) prefill.idealProjectTypes = ipt;
  const ibt = parseMultiValue(ifields["Acceptable Building Types"]);
  if (ibt.length) prefill.idealBuildingTypes = ibt;
  const iat = parseMultiValue(ifields["Acceptable Agreement Types"]);
  if (iat.length) prefill.idealAgreementTypes = iat;
  const ips = parseMultiValue(ifields["Acceptable Project Stages"]);
  if (ips.length) prefill.projectStage = ips;

  prefill.idealRoomCountMin = str(ifields["Ideal Room Count Min"]) || prefill.idealRoomCountMin || "";
  prefill.idealRoomCountMax = str(ifields["Ideal Room Count Max"]) || prefill.idealRoomCountMax || "";
  prefill.idealProjectSizeMin = str(ifields["Ideal Project Size Min"]) || prefill.idealProjectSizeMin || "";
  prefill.idealProjectSizeMax = str(ifields["Ideal Project Size Max"]) || prefill.idealProjectSizeMax || "";
  prefill.minLeadTimeMonths = str(ifields["Min Lead Time Months"]) || prefill.minLeadTimeMonths || "";
  prefill.preferredOwnerType = str(ifields["Preferred Owner Type"]) || prefill.preferredOwnerType || "";
  prefill.coBrandingAllowed = str(ifields["Co-Branding Allowed"]) || prefill.coBrandingAllowed || "";
  prefill.brandedResidencesAllowed = str(ifields["Branded Residences Allowed"]) ? "Yes" : prefill.brandedResidencesAllowed || "";
  prefill.mixedUseAllowed = str(ifields["Mixed-Use Development Allowed"]) ? "Yes" : prefill.mixedUseAllowed || "";
  const pm = parseMultiValue(ifields["Priority Markets"]);
  if (pm.length) prefill.priorityMarkets = pm;
  prefill.priorityMarketsOther = str(ifields["Priority Markets Other"]) || prefill.priorityMarketsOther || "";
  const ma = parseMultiValue(ifields["Markets To Avoid"]);
  if (ma.length) prefill.marketsToAvoid = ma;
  prefill.marketsToAvoidOther = str(ifields["Markets To Avoid Other"]) || prefill.marketsToAvoidOther || "";

  prefill.milestoneOperatorSelectionMinMonths =
    str(ifields["Milestone Operator Selection Min Months"]) || prefill.milestoneOperatorSelectionMinMonths || "";
  prefill.milestoneConstructionStartMinMonths =
    str(ifields["Milestone Construction Start Min Months"]) || prefill.milestoneConstructionStartMinMonths || "";
  prefill.milestoneSoftOpeningMinMonths =
    str(ifields["Milestone Soft Opening Min Months"]) || prefill.milestoneSoftOpeningMinMonths || "";
  prefill.milestoneGrandOpeningMinMonths =
    str(ifields["Milestone Grand Opening Min Months"]) || prefill.milestoneGrandOpeningMinMonths || "";
  prefill.dateFlexibility = str(ifields["Date Flexibility"]) || prefill.dateFlexibility || "";
  const bs = parseMultiValue(ifields["Brand Status Scenarios"]);
  if (bs.length) prefill.brandStatus = bs;
  prefill.pipRepositioningDetails =
    str(ifields["Typical PIP / Repositioning Profile You Will Consider (If Existing Hotel)"]) ||
    str(ifields["Operator Role in Brand Selection"]) ||
    prefill.pipRepositioningDetails ||
    "";

  const ohe = parseMultiValue(ifields["Owner / Sponsor Hotel Experience"]);
  if (ohe.length) prefill.ownerHotelExperience = ohe;
  const oil = parseMultiValue(ifields["Acceptable Owner Involvement Levels"]);
  if (oil.length) prefill.ownerInvolvementLevel = oil;
  const ont = parseMultiValue(ifields["Owner Non-Negotiables (Types)"]);
  if (ont.length) prefill.ownerNonNegotiableTypes = ont;
  prefill.ownerNonNegotiables =
    str(ifields["Owner Non-Negotiables & Decision Rights"]) || prefill.ownerNonNegotiables || "";
  const fem = parseMultiValue(ifields["Acceptable Fee Expectations vs Market"]);
  if (fem.length) prefill.feeExpectationVsMarket = fem;
  prefill.capexSupport = str(ifields["CapEx and FF&E Support"]) || prefill.capexSupport || "";
  const exh = parseMultiValue(ifields["Acceptable Exit Horizon"]);
  if (exh.length) prefill.exitHorizon = exh;
  const capEng = parseMultiValue(ifields["Acceptable Capital Status at Engagement "]);
  if (capEng.length) prefill.capitalStatus = capEng;
  prefill.knownRedFlags =
    str(ifields["Red Flag Items That Typically Make You Decline or Proceed With Caution"]) ||
    prefill.knownRedFlags ||
    "";
  prefill.esgExpectations =
    str(ifields["ESG / Sustainability Expectations You Prefer Projects to Meet"]) || prefill.esgExpectations || "";
  prefill.idealProjectsAdditionalNotes =
    str(ifields["Anything else about your commercial 'sweet spot' we should know?"]) ||
    str(ifields["Ideal Projects Additional Notes"]) ||
    prefill.idealProjectsAdditionalNotes ||
    "";

  prefill.caseStudiesDetail = caseStudies;
  prefill.ownerDiligenceQa = ownerDiligenceQa;

  const explorerFieldName = process.env.AIRTABLE_BASICS_EXPLORER_PROFILE_JSON_FIELD || "Explorer Profile JSON";
  const explorerRaw = readFieldByNormalizedName(f, new Set([normalizeKey(explorerFieldName)]));
  if (explorerRaw != null && String(explorerRaw).trim() !== "") {
    prefill.explorerProfileJson = String(explorerRaw).trim();
  }

  applyNewTwoPrefillFromSplitTables(prefill, { f, pf, sf, ff, ifields, of, dtf });

  resolvePrefillBrandsToNames(prefill, brandNameById);
  const brandProfiles = buildBrandProfilesFromPrefill(prefill, brandBasicsRecords, brandLogoByName);

  normalizeOperatorSetupSelectPrefill(prefill);

  return { prefill, caseStudies, ownerDiligenceQa, brandProfiles };
}

/** Resolved brand names → `{ name, logoUrl }` for detail API (Basics or new-base prefill). */
export function buildBrandProfilesFromPrefill(prefill, brandBasicsRecords, brandLogoByNameOverride) {
  const brandLogoByName =
    brandLogoByNameOverride || buildBrandLogoByNameFromBasicsRecords(brandBasicsRecords);
  return (Array.isArray(prefill.brands) ? prefill.brands : [])
    .map((name) => {
      const n = formatListValue(name).trim();
      if (!n) return null;
      return {
        name: n,
        logoUrl: brandLogoByName.get(n.toLowerCase()) || "",
      };
    })
    .filter(Boolean);
}

/**
 * @param {{ id: string, fields?: object }} operatorRecord Basics row
 */
export async function buildThirdPartyOperatorPrefill(operatorRecord) {
  const ctx = await fetchThirdPartyOperatorPrefillContext();
  return buildThirdPartyOperatorPrefillFromContext(operatorRecord, ctx);
}

export { TABLE_NAME as THIRD_PARTY_OPERATOR_BASICS_TABLE };
export { REPRESENTATIVE_PROPERTIES_TABLE as THIRD_PARTY_OPERATOR_REPRESENTATIVE_PROPERTIES_TABLE };
export { LEADERSHIP_TEAM_TABLE as THIRD_PARTY_OPERATOR_LEADERSHIP_TEAM_TABLE };
