import { airtableBasicsFieldsToPrefill, applyFootprintFieldsToPrefill } from "./third-party-operator-basics-to-prefill.js";
import { formatListValue, parseMultiValue } from "./third-party-operator-value-utils.js";

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

/**
 * Intake multi-select options use brand *names*; Airtable often stores linked record IDs.
 */
function resolvePrefillBrandsToNames(prefill, brandNameById) {
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
    brandBasicsRecords,
  ] = await Promise.all([
    fetchAllRecordsFromAirtable(CASE_STUDIES_TABLE).catch(() => []),
    fetchAllRecordsFromAirtable(OWNER_DILIGENCE_QA_TABLE).catch(() => []),
    fetchAllRecordsFromAirtable(DEAL_TERMS_TABLE).catch(() => []),
    fetchAllRecordsFromAirtable(FOOTPRINT_TABLE).catch(() => []),
    fetchAllRecordsFromAirtable("3rd Party Operator - Performance & Operations").catch(() => []),
    fetchAllRecordsFromAirtable("3rd Party Operator - Service Offerings").catch(() => []),
    fetchAllRecordsFromAirtable("3rd Party Operator - Ideal Projects & Deal Fit").catch(() => []),
    fetchAllRecordsFromAirtable("3rd Party Operator - Owner Relations & Communication").catch(() => []),
    fetchAllRecordsFromAirtable(BRAND_BASICS_TABLE).catch(() => []),
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
    brandBasicsRecords,
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
  } = ctx;
  const brandNameById = buildBrandNameByIdFromBasicsRecords(brandBasicsRecords);

  const caseStudies = caseStudyRecords
    .filter((r) => formatListValue((r.fields || {})["Operator Record ID"]) === recordId)
    .map((r) => {
      const row = r.fields || {};
      return {
        hotel_type: formatListValue(row["Hotel Type"]),
        region: formatListValue(row["Region"]),
        branded_independent: formatListValue(row["Branded / Independent"]),
        situation: formatListValue(row["Situation"]),
        services: formatListValue(row["Services"]),
        outcome: formatListValue(row["Outcome"]),
        owner_relevance: formatListValue(row["Owner Relevance"]),
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

  const ff = (footprint && footprint.fields) || {};
  const dtf = (dealTerms && dealTerms.fields) || {};
  const pf = (perfOps && perfOps.fields) || {};
  const sf = (services && services.fields) || {};
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

  const rm = parseMultiValue(sf["Revenue Management Services"]);
  if (rm.length) prefill.revenueManagementServices = rm;
  prefill.revenueManagementOther = str(sf["Revenue Management Other"]) || prefill.revenueManagementOther || "";
  const sm = parseMultiValue(sf["Sales Marketing Support"]);
  if (sm.length) prefill.salesMarketingSupport = sm;
  prefill.salesMarketingOther = str(sf["Sales Marketing Other"]) || prefill.salesMarketingOther || "";
  const ar = parseMultiValue(sf["Accounting Reporting"]);
  if (ar.length) prefill.accountingReporting = ar;
  prefill.accountingReportingOther = str(sf["Accounting Reporting Other"]) || prefill.accountingReportingOther || "";
  const proc = parseMultiValue(sf["Procurement Services"]);
  if (proc.length) prefill.procurementServices = proc;
  prefill.procurementServicesOther = str(sf["Procurement Services Other"]) || prefill.procurementServicesOther || "";
  const hr = parseMultiValue(sf["HR Training Services"]);
  if (hr.length) prefill.hrTrainingServices = hr;
  prefill.hrTrainingServicesOther = str(sf["HR Training Services Other"]) || prefill.hrTrainingServicesOther || "";
  const tech = parseMultiValue(sf["Technology Services"]);
  if (tech.length) prefill.technologyServices = tech;
  prefill.technologyServicesOther = str(sf["Technology Services Other"]) || prefill.technologyServicesOther || "";
  const des = parseMultiValue(sf["Design Renovation Support"]);
  if (des.length) prefill.designRenovationSupport = des;
  prefill.designRenovationSupportOther = str(sf["Design Renovation Support Other"]) || prefill.designRenovationSupportOther || "";
  const dev = parseMultiValue(sf["Development Services"]);
  if (dev.length) prefill.developmentServices = dev;
  prefill.developmentServicesOther = str(sf["Development Services Other"]) || prefill.developmentServicesOther || "";
  prefill.serviceDifferentiators = str(sf["Service Offering Summary"]) || prefill.serviceDifferentiators || "";

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

  resolvePrefillBrandsToNames(prefill, brandNameById);

  return { prefill, caseStudies, ownerDiligenceQa };
}

/**
 * @param {{ id: string, fields?: object }} operatorRecord Basics row
 */
export async function buildThirdPartyOperatorPrefill(operatorRecord) {
  const ctx = await fetchThirdPartyOperatorPrefillContext();
  return buildThirdPartyOperatorPrefillFromContext(operatorRecord, ctx);
}

export { TABLE_NAME as THIRD_PARTY_OPERATOR_BASICS_TABLE };
