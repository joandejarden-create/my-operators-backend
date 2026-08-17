/**
 * Project Fit engagement multi-selects: stages, owner involvement, capital, fees, exit.
 * Plus re-export agreement types from brand-project-agreement-types-profiles.
 */
import { getBrandProjectAndAgreementTypes } from "./brand-project-agreement-types-profiles.mjs";
import {
  AMENITY_SEGMENTS,
  PARENT_AMENITY_SEGMENT,
  BRAND_AMENITY_OVERRIDES,
} from "./brand-additional-amenities-profiles.mjs";

export const PROJECT_STAGES_ALLOWED = Object.freeze([
  "Land Under Control Only",
  "Entitlements in Process",
  "Fully Entitled",
  "Under Construction",
  "Stabilized Operating Asset",
]);

export const OWNER_INVOLVEMENT_ALLOWED = Object.freeze([
  "Silent Investor",
  "High-Level Oversight Only",
  "Hands-On in Operations",
  "Family in Key Staff Roles",
  "Moderate",
  "Hands-on on design and capex",
]);

export const CAPITAL_STATUS_ALLOWED = Object.freeze([
  "Equity and Debt Fully Committed",
  "Equity Committed, Debt in Process",
  "Equity in Process, Debt Not Started",
  "Both Equity and Debt Still Being Raised",
]);

export const FEE_EXPECTATIONS_ALLOWED = Object.freeze([
  "Below Typical Market Fees",
  "In Line with Market Fees",
  "Premium for Complexity / Performance",
  "Open to Incentive Structures / Performance Fees",
]);

export const EXIT_HORIZON_ALLOWED = Object.freeze([
  "Hold 10+ Years",
  "Hold 5–10 Years",
  "Sell or Recap Within 3–5 Years",
  "Merchant Build / Immediate Sale after Stabilization",
]);

function uniq(allowed, list) {
  const out = [];
  const seen = new Set();
  for (const x of list) {
    if (!x || !allowed.includes(x) || seen.has(x)) continue;
    seen.add(x);
    out.push(x);
  }
  return out;
}

const STAGES_ALL = [...PROJECT_STAGES_ALLOWED];
const STAGES_OPERATING_HEAVY = uniq(PROJECT_STAGES_ALLOWED, [
  "Stabilized Operating Asset",
  "Under Construction",
  "Fully Entitled",
  "Entitlements in Process",
]);

const OWNER_FRANCHISE = uniq(OWNER_INVOLVEMENT_ALLOWED, [
  "Silent Investor",
  "High-Level Oversight Only",
  "Hands-On in Operations",
  "Moderate",
]);

const OWNER_SOFT = uniq(OWNER_INVOLVEMENT_ALLOWED, [
  "Silent Investor",
  "High-Level Oversight Only",
  "Hands-On in Operations",
  "Moderate",
  "Hands-on on design and capex",
]);

const OWNER_MEMBERSHIP = uniq(OWNER_INVOLVEMENT_ALLOWED, [
  "High-Level Oversight Only",
  "Moderate",
  "Hands-on on design and capex",
  "Hands-On in Operations",
]);

const OWNER_LUXURY = uniq(OWNER_INVOLVEMENT_ALLOWED, [
  "Silent Investor",
  "High-Level Oversight Only",
  "Moderate",
  "Hands-on on design and capex",
]);

const CAPITAL_OPEN = uniq(CAPITAL_STATUS_ALLOWED, [
  "Equity and Debt Fully Committed",
  "Equity Committed, Debt in Process",
  "Equity in Process, Debt Not Started",
]);

const CAPITAL_STRICT = uniq(CAPITAL_STATUS_ALLOWED, [
  "Equity and Debt Fully Committed",
  "Equity Committed, Debt in Process",
]);

const FEES_FRANCHISE = uniq(FEE_EXPECTATIONS_ALLOWED, [
  "In Line with Market Fees",
  "Open to Incentive Structures / Performance Fees",
]);

const FEES_LUXURY = uniq(FEE_EXPECTATIONS_ALLOWED, [
  "In Line with Market Fees",
  "Premium for Complexity / Performance",
  "Open to Incentive Structures / Performance Fees",
]);

const FEES_MEMBERSHIP = uniq(FEE_EXPECTATIONS_ALLOWED, [
  "In Line with Market Fees",
  "Open to Incentive Structures / Performance Fees",
  "Below Typical Market Fees",
]);

const EXIT_FRANCHISE = uniq(EXIT_HORIZON_ALLOWED, [
  "Hold 5–10 Years",
  "Hold 10+ Years",
  "Sell or Recap Within 3–5 Years",
]);

const EXIT_LUXURY = uniq(EXIT_HORIZON_ALLOWED, [
  "Hold 10+ Years",
  "Hold 5–10 Years",
]);

const EXIT_SOFT = uniq(EXIT_HORIZON_ALLOWED, [
  "Hold 5–10 Years",
  "Hold 10+ Years",
  "Sell or Recap Within 3–5 Years",
]);

/** @type {Record<string, object>} */
export const ENGAGEMENT_SEGMENTS = Object.freeze({
  economyLimited: {
    projectStages: STAGES_ALL,
    ownerInvolvement: OWNER_FRANCHISE,
    capitalStatus: CAPITAL_OPEN,
    feeExpectations: FEES_FRANCHISE,
    exitHorizon: EXIT_FRANCHISE,
  },
  midscaleSelect: {
    projectStages: STAGES_ALL,
    ownerInvolvement: OWNER_FRANCHISE,
    capitalStatus: CAPITAL_OPEN,
    feeExpectations: FEES_FRANCHISE,
    exitHorizon: EXIT_FRANCHISE,
  },
  upperMidscale: {
    projectStages: STAGES_ALL,
    ownerInvolvement: OWNER_FRANCHISE,
    capitalStatus: CAPITAL_OPEN,
    feeExpectations: FEES_FRANCHISE,
    exitHorizon: EXIT_FRANCHISE,
  },
  upscaleFullService: {
    projectStages: STAGES_ALL,
    ownerInvolvement: OWNER_SOFT,
    capitalStatus: CAPITAL_OPEN,
    feeExpectations: FEES_LUXURY,
    exitHorizon: EXIT_SOFT,
  },
  upperUpscaleLifestyle: {
    projectStages: STAGES_ALL,
    ownerInvolvement: OWNER_SOFT,
    capitalStatus: CAPITAL_OPEN,
    feeExpectations: FEES_LUXURY,
    exitHorizon: EXIT_SOFT,
  },
  luxury: {
    projectStages: STAGES_ALL,
    ownerInvolvement: OWNER_LUXURY,
    capitalStatus: CAPITAL_STRICT,
    feeExpectations: FEES_LUXURY,
    exitHorizon: EXIT_LUXURY,
  },
  luxuryResort: {
    projectStages: STAGES_ALL,
    ownerInvolvement: OWNER_LUXURY,
    capitalStatus: CAPITAL_STRICT,
    feeExpectations: FEES_LUXURY,
    exitHorizon: EXIT_LUXURY,
  },
  allInclusive: {
    projectStages: STAGES_ALL,
    ownerInvolvement: OWNER_LUXURY,
    capitalStatus: CAPITAL_STRICT,
    feeExpectations: FEES_LUXURY,
    exitHorizon: EXIT_LUXURY,
  },
  extendedStay: {
    projectStages: STAGES_ALL,
    ownerInvolvement: OWNER_FRANCHISE,
    capitalStatus: CAPITAL_OPEN,
    feeExpectations: FEES_FRANCHISE,
    exitHorizon: EXIT_FRANCHISE,
  },
  softBrandBoutique: {
    projectStages: STAGES_ALL,
    ownerInvolvement: OWNER_SOFT,
    capitalStatus: CAPITAL_OPEN,
    feeExpectations: FEES_FRANCHISE,
    exitHorizon: EXIT_SOFT,
  },
  membershipNetwork: {
    projectStages: STAGES_OPERATING_HEAVY,
    ownerInvolvement: OWNER_MEMBERSHIP,
    capitalStatus: CAPITAL_OPEN,
    feeExpectations: FEES_MEMBERSHIP,
    exitHorizon: EXIT_SOFT,
  },
  aparthotel: {
    projectStages: STAGES_ALL,
    ownerInvolvement: OWNER_FRANCHISE,
    capitalStatus: CAPITAL_OPEN,
    feeExpectations: FEES_FRANCHISE,
    exitHorizon: EXIT_FRANCHISE,
  },
});

function inferSegmentFromAmenityList(list) {
  if (!Array.isArray(list) || !list.length) return null;
  let best = null;
  let bestScore = -1;
  for (const [key, seg] of Object.entries(AMENITY_SEGMENTS)) {
    if (!ENGAGEMENT_SEGMENTS[key]) continue;
    const set = new Set(seg);
    const overlap = list.filter((x) => set.has(x)).length;
    const score = overlap / Math.max(seg.length, 1) - Math.abs(list.length - seg.length) * 0.02;
    if (score > bestScore) {
      bestScore = score;
      best = key;
    }
  }
  return bestScore >= 0.55 ? best : null;
}

function resolveSegmentKey(brandName, parentCompany) {
  const name = String(brandName || "").trim();
  const parent = String(parentCompany || "").trim();
  const fromAmenities = inferSegmentFromAmenityList(BRAND_AMENITY_OVERRIDES[name]);
  if (fromAmenities) return fromAmenities;
  if (PARENT_AMENITY_SEGMENT[parent] && ENGAGEMENT_SEGMENTS[PARENT_AMENITY_SEGMENT[parent]]) {
    return PARENT_AMENITY_SEGMENT[parent];
  }
  return "upperMidscale";
}

/**
 * Full Project Fit multi-select payload for blank-fill / correct.
 * @returns {{
 *   fields: Record<string, string[]>,
 *   resolveSource: string,
 *   segment: string
 * }}
 */
export function getBrandProjectFitEngagementFields(brandName, parentCompany = "") {
  const name = String(brandName || "").trim();
  const parent = String(parentCompany || "").trim();
  const { projectTypes, agreementTypes, resolveSource, segment: agrSeg } =
    getBrandProjectAndAgreementTypes(name, parent);

  const segKey = resolveSegmentKey(name, parent);
  const eng = ENGAGEMENT_SEGMENTS[segKey] || ENGAGEMENT_SEGMENTS.upperMidscale;

  return {
    fields: {
      "Acceptable Project Type": projectTypes,
      "Acceptable Agreements Type": agreementTypes,
      "Acceptable Project Stages": eng.projectStages,
      "Acceptable Owner Involvement Levels": eng.ownerInvolvement,
      "Acceptable Capital Status at Engagement": eng.capitalStatus,
      "Acceptable Fee Expectations vs Market": eng.feeExpectations,
      "Acceptable Exit Horizon": eng.exitHorizon,
    },
    resolveSource,
    segment: agrSeg || segKey,
    engagementSegment: segKey,
  };
}

export const MAP_PROJECT_FIT_ENGAGEMENT = Object.freeze({
  "Acceptable Project Type": "Acceptable Project Type",
  "Acceptable Agreements Type": "Acceptable Agreements Type",
  "Acceptable Project Stages": "Acceptable Project Stages",
  "Acceptable Owner Involvement Levels": "Acceptable Owner Involvement Levels",
  "Acceptable Capital Status at Engagement": "Acceptable Capital Status at Engagement",
  "Acceptable Fee Expectations vs Market": "Acceptable Fee Expectations vs Market",
  "Acceptable Exit Horizon": "Acceptable Exit Horizon",
});
