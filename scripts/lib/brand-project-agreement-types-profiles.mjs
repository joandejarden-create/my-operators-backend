/**
 * Brand Setup - Project Fit → Acceptable Project Type + Acceptable Agreements Type.
 * Meta multi-select only. Resolve: brand override → amenity segment → parent → default.
 */
import {
  AMENITY_SEGMENTS,
  PARENT_AMENITY_SEGMENT,
  BRAND_AMENITY_OVERRIDES,
} from "./brand-additional-amenities-profiles.mjs";
import { BUILDING_TYPE_SEGMENTS } from "./brand-acceptable-building-types-profiles.mjs";

/** @type {readonly string[]} */
export const ACCEPTABLE_PROJECT_TYPES_ALLOWED = Object.freeze([
  "New Build",
  "Conversion / Reflag",
  "Renovation / Repositioning",
  "Expansion / Add-on",
  "Mixed-Use Hospitality Project",
  "Existing Operating Hotel",
  "Adaptive Reuse",
]);

/** @type {readonly string[]} */
export const ACCEPTABLE_AGREEMENTS_ALLOWED = Object.freeze([
  "Flexible/Open",
  "Franchise Only",
  "Brand-Managed Only",
  "Third-Party Management Only",
  "Lease",
  "Joint Venture",
  "Brand + Third-Party Mgmt. (Combined)",
  "Brand + Third-Party Mgmt. (Separate)",
]);

const P = Object.fromEntries(ACCEPTABLE_PROJECT_TYPES_ALLOWED.map((x) => [x, x]));
const A = Object.fromEntries(ACCEPTABLE_AGREEMENTS_ALLOWED.map((x) => [x, x]));

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

const uniqP = (list) => uniq(ACCEPTABLE_PROJECT_TYPES_ALLOWED, list);
const uniqA = (list) => uniq(ACCEPTABLE_AGREEMENTS_ALLOWED, list);

/** Typical franchise hard-brand project set */
const PROJ_FRANCHISE_CORE = uniqP([
  P["New Build"],
  P["Conversion / Reflag"],
  P["Renovation / Repositioning"],
  P["Expansion / Add-on"],
]);

const PROJ_SOFT_CONVERSION = uniqP([
  P["Conversion / Reflag"],
  P["Adaptive Reuse"],
  P["Existing Operating Hotel"],
  P["Renovation / Repositioning"],
  P["Mixed-Use Hospitality Project"],
  P["New Build"],
  P["Expansion / Add-on"],
]);

const PROJ_MEMBERSHIP = uniqP([
  P["Existing Operating Hotel"],
  P["Conversion / Reflag"],
  P["Renovation / Repositioning"],
  P["Adaptive Reuse"],
  P["Mixed-Use Hospitality Project"],
]);

const PROJ_LUXURY_MGMT = uniqP([
  P["New Build"],
  P["Renovation / Repositioning"],
  P["Mixed-Use Hospitality Project"],
  P["Expansion / Add-on"],
  P["Adaptive Reuse"],
]);

const PROJ_RESORT_AI = uniqP([
  P["New Build"],
  P["Renovation / Repositioning"],
  P["Expansion / Add-on"],
  P["Mixed-Use Hospitality Project"],
  P["Existing Operating Hotel"],
]);

/**
 * Agreement deal-model presets:
 * - Franchise: brand licenses only (Choice, Hampton, etc.). Owner may hire TPM separately.
 *   Never Brand-Managed or Brand+TPM Combined (brand does not operate).
 * - Soft franchise: same deal model; conversion/collection flags still franchise/affiliation.
 * - Franchise-or-mgmt: full-service parents that both franchise and brand-manage.
 * - Membership: no franchise/management by the network.
 * - Luxury/AI mgmt: brand operates (or JV/lease).
 */
const AGR_FRANCHISE = uniqA([
  A["Franchise Only"],
  A["Flexible/Open"],
  A["Brand + Third-Party Mgmt. (Separate)"],
]);

const AGR_SOFT_FRANCHISE = uniqA([
  A["Franchise Only"],
  A["Flexible/Open"],
  A["Brand + Third-Party Mgmt. (Separate)"],
]);

const AGR_FRANCHISE_OR_MGMT = uniqA([
  A["Franchise Only"],
  A["Brand-Managed Only"],
  A["Flexible/Open"],
  A["Brand + Third-Party Mgmt. (Separate)"],
  A["Brand + Third-Party Mgmt. (Combined)"],
]);

const AGR_MEMBERSHIP = uniqA([
  A["Flexible/Open"],
  A["Third-Party Management Only"],
]);

const AGR_LUXURY_MGMT = uniqA([
  A["Brand-Managed Only"],
  A["Flexible/Open"],
  A["Joint Venture"],
  A["Lease"],
]);

const AGR_AI_MGMT = uniqA([
  A["Brand-Managed Only"],
  A["Flexible/Open"],
  A["Franchise Only"],
  A["Joint Venture"],
]);

/** Parents that do not brand-manage hotels as the commercial model */
const PURE_FRANCHISOR_PARENTS = new Set([
  "Choice Hotels International",
  "Wyndham Hotels & Resorts",
  "Red Roof Franchise, UK",
]);

/**
 * @typedef {{ projectTypes: string[], agreementTypes: string[] }} FitDealProfile
 */

/** @type {Record<string, FitDealProfile>} */
export const PROJECT_AGREEMENT_SEGMENTS = Object.freeze({
  economyLimited: {
    projectTypes: PROJ_FRANCHISE_CORE,
    agreementTypes: AGR_FRANCHISE,
  },
  midscaleSelect: {
    projectTypes: PROJ_FRANCHISE_CORE,
    agreementTypes: AGR_FRANCHISE,
  },
  upperMidscale: {
    projectTypes: uniqP([...PROJ_FRANCHISE_CORE, P["Mixed-Use Hospitality Project"]]),
    agreementTypes: AGR_FRANCHISE,
  },
  upscaleFullService: {
    projectTypes: uniqP([
      ...PROJ_FRANCHISE_CORE,
      P["Mixed-Use Hospitality Project"],
      P["Adaptive Reuse"],
    ]),
    agreementTypes: AGR_FRANCHISE_OR_MGMT,
  },
  upperUpscaleLifestyle: {
    projectTypes: uniqP([
      P["New Build"],
      P["Conversion / Reflag"],
      P["Renovation / Repositioning"],
      P["Adaptive Reuse"],
      P["Mixed-Use Hospitality Project"],
      P["Existing Operating Hotel"],
      P["Expansion / Add-on"],
    ]),
    agreementTypes: AGR_SOFT_FRANCHISE,
  },
  luxury: {
    projectTypes: PROJ_LUXURY_MGMT,
    agreementTypes: AGR_LUXURY_MGMT,
  },
  luxuryResort: {
    projectTypes: PROJ_RESORT_AI,
    agreementTypes: AGR_LUXURY_MGMT,
  },
  allInclusive: {
    projectTypes: PROJ_RESORT_AI,
    agreementTypes: AGR_AI_MGMT,
  },
  extendedStay: {
    projectTypes: PROJ_FRANCHISE_CORE,
    agreementTypes: AGR_FRANCHISE,
  },
  softBrandBoutique: {
    projectTypes: PROJ_SOFT_CONVERSION,
    agreementTypes: AGR_SOFT_FRANCHISE,
  },
  membershipNetwork: {
    projectTypes: PROJ_MEMBERSHIP,
    agreementTypes: AGR_MEMBERSHIP,
  },
  aparthotel: {
    projectTypes: uniqP([
      P["New Build"],
      P["Conversion / Reflag"],
      P["Renovation / Repositioning"],
      P["Mixed-Use Hospitality Project"],
      P["Adaptive Reuse"],
    ]),
    agreementTypes: AGR_FRANCHISE,
  },
});

/** Exact brand → partial or full profile override */
export const BRAND_PROJECT_AGREEMENT_OVERRIDES = Object.freeze({
  // Choice
  "Ascend Hotel Collection": {
    projectTypes: PROJ_SOFT_CONVERSION,
    agreementTypes: AGR_SOFT_FRANCHISE,
  },
  "Comfort Inn & Suites": {
    projectTypes: PROJ_FRANCHISE_CORE,
    agreementTypes: AGR_FRANCHISE,
  },
  "Quality Inn": {
    projectTypes: PROJ_FRANCHISE_CORE,
    agreementTypes: AGR_FRANCHISE,
  },
  "Radisson Individuals by Choice": {
    projectTypes: PROJ_SOFT_CONVERSION,
    agreementTypes: AGR_SOFT_FRANCHISE,
  },
  "Radisson Blu by Choice": {
    projectTypes: uniqP([...PROJ_FRANCHISE_CORE, P["Mixed-Use Hospitality Project"], P["Adaptive Reuse"]]),
    agreementTypes: AGR_FRANCHISE,
  },
  "WoodSpring Suites": {
    projectTypes: uniqP([P["New Build"], P["Conversion / Reflag"], P["Renovation / Repositioning"]]),
    agreementTypes: AGR_FRANCHISE,
  },
  "Suburban Studios": {
    projectTypes: uniqP([P["New Build"], P["Conversion / Reflag"], P["Renovation / Repositioning"]]),
    agreementTypes: AGR_FRANCHISE,
  },
  "Everhome Suites": {
    projectTypes: PROJ_FRANCHISE_CORE,
    agreementTypes: AGR_FRANCHISE,
  },

  // Soft / lifestyle
  "Curio Collection by Hilton": {
    projectTypes: PROJ_SOFT_CONVERSION,
    agreementTypes: AGR_SOFT_FRANCHISE,
  },
  "Tapestry Collection by Hilton": {
    projectTypes: PROJ_SOFT_CONVERSION,
    agreementTypes: AGR_SOFT_FRANCHISE,
  },
  "Autograph Collection": {
    projectTypes: PROJ_SOFT_CONVERSION,
    agreementTypes: AGR_SOFT_FRANCHISE,
  },
  "Tribute Portfolio": {
    projectTypes: PROJ_SOFT_CONVERSION,
    agreementTypes: AGR_SOFT_FRANCHISE,
  },
  "Design Hotels": {
    projectTypes: PROJ_MEMBERSHIP,
    agreementTypes: AGR_MEMBERSHIP,
  },
  "Kimpton Hotels": {
    projectTypes: uniqP([
      P["New Build"],
      P["Conversion / Reflag"],
      P["Renovation / Repositioning"],
      P["Adaptive Reuse"],
      P["Mixed-Use Hospitality Project"],
      P["Existing Operating Hotel"],
      P["Expansion / Add-on"],
    ]),
    agreementTypes: AGR_SOFT_FRANCHISE,
  },
  "Hotel Indigo": {
    projectTypes: uniqP([
      P["New Build"],
      P["Conversion / Reflag"],
      P["Renovation / Repositioning"],
      P["Adaptive Reuse"],
      P["Mixed-Use Hospitality Project"],
      P["Existing Operating Hotel"],
    ]),
    agreementTypes: AGR_SOFT_FRANCHISE,
  },
  "Vignette Collection": {
    projectTypes: PROJ_SOFT_CONVERSION,
    agreementTypes: AGR_SOFT_FRANCHISE,
  },
  "Handwritten Collection": {
    projectTypes: PROJ_SOFT_CONVERSION,
    agreementTypes: AGR_SOFT_FRANCHISE,
  },
  "BW Premier Collection": {
    projectTypes: PROJ_SOFT_CONVERSION,
    agreementTypes: AGR_MEMBERSHIP,
  },
  "BW Signature Collection": {
    projectTypes: PROJ_SOFT_CONVERSION,
    agreementTypes: AGR_MEMBERSHIP,
  },
  "Preferred Hotels & Resorts": {
    projectTypes: PROJ_MEMBERSHIP,
    agreementTypes: AGR_MEMBERSHIP,
  },
  "Small Luxury Hotels of the World": {
    projectTypes: PROJ_MEMBERSHIP,
    agreementTypes: AGR_MEMBERSHIP,
  },
  "The Leading Hotels of the World": {
    projectTypes: PROJ_MEMBERSHIP,
    agreementTypes: AGR_MEMBERSHIP,
  },
  "Mr & Mrs Smith": {
    projectTypes: PROJ_MEMBERSHIP,
    agreementTypes: AGR_MEMBERSHIP,
  },

  // Hilton hard
  "Hampton by Hilton": {
    projectTypes: PROJ_FRANCHISE_CORE,
    agreementTypes: AGR_FRANCHISE,
  },
  "Hilton Garden Inn": {
    projectTypes: uniqP([...PROJ_FRANCHISE_CORE, P["Mixed-Use Hospitality Project"]]),
    agreementTypes: AGR_FRANCHISE,
  },
  "Home2 Suites by Hilton": {
    projectTypes: PROJ_FRANCHISE_CORE,
    agreementTypes: AGR_FRANCHISE,
  },
  "Homewood Suites by Hilton": {
    projectTypes: PROJ_FRANCHISE_CORE,
    agreementTypes: AGR_FRANCHISE,
  },
  "Spark by Hilton": {
    projectTypes: PROJ_FRANCHISE_CORE,
    agreementTypes: AGR_FRANCHISE,
  },
  "Tru by Hilton": {
    projectTypes: PROJ_FRANCHISE_CORE,
    agreementTypes: AGR_FRANCHISE,
  },
  "Conrad Hotels & Resorts": {
    projectTypes: PROJ_LUXURY_MGMT,
    agreementTypes: AGR_LUXURY_MGMT,
  },
  "Waldorf Astoria": {
    projectTypes: PROJ_LUXURY_MGMT,
    agreementTypes: AGR_LUXURY_MGMT,
  },
  "LXR Hotels & Resorts": {
    projectTypes: PROJ_LUXURY_MGMT,
    agreementTypes: AGR_LUXURY_MGMT,
  },

  // Luxury / resort
  Aman: {
    projectTypes: PROJ_RESORT_AI,
    agreementTypes: AGR_LUXURY_MGMT,
  },
  "Four Seasons": {
    projectTypes: PROJ_LUXURY_MGMT,
    agreementTypes: AGR_LUXURY_MGMT,
  },
  "Ritz-Carlton": {
    projectTypes: PROJ_LUXURY_MGMT,
    agreementTypes: AGR_LUXURY_MGMT,
  },
  "St. Regis": {
    projectTypes: PROJ_LUXURY_MGMT,
    agreementTypes: AGR_LUXURY_MGMT,
  },
  "JW Marriott": {
    projectTypes: PROJ_LUXURY_MGMT,
    agreementTypes: AGR_FRANCHISE_OR_MGMT,
  },
  "Iberostar Waves": {
    projectTypes: PROJ_RESORT_AI,
    agreementTypes: AGR_AI_MGMT,
  },
  "Iberostar Selection": {
    projectTypes: PROJ_RESORT_AI,
    agreementTypes: AGR_AI_MGMT,
  },
  "Hyatt Ziva": {
    projectTypes: PROJ_RESORT_AI,
    agreementTypes: AGR_AI_MGMT,
  },
  "Hyatt Zilara": {
    projectTypes: PROJ_RESORT_AI,
    agreementTypes: AGR_AI_MGMT,
  },
  "Six Senses Hotels Resorts Spas": {
    projectTypes: PROJ_RESORT_AI,
    agreementTypes: AGR_LUXURY_MGMT,
  },
});

function inferSegmentFromAmenityList(list) {
  if (!Array.isArray(list) || !list.length) return null;
  let best = null;
  let bestScore = -1;
  for (const [key, seg] of Object.entries(AMENITY_SEGMENTS)) {
    if (!PROJECT_AGREEMENT_SEGMENTS[key] || !BUILDING_TYPE_SEGMENTS[key]) continue;
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
  if (PARENT_AMENITY_SEGMENT[parent] && PROJECT_AGREEMENT_SEGMENTS[PARENT_AMENITY_SEGMENT[parent]]) {
    return PARENT_AMENITY_SEGMENT[parent];
  }
  return "upperMidscale";
}

/**
 * @returns {{
 *   projectTypes: string[],
 *   agreementTypes: string[],
 *   resolveSource: string,
 *   segment: string
 * }}
 */
export function getBrandProjectAndAgreementTypes(brandName, parentCompany = "") {
  const name = String(brandName || "").trim();
  const parent = String(parentCompany || "").trim();
  const override = BRAND_PROJECT_AGREEMENT_OVERRIDES[name] || null;
  const segKey = override
    ? inferSegmentFromAmenityList(BRAND_AMENITY_OVERRIDES[name]) ||
      (PARENT_AMENITY_SEGMENT[parent] && PROJECT_AGREEMENT_SEGMENTS[PARENT_AMENITY_SEGMENT[parent]]
        ? PARENT_AMENITY_SEGMENT[parent]
        : "upperMidscale")
    : resolveSegmentKey(name, parent);

  // Prefer override segment hints via amenity when brand override exists
  let segment = segKey;
  if (override && BRAND_AMENITY_OVERRIDES[name]) {
    const inferred = inferSegmentFromAmenityList(BRAND_AMENITY_OVERRIDES[name]);
    if (inferred) segment = inferred;
  }

  const base = PROJECT_AGREEMENT_SEGMENTS[segment] || PROJECT_AGREEMENT_SEGMENTS.upperMidscale;
  let projectTypes = uniqP(override?.projectTypes || base.projectTypes);
  let agreementTypes = uniqA(override?.agreementTypes || base.agreementTypes);

  // Pure franchisors never brand-manage — strip Combined / Brand-Managed regardless of segment
  if (PURE_FRANCHISOR_PARENTS.has(parent)) {
    agreementTypes = AGR_FRANCHISE;
  }

  let resolveSource = `segment:${segment}`;
  if (PURE_FRANCHISOR_PARENTS.has(parent)) {
    resolveSource = override
      ? `brand-override+pure-franchisor:${parent}`
      : `pure-franchisor:${parent}`;
  } else if (override) resolveSource = "brand-override";
  else if (BRAND_AMENITY_OVERRIDES[name]) resolveSource = `amenity-segment:${segment}`;
  else if (PARENT_AMENITY_SEGMENT[parent]) resolveSource = `parent:${parent}:${segment}`;
  else resolveSource = "default:upperMidscale";

  return { projectTypes, agreementTypes, resolveSource, segment };
}

export const MAP_PROJECT_AGREEMENT_TYPES = Object.freeze({
  projectTypes: "Acceptable Project Type",
  agreementTypes: "Acceptable Agreements Type",
  table: "Brand Setup - Project Fit",
});
