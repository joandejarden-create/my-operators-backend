/**
 * Project Fit: Acceptable Project Stages, Priority Markets, Markets to Avoid,
 * Other text fields, Min/Max Room Count.
 *
 * Markets to Avoid is a Match Score hard fail — default empty (no invented blacklist).
 */
import {
  AMENITY_SEGMENTS,
  PARENT_AMENITY_SEGMENT,
  BRAND_AMENITY_OVERRIDES,
} from "./brand-additional-amenities-profiles.mjs";
import { PROJECT_STAGES_ALLOWED } from "./brand-project-fit-engagement-profiles.mjs";

export const PRIORITY_MARKETS_ALLOWED = Object.freeze([
  "Global",
  "United States (Broad)",
  "Northeast (US)",
  "Southeast (US)",
  "Midwest (US)",
  "Southwest (US)",
  "West (US)",
  "Pacific (US)",
  "Canada",
  "Mexico",
  "Central America",
  "Caribbean",
  "South America",
  "Latin America (Broad)",
  "Middle East",
  "Western Europe",
  "Eastern Europe",
  "Southern Europe",
  "Northern Europe",
  "Nordic Countries",
  "United Kingdom",
  "Other (specify)",
]);

export const MARKETS_TO_AVOID_ALLOWED = PRIORITY_MARKETS_ALLOWED;

const M = Object.fromEntries(PRIORITY_MARKETS_ALLOWED.map((x) => [x, x]));

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

const uniqM = (list) => uniq(PRIORITY_MARKETS_ALLOWED, list);
const uniqS = (list) => uniq(PROJECT_STAGES_ALLOWED, list);

const STAGES_ALL = uniqS([...PROJECT_STAGES_ALLOWED]);
const STAGES_OPERATING_HEAVY = uniqS([
  "Stabilized Operating Asset",
  "Under Construction",
  "Fully Entitled",
  "Entitlements in Process",
]);

const MKT_US = uniqM([M["United States (Broad)"], M.Canada]);
const MKT_US_CALA = uniqM([
  M["United States (Broad)"],
  M.Canada,
  M.Mexico,
  M["Central America"],
  M.Caribbean,
  M["South America"],
  M["Latin America (Broad)"],
]);
const MKT_CALA_EURO = uniqM([
  M.Mexico,
  M["Central America"],
  M.Caribbean,
  M["South America"],
  M["Latin America (Broad)"],
  M["Southern Europe"],
  M["Western Europe"],
]);
const MKT_GLOBAL = uniqM([M.Global]);
const MKT_GLOBAL_REGIONS = uniqM([
  M["United States (Broad)"],
  M.Canada,
  M.Mexico,
  M["Central America"],
  M.Caribbean,
  M["South America"],
  M["Latin America (Broad)"],
  M["Western Europe"],
  M["Southern Europe"],
  M["United Kingdom"],
  M["Middle East"],
  M["Northern Europe"],
]);
const MKT_NA_EURO = uniqM([
  ...MKT_US,
  M["Western Europe"],
  M["United Kingdom"],
  M["Southern Europe"],
  M["Northern Europe"],
]);

const OTHER_PRIORITY_DEFAULT =
  "Priority markets listed are the primary growth focus; other geographies evaluated case-by-case.";
const OTHER_AVOID_DEFAULT =
  "No standing geographic blacklist—underwrite destination demand and brand fit market-by-market.";

/** @type {Record<string, { stages: string[], priorityMarkets: string[], marketsToAvoid: string[], roomMin: number, roomMax: number, otherPriority: string, otherAvoid: string }>} */
export const MARKETS_ROOMS_SEGMENTS = Object.freeze({
  economyLimited: {
    stages: STAGES_ALL,
    priorityMarkets: MKT_US,
    marketsToAvoid: [],
    roomMin: 40,
    roomMax: 120,
    otherPriority: "Economy limited-service growth focus: United States & Canada. " + OTHER_PRIORITY_DEFAULT,
    otherAvoid: OTHER_AVOID_DEFAULT,
  },
  midscaleSelect: {
    stages: STAGES_ALL,
    priorityMarkets: MKT_US_CALA,
    marketsToAvoid: [],
    roomMin: 60,
    roomMax: 200,
    otherPriority: "Midscale select-service focus: US & CALA. " + OTHER_PRIORITY_DEFAULT,
    otherAvoid: OTHER_AVOID_DEFAULT,
  },
  upperMidscale: {
    stages: STAGES_ALL,
    priorityMarkets: MKT_US_CALA,
    marketsToAvoid: [],
    roomMin: 80,
    roomMax: 250,
    otherPriority: "Upper-midscale focus: US & CALA. " + OTHER_PRIORITY_DEFAULT,
    otherAvoid: OTHER_AVOID_DEFAULT,
  },
  upscaleFullService: {
    stages: STAGES_ALL,
    priorityMarkets: MKT_GLOBAL,
    marketsToAvoid: [],
    roomMin: 120,
    roomMax: 450,
    otherPriority: "Full-service upscale — global development appetite. " + OTHER_PRIORITY_DEFAULT,
    otherAvoid: OTHER_AVOID_DEFAULT,
  },
  upperUpscaleLifestyle: {
    stages: STAGES_ALL,
    priorityMarkets: MKT_GLOBAL_REGIONS,
    marketsToAvoid: [],
    roomMin: 80,
    roomMax: 300,
    otherPriority: "Lifestyle / boutique urban & gateway markets. " + OTHER_PRIORITY_DEFAULT,
    otherAvoid: OTHER_AVOID_DEFAULT,
  },
  luxury: {
    stages: STAGES_ALL,
    priorityMarkets: MKT_GLOBAL,
    marketsToAvoid: [],
    roomMin: 80,
    roomMax: 350,
    otherPriority: "Luxury full-service — global gateway & resort destinations. " + OTHER_PRIORITY_DEFAULT,
    otherAvoid: OTHER_AVOID_DEFAULT,
  },
  luxuryResort: {
    stages: STAGES_ALL,
    priorityMarkets: MKT_GLOBAL,
    marketsToAvoid: [],
    roomMin: 40,
    roomMax: 200,
    otherPriority: "Luxury resort / sanctuary destinations worldwide. " + OTHER_PRIORITY_DEFAULT,
    otherAvoid: OTHER_AVOID_DEFAULT,
  },
  allInclusive: {
    stages: STAGES_ALL,
    priorityMarkets: MKT_CALA_EURO,
    marketsToAvoid: [],
    roomMin: 200,
    roomMax: 800,
    otherPriority: "All-inclusive beach / resort destinations (CALA & leisure Europe). " + OTHER_PRIORITY_DEFAULT,
    otherAvoid: OTHER_AVOID_DEFAULT,
  },
  extendedStay: {
    stages: STAGES_ALL,
    priorityMarkets: MKT_US_CALA,
    marketsToAvoid: [],
    roomMin: 80,
    roomMax: 160,
    otherPriority: "Extended-stay focus: US & CALA employment / medical demand nodes. " + OTHER_PRIORITY_DEFAULT,
    otherAvoid: OTHER_AVOID_DEFAULT,
  },
  softBrandBoutique: {
    stages: STAGES_ALL,
    priorityMarkets: MKT_GLOBAL_REGIONS,
    marketsToAvoid: [],
    roomMin: 50,
    roomMax: 250,
    otherPriority: "Soft brand / collection — conversion-friendly markets with distinctive assets. " + OTHER_PRIORITY_DEFAULT,
    otherAvoid: OTHER_AVOID_DEFAULT,
  },
  membershipNetwork: {
    stages: STAGES_OPERATING_HEAVY,
    priorityMarkets: MKT_GLOBAL,
    marketsToAvoid: [],
    roomMin: 20,
    roomMax: 300,
    otherPriority: "Membership network — property-led geography; global member coverage. " + OTHER_PRIORITY_DEFAULT,
    otherAvoid: OTHER_AVOID_DEFAULT,
  },
  aparthotel: {
    stages: STAGES_ALL,
    priorityMarkets: MKT_NA_EURO,
    marketsToAvoid: [],
    roomMin: 50,
    roomMax: 200,
    otherPriority: "Aparthotel / serviced apartment urban markets. " + OTHER_PRIORITY_DEFAULT,
    otherAvoid: OTHER_AVOID_DEFAULT,
  },
});

/** Parent Company → priority market preset (overrides segment markets when no brand override) */
export const PARENT_PRIORITY_MARKETS = Object.freeze({
  "Choice Hotels International": MKT_US_CALA,
  "Wyndham Hotels & Resorts": uniqM([...MKT_US_CALA, M["Western Europe"], M["United Kingdom"]]),
  "Hilton Worldwide": MKT_GLOBAL,
  "Marriott International, Inc.": MKT_GLOBAL,
  "InterContinental Hotels Group": MKT_GLOBAL,
  "Hyatt Hotels Corporation": MKT_GLOBAL,
  "Hyatt Vacation Ownership": MKT_CALA_EURO,
  AccorHotels: MKT_GLOBAL,
  "BWH Hotels": uniqM([...MKT_US_CALA, M["Western Europe"], M["United Kingdom"]]),
  "Sonesta International Hotels Corporation": MKT_US_CALA,
  "Radisson Hotel Group": MKT_GLOBAL_REGIONS,
  "Iberostar Hotels & Resorts": MKT_CALA_EURO,
  "Four Seasons Hotels and Resorts": MKT_GLOBAL,
  "Aman Group": MKT_GLOBAL,
  "Preferred Hotels & Resorts": MKT_GLOBAL,
  "Small Luxury Hotels of the World": MKT_GLOBAL,
  "Leading Hotels of the World": MKT_GLOBAL,
  "Red Roof Franchise, UK": MKT_US,
  "Dovetail + Co": MKT_US,
  "Coast Hotels Limited": uniqM([M.Canada, M["United States (Broad)"], M["Pacific (US)"]]),
  "Northland Properties": uniqM([M.Canada, M["United States (Broad)"]]),
});

/** Exact brand overrides (partial ok) */
export const BRAND_MARKETS_ROOMS_OVERRIDES = Object.freeze({
  "Comfort Inn & Suites": {
    priorityMarkets: MKT_US_CALA,
    roomMin: 60,
    roomMax: 200,
    otherPriority: "Choice Comfort — US & CALA midscale select. " + OTHER_PRIORITY_DEFAULT,
  },
  "Quality Inn": {
    priorityMarkets: MKT_US_CALA,
    roomMin: 50,
    roomMax: 160,
  },
  "Ascend Hotel Collection": {
    priorityMarkets: MKT_US_CALA,
    roomMin: 50,
    roomMax: 300,
    stages: STAGES_ALL,
    otherPriority: "Choice Ascend soft collection — US & CALA conversions. " + OTHER_PRIORITY_DEFAULT,
  },
  "WoodSpring Suites": {
    priorityMarkets: MKT_US,
    roomMin: 80,
    roomMax: 140,
  },
  "Suburban Studios": {
    priorityMarkets: MKT_US,
    roomMin: 60,
    roomMax: 130,
  },
  "Everhome Suites": {
    priorityMarkets: MKT_US,
    roomMin: 80,
    roomMax: 140,
  },
  "Cambria Hotels": {
    priorityMarkets: MKT_US_CALA,
    roomMin: 120,
    roomMax: 250,
  },
  "Radisson Blu by Choice": {
    priorityMarkets: uniqM([...MKT_US_CALA, M["Western Europe"], M["United Kingdom"], M["Middle East"]]),
    roomMin: 120,
    roomMax: 450,
  },
  "Radisson Individuals by Choice": {
    priorityMarkets: MKT_US_CALA,
    roomMin: 50,
    roomMax: 250,
  },
  "Hampton by Hilton": {
    priorityMarkets: MKT_GLOBAL,
    roomMin: 70,
    roomMax: 200,
    otherPriority: "Hampton — global select-service franchise footprint. " + OTHER_PRIORITY_DEFAULT,
  },
  "Hilton Garden Inn": {
    priorityMarkets: MKT_GLOBAL,
    roomMin: 90,
    roomMax: 250,
    otherPriority: "Hilton Garden Inn — global upper-midscale. " + OTHER_PRIORITY_DEFAULT,
  },
  "Home2 Suites by Hilton": {
    priorityMarkets: MKT_US_CALA,
    roomMin: 90,
    roomMax: 160,
    otherPriority: "Home2 extended-stay — US & CALA. " + OTHER_PRIORITY_DEFAULT,
  },
  "Spark by Hilton": {
    priorityMarkets: MKT_US,
    roomMin: 60,
    roomMax: 120,
    otherPriority: "Spark by Hilton — US economy focus. " + OTHER_PRIORITY_DEFAULT,
  },
  "Curio Collection by Hilton": {
    priorityMarkets: MKT_GLOBAL,
    roomMin: 50,
    roomMax: 300,
    otherPriority: "Curio soft collection — global conversions. " + OTHER_PRIORITY_DEFAULT,
  },
  "Kimpton Hotels": {
    priorityMarkets: MKT_GLOBAL_REGIONS,
    roomMin: 80,
    roomMax: 300,
    otherPriority: "Kimpton lifestyle — gateway & urban lifestyle markets. " + OTHER_PRIORITY_DEFAULT,
  },
  "Design Hotels": {
    priorityMarkets: MKT_GLOBAL,
    roomMin: 20,
    roomMax: 200,
    stages: STAGES_OPERATING_HEAVY,
  },
  "Preferred Hotels & Resorts": {
    priorityMarkets: MKT_GLOBAL,
    roomMin: 20,
    roomMax: 300,
    stages: STAGES_OPERATING_HEAVY,
  },
  "Small Luxury Hotels of the World": {
    priorityMarkets: MKT_GLOBAL,
    roomMin: 10,
    roomMax: 150,
    stages: STAGES_OPERATING_HEAVY,
  },
  Aman: {
    priorityMarkets: MKT_GLOBAL,
    roomMin: 20,
    roomMax: 80,
  },
  "Four Seasons": {
    priorityMarkets: MKT_GLOBAL,
    roomMin: 100,
    roomMax: 400,
  },
  "Iberostar Waves": {
    priorityMarkets: MKT_CALA_EURO,
    roomMin: 200,
    roomMax: 700,
  },
  "Iberostar Selection": {
    priorityMarkets: MKT_CALA_EURO,
    roomMin: 200,
    roomMax: 800,
  },
  "Hyatt Ziva": {
    priorityMarkets: MKT_CALA_EURO,
    roomMin: 300,
    roomMax: 800,
  },
  "Hyatt Zilara": {
    priorityMarkets: MKT_CALA_EURO,
    roomMin: 250,
    roomMax: 700,
  },
  "Canadas Best Value Inn": {
    priorityMarkets: uniqM([M.Canada, M["United States (Broad)"]]),
    roomMin: 40,
    roomMax: 120,
    otherPriority: "Canada-focused economy brand. " + OTHER_PRIORITY_DEFAULT,
  },
  "Coast Hotels": {
    priorityMarkets: uniqM([M.Canada, M["United States (Broad)"], M["Pacific (US)"]]),
    roomMin: 80,
    roomMax: 250,
  },
});

function inferSegmentFromAmenityList(list) {
  if (!Array.isArray(list) || !list.length) return null;
  let best = null;
  let bestScore = -1;
  for (const [key, seg] of Object.entries(AMENITY_SEGMENTS)) {
    if (!MARKETS_ROOMS_SEGMENTS[key]) continue;
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
  if (PARENT_AMENITY_SEGMENT[parent] && MARKETS_ROOMS_SEGMENTS[PARENT_AMENITY_SEGMENT[parent]]) {
    return PARENT_AMENITY_SEGMENT[parent];
  }
  return "upperMidscale";
}

/**
 * @returns {{
 *   fields: Record<string, unknown>,
 *   resolveSource: string,
 *   segment: string
 * }}
 */
export function getBrandMarketsRoomsStagesProfile(brandName, parentCompany = "") {
  const name = String(brandName || "").trim();
  const parent = String(parentCompany || "").trim();
  const override = BRAND_MARKETS_ROOMS_OVERRIDES[name] || null;
  const segKey = resolveSegmentKey(name, parent);
  const base = MARKETS_ROOMS_SEGMENTS[segKey] || MARKETS_ROOMS_SEGMENTS.upperMidscale;

  let priorityMarkets = uniqM(
    override?.priorityMarkets || PARENT_PRIORITY_MARKETS[parent] || base.priorityMarkets
  );
  let marketsToAvoid = uniqM(override?.marketsToAvoid ?? base.marketsToAvoid);
  let stages = uniqS(override?.stages || base.stages);
  let roomMin = override?.roomMin ?? base.roomMin;
  let roomMax = override?.roomMax ?? base.roomMax;
  if (roomMin > roomMax) {
    const t = roomMin;
    roomMin = roomMax;
    roomMax = t;
  }

  const otherPriority = override?.otherPriority || base.otherPriority;
  const otherAvoid = override?.otherAvoid || base.otherAvoid;

  let resolveSource = `segment:${segKey}`;
  if (override) resolveSource = "brand-override";
  else if (PARENT_PRIORITY_MARKETS[parent]) resolveSource = `parent-markets:${parent}:${segKey}`;
  else if (BRAND_AMENITY_OVERRIDES[name]) resolveSource = `amenity-segment:${segKey}`;
  else if (PARENT_AMENITY_SEGMENT[parent]) resolveSource = `parent:${parent}:${segKey}`;

  /** @type {Record<string, unknown>} */
  const fields = {
    "Acceptable Project Stages": stages,
    "Priority Markets": priorityMarkets,
    "Min - Room Count": roomMin,
    "Max - Room Count": roomMax,
    "Other - Priority Markets Text": otherPriority,
    "Other - Markets to Avoid Text": otherAvoid,
  };
  // Only include Markets to Avoid when non-empty — never invent hard fails
  if (marketsToAvoid.length) {
    fields["Markets to Avoid"] = marketsToAvoid;
  }

  return { fields, resolveSource, segment: segKey };
}

export const MAP_MARKETS_ROOMS_STAGES = Object.freeze({
  stages: "Acceptable Project Stages",
  priorityMarkets: "Priority Markets",
  marketsToAvoid: "Markets to Avoid",
  otherPriority: "Other - Priority Markets Text",
  otherAvoid: "Other - Markets to Avoid Text",
  roomMin: "Min - Room Count",
  roomMax: "Max - Room Count",
});
