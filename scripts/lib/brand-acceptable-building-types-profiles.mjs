/**
 * Brand Setup - Project Fit → Acceptable Building Types (multi-select).
 * Values must match Airtable Meta choices exactly.
 * Resolve: brand override → amenity-segment inference → parent segment → default.
 */
import {
  AMENITY_SEGMENTS,
  PARENT_AMENITY_SEGMENT,
  BRAND_AMENITY_OVERRIDES,
} from "./brand-additional-amenities-profiles.mjs";

/** @type {readonly string[]} */
export const ACCEPTABLE_BUILDING_TYPES_ALLOWED = Object.freeze([
  "Low-Rise",
  "Mid-Rise",
  "High-Rise",
  "Mixed-Use",
  "Podium / Tower",
  "Historic / Renovated",
  "Resort-Style Compound",
  "Historic / Adaptive Reuse",
]);

const B = Object.fromEntries(ACCEPTABLE_BUILDING_TYPES_ALLOWED.map((x) => [x, x]));

/** Both historic Meta options — deal form uses either; include both for match score. */
const HISTORIC = Object.freeze([B["Historic / Renovated"], B["Historic / Adaptive Reuse"]]);

function uniq(list) {
  const out = [];
  const seen = new Set();
  for (const x of list) {
    if (!x || !ACCEPTABLE_BUILDING_TYPES_ALLOWED.includes(x) || seen.has(x)) continue;
    seen.add(x);
    out.push(x);
  }
  return out;
}

/** Segment presets — likely acceptable building forms by brand archetype */
export const BUILDING_TYPE_SEGMENTS = Object.freeze({
  economyLimited: uniq([B["Low-Rise"], B["Mid-Rise"]]),

  midscaleSelect: uniq([
    B["Low-Rise"],
    B["Mid-Rise"],
    B["Mixed-Use"],
    B["Podium / Tower"],
  ]),

  upperMidscale: uniq([
    B["Low-Rise"],
    B["Mid-Rise"],
    B["High-Rise"],
    B["Mixed-Use"],
    B["Podium / Tower"],
  ]),

  upscaleFullService: uniq([
    B["Mid-Rise"],
    B["High-Rise"],
    B["Mixed-Use"],
    B["Podium / Tower"],
    ...HISTORIC,
  ]),

  upperUpscaleLifestyle: uniq([
    B["Mid-Rise"],
    B["High-Rise"],
    B["Mixed-Use"],
    B["Podium / Tower"],
    ...HISTORIC,
  ]),

  luxury: uniq([
    B["Mid-Rise"],
    B["High-Rise"],
    B["Mixed-Use"],
    B["Podium / Tower"],
    B["Resort-Style Compound"],
    ...HISTORIC,
  ]),

  luxuryResort: uniq([
    B["Resort-Style Compound"],
    B["Low-Rise"],
    B["Mid-Rise"],
    ...HISTORIC,
  ]),

  allInclusive: uniq([
    B["Resort-Style Compound"],
    B["Low-Rise"],
    B["Mid-Rise"],
  ]),

  extendedStay: uniq([
    B["Low-Rise"],
    B["Mid-Rise"],
    B["High-Rise"],
    B["Mixed-Use"],
    B["Podium / Tower"],
  ]),

  softBrandBoutique: uniq([
    B["Low-Rise"],
    B["Mid-Rise"],
    B["High-Rise"],
    B["Mixed-Use"],
    B["Podium / Tower"],
    ...HISTORIC,
  ]),

  membershipNetwork: uniq([
    B["Low-Rise"],
    B["Mid-Rise"],
    B["High-Rise"],
    B["Mixed-Use"],
    B["Podium / Tower"],
    B["Resort-Style Compound"],
    ...HISTORIC,
  ]),

  aparthotel: uniq([
    B["Low-Rise"],
    B["Mid-Rise"],
    B["High-Rise"],
    B["Mixed-Use"],
    B["Podium / Tower"],
  ]),
});

/** Exact Brand Name → building types */
export const BRAND_BUILDING_TYPE_OVERRIDES = Object.freeze({
  // Choice Active
  "Ascend Hotel Collection": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "Comfort Inn & Suites": BUILDING_TYPE_SEGMENTS.midscaleSelect,
  "Quality Inn": uniq([B["Low-Rise"], B["Mid-Rise"], B["Mixed-Use"]]),
  "Everhome Suites": BUILDING_TYPE_SEGMENTS.extendedStay,
  "Suburban Studios": uniq([B["Low-Rise"], B["Mid-Rise"]]),
  "WoodSpring Suites": uniq([B["Low-Rise"], B["Mid-Rise"]]),
  "Country Inn & Suites by Choice": BUILDING_TYPE_SEGMENTS.midscaleSelect,
  "Radisson by Choice": BUILDING_TYPE_SEGMENTS.upperMidscale,
  "Radisson Blu by Choice": BUILDING_TYPE_SEGMENTS.upscaleFullService,
  "Radisson RED by Choice": BUILDING_TYPE_SEGMENTS.upperUpscaleLifestyle,
  "Radisson Individuals by Choice": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "Cambria Hotels": BUILDING_TYPE_SEGMENTS.upperMidscale,
  Clarion: uniq([B["Low-Rise"], B["Mid-Rise"], B["Mixed-Use"], ...HISTORIC]),
  "Clarion Pointe": uniq([B["Low-Rise"], B["Mid-Rise"]]),
  "Sleep Inn": uniq([B["Low-Rise"], B["Mid-Rise"]]),
  "Econo Lodge": BUILDING_TYPE_SEGMENTS.economyLimited,
  "Rodeway Inn": BUILDING_TYPE_SEGMENTS.economyLimited,
  "MainStay Suites": BUILDING_TYPE_SEGMENTS.extendedStay,

  // Soft / lifestyle Active
  "Curio Collection by Hilton": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "Tapestry Collection by Hilton": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "Autograph Collection": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "Tribute Portfolio": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "Design Hotels": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "Kimpton Hotels": uniq([
    B["Mid-Rise"],
    B["High-Rise"],
    B["Mixed-Use"],
    B["Podium / Tower"],
    ...HISTORIC,
  ]),
  "Hotel Indigo": BUILDING_TYPE_SEGMENTS.upperUpscaleLifestyle,
  "Vignette Collection": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "Handwritten Collection": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "MGallery Collection": BUILDING_TYPE_SEGMENTS.upperUpscaleLifestyle,
  "BW Premier Collection": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "BW Signature Collection": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "Preferred Hotels & Resorts": BUILDING_TYPE_SEGMENTS.membershipNetwork,
  "Small Luxury Hotels of the World": BUILDING_TYPE_SEGMENTS.membershipNetwork,
  "The Leading Hotels of the World": BUILDING_TYPE_SEGMENTS.luxury,
  "Mr & Mrs Smith": BUILDING_TYPE_SEGMENTS.membershipNetwork,

  // Hilton hard
  "Hampton by Hilton": BUILDING_TYPE_SEGMENTS.midscaleSelect,
  "Hilton Garden Inn": BUILDING_TYPE_SEGMENTS.upperMidscale,
  "Homewood Suites by Hilton": BUILDING_TYPE_SEGMENTS.extendedStay,
  "Home2 Suites by Hilton": BUILDING_TYPE_SEGMENTS.extendedStay,
  "Embassy Suites by Hilton": BUILDING_TYPE_SEGMENTS.upscaleFullService,
  "DoubleTree by Hilton": BUILDING_TYPE_SEGMENTS.upscaleFullService,
  "Hilton Hotels & Resorts": BUILDING_TYPE_SEGMENTS.upscaleFullService,
  "Conrad Hotels & Resorts": BUILDING_TYPE_SEGMENTS.luxury,
  "Waldorf Astoria": BUILDING_TYPE_SEGMENTS.luxury,
  "Canopy by Hilton": BUILDING_TYPE_SEGMENTS.upperUpscaleLifestyle,
  "Motto by Hilton": uniq([B["Mid-Rise"], B["High-Rise"], B["Mixed-Use"], B["Podium / Tower"]]),
  "Tempo by Hilton": BUILDING_TYPE_SEGMENTS.upperUpscaleLifestyle,
  "Spark by Hilton": BUILDING_TYPE_SEGMENTS.economyLimited,
  "Tru by Hilton": BUILDING_TYPE_SEGMENTS.economyLimited,
  "Signia by Hilton": uniq([
    B["Mid-Rise"],
    B["High-Rise"],
    B["Mixed-Use"],
    B["Podium / Tower"],
  ]),
  "LXR Hotels & Resorts": BUILDING_TYPE_SEGMENTS.luxury,

  // Marriott
  "Marriott Hotels": BUILDING_TYPE_SEGMENTS.upscaleFullService,
  "Courtyard by Marriott": BUILDING_TYPE_SEGMENTS.upperMidscale,
  "Fairfield by Marriott": BUILDING_TYPE_SEGMENTS.midscaleSelect,
  "Residence Inn by Marriott": BUILDING_TYPE_SEGMENTS.extendedStay,
  "SpringHill Suites by Marriott": BUILDING_TYPE_SEGMENTS.upperMidscale,
  "TownePlace Suites by Marriott": BUILDING_TYPE_SEGMENTS.extendedStay,
  "AC Hotels by Marriott": BUILDING_TYPE_SEGMENTS.upperUpscaleLifestyle,
  "Aloft Hotels": BUILDING_TYPE_SEGMENTS.upperUpscaleLifestyle,
  "Moxy Hotels": uniq([B["Mid-Rise"], B["High-Rise"], B["Mixed-Use"], B["Podium / Tower"]]),
  "Element by Westin": BUILDING_TYPE_SEGMENTS.extendedStay,
  Westin: BUILDING_TYPE_SEGMENTS.upscaleFullService,
  Sheraton: BUILDING_TYPE_SEGMENTS.upscaleFullService,
  Renaissance: BUILDING_TYPE_SEGMENTS.upscaleFullService,
  "JW Marriott": BUILDING_TYPE_SEGMENTS.luxury,
  "Ritz-Carlton": BUILDING_TYPE_SEGMENTS.luxury,
  "The Ritz-Carlton Reserve": BUILDING_TYPE_SEGMENTS.luxuryResort,
  "St. Regis": BUILDING_TYPE_SEGMENTS.luxury,
  "W Hotels": uniq([
    B["Mid-Rise"],
    B["High-Rise"],
    B["Mixed-Use"],
    B["Podium / Tower"],
    ...HISTORIC,
  ]),
  Edition: BUILDING_TYPE_SEGMENTS.luxury,
  "Luxury Collection": BUILDING_TYPE_SEGMENTS.luxury,
  "Gaylord Hotels": uniq([B["Low-Rise"], B["Mid-Rise"], B["Resort-Style Compound"]]),

  // IHG
  "Holiday Inn": BUILDING_TYPE_SEGMENTS.upperMidscale,
  "Holiday Inn Express": BUILDING_TYPE_SEGMENTS.midscaleSelect,
  "Crowne Plaza": BUILDING_TYPE_SEGMENTS.upscaleFullService,
  InterContinental: BUILDING_TYPE_SEGMENTS.luxury,
  "Staybridge Suites": BUILDING_TYPE_SEGMENTS.extendedStay,
  "Candlewood Suites": BUILDING_TYPE_SEGMENTS.extendedStay,
  "Even Hotels": BUILDING_TYPE_SEGMENTS.upperMidscale,
  "avid hotels": BUILDING_TYPE_SEGMENTS.economyLimited,
  "Voco Hotels": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "Six Senses Hotels Resorts Spas": BUILDING_TYPE_SEGMENTS.luxuryResort,

  // Hyatt
  Hyatt: BUILDING_TYPE_SEGMENTS.upscaleFullService,
  "Hyatt Place": BUILDING_TYPE_SEGMENTS.upperMidscale,
  "Hyatt House": BUILDING_TYPE_SEGMENTS.extendedStay,
  "Hyatt Regency": BUILDING_TYPE_SEGMENTS.upscaleFullService,
  "Grand Hyatt": BUILDING_TYPE_SEGMENTS.luxury,
  "Park Hyatt": BUILDING_TYPE_SEGMENTS.luxury,
  Andaz: BUILDING_TYPE_SEGMENTS.upperUpscaleLifestyle,
  "Hyatt Centric": BUILDING_TYPE_SEGMENTS.upperUpscaleLifestyle,
  "Destination by Hyatt": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "Unbound Collection by Hyatt": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "JdV by Hyatt": BUILDING_TYPE_SEGMENTS.softBrandBoutique,
  "Thompson Hotels": BUILDING_TYPE_SEGMENTS.upperUpscaleLifestyle,
  "Hyatt Ziva": BUILDING_TYPE_SEGMENTS.allInclusive,
  "Hyatt Zilara": BUILDING_TYPE_SEGMENTS.allInclusive,
  "Hyatt Vivid": BUILDING_TYPE_SEGMENTS.allInclusive,

  // Wyndham
  "Days Inn by Wyndham": BUILDING_TYPE_SEGMENTS.economyLimited,
  "Super 8 by Wyndham": BUILDING_TYPE_SEGMENTS.economyLimited,
  "Microtel by Wyndham": BUILDING_TYPE_SEGMENTS.economyLimited,
  "La Quinta by Wyndham": BUILDING_TYPE_SEGMENTS.midscaleSelect,
  "Trademark Collection by Wyndham": BUILDING_TYPE_SEGMENTS.softBrandBoutique,

  // Accor
  ibis: BUILDING_TYPE_SEGMENTS.economyLimited,
  "ibis Styles": BUILDING_TYPE_SEGMENTS.midscaleSelect,
  "ibis budget": BUILDING_TYPE_SEGMENTS.economyLimited,
  Novotel: BUILDING_TYPE_SEGMENTS.upperMidscale,
  Sofitel: BUILDING_TYPE_SEGMENTS.luxury,
  Fairmont: BUILDING_TYPE_SEGMENTS.luxury,
  Raffles: BUILDING_TYPE_SEGMENTS.luxury,
  "Adagio Aparthotel": BUILDING_TYPE_SEGMENTS.aparthotel,

  // Best Western
  "Best Western": BUILDING_TYPE_SEGMENTS.midscaleSelect,
  "Best Western Plus": BUILDING_TYPE_SEGMENTS.upperMidscale,
  "Best Western Premier": BUILDING_TYPE_SEGMENTS.upscaleFullService,

  // Resort / AI / luxury
  "Secrets Resorts & Spas": BUILDING_TYPE_SEGMENTS.allInclusive,
  "Dreams Resorts & Spas": BUILDING_TYPE_SEGMENTS.allInclusive,
  "Iberostar Selection": BUILDING_TYPE_SEGMENTS.allInclusive,
  "Iberostar Waves": BUILDING_TYPE_SEGMENTS.allInclusive,
  "Four Seasons": BUILDING_TYPE_SEGMENTS.luxuryResort,
  Aman: BUILDING_TYPE_SEGMENTS.luxuryResort,
  "Rosewood Hotel Group": BUILDING_TYPE_SEGMENTS.luxury,
  "Mandarin Oriental Hotel Group": BUILDING_TYPE_SEGMENTS.luxury,
  "Banyan Tree": BUILDING_TYPE_SEGMENTS.luxuryResort,
});

function inferSegmentFromAmenityList(list) {
  if (!Array.isArray(list) || !list.length) return null;
  let best = null;
  let bestScore = -1;
  for (const [key, seg] of Object.entries(AMENITY_SEGMENTS)) {
    if (!BUILDING_TYPE_SEGMENTS[key]) continue;
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
  if (BRAND_BUILDING_TYPE_OVERRIDES[name]) {
    // Prefer explicit list; segment key only needed for parent fallback path
  }
  const fromAmenities = inferSegmentFromAmenityList(BRAND_AMENITY_OVERRIDES[name]);
  if (fromAmenities) return fromAmenities;
  if (PARENT_AMENITY_SEGMENT[parent] && BUILDING_TYPE_SEGMENTS[PARENT_AMENITY_SEGMENT[parent]]) {
    return PARENT_AMENITY_SEGMENT[parent];
  }
  return "upperMidscale";
}

/**
 * @returns {{ buildingTypes: string[], resolveSource: string, segment: string }}
 */
export function getBrandAcceptableBuildingTypes(brandName, parentCompany = "") {
  const name = String(brandName || "").trim();
  const parent = String(parentCompany || "").trim();

  if (BRAND_BUILDING_TYPE_OVERRIDES[name]) {
    return {
      buildingTypes: uniq(BRAND_BUILDING_TYPE_OVERRIDES[name]),
      resolveSource: "brand-override",
      segment: resolveSegmentKey(name, parent),
    };
  }

  const segKey = resolveSegmentKey(name, parent);
  const list = BUILDING_TYPE_SEGMENTS[segKey] || BUILDING_TYPE_SEGMENTS.upperMidscale;
  if (PARENT_AMENITY_SEGMENT[parent] || BRAND_AMENITY_OVERRIDES[name]) {
    return {
      buildingTypes: uniq(list),
      resolveSource: BRAND_AMENITY_OVERRIDES[name]
        ? `amenity-segment:${segKey}`
        : `parent:${parent}:${segKey}`,
      segment: segKey,
    };
  }

  return {
    buildingTypes: uniq(BUILDING_TYPE_SEGMENTS.upperMidscale),
    resolveSource: "default:upperMidscale",
    segment: "upperMidscale",
  };
}

export const MAP_ACCEPTABLE_BUILDING_TYPES = Object.freeze({
  field: "Acceptable Building Types",
  table: "Brand Setup - Project Fit",
});
