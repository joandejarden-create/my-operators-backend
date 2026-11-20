/**
 * Multi-family brand registry for historical affiliation detection.
 * Originally Marriott/Hilton; V2.4+ expands to Hyatt/IHG/Choice/etc. without restricting research.
 */

export const BRAND_FAMILIES_V2 = "sec-edgar-brand-families-v2";

export const MARRIOTT_BRANDS = Object.freeze([
  { key: "marriott-hotels", label: "Marriott Hotels", aliases: ["Marriott Hotel", "Marriott Hotels & Resorts"] },
  { key: "jw-marriott", label: "JW Marriott", aliases: ["J.W. Marriott"] },
  { key: "renaissance", label: "Renaissance", aliases: ["Renaissance Hotels"] },
  { key: "courtyard", label: "Courtyard", aliases: ["Courtyard by Marriott"] },
  { key: "residence-inn", label: "Residence Inn", aliases: ["Residence Inn by Marriott"] },
  { key: "fairfield", label: "Fairfield", aliases: ["Fairfield Inn", "Fairfield Inn & Suites", "Fairfield by Marriott"] },
  { key: "springhill-suites", label: "SpringHill Suites", aliases: ["Springhill Suites"] },
  { key: "towneplace-suites", label: "TownePlace Suites", aliases: ["Towneplace Suites"] },
  { key: "ac-hotels", label: "AC Hotels", aliases: ["AC Hotel", "AC Hotels by Marriott"] },
  { key: "autograph", label: "Autograph Collection", aliases: ["Autograph"] },
  { key: "tribute", label: "Tribute Portfolio", aliases: ["Tribute"] },
  { key: "sheraton", label: "Sheraton", aliases: [] },
  { key: "westin", label: "Westin", aliases: ["The Westin"] },
  { key: "le-meridien", label: "Le Méridien", aliases: ["Le Meridien", "LeMeridien"] },
  { key: "st-regis", label: "St. Regis", aliases: ["St Regis", "The St. Regis"] },
  { key: "w-hotels", label: "W Hotels", aliases: ["W Hotel"] },
  { key: "luxury-collection", label: "The Luxury Collection", aliases: ["Luxury Collection"] },
  { key: "four-points", label: "Four Points", aliases: ["Four Points by Sheraton"] },
  { key: "aloft", label: "Aloft", aliases: [] },
  { key: "element", label: "Element", aliases: ["Element by Westin"] },
  { key: "moxy", label: "Moxy", aliases: ["Moxy Hotels"] },
  { key: "edition", label: "Edition", aliases: ["EDITION"] },
  { key: "design-hotels", label: "Design Hotels", aliases: [] },
  { key: "delta-hotels", label: "Delta Hotels", aliases: ["Delta"] },
  { key: "protea", label: "Protea Hotels", aliases: ["Protea"] },
  { key: "gaylord", label: "Gaylord", aliases: ["Gaylord Hotels"] },
]);

export const HILTON_BRANDS = Object.freeze([
  { key: "hilton-hotels", label: "Hilton Hotels & Resorts", aliases: ["Hilton Hotel", "Hilton"] },
  { key: "conrad", label: "Conrad", aliases: ["Conrad Hotels"] },
  { key: "waldorf-astoria", label: "Waldorf Astoria", aliases: ["Waldorf-Astoria"] },
  { key: "doubletree", label: "DoubleTree", aliases: ["Doubletree", "DoubleTree by Hilton"] },
  { key: "embassy-suites", label: "Embassy Suites", aliases: ["Embassy Suites by Hilton"] },
  { key: "hampton", label: "Hampton", aliases: ["Hampton Inn", "Hampton by Hilton", "Hampton Inn & Suites"] },
  { key: "hilton-garden-inn", label: "Hilton Garden Inn", aliases: ["Garden Inn"] },
  { key: "homewood-suites", label: "Homewood Suites", aliases: ["Homewood Suites by Hilton"] },
  { key: "home2-suites", label: "Home2 Suites", aliases: ["Home2 Suites by Hilton"] },
  { key: "curio", label: "Curio Collection", aliases: ["Curio", "Curio Collection by Hilton"] },
  { key: "tapestry", label: "Tapestry Collection", aliases: ["Tapestry", "Tapestry Collection by Hilton"] },
  { key: "canopy", label: "Canopy", aliases: ["Canopy by Hilton"] },
  { key: "lxr", label: "LXR", aliases: ["LXR Hotels"] },
  { key: "tempo", label: "Tempo", aliases: ["Tempo by Hilton"] },
  { key: "spark", label: "Spark", aliases: ["Spark by Hilton"] },
  { key: "motto", label: "Motto", aliases: ["Motto by Hilton"] },
  { key: "signia", label: "Signia", aliases: ["Signia by Hilton"] },
]);

export const HYATT_BRANDS = Object.freeze([
  { key: "hyatt-regency", label: "Hyatt Regency", aliases: [] },
  { key: "park-hyatt", label: "Park Hyatt", aliases: [] },
  { key: "grand-hyatt", label: "Grand Hyatt", aliases: [] },
  { key: "andaz", label: "Andaz", aliases: [] },
  { key: "hyatt-place", label: "Hyatt Place", aliases: [] },
  { key: "hyatt-house", label: "Hyatt House", aliases: [] },
  { key: "hyatt-centrics", label: "Hyatt Centric", aliases: ["Hyatt Centric"] },
  { key: "thompson", label: "Thompson Hotels", aliases: ["Thompson"] },
  { key: "destination", label: "Destination Hotels", aliases: ["Destination"] },
  { key: "alila", label: "Alila", aliases: [] },
  { key: "hyatt", label: "Hyatt", aliases: ["Hyatt Hotels"] },
]);

export const IHG_BRANDS = Object.freeze([
  { key: "holiday-inn", label: "Holiday Inn", aliases: ["Holiday Inn Express"] },
  { key: "crowne-plaza", label: "Crowne Plaza", aliases: [] },
  { key: "intercontinental", label: "InterContinental", aliases: ["Intercontinental"] },
  { key: "hotel-indigo", label: "Hotel Indigo", aliases: ["Indigo"] },
  { key: "staybridge", label: "Staybridge Suites", aliases: ["Staybridge"] },
  { key: "candlewood", label: "Candlewood Suites", aliases: ["Candlewood"] },
  { key: "voco", label: "voco", aliases: ["Voco"] },
  { key: "kimpton", label: "Kimpton", aliases: ["Kimpton Hotels"] },
]);

export const CHOICE_BRANDS = Object.freeze([
  { key: "comfort", label: "Comfort Inn", aliases: ["Comfort Suites", "Comfort"] },
  { key: "quality", label: "Quality Inn", aliases: ["Quality"] },
  { key: "sleep-inn", label: "Sleep Inn", aliases: [] },
  { key: "clarion", label: "Clarion", aliases: [] },
  { key: "ascend", label: "Ascend", aliases: ["Ascend Collection", "Ascend Hotel Collection"] },
  { key: "radisson", label: "Radisson", aliases: ["Radisson Blu", "Radisson RED"] },
]);

function escapeRe(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function brandPatterns(brands, family) {
  return brands.map((b) => {
    const names = [b.label, ...(b.aliases || [])].filter(Boolean);
    const re = new RegExp(`\\b(?:${names.map(escapeRe).join("|")})\\b`, "i");
    return { ...b, family, re };
  });
}

const MARRIOTT_PATTERNS = brandPatterns(MARRIOTT_BRANDS, "MARRIOTT");
const HILTON_PATTERNS = brandPatterns(HILTON_BRANDS, "HILTON");
const HYATT_PATTERNS = brandPatterns(HYATT_BRANDS, "HYATT");
const IHG_PATTERNS = brandPatterns(IHG_BRANDS, "IHG");
const CHOICE_PATTERNS = brandPatterns(CHOICE_BRANDS, "CHOICE");

/**
 * Detect brand affiliation from a hotel name or snippet.
 * Returns historical brand as disclosed — does not assume current brand.
 */
export function detectBrandFamily(text) {
  const s = String(text || "");
  if (!s) return { family: null, brandKey: null, brandLabel: null };
  for (const p of [
    ...MARRIOTT_PATTERNS,
    ...HILTON_PATTERNS,
    ...HYATT_PATTERNS,
    ...IHG_PATTERNS,
    ...CHOICE_PATTERNS,
  ]) {
    if (p.re.test(s)) {
      return {
        family: p.family,
        brandKey: p.key,
        brandLabel: p.label,
      };
    }
  }
  // Parent company mentions without brand
  if (/\bMarriott\b/i.test(s)) {
    return { family: "MARRIOTT", brandKey: "marriott-unspecified", brandLabel: "Marriott (unspecified)" };
  }
  if (/\bHilton\b/i.test(s)) {
    return { family: "HILTON", brandKey: "hilton-unspecified", brandLabel: "Hilton (unspecified)" };
  }
  if (/\bHyatt\b/i.test(s)) {
    return { family: "HYATT", brandKey: "hyatt-unspecified", brandLabel: "Hyatt (unspecified)" };
  }
  return { family: null, brandKey: null, brandLabel: null };
}

export function isMarriottOrHiltonText(text) {
  const d = detectBrandFamily(text);
  return d.family === "MARRIOTT" || d.family === "HILTON";
}

export function brandFamilySummary(hotels = []) {
  let marriott = 0;
  let hilton = 0;
  let other = 0;
  for (const h of hotels) {
    const fam =
      h.brandFamily ||
      detectBrandFamily([h.hotelName, h.historicalBrand, h.currentBrand].filter(Boolean).join(" "))
        .family;
    if (fam === "MARRIOTT") marriott += 1;
    else if (fam === "HILTON") hilton += 1;
    else other += 1;
  }
  return { marriott, hilton, other, total: hotels.length };
}
