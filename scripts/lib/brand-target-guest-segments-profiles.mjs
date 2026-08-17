/**
 * Brand Setup - Brand Basics → Target Guest Segments (multi-select).
 * Values must match shared KEEP vocabulary (docs/target-guest-segment-vocabulary.md).
 *
 * Resolve: brand guest override → amenity-segment key → parent segment → default.
 */
import {
  AMENITY_SEGMENTS,
  BRAND_AMENITY_OVERRIDES,
  PARENT_AMENITY_SEGMENT,
} from "./brand-additional-amenities-profiles.mjs";
import { TARGET_GUEST_SEGMENT_BRAND_KEEP } from "./target-guest-segment-vocabulary.mjs";

/** @type {readonly string[]} */
export const TARGET_GUEST_SEGMENTS_ALLOWED = TARGET_GUEST_SEGMENT_BRAND_KEEP;

const G = Object.fromEntries(TARGET_GUEST_SEGMENTS_ALLOWED.map((x) => [x, x]));

function uniq(list) {
  const out = [];
  const seen = new Set();
  for (const x of list) {
    if (!x || !TARGET_GUEST_SEGMENTS_ALLOWED.includes(x) || seen.has(x)) continue;
    seen.add(x);
    out.push(x);
  }
  return out;
}

/** Guest presets aligned to amenity/building archetypes */
export const GUEST_SEGMENT_PRESETS = Object.freeze({
  economyLimited: uniq([
    G["Corporate / Business"],
    G["Leisure"],
    G["Family"],
  ]),
  midscaleSelect: uniq([
    G["Corporate / Business"],
    G["Leisure"],
    G["Bleisure"],
    G["Family"],
  ]),
  upperMidscale: uniq([
    G["Corporate / Business"],
    G["Leisure"],
    G["Bleisure"],
    G["Group / MICE"],
    G["Family"],
  ]),
  upscaleFullService: uniq([
    G["Corporate / Business"],
    G["Leisure"],
    G["Bleisure"],
    G["Group / MICE"],
    G["International Inbound"],
  ]),
  upperUpscaleLifestyle: uniq([
    G["Leisure"],
    G["Bleisure"],
    G["Experience-Oriented"],
    G["Solo Traveler"],
    G["Digital Nomad"],
    G["Corporate / Business"],
  ]),
  luxury: uniq([
    G["Luxury / Discerning"],
    G["Leisure"],
    G["Experience-Oriented"],
    G["International Inbound"],
  ]),
  luxuryResort: uniq([
    G["Luxury / Discerning"],
    G["Leisure"],
    G["Family"],
    G["Wellness Seeker"],
    G["Experience-Oriented"],
    G["International Inbound"],
  ]),
  allInclusive: uniq([
    G["Leisure"],
    G["Family"],
    G["Group / MICE"],
    G["International Inbound"],
    G["Wellness Seeker"],
  ]),
  extendedStay: uniq([
    G["Corporate / Business"],
    G["Contract / Extended Stay"],
    G["Bleisure"],
  ]),
  softBrandBoutique: uniq([
    G["Leisure"],
    G["Experience-Oriented"],
    G["Luxury / Discerning"],
    G["Solo Traveler"],
    G["Bleisure"],
  ]),
  membershipNetwork: uniq([
    G["Luxury / Discerning"],
    G["Leisure"],
    G["Experience-Oriented"],
    G["International Inbound"],
  ]),
  aparthotel: uniq([
    G["Corporate / Business"],
    G["Contract / Extended Stay"],
    G["Digital Nomad"],
    G["Bleisure"],
    G["Leisure"],
  ]),
});

/** Exact Brand Name → guest segments (highest priority) */
export const BRAND_GUEST_OVERRIDES = Object.freeze({
  // Soft / lifestyle / blanks
  onefinestay: uniq([
    G["Luxury / Discerning"],
    G["Leisure"],
    G["Experience-Oriented"],
    G["Family"],
    G["Staycation / Local"],
  ]),
  "Design Hotels": GUEST_SEGMENT_PRESETS.softBrandBoutique,
  "Four Seasons": GUEST_SEGMENT_PRESETS.luxuryResort,
  "The House of Originals": uniq([
    G["Leisure"],
    G["Experience-Oriented"],
    G["Solo Traveler"],
    G["Luxury / Discerning"],
  ]),
  "Kimpton Hotels": uniq([
    G["Leisure"],
    G["Bleisure"],
    G["Experience-Oriented"],
    G["Solo Traveler"],
    G["Corporate / Business"],
  ]),
  "Banyan Tree": GUEST_SEGMENT_PRESETS.luxuryResort,
  Angsana: GUEST_SEGMENT_PRESETS.luxuryResort,
  "Caption by Hyatt": uniq([
    G["Leisure"],
    G["Bleisure"],
    G["Digital Nomad"],
    G["Solo Traveler"],
    G["Experience-Oriented"],
  ]),
  "Mr & Mrs Smith": GUEST_SEGMENT_PRESETS.membershipNetwork,
  "Orient Express": uniq([
    G["Luxury / Discerning"],
    G["Leisure"],
    G["Experience-Oriented"],
    G["International Inbound"],
  ]),
  "Iberostar Waves": GUEST_SEGMENT_PRESETS.allInclusive,
  "Spark by Hilton": GUEST_SEGMENT_PRESETS.economyLimited,
  "DoubleTree by Hilton": GUEST_SEGMENT_PRESETS.upscaleFullService,
  "Cambridge Beaches": uniq([
    G["Luxury / Discerning"],
    G["Leisure"],
    G["Family"],
    G["Wellness Seeker"],
    G["Experience-Oriented"],
  ]),
  citizenM: uniq([
    G["Corporate / Business"],
    G["Bleisure"],
    G["Solo Traveler"],
    G["Digital Nomad"],
    G["Leisure"],
  ]),
  "Urban Cowboy": uniq([
    G["Leisure"],
    G["Experience-Oriented"],
    G["Solo Traveler"],
    G["Staycation / Local"],
  ]),
  NoMad: uniq([
    G["Luxury / Discerning"],
    G["Leisure"],
    G["Experience-Oriented"],
    G["Bleisure"],
  ]),
  "Hilton Garden Inn": GUEST_SEGMENT_PRESETS.upperMidscale,
  "The Ritz-Carlton Reserve": GUEST_SEGMENT_PRESETS.luxuryResort,
  "Hampton by Hilton": GUEST_SEGMENT_PRESETS.midscaleSelect,
  "AmericInn by Wyndham": GUEST_SEGMENT_PRESETS.midscaleSelect,
  AutoCamp: uniq([
    G["Leisure"],
    G["Experience-Oriented"],
    G["Family"],
    G["Solo Traveler"],
    G["Wellness Seeker"],
  ]),
  Aman: GUEST_SEGMENT_PRESETS.luxuryResort,
  Wayfinder: uniq([
    G["Leisure"],
    G["Experience-Oriented"],
    G["Solo Traveler"],
    G["Digital Nomad"],
    G["Staycation / Local"],
  ]),

  // Best Western family blanks
  "Best Western": GUEST_SEGMENT_PRESETS.midscaleSelect,
  "Best Western Plus": GUEST_SEGMENT_PRESETS.upperMidscale,
  "Best Western Premier": GUEST_SEGMENT_PRESETS.upscaleFullService,
  "Vīb by Best Western": GUEST_SEGMENT_PRESETS.upperUpscaleLifestyle,
  "SureStay Collection by Best Western": GUEST_SEGMENT_PRESETS.softBrandBoutique,
  "SureStay Hotel by Best Western": GUEST_SEGMENT_PRESETS.economyLimited,
  "Glō by Best Western": GUEST_SEGMENT_PRESETS.economyLimited,
  "Aiden by Best Western": GUEST_SEGMENT_PRESETS.upperUpscaleLifestyle,
  "SureStay Studio": GUEST_SEGMENT_PRESETS.extendedStay,
  "BW Signature Collection": GUEST_SEGMENT_PRESETS.softBrandBoutique,
  "SureStay Plus Hotel by Best Western": GUEST_SEGMENT_PRESETS.midscaleSelect,
  "Executive Residency by Best Western": GUEST_SEGMENT_PRESETS.extendedStay,
  "BW Premier Collection": GUEST_SEGMENT_PRESETS.softBrandBoutique,

  // Accor blanks
  "Adagio Aparthotel Extra": GUEST_SEGMENT_PRESETS.aparthotel,

  // Choice / Radisson Individuals
  "Radisson Individuals by Choice": uniq([
    G["Luxury / Discerning"],
    G["Leisure"],
    G["Experience-Oriented"],
  ]),
  "Ascend Hotel Collection": GUEST_SEGMENT_PRESETS.softBrandBoutique,
  Clarion: uniq([
    G["Corporate / Business"],
    G["Leisure"],
    G["Group / MICE"],
    G["Bleisure"],
  ]),
  "Gaylord Hotels": uniq([
    G["Group / MICE"],
    G["Leisure"],
    G["Family"],
    G["Corporate / Business"],
  ]),
  "Embassy Suites by Hilton": uniq([
    G["Corporate / Business"],
    G["Leisure"],
    G["Family"],
    G["Group / MICE"],
    G["Bleisure"],
  ]),
  Westin: uniq([
    G["Corporate / Business"],
    G["Leisure"],
    G["Wellness Seeker"],
    G["Bleisure"],
    G["International Inbound"],
  ]),
  "Even Hotels": uniq([
    G["Corporate / Business"],
    G["Wellness Seeker"],
    G["Bleisure"],
    G["Leisure"],
  ]),
  "Six Senses Hotels Resorts Spas": uniq([
    G["Luxury / Discerning"],
    G["Wellness Seeker"],
    G["Leisure"],
    G["Experience-Oriented"],
    G["International Inbound"],
  ]),
});

function sameList(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
  const aa = [...a].sort();
  const bb = [...b].sort();
  return aa.every((x, i) => x === bb[i]);
}

function amenityListToSegmentKey(list) {
  if (!list) return null;
  for (const [key, seg] of Object.entries(AMENITY_SEGMENTS)) {
    if (list === seg || sameList(list, seg)) return key;
  }
  return null;
}

/**
 * @param {string} brandName
 * @param {string} [parentCompany]
 * @returns {{ segments: string[], resolveSource: string, segment: string|null }}
 */
export function getBrandTargetGuestSegments(brandName, parentCompany = "") {
  const name = String(brandName || "").trim();
  const parent = String(parentCompany || "").trim();

  if (BRAND_GUEST_OVERRIDES[name]) {
    return {
      segments: [...BRAND_GUEST_OVERRIDES[name]],
      resolveSource: "brand_guest_override",
      segment: null,
    };
  }

  const amenityList = BRAND_AMENITY_OVERRIDES[name];
  if (amenityList) {
    const key = amenityListToSegmentKey(amenityList);
    if (key && GUEST_SEGMENT_PRESETS[key]) {
      return {
        segments: [...GUEST_SEGMENT_PRESETS[key]],
        resolveSource: "brand_amenity_segment",
        segment: key,
      };
    }
  }

  const parentKey = PARENT_AMENITY_SEGMENT[parent];
  if (parentKey && GUEST_SEGMENT_PRESETS[parentKey]) {
    return {
      segments: [...GUEST_SEGMENT_PRESETS[parentKey]],
      resolveSource: "parent_segment",
      segment: parentKey,
    };
  }

  return {
    segments: [...GUEST_SEGMENT_PRESETS.midscaleSelect],
    resolveSource: "default",
    segment: "midscaleSelect",
  };
}
