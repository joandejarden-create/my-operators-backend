/**
 * Hotel identity quality classification for SEC discovery inventory.
 * Priority: reliable identities over raw name counts.
 */

import crypto from "node:crypto";
import { detectBrandFamily } from "./brand-families-v2.js";

export const HOTEL_IDENTITY_QUALITY_V2 = "sec-edgar-hotel-identity-quality-v2";

export const IDENTITY_CLASS = Object.freeze({
  VERIFIED_PROPERTY: "VERIFIED_PROPERTY",
  PROBABLE_PROPERTY: "PROBABLE_PROPERTY",
  AMBIGUOUS_PROPERTY: "AMBIGUOUS_PROPERTY",
  MULTIPLE_PROPERTIES: "MULTIPLE_PROPERTIES",
  GENERIC_BRAND_OR_COMPANY: "GENERIC_BRAND_OR_COMPANY",
  EXTRACTION_ARTIFACT: "EXTRACTION_ARTIFACT",
  DUPLICATE_ALIAS: "DUPLICATE_ALIAS",
});

const ARTIFACT_RE =
  /\b(comparable hotel|contents|blue chip|economic indicators|table of contents|hotel ownership total|see comparable|trs\.|total hotel ownership|section|item\s+\d|form\s+10|exhibit\s+10)\b/i;

const GENERIC_BRAND_ONLY_RE =
  /^(the\s+)?(ac hotels?|autograph collection|tribute portfolio|design hotels|courtyard(?:\s+by\s+marriott)?|residence inn(?:\s+by\s+marriott)?|springhill suites|towneplace suites|embassy suites(?:\s+by\s+hilton)?|hampton(?:\s+inn)?(?:\s+&\s+suites)?(?:\s+by\s+hilton)?|homewood suites|home2 suites|hilton garden inn|doubletree(?:\s+by\s+hilton)?|marriott(?:\s+hotels)?|hilton(?:\s+hotels)?|westin|sheraton|renaissance|hyatt(?:\s+regency|\s+place|\s+house)?|four seasons|hotel indigo|canopy|curio(?:\s+collection)?|tapestry(?:\s+collection)?|marriott bonvoy|marriott-managed hotels)$/i;

const MULTI_HOTEL_AND_RE =
  /\band\b(?!\s+spa\b)(?!\s+residences?\b)(?!\s+suites\b)/i;

const STATE_PREFIX_RE =
  /^(AZ|CA|NY|FL|TX|CO|WA|IL|MA|MD|NJ|NV|VA|PA|GA|NC|OR|OH|MI|TN|IN|WI|MN|MO|SC|LA|AL|KY|OK|CT|IA|UT|AR|MS|KS|NM|NE|ID|WV|HI|NH|ME|RI|MT|DE|SD|ND|AK|VT|WY|DC)\s+/i;

const RESORT_AND_SPA_OK =
  /\bresort\s+and\s+spa\b|\bresort\s+and\s+residences?\b|\binn\s+and\s+suites\b/i;

function normalizeKey(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeHotelDisplayName(name) {
  let n = String(name || "").replace(/\s+/g, " ").trim();
  n = n.replace(/[.\s]+$/, "").trim();
  // Strip leading state abbreviations used as table column bleed
  n = n.replace(STATE_PREFIX_RE, "").trim();
  // Drop trailing punctuation fragments
  n = n.replace(/\s+and\s+Mr\.?\s+C\.?$/i, "").trim();
  return n;
}

function looksLikeSecondHotel(segment) {
  const s = String(segment || "").trim();
  if (s.length < 6) return false;
  if (/^(spa|residences?|suites|golf|casino)\b/i.test(s)) return false;
  return detectBrandFamily(s).family != null || /\b(Hotel|Inn|Resort|Suites)\b/i.test(s);
}

/**
 * Classify a single candidate row (does not mutate source evidence).
 */
export function classifyHotelIdentity(row = {}) {
  const rawName = String(row.hotelName || row.name || "").trim();
  const normalizedName = normalizeHotelDisplayName(rawName);
  const brand = detectBrandFamily(normalizedName);
  const city = row.city ? String(row.city).trim() : null;
  const state = row.stateRegion || row.state ? String(row.stateRegion || row.state).trim() : null;
  const method = String(row.discoveryMethod || "");
  const reasons = [];

  if (!rawName) {
    return {
      identityClass: IDENTITY_CLASS.EXTRACTION_ARTIFACT,
      normalizedName: "",
      reasons: ["empty_name"],
      promotionEligible: false,
      brandFamily: null,
    };
  }

  if (ARTIFACT_RE.test(rawName) || ARTIFACT_RE.test(normalizedName)) {
    return {
      identityClass: IDENTITY_CLASS.EXTRACTION_ARTIFACT,
      normalizedName,
      rawName,
      reasons: ["artifact_language"],
      promotionEligible: false,
      brandFamily: brand.family,
    };
  }

  // Multi-hotel conjunction (not Resort and Spa)
  if (MULTI_HOTEL_AND_RE.test(rawName) && !RESORT_AND_SPA_OK.test(rawName)) {
    const parts = rawName.split(/\band\b/i).map((p) => p.trim());
    if (parts.length >= 2 && looksLikeSecondHotel(parts[1])) {
      return {
        identityClass: IDENTITY_CLASS.MULTIPLE_PROPERTIES,
        normalizedName,
        rawName,
        reasons: ["multi_hotel_conjunction"],
        splitCandidates: parts.filter(Boolean),
        promotionEligible: false,
        brandFamily: brand.family,
      };
    }
  }

  if (GENERIC_BRAND_ONLY_RE.test(normalizedName) || GENERIC_BRAND_ONLY_RE.test(rawName)) {
    return {
      identityClass: IDENTITY_CLASS.GENERIC_BRAND_OR_COMPANY,
      normalizedName,
      rawName,
      reasons: ["generic_brand_or_company_label"],
      promotionEligible: false,
      brandFamily: brand.family,
      note: "Never invent a property from brand (+ optional city) alone",
    };
  }

  // State-column bleed: "AZ Residence Inn"
  if (STATE_PREFIX_RE.test(rawName) && !city) {
    reasons.push("state_prefix_without_property_place");
    return {
      identityClass: IDENTITY_CLASS.AMBIGUOUS_PROPERTY,
      normalizedName,
      rawName,
      reasons,
      promotionEligible: false,
      brandFamily: brand.family,
    };
  }

  // Thin place tokens
  if (
    /^(Courtyard|Embassy Suites|Residence Inn|Marriott|Hilton|Hyatt Regency|Sheraton)\s+(Downtown|Airport)?$/i.test(
      normalizedName,
    )
  ) {
    return {
      identityClass: IDENTITY_CLASS.AMBIGUOUS_PROPERTY,
      normalizedName,
      rawName,
      reasons: ["incomplete_property_place"],
      promotionEligible: false,
      brandFamily: brand.family,
    };
  }

  const hasPlaceToken =
    Boolean(city) ||
    Boolean(state) ||
    /\b(Downtown|Airport|Harbor|Beach|Park|Square|Center|Centre|Valley|Hills|Bay|Island|Wharf|Mile)\b/i.test(
      normalizedName,
    ) ||
    // Distinctive place name after brand (e.g. Courtyard Basking Ridge)
    (brand.family &&
      normalizedName.split(/\s+/).length >= 3 &&
      !GENERIC_BRAND_ONLY_RE.test(normalizedName));

  const scheduleBacked =
    method === "FRANCHISE_EXPIRATION_SCHEDULE" ||
    method === "SCHEDULE_LINE_CITY_STATE" ||
    method === "SCHEDULE_LINE_HOTEL_CITY_STATE";

  const hasCityState = Boolean(city && state);
  const hasVerifiedContract = (row.contractFindings || []).some((f) =>
    String(f.verificationClass || "").startsWith("VERIFIED_"),
  );

  // VERIFIED_PROPERTY gate: schedule-backed + city/state + non-generic name
  if (scheduleBacked && hasCityState && hasPlaceToken && brand.family) {
    return {
      identityClass: IDENTITY_CLASS.VERIFIED_PROPERTY,
      normalizedName,
      rawName,
      reasons: ["schedule_backed_city_state_brand"],
      promotionEligible: true,
      brandFamily: brand.family,
      historicalBrand: brand.brandLabel,
      confidence: 0.92,
    };
  }

  if (scheduleBacked && hasCityState && hasPlaceToken) {
    return {
      identityClass: IDENTITY_CLASS.VERIFIED_PROPERTY,
      normalizedName,
      rawName,
      reasons: ["schedule_backed_city_state"],
      promotionEligible: true,
      brandFamily: brand.family,
      confidence: 0.9,
    };
  }

  if (hasPlaceToken && (hasCityState || (city && brand.family)) && normalizedName.length >= 10) {
    reasons.push("distinctive_name_with_location");
    if (hasVerifiedContract) reasons.push("has_contract_finding");
    return {
      identityClass: IDENTITY_CLASS.PROBABLE_PROPERTY,
      normalizedName,
      rawName,
      reasons,
      promotionEligible: false,
      brandFamily: brand.family,
      historicalBrand: brand.brandLabel,
      confidence: hasCityState ? 0.75 : 0.62,
      note: "PROBABLE — not promoted to VERIFIED without schedule/address confirmation",
    };
  }

  if (hasPlaceToken || brand.family) {
    return {
      identityClass: IDENTITY_CLASS.AMBIGUOUS_PROPERTY,
      normalizedName,
      rawName,
      reasons: reasons.length ? reasons : ["insufficient_location_or_distinctiveness"],
      promotionEligible: false,
      brandFamily: brand.family,
      confidence: 0.4,
    };
  }

  return {
    identityClass: IDENTITY_CLASS.EXTRACTION_ARTIFACT,
    normalizedName,
    rawName,
    reasons: ["unrecognized_or_non_property"],
    promotionEligible: false,
    brandFamily: null,
  };
}

/**
 * Canonical property key for dedupe (normalized name + city + state).
 */
export function propertyIdentityKey(row, classified) {
  const name = normalizeKey(classified?.normalizedName || row.hotelName);
  const city = normalizeKey(row.city);
  const state = normalizeKey(row.stateRegion || row.state);
  return `${name}|${city}|${state}`;
}

export function identityKeyHash(key) {
  return `prop_${crypto.createHash("sha1").update(key).digest("hex").slice(0, 16)}`;
}

/**
 * Classify a list of inventory hotels; mark duplicate aliases.
 */
export function classifyInventoryHotels(hotels = []) {
  const classified = hotels.map((h) => {
    const c = classifyHotelIdentity(h);
    return {
      ...h,
      rawHotelName: h.hotelName,
      hotelName: c.normalizedName || h.hotelName,
      identityClass: c.identityClass,
      identityReasons: c.reasons,
      promotionEligible: c.promotionEligible,
      brandFamily: c.brandFamily || h.brandFamily || null,
      historicalBrand: c.historicalBrand || h.historicalBrand || null,
      identityConfidence: c.confidence ?? null,
      identityNote: c.note || null,
      splitCandidates: c.splitCandidates || null,
      propertyKey: propertyIdentityKey(h, c),
      propertyId: identityKeyHash(propertyIdentityKey(h, c)),
    };
  });

  // Duplicate aliases: same propertyKey, keep richest location / schedule-backed as primary
  const byKey = new Map();
  for (const row of classified) {
    if (
      row.identityClass === IDENTITY_CLASS.EXTRACTION_ARTIFACT ||
      row.identityClass === IDENTITY_CLASS.GENERIC_BRAND_OR_COMPANY ||
      row.identityClass === IDENTITY_CLASS.MULTIPLE_PROPERTIES
    ) {
      continue;
    }
    const list = byKey.get(row.propertyKey) || [];
    list.push(row);
    byKey.set(row.propertyKey, list);
  }

  for (const [, list] of byKey) {
    if (list.length < 2) continue;
    list.sort((a, b) => scoreRichness(b) - scoreRichness(a));
    for (let i = 1; i < list.length; i++) {
      list[i].identityClass = IDENTITY_CLASS.DUPLICATE_ALIAS;
      list[i].promotionEligible = false;
      list[i].identityReasons = [
        ...(list[i].identityReasons || []),
        `alias_of:${list[0].propertyId}`,
      ];
      list[i].primaryPropertyId = list[0].propertyId;
    }
  }

  return classified;
}

function scoreRichness(h) {
  let s = 0;
  if (h.city) s += 3;
  if (h.stateRegion || h.state) s += 2;
  if (h.discoveryMethod === "FRANCHISE_EXPIRATION_SCHEDULE") s += 5;
  if ((h.contractFindings || []).length) s += 2;
  if (h.identityClass === IDENTITY_CLASS.VERIFIED_PROPERTY) s += 4;
  if (h.identityClass === IDENTITY_CLASS.PROBABLE_PROPERTY) s += 2;
  s += String(h.hotelName || "").length / 100;
  return s;
}

export function summarizeIdentityClasses(rows = []) {
  const counts = {};
  for (const c of Object.values(IDENTITY_CLASS)) counts[c] = 0;
  for (const r of rows) {
    const k = r.identityClass || "UNKNOWN";
    counts[k] = (counts[k] || 0) + 1;
  }
  return {
    total: rows.length,
    uniquePropertyKeys: new Set(
      rows
        .filter(
          (r) =>
            r.identityClass === IDENTITY_CLASS.VERIFIED_PROPERTY ||
            r.identityClass === IDENTITY_CLASS.PROBABLE_PROPERTY,
        )
        .map((r) => r.propertyKey),
    ).size,
    verifiedProperties: counts[IDENTITY_CLASS.VERIFIED_PROPERTY] || 0,
    probableProperties: counts[IDENTITY_CLASS.PROBABLE_PROPERTY] || 0,
    counts,
  };
}

/**
 * Manual sample precision helper: given reviewer labels for a sample of promoted rows.
 * Gate target: >= 0.95 for VERIFIED_PROPERTY. Do not weaken if unmet.
 */
export function evaluateVerifiedPrecision(sample = []) {
  const n = sample.length;
  if (!n) {
    return {
      sampleSize: 0,
      precision: null,
      meetsGate: false,
      gateThreshold: 0.95,
      note: "No labeled sample — do not claim 95% precision",
    };
  }
  const correct = sample.filter((s) => s.correct === true).length;
  const precision = correct / n;
  return {
    sampleSize: n,
    correct,
    precision,
    meetsGate: precision >= 0.95,
    gateThreshold: 0.95,
  };
}
