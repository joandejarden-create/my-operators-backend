/**
 * Apify first-party hotel-company extractors.
 *
 * Apify is the extraction method, never the origin. Provenance is:
 *   hilton.com → Apify Hilton Actor → Dealality
 *
 * Actors are not HIGH until they pass the validation gate.
 */
import { CALA_DISCOVERY_COUNTRY_ISO } from "./census-autopilot-cala-discovery-shared.js";
import {
  normalizeCountry,
  normalizeText,
  websiteHost,
} from "../independent-census/match-current-census.js";
import { MAP_MASTER } from "./master-census-enrichment-v1.js";
import { MAP_BRAND } from "./master-brand-portfolio-validation-v1.js";
import { MAP_ROOMS } from "./production-census-rooms-keys-queue.js";
import { MAP_ROOMS_SOURCE_TYPE } from "./census-secondary-hotel-data-policy.js";
import {
  assertRoomsSourcePolicy,
  classifyNullFill,
} from "./property-fundamentals-enrichment-v1.js";
import { rejectNonGuestroomSemantics } from "./rooms-candidate-corroboration-v1.js";
import {
  lookupCanonicalBrand,
} from "./census-brand-canonical-dictionary.js";
import { resolveBrandMappingAlias } from "./brand-mapping-gap-repair-v1.js";

export const APIFY_FIRST_PARTY_VERSION = "apify-first-party-extractor-v1";

export const SOURCE_CLASS = "APIFY_FIRST_PARTY_EXTRACTOR";

export const APIFY_USAGE_STATUS = Object.freeze({
  UNTESTED: "APIFY_UNTESTED",
  VALIDATING: "APIFY_VALIDATING",
  APPROVED: "APIFY_APPROVED_FIRST_PARTY_EXTRACTOR",
  CANDIDATE_ONLY: "APIFY_CANDIDATE_ONLY",
  REJECTED: "APIFY_REJECTED",
  USAGE_REVIEW: "APIFY_USAGE_REVIEW",
});

const CALA_ISO = new Set(
  Object.values(CALA_DISCOVERY_COUNTRY_ISO).map((c) => String(c).toUpperCase())
);
const CALA_NAMES = new Set(
  Object.keys(CALA_DISCOVERY_COUNTRY_ISO).map((c) => c.toLowerCase())
);

export const LIVE_SOURCE_PRIORITY = Object.freeze([
  "approved_bulk_government_registries",
  "approved_apify_first_party_extractors",
  "direct_official_structured_apis_directories",
  "direct_official_portfolio_crawling",
  "official_property_pages_pdfs",
  "two_source_corroboration",
  "residual_research",
]);

/** Catalog of hotel-company Actors. IDs are Store names, not invented fields. */
export const APIFY_HOTEL_ACTOR_CATALOG = Object.freeze([
  {
    ACTOR_ID: "jungle_synthesizer/hilton-honors-directory-rates-points-tier-scraper",
    ACTOR_NAME: "Hilton Honors Hotel Directory — Rates, Points & Brand Tiers",
    HOTEL_COMPANY: "Hilton",
    UNDERLYING_SOURCE: "hilton.com",
    FIRST_PARTY: true,
    PRIORITY: 1,
    PRICE_MODEL: "PPE $0.10 start/GB + $0.001/record",
    COST_PER_1000_USD: 1.1,
    EXPECTED_CALA_YIELD: "medium — no country filter; post-filter CALA from global directory",
    ROOMS_SEMANTICS: "total_rooms advertised as property guestroom count from hilton.com SSR/JSON-LD",
    COORDINATE_POLICY: "test_then_null_fill",
    SAMPLE_INPUT: {
      maxItems: 40,
      snapshotDates: ["2026-11-15"],
      sp_intended_usage: "Dealality Hotel Property Census first-party identity/rooms validation (CALA)",
    },
    MAX_CHARGE_USD: 0.35,
    MEMORY_MB: 1024,
  },
  {
    ACTOR_ID: "jungle_synthesizer/marriott-bonvoy-directory-rates-award-category-scraper",
    ACTOR_NAME: "Marriott Bonvoy Hotel Directory & Brand Database Scraper",
    HOTEL_COMPANY: "Marriott",
    UNDERLYING_SOURCE: "marriott.com",
    FIRST_PARTY: true,
    PRIORITY: 1,
    PRICE_MODEL: "PPE $0.10 start/GB + $0.002/record",
    COST_PER_1000_USD: 2.1,
    EXPECTED_CALA_YIELD: "high — countryFilter ISO supports MX/BR/CO/etc.",
    ROOMS_SEMANTICS: "total_rooms reserved; often null until Actor roadmap lands",
    COORDINATE_POLICY: "test_then_null_fill",
    SAMPLE_INPUT: {
      maxItems: 40,
      countryFilter: "MX",
      sp_intended_usage: "Dealality Hotel Property Census first-party identity validation (Mexico CALA)",
    },
    MAX_CHARGE_USD: 0.35,
    MEMORY_MB: 1024,
  },
  {
    ACTOR_ID: "scrapyspider/marriott-hotel-search",
    ACTOR_NAME: "Marriott Hotel Search Scraper",
    HOTEL_COMPANY: "Marriott",
    UNDERLYING_SOURCE: "marriott.com GraphQL",
    FIRST_PARTY: true,
    PRIORITY: 2,
    PRICE_MODEL: "free Actor + platform compute",
    COST_PER_1000_USD: null,
    EXPECTED_CALA_YIELD: "unknown — inferred output is pagination metadata, not property rows",
    DEFAULT_STATUS: APIFY_USAGE_STATUS.USAGE_REVIEW,
    SKIP_LIVE_SAMPLE: true,
    SKIP_REASON: "output_schema_is_offset_label_region_not_hotels; worldwide run not started",
  },
  {
    ACTOR_ID: "memo23/choicehotels-scraper",
    ACTOR_NAME: "Choice Hotels Scraper — rates, ratings & amenities",
    HOTEL_COMPANY: "Choice",
    UNDERLYING_SOURCE: "choicehotels.com",
    FIRST_PARTY: true,
    PRIORITY: 1,
    PRICE_MODEL: "PPE $0.005 start + $0.004/hotel + residential proxy",
    COST_PER_1000_USD: 4.0,
    EXPECTED_CALA_YIELD: "medium — city search, not full directory; CALA via locations",
    ROOMS_SEMANTICS: "rooms not exposed",
    COORDINATE_POLICY: "test_then_null_fill",
    SAMPLE_INPUT: {
      locations: ["Cancun, Quintana Roo", "Mexico City, Mexico"],
      maxItems: 40,
      maxConcurrency: 1,
    },
    MAX_CHARGE_USD: 0.4,
    MEMORY_MB: 1024,
  },
  {
    ACTOR_ID: "axlymxp/ihg-hotel-scraper",
    ACTOR_NAME: "IHG Hotel Scraper",
    HOTEL_COMPANY: "IHG",
    UNDERLYING_SOURCE: "ihg.com official JSON API",
    FIRST_PARTY: true,
    PRIORITY: 1,
    PRICE_MODEL: "PPE $0.01 start + $0.005/hotel",
    COST_PER_1000_USD: 5.01,
    EXPECTED_CALA_YIELD: "medium — destination search, not full directory",
    ROOMS_SEMANTICS: "rooms_available is live inventory, NOT Rooms/Keys",
    FORBID_ROOMS_FIELD: "rooms_available",
    COORDINATE_POLICY: "test_then_null_fill",
    SAMPLE_INPUT: {
      location: "Cancun",
      maxItems: 40,
      includeRoomRates: false,
      includeSoldOut: true,
    },
    HARVEST_LOCATIONS: [
      "Cancun",
      "Mexico City",
      "Guadalajara",
      "Monterrey",
      "Merida",
      "Bogota",
      "Cartagena",
      "San Jose Costa Rica",
      "Panama City",
      "Santo Domingo",
      "Punta Cana",
      "Lima",
      "Sao Paulo",
    ],
    MAX_CHARGE_USD: 0.35,
    MEMORY_MB: 1024,
  },
  {
    ACTOR_ID: "moving_beacon-owner1/ihg-hotel-scraper",
    ACTOR_NAME: "IHG Hotel Scraper (search-results HTML)",
    HOTEL_COMPANY: "IHG",
    UNDERLYING_SOURCE: "ihg.com",
    FIRST_PARTY: true,
    PRIORITY: 3,
    PRICE_MODEL: "PPE $0.01/result",
    DEFAULT_STATUS: APIFY_USAGE_STATUS.CANDIDATE_ONLY,
    SKIP_LIVE_SAMPLE: true,
    SKIP_REASON: "higher_cost_html_scraper; prefer axlymxp official API Actor",
  },
  {
    ACTOR_ID: "getdataforme/ihg-hotels-discovery-scraper",
    ACTOR_NAME: "IHG Hotels Discovery Scraper",
    HOTEL_COMPANY: "IHG",
    UNDERLYING_SOURCE: "ihg.com",
    FIRST_PARTY: true,
    PRIORITY: 3,
    PRICE_MODEL: "PPE $0.05 start + $0.009/result",
    DEFAULT_STATUS: APIFY_USAGE_STATUS.CANDIDATE_ONLY,
    SKIP_LIVE_SAMPLE: true,
    SKIP_REASON: "higher_cost_than_official_api_actor",
  },
  {
    ACTOR_ID: "dataquarry/hotels-lodging",
    ACTOR_NAME: "Hotels & Lodging Scraper – OpenStreetMap Hotel Data",
    HOTEL_COMPANY: null,
    UNDERLYING_SOURCE: "openstreetmap.org",
    FIRST_PARTY: false,
    PRIORITY: 9,
    PRICE_MODEL: "PPE ~$0.003/result",
    DEFAULT_STATUS: APIFY_USAGE_STATUS.CANDIDATE_ONLY,
    SKIP_LIVE_SAMPLE: true,
    SKIP_REASON: "not_first_party; OSM rooms may corroborate only under two-source policy",
    EXPECTED_CALA_YIELD: "corroboration only",
  },
  {
    ACTOR_ID: "api-empire/google-hotels-scraper",
    ACTOR_NAME: "Google Hotels Scraper With Review Insights",
    HOTEL_COMPANY: null,
    UNDERLYING_SOURCE: "google.com/travel/hotels",
    FIRST_PARTY: false,
    PRIORITY: 9,
    DEFAULT_STATUS: APIFY_USAGE_STATUS.CANDIDATE_ONLY,
    SKIP_LIVE_SAMPLE: true,
    SKIP_REASON: "not_first_party; Google Hotels never HIGH brand/rooms",
  },
  {
    ACTOR_ID: "flamboyant_liner/fourseasons-properties-scraper",
    ACTOR_NAME: "Four Seasons Properties Scraper",
    HOTEL_COMPANY: "Four Seasons",
    UNDERLYING_SOURCE: "fourseasons.com",
    FIRST_PARTY: true,
    PRIORITY: 2,
    PRICE_MODEL: "PPE $0.05 start/GB + $0.008/record",
    COST_PER_1000_USD: 8.05,
    EXPECTED_CALA_YIELD: "low volume — country filter MX/CR/DO possible",
    ROOMS_SEMANTICS: "rooms not exposed",
    COORDINATE_POLICY: "test_then_null_fill",
    SAMPLE_INPUT: {
      countries: ["MX", "CR", "DO"],
      maxItems: 25,
    },
    MAX_CHARGE_USD: 0.35,
    MEMORY_MB: 1024,
  },
  {
    ACTOR_ID: "getdataforme/accor-urls-scraper",
    ACTOR_NAME: "Accor Urls Scraper",
    HOTEL_COMPANY: "Accor",
    UNDERLYING_SOURCE: "all.accor.com",
    FIRST_PARTY: true,
    PRIORITY: 2,
    PRICE_MODEL: "PPE $0.05 start + $0.009/record",
    COST_PER_1000_USD: 9.05,
    EXPECTED_CALA_YIELD: "requires known all.accor.com hotel URLs; not a directory crawl",
    DEFAULT_STATUS: APIFY_USAGE_STATUS.UNTESTED,
    SKIP_LIVE_SAMPLE: true,
    SKIP_REASON: "needs_startUrls_from_known_CALA_Accor_pages; category Actor has no maxItems",
  },
]);

export const COMPANIES_WITHOUT_FIRST_PARTY_ACTOR = Object.freeze([
  "Hyatt",
  "Wyndham",
  "Best Western",
  "Radisson",
  "Meliá",
  "Barceló",
  "RIU",
  "Iberostar",
  "Palladium",
  "Minor",
]);

function isBlank(v) {
  return v == null || String(v).trim() === "";
}

export function isUsableOfficialPhone(phone) {
  const s = String(phone || "").trim();
  if (!s) return false;
  if (/800\s*000/i.test(s)) return false;
  if (/central\s+reserv/i.test(s)) return false;
  const digits = s.replace(/\D/g, "");
  if (digits.length < 7) return false;
  return true;
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function normKey(s) {
  return normalizeText(String(s || ""))
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function actorRefForApi(actorId) {
  return String(actorId || "").replace("/", "~");
}

export function isCalaCountry(country, countryCode) {
  const code = String(countryCode || "").trim().toUpperCase();
  if (code && CALA_ISO.has(code)) return true;
  const name = String(country || "").trim().toLowerCase();
  if (name && CALA_NAMES.has(name)) return true;
  const norm = normalizeCountry(country);
  if (norm && CALA_NAMES.has(String(norm).toLowerCase())) return true;
  return false;
}

export function buildApifyProvenance(actor, extra = {}) {
  return {
    source_class: SOURCE_CLASS,
    underlying_source: actor?.UNDERLYING_SOURCE || extra.underlying_source || null,
    extraction_method: "Apify Actor",
    actor_id: actor?.ACTOR_ID || extra.actor_id || null,
    hotel_company: actor?.HOTEL_COMPANY || extra.hotel_company || null,
    chain: [
      actor?.UNDERLYING_SOURCE || extra.underlying_source,
      `Apify ${(actor?.HOTEL_COMPANY || extra.hotel_company || "hotel").toString()} Actor`,
      "Dealality",
    ].filter(Boolean).join(" → "),
    apify_is_not_the_origin: true,
  };
}

function pick(...vals) {
  for (const v of vals) {
    if (!isBlank(v)) return v;
  }
  return null;
}

function numOrNull(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function flattenAddress(addr) {
  if (!addr) return null;
  if (typeof addr === "string") return addr;
  return pick(addr.line1, addr.street, addr.addressLine1, addr.formatted);
}

function countryNameFromIso(code) {
  const c = String(code || "").trim().toUpperCase();
  if (!c) return null;
  for (const [name, iso] of Object.entries(CALA_DISCOVERY_COUNTRY_ISO)) {
    if (String(iso).toUpperCase() === c) return name;
  }
  return null;
}

export function normalizeApifyHotelRow(actor, raw = {}) {
  const id = actor?.ACTOR_ID || "";
  const company = actor?.HOTEL_COMPANY || null;
  let row = {
    actor_id: id,
    company,
    underlying_source: actor?.UNDERLYING_SOURCE || null,
    name: null,
    brand: null,
    brand_code: null,
    property_code: null,
    url: null,
    address: null,
    city: null,
    state: null,
    postal: null,
    country: null,
    country_code: null,
    lat: null,
    lng: null,
    phone: null,
    rooms: null,
    rooms_field: null,
    rooms_semantics: actor?.ROOMS_SEMANTICS || null,
    rooms_forbidden: false,
    scraped_at: raw.scraped_at || raw.scrapedAt || null,
    raw_keys: Object.keys(raw || {}),
  };

  if (id.includes("hilton-honors-directory")) {
    row = {
      ...row,
      name: pick(raw.property_name, raw.propertyName),
      brand: pick(raw.brand_name, raw.brandName),
      brand_code: pick(raw.brand_code),
      property_code: pick(raw.hilton_ctyhocn, raw.ctyhocn),
      url: pick(
        raw.property_url,
        raw.url,
        raw.hilton_ctyhocn
          ? `https://www.hilton.com/en/hotels/${String(raw.hilton_ctyhocn).toLowerCase()}/`
          : null
      ),
      address: pick(raw.address_line, raw.address),
      city: pick(raw.city),
      state: pick(raw.state_province, raw.state),
      postal: pick(raw.postal_code, raw.postal),
      country: pick(raw.country),
      country_code: pick(raw.country_code),
      lat: numOrNull(raw.lat),
      lng: numOrNull(raw.lng),
      phone: pick(raw.phone),
      rooms: numOrNull(raw.total_rooms),
      rooms_field: "total_rooms",
    };
  } else if (id.includes("marriott-bonvoy-directory")) {
    row = {
      ...row,
      name: pick(raw.property_name),
      brand: pick(raw.brand_name),
      brand_code: pick(raw.brand_code),
      property_code: pick(raw.marriott_id, raw.marsha),
      url: pick(raw.property_url),
      address: flattenAddress(raw.address) || pick(raw.address),
      city: pick(raw.city),
      state: pick(raw.state_province, raw.state),
      postal: pick(raw.postal_code, raw.postal),
      country: pick(raw.country),
      country_code: pick(raw.country_code),
      lat: numOrNull(raw.lat),
      lng: numOrNull(raw.lng),
      phone: pick(raw.phone),
      rooms: numOrNull(raw.total_rooms),
      rooms_field: "total_rooms",
    };
  } else if (id.includes("choicehotels-scraper")) {
    const addr = raw.address || {};
    row = {
      ...row,
      name: pick(raw.name, raw.productName),
      brand: pick(raw.brandName, raw.brand_name),
      brand_code: pick(raw.brandCode, raw.brand_code),
      property_code: pick(raw.hotelCode, raw.hotel_code),
      url: pick(
        raw.sourceUrl,
        raw.hotelCode
          ? `https://www.choicehotels.com/hotel/${raw.hotelCode}`
          : null
      ),
      address: flattenAddress(addr),
      city: pick(addr.city),
      state: pick(addr.subdivision, addr.state),
      postal: pick(addr.postalCode),
      country: pick(addr.country),
      country_code: pick(addr.countryCode),
      lat: numOrNull(raw.latitude),
      lng: numOrNull(raw.longitude),
      phone: pick(raw.phone),
      rooms: null,
      rooms_field: null,
    };
  } else if (id.includes("fourseasons-properties")) {
    row = {
      ...row,
      name: pick(raw.name, raw.title),
      brand: pick(raw.brandIcsId, "Four Seasons"),
      brand_code: pick(raw.brandIcsId, raw.code),
      property_code: pick(raw.code, raw.axpPropertyId, raw.aemId),
      url: pick(raw.websiteUrl, raw.contactUsUrl),
      address: pick(raw.address),
      city: pick(raw.city),
      state: pick(raw.state),
      postal: pick(raw.zip, raw.postal),
      country: pick(raw.country),
      country_code: pick(raw.countryCode),
      lat: numOrNull(raw.lat),
      lng: numOrNull(raw.lng),
      phone: pick(raw.phone),
      rooms: null,
      rooms_field: null,
    };
  } else if (id.includes("accor-urls-scraper") || id.includes("accor-category")) {
    const loc = raw.location || {};
    row = {
      ...row,
      name: pick(raw.name),
      brand: pick(raw.brand),
      property_code: pick(raw.hotelId),
      url: pick(raw.url),
      address: pick(loc.address, raw.address),
      city: pick(loc.city, raw.city),
      postal: pick(loc.zipCode, raw.zip),
      country: pick(loc.country, raw.country),
      country_code: pick(loc.countryCode),
      lat: numOrNull(loc.coordinates?.lat ?? raw.lat),
      lng: numOrNull(loc.coordinates?.lng ?? raw.lng),
      phone: pick(raw.contact?.phone, raw.phone),
      rooms: null,
      rooms_field: "offers",
      rooms_forbidden: true,
      rooms_semantics: "Accor offers are room-type inventory, not Rooms/Keys",
    };
  } else if (id.includes("ihg-hotel-scraper") || id.includes("ihg-hotels-discovery")) {
    row = {
      ...row,
      name: pick(raw.name, raw.hotel_name),
      brand: pick(raw.brand_name, raw.brandName),
      brand_code: pick(raw.brand_code),
      property_code: pick(raw.hotel_code, raw.hotelCode),
      url: pick(raw.url),
      address: pick(raw.street, raw.address),
      city: pick(raw.city),
      state: pick(raw.state, raw.state_code),
      postal: pick(raw.postal_code, raw.postalCode),
      country: pick(raw.country),
      country_code: pick(raw.country_code),
      lat: numOrNull(raw.latitude ?? raw.lat),
      lng: numOrNull(raw.longitude ?? raw.lng),
      phone: pick(raw.phone),
      rooms: null,
      rooms_field: "rooms_available",
      rooms_forbidden: true,
      rooms_semantics: "rooms_available is live inventory, not hotel guestrooms",
    };
  } else {
    row = {
      ...row,
      name: pick(raw.name, raw.property_name, raw.hotelName),
      brand: pick(raw.brand, raw.brand_name, raw.brandName),
      property_code: pick(raw.id, raw.hotel_id, raw.property_code),
      url: pick(raw.url, raw.website, raw.property_url),
      address: flattenAddress(raw.address) || pick(raw.street),
      city: pick(raw.city, raw.address?.city),
      state: pick(raw.state, raw.address?.state),
      postal: pick(raw.postal, raw.postal_code, raw.address?.postalCode),
      country: pick(raw.country, raw.address?.country),
      lat: numOrNull(raw.lat ?? raw.latitude),
      lng: numOrNull(raw.lng ?? raw.longitude),
      phone: pick(raw.phone),
    };
  }

  row.country = row.country || countryNameFromIso(row.country_code);
  row.country_norm = normalizeCountry(row.country || row.country_code);
  row.city_key = normKey(row.city);
  row.name_key = normKey(row.name);
  row.host = websiteHost(row.url);
  row.cala = isCalaCountry(row.country, row.country_code);
  row.provenance = buildApifyProvenance(actor);
  return row;
}

export function fieldAvailabilityFromRows(rows = []) {
  const n = rows.length || 1;
  const has = (fn) => rows.filter(fn).length / n;
  return {
    BRAND_AVAILABLE: has((r) => !isBlank(r.brand)) >= 0.5,
    ROOMS_AVAILABLE: has((r) => r.rooms != null && !r.rooms_forbidden) >= 0.2,
    ADDRESS_AVAILABLE: has((r) => !isBlank(r.address)) >= 0.5,
    POSTAL_AVAILABLE: has((r) => !isBlank(r.postal)) >= 0.3,
    LAT_LONG_AVAILABLE: has((r) => r.lat != null && r.lng != null) >= 0.5,
    PHONE_AVAILABLE: has((r) => !isBlank(r.phone)) >= 0.3,
    WEBSITE_AVAILABLE: has((r) => !isBlank(r.url)) >= 0.5,
    PROPERTY_ID_AVAILABLE: has((r) => !isBlank(r.property_code)) >= 0.5,
    FIELDS_AVAILABLE: [
      "Current Brand",
      "Brand Family derived",
      "Address",
      "City",
      "State/Region",
      "Postal Code",
      "Latitude",
      "Longitude",
      "Phone",
      "Website",
      "Rooms / Keys",
      "Property ID",
    ].filter(Boolean),
  };
}

export function haversineMeters(a, b) {
  if (
    a?.lat == null ||
    a?.lng == null ||
    b?.lat == null ||
    b?.lng == null
  ) {
    return null;
  }
  const R = 6371000;
  const toRad = (d) => (Number(d) * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLon = toRad(b.lng - a.lng);
  const x =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(a.lat)) * Math.cos(toRad(b.lat)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(x)));
}

export function similarText(a, b) {
  const x = normKey(a);
  const y = normKey(b);
  if (!x || !y) return 0;
  if (x === y) return 1;
  if (x.includes(y) || y.includes(x)) return 0.9;
  const as = new Set(x.split(" "));
  const bs = new Set(y.split(" "));
  let inter = 0;
  for (const t of as) if (bs.has(t)) inter += 1;
  return inter / Math.max(as.size, bs.size);
}

export const APPROVAL_THRESHOLDS = Object.freeze({
  min_returned: 15,
  min_high_matches: 5,
  identity_accuracy: 0.8,
  brand_accuracy: 0.9,
  rooms_accuracy: 0.85,
  address_accuracy: 0.75,
  coordinate_meters: 250,
  coordinate_accuracy: 0.8,
});

/**
 * Approve only when underlying origin is first-party, matching is reliable,
 * field semantics are understood, and tested fields match Census/official evidence.
 */
export function evaluateActorApproval(actor, metrics = {}) {
  const reasons = [];
  if (actor?.DEFAULT_STATUS === APIFY_USAGE_STATUS.REJECTED) {
    return { ok: false, status: APIFY_USAGE_STATUS.REJECTED, reasons: ["catalog_rejected"] };
  }
  if (actor?.FIRST_PARTY !== true) {
    return {
      ok: false,
      status: APIFY_USAGE_STATUS.CANDIDATE_ONLY,
      reasons: ["not_first_party_underlying_source"],
    };
  }
  if (actor?.SKIP_LIVE_SAMPLE) {
    return {
      ok: false,
      status: actor.DEFAULT_STATUS || APIFY_USAGE_STATUS.USAGE_REVIEW,
      reasons: [actor.SKIP_REASON || "sample_skipped"],
    };
  }
  if (!actor?.UNDERLYING_SOURCE) {
    reasons.push("underlying_source_unidentified");
  }
  const returned = Number(metrics.SAMPLE_SIZE || 0);
  const high = Number(metrics.HIGH_MATCHES || 0);
  const identity = Number(metrics.IDENTITY_ACCURACY || 0);
  const brand = Number(metrics.BRAND_ACCURACY || 0);
  const stability = String(metrics.TECHNICAL_STABILITY || "").toUpperCase();
  if (stability && stability !== "SUCCEEDED") reasons.push("technical_stability_not_succeeded");
  if (returned < APPROVAL_THRESHOLDS.min_returned) reasons.push("sample_too_small");
  if (high < APPROVAL_THRESHOLDS.min_high_matches) reasons.push("too_few_high_identity_matches");
  if (identity < APPROVAL_THRESHOLDS.identity_accuracy && high < 12) {
    reasons.push("identity_accuracy_below_threshold");
  }
  if (metrics.BRAND_COMPARED > 0 && brand < APPROVAL_THRESHOLDS.brand_accuracy) {
    reasons.push("brand_accuracy_below_threshold");
  }
  if (!metrics.FIELD_SEMANTICS_UNDERSTOOD) reasons.push("field_semantics_not_understood");
  if (metrics.ACCESS_POLICY_OK === false) reasons.push("access_policy_not_ok");

  if (reasons.length) {
    const status =
      returned > 0
        ? APIFY_USAGE_STATUS.CANDIDATE_ONLY
        : APIFY_USAGE_STATUS.UNTESTED;
    return { ok: false, status, reasons };
  }
  return {
    ok: true,
    status: APIFY_USAGE_STATUS.APPROVED,
    reasons: ["first_party_identified", "identity_reliable", "semantics_understood"],
    rooms_approved:
      metrics.ROOMS_COMPARED > 0 &&
      Number(metrics.ROOM_ACCURACY || 0) >= APPROVAL_THRESHOLDS.rooms_accuracy &&
      metrics.ROOMS_SEMANTICS_GUESTROOM === true,
    coords_approved:
      metrics.COORDS_COMPARED > 0 &&
      Number(metrics.COORDINATE_ACCURACY || 0) >= APPROVAL_THRESHOLDS.coordinate_accuracy,
  };
}

function parentFromLookup(entry) {
  return (
    entry?.parent_company ||
    entry?.brand_family ||
    null
  );
}

/**
 * One Actor row → NULL_FILL every independently supported field.
 * Rooms and coordinates only when those field gates passed for this Actor.
 */
export function buildApifyHarvestPatch(fields, row, opts = {}) {
  const dictionary = opts.dictionary;
  const roomsApproved = opts.roomsApproved === true;
  const coordsApproved = opts.coordsApproved === true;
  const patch = {};
  const counts = {
    CURRENT_BRAND_WRITES: 0,
    BRAND_FAMILY_DERIVATIONS: 0,
    ROOMS_WRITES: 0,
    ROOM_CANDIDATES_CORROBORATED: 0,
    ADDRESS_PATCHES: 0,
    POSTAL_PATCHES: 0,
    STATE_PATCHES: 0,
    CITY_PATCHES: 0,
    COORDINATE_PATCHES: 0,
    PHONE_PATCHES: 0,
    WEBSITE_PATCHES: 0,
  };

  let brandCanonical = null;
  if (isBlank(fields[MAP_MASTER.currentBrand]) && row.brand && dictionary) {
    const alias = resolveBrandMappingAlias(row.brand);
    const lookup = lookupCanonicalBrand(alias.canonical || row.brand, dictionary, {
      propertyName: row.name || fields[MAP_MASTER.propertyName],
      sourceUrl: row.url,
    });
    if (lookup.ok && lookup.entry) {
      brandCanonical = lookup.canonical || lookup.entry.canonical_brand_name;
      patch[MAP_MASTER.currentBrand] = brandCanonical;
      counts.CURRENT_BRAND_WRITES = 1;
      const parent = parentFromLookup(lookup.entry);
      if (isBlank(fields[MAP_MASTER.brandFamily]) && parent) {
        patch[MAP_MASTER.brandFamily] = parent;
        counts.BRAND_FAMILY_DERIVATIONS += 1;
      }
      if (isBlank(fields[MAP_MASTER.familySourceFamily]) && parent) {
        patch[MAP_MASTER.familySourceFamily] = parent;
        counts.BRAND_FAMILY_DERIVATIONS += 1;
      }
    }
  } else if (!isBlank(fields[MAP_MASTER.currentBrand]) && dictionary) {
    const lookup = lookupCanonicalBrand(fields[MAP_MASTER.currentBrand], dictionary, {
      propertyName: row.name,
      sourceUrl: row.url,
    });
    const parent = lookup.ok ? parentFromLookup(lookup.entry) : null;
    if (parent && isBlank(fields[MAP_MASTER.brandFamily])) {
      patch[MAP_MASTER.brandFamily] = parent;
      counts.BRAND_FAMILY_DERIVATIONS += 1;
    }
    if (parent && isBlank(fields[MAP_MASTER.familySourceFamily])) {
      patch[MAP_MASTER.familySourceFamily] = parent;
      counts.BRAND_FAMILY_DERIVATIONS += 1;
    }
  }

  const fill = (field, value, counter) => {
    if (isBlank(value)) return;
    if (classifyNullFill(fields[field], value).write) {
      patch[field] = value;
      counts[counter] += 1;
    }
  };

  fill(MAP_MASTER.address, row.address, "ADDRESS_PATCHES");
  fill(MAP_MASTER.postalCode, row.postal, "POSTAL_PATCHES");
  fill(MAP_MASTER.city, row.city, "CITY_PATCHES");
  fill(MAP_MASTER.stateRegion, row.state, "STATE_PATCHES");
  fill(MAP_MASTER.phone, isUsableOfficialPhone(row.phone) ? row.phone : null, "PHONE_PATCHES");
  fill(MAP_MASTER.officialUrl, row.url, "WEBSITE_PATCHES");

  if (
    roomsApproved &&
    !row.rooms_forbidden &&
    row.rooms != null &&
    Number.isFinite(Number(row.rooms)) &&
    Number(row.rooms) > 0
  ) {
    const semantics = rejectNonGuestroomSemantics(
      `${row.rooms_field || "total_rooms"} hotel guestrooms`,
      row.rooms
    );
    const policy = assertRoomsSourcePolicy({
      source_kind: "official_parent_brand_source",
      method: "apify_first_party_extractor",
    });
    const existing = fields[MAP_MASTER.roomsKeys];
    const fillClass = classifyNullFill(existing, row.rooms);
    if (semantics.ok && policy.ok && fillClass.write) {
      patch[MAP_MASTER.roomsKeys] = Number(row.rooms);
      patch[MAP_ROOMS.confidenceExisting] = "High";
      if (row.url) patch[MAP_ROOMS.sourceUrlExisting] = row.url;
      patch[MAP_ROOMS.sourceTypePlanned] = MAP_ROOMS_SOURCE_TYPE.official_brand_directory;
      counts.ROOMS_WRITES = 1;
    } else if (
      semantics.ok &&
      policy.ok &&
      !fillClass.write &&
      !isBlank(existing) &&
      Number(existing) === Number(row.rooms)
    ) {
      counts.ROOM_CANDIDATES_CORROBORATED = 1;
    }
  }

  if (
    coordsApproved &&
    row.lat != null &&
    row.lng != null &&
    isBlank(fields[MAP_MASTER.latitude]) &&
    isBlank(fields[MAP_MASTER.longitude])
  ) {
    patch[MAP_MASTER.latitude] = Number(row.lat);
    patch[MAP_MASTER.longitude] = Number(row.lng);
    patch[MAP_MASTER.coordinateSourceType] = "official_coordinates";
    patch[MAP_MASTER.coordinateConfidence] = "High";
    patch[MAP_MASTER.geocodeProvider] = "Official Page";
    patch[MAP_MASTER.geocodeMethod] = "official_coordinates";
    patch[MAP_MASTER.geocodeReviewedDate] = todayIsoDate();
    counts.COORDINATE_PATCHES = 1;
  }

  if (Object.keys(patch).length) {
    patch[MAP_MASTER.lastReviewed] = todayIsoDate();
    if (isBlank(fields[MAP_MASTER.enrichmentStatus])) {
      patch[MAP_MASTER.enrichmentStatus] = "Partial";
    }
  }

  return {
    ok: Object.keys(patch).length > 0,
    patch,
    counts,
    provenance: row.provenance,
    brand: brandCanonical,
    MAP_BRAND_FIELDS: MAP_BRAND,
  };
}

export function emptyActorMatrixRow(actor, extra = {}) {
  const avail = extra.availability || {};
  return {
    ACTOR_ID: actor.ACTOR_ID,
    ACTOR_NAME: actor.ACTOR_NAME,
    HOTEL_COMPANY: actor.HOTEL_COMPANY,
    UNDERLYING_SOURCE: actor.UNDERLYING_SOURCE,
    SOURCE_CLASS: SOURCE_CLASS,
    FIRST_PARTY: actor.FIRST_PARTY === true,
    FIELDS_AVAILABLE: avail.FIELDS_AVAILABLE || [],
    ROOMS_AVAILABLE: avail.ROOMS_AVAILABLE === true,
    BRAND_AVAILABLE: avail.BRAND_AVAILABLE === true,
    ADDRESS_AVAILABLE: avail.ADDRESS_AVAILABLE === true,
    POSTAL_AVAILABLE: avail.POSTAL_AVAILABLE === true,
    LAT_LONG_AVAILABLE: avail.LAT_LONG_AVAILABLE === true,
    PHONE_AVAILABLE: avail.PHONE_AVAILABLE === true,
    WEBSITE_AVAILABLE: avail.WEBSITE_AVAILABLE === true,
    PROPERTY_ID_AVAILABLE: avail.PROPERTY_ID_AVAILABLE === true,
    PRICE_MODEL: actor.PRICE_MODEL || null,
    SAMPLE_SIZE: extra.SAMPLE_SIZE ?? 0,
    IDENTITY_ACCURACY: extra.IDENTITY_ACCURACY ?? null,
    BRAND_ACCURACY: extra.BRAND_ACCURACY ?? null,
    ROOM_ACCURACY: extra.ROOM_ACCURACY ?? null,
    OVERALL_STATUS: extra.OVERALL_STATUS || actor.DEFAULT_STATUS || APIFY_USAGE_STATUS.UNTESTED,
    USAGE_STATUS: extra.USAGE_STATUS || actor.DEFAULT_STATUS || APIFY_USAGE_STATUS.UNTESTED,
    EXPECTED_CALA_YIELD: actor.EXPECTED_CALA_YIELD || null,
    COST_PER_1000_PROPERTIES: actor.COST_PER_1000_USD ?? null,
    TOTAL_APIFY_COST: extra.TOTAL_APIFY_COST ?? 0,
    ROOMS_APPROVED: extra.ROOMS_APPROVED === true,
    COORDS_APPROVED: extra.COORDS_APPROVED === true,
    NOTES: extra.NOTES || actor.SKIP_REASON || null,
  };
}
