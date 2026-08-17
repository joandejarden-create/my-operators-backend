/**
 * Production Census first-pass enrichment — geography, Radar readiness, safe fields.
 * Reads VIC freeze claims + live Census. Never writes Brand Explorer / blocked fields.
 */

import { readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { PRODUCTION_USE_STATUS, TABLE_IDS } from "./production-census-write.js";
import { COUNTRY_CONFIGS } from "../radar-buildout/country-configs.js";
import { inferCensusSubmarketCorridor } from "../radar-buildout/travel-infrastructure-submarket-inference.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const FIRST_PASS_VERSION = "production-census-first-pass-enrichment-v1";
export const CENSUS_TABLE = "Hotel Property Census";
export const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];
export const EXPECTED_RECORD_COUNT = 666;
export const EXPECTED_FIELD_COUNT = 101;
export const EXPECTED_HELD = 4;

export const STATUS = Object.freeze({
  CONFIRMATION_MISSING: "production_census_first_pass_blocked_needs_steward_review",
  DRY_RUN_READY: "production_census_first_pass_dry_run_ready_for_founder_review",
  DRY_RUN_BLOCKED: "production_census_first_pass_blocked_needs_steward_review",
  APPLIED: "production_census_first_pass_applied_ready_for_next_enrichment_lane",
  APPLY_BLOCKED: "production_census_first_pass_blocked_needs_steward_review",
});

/** Central Airtable field map for first-pass writes. */
export const MAP_FIRST_PASS = Object.freeze({
  propertyName: "Property Name",
  canonicalPropertyName: "Canonical Property Name",
  identityKey: "Property Identity Key",
  country: "Country",
  stateRegion: "State / Region",
  city: "City",
  address: "Address",
  latitude: "Latitude",
  longitude: "Longitude",
  marketSubmarket: "Market / Submarket",
  currentBrand: "Current Brand",
  brandFamily: "Brand Family",
  brandSlug: "Brand Explorer Slug if mapped",
  affiliationStatus: "Affiliation Status",
  sourceUrl: "Source URL",
  officialUrl: "Official Property URL",
  family: "Family / Source Family",
  humanReview: "Human Review Required",
  dataEligible: "Data Eligible",
  enrichmentStatus: "Enrichment Status",
  enrichmentPriority: "Enrichment Priority",
  lastReviewed: "Last Reviewed Date",
  descriptionSource: "Hotel Description - Source Text",
  descriptionAi: "Hotel Description - AI Summary",
  amenitiesSource: "Amenities - Source Text",
  amenitiesTags: "Amenities - Structured Tags",
  propertyType: "Property Type",
  assetContext: "Asset Context",
  flagFb: "F&B Flag",
  flagMeeting: "Meeting Space Flag",
  flagResort: "Resort / Leisure Flag",
  flagExtendedStay: "Extended Stay Flag",
  flagMixedUse: "Mixed-Use Flag",
  flagResidences: "Branded Residences Flag",
  radarDisplayStatus: "Radar Display Status",
  radarDisplayReason: "Radar Display Reason",
  radarGeographyStatus: "Radar Geography Status",
  publicCensusEligibility: "Public Census Eligibility",
  publicDisplayConfidence: "Public Display Confidence",
  publicDisplayReviewStatus: "Public Display Review Status",
  addressConfidence: "Address Confidence",
  addressSourceUrl: "Address Source URL",
  coordinateSourceType: "Coordinate Source Type",
  coordinateConfidence: "Coordinate Confidence",
  geocodeProvider: "Geocode Provider",
  geocodeMethod: "Geocode Method",
  geocodeReviewedDate: "Geocode Reviewed Date",
});

export const ALLOWED_WRITE_FIELDS = Object.freeze([
  MAP_FIRST_PASS.latitude,
  MAP_FIRST_PASS.longitude,
  MAP_FIRST_PASS.radarDisplayStatus,
  MAP_FIRST_PASS.radarDisplayReason,
  MAP_FIRST_PASS.radarGeographyStatus,
  MAP_FIRST_PASS.publicCensusEligibility,
  MAP_FIRST_PASS.publicDisplayConfidence,
  MAP_FIRST_PASS.publicDisplayReviewStatus,
  MAP_FIRST_PASS.descriptionSource,
  MAP_FIRST_PASS.descriptionAi,
  MAP_FIRST_PASS.amenitiesSource,
  MAP_FIRST_PASS.amenitiesTags,
  MAP_FIRST_PASS.propertyType,
  MAP_FIRST_PASS.assetContext,
  MAP_FIRST_PASS.marketSubmarket,
  MAP_FIRST_PASS.flagFb,
  MAP_FIRST_PASS.flagMeeting,
  MAP_FIRST_PASS.flagResort,
  MAP_FIRST_PASS.flagExtendedStay,
  MAP_FIRST_PASS.flagMixedUse,
  MAP_FIRST_PASS.flagResidences,
  MAP_FIRST_PASS.enrichmentStatus,
  MAP_FIRST_PASS.enrichmentPriority,
  MAP_FIRST_PASS.lastReviewed,
]);

export const FORBIDDEN_WRITE_FIELDS = Object.freeze([
  "Owner Name",
  "Developer Name",
  "Developer",
  "Operator / Management Company",
  "Rooms / Keys",
  "Opening Date",
  "Renovation / Conversion Date",
  "Renovation Date",
  "Affiliation Start Date",
  "Company Validated",
  "Brand Verified",
  "Recent Momentum",
]);

const VIC_SOURCES = Object.freeze([
  {
    family: "Hilton",
    path: "data/research-engine-v2/verified-independent-census-wave1b-hilton/02-hilton-full-records.json",
  },
  {
    family: "Choice",
    path: "data/research-engine-v2/verified-independent-census-wave1c-choice/02-choice-full-records.json",
  },
  {
    family: "Marriott",
    path: "data/research-engine-v2/verified-independent-census-wave1d-marriott/02-marriott-full-records.json",
  },
  {
    family: "IHG",
    path: "data/research-engine-v2/verified-independent-census-v1/08-expanded-benchmark-full-records.json",
  },
]);

const EXTENDED_STAY_BRAND_RE =
  /\b(staybridge|candlewood|homewood|home2|element|residence inn|towneplace|extended stay|studio[s]?)\b/i;
const RESORT_BRAND_RE =
  /\b(resort|all.?inclusive|iberostar|joia|walidorf|grand.?fiesta|secrets|dreams|hyatt.?ziva|hyatt.?zilara)\b/i;
const BOUTIQUE_BRAND_RE =
  /\b(design hotels|kimpton|autograph|curio|tapestry|ascend|slh|small luxury|boutique)\b/i;

const MEXICO_CITY_TO_MARKET = Object.freeze([
  { re: /canc[uú]n|riviera maya|playa del carmen|tulum|cozumel|isla mujeres|akumal|mayakoba|puerto aventuras|costa mujeres/i, market: "Cancún / Riviera Maya" },
  { re: /mexico city|ciudad de m[eé]xico|cdmx|polanco|santa fe|reforma/i, market: "Mexico City" },
  { re: /los cabos|cabo san lucas|san jos[eé] del cabo|san jose del cabo/i, market: "Los Cabos" },
  { re: /guadalajara|zapopan|tlaquepaque/i, market: "Guadalajara" },
  { re: /monterrey|san pedro garza|santa catarina/i, market: "Monterrey" },
  { re: /puerto vallarta|nuevo vallarta|riviera nayarit|nayarit|bucer[ií]as/i, market: "Puerto Vallarta / Riviera Nayarit" },
  { re: /m[eé]rida|merida|yucat[aá]n|yucatan/i, market: "Mérida / Yucatán" },
  { re: /oaxaca/i, market: "Other", corridorHint: "Oaxaca" },
  { re: /puebla/i, market: "Other", corridorHint: "Puebla" },
  { re: /quer[eé]taro|queretaro/i, market: "Other", corridorHint: "Querétaro" },
  { re: /mazatl[aá]n|mazatlan/i, market: "Other", corridorHint: "Mazatlán" },
  { re: /veracruz/i, market: "Other", corridorHint: "Veracruz" },
  { re: /acapulco/i, market: "Other", corridorHint: "Acapulco" },
  { re: /toluca/i, market: "Other", corridorHint: "Other" },
  { re: /aguascalientes/i, market: "Other", corridorHint: "Other" },
]);

const BATCH_SIZE = 10;
const BATCH_DELAY_MS = 220;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function mask(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}
function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}
function norm(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
function isBlank(v) {
  return v == null || v === "" || (typeof v === "string" && !v.trim());
}
function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}
function claimValue(claims, field) {
  const c = (claims || []).find((x) => x.field === field && x.value != null && x.value !== "");
  return c || null;
}

export function checkFirstPassEnvFlags() {
  const flags = {
    ALLOW_PRODUCTION_CENSUS_FIRST_PASS:
      process.env.ALLOW_PRODUCTION_CENSUS_FIRST_PASS === "1",
    CONFIRM_NO_BRAND_EXPLORER_WRITES: process.env.CONFIRM_NO_BRAND_EXPLORER_WRITES === "1",
    CONFIRM_NO_OWNER_OPERATOR_WRITES: process.env.CONFIRM_NO_OWNER_OPERATOR_WRITES === "1",
    CONFIRM_NO_ROOM_DATE_WRITES: process.env.CONFIRM_NO_ROOM_DATE_WRITES === "1",
  };
  return { allOk: Object.values(flags).every(Boolean), flags };
}

export function parseFirstPassArgs(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--")));
  const confirms = {
    firstPass: flags.has("--confirm-first-pass-census-enrichment"),
    sourceCoords: flags.has("--confirm-source-supported-coordinates-only"),
    noCityCentroid: flags.has("--confirm-no-city-centroid-coordinates"),
    noZeroZero: flags.has("--confirm-no-zero-zero-coordinates"),
    officialSources: flags.has("--confirm-official-public-sources-only"),
    noBe: flags.has("--confirm-no-brand-explorer-writes"),
    noOwner: flags.has("--confirm-no-owner-operator-writes"),
    noRoomDate: flags.has("--confirm-no-room-date-writes"),
    noMomentum: flags.has("--confirm-no-recent-momentum"),
    heldBlocked: flags.has("--confirm-held-records-blocked"),
  };
  const allConfirms = Object.values(confirms).every(Boolean);
  return {
    dryRun: flags.has("--dry-run") || !flags.has("--apply"),
    apply: flags.has("--apply"),
    confirms,
    allConfirms,
  };
}

async function listAllRecords(baseId, token, tableId, fields = []) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${tableId} ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

async function batchPatch(baseId, token, tableId, updates) {
  const errors = [];
  let updated = 0;
  for (let i = 0; i < updates.length; i += BATCH_SIZE) {
    const chunk = updates.slice(i, i + BATCH_SIZE).map((u) => ({
      id: u.id,
      fields: u.fields,
    }));
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: chunk, typecast: true }),
      }
    );
    const json = await res.json();
    if (!res.ok) {
      errors.push({ status: res.status, error: json.error || json, chunk_start: i });
    } else {
      updated += (json.records || []).length;
    }
    await sleep(BATCH_DELAY_MS);
  }
  return { updated, errors };
}

export function loadActiveBrandUniverse() {
  const path = join(ROOT, "reports/brand-explorer-62-active-public-full-baseline.json");
  if (!existsSync(path)) throw new Error(`Missing active universe baseline: ${path}`);
  const data = readJson(path);
  const brands = data.brands || [];
  const bySlug = new Map();
  const byName = new Map();
  const aliases = new Map([
    // Aliases only when target slug exists in Active/Live 62 (validated at load)
    ["tapestry by hilton", "tapestry-collection-by-hilton"],
    ["tapestry collection by hilton", "tapestry-collection-by-hilton"],
    ["hilton hotels & resorts", "hilton-hotels-and-resorts"],
    ["hilton hotels and resorts", "hilton-hotels-and-resorts"],
    ["hampton by hilton", "hampton-by-hilton"],
    ["hilton garden inn", "hilton-garden-inn"],
    ["doubletree by hilton", "doubletree-by-hilton"],
    ["homewood suites by hilton", "homewood-suites-by-hilton"],
    ["home2 suites by hilton", "home2-suites-by-hilton"],
    ["canopy by hilton", "canopy-by-hilton"],
    ["spark by hilton", "spark-by-hilton"],
    ["tempo by hilton", "tempo-by-hilton"],
    ["tru by hilton", "tru-by-hilton"],
    ["motto by hilton", "motto-by-hilton"],
    ["curio collection by hilton", "curio-collection"],
    ["city express by marriott", "city-express-by-marriott"],
    ["courtyard by marriott", "courtyard-by-marriott"],
    ["ac hotels by marriott", "ac-hotels-by-marriott"],
    ["marriott hotels", "marriott-hotels"],
    ["aloft", "aloft-hotels"],
    ["aloft hotels", "aloft-hotels"],
    ["design hotels", "design-hotels"],
    ["holiday inn express", "holiday-inn-express"],
    ["hotel indigo", "hotel-indigo"],
    ["voco", "voco-hotels"],
    ["voco hotels", "voco-hotels"],
    ["kimpton", "kimpton"],
    ["kimpton hotels", "kimpton"],
    ["avid hotels", "avid-hotels"],
    ["avid", "avid-hotels"],
    ["autograph collection", "autograph-collection"],
    ["ascend hotel collection", "ascend"],
    ["ascend", "ascend"],
    ["even hotels", "even-hotels"],
    ["everhome", "everhome-suites"],
    ["everhome suites", "everhome-suites"],
    ["comfort inn", "comfort-inn-suites"],
    ["comfort inn & suites", "comfort-inn-suites"],
    ["comfort inn and suites", "comfort-inn-suites"],
    ["country inn & suites by choice", "country-inn-suites"],
    ["country inn and suites", "country-inn-suites"],
    ["country inn & suites", "country-inn-suites"],
    ["small luxury hotels of the world", "small-luxury-hotels-of-the-world"],
    ["slh", "small-luxury-hotels-of-the-world"],
    ["bunkhouse hotels", "bunkhouse-hotels"],
    ["bw premier collection", "bw-premier-collection"],
    ["bw signature collection", "bw-signature-collection"],
    ["fairmont", "fairmont-hotels-and-resorts"],
    ["fairmont hotels and resorts", "fairmont-hotels-and-resorts"],
    ["dazzler by wyndham", "dazzler-by-wyndham"],
    ["quality inn", "quality-inn"],
    ["radisson", "radisson"],
    ["radisson blu", "radisson-blu"],
    ["radisson red", "radisson-red"],
    ["westin", "westin"],
    ["sheraton", "sheraton"],
    ["tribute portfolio", "tribute-portfolio"],
    ["residence inn", "residence-inn-by-marriott"],
    ["residence inn by marriott", "residence-inn-by-marriott"],
    ["springhill suites", "springhill-suites-by-marriott"],
    ["springhill suites by marriott", "springhill-suites-by-marriott"],
    ["towneplace suites", "towneplace-suites-by-marriott"],
    ["towneplace suites by marriott", "towneplace-suites-by-marriott"],
    ["moxy", "moxy-hotels"],
    ["moxy hotels", "moxy-hotels"],
    ["suburban", "suburban-studios"],
    ["suburban studios", "suburban-studios"],
    ["woodspring suites", "woodspring-suites"],
    ["ibis", "ibis"],
    ["novotel", "novotel"],
    ["mercure", "mercure"],
    ["pullman", "pullman"],
    ["mama shelter", "mama-shelter"],
  ]);

  for (const b of brands) {
    const slug = b.slug;
    const name = String(b.brandName || b.name || "").trim();
    if (slug) bySlug.set(slug, b);
    if (name) byName.set(norm(name), { slug, brand: b, match: "exact_match" });
  }
  for (const [alias, slug] of aliases) {
    if (bySlug.has(slug) && !byName.has(norm(alias))) {
      byName.set(norm(alias), { slug, brand: bySlug.get(slug), match: "alias_match" });
    }
  }
  return { brands, bySlug, byName, activeCount: brands.length };
}

export function loadVicClaimIndex() {
  /** @type {Map<string, object>} */
  const byId = new Map();
  const loaded = [];
  for (const src of VIC_SOURCES) {
    const abs = join(ROOT, src.path);
    if (!existsSync(abs)) {
      loaded.push({ family: src.family, path: src.path, ok: false, count: 0 });
      continue;
    }
    const data = readJson(abs);
    const records = data.records || [];
    for (const rec of records) {
      const id = rec.independent_record_id;
      if (!id) continue;
      byId.set(id, {
        family: src.family,
        independent_record_id: id,
        name: rec.canonical_hotel_name || rec.name,
        brand: rec.brand,
        city: rec.normalized_city || rec.city,
        country: rec.country || "Mexico",
        official_property_url: rec.official_property_url || rec.website,
        discovery_source: rec.discovery_source,
        field_claims: rec.field_claims || [],
        fields: rec.fields || {},
      });
    }
    loaded.push({ family: src.family, path: src.path, ok: true, count: records.length });
  }
  return { byId, loaded };
}

export function mapCensusBrand(fields, universe) {
  const slug = String(fields[MAP_FIRST_PASS.brandSlug] || "").trim();
  const brandName = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
  const affiliation = String(fields[MAP_FIRST_PASS.affiliationStatus] || "").trim();

  if (affiliation === "Brand-Unconfirmed") {
    return {
      classification: "blocked_brand_unconfirmed",
      slug: slug || null,
      brandName,
      active: false,
    };
  }

  if (slug && universe.bySlug.has(slug)) {
    return {
      classification: "exact_match",
      slug,
      brandName: universe.bySlug.get(slug).brandName || brandName,
      active: true,
    };
  }

  const hit = universe.byName.get(norm(brandName));
  if (hit) {
    return {
      classification: hit.match,
      slug: hit.slug,
      brandName: hit.brand?.brandName || brandName,
      active: true,
    };
  }

  // Clear brand string but not in Active/Live 62 — out of first-pass enrichment scope
  if (brandName && affiliation && affiliation !== "Independent") {
    return {
      classification: "not_in_active_universe",
      slug: slug || null,
      brandName,
      active: false,
    };
  }

  return {
    classification: "no_census_records_found",
    slug: slug || null,
    brandName,
    active: false,
  };
}

function isValidCoordPair(lat, lng) {
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return false;
  if (lat === 0 && lng === 0) return false;
  if (lat < -90 || lat > 90 || lng < -180 || lng > 180) return false;
  return true;
}

/**
 * Reject known airport / city-centroid patterns and non-property sources.
 */
export function assessCoordinateClaim(latClaim, lngClaim, censusFields, vic) {
  if (!latClaim || !lngClaim) {
    return { ok: false, reason: "missing_coordinate_claims" };
  }
  const lat = Number(latClaim.value);
  const lng = Number(lngClaim.value);
  if (!isValidCoordPair(lat, lng)) {
    return { ok: false, reason: "invalid_or_zero_zero" };
  }
  const conf = String(latClaim.confidence || lngClaim.confidence || "").trim();
  if (conf === "Low" || conf === "Insufficient" || conf === "Unknown" || !conf) {
    return { ok: false, reason: "confidence_too_low", confidence: conf || null };
  }
  const source = String(latClaim.source || "");
  const sourceType = String(latClaim.source_type || "");
  const evidenceUrl = latClaim.evidence_url || lngClaim.evidence_url || vic?.official_property_url;
  if (!evidenceUrl) {
    return { ok: false, reason: "source_url_missing" };
  }
  // City-centroid / tourism-board rejection
  if (/city.?centroid|tourism.?board|airport.?coordinate|city.?only/i.test(source)) {
    return { ok: false, reason: "city_centroid_or_airport_source", source };
  }
  // Choice regional geoLocation is still per-hotel-card geo — allow when unique + High/Medium
  const propertyLevel =
    /directory localization\.coordinate|hotel card|geoLocation|property page|official/i.test(
      source
    ) || /Official (Brand|Parent Company|Property)/i.test(sourceType);
  if (!propertyLevel && conf !== "High") {
    return { ok: false, reason: "not_property_level", source, sourceType };
  }

  const city = String(censusFields[MAP_FIRST_PASS.city] || vic?.city || "");
  const name = String(censusFields[MAP_FIRST_PASS.propertyName] || vic?.name || "");
  if (!name) return { ok: false, reason: "property_name_missing" };

  return {
    ok: true,
    lat,
    lng,
    confidence: conf,
    source,
    source_type: sourceType,
    evidence_url: evidenceUrl,
    city,
    name,
  };
}

export function resolveDealalityMarketSubmarket(fields) {
  const country = String(fields[MAP_FIRST_PASS.country] || "").trim();
  const city = String(fields[MAP_FIRST_PASS.city] || "").trim();
  const name = String(fields[MAP_FIRST_PASS.propertyName] || "").trim();
  if (!country || !city) {
    return { ok: false, reason: "missing_city_or_country" };
  }
  if (norm(country) !== "mexico") {
    return { ok: false, reason: "non_mexico_deferred" };
  }

  const hay = `${city} ${name}`;
  let market = "Other";
  let corridorHint = null;
  for (const row of MEXICO_CITY_TO_MARKET) {
    if (row.re.test(hay)) {
      market = row.market;
      corridorHint = row.corridorHint || null;
      break;
    }
  }

  const corridor = inferCensusSubmarketCorridor({
    country: "Mexico",
    city,
    name,
    market,
  });
  const inferred = corridor?.inferredSubmarket || corridor?.submarket || null;
  const sub =
    (inferred && corridor?.confidence && corridor.confidence !== "No Match" ? inferred : null) ||
    corridorHint ||
    null;

  const value = sub ? `${market} · ${sub}` : market;
  const mx = COUNTRY_CONFIGS.Mexico;
  const marketOk = !mx?.initialMarkets || mx.initialMarkets.includes(market) || market === "Other";
  if (!marketOk) {
    return { ok: false, reason: "market_not_in_dealality_config", market };
  }
  // Avoid fake completeness: do not write bare "Other" without corridor support
  const corridorConf = corridor?.confidence || "No Match";
  const namedMarket = market !== "Other";
  const corridorOk =
    Boolean(sub) && corridorConf !== "No Match" && corridorConf !== "Low";
  if (!namedMarket && !corridorOk) {
    return { ok: false, reason: "market_inference_too_weak", market, city };
  }
  return {
    ok: true,
    value,
    market,
    submarket: sub,
    confidence: namedMarket
      ? corridorOk
        ? corridorConf === "High"
          ? "High"
          : "Medium"
        : "Medium"
      : corridorConf,
    source: "Dealality Market + corridor inference from Census city (not STR)",
    evidence_url: fields[MAP_FIRST_PASS.sourceUrl] || fields[MAP_FIRST_PASS.officialUrl] || null,
  };
}

function structuredTagsFromAmenities(text) {
  return String(text || "")
    .split(/[;\n|,]+/)
    .map((s) => s.trim())
    .filter(Boolean)
    .slice(0, 40);
}

function inferFlagsFromAmenitiesAndBrand(amenText, brandName, propertyName) {
  const hay = `${amenText || ""} ${brandName || ""} ${propertyName || ""}`.toLowerCase();
  const flags = {};
  if (/restaurant|dining|f&b|on-site restaurant|bar \/ lounge|food/i.test(hay)) {
    flags[MAP_FIRST_PASS.flagFb] = true;
  }
  if (/meeting|conference|ballroom|event space|business center/i.test(hay)) {
    flags[MAP_FIRST_PASS.flagMeeting] = true;
  }
  if (/resort|leisure|outdoor pool|beach|spa/i.test(hay) || RESORT_BRAND_RE.test(brandName || "")) {
    flags[MAP_FIRST_PASS.flagResort] = true;
  }
  if (EXTENDED_STAY_BRAND_RE.test(`${brandName} ${propertyName}`) || /extended stay|kitchenette|suite kitchen/i.test(hay)) {
    flags[MAP_FIRST_PASS.flagExtendedStay] = true;
  }
  if (/mixed.?use|residences|branded residence/i.test(hay)) {
    if (/mixed.?use/i.test(hay)) flags[MAP_FIRST_PASS.flagMixedUse] = true;
    if (/residence/i.test(hay)) flags[MAP_FIRST_PASS.flagResidences] = true;
  }
  return flags;
}

function inferPropertyType(amenText, brandName, propertyName, flags) {
  const hay = `${amenText || ""} ${brandName || ""} ${propertyName || ""}`;
  if (flags[MAP_FIRST_PASS.flagExtendedStay] || EXTENDED_STAY_BRAND_RE.test(hay)) {
    return { value: "Extended Stay", confidence: "High", reason: "brand_or_amenity_extended_stay" };
  }
  if (/all.?inclusive/i.test(hay)) {
    return { value: "All-Inclusive", confidence: "Medium", reason: "all_inclusive_signal" };
  }
  if (flags[MAP_FIRST_PASS.flagResort] || /resort/i.test(hay) || RESORT_BRAND_RE.test(brandName || "")) {
    return { value: "Resort", confidence: "Medium", reason: "resort_signal" };
  }
  if (BOUTIQUE_BRAND_RE.test(hay) || /\bboutique\b/i.test(amenText || "")) {
    return { value: "Boutique Hotel", confidence: "Medium", reason: "boutique_or_collection_brand" };
  }
  if (/serviced apartment|apartment hotel/i.test(hay)) {
    return { value: "Serviced Apartment", confidence: "Medium", reason: "serviced_apartment_signal" };
  }
  if (brandName) {
    return { value: "Hotel", confidence: "Medium", reason: "branded_hotel_default" };
  }
  return null;
}

function inferAssetContext(fields, amenText, market) {
  const hay = `${fields[MAP_FIRST_PASS.propertyName] || ""} ${fields[MAP_FIRST_PASS.city] || ""} ${amenText || ""} ${market || ""}`;
  if (/airport|aeropuerto/i.test(hay)) {
    return { value: "Airport", confidence: "High", reason: "airport_in_name_or_city" };
  }
  if (/beach|waterfront|zona hotelera|hotel zone|riviera|cabo|vallarta|tulum|cozumel/i.test(hay)) {
    return { value: "Beach / Waterfront", confidence: "Medium", reason: "coastal_market_or_name" };
  }
  if (/resort destination|all.?inclusive|iberostar|joia/i.test(hay)) {
    return { value: "Resort Destination", confidence: "Medium", reason: "resort_destination_signal" };
  }
  if (/mexico city|monterrey|guadalajara|polanco|santa fe|centro|urban/i.test(hay)) {
    return { value: "Urban", confidence: "Medium", reason: "urban_market_or_city" };
  }
  if (/highway|express|roadside/i.test(hay)) {
    return { value: "Highway / Transit", confidence: "Low", reason: "highway_signal" };
  }
  return null;
}

export function classifyRadarReadiness({
  fields,
  held,
  brandUnconfirmed,
  uncertainBrand,
  notInActiveUniverse,
  coordAssessment,
  afterLat,
  afterLng,
}) {
  const name = fields[MAP_FIRST_PASS.propertyName];
  const city = fields[MAP_FIRST_PASS.city];
  const state = fields[MAP_FIRST_PASS.stateRegion];
  const country = fields[MAP_FIRST_PASS.country];
  const sourceUrl = fields[MAP_FIRST_PASS.sourceUrl] || fields[MAP_FIRST_PASS.officialUrl];
  const affiliation = fields[MAP_FIRST_PASS.affiliationStatus];
  const affiliationClear =
    affiliation === "Branded" ||
    affiliation === "Soft-Branded / Collection" ||
    affiliation === "Future / Pipeline";

  if (held || brandUnconfirmed || uncertainBrand) {
    return {
      [MAP_FIRST_PASS.radarDisplayStatus]: "Hold",
      [MAP_FIRST_PASS.radarDisplayReason]: held
        ? "Human Review Required — steward hold"
        : brandUnconfirmed
          ? "Brand-Unconfirmed steward hold"
          : "Uncertain brand mapping — steward review",
      [MAP_FIRST_PASS.radarGeographyStatus]: "Hold",
      [MAP_FIRST_PASS.publicCensusEligibility]: "Hold",
      [MAP_FIRST_PASS.publicDisplayConfidence]: "Hold",
      [MAP_FIRST_PASS.publicDisplayReviewStatus]: "Hold",
    };
  }

  if (notInActiveUniverse) {
    const hasCoords = isValidCoordPair(Number(afterLat), Number(afterLng));
    return {
      [MAP_FIRST_PASS.radarDisplayStatus]: "Internal Only",
      [MAP_FIRST_PASS.radarDisplayReason]:
        "Brand not in Active/Live Brand Explorer universe — research-only for this first pass",
      [MAP_FIRST_PASS.radarGeographyStatus]: hasCoords
        ? "Coordinates Available"
        : city
          ? "City-Level Only"
          : "Geography Insufficient",
      [MAP_FIRST_PASS.publicCensusEligibility]: "Not Eligible",
      [MAP_FIRST_PASS.publicDisplayConfidence]: "Low",
      [MAP_FIRST_PASS.publicDisplayReviewStatus]: "Needs Review",
    };
  }

  const hasGeoBasics = Boolean(name && city && country);
  const hasCoords = isValidCoordPair(Number(afterLat), Number(afterLng));
  const coordsPropertyLevel = Boolean(coordAssessment?.ok);

  let geographyStatus = "Geography Insufficient";
  if (hasCoords && coordsPropertyLevel) geographyStatus = "Coordinates Available";
  else if (hasCoords && !coordsPropertyLevel) geographyStatus = "City-Level Only";
  else if (!isBlank(fields[MAP_FIRST_PASS.address]) && !hasCoords) {
    geographyStatus = "Address Available No Coordinates";
  } else if (city && !hasCoords) geographyStatus = "City-Level Only";

  if (
    hasGeoBasics &&
    affiliationClear &&
    hasCoords &&
    coordsPropertyLevel &&
    sourceUrl &&
    !held
  ) {
    return {
      [MAP_FIRST_PASS.radarDisplayStatus]: "Public Map Eligible",
      [MAP_FIRST_PASS.radarDisplayReason]: `Property-level coordinates from ${coordAssessment.source}; affiliation clear; source URL present`,
      [MAP_FIRST_PASS.radarGeographyStatus]: geographyStatus,
      [MAP_FIRST_PASS.publicCensusEligibility]: "Eligible",
      [MAP_FIRST_PASS.publicDisplayConfidence]:
        coordAssessment.confidence === "High" ? "High" : "Medium",
      [MAP_FIRST_PASS.publicDisplayReviewStatus]: "Auto-Classified",
    };
  }

  if (hasGeoBasics && sourceUrl && !held) {
    return {
      [MAP_FIRST_PASS.radarDisplayStatus]: "Public List Eligible",
      [MAP_FIRST_PASS.radarDisplayReason]: hasCoords
        ? "List-eligible; coordinates present but not confirmed property-level for map pin"
        : "List-eligible; city/country/source present; coordinates missing or not property-level",
      [MAP_FIRST_PASS.radarGeographyStatus]: geographyStatus,
      [MAP_FIRST_PASS.publicCensusEligibility]: "Eligible With Limits",
      [MAP_FIRST_PASS.publicDisplayConfidence]: "Medium",
      [MAP_FIRST_PASS.publicDisplayReviewStatus]: "Auto-Classified",
    };
  }

  return {
    [MAP_FIRST_PASS.radarDisplayStatus]: "Internal Only",
    [MAP_FIRST_PASS.radarDisplayReason]:
      "Geography incomplete, weak coordinate confidence, or missing source URL — research-only",
    [MAP_FIRST_PASS.radarGeographyStatus]: geographyStatus,
    [MAP_FIRST_PASS.publicCensusEligibility]: "Not Eligible",
    [MAP_FIRST_PASS.publicDisplayConfidence]: "Low",
    [MAP_FIRST_PASS.publicDisplayReviewStatus]: "Needs Review",
  };
}

function buildBlockedQueueItems(vic, censusFields) {
  const items = [];
  if (!vic) return items;
  const blockedMap = [
    { claim: "rooms", field: "Rooms / Keys", lane: "rooms_keys_approval" },
    { claim: "Open Date", field: "Opening Date", lane: "opening_date_approval" },
    { claim: "Owner", field: "Owner Name", lane: "owner_approval" },
    { claim: "Owner Name", field: "Owner Name", lane: "owner_approval" },
    { claim: "Developer", field: "Developer Name", lane: "developer_approval" },
    { claim: "Operator", field: "Operator / Management Company", lane: "operator_approval" },
    {
      claim: "Operator / Management Company",
      field: "Operator / Management Company",
      lane: "operator_approval",
    },
    { claim: "Affiliation Start Date", field: "Affiliation Start Date", lane: "affiliation_start_approval" },
  ];
  for (const row of blockedMap) {
    const c = claimValue(vic.field_claims, row.claim);
    if (!c || c.value == null || c.value === "") continue;
    items.push({
      property: censusFields[MAP_FIRST_PASS.propertyName] || vic.name,
      identity_key: vic.independent_record_id,
      field: row.field,
      possible_value: c.value,
      source_url: c.evidence_url || vic.official_property_url || null,
      source_confidence: c.confidence || null,
      reason_not_written: "Blocked in first-pass enrichment lane — research queue only",
      required_next_approval_lane: row.lane,
    });
  }
  return items;
}

function sanitizePatch(fields) {
  const out = {};
  for (const [k, v] of Object.entries(fields)) {
    if (!ALLOWED_WRITE_FIELDS.includes(k)) continue;
    if (FORBIDDEN_WRITE_FIELDS.includes(k)) continue;
    out[k] = v;
  }
  return out;
}

/**
 * Build proposed updates for one Census row.
 */
export function proposeRecordUpdate(record, ctx) {
  const fields = record.fields || {};
  const key = fields[MAP_FIRST_PASS.identityKey];
  const held = fields[MAP_FIRST_PASS.humanReview] === true;
  const brandMap = mapCensusBrand(fields, ctx.universe);
  const vic = key ? ctx.vic.byId.get(key) : null;
  const affiliation = String(fields[MAP_FIRST_PASS.affiliationStatus] || "");
  const brandUnconfirmed = affiliation === "Brand-Unconfirmed";
  const uncertainBrand = brandMap.classification === "blocked_brand_unconfirmed";
  const notInActiveUniverse =
    brandMap.classification === "not_in_active_universe" ||
    brandMap.classification === "no_census_records_found";

  const blockedItems = buildBlockedQueueItems(vic, fields);

  const baseMeta = {
    record_id: record.id,
    identity_key: key,
    property_name: fields[MAP_FIRST_PASS.propertyName],
    brand_mapping: brandMap,
    held,
    family: fields[MAP_FIRST_PASS.family] || vic?.family || null,
  };

  // Held / unconfirmed / not-in-active-universe: Radar classification only (no enrichment content)
  if (held || brandUnconfirmed || uncertainBrand || !brandMap.active) {
    const radar = classifyRadarReadiness({
      fields,
      held,
      brandUnconfirmed,
      uncertainBrand,
      notInActiveUniverse: !held && !brandUnconfirmed && notInActiveUniverse,
      coordAssessment: null,
      afterLat: fields[MAP_FIRST_PASS.latitude],
      afterLng: fields[MAP_FIRST_PASS.longitude],
    });
    const patch = sanitizePatch({
      ...radar,
      ...(held || brandUnconfirmed
        ? {
            [MAP_FIRST_PASS.enrichmentStatus]: "Held",
            [MAP_FIRST_PASS.enrichmentPriority]: "High",
            [MAP_FIRST_PASS.lastReviewed]: todayIsoDate(),
          }
        : {}),
    });
    return {
      ...baseMeta,
      eligible: false,
      block_reason: held
        ? "human_review_required"
        : brandUnconfirmed
          ? "brand_unconfirmed"
          : notInActiveUniverse
            ? "not_in_active_universe"
            : "not_active_brand_mapped",
      patch,
      sources: [],
      blocked_queue: blockedItems,
      coordinate: null,
    };
  }

  /** @type {Record<string, unknown>} */
  const patch = {};
  /** @type {object[]} */
  const sources = [];
  let coordAssessment = null;

  // Coordinates from VIC claims only (no fabrication / no live geocode invent)
  if (isBlank(fields[MAP_FIRST_PASS.latitude]) || isBlank(fields[MAP_FIRST_PASS.longitude])) {
    const latClaim = vic ? claimValue(vic.field_claims, "Latitude") : null;
    const lngClaim = vic ? claimValue(vic.field_claims, "Longitude") : null;
    coordAssessment = assessCoordinateClaim(latClaim, lngClaim, fields, vic);
    if (coordAssessment.ok) {
      patch[MAP_FIRST_PASS.latitude] = coordAssessment.lat;
      patch[MAP_FIRST_PASS.longitude] = coordAssessment.lng;
      sources.push({
        fields: [MAP_FIRST_PASS.latitude, MAP_FIRST_PASS.longitude],
        source_url: coordAssessment.evidence_url,
        confidence: coordAssessment.confidence,
        source: coordAssessment.source,
      });
    }
  } else {
    // Existing coords — validate only
    const lat = Number(fields[MAP_FIRST_PASS.latitude]);
    const lng = Number(fields[MAP_FIRST_PASS.longitude]);
    if (isValidCoordPair(lat, lng)) {
      coordAssessment = {
        ok: true,
        lat,
        lng,
        confidence: "High",
        source: "existing_census_coordinates",
        evidence_url: fields[MAP_FIRST_PASS.sourceUrl] || fields[MAP_FIRST_PASS.officialUrl],
      };
    }
  }

  // Amenities
  const amenClaim = vic ? claimValue(vic.field_claims, "Amenities") : null;
  if (
    amenClaim &&
    isBlank(fields[MAP_FIRST_PASS.amenitiesSource]) &&
    amenClaim.evidence_url &&
    amenClaim.confidence &&
    amenClaim.confidence !== "Low" &&
    amenClaim.confidence !== "Insufficient"
  ) {
    const tags = structuredTagsFromAmenities(amenClaim.value);
    patch[MAP_FIRST_PASS.amenitiesSource] = String(amenClaim.value);
    patch[MAP_FIRST_PASS.amenitiesTags] = tags.join("\n");
    sources.push({
      fields: [MAP_FIRST_PASS.amenitiesSource, MAP_FIRST_PASS.amenitiesTags],
      source_url: amenClaim.evidence_url,
      confidence: amenClaim.confidence,
      source: amenClaim.source,
    });

    const flags = inferFlagsFromAmenitiesAndBrand(
      amenClaim.value,
      fields[MAP_FIRST_PASS.currentBrand],
      fields[MAP_FIRST_PASS.propertyName]
    );
    Object.assign(patch, flags);
    if (Object.keys(flags).length) {
      sources.push({
        fields: Object.keys(flags),
        source_url: amenClaim.evidence_url,
        confidence: amenClaim.confidence,
        source: "amenity_text_and_brand_evidence",
      });
    }

    const pType = inferPropertyType(
      amenClaim.value,
      fields[MAP_FIRST_PASS.currentBrand],
      fields[MAP_FIRST_PASS.propertyName],
      flags
    );
    if (pType && isBlank(fields[MAP_FIRST_PASS.propertyType])) {
      patch[MAP_FIRST_PASS.propertyType] = pType.value;
      sources.push({
        fields: [MAP_FIRST_PASS.propertyType],
        source_url: amenClaim.evidence_url,
        confidence: pType.confidence,
        source: pType.reason,
      });
    }
  } else {
    // Brand-only extended stay / property type when no amenities
    const flags = inferFlagsFromAmenitiesAndBrand(
      "",
      fields[MAP_FIRST_PASS.currentBrand],
      fields[MAP_FIRST_PASS.propertyName]
    );
    if (flags[MAP_FIRST_PASS.flagExtendedStay] && isBlank(fields[MAP_FIRST_PASS.flagExtendedStay])) {
      patch[MAP_FIRST_PASS.flagExtendedStay] = true;
      sources.push({
        fields: [MAP_FIRST_PASS.flagExtendedStay],
        source_url: fields[MAP_FIRST_PASS.sourceUrl] || fields[MAP_FIRST_PASS.officialUrl],
        confidence: "High",
        source: "extended_stay_brand_identity",
      });
    }
    const pType = inferPropertyType(
      "",
      fields[MAP_FIRST_PASS.currentBrand],
      fields[MAP_FIRST_PASS.propertyName],
      flags
    );
    if (pType && pType.value === "Extended Stay" && isBlank(fields[MAP_FIRST_PASS.propertyType])) {
      patch[MAP_FIRST_PASS.propertyType] = pType.value;
      sources.push({
        fields: [MAP_FIRST_PASS.propertyType],
        source_url: fields[MAP_FIRST_PASS.sourceUrl] || fields[MAP_FIRST_PASS.officialUrl],
        confidence: pType.confidence,
        source: pType.reason,
      });
    }
  }

  // Market / Submarket
  if (isBlank(fields[MAP_FIRST_PASS.marketSubmarket])) {
    const mkt = resolveDealalityMarketSubmarket(fields);
    if (mkt.ok) {
      patch[MAP_FIRST_PASS.marketSubmarket] = mkt.value;
      sources.push({
        fields: [MAP_FIRST_PASS.marketSubmarket],
        source_url: mkt.evidence_url,
        confidence: mkt.confidence,
        source: mkt.source,
      });
    }
  }

  // Asset context (only with supporting geography/amenity signal)
  if (isBlank(fields[MAP_FIRST_PASS.assetContext])) {
    const amenText = patch[MAP_FIRST_PASS.amenitiesSource] || fields[MAP_FIRST_PASS.amenitiesSource] || "";
    const mktVal = patch[MAP_FIRST_PASS.marketSubmarket] || fields[MAP_FIRST_PASS.marketSubmarket] || "";
    const asset = inferAssetContext(fields, amenText, mktVal);
    if (asset && asset.confidence !== "Low") {
      patch[MAP_FIRST_PASS.assetContext] = asset.value;
      sources.push({
        fields: [MAP_FIRST_PASS.assetContext],
        source_url: fields[MAP_FIRST_PASS.sourceUrl] || fields[MAP_FIRST_PASS.officialUrl],
        confidence: asset.confidence,
        source: asset.reason,
      });
    }
  }

  // Descriptions — only when VIC has grounded source text (currently rare)
  const descClaim =
    (vic && claimValue(vic.field_claims, "Hotel Description - Source Text")) ||
    (vic && claimValue(vic.field_claims, "description")) ||
    (vic && claimValue(vic.field_claims, "Description"));
  if (
    descClaim &&
    isBlank(fields[MAP_FIRST_PASS.descriptionSource]) &&
    descClaim.evidence_url &&
    descClaim.confidence &&
    descClaim.confidence !== "Low"
  ) {
    patch[MAP_FIRST_PASS.descriptionSource] = String(descClaim.value);
    // Grounded AI summary: compress source text without inventing facts
    const summary = String(descClaim.value).replace(/\s+/g, " ").trim().slice(0, 400);
    if (summary) {
      patch[MAP_FIRST_PASS.descriptionAi] = summary;
    }
    sources.push({
      fields: [MAP_FIRST_PASS.descriptionSource, MAP_FIRST_PASS.descriptionAi],
      source_url: descClaim.evidence_url,
      confidence: descClaim.confidence,
      source: descClaim.source,
    });
  }

  const afterLat = patch[MAP_FIRST_PASS.latitude] ?? fields[MAP_FIRST_PASS.latitude];
  const afterLng = patch[MAP_FIRST_PASS.longitude] ?? fields[MAP_FIRST_PASS.longitude];
  const radar = classifyRadarReadiness({
    fields,
    held: false,
    brandUnconfirmed: false,
    uncertainBrand: false,
    notInActiveUniverse: false,
    coordAssessment,
    afterLat,
    afterLng,
  });
  Object.assign(patch, radar);

  const contentKeys = Object.keys(patch).filter(
    (k) =>
      ![
        MAP_FIRST_PASS.radarDisplayStatus,
        MAP_FIRST_PASS.radarDisplayReason,
        MAP_FIRST_PASS.radarGeographyStatus,
        MAP_FIRST_PASS.publicCensusEligibility,
        MAP_FIRST_PASS.publicDisplayConfidence,
        MAP_FIRST_PASS.publicDisplayReviewStatus,
        MAP_FIRST_PASS.enrichmentStatus,
        MAP_FIRST_PASS.enrichmentPriority,
        MAP_FIRST_PASS.lastReviewed,
      ].includes(k)
  );
  patch[MAP_FIRST_PASS.enrichmentStatus] = contentKeys.length ? "Partial" : "In Progress";
  patch[MAP_FIRST_PASS.enrichmentPriority] = "Medium";
  patch[MAP_FIRST_PASS.lastReviewed] = todayIsoDate();

  const clean = sanitizePatch(patch);
  return {
    ...baseMeta,
    eligible: true,
    block_reason: null,
    patch: clean,
    sources,
    blocked_queue: blockedItems,
    coordinate: coordAssessment?.ok
      ? {
          lat: coordAssessment.lat,
          lng: coordAssessment.lng,
          source_url: coordAssessment.evidence_url,
          confidence: coordAssessment.confidence,
          source: coordAssessment.source,
        }
      : coordAssessment
        ? { rejected: true, reason: coordAssessment.reason }
        : { rejected: true, reason: "no_vic_coordinate_claim" },
  };
}

function countPatchField(proposals, field) {
  return proposals.filter((p) => p.patch && p.patch[field] != null && p.patch[field] !== "").length;
}

function summarizeProposals(proposals, censusRows, brandClassCounts) {
  const eligible = proposals.filter((p) => p.eligible);
  const blocked = proposals.filter((p) => !p.eligible);
  const coordUpdates = proposals.filter((p) => p.patch?.[MAP_FIRST_PASS.latitude] != null);
  const radarUpdates = proposals.filter((p) => p.patch?.[MAP_FIRST_PASS.radarDisplayStatus]);
  const descUpdates = proposals.filter((p) => p.patch?.[MAP_FIRST_PASS.descriptionSource]);
  const amenUpdates = proposals.filter((p) => p.patch?.[MAP_FIRST_PASS.amenitiesSource]);
  const typeUpdates = proposals.filter(
    (p) => p.patch?.[MAP_FIRST_PASS.propertyType] || p.patch?.[MAP_FIRST_PASS.assetContext]
  );
  const flagUpdates = proposals.filter(
    (p) =>
      p.patch?.[MAP_FIRST_PASS.flagFb] ||
      p.patch?.[MAP_FIRST_PASS.flagMeeting] ||
      p.patch?.[MAP_FIRST_PASS.flagResort] ||
      p.patch?.[MAP_FIRST_PASS.flagExtendedStay] ||
      p.patch?.[MAP_FIRST_PASS.flagMixedUse] ||
      p.patch?.[MAP_FIRST_PASS.flagResidences]
  );
  const marketUpdates = proposals.filter((p) => p.patch?.[MAP_FIRST_PASS.marketSubmarket]);

  const radarCounts = {};
  for (const p of proposals) {
    const s = p.patch?.[MAP_FIRST_PASS.radarDisplayStatus];
    if (!s) continue;
    radarCounts[s] = (radarCounts[s] || 0) + 1;
  }

  const blockedQueue = proposals.flatMap((p) => p.blocked_queue || []);
  const airtableUpdateCount = proposals.filter((p) => Object.keys(p.patch || {}).length > 0).length;

  const withCoordsNow = censusRows.filter((r) =>
    isValidCoordPair(Number(r.fields?.[MAP_FIRST_PASS.latitude]), Number(r.fields?.[MAP_FIRST_PASS.longitude]))
  ).length;

  return {
    total_records_scanned: censusRows.length,
    active_brand_mapped_records: proposals.filter((p) => p.brand_mapping?.active).length,
    eligible_records: eligible.length,
    blocked_records: blocked.length,
    brand_mapping_counts: brandClassCounts,
    records_with_coordinates_before: withCoordsNow,
    records_missing_coordinates_before: censusRows.length - withCoordsNow,
    coordinate_updates_proposed: coordUpdates.length,
    radar_status_updates_proposed: radarUpdates.length,
    radar_display_status_counts: radarCounts,
    description_updates_proposed: descUpdates.length,
    amenity_updates_proposed: amenUpdates.length,
    property_type_asset_context_updates_proposed: typeUpdates.length,
    strategic_flag_updates_proposed: flagUpdates.length,
    market_submarket_updates_proposed: marketUpdates.length,
    blocked_field_research_queue_count: blockedQueue.length,
    exact_airtable_update_count: airtableUpdateCount,
    field_update_counts: Object.fromEntries(
      ALLOWED_WRITE_FIELDS.map((f) => [f, countPatchField(proposals, f)])
    ),
  };
}

const CENSUS_READ_FIELDS = [
  ...new Set([
    ...Object.values(MAP_FIRST_PASS),
    "Owner Name",
    "Developer Name",
    "Operator / Management Company",
    "Rooms / Keys",
    "Opening Date",
    "Renovation / Conversion Date",
    "Affiliation Start Date",
    "Production Use Status",
    "Data Eligible",
  ]),
];

export async function runFirstPassDryRun() {
  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token) throw new Error("AIRTABLE_PAT missing");
  if (!bases?.target_base_id) throw new Error("AIRTABLE_BASE_ID_ALT missing");

  const universe = loadActiveBrandUniverse();
  const vic = loadVicClaimIndex();
  const censusRows = await listAllRecords(
    bases.target_base_id,
    token,
    CENSUS_TABLE_ID,
    CENSUS_READ_FIELDS
  );

  const brandClassCounts = {};
  const proposals = [];
  for (const row of censusRows) {
    const p = proposeRecordUpdate(row, { universe, vic });
    const cls = p.brand_mapping?.classification || "unknown";
    brandClassCounts[cls] = (brandClassCounts[cls] || 0) + 1;
    proposals.push(p);
  }

  // Shared-pin detection among proposed coords
  const pinMap = new Map();
  for (const p of proposals) {
    if (p.patch?.[MAP_FIRST_PASS.latitude] == null) continue;
    const k = `${Number(p.patch[MAP_FIRST_PASS.latitude]).toFixed(5)},${Number(p.patch[MAP_FIRST_PASS.longitude]).toFixed(5)}`;
    if (!pinMap.has(k)) pinMap.set(k, []);
    pinMap.get(k).push(p.property_name);
  }
  const sharedPins = [...pinMap.entries()]
    .filter(([, names]) => names.length > 1)
    .map(([coord, names]) => ({ coord, properties: names }));

  // Downgrade shared campus pins to Medium confidence note
  for (const p of proposals) {
    if (p.patch?.[MAP_FIRST_PASS.latitude] == null) continue;
    const k = `${Number(p.patch[MAP_FIRST_PASS.latitude]).toFixed(5)},${Number(p.patch[MAP_FIRST_PASS.longitude]).toFixed(5)}`;
    const cluster = pinMap.get(k) || [];
    if (cluster.length > 1 && p.patch[MAP_FIRST_PASS.radarDisplayStatus] === "Public Map Eligible") {
      p.patch[MAP_FIRST_PASS.publicDisplayConfidence] = "Medium";
      p.patch[MAP_FIRST_PASS.radarDisplayReason] =
        `${p.patch[MAP_FIRST_PASS.radarDisplayReason]} · shared campus pin (${cluster.length} properties)`;
    }
  }

  const summary = summarizeProposals(proposals, censusRows, brandClassCounts);
  const blockedQueue = proposals.flatMap((p) => p.blocked_queue || []).slice(0, 500);

  const coverage = {
    geography: {
      latitude: censusRows.filter((r) => !isBlank(r.fields?.[MAP_FIRST_PASS.latitude])).length,
      longitude: censusRows.filter((r) => !isBlank(r.fields?.[MAP_FIRST_PASS.longitude])).length,
      address: censusRows.filter((r) => !isBlank(r.fields?.[MAP_FIRST_PASS.address])).length,
      city: censusRows.filter((r) => !isBlank(r.fields?.[MAP_FIRST_PASS.city])).length,
      state_region: censusRows.filter((r) => !isBlank(r.fields?.[MAP_FIRST_PASS.stateRegion])).length,
      country: censusRows.filter((r) => !isBlank(r.fields?.[MAP_FIRST_PASS.country])).length,
    },
    radar_public_before: Object.fromEntries(
      [
        MAP_FIRST_PASS.radarDisplayStatus,
        MAP_FIRST_PASS.radarDisplayReason,
        MAP_FIRST_PASS.radarGeographyStatus,
        MAP_FIRST_PASS.publicCensusEligibility,
        MAP_FIRST_PASS.publicDisplayConfidence,
        MAP_FIRST_PASS.publicDisplayReviewStatus,
      ].map((f) => [f, censusRows.filter((r) => !isBlank(r.fields?.[f])).length])
    ),
    safe_enrichment_before: Object.fromEntries(
      [
        MAP_FIRST_PASS.descriptionSource,
        MAP_FIRST_PASS.amenitiesSource,
        MAP_FIRST_PASS.propertyType,
        MAP_FIRST_PASS.assetContext,
        MAP_FIRST_PASS.marketSubmarket,
      ].map((f) => [f, censusRows.filter((r) => !isBlank(r.fields?.[f])).length])
    ),
  };

  const forbiddenUntouched = {
    fields: FORBIDDEN_WRITE_FIELDS,
    proposed_writes: FORBIDDEN_WRITE_FIELDS.filter((f) =>
      proposals.some((p) => p.patch && Object.prototype.hasOwnProperty.call(p.patch, f))
    ),
    ok: true,
  };
  forbiddenUntouched.ok = forbiddenUntouched.proposed_writes.length === 0;

  const heldPublicEligible = proposals.filter(
    (p) =>
      p.held &&
      (p.patch?.[MAP_FIRST_PASS.radarDisplayStatus] === "Public Map Eligible" ||
        p.patch?.[MAP_FIRST_PASS.radarDisplayStatus] === "Public List Eligible")
  );

  const sample = proposals
    .filter((p) => p.eligible && Object.keys(p.patch || {}).length > 0)
    .slice(0, 8)
    .map((p) => {
      const before = censusRows.find((r) => r.id === p.record_id)?.fields || {};
      const beforeSnap = {};
      const afterSnap = {};
      for (const k of Object.keys(p.patch)) {
        beforeSnap[k] = before[k] ?? null;
        afterSnap[k] = p.patch[k];
      }
      return {
        record_id: mask(p.record_id),
        identity_key: p.identity_key,
        property_name: p.property_name,
        before: beforeSnap,
        after: afterSnap,
        sources: p.sources,
      };
    });

  const dryPass =
    censusRows.length === EXPECTED_RECORD_COUNT &&
    summary.coordinate_updates_proposed >= 0 &&
    forbiddenUntouched.ok &&
    heldPublicEligible.length === 0 &&
    universe.activeCount === 62;

  return {
    version: FIRST_PASS_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry-run",
    status: dryPass ? STATUS.DRY_RUN_READY : STATUS.DRY_RUN_BLOCKED,
    base_id_masked: mask(bases.target_base_id),
    token_masked: mask(token),
    vic_sources: vic.loaded,
    active_brand_universe: universe.activeCount,
    coverage_audit: coverage,
    summary,
    shared_campus_pins: sharedPins,
    blocked_field_research_queue: blockedQueue,
    sample_before_after: sample,
    forbidden_fields_untouched: forbiddenUntouched,
    held_not_public_eligible: heldPublicEligible.length === 0,
    proposals_path_note: "Full proposal list retained in-memory for apply; sample only in report",
    proposals, // retained for apply path when same process; dry-run JSON strips later
    dry_run_pass: dryPass,
    next_recommended_lane: dryPass
      ? "Founder review → apply with confirm flags → Marriott/IHG coordinate sourcing lane + description page scrape"
      : "Resolve dry-run blockers before apply",
  };
}

export async function runFirstPassApply(dryReport) {
  const args = parseFirstPassArgs();
  const env = checkFirstPassEnvFlags();
  if (!args.allConfirms || !env.allOk) {
    return {
      version: FIRST_PASS_VERSION,
      status: STATUS.CONFIRMATION_MISSING,
      apply_executed: false,
      missing_cli_confirms: Object.entries(args.confirms)
        .filter(([, v]) => !v)
        .map(([k]) => k),
      env_flags: env.flags,
    };
  }
  if (!dryReport?.dry_run_pass) {
    return {
      version: FIRST_PASS_VERSION,
      status: STATUS.APPLY_BLOCKED,
      apply_executed: false,
      reason: "dry_run_did_not_pass",
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  const updates = (dryReport.proposals || [])
    .filter((p) => p.patch && Object.keys(p.patch).length > 0)
    .map((p) => ({ id: p.record_id, fields: sanitizePatch(p.patch) }));

  // Safety: strip forbidden again
  for (const u of updates) {
    for (const f of FORBIDDEN_WRITE_FIELDS) {
      if (f in u.fields) delete u.fields[f];
    }
    if (u.fields.Latitude === 0 || u.fields.Longitude === 0) {
      delete u.fields.Latitude;
      delete u.fields.Longitude;
    }
  }

  const write = await batchPatch(bases.target_base_id, token, CENSUS_TABLE_ID, updates);

  const postRows = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    MAP_FIRST_PASS.identityKey,
    MAP_FIRST_PASS.latitude,
    MAP_FIRST_PASS.longitude,
    MAP_FIRST_PASS.radarDisplayStatus,
    MAP_FIRST_PASS.humanReview,
    MAP_FIRST_PASS.affiliationStatus,
    "Owner Name",
    "Operator / Management Company",
    "Rooms / Keys",
    "Opening Date",
    "Renovation / Conversion Date",
    "Affiliation Start Date",
    MAP_FIRST_PASS.amenitiesSource,
    MAP_FIRST_PASS.descriptionSource,
  ]);

  const zeroZero = postRows.filter(
    (r) => r.fields?.Latitude === 0 && r.fields?.Longitude === 0
  ).length;
  const heldPublic = postRows.filter(
    (r) =>
      r.fields?.[MAP_FIRST_PASS.humanReview] === true &&
      (r.fields?.[MAP_FIRST_PASS.radarDisplayStatus] === "Public Map Eligible" ||
        r.fields?.[MAP_FIRST_PASS.radarDisplayStatus] === "Public List Eligible")
  ).length;
  const brandUnconfirmedMap = postRows.filter(
    (r) =>
      r.fields?.[MAP_FIRST_PASS.affiliationStatus] === "Brand-Unconfirmed" &&
      r.fields?.[MAP_FIRST_PASS.radarDisplayStatus] === "Public Map Eligible"
  ).length;

  const validation = {
    record_count: postRows.length,
    expected_record_count: EXPECTED_RECORD_COUNT,
    zero_zero: zeroZero,
    held_public_eligible: heldPublic,
    brand_unconfirmed_public_map: brandUnconfirmedMap,
    owner_filled: postRows.filter((r) => !isBlank(r.fields?.["Owner Name"])).length,
    operator_filled: postRows.filter((r) => !isBlank(r.fields?.["Operator / Management Company"]))
      .length,
    rooms_filled: postRows.filter((r) => r.fields?.["Rooms / Keys"] != null).length,
    opening_filled: postRows.filter((r) => !isBlank(r.fields?.["Opening Date"])).length,
    renovation_filled: postRows.filter((r) => !isBlank(r.fields?.["Renovation / Conversion Date"]))
      .length,
    affiliation_start_filled: postRows.filter((r) => !isBlank(r.fields?.["Affiliation Start Date"]))
      .length,
    coords_filled: postRows.filter((r) =>
      isValidCoordPair(Number(r.fields?.Latitude), Number(r.fields?.Longitude))
    ).length,
    radar_populated: postRows.filter((r) => !isBlank(r.fields?.[MAP_FIRST_PASS.radarDisplayStatus]))
      .length,
    amenities_filled: postRows.filter((r) => !isBlank(r.fields?.[MAP_FIRST_PASS.amenitiesSource]))
      .length,
  };
  validation.pass =
    validation.record_count === EXPECTED_RECORD_COUNT &&
    validation.zero_zero === 0 &&
    validation.held_public_eligible === 0 &&
    validation.brand_unconfirmed_public_map === 0 &&
    validation.owner_filled === 0 &&
    validation.operator_filled === 0 &&
    validation.rooms_filled === 0 &&
    validation.opening_filled === 0 &&
    validation.renovation_filled === 0 &&
    validation.affiliation_start_filled === 0 &&
    write.errors.length === 0;

  return {
    version: FIRST_PASS_VERSION,
    generated_at: new Date().toISOString(),
    mode: "apply",
    apply_executed: true,
    status: validation.pass ? STATUS.APPLIED : STATUS.APPLY_BLOCKED,
    base_id_masked: mask(bases.target_base_id),
    updates_attempted: updates.length,
    updates_written: write.updated,
    airtable_errors: write.errors,
    summary_from_dry_run: dryReport.summary,
    post_apply_validation: validation,
    forbidden_fields_untouched: dryReport.forbidden_fields_untouched,
    blocked_field_research_queue_count: dryReport.summary?.blocked_field_research_queue_count,
    next_recommended_lane:
      "Marriott/IHG property-level coordinate sourcing + official-page description scrape (blocked owner/operator/rooms/dates remain queued)",
  };
}

export function renderFirstPassDryRunMarkdown(report) {
  const s = report.summary || {};
  return `# Production Census First Pass — Dry Run

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Base:** \`${report.base_id_masked}\`

## 1. Executive summary

- Census scanned: **${s.total_records_scanned}**
- Active-brand mapped: **${s.active_brand_mapped_records}**
- Eligible for first pass: **${s.eligible_records}**
- Blocked: **${s.blocked_records}**
- Coordinate updates proposed: **${s.coordinate_updates_proposed}**
- Radar updates proposed: **${s.radar_status_updates_proposed}**
- Amenity updates proposed: **${s.amenity_updates_proposed}**
- Description updates proposed: **${s.description_updates_proposed}**
- Exact Airtable update count: **${s.exact_airtable_update_count}**
- Dry-run pass: **${report.dry_run_pass}**

## 2. Active-brand Census scope

\`\`\`json
${JSON.stringify(
  {
    active_universe: report.active_brand_universe,
    brand_mapping_counts: s.brand_mapping_counts,
    vic_sources: report.vic_sources,
  },
  null,
  2
)}
\`\`\`

## 3. Coordinate coverage audit

\`\`\`json
${JSON.stringify(
  {
    before: {
      with_coordinates: s.records_with_coordinates_before,
      missing: s.records_missing_coordinates_before,
    },
    proposed_updates: s.coordinate_updates_proposed,
    shared_campus_pins: report.shared_campus_pins,
    geography_coverage: report.coverage_audit?.geography,
  },
  null,
  2
)}
\`\`\`

## 4. Proposed coordinate updates

See JSON \`field_update_counts.Latitude/Longitude\` and sample before/after.

## 5. Radar readiness classification counts

\`\`\`json
${JSON.stringify(s.radar_display_status_counts, null, 2)}
\`\`\`

## 6–9. Safe enrichment proposals

| Lane | Count |
| --- | ---: |
| Descriptions | ${s.description_updates_proposed} |
| Amenities | ${s.amenity_updates_proposed} |
| Property type / asset context | ${s.property_type_asset_context_updates_proposed} |
| Strategic flags | ${s.strategic_flag_updates_proposed} |
| Market / Submarket | ${s.market_submarket_updates_proposed} |

## 10. Blocked field research queue

Count: **${s.blocked_field_research_queue_count}** (owner / operator / rooms / dates researched but not written)

## 11. Source support summary

All proposed coordinate/amenity writes require evidence URL + Medium/High confidence from VIC official directory/property claims.

## 12. Webhound usage

See durable doc after sidecar completes (Marriott coordinate extraction patterns). Webhound output is never written directly to Airtable.

## 13. Forbidden fields untouched

\`\`\`json
${JSON.stringify(report.forbidden_fields_untouched, null, 2)}
\`\`\`

## 14. Brand Explorer safety

Run post-apply gates. This dry-run does not touch Brand Explorer.

## 15. Next recommended lane

${report.next_recommended_lane}

## Sample before/after

\`\`\`json
${JSON.stringify(report.sample_before_after, null, 2)}
\`\`\`
`;
}

export function renderFirstPassApplyMarkdown(report) {
  return `# Production Census First Pass — Apply

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Apply executed:** ${report.apply_executed}

## Summary

- Updates attempted: **${report.updates_attempted}**
- Updates written: **${report.updates_written}**
- Airtable errors: **${(report.airtable_errors || []).length}**

## Post-apply validation

\`\`\`json
${JSON.stringify(report.post_apply_validation, null, 2)}
\`\`\`

## Forbidden fields untouched

\`\`\`json
${JSON.stringify(report.forbidden_fields_untouched, null, 2)}
\`\`\`

## Next recommended lane

${report.next_recommended_lane}
`;
}
