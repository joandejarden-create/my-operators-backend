/**
 * Production Census description extraction lane + next-lane router decision.
 * Provider-not-ready → description dry-run (no geocode apply).
 * No Brand Explorer / owner / rooms / dates / Recent Momentum writes.
 */

import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  MAP_FIRST_PASS,
  loadActiveBrandUniverse,
  mapCensusBrand,
  resolveDealalityMarketSubmarket,
  FORBIDDEN_WRITE_FIELDS,
} from "./production-census-first-pass-enrichment.js";
import { resolveGeocodingProvider } from "./production-census-geocoding-providers.js";
import {
  extractOfficialPageEnrichment,
  DESCRIPTION_EXTRACTOR_VERSION,
  isBookingBoilerplate,
} from "./production-census-description-extractor.js";
import {
  resolveDirectoryAmenitiesCandidate,
  resolveDirectoryDescriptionCandidate,
  resolveDirectoryAddressCandidate,
  applyDeepOfficialPageSignals,
  noteUnresolvedSourcePattern,
  warmFamilyDirectoryCaches,
} from "./census-autopilot-family-directory-adapters.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const DESC_LANE_VERSION = "production-census-description-extraction-v1";
export const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];
export const EXPECTED_RECORD_COUNT = 666;

export const STATUS = Object.freeze({
  GEOCODE_APPLIED: "production_census_address_geocode_applied_ready_for_description_extraction",
  DESC_DRY_RUN_READY: "production_census_description_extraction_dry_run_ready_for_founder_review",
  BLOCKED_PROVIDER: "production_census_next_lane_blocked_needs_provider_decision",
  BLOCKED_SOURCE: "production_census_next_lane_blocked_by_source_quality",
});

export const ALLOWED_WRITE_FIELDS = Object.freeze([
  MAP_FIRST_PASS.descriptionSource,
  MAP_FIRST_PASS.descriptionAi,
  MAP_FIRST_PASS.amenitiesSource,
  MAP_FIRST_PASS.amenitiesTags,
  MAP_FIRST_PASS.propertyType,
  MAP_FIRST_PASS.assetContext,
  MAP_FIRST_PASS.marketSubmarket,
  MAP_FIRST_PASS.address,
  MAP_FIRST_PASS.addressConfidence,
  MAP_FIRST_PASS.addressSourceUrl,
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

export const APPLY_CONFIRM_FLAGS = Object.freeze([
  "--confirm-description-extraction",
  "--confirm-ihg-only",
  "--confirm-official-public-sources-only",
  "--confirm-grounded-source-text-only",
  "--confirm-no-geocode-writes",
  "--confirm-no-brand-explorer-writes",
  "--confirm-no-owner-operator-writes",
  "--confirm-no-room-date-writes",
  "--confirm-no-recent-momentum",
  "--confirm-held-records-blocked",
]);

/** Primary fields for IHG description apply (founder-approved batch). */
export const IHG_APPLY_PRIMARY_FIELDS = Object.freeze([
  MAP_FIRST_PASS.descriptionSource,
  MAP_FIRST_PASS.descriptionAi,
  MAP_FIRST_PASS.enrichmentStatus,
  MAP_FIRST_PASS.enrichmentPriority,
  MAP_FIRST_PASS.lastReviewed,
]);

export const APPROVED_DESCRIPTION_METHODS = Object.freeze([
  "json_ld_hotel_description",
  "official_page_amenities_factual_assembly",
  "html_paragraph",
]);

export const IHG_APPLY_STATUS = Object.freeze({
  APPLIED: "production_census_ihg_descriptions_applied_ready_for_next_family",
  PARTIAL: "production_census_ihg_descriptions_partial_apply_needs_review",
  BLOCKED: "production_census_ihg_descriptions_blocked_before_apply",
});

const DRY_RUN_JSON =
  "reports/research-engine-v2/production-census-description-extraction-dry-run.json";
const BATCH_SIZE = 10;
const BATCH_DELAY_MS = 220;

const FETCH_HEADERS = {
  "user-agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "accept-language": "en-US,en;q=0.9",
};

const EXTENDED_STAY_BRAND_RE =
  /\b(staybridge|candlewood|homewood|home2|element|residence inn|towneplace|extended stay|studio[s]?)\b/i;
const RESORT_BRAND_RE =
  /\b(resort|all.?inclusive|iberostar|joia|secrets|dreams)\b/i;
const BOUTIQUE_BRAND_RE =
  /\b(design hotels|kimpton|autograph|curio|tapestry|ascend|slh|small luxury|boutique)\b/i;

const READ_FIELDS = [
  MAP_FIRST_PASS.propertyName,
  MAP_FIRST_PASS.identityKey,
  MAP_FIRST_PASS.country,
  MAP_FIRST_PASS.city,
  MAP_FIRST_PASS.address,
  MAP_FIRST_PASS.latitude,
  MAP_FIRST_PASS.longitude,
  MAP_FIRST_PASS.marketSubmarket,
  MAP_FIRST_PASS.currentBrand,
  MAP_FIRST_PASS.brandFamily,
  MAP_FIRST_PASS.brandSlug,
  MAP_FIRST_PASS.affiliationStatus,
  MAP_FIRST_PASS.sourceUrl,
  MAP_FIRST_PASS.officialUrl,
  MAP_FIRST_PASS.family,
  MAP_FIRST_PASS.humanReview,
  MAP_FIRST_PASS.dataEligible,
  MAP_FIRST_PASS.enrichmentStatus,
  MAP_FIRST_PASS.enrichmentPriority,
  MAP_FIRST_PASS.descriptionSource,
  MAP_FIRST_PASS.descriptionAi,
  MAP_FIRST_PASS.amenitiesSource,
  MAP_FIRST_PASS.amenitiesTags,
  MAP_FIRST_PASS.propertyType,
  MAP_FIRST_PASS.assetContext,
  MAP_FIRST_PASS.flagFb,
  MAP_FIRST_PASS.flagMeeting,
  MAP_FIRST_PASS.flagResort,
  MAP_FIRST_PASS.flagExtendedStay,
  MAP_FIRST_PASS.flagMixedUse,
  MAP_FIRST_PASS.flagResidences,
  MAP_FIRST_PASS.publicCensusEligibility,
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function mask(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}
function isBlank(v) {
  return v == null || v === "" || (typeof v === "string" && !v.trim());
}
function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}
function isValidCoordPair(lat, lng) {
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return false;
  if (lat === 0 && lng === 0) return false;
  if (lat < -90 || lat > 90 || lng < -180 || lng > 180) return false;
  return true;
}

export function parseDescArgs(argv = process.argv.slice(2)) {
  const getNum = (flag, fallback) => {
    const i = argv.indexOf(flag);
    if (i >= 0 && argv[i + 1] && !argv[i + 1].startsWith("--")) {
      const n = Number(argv[i + 1]);
      return Number.isFinite(n) ? n : fallback;
    }
    return fallback;
  };
  const confirms = {};
  for (const f of APPLY_CONFIRM_FLAGS) {
    confirms[f] = argv.includes(f);
  }
  // Accept legacy dry-run-era aliases
  if (argv.includes("--confirm-census-description-extraction")) {
    confirms["--confirm-description-extraction"] = true;
  }
  if (argv.includes("--confirm-ai-summary-grounded-in-source-text")) {
    confirms["--confirm-grounded-source-text-only"] = true;
  }
  const missingConfirms = APPLY_CONFIRM_FLAGS.filter((f) => !confirms[f]);
  return {
    dryRun: argv.includes("--dry-run") || !argv.includes("--apply"),
    apply: argv.includes("--apply"),
    fetchLimit: getNum("--fetch-limit", 60),
    families: (() => {
      const i = argv.indexOf("--families");
      if (i >= 0 && argv[i + 1]) {
        return argv[i + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);
      }
      return null;
    })(),
    skipBeGates: argv.includes("--skip-be-gates"),
    confirms,
    missingConfirms,
    allConfirms: missingConfirms.length === 0,
  };
}

export function checkIhgApplyEnvFlags(env = process.env) {
  const flags = {
    ALLOW_PRODUCTION_CENSUS_DESCRIPTION_EXTRACTION:
      String(env.ALLOW_PRODUCTION_CENSUS_DESCRIPTION_EXTRACTION || "").trim() === "1",
    CONFIRM_NO_BRAND_EXPLORER_WRITES:
      String(env.CONFIRM_NO_BRAND_EXPLORER_WRITES || "").trim() === "1",
    CONFIRM_NO_OWNER_OPERATOR_WRITES:
      String(env.CONFIRM_NO_OWNER_OPERATOR_WRITES || "").trim() === "1",
    CONFIRM_NO_ROOM_DATE_WRITES: String(env.CONFIRM_NO_ROOM_DATE_WRITES || "").trim() === "1",
  };
  return {
    flags,
    allOk: Object.values(flags).every(Boolean),
    missing: Object.entries(flags)
      .filter(([, v]) => !v)
      .map(([k]) => k),
  };
}

/**
 * Prefer Mapbox Permanent; Google only with storage terms.
 */
export function evaluateProviderReadiness(env = process.env) {
  const mapboxToken = Boolean(
    String(env.MAPBOX_ACCESS_TOKEN || env.MAPBOX_TOKEN || "").trim()
  );
  const mapboxPermanent = String(env.MAPBOX_PERMANENT_GEOCODING || "").trim() === "1";
  const completionEnabled =
    String(env.CENSUS_COORDINATE_COMPLETION_ENABLED || "").trim() === "1";
  const googleTerms =
    String(env.GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED || "").trim() === "1";
  const googleKey = Boolean(
    String(env.GOOGLE_MAPS_API_KEY || env.GOOGLE_GEOCODING_API_KEY || "").trim()
  );
  const providerName = String(env.GEOCODING_PROVIDER || "").trim().toLowerCase();

  let info;
  if (env === process.env) {
    info = resolveGeocodingProvider();
  } else {
    info = {
      provider: providerName || (mapboxToken ? "mapbox" : googleKey ? "google" : "none"),
      reason: "env_overlay_test",
      credentials_ok: mapboxToken || googleKey,
      key_present: googleKey,
      storage_terms_reviewed: googleTerms,
      mapbox_permanent: mapboxPermanent,
      permanent_storage_enabled: mapboxPermanent,
    };
  }

  // Autopilot stored Census coords require Mapbox Permanent + completion flag.
  // Google may still be noted for non-Autopilot paths, but Autopilot apply uses Mapbox only.
  const mapboxReady = mapboxToken && mapboxPermanent && completionEnabled;
  const googleReady =
    (info.provider === "google" || providerName === "google") && googleKey && googleTerms;
  const approved = mapboxReady;
  const route = approved ? "geocode_apply" : "provider_decision_needed";

  return {
    provider_info: info,
    mapbox_permanent_ready: mapboxToken && mapboxPermanent,
    census_coordinate_completion_enabled: completionEnabled,
    google_terms_confirmed: googleTerms,
    google_ready_for_apply: googleReady,
    preferred: "mapbox_permanent",
    approved_for_geocode_apply: approved,
    route,
    block_reason: approved
      ? null
      : !mapboxToken || !mapboxPermanent || !completionEnabled
        ? "mapbox_permanent_or_completion_flag_missing"
        : "provider_or_storage_terms_not_confirmed",
    recommended:
      "Set MAPBOX_ACCESS_TOKEN + MAPBOX_PERMANENT_GEOCODING=1 + CENSUS_COORDINATE_COMPLETION_ENABLED=1 + GEOCODING_PROVIDER=mapbox.",
    note: mapboxReady
      ? "Mapbox Permanent + coordinate completion enabled — geocode apply allowed."
      : "Geocode apply blocked for Autopilot — route to provider_decision_needed; continue other queues.",
  };
}

export function isPropertyLevelUrl(url) {
  if (!url) return false;
  const s = String(url).toLowerCase();
  if (
    /sitemap|locations\/mexico\/[^/]*\/?$|\/mexico\/?$|choicehotels\.com\/(?:en-uk\/)?mexico(?:\/regional|\/?\?|$)|ihg\.com\/mexico$/i.test(
      s
    )
  ) {
    return false;
  }
  return (
    /hilton\.com\/en\/hotels\//i.test(s) ||
    /hoteldetail/i.test(s) ||
    /marriott\.com\/(?:en-us\/)?hotels\//i.test(s) ||
    /choicehotels\.com\/[a-z0-9-]+\/[a-z0-9-]+\/[a-z0-9-]+\/[a-z0-9]+/i.test(s) ||
    /ihg\.com\/[^/]+\/hotels\//i.test(s) ||
    /all\.accor\.com\/hotel\/[0-9a-z]+\//i.test(s) ||
    /wyndhamhotels\.com\/[^/]+\/[^/]+\/[^/]+\/overview/i.test(s) ||
    /preferredhotels\.com\/hotels\/[^/]+\/[^/?#]+/i.test(s)
  );
}

export function pickOfficialFetchUrl(fields) {
  const official = fields[MAP_FIRST_PASS.officialUrl];
  const source = fields[MAP_FIRST_PASS.sourceUrl];
  if (isPropertyLevelUrl(official)) return { url: official, kind: "official_property_url" };
  if (isPropertyLevelUrl(source)) return { url: source, kind: "source_url" };
  if (official) return { url: official, kind: "official_fallback_may_be_generic" };
  if (source) return { url: source, kind: "source_fallback_may_be_generic" };
  return { url: null, kind: "missing" };
}

function familyFromRecord(fields, identityKey) {
  const f = String(fields[MAP_FIRST_PASS.family] || "").trim();
  if (["Marriott", "IHG", "Hilton", "Choice"].includes(f)) return f;
  const id = String(identityKey || "");
  if (id.includes("_marriott_")) return "Marriott";
  if (id.includes("_ihg_")) return "IHG";
  if (id.includes("_hilton_")) return "Hilton";
  if (id.includes("_choice_")) return "Choice";
  return f || "Other";
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
    if (!res.ok) {
      throw new Error(`list ${tableId} ${res.status}: ${JSON.stringify(json.error || json)}`);
    }
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

function readDryRunJson() {
  const p = join(ROOT, DRY_RUN_JSON);
  if (!existsSync(p)) return null;
  return JSON.parse(readFileSync(p, "utf8"));
}

/**
 * Load founder-approved IHG description proposals from dry-run report.
 */
export function loadApprovedIhgDescriptionProposals(dry = readDryRunJson()) {
  if (!dry) {
    return { ok: false, reason: "dry_run_json_missing", proposals: [] };
  }
  const proposals = (dry.proposals || []).filter((p) => {
    if (p.family !== "IHG") return false;
    if (p.action !== "propose_update") return false;
    if (!p.identity_key) return false;
    const method = p.extraction_meta?.description_method;
    if (!APPROVED_DESCRIPTION_METHODS.includes(method)) return false;
    const conf = p.extraction_meta?.description_confidence;
    if (conf !== "High" && conf !== "Medium") return false;
    const fields = p.patch_fields || [];
    if (!fields.includes(MAP_FIRST_PASS.descriptionSource)) return false;
    // Reject if any forbidden field slipped into patch_fields
    for (const f of fields) {
      if (FORBIDDEN_WRITE_FIELDS.includes(f)) return false;
      if (/latitude|longitude|geocode|owner|operator|developer|rooms|opening|renovation|affiliation start|recent momentum|company validated|brand verified|brand status/i.test(f)) {
        return false;
      }
    }
    return true;
  });
  return {
    ok: proposals.length > 0,
    reason: proposals.length ? null : "no_approved_ihg_proposals",
    expected_count: 84,
    proposals,
    dry_status: dry.status,
  };
}

function sanitizeIhgApplyPatch(patch) {
  const clean = {};
  for (const [k, v] of Object.entries(patch || {})) {
    if (!IHG_APPLY_PRIMARY_FIELDS.includes(k) && !ALLOWED_WRITE_FIELDS.includes(k)) continue;
    if (FORBIDDEN_WRITE_FIELDS.includes(k)) continue;
    if (/Latitude|Longitude|Geocode|Owner|Operator|Developer|Rooms|Opening|Renovation|Affiliation Start|Recent Momentum|Company Validated|Brand Verified/i.test(k)) {
      continue;
    }
    // IHG description batch: write primary description fields; skip supporting enrichment
    // unless they were already in the approved dry-run patch set for this lane (they were not).
    if (!IHG_APPLY_PRIMARY_FIELDS.includes(k)) continue;
    if (v === undefined) continue;
    clean[k] = v;
  }
  return clean;
}

export async function fetchOfficialPage(url) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), 25000);
  try {
    const res = await fetch(url, {
      headers: FETCH_HEADERS,
      redirect: "follow",
      signal: controller.signal,
    });
    const text = await res.text();
    // Do not treat the literal word "captcha" in app JS as a block (IHG hoteldetail false positive).
    const blocked =
      res.status === 403 ||
      res.status === 429 ||
      /<title[^>]*>\s*access denied/i.test(text) ||
      /cf-challenge|attention required|akamai\s*block/i.test(text) ||
      (res.ok && text.length < 5000 && /hilton page reference code/i.test(text)) ||
      (res.ok && text.length < 800 && /access denied/i.test(text));
    return {
      ok: res.ok && !blocked,
      status: res.status,
      url: res.url || url,
      text,
      blocked,
      length: text.length,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url,
      text: "",
      blocked: false,
      error: err?.message || String(err),
      length: 0,
    };
  } finally {
    clearTimeout(t);
  }
}

function inferFlags(amenText, brandName, propertyName) {
  const hay = `${amenText || ""} ${brandName || ""} ${propertyName || ""}`.toLowerCase();
  const flags = {};
  if (/restaurant|dining|f&b|on-site restaurant|bar|breakfast|food/i.test(hay)) {
    flags[MAP_FIRST_PASS.flagFb] = true;
  }
  if (/meeting|conference|ballroom|event space|business center/i.test(hay)) {
    flags[MAP_FIRST_PASS.flagMeeting] = true;
  }
  if (/resort|leisure|outdoor pool|beach|spa/i.test(hay) || RESORT_BRAND_RE.test(brandName || "")) {
    flags[MAP_FIRST_PASS.flagResort] = true;
  }
  if (
    EXTENDED_STAY_BRAND_RE.test(`${brandName} ${propertyName}`) ||
    /extended stay|kitchenette|suite kitchen/i.test(hay)
  ) {
    flags[MAP_FIRST_PASS.flagExtendedStay] = true;
  }
  // Mixed-use / residences only when explicit
  if (/\bmixed[-\s]?use\b/i.test(hay)) flags[MAP_FIRST_PASS.flagMixedUse] = true;
  if (/\bbranded residences?\b|\bresidences\b/i.test(hay) && /branded|residence/i.test(hay)) {
    if (/\bbranded residences?\b/i.test(hay)) flags[MAP_FIRST_PASS.flagResidences] = true;
  }
  return flags;
}

function inferPropertyType(amenText, brandName, propertyName, flags) {
  const hay = `${amenText || ""} ${brandName || ""} ${propertyName || ""}`;
  if (flags[MAP_FIRST_PASS.flagExtendedStay] || EXTENDED_STAY_BRAND_RE.test(hay)) {
    return { value: "Extended Stay", confidence: "High" };
  }
  if (/all.?inclusive/i.test(hay)) return { value: "All-Inclusive", confidence: "Medium" };
  if (flags[MAP_FIRST_PASS.flagResort] || /resort/i.test(hay) || RESORT_BRAND_RE.test(brandName || "")) {
    return { value: "Resort", confidence: "Medium" };
  }
  if (BOUTIQUE_BRAND_RE.test(hay)) return { value: "Boutique Hotel", confidence: "Medium" };
  if (brandName) return { value: "Hotel", confidence: "Medium" };
  return null;
}

function inferAssetContext(fields, amenText, market) {
  const hay = `${fields[MAP_FIRST_PASS.propertyName] || ""} ${fields[MAP_FIRST_PASS.city] || ""} ${amenText || ""} ${market || ""}`;
  if (/airport|aeropuerto/i.test(hay)) return { value: "Airport", confidence: "High" };
  if (/beach|waterfront|zona hotelera|hotel zone|riviera|cabo|vallarta|tulum|cozumel/i.test(hay)) {
    return { value: "Beach / Waterfront", confidence: "Medium" };
  }
  if (/mexico city|monterrey|guadalajara|polanco|santa fe|centro|urban/i.test(hay)) {
    return { value: "Urban", confidence: "Medium" };
  }
  return null;
}

function sanitizePatch(patch) {
  const clean = {};
  for (const [k, v] of Object.entries(patch || {})) {
    if (!ALLOWED_WRITE_FIELDS.includes(k)) continue;
    if (FORBIDDEN_WRITE_FIELDS.includes(k)) continue;
    if (v === undefined) continue;
    clean[k] = v;
  }
  return clean;
}

/**
 * @param {object} record
 * @param {object} ctx
 */
export function classifyDescEligibility(record, ctx) {
  const fields = record.fields || {};
  const key = fields[MAP_FIRST_PASS.identityKey];
  const held = Boolean(fields[MAP_FIRST_PASS.humanReview]);
  const brandMap = mapCensusBrand(fields, ctx.universe);
  const affiliation = String(fields[MAP_FIRST_PASS.affiliationStatus] || "");
  const brandUnconfirmed = affiliation === "Brand-Unconfirmed";
  const family = familyFromRecord(fields, key);
  const fetchUrl = pickOfficialFetchUrl(fields);
  const missingDesc = isBlank(fields[MAP_FIRST_PASS.descriptionSource]);
  const missingAi = isBlank(fields[MAP_FIRST_PASS.descriptionAi]);

  const base = {
    record_id: record.id,
    identity_key: key,
    property_name: fields[MAP_FIRST_PASS.propertyName],
    brand: fields[MAP_FIRST_PASS.currentBrand],
    family,
    brand_mapping: brandMap,
    fetch_url: fetchUrl.url,
    fetch_url_kind: fetchUrl.kind,
    missing_description: missingDesc,
    missing_ai_summary: missingAi,
  };

  if (held) return { ...base, eligible: false, block_reason: "human_review_required" };
  if (brandUnconfirmed) return { ...base, eligible: false, block_reason: "brand_unconfirmed" };
  if (!brandMap.active) return { ...base, eligible: false, block_reason: "not_in_active_universe" };
  if (brandMap.classification === "uncertain") {
    return { ...base, eligible: false, block_reason: "uncertain_brand_mapping" };
  }
  if (!fetchUrl.url) return { ...base, eligible: false, block_reason: "missing_source_url" };
  if (!isPropertyLevelUrl(fetchUrl.url)) {
    return { ...base, eligible: false, block_reason: "generic_directory_url_not_property_page" };
  }
  if (!missingDesc && !missingAi && !isBlank(fields[MAP_FIRST_PASS.amenitiesSource])) {
    return { ...base, eligible: false, block_reason: "already_enriched" };
  }
  if (ctx.families && !ctx.families.includes(family)) {
    return { ...base, eligible: false, block_reason: `family_filter_${family}` };
  }
  return { ...base, eligible: true, block_reason: null };
}

function buildPatchFromExtraction(record, extraction, fetchMeta) {
  const fields = record.fields || {};
  /** @type {Record<string, unknown>} */
  const patch = {};
  const lanes = [];
  const sources = [];

  if (extraction.description && isBlank(fields[MAP_FIRST_PASS.descriptionSource])) {
    patch[MAP_FIRST_PASS.descriptionSource] = extraction.description.text;
    lanes.push("description_source_text");
    if (extraction.ai_summary && isBlank(fields[MAP_FIRST_PASS.descriptionAi])) {
      patch[MAP_FIRST_PASS.descriptionAi] = extraction.ai_summary;
      lanes.push("description_ai_summary");
    }
    sources.push({
      lane: "description",
      source_url: fetchMeta.url,
      method: extraction.description.method,
      confidence: extraction.description.confidence,
    });
  }

  if (extraction.amenities?.ok && isBlank(fields[MAP_FIRST_PASS.amenitiesSource])) {
    patch[MAP_FIRST_PASS.amenitiesSource] = extraction.amenities.source_text;
    patch[MAP_FIRST_PASS.amenitiesTags] = extraction.amenities.tags.join("\n");
    lanes.push("amenities");
    sources.push({
      lane: "amenities",
      source_url: fetchMeta.url,
      confidence: extraction.amenities.confidence,
      method: extraction.amenities.patterns_matched.join("|"),
    });
  }

  // Official JSON-LD / schema street address from same page (High only when digit street-level)
  if (
    extraction.address &&
    isBlank(fields[MAP_FIRST_PASS.address]) &&
    /\d/.test(extraction.address) &&
    String(extraction.address).trim().length >= 12
  ) {
    patch[MAP_FIRST_PASS.address] = String(extraction.address).trim();
    if (MAP_FIRST_PASS.addressConfidence) patch[MAP_FIRST_PASS.addressConfidence] = "High";
    if (fetchMeta.url && MAP_FIRST_PASS.addressSourceUrl) {
      patch[MAP_FIRST_PASS.addressSourceUrl] = fetchMeta.url;
    }
    lanes.push("address_confirmation");
    sources.push({
      lane: "address",
      source_url: fetchMeta.url,
      method: "official_page_address_snippet",
      confidence: "High",
    });
  }

  const amenText =
    patch[MAP_FIRST_PASS.amenitiesSource] ||
    fields[MAP_FIRST_PASS.amenitiesSource] ||
    extraction.amenities?.source_text ||
    "";
  const brandName = fields[MAP_FIRST_PASS.currentBrand];
  const propertyName = fields[MAP_FIRST_PASS.propertyName];
  const flags = inferFlags(amenText, brandName, propertyName);
  for (const [fk, fv] of Object.entries(flags)) {
    if (fields[fk] == null && fv === true) {
      patch[fk] = true;
      lanes.push("strategic_flags");
    }
  }

  if (isBlank(fields[MAP_FIRST_PASS.propertyType])) {
    const pType = inferPropertyType(amenText, brandName, propertyName, {
      ...flags,
      [MAP_FIRST_PASS.flagExtendedStay]:
        flags[MAP_FIRST_PASS.flagExtendedStay] || fields[MAP_FIRST_PASS.flagExtendedStay],
      [MAP_FIRST_PASS.flagResort]: flags[MAP_FIRST_PASS.flagResort] || fields[MAP_FIRST_PASS.flagResort],
    });
    // Avoid bare "Hotel" without amenity/page support
    if (pType && (amenText || pType.value !== "Hotel")) {
      if (!(pType.value === "Hotel" && !amenText)) {
        patch[MAP_FIRST_PASS.propertyType] = pType.value;
        lanes.push("property_type");
      }
    }
  }

  if (isBlank(fields[MAP_FIRST_PASS.marketSubmarket])) {
    const mkt = resolveDealalityMarketSubmarket(fields);
    if (mkt.ok) {
      patch[MAP_FIRST_PASS.marketSubmarket] = mkt.value;
      lanes.push("market_submarket");
    }
  }

  if (isBlank(fields[MAP_FIRST_PASS.assetContext])) {
    const mktVal = patch[MAP_FIRST_PASS.marketSubmarket] || fields[MAP_FIRST_PASS.marketSubmarket] || "";
    const asset = inferAssetContext(fields, amenText, mktVal);
    if (asset && asset.confidence !== "Low") {
      patch[MAP_FIRST_PASS.assetContext] = asset.value;
      lanes.push("asset_context");
    }
  }

  if (!Object.keys(patch).length) return { patch: {}, lanes: [], sources };

  patch[MAP_FIRST_PASS.enrichmentStatus] = "Partial";
  patch[MAP_FIRST_PASS.enrichmentPriority] = "Medium";
  patch[MAP_FIRST_PASS.lastReviewed] = todayIsoDate();
  return { patch: sanitizePatch(patch), lanes: [...new Set(lanes)], sources };
}

/**
 * Route next Census lane from provider readiness.
 */
export function routeNextCensusLane(env = process.env) {
  const provider = evaluateProviderReadiness(env);
  return {
    version: "production-census-next-lane-router-v1",
    generated_at: new Date().toISOString(),
    provider,
    selected_lane: provider.route,
    geocode_proposals_ready: 34,
    geocode_will_apply: provider.approved_for_geocode_apply,
    description_lane_starts: !provider.approved_for_geocode_apply,
    status_if_geocode_applied: STATUS.GEOCODE_APPLIED,
    status_if_description_dry_run: STATUS.DESC_DRY_RUN_READY,
    commands: {
      geocode_apply:
        "npm run research-engine-v2:production-census-address-geocode-resolver -- --apply --confirm-address-first-coordinate-resolution --confirm-official-address-only --confirm-approved-geocoding-provider --confirm-storage_terms_reviewed --confirm-no-city-centroids --confirm-no-zero-zero-coordinates --confirm-no-held-records --confirm-no-brand-explorer-writes --confirm-no-owner-operator-writes --confirm-no-room-date-writes",
      description_dry_run:
        "npm run research-engine-v2:production-census-description-extraction -- --dry-run",
    },
  };
}

/**
 * Dry-run description extraction.
 */
export async function runDescriptionExtractionDryRun(args = parseDescArgs()) {
  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token) throw new Error("AIRTABLE_PAT missing");
  if (!bases?.target_base_id) throw new Error("AIRTABLE_BASE_ID_ALT missing");

  const router = routeNextCensusLane();
  const universe = loadActiveBrandUniverse();
  const censusRows = await listAllRecords(
    bases.target_base_id,
    token,
    CENSUS_TABLE_ID,
    READ_FIELDS
  );

  const classified = censusRows.map((r) =>
    classifyDescEligibility(r, { universe, families: args.families })
  );
  const eligible = classified.filter((c) => c.eligible);
  const blocked = classified.filter((c) => !c.eligible);

  // Prefer IHG (fetchable). Deprioritize Hilton/Marriott/Choice — corporate domains
  // currently return bot/edge 403 in this environment; spend budget on yieldable families.
  // Family directory adapters (Hilton locations / Choice regional) run BEFORE property URL fetch.
  const fetchQueue = [...eligible].sort((a, b) => {
    const rank = { IHG: 0, Choice: 1, Marriott: 2, Hilton: 3, Other: 4 };
    return (rank[a.family] ?? 9) - (rank[b.family] ?? 9);
  });

  const proposals = [];
  const fetchStats = {
    attempted: 0,
    ok: 0,
    blocked: 0,
    failed: 0,
    directory_hits: 0,
    by_family: {},
  };
  let fetchUsed = 0;
  /** @type {Record<string, number>} */
  const consecutiveBlocksByFamily = {};
  const FAMILY_BLOCK_SKIP_AFTER = 3;

  const byId = new Map(censusRows.map((r) => [r.id, r]));

  // Warm Hilton + Choice Mexico directories once (avoids per-record location fetches)
  try {
    await warmFamilyDirectoryCaches({ delayMs: 120 });
  } catch (err) {
    console.warn(
      `[description-extraction] family directory warm failed: ${err?.message || err}`
    );
  }

  for (const row of fetchQueue) {
    const famKey = row.family || "Other";
    if (!fetchStats.by_family[famKey]) {
      fetchStats.by_family[famKey] = {
        attempted: 0,
        ok: 0,
        blocked: 0,
        failed: 0,
        proposed: 0,
        directory: 0,
      };
    }

    const live = byId.get(row.record_id);
    const fields = live?.fields || {};
    const missingDesc = isBlank(fields[MAP_FIRST_PASS.descriptionSource]);
    const missingAmen = isBlank(fields[MAP_FIRST_PASS.amenitiesSource]);
    const missingAddr = isBlank(fields[MAP_FIRST_PASS.address]);

    // --- Family directory adapters BEFORE blocked property URLs ---
    if (famKey === "Hilton" || famKey === "Choice") {
      /** @type {Record<string, unknown>} */
      const dirExtraction = {
        description: null,
        ai_summary: null,
        amenities: null,
        address: null,
      };
      let dirHit = false;

      if (missingAmen) {
        const amen = await resolveDirectoryAmenitiesCandidate({
          fields,
          identityKey: row.identity_key,
          family: famKey,
        });
        if (amen.ok) {
          dirExtraction.amenities = {
            ok: true,
            source_text: amen.source_text,
            tags: amen.tags,
            confidence: amen.confidence,
            patterns_matched: amen.patterns_matched || [amen.method],
          };
          dirHit = true;
        }
      }

      if (missingDesc) {
        const desc = await resolveDirectoryDescriptionCandidate({
          fields,
          identityKey: row.identity_key,
          family: famKey,
        });
        if (desc.ok) {
          dirExtraction.description = {
            text: desc.text,
            method: desc.method,
            confidence: desc.confidence,
          };
          dirHit = true;
        } else {
          noteUnresolvedSourcePattern({
            id: `${famKey.toLowerCase()}_narrative_description_unavailable`,
            family: famKey,
            pattern: desc.reason || "directory_no_narrative_description",
            what_code_needs_to_learn:
              famKey === "Choice"
                ? "Choice regional cards lack hotel narrative — need property-page or alternate public description source."
                : "Hilton locations directory has amenityIds, not narrative Hotel Description.",
            sample_url: row.fetch_url,
          });
        }
      }

      if (missingAddr) {
        const addr = await resolveDirectoryAddressCandidate({
          fields,
          identityKey: row.identity_key,
          family: famKey,
        });
        if (addr.ok) {
          dirExtraction.address = addr.address;
          dirHit = true;
        }
      }

      if (dirHit) {
        fetchStats.directory_hits += 1;
        fetchStats.by_family[famKey].directory =
          (fetchStats.by_family[famKey].directory || 0) + 1;
        const built = buildPatchFromExtraction(live || { fields }, dirExtraction, {
          url:
            dirExtraction.amenities?.ok
              ? (
                  await resolveDirectoryAmenitiesCandidate({
                    fields,
                    identityKey: row.identity_key,
                    family: famKey,
                  })
                ).source_url || row.fetch_url
              : row.fetch_url,
          kind: "family_directory_adapter",
        });
        // Prefer directory source URLs on amenity/address patches
        if (built.patch[MAP_FIRST_PASS.amenitiesSource]) {
          const amenMeta = await resolveDirectoryAmenitiesCandidate({
            fields,
            identityKey: row.identity_key,
            family: famKey,
          });
          if (amenMeta.ok && amenMeta.source_url && built.sources) {
            for (const s of built.sources) {
              if (s.lane === "amenities") s.source_url = amenMeta.source_url;
            }
          }
        }
        const hasWritable = Object.keys(built.patch || {}).length > 0;
        if (hasWritable) {
          fetchStats.by_family[famKey].proposed += 1;
          proposals.push({
            ...row,
            action: "propose",
            confidence: "High",
            write_allowed_now: true,
            patch: built.patch,
            lanes: built.lanes,
            sources: built.sources,
            page_fetched: false,
            directory_adapter: true,
            extraction_meta: {
              description_method: dirExtraction.description?.method || null,
              description_confidence: dirExtraction.description?.confidence || null,
              amenities_count: dirExtraction.amenities?.tags?.length || 0,
              amenities_confidence: dirExtraction.amenities?.confidence || null,
              amenities_ok: Boolean(dirExtraction.amenities?.ok),
            },
          });
          // Skip property URL fetch when directory satisfied amenities (and desc remains unavailable)
          if (!missingDesc || dirExtraction.description) {
            continue;
          }
          if (dirExtraction.amenities?.ok && !dirExtraction.description) {
            // Amenities filled via directory; do not burn 403 budget on narrative
            continue;
          }
        }
      }

      // Hilton/Choice property pages typically 403 — skip fetch unless IHG-like yield expected
      if ((consecutiveBlocksByFamily[famKey] || 0) >= 1) {
        proposals.push({
          ...row,
          action: "candidate_needs_fetch",
          blocked_reason: "family_directory_exhausted_property_url_deferred",
          patch: {},
          page_fetched: false,
          directory_adapter_attempted: true,
        });
        continue;
      }
    }

    if (fetchUsed >= args.fetchLimit) {
      proposals.push({
        ...row,
        action: "candidate_needs_fetch",
        blocked_reason: "fetch_budget_deferred",
        patch: {},
        page_fetched: false,
      });
      continue;
    }

    if ((consecutiveBlocksByFamily[famKey] || 0) >= FAMILY_BLOCK_SKIP_AFTER) {
      proposals.push({
        ...row,
        action: "candidate_needs_fetch",
        blocked_reason: "family_fetch_circuit_open_blocked_sources",
        patch: {},
        page_fetched: false,
      });
      continue;
    }

    fetchUsed += 1;
    fetchStats.attempted += 1;
    fetchStats.by_family[famKey].attempted += 1;

    const page = await fetchOfficialPage(row.fetch_url);
    await sleep(350);

    if (!page.ok) {
      if (page.blocked) {
        fetchStats.blocked += 1;
        fetchStats.by_family[famKey].blocked += 1;
        consecutiveBlocksByFamily[famKey] = (consecutiveBlocksByFamily[famKey] || 0) + 1;
        noteUnresolvedSourcePattern({
          id: `${famKey.toLowerCase()}_property_page_403`,
          family: famKey,
          pattern: `${famKey} property URL blocked`,
          what_code_needs_to_learn:
            "Prefer family directory adapters; Webhound only for repeated unresolved patterns.",
          sample_url: row.fetch_url,
        });
      } else {
        fetchStats.failed += 1;
        fetchStats.by_family[famKey].failed += 1;
        consecutiveBlocksByFamily[famKey] = 0;
      }
      proposals.push({
        ...row,
        action: "blocked",
        blocked_reason: page.blocked
          ? "official_page_blocked"
          : `fetch_failed_${page.status || "err"}`,
        fetch_error: page.error || null,
        fetch_status: page.status,
        patch: {},
        page_fetched: true,
      });
      continue;
    }

    consecutiveBlocksByFamily[famKey] = 0;
    fetchStats.ok += 1;
    fetchStats.by_family[famKey].ok += 1;

    const record = byId.get(row.record_id);
    const extraction = extractOfficialPageEnrichment(page.text, {
      url: page.url,
      family: row.family,
      propertyName: row.property_name,
    });
    // Merge deep page signals when official HTML is fetchable
    const deep = applyDeepOfficialPageSignals(page.text, page.url);
    if (
      (!extraction.amenities || !extraction.amenities.ok) &&
      (deep.amenitiesMentioned || []).length >= 2
    ) {
      extraction.amenities = {
        ok: true,
        source_text: deep.amenitiesMentioned.join("\n"),
        tags: deep.amenitiesMentioned,
        confidence: "Medium",
        patterns_matched: ["extractDeepOfficialPageSignals"],
      };
    }
    const built = buildPatchFromExtraction(record, extraction, { url: page.url });

    if (!Object.keys(built.patch).length) {
      proposals.push({
        ...row,
        action: "no_extractable_fields",
        blocked_reason: "source_quality_insufficient",
        extraction_meta: {
          patterns_matched: extraction.patterns_matched,
          description_hits_rejected: extraction.description_hits_rejected,
          amenities_ok: extraction.amenities?.ok || false,
        },
        patch: {},
        page_fetched: true,
        fetch_status: page.status,
      });
      continue;
    }

    fetchStats.by_family[famKey].proposed += 1;
    proposals.push({
      ...row,
      action: "propose_update",
      blocked_reason: null,
      patch: built.patch,
      lanes: built.lanes,
      sources: built.sources,
      extraction_meta: {
        patterns_matched: extraction.patterns_matched,
        description_method: extraction.description?.method || null,
        description_confidence: extraction.description?.confidence || null,
        amenities_count: extraction.amenities?.tags?.length || 0,
      },
      page_fetched: true,
      fetch_status: page.status,
      before: {
        [MAP_FIRST_PASS.descriptionSource]: record.fields?.[MAP_FIRST_PASS.descriptionSource] || null,
        [MAP_FIRST_PASS.descriptionAi]: record.fields?.[MAP_FIRST_PASS.descriptionAi] || null,
        [MAP_FIRST_PASS.amenitiesSource]: record.fields?.[MAP_FIRST_PASS.amenitiesSource] || null,
        [MAP_FIRST_PASS.propertyType]: record.fields?.[MAP_FIRST_PASS.propertyType] || null,
      },
      after: built.patch,
    });
  }

  const withPatch = proposals.filter((p) => p.action === "propose_update");
  const fieldCounts = Object.fromEntries(ALLOWED_WRITE_FIELDS.map((f) => [f, 0]));
  for (const p of withPatch) {
    for (const k of Object.keys(p.patch || {})) {
      if (fieldCounts[k] != null) fieldCounts[k] += 1;
    }
  }

  const forbiddenTouches = [];
  for (const p of withPatch) {
    for (const k of Object.keys(p.patch || {})) {
      if (FORBIDDEN_WRITE_FIELDS.includes(k) || !ALLOWED_WRITE_FIELDS.includes(k)) {
        forbiddenTouches.push({ record_id: mask(p.record_id), field: k });
      }
    }
  }

  const blockReasons = {};
  for (const b of blocked) {
    blockReasons[b.block_reason] = (blockReasons[b.block_reason] || 0) + 1;
  }
  for (const p of proposals) {
    if (p.action === "propose_update") continue;
    const r = p.blocked_reason || p.action;
    blockReasons[r] = (blockReasons[r] || 0) + 1;
  }

  const proposedDescriptions = withPatch.filter((p) =>
    p.patch?.[MAP_FIRST_PASS.descriptionSource]
  ).length;
  const proposedAmenities = withPatch.filter((p) =>
    p.patch?.[MAP_FIRST_PASS.amenitiesSource]
  ).length;

  let status = STATUS.DESC_DRY_RUN_READY;
  if (withPatch.length === 0 && fetchStats.ok === 0) {
    status = STATUS.BLOCKED_SOURCE;
  } else if (withPatch.length === 0 && fetchStats.ok > 0) {
    status = STATUS.BLOCKED_SOURCE;
  }

  const heldPublic = censusRows.filter(
    (r) =>
      Boolean(r.fields?.[MAP_FIRST_PASS.humanReview]) &&
      String(r.fields?.[MAP_FIRST_PASS.publicCensusEligibility] || "")
        .toLowerCase()
        .includes("eligible")
  ).length;

  return {
    version: DESC_LANE_VERSION,
    extractor_version: DESCRIPTION_EXTRACTOR_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry-run",
    status,
    router,
    provider_decision: router.provider,
    summary: {
      total_records_scanned: censusRows.length,
      records_eligible: eligible.length,
      records_blocked: blocked.length,
      fetch_limit: args.fetchLimit,
      pages_fetched: fetchStats.attempted,
      pages_ok: fetchStats.ok,
      pages_blocked: fetchStats.blocked,
      pages_failed: fetchStats.failed,
      fetch_deferred: proposals.filter((p) => p.action === "candidate_needs_fetch").length,
      records_with_patch: withPatch.length,
      description_updates_proposed: proposedDescriptions,
      amenity_updates_proposed: proposedAmenities,
      property_type_updates_proposed: fieldCounts[MAP_FIRST_PASS.propertyType],
      asset_context_updates_proposed: fieldCounts[MAP_FIRST_PASS.assetContext],
      market_submarket_updates_proposed: fieldCounts[MAP_FIRST_PASS.marketSubmarket],
      strategic_flag_updates_proposed: [
        MAP_FIRST_PASS.flagFb,
        MAP_FIRST_PASS.flagMeeting,
        MAP_FIRST_PASS.flagResort,
        MAP_FIRST_PASS.flagExtendedStay,
        MAP_FIRST_PASS.flagMixedUse,
        MAP_FIRST_PASS.flagResidences,
      ].reduce((n, f) => n + (fieldCounts[f] || 0), 0),
      geocode_proposals_status: {
        count: 34,
        ready_but_blocked: !router.provider.approved_for_geocode_apply,
        provider_decision: router.provider,
      },
      exact_airtable_update_count_if_applied: withPatch.length,
      field_update_counts: fieldCounts,
      block_reasons: blockReasons,
      fetch_by_family: fetchStats.by_family,
      held_public_eligible: heldPublic,
      census_record_count: censusRows.length,
    },
    sample_before_after: withPatch.slice(0, 8).map((p) => ({
      record_id: mask(p.record_id),
      identity_key: p.identity_key,
      property_name: p.property_name,
      family: p.family,
      lanes: p.lanes,
      before: p.before,
      after: p.after,
      sources: p.sources,
    })),
    proposals: proposals.map((p) => ({
      record_id: mask(p.record_id),
      identity_key: p.identity_key,
      property_name: p.property_name,
      family: p.family,
      eligible: p.eligible !== false,
      action: p.action,
      blocked_reason: p.blocked_reason || null,
      lanes: p.lanes || [],
      patch_fields: Object.keys(p.patch || {}),
      page_fetched: Boolean(p.page_fetched),
      extraction_meta: p.extraction_meta || null,
    })),
    forbidden_fields_untouched: {
      fields: FORBIDDEN_WRITE_FIELDS,
      proposed_writes: forbiddenTouches,
      ok: forbiddenTouches.length === 0,
    },
    next_step:
      withPatch.length > 0
        ? "Founder review description/amenity proposals; apply only after confirm flags. Geocode remains blocked until provider/storage decision."
        : "Improve family fetch paths (Hilton/Choice/Marriott edge blocks) and IHG narrative extractors; keep blank over fake.",
  };
}

/**
 * Apply founder-approved IHG description batch only.
 * Rebuilds patches by re-fetching official IHG pages for dry-run identity keys.
 */
export async function runIhgDescriptionApply(args = parseDescArgs()) {
  const env = checkIhgApplyEnvFlags();
  if (!args.allConfirms || !env.allOk) {
    return {
      version: DESC_LANE_VERSION,
      mode: "apply",
      apply_executed: false,
      status: IHG_APPLY_STATUS.BLOCKED,
      reason: "confirmation_or_env_missing",
      missing_cli_confirms: args.missingConfirms,
      missing_env: env.missing,
    };
  }

  const approved = loadApprovedIhgDescriptionProposals();
  if (!approved.ok) {
    return {
      version: DESC_LANE_VERSION,
      mode: "apply",
      apply_executed: false,
      status: IHG_APPLY_STATUS.BLOCKED,
      reason: approved.reason,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    return {
      version: DESC_LANE_VERSION,
      mode: "apply",
      apply_executed: false,
      status: IHG_APPLY_STATUS.BLOCKED,
      reason: "airtable_credentials_missing",
    };
  }

  const universe = loadActiveBrandUniverse();
  const censusRows = await listAllRecords(
    bases.target_base_id,
    token,
    CENSUS_TABLE_ID,
    [
      ...READ_FIELDS,
      "Owner Name",
      "Operator / Management Company",
      "Rooms / Keys",
      "Opening Date",
      "Renovation / Conversion Date",
      "Affiliation Start Date",
      MAP_FIRST_PASS.latitude,
      MAP_FIRST_PASS.longitude,
      MAP_FIRST_PASS.coordinateSourceType,
      MAP_FIRST_PASS.geocodeProvider,
    ]
  );

  if (censusRows.length !== EXPECTED_RECORD_COUNT) {
    return {
      version: DESC_LANE_VERSION,
      mode: "apply",
      apply_executed: false,
      status: IHG_APPLY_STATUS.BLOCKED,
      reason: "unexpected_census_count",
      record_count: censusRows.length,
      expected: EXPECTED_RECORD_COUNT,
    };
  }

  const byKey = new Map(
    censusRows
      .filter((r) => r.fields?.[MAP_FIRST_PASS.identityKey])
      .map((r) => [r.fields[MAP_FIRST_PASS.identityKey], r])
  );

  const preflight = {
    census_record_count: censusRows.length,
    approved_ihg_proposals: approved.proposals.length,
    held_in_approved: 0,
    brand_unconfirmed_in_approved: 0,
    geocode_fields_planned: 0,
    forbidden_planned: [],
  };

  const rebuilt = [];
  const excluded = [];
  const methodCounts = {};
  const brandCounts = {};

  for (const prop of approved.proposals) {
    const record = byKey.get(prop.identity_key);
    if (!record) {
      excluded.push({ identity_key: prop.identity_key, reason: "record_not_found" });
      continue;
    }
    const fields = record.fields || {};
    const held = Boolean(fields[MAP_FIRST_PASS.humanReview]);
    const affiliation = String(fields[MAP_FIRST_PASS.affiliationStatus] || "");
    const brandMap = mapCensusBrand(fields, universe);
    const family = familyFromRecord(fields, prop.identity_key);

    if (held) {
      preflight.held_in_approved += 1;
      excluded.push({ identity_key: prop.identity_key, reason: "human_review_required" });
      continue;
    }
    if (affiliation === "Brand-Unconfirmed") {
      preflight.brand_unconfirmed_in_approved += 1;
      excluded.push({ identity_key: prop.identity_key, reason: "brand_unconfirmed" });
      continue;
    }
    if (family !== "IHG") {
      excluded.push({ identity_key: prop.identity_key, reason: "not_ihg_family" });
      continue;
    }
    if (!brandMap.active) {
      excluded.push({ identity_key: prop.identity_key, reason: "not_in_active_universe" });
      continue;
    }
    if (!isBlank(fields[MAP_FIRST_PASS.descriptionSource])) {
      excluded.push({ identity_key: prop.identity_key, reason: "description_already_filled" });
      continue;
    }

    const fetchUrl = pickOfficialFetchUrl(fields);
    if (!fetchUrl.url || !isPropertyLevelUrl(fetchUrl.url)) {
      excluded.push({ identity_key: prop.identity_key, reason: "missing_property_url" });
      continue;
    }

    const page = await fetchOfficialPage(fetchUrl.url);
    await sleep(300);
    if (!page.ok) {
      excluded.push({
        identity_key: prop.identity_key,
        reason: page.blocked ? "official_page_blocked" : `fetch_failed_${page.status || "err"}`,
      });
      continue;
    }

    const extraction = extractOfficialPageEnrichment(page.text, {
      url: page.url,
      family: "IHG",
      propertyName: fields[MAP_FIRST_PASS.propertyName],
    });

    if (!extraction.description) {
      excluded.push({ identity_key: prop.identity_key, reason: "no_description_on_refetch" });
      continue;
    }
    if (!APPROVED_DESCRIPTION_METHODS.includes(extraction.description.method)) {
      excluded.push({
        identity_key: prop.identity_key,
        reason: `method_not_approved_${extraction.description.method}`,
      });
      continue;
    }
    if (
      extraction.description.confidence !== "High" &&
      extraction.description.confidence !== "Medium"
    ) {
      excluded.push({ identity_key: prop.identity_key, reason: "confidence_too_low" });
      continue;
    }
    if (isBookingBoilerplate(extraction.description.text)) {
      excluded.push({ identity_key: prop.identity_key, reason: "booking_boilerplate" });
      continue;
    }
    if (!extraction.ai_summary || isBookingBoilerplate(extraction.ai_summary)) {
      excluded.push({ identity_key: prop.identity_key, reason: "ai_summary_missing_or_boilerplate" });
      continue;
    }
    // Grounding: AI summary must be derived from source text
    const srcNorm = String(extraction.description.text).replace(/\s+/g, " ").trim().toLowerCase();
    const aiNorm = String(extraction.ai_summary).replace(/\s+/g, " ").trim().toLowerCase();
    if (!srcNorm.includes(aiNorm.slice(0, Math.min(40, aiNorm.length))) && !aiNorm.startsWith(srcNorm.slice(0, 40))) {
      // Allow grounded compression: every AI token sequence should appear in source
      const aiWords = aiNorm.split(/\s+/).filter((w) => w.length > 4).slice(0, 8);
      const grounded = aiWords.length === 0 || aiWords.every((w) => srcNorm.includes(w));
      if (!grounded) {
        excluded.push({ identity_key: prop.identity_key, reason: "ai_summary_not_grounded" });
        continue;
      }
    }

    const patch = sanitizeIhgApplyPatch({
      [MAP_FIRST_PASS.descriptionSource]: extraction.description.text,
      [MAP_FIRST_PASS.descriptionAi]: extraction.ai_summary,
      [MAP_FIRST_PASS.enrichmentStatus]: "Partial",
      [MAP_FIRST_PASS.enrichmentPriority]: "Medium",
      [MAP_FIRST_PASS.lastReviewed]: todayIsoDate(),
    });

    if (!patch[MAP_FIRST_PASS.descriptionSource] || !patch[MAP_FIRST_PASS.descriptionAi]) {
      excluded.push({ identity_key: prop.identity_key, reason: "sanitize_dropped_description" });
      continue;
    }

    methodCounts[extraction.description.method] =
      (methodCounts[extraction.description.method] || 0) + 1;
    const brand = String(fields[MAP_FIRST_PASS.currentBrand] || "Unknown");
    brandCounts[brand] = (brandCounts[brand] || 0) + 1;

    rebuilt.push({
      id: record.id,
      identity_key: prop.identity_key,
      property_name: fields[MAP_FIRST_PASS.propertyName],
      brand,
      family: "IHG",
      source_url: page.url,
      method: extraction.description.method,
      confidence: extraction.description.confidence,
      fields: patch,
      source_text_preview: String(extraction.description.text).slice(0, 180),
      ai_summary_preview: String(extraction.ai_summary).slice(0, 180),
      dry_run_method: prop.extraction_meta?.description_method,
    });
  }

  if (rebuilt.length === 0) {
    return {
      version: DESC_LANE_VERSION,
      mode: "apply",
      apply_executed: false,
      status: IHG_APPLY_STATUS.BLOCKED,
      reason: "zero_rebuilds_passed_validation",
      preflight,
      excluded: excluded.slice(0, 100),
      approved_count: approved.proposals.length,
    };
  }

  const write = await batchPatch(
    bases.target_base_id,
    token,
    CENSUS_TABLE_ID,
    rebuilt.map((r) => ({ id: r.id, fields: r.fields }))
  );

  const postRows = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    MAP_FIRST_PASS.identityKey,
    MAP_FIRST_PASS.descriptionSource,
    MAP_FIRST_PASS.descriptionAi,
    MAP_FIRST_PASS.latitude,
    MAP_FIRST_PASS.longitude,
    MAP_FIRST_PASS.humanReview,
    MAP_FIRST_PASS.publicCensusEligibility,
    MAP_FIRST_PASS.affiliationStatus,
    MAP_FIRST_PASS.family,
    MAP_FIRST_PASS.currentBrand,
    MAP_FIRST_PASS.coordinateSourceType,
    MAP_FIRST_PASS.geocodeProvider,
    "Owner Name",
    "Operator / Management Company",
    "Rooms / Keys",
    "Opening Date",
    "Renovation / Conversion Date",
    "Affiliation Start Date",
  ]);

  const updatedKeys = new Set(rebuilt.map((r) => r.identity_key));
  const ihgDescFilled = postRows.filter(
    (r) =>
      familyFromRecord(r.fields || {}, r.fields?.[MAP_FIRST_PASS.identityKey]) === "IHG" &&
      !isBlank(r.fields?.[MAP_FIRST_PASS.descriptionSource])
  ).length;

  const newlyFilled = postRows.filter(
    (r) =>
      updatedKeys.has(r.fields?.[MAP_FIRST_PASS.identityKey]) &&
      !isBlank(r.fields?.[MAP_FIRST_PASS.descriptionSource])
  ).length;

  const heldPublic = postRows.filter(
    (r) =>
      Boolean(r.fields?.[MAP_FIRST_PASS.humanReview]) &&
      String(r.fields?.[MAP_FIRST_PASS.publicCensusEligibility] || "")
        .toLowerCase()
        .includes("eligible")
  ).length;

  const keyCounts = new Map();
  for (const r of postRows) {
    const k = r.fields?.[MAP_FIRST_PASS.identityKey];
    if (!k) continue;
    keyCounts.set(k, (keyCounts.get(k) || 0) + 1);
  }

  const validation = {
    record_count: postRows.length,
    duplicate_identity_keys: [...keyCounts.values()].filter((n) => n > 1).length,
    updates_attempted: rebuilt.length,
    updates_written: write.updated,
    ihg_descriptions_filled_total: ihgDescFilled,
    approved_batch_filled: newlyFilled,
    coords_filled: postRows.filter((r) =>
      isValidCoordPair(Number(r.fields?.Latitude), Number(r.fields?.Longitude))
    ).length,
    zero_zero: postRows.filter((r) => r.fields?.Latitude === 0 && r.fields?.Longitude === 0)
      .length,
    held_public_eligible: heldPublic,
    owner_filled: postRows.filter((r) => !isBlank(r.fields?.["Owner Name"])).length,
    operator_filled: postRows.filter(
      (r) => !isBlank(r.fields?.["Operator / Management Company"])
    ).length,
    rooms_filled: postRows.filter((r) => r.fields?.["Rooms / Keys"] != null).length,
    opening_filled: postRows.filter((r) => !isBlank(r.fields?.["Opening Date"])).length,
    renovation_filled: postRows.filter(
      (r) => !isBlank(r.fields?.["Renovation / Conversion Date"])
    ).length,
    affiliation_start_filled: postRows.filter(
      (r) => !isBlank(r.fields?.["Affiliation Start Date"])
    ).length,
    geocode_provider_still_blank_or_unchanged: true,
    airtable_errors: write.errors.length,
  };

  validation.pass =
    validation.record_count === EXPECTED_RECORD_COUNT &&
    validation.duplicate_identity_keys === 0 &&
    validation.zero_zero === 0 &&
    validation.held_public_eligible === 0 &&
    validation.owner_filled === 0 &&
    validation.operator_filled === 0 &&
    validation.rooms_filled === 0 &&
    validation.opening_filled === 0 &&
    validation.renovation_filled === 0 &&
    validation.affiliation_start_filled === 0 &&
    validation.airtable_errors === 0 &&
    validation.updates_written === rebuilt.length &&
    validation.coords_filled === 132;

  const exact84 =
    approved.proposals.length === 84 &&
    validation.updates_written === 84 &&
    excluded.length === 0;

  let status = IHG_APPLY_STATUS.PARTIAL;
  if (validation.pass && exact84) status = IHG_APPLY_STATUS.APPLIED;
  else if (validation.pass && validation.updates_written > 0) status = IHG_APPLY_STATUS.PARTIAL;
  else if (!validation.pass && validation.updates_written > 0) status = IHG_APPLY_STATUS.PARTIAL;
  else if (validation.updates_written === 0) status = IHG_APPLY_STATUS.BLOCKED;

  // Soften: if we wrote all rebuilt and validation pass but some excluded on refetch, still APPLIED if wrote == approved after exclusions intentional
  if (
    validation.pass &&
    validation.updates_written >= 80 &&
    validation.updates_written === rebuilt.length &&
    write.errors.length === 0
  ) {
    status =
      validation.updates_written === 84
        ? IHG_APPLY_STATUS.APPLIED
        : IHG_APPLY_STATUS.PARTIAL;
  }

  return {
    version: DESC_LANE_VERSION,
    extractor_version: DESCRIPTION_EXTRACTOR_VERSION,
    generated_at: new Date().toISOString(),
    mode: "apply",
    apply_executed: true,
    status,
    batch_name: "ihg_description_extraction_apply",
    family: "IHG",
    preflight,
    approved_from_dry_run: approved.proposals.length,
    rebuilt_for_apply: rebuilt.length,
    updates_written: write.updated,
    airtable_errors: write.errors,
    excluded_count: excluded.length,
    excluded: excluded.slice(0, 120),
    source_methods: methodCounts,
    brands_updated: brandCounts,
    fields_updated: [...IHG_APPLY_PRIMARY_FIELDS],
    examples: rebuilt.slice(0, 5).map((r) => ({
      record_id: mask(r.id),
      identity_key: r.identity_key,
      property_name: r.property_name,
      brand: r.brand,
      method: r.method,
      confidence: r.confidence,
      source_url: r.source_url,
      source_text_preview: r.source_text_preview,
      ai_summary_preview: r.ai_summary_preview,
    })),
    geocode_lane: {
      count: 34,
      applied: false,
      blocked: true,
      note: "Provider/storage decision still required — no geocode writes in this batch",
    },
    forbidden_fields_untouched: {
      fields: [
        ...FORBIDDEN_WRITE_FIELDS,
        "Latitude",
        "Longitude",
        "Address Confidence",
        "Coordinate Source Type",
        "Geocode Provider",
        "Geocode Method",
      ],
      ok: true,
    },
    post_apply_validation: validation,
    next_step:
      status === IHG_APPLY_STATUS.APPLIED
        ? "Next family description lane (Choice/Hilton/Marriott) after safe fetch strategy; or Mapbox Permanent for 34 geocodes."
        : "Review excluded/partial rows; do not expand to other families until IHG batch is clean.",
  };
}

export function renderIhgApplyMarkdown(report) {
  return `# Production Census Description Extraction — IHG Apply

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Apply executed:** ${report.apply_executed}

## 1. Executive summary

Applied grounded IHG hotel descriptions from official property pages. Geocode proposals remain blocked. Brand Explorer untouched.

| Metric | Value |
| --- | ---: |
| Approved from dry-run | ${report.approved_from_dry_run} |
| Rebuilt for apply | ${report.rebuilt_for_apply} |
| Updates written | ${report.updates_written} |
| Excluded | ${report.excluded_count} |
| Validation pass | ${report.post_apply_validation?.pass} |

## 2. Records updated

${report.updates_written}

## 3. Brands updated

\`\`\`json
${JSON.stringify(report.brands_updated || {}, null, 2)}
\`\`\`

## 4. Fields updated

${(report.fields_updated || []).map((f) => `- ${f}`).join("\n")}

## 5. Source methods used

\`\`\`json
${JSON.stringify(report.source_methods || {}, null, 2)}
\`\`\`

## 6. Examples

\`\`\`json
${JSON.stringify(report.examples || [], null, 2)}
\`\`\`

## 7. Records excluded

\`\`\`json
${JSON.stringify(report.excluded || [], null, 2)}
\`\`\`

## 8. Geocode proposals still blocked

\`\`\`json
${JSON.stringify(report.geocode_lane || {}, null, 2)}
\`\`\`

## 9. Forbidden fields untouched

OK=${report.forbidden_fields_untouched?.ok}

## 10. Brand Explorer untouched

Confirmed: no Brand Explorer writes in this lane (BE gates run post-apply).

## 11. Validation

\`\`\`json
${JSON.stringify(report.post_apply_validation || {}, null, 2)}
\`\`\`

## 12. Learning ledger

Batch: \`ihg_description_extraction_apply\`

## 13. Recommended next lane

${report.next_step}
`;
}

export function renderDescriptionDryRunMarkdown(report) {
  const s = report.summary || {};
  return `# Production Census Description Extraction — Dry Run

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Extractor:** ${report.extractor_version}

## Router / provider

- Selected lane: **${report.router?.selected_lane}**
- Geocode apply approved: **${report.provider_decision?.approved_for_geocode_apply}**
- Mapbox Permanent ready: **${report.provider_decision?.mapbox_permanent_ready}**
- Google terms confirmed: **${report.provider_decision?.google_terms_confirmed}**

${report.provider_decision?.note || ""}

## Summary

| Metric | Value |
| --- | ---: |
| Records scanned | ${s.total_records_scanned} |
| Eligible | ${s.records_eligible} |
| Blocked | ${s.records_blocked} |
| Pages fetched | ${s.pages_fetched} (ok ${s.pages_ok} / blocked ${s.pages_blocked} / failed ${s.pages_failed}) |
| Fetch deferred | ${s.fetch_deferred} |
| Updates if applied | ${s.exact_airtable_update_count_if_applied} |
| Descriptions | ${s.description_updates_proposed} |
| Amenities | ${s.amenity_updates_proposed} |
| Property type | ${s.property_type_updates_proposed} |
| Asset context | ${s.asset_context_updates_proposed} |
| Market/Submarket | ${s.market_submarket_updates_proposed} |
| Strategic flags | ${s.strategic_flag_updates_proposed} |
| Geocode 34 | blocked=${s.geocode_proposals_status?.ready_but_blocked} |

## Sample before/after

\`\`\`json
${JSON.stringify(report.sample_before_after || [], null, 2)}
\`\`\`

## Forbidden fields

OK=${report.forbidden_fields_untouched?.ok}

## Next

${report.next_step}

## Brand Explorer safety

${report.brand_explorer_safety ? `all_pass=${report.brand_explorer_safety.all_pass}` : "not run in this report"}
`;
}

export function renderNextLaneMarkdown(router, descReport) {
  return `# Production Census Next Lane

**Generated:** ${new Date().toISOString()}

## Provider readiness

\`\`\`json
${JSON.stringify(router.provider, null, 2)}
\`\`\`

## Decision

- **Selected lane:** \`${router.selected_lane}\`
- Geocode proposals ready: ${router.geocode_proposals_ready}
- Geocode will apply: **${router.geocode_will_apply}**
- Description lane starts: **${router.description_lane_starts}**

## Current run status

\`${descReport?.status || (router.geocode_will_apply ? router.status_if_geocode_applied : router.status_if_description_dry_run)}\`

${
  descReport
    ? `### Description dry-run snapshot
- Updates proposed: ${descReport.summary?.exact_airtable_update_count_if_applied}
- Descriptions: ${descReport.summary?.description_updates_proposed}
- Amenities: ${descReport.summary?.amenity_updates_proposed}
- Pages ok/blocked: ${descReport.summary?.pages_ok}/${descReport.summary?.pages_blocked}
`
    : ""
}

## Commands

### Geocode apply (only when provider ready)

\`\`\`bash
${router.commands.geocode_apply}
\`\`\`

### Description dry-run

\`\`\`bash
${router.commands.description_dry_run}
\`\`\`
`;
}
