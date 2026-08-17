/**
 * Production Census address-first coordinate resolver.
 *
 * Order: official Source URL → brand/property page → structured address/coords →
 * confirm address → official coords OR geocode name+street address only.
 *
 * Dry-run by default. No Webhound. No Brand Explorer writes.
 */

import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  loadActiveBrandUniverse,
  mapCensusBrand,
  MAP_FIRST_PASS,
  loadVicClaimIndex,
} from "./production-census-first-pass-enrichment.js";
import {
  extractCoordinatesFromOfficialHtml,
  selectBestCoordinateHit,
  isValidCoordPair,
  matchesRejectedPin,
} from "./production-census-coordinate-extractor.js";
import {
  resolveGeocodingProvider,
  geocodingTermsWarnings,
  geocodeOfficialAddress,
  isStreetLevelAddress,
  estimateGeocodeCostUsd,
} from "./production-census-geocoding-providers.js";
import {
  resolveDirectoryAddressCandidate,
  applyDeepOfficialPageSignals,
} from "./census-autopilot-family-directory-adapters.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const RESOLVER_VERSION = "production-census-address-geocode-resolver-v1";
export const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];

export const STATUS = Object.freeze({
  READY: "production_census_address_geocode_dry_run_ready_for_founder_review",
  NEEDS_PROVIDER_OR_TERMS: "production_census_address_geocode_needs_provider_or_terms_decision",
  NEEDS_SCHEMA_V113: "production_census_address_geocode_needs_schema_v113",
  BLOCKED_SOURCE_QUALITY: "production_census_address_geocode_blocked_by_source_quality",
});

/** Supporting provenance fields — added in schema v1.1.3 (blank until geocode apply). */
export const RECOMMENDED_V113_FIELDS = Object.freeze([
  { name: "Address Confidence", type: "singleSelect", options: ["High", "Medium", "Low", "Hold"] },
  { name: "Address Source URL", type: "url" },
  {
    name: "Coordinate Source Type",
    type: "singleSelect",
    options: [
      "official_coordinates",
      "official_address_geocode",
      "existing_source",
      "structured_data_extraction",
      "embedded_map_extraction",
      "blocked_low_confidence",
      "blocked_no_official_address",
      "steward_review",
    ],
  },
  { name: "Coordinate Confidence", type: "singleSelect", options: ["High", "Medium", "Low", "Hold"] },
  {
    name: "Geocode Provider",
    type: "singleSelect",
    options: ["Mapbox", "Google", "Official Page", "Existing Source", "Manual Review", "None"],
  },
  {
    name: "Geocode Method",
    type: "singleSelect",
    options: [
      "official_coordinates",
      "official_address_geocode",
      "structured_data_extraction",
      "embedded_map_extraction",
      "manual_review",
      "none",
    ],
  },
  { name: "Geocode Reviewed Date", type: "date" },
]);

export const ALLOWED_FUTURE_WRITE_FIELDS = Object.freeze([
  MAP_FIRST_PASS.address,
  MAP_FIRST_PASS.latitude,
  MAP_FIRST_PASS.longitude,
  MAP_FIRST_PASS.radarGeographyStatus,
  MAP_FIRST_PASS.radarDisplayStatus,
  MAP_FIRST_PASS.radarDisplayReason,
  MAP_FIRST_PASS.publicCensusEligibility,
  MAP_FIRST_PASS.publicDisplayConfidence,
  MAP_FIRST_PASS.publicDisplayReviewStatus,
  MAP_FIRST_PASS.lastReviewed,
]);

export const FORBIDDEN_WRITE_FIELDS = Object.freeze([
  "Owner Name",
  "Developer Name",
  "Operator / Management Company",
  "Rooms / Keys",
  "Opening Date",
  "Renovation / Conversion Date",
  "Affiliation Start Date",
  "Company Validated",
  "Brand Verified",
  "Recent Momentum",
]);

export const APPLY_CONFIRM_FLAGS = Object.freeze([
  "--confirm-address-first-coordinate-resolution",
  "--confirm-official-address-only",
  "--confirm-approved-geocoding-provider",
  "--confirm-storage_terms_reviewed",
  "--confirm-no-city-centroids",
  "--confirm-no-zero-zero-coordinates",
  "--confirm-no-held-records",
  "--confirm-no-brand-explorer-writes",
  "--confirm-no-owner-operator-writes",
  "--confirm-no-room-date-writes",
]);

const FETCH_HEADERS = {
  "user-agent":
    "Mozilla/5.0 (compatible; DealalityCensusAddressGeocodeResolver/1.0; +https://dealality.com)",
  accept: "text/html,application/xhtml+xml,application/json",
};

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
function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function parseAddressGeocodeArgs(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--") && !a.includes("=")));
  const getNum = (name, fallback) => {
    const hit = argv.find((a) => a.startsWith(`${name}=`));
    if (!hit) return fallback;
    const n = Number(hit.split("=")[1]);
    return Number.isFinite(n) ? n : fallback;
  };
  const getStr = (name) => {
    const hit = argv.find((a) => a.startsWith(`${name}=`));
    return hit ? hit.split("=").slice(1).join("=").trim() : null;
  };
  const confirms = APPLY_CONFIRM_FLAGS.filter((f) => flags.has(f));
  return {
    dryRun: flags.has("--dry-run") || !flags.has("--apply"),
    apply: flags.has("--apply"),
    fetchLimit: getNum("--fetch-limit", 30),
    geocodeLimit: getNum("--geocode-limit", 80),
    delayMs: getNum("--delay-ms", 350),
    providerOverride: getStr("--provider"),
    families: (() => {
      const hit = argv.find((a) => a.startsWith("--families="));
      if (!hit) return null;
      return hit
        .split("=")[1]
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    })(),
    confirms,
    allConfirms: confirms.length === APPLY_CONFIRM_FLAGS.length,
    skipBeGates: flags.has("--skip-be-gates"),
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

async function fetchOfficialPage(url) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), 25000);
  try {
    const res = await fetch(url, {
      headers: FETCH_HEADERS,
      redirect: "follow",
      signal: controller.signal,
    });
    const text = await res.text();
    const blocked =
      res.status === 403 ||
      res.status === 429 ||
      /access denied|robot check|captcha|akamai/i.test(text);
    return {
      ok: res.ok && !blocked,
      status: res.status,
      url: res.url || url,
      text,
      blocked,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url,
      text: "",
      blocked: false,
      error: err?.message || String(err),
    };
  } finally {
    clearTimeout(t);
  }
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

function vicAddressClaim(vicRec) {
  if (!vicRec) return null;
  const claim = (vicRec.field_claims || []).find(
    (c) =>
      (c.field === "Address 1" || c.field === "Address" || c.field === "Street Address") &&
      c.value != null &&
      String(c.value).trim()
  );
  if (!claim) return null;
  return {
    address: String(claim.value).trim(),
    source_url: claim.evidence_url || null,
    confidence: claim.confidence || "Medium",
    method: "vic_claim",
  };
}

/**
 * Confirm official address against Census identity geography.
 */
export function confirmOfficialAddress({ address, propertyName, city, state, country }) {
  if (!isStreetLevelAddress(address)) {
    return { ok: false, reason: "not_street_level" };
  }
  const a = norm(address);
  const n = norm(propertyName);
  const c = norm(city);
  const s = norm(state);
  const co = norm(country);

  // Address must not be brand-only / city-only
  if (n && a === n) return { ok: false, reason: "address_equals_property_name" };
  if (c && a === c) return { ok: false, reason: "address_equals_city" };
  if (co && a === co) return { ok: false, reason: "address_equals_country" };

  const failures = [];
  // City should appear in address OR be provided separately for geocode query
  // Soft: if city blank, still ok
  if (c && c.length >= 4 && !a.includes(c)) {
    // still allow — geocode query will append city; flag soft
    failures.push("city_not_in_address_text");
  }
  if (s && s.length >= 4 && !a.includes(s)) {
    failures.push("state_not_in_address_text");
  }
  if (co && !a.includes(co) && co !== "mexico") {
    failures.push("country_not_in_address_text");
  }

  return {
    ok: true,
    confidence: failures.length === 0 ? "High" : "Medium",
    soft_flags: failures,
    address: String(address).trim(),
  };
}

function radarPatchForCoords(confidence, sourceType) {
  return {
    [MAP_FIRST_PASS.radarGeographyStatus]: "Coordinates Available",
    [MAP_FIRST_PASS.radarDisplayStatus]: "Public Map Eligible",
    [MAP_FIRST_PASS.radarDisplayReason]:
      sourceType === "official_coordinates"
        ? `Property-level coordinates from official page (${confidence})`
        : `Property-level coordinates from official-address geocode (${confidence})`,
    [MAP_FIRST_PASS.publicCensusEligibility]: "Eligible",
    [MAP_FIRST_PASS.publicDisplayConfidence]: confidence,
    [MAP_FIRST_PASS.publicDisplayReviewStatus]:
      confidence === "High" ? "Auto-Classified" : "Needs Review",
    [MAP_FIRST_PASS.lastReviewed]: todayIsoDate(),
  };
}

/**
 * High-confidence Address-only patch when geocode is deferred/unavailable.
 * Does not invent addresses; requires confirmed street-level official address.
 * Autopilot would-writes stay High-only — Medium stays out.
 */
export function buildAddressOnlyProposal({ fields, confirmed, addressCandidate, sourceUrl }) {
  if (!confirmed?.ok || !confirmed.address) return null;
  if (!isBlank(fields?.[MAP_FIRST_PASS.address])) return null;

  const claimConf = String(addressCandidate?.confidence || "");
  const method = addressCandidate?.method || "official_address";
  // VIC / census / page: require High source signal (do not promote Medium claims)
  if (claimConf && claimConf !== "High" && method === "vic_claim") return null;
  if (method === "census_address_field" && claimConf === "Medium") return null;

  // Soft geography flags (city/state not substring of address) do not invalidate a High VIC
  // street address when City/State are separate Census fields — used for geocode query quality only.
  const softOnly = (confirmed.soft_flags || []).every((f) =>
    ["city_not_in_address_text", "state_not_in_address_text", "country_not_in_address_text"].includes(
      f
    )
  );
  const confirmOk =
    confirmed.confidence === "High" ||
    (confirmed.confidence === "Medium" &&
      claimConf === "High" &&
      method === "vic_claim" &&
      softOnly);
  if (!confirmOk) return null;

  const evidence = addressCandidate?.source_url || sourceUrl || null;
  if (method === "vic_claim" && !evidence) return null;

  /** @type {Record<string, unknown>} */
  const patch = {
    Address: confirmed.address,
  };
  if (MAP_FIRST_PASS.addressConfidence) {
    patch[MAP_FIRST_PASS.addressConfidence] = "High";
  }
  if (evidence && MAP_FIRST_PASS.addressSourceUrl) {
    patch[MAP_FIRST_PASS.addressSourceUrl] = evidence;
  }
  patch[MAP_FIRST_PASS.lastReviewed] = todayIsoDate();

  return {
    patch,
    method: `address_only:${method}`,
  };
}

/**
 * Resolve one active-brand record missing coordinates (address-first).
 */
export async function resolveAddressFirstRecord(record, ctx) {
  const fields = record.fields || {};
  const key = fields[MAP_FIRST_PASS.identityKey];
  const held = fields[MAP_FIRST_PASS.humanReview] === true;
  const brandMap = mapCensusBrand(fields, ctx.universe);
  const affiliation = String(fields[MAP_FIRST_PASS.affiliationStatus] || "");
  const brandUnconfirmed = affiliation === "Brand-Unconfirmed";
  const family = familyFromRecord(fields, key);
  const sourceUrl =
    fields[MAP_FIRST_PASS.officialUrl] || fields[MAP_FIRST_PASS.sourceUrl] || null;
  const name = fields[MAP_FIRST_PASS.propertyName];
  const city = fields[MAP_FIRST_PASS.city];
  const state = fields[MAP_FIRST_PASS.stateRegion];
  const country = fields[MAP_FIRST_PASS.country] || "Mexico";

  const base = {
    record_id: record.id,
    identity_key: key,
    property_name: name,
    brand: fields[MAP_FIRST_PASS.currentBrand],
    family,
    source_url: sourceUrl,
    brand_mapping: brandMap,
  };

  if (held) {
    return {
      ...base,
      action: "blocked",
      blocked_reason: "human_review_required",
      coordinate_source_type: "blocked_no_official_address",
      proposal: null,
    };
  }
  if (brandUnconfirmed) {
    return {
      ...base,
      action: "blocked",
      blocked_reason: "brand_unconfirmed",
      coordinate_source_type: "blocked_no_official_address",
      proposal: null,
    };
  }
  if (!brandMap.active) {
    return {
      ...base,
      action: "blocked",
      blocked_reason: "not_in_active_universe",
      coordinate_source_type: "blocked_no_official_address",
      proposal: null,
    };
  }
  if (
    isValidCoordPair(
      Number(fields[MAP_FIRST_PASS.latitude]),
      Number(fields[MAP_FIRST_PASS.longitude])
    ) &&
    !isBlank(fields[MAP_FIRST_PASS.address])
  ) {
    return {
      ...base,
      action: "already_has_valid_coordinates",
      proposal: null,
    };
  }
  // Coords present but Address blank → continue into address-only High path below
  const coordsAlreadyPresent = isValidCoordPair(
    Number(fields[MAP_FIRST_PASS.latitude]),
    Number(fields[MAP_FIRST_PASS.longitude])
  );
  if (ctx.families?.length && !ctx.families.includes(family)) {
    return {
      ...base,
      action: "skipped_family_filter",
      blocked_reason: `family_not_in_${ctx.families.join("|")}`,
      proposal: null,
    };
  }

  // --- Address candidates (Census → VIC → page) ---
  let addressCandidate = null;
  const censusAddr = String(fields[MAP_FIRST_PASS.address] || "").trim();
  if (isStreetLevelAddress(censusAddr)) {
    addressCandidate = {
      address: censusAddr,
      source_url: sourceUrl,
      method: "census_address_field",
      confidence: "Medium",
    };
  }

  const vic = key ? ctx.vic.byId.get(key) : null;
  if (!addressCandidate) {
    const vac = vicAddressClaim(vic);
    if (vac && isStreetLevelAddress(vac.address)) {
      addressCandidate = vac;
    }
  }

  // Family directory adapters (Hilton locations / Choice regional cards) before property URL fetch
  if (!addressCandidate && ctx.useFamilyDirectoryAdapters !== false) {
    try {
      const dirAddr = await resolveDirectoryAddressCandidate({
        fields,
        identityKey: key,
        family,
      });
      if (dirAddr.ok && isStreetLevelAddress(dirAddr.address)) {
        addressCandidate = {
          address: dirAddr.address,
          source_url: dirAddr.source_url,
          confidence: dirAddr.confidence || "High",
          method: dirAddr.method,
        };
      }
    } catch (err) {
      // Directory miss is non-fatal — fall through to page fetch / VIC
      if (ctx.log) {
        ctx.log(
          `[address] directory adapter miss ${key}: ${err?.message || err}`
        );
      }
    }
  }

  // Prefer VIC lat/lng if present with evidence — skip when coords already on Census
  // (address-only fill should not re-propose coordinates).
  if (vic && !coordsAlreadyPresent) {
    const latC = (vic.field_claims || []).find((c) => c.field === "Latitude" && c.value != null);
    const lngC = (vic.field_claims || []).find((c) => c.field === "Longitude" && c.value != null);
    if (latC && lngC && latC.evidence_url) {
      const lat = Number(latC.value);
      const lng = Number(lngC.value);
      if (
        isValidCoordPair(lat, lng) &&
        !matchesRejectedPin(lat, lng, { propertyName: name })
      ) {
        const conf = String(latC.confidence || "Medium");
        if (conf === "High" || conf === "Medium") {
          return {
            ...base,
            action: "propose",
            official_address_found: Boolean(addressCandidate),
            official_address: addressCandidate?.address || null,
            address_source_url: addressCandidate?.source_url || latC.evidence_url,
            coordinate_source_type: "official_coordinates",
            confidence: conf,
            proposal: {
              Latitude: lat,
              Longitude: lng,
              ...(addressCandidate && isBlank(fields[MAP_FIRST_PASS.address])
                ? { Address: addressCandidate.address }
                : {}),
              ...radarPatchForCoords(conf, "official_coordinates"),
            },
            report_meta: {
              geocode_provider: "n/a",
              geocode_method: `vic_claim:${latC.source || "directory"}`,
              extraction_method: "vic_official_coordinate_claim",
            },
            page_fetched: false,
            geocoded: false,
          };
        }
      }
    }
  }

  let pageFetched = false;
  let pageExtract = null;
  if (ctx.doFetch && sourceUrl) {
    const page = await fetchOfficialPage(sourceUrl);
    pageFetched = true;
    if (page.ok) {
      // Deep page signals when property HTML is fetchable
      const deep = applyDeepOfficialPageSignals(page.text, page.url);
      pageExtract = extractCoordinatesFromOfficialHtml(page.text, {
        url: page.url,
        family,
      });
      if (
        deep.latitude != null &&
        deep.longitude != null &&
        isValidCoordPair(deep.latitude, deep.longitude) &&
        !(pageExtract.hits || []).length
      ) {
        pageExtract.hits = pageExtract.hits || [];
        pageExtract.hits.push({
          lat: deep.latitude,
          lng: deep.longitude,
          confidence: "High",
          method: "deep_official_page_signals",
          address: null,
        });
      }
      const best = selectBestCoordinateHit(pageExtract.hits);
      if (best) {
        const conf = best.confidence === "High" ? "High" : "Medium";
        const pageAddr =
          best.address ||
          pageExtract.addresses?.[0]?.address ||
          addressCandidate?.address ||
          null;
        const addrConfirm = pageAddr
          ? confirmOfficialAddress({
              address: pageAddr,
              propertyName: name,
              city,
              state,
              country,
            })
          : { ok: false };
        return {
          ...base,
          action: "propose",
          official_address_found: Boolean(addrConfirm.ok || addressCandidate),
          official_address: addrConfirm.ok ? addrConfirm.address : addressCandidate?.address || null,
          address_source_url: sourceUrl,
          coordinate_source_type: "official_coordinates",
          confidence: conf,
          proposal: {
            Latitude: best.lat,
            Longitude: best.lng,
            ...(addrConfirm.ok && isBlank(fields[MAP_FIRST_PASS.address])
              ? { Address: addrConfirm.address }
              : {}),
            ...radarPatchForCoords(conf, "official_coordinates"),
          },
          report_meta: {
            geocode_provider: "n/a",
            geocode_method: best.method,
            extraction_method: best.method,
          },
          page_fetched: true,
          geocoded: false,
        };
      }
      if (!addressCandidate && pageExtract.addresses?.[0]?.address) {
        addressCandidate = {
          address: pageExtract.addresses[0].address,
          source_url: sourceUrl,
          method: pageExtract.addresses[0].method || "page_schema_address",
          confidence: pageExtract.addresses[0].confidence || "High",
        };
      }
    } else if (!addressCandidate) {
      return {
        ...base,
        action: "blocked",
        blocked_reason: page.blocked ? "official_page_blocked" : `fetch_failed_${page.status || "err"}`,
        coordinate_source_type: "blocked_no_official_address",
        proposal: null,
        page_fetched: true,
        geocoded: false,
      };
    }
  } else if (!addressCandidate && sourceUrl && !ctx.doFetch) {
    return {
      ...base,
      action: "deferred_needs_fetch",
      blocked_reason: "fetch_budget_deferred",
      coordinate_source_type: "blocked_no_official_address",
      proposal: null,
      page_fetched: false,
      geocoded: false,
    };
  }

  if (!addressCandidate) {
    return {
      ...base,
      action: "blocked",
      blocked_reason: "blocked_no_official_address",
      coordinate_source_type: "blocked_no_official_address",
      proposal: null,
      page_fetched: pageFetched,
      geocoded: false,
    };
  }

  const confirmed = confirmOfficialAddress({
    address: addressCandidate.address,
    propertyName: name,
    city,
    state,
    country,
  });
  if (!confirmed.ok) {
    return {
      ...base,
      action: "blocked",
      blocked_reason: `address_confirm_failed_${confirmed.reason}`,
      coordinate_source_type: "blocked_low_confidence",
      official_address_found: false,
      proposal: null,
      page_fetched: pageFetched,
      geocoded: false,
    };
  }

  // Geocode path — still allow High address-only writes when Address is blank
  if (!ctx.doGeocode) {
    const addressOnly = buildAddressOnlyProposal({
      fields,
      confirmed,
      addressCandidate,
      sourceUrl,
    });
    if (addressOnly) {
      return {
        ...base,
        action: "propose",
        official_address_found: true,
        official_address: confirmed.address,
        address_source_url: addressCandidate.source_url || sourceUrl,
        coordinate_source_type: "address_only_geocode_deferred",
        confidence: "High",
        proposal: addressOnly.patch,
        report_meta: {
          geocode_provider: "n/a",
          geocode_method: "deferred",
          extraction_method: addressOnly.method,
        },
        page_fetched: pageFetched,
        geocoded: false,
      };
    }
    return {
      ...base,
      action: "deferred_needs_geocode",
      blocked_reason: "geocode_budget_deferred",
      official_address_found: true,
      official_address: confirmed.address,
      address_source_url: addressCandidate.source_url || sourceUrl,
      coordinate_source_type: "blocked_no_official_address",
      proposal: null,
      page_fetched: pageFetched,
      geocoded: false,
    };
  }

  if (ctx.providerInfo.provider === "none") {
    const addressOnly = buildAddressOnlyProposal({
      fields,
      confirmed,
      addressCandidate,
      sourceUrl,
    });
    if (addressOnly) {
      return {
        ...base,
        action: "propose",
        official_address_found: true,
        official_address: confirmed.address,
        address_source_url: addressCandidate.source_url || sourceUrl,
        coordinate_source_type: "address_only_provider_none",
        confidence: "High",
        proposal: addressOnly.patch,
        report_meta: {
          geocode_provider: "none",
          geocode_method: "n/a",
          extraction_method: addressOnly.method,
        },
        page_fetched: pageFetched,
        geocoded: false,
      };
    }
    return {
      ...base,
      action: "blocked",
      blocked_reason: "geocoding_provider_none",
      official_address_found: true,
      official_address: confirmed.address,
      address_source_url: addressCandidate.source_url || sourceUrl,
      coordinate_source_type: "blocked_no_official_address",
      proposal: null,
      page_fetched: pageFetched,
      geocoded: false,
    };
  }

  const geo = await geocodeOfficialAddress(
    {
      name,
      address: confirmed.address,
      city,
      state,
      country,
    },
    ctx.providerInfo
  );

  if (!geo.ok) {
    return {
      ...base,
      action: "blocked",
      blocked_reason: `geocode_failed_${geo.reason}`,
      official_address_found: true,
      official_address: confirmed.address,
      address_source_url: addressCandidate.source_url || sourceUrl,
      coordinate_source_type: "blocked_low_confidence",
      confidence: "Low",
      geocode_detail: {
        reason: geo.reason,
        failures: geo.failures || null,
        formatted_address: geo.formatted_address || geo.place_name || null,
      },
      proposal: null,
      page_fetched: pageFetched,
      geocoded: true,
    };
  }

  if (geo.confidence !== "High" && geo.confidence !== "Medium") {
    return {
      ...base,
      action: "blocked",
      blocked_reason: "blocked_low_confidence",
      official_address_found: true,
      official_address: confirmed.address,
      address_source_url: addressCandidate.source_url || sourceUrl,
      coordinate_source_type: "blocked_low_confidence",
      confidence: "Low",
      proposal: null,
      page_fetched: pageFetched,
      geocoded: true,
    };
  }

  // Mapbox temporary / Google without terms → still propose in dry-run but flag
  const storageOk =
    (ctx.providerInfo.provider === "mapbox" && ctx.providerInfo.permanent_storage_enabled) ||
    (ctx.providerInfo.provider === "google" && ctx.providerInfo.storage_terms_reviewed);

  return {
    ...base,
    action: "propose",
    official_address_found: true,
    official_address: confirmed.address,
    address_source_url: addressCandidate.source_url || sourceUrl,
    coordinate_source_type: "official_address_geocode",
    confidence: geo.confidence,
    storage_terms_ok_for_apply: storageOk,
    proposal: {
      Latitude: geo.lat,
      Longitude: geo.lng,
      ...(isBlank(fields[MAP_FIRST_PASS.address]) ? { Address: confirmed.address } : {}),
      ...radarPatchForCoords(geo.confidence, "official_address_geocode"),
    },
    report_meta: {
      geocode_provider: geo.provider,
      geocode_method: geo.method,
      geocode_query: geo.query,
      formatted_address: geo.formatted_address,
      location_type: geo.location_type || null,
      relevance: geo.relevance ?? null,
    },
    page_fetched: pageFetched,
    geocoded: true,
  };
}

export async function runAddressGeocodeDryRun(args = parseAddressGeocodeArgs()) {
  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token) throw new Error("AIRTABLE_PAT missing");
  if (!bases?.target_base_id) throw new Error("AIRTABLE_BASE_ID_ALT missing");

  const providerInfo = resolveGeocodingProvider(args.providerOverride);
  const termsWarnings = geocodingTermsWarnings(providerInfo);

  console.log("[address-geocode] loading active brand universe + VIC claim index…");
  const universe = loadActiveBrandUniverse();
  const vic = loadVicClaimIndex();

  console.log("[address-geocode] listing Hotel Property Census…");
  const censusRows = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    MAP_FIRST_PASS.propertyName,
    MAP_FIRST_PASS.identityKey,
    MAP_FIRST_PASS.latitude,
    MAP_FIRST_PASS.longitude,
    MAP_FIRST_PASS.city,
    MAP_FIRST_PASS.stateRegion,
    MAP_FIRST_PASS.country,
    MAP_FIRST_PASS.address,
    MAP_FIRST_PASS.currentBrand,
    MAP_FIRST_PASS.brandSlug,
    MAP_FIRST_PASS.affiliationStatus,
    MAP_FIRST_PASS.family,
    MAP_FIRST_PASS.sourceUrl,
    MAP_FIRST_PASS.officialUrl,
    MAP_FIRST_PASS.humanReview,
    MAP_FIRST_PASS.radarDisplayStatus,
    MAP_FIRST_PASS.radarGeographyStatus,
    MAP_FIRST_PASS.publicCensusEligibility,
    MAP_FIRST_PASS.dataEligible,
  ]);

  // Schema probe for supporting fields (list table metadata via first record keys only —
  // supporting fields absent from MAP and from known contract).
  const schemaSupportingPresent = {
    "Address Confidence": false,
    "Address Source URL": false,
    "Coordinate Source Type": false,
    "Coordinate Confidence": false,
    "Geocode Provider": false,
    "Geocode Method": false,
    "Geocode Reviewed Date": false,
  };

  const missing = [];
  const addressOnlyQueue = [];
  const already = [];
  const blockedPre = [];
  let censusStreetAddr = 0;
  let vicStreetAddr = 0;

  for (const row of censusRows) {
    const fields = row.fields || {};
    const brandMap = mapCensusBrand(fields, universe);
    const hasCoords = isValidCoordPair(
      Number(fields[MAP_FIRST_PASS.latitude]),
      Number(fields[MAP_FIRST_PASS.longitude])
    );
    const blankAddress = isBlank(fields[MAP_FIRST_PASS.address]);

    if (fields[MAP_FIRST_PASS.humanReview] === true) {
      blockedPre.push({ id: row.id, reason: "human_review_required" });
      continue;
    }
    if (fields[MAP_FIRST_PASS.affiliationStatus] === "Brand-Unconfirmed") {
      blockedPre.push({ id: row.id, reason: "brand_unconfirmed" });
      continue;
    }
    if (!brandMap.active) {
      blockedPre.push({ id: row.id, reason: "not_in_active_universe" });
      continue;
    }

    // Already has coords AND address → skip
    if (hasCoords && !blankAddress) {
      already.push(row);
      continue;
    }

    // Has coords but blank Address → address-only path (no geocode)
    if (hasCoords && blankAddress) {
      addressOnlyQueue.push(row);
      const vac = vicAddressClaim(vic.byId.get(fields[MAP_FIRST_PASS.identityKey]));
      if (vac && isStreetLevelAddress(vac.address)) vicStreetAddr += 1;
      continue;
    }

    // Missing coords → classic address-first → geocode path
    missing.push(row);
    if (isStreetLevelAddress(fields[MAP_FIRST_PASS.address])) censusStreetAddr += 1;
    const vac = vicAddressClaim(vic.byId.get(fields[MAP_FIRST_PASS.identityKey]));
    if (vac && isStreetLevelAddress(vac.address)) vicStreetAddr += 1;
  }

  const prioritized = [...addressOnlyQueue, ...missing]
    .map((r) => ({
      r,
      family: familyFromRecord(r.fields || {}, r.fields?.[MAP_FIRST_PASS.identityKey]),
      hasStreet:
        isStreetLevelAddress(r.fields?.[MAP_FIRST_PASS.address]) ||
        (() => {
          const vac = vicAddressClaim(vic.byId.get(r.fields?.[MAP_FIRST_PASS.identityKey]));
          return Boolean(vac && isStreetLevelAddress(vac.address));
        })(),
      address_only:
        isValidCoordPair(
          Number(r.fields?.[MAP_FIRST_PASS.latitude]),
          Number(r.fields?.[MAP_FIRST_PASS.longitude])
        ) && isBlank(r.fields?.[MAP_FIRST_PASS.address]),
    }))
    .sort((a, b) => {
      // Prefer address-only fills (coords already present) — highest safe yield
      if (a.address_only !== b.address_only) return a.address_only ? -1 : 1;
      if (a.hasStreet !== b.hasStreet) return a.hasStreet ? -1 : 1;
      const famRank = { IHG: 0, Choice: 1, Hilton: 2, Marriott: 3, Other: 4 };
      const fr = (famRank[a.family] ?? 9) - (famRank[b.family] ?? 9);
      if (fr !== 0) return fr;
      if (args.families?.length) {
        const rank = (f) => (args.families.includes(f) ? 0 : 1);
        return rank(a.family) - rank(b.family);
      }
      return 0;
    });

  const results = [];
  let fetchUsed = 0;
  let geocodeUsed = 0;
  let processed = 0;
  const progressEvery = args.forAutopilot ? 25 : 50;
  // Autopilot/mission: cap heavy resolve work so address phase cannot hang the mission.
  // Street-level + address-only rows stay eligible; fetch budget caps page pulls.
  const maxHeavyResolves = args.forAutopilot
    ? Math.min(
        prioritized.length,
        Math.max(args.fetchLimit * 2, addressOnlyQueue.length + args.fetchLimit + 20)
      )
    : prioritized.length;

  for (const { r, hasStreet, address_only } of prioritized) {
    // Fetch only when no census/VIC street address yet
    const needFetch = !hasStreet;
    const doFetch = needFetch && fetchUsed < args.fetchLimit;
    // Never geocode when only filling Address on records that already have coordinates
    const doGeocode = !address_only && geocodeUsed < args.geocodeLimit;

    // Soft skip: no street, no address-only path, fetch+geocode budgets exhausted → defer
    // without directory/page work (prevents O(n) hang across full census).
    if (!hasStreet && !address_only && !doFetch && !doGeocode) {
      results.push({
        record_id: r.id,
        identity_key: r.fields?.[MAP_FIRST_PASS.identityKey],
        property_name: r.fields?.[MAP_FIRST_PASS.propertyName],
        action: "deferred_needs_fetch",
        blocked_reason: "fetch_budget_exhausted",
        proposal: null,
        page_fetched: false,
        geocoded: false,
      });
      continue;
    }

    // Autopilot address-only phase (geocodeLimit=0): skip records that already have Address
    // unless they are address_only fills (coords present, address blank) or need fetch.
    if (
      args.forAutopilot &&
      !doGeocode &&
      hasStreet &&
      !isBlank(r.fields?.[MAP_FIRST_PASS.address]) &&
      !isBlank(r.fields?.[MAP_FIRST_PASS.addressSourceUrl] || r.fields?.["Address Source URL"])
    ) {
      results.push({
        record_id: r.id,
        identity_key: r.fields?.[MAP_FIRST_PASS.identityKey],
        property_name: r.fields?.[MAP_FIRST_PASS.propertyName],
        action: "already_has_address",
        proposal: null,
        page_fetched: false,
        geocoded: false,
      });
      continue;
    }

    if (processed >= maxHeavyResolves && !hasStreet && !address_only) {
      results.push({
        record_id: r.id,
        identity_key: r.fields?.[MAP_FIRST_PASS.identityKey],
        property_name: r.fields?.[MAP_FIRST_PASS.propertyName],
        action: "deferred_needs_fetch",
        blocked_reason: "autopilot_resolve_cap",
        proposal: null,
        page_fetched: false,
        geocoded: false,
      });
      continue;
    }

    processed += 1;
    if (processed === 1 || processed % progressEvery === 0) {
      console.log(
        `[address-geocode] resolve progress ${processed}/${prioritized.length} fetchUsed=${fetchUsed}/${args.fetchLimit} geocodeUsed=${geocodeUsed}/${args.geocodeLimit}`
      );
    }

    const resolveOpts = {
      universe,
      vic,
      families: args.families,
      doFetch,
      doGeocode,
      providerInfo,
      // Prefer preloaded VIC + warm Hilton/Choice directory cache (in-memory after warm).
      useFamilyDirectoryAdapters: true,
    };
    let resolved;
    if (args.forAutopilot) {
      // Hard cap per-record work so one hung page cannot stall the mission.
      const perRecordMs = Math.max(8000, Number(args.perRecordTimeoutMs) || 15000);
      resolved = await Promise.race([
        resolveAddressFirstRecord(r, resolveOpts),
        new Promise((resolve) =>
          setTimeout(
            () =>
              resolve({
                record_id: r.id,
                identity_key: r.fields?.[MAP_FIRST_PASS.identityKey],
                property_name: r.fields?.[MAP_FIRST_PASS.propertyName],
                action: "blocked",
                blocked_reason: "autopilot_per_record_timeout",
                proposal: null,
                page_fetched: false,
                geocoded: false,
              }),
            perRecordMs
          )
        ),
      ]);
    } else {
      resolved = await resolveAddressFirstRecord(r, resolveOpts);
    }
    results.push(resolved);

    if (resolved.page_fetched) {
      fetchUsed += 1;
      if (args.delayMs) await sleep(args.delayMs);
    }
    if (resolved.geocoded) {
      geocodeUsed += 1;
      if (args.delayMs) await sleep(Math.min(args.delayMs, 250));
    }
  }

  const proposed = results.filter((r) => r.action === "propose" && r.proposal);
  const blocked = results.filter((r) => r.action === "blocked");
  const deferred = results.filter(
    (r) => r.action === "deferred_needs_fetch" || r.action === "deferred_needs_geocode"
  );

  const officialAddrFound = results.filter((r) => r.official_address_found).length;
  const officialCoordsFound = proposed.filter(
    (r) => r.coordinate_source_type === "official_coordinates"
  ).length;
  const geocodeProposed = proposed.filter(
    (r) => r.coordinate_source_type === "official_address_geocode"
  ).length;

  const confidenceDist = { High: 0, Medium: 0, Low: 0 };
  for (const p of proposed) {
    confidenceDist[p.confidence] = (confidenceDist[p.confidence] || 0) + 1;
  }

  // Post dry-run validation of proposals
  const proposalValidation = {
    zero_zero: 0,
    invalid: 0,
    rejected_pin: 0,
    held: 0,
    brand_unconfirmed: 0,
    low_confidence: 0,
    missing_support: 0,
    pass: true,
    failures: [],
  };
  for (const p of proposed) {
    const addressOnly =
      p.coordinate_source_type === "address_only_geocode_deferred" ||
      p.coordinate_source_type === "address_only_provider_none" ||
      (p.proposal?.Address && p.proposal.Latitude == null && p.proposal.Longitude == null);

    if (!addressOnly) {
      const lat = Number(p.proposal.Latitude);
      const lng = Number(p.proposal.Longitude);
      if (lat === 0 && lng === 0) {
        proposalValidation.zero_zero += 1;
        proposalValidation.failures.push({ id: mask(p.record_id), reason: "zero_zero" });
      }
      if (!isValidCoordPair(lat, lng)) {
        proposalValidation.invalid += 1;
        proposalValidation.failures.push({ id: mask(p.record_id), reason: "invalid" });
      }
      const rej = matchesRejectedPin(lat, lng, { propertyName: p.property_name });
      if (rej) {
        proposalValidation.rejected_pin += 1;
        proposalValidation.failures.push({
          id: mask(p.record_id),
          reason: `rejected_pin:${rej.label}`,
        });
      }
    } else if (!p.proposal?.Address) {
      proposalValidation.missing_support += 1;
      proposalValidation.failures.push({ id: mask(p.record_id), reason: "address_only_missing_address" });
    }
    if (p.confidence === "Low") {
      proposalValidation.low_confidence += 1;
      proposalValidation.failures.push({ id: mask(p.record_id), reason: "low_confidence" });
    }
    if (
      p.coordinate_source_type === "official_address_geocode" &&
      !p.official_address
    ) {
      proposalValidation.missing_support += 1;
      proposalValidation.failures.push({ id: mask(p.record_id), reason: "geocode_without_address" });
    }
    if (
      p.coordinate_source_type === "official_coordinates" &&
      !p.report_meta?.extraction_method &&
      !p.address_source_url
    ) {
      proposalValidation.missing_support += 1;
    }
  }
  proposalValidation.pass =
    proposalValidation.zero_zero === 0 &&
    proposalValidation.invalid === 0 &&
    proposalValidation.rejected_pin === 0 &&
    proposalValidation.low_confidence === 0 &&
    proposalValidation.held === 0 &&
    proposalValidation.brand_unconfirmed === 0;

  const cost = estimateGeocodeCostUsd(geocodeUsed, providerInfo);

  const streetCoverageAmongMissing = censusStreetAddr + vicStreetAddr;
  const termsBlockApply =
    providerInfo.provider === "google" && !providerInfo.storage_terms_reviewed
      ? true
      : providerInfo.provider === "mapbox" && !providerInfo.permanent_storage_enabled
        ? true
        : providerInfo.provider === "none";

  let status = STATUS.READY;
  if (providerInfo.provider === "none" || !providerInfo.credentials_ok || termsBlockApply) {
    status = STATUS.NEEDS_PROVIDER_OR_TERMS;
  } else if (
    missing.length > 50 &&
    streetCoverageAmongMissing < 10 &&
    proposed.length === 0
  ) {
    status = STATUS.BLOCKED_SOURCE_QUALITY;
  } else if (!proposalValidation.pass) {
    status = STATUS.BLOCKED_SOURCE_QUALITY;
  }
  // Schema v113 recommended but not blocking dry-run review when proposals exist
  const schemaV113Recommended = Object.values(schemaSupportingPresent).every((v) => !v);

  return {
    version: RESOLVER_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry-run",
    apply_executed: false,
    status,
    base_id_masked: mask(bases.target_base_id),
    geocoding: {
      provider: providerInfo.provider,
      resolve_reason: providerInfo.reason,
      credentials_ok: providerInfo.credentials_ok,
      permanent_storage_enabled: providerInfo.permanent_storage_enabled ?? null,
      storage_terms_reviewed: providerInfo.storage_terms_reviewed ?? null,
      terms_warnings: termsWarnings,
      terms_block_apply: termsBlockApply,
    },
    schema: {
      supporting_fields_present: schemaSupportingPresent,
      v113_recommended: schemaV113Recommended,
      recommended_fields: RECOMMENDED_V113_FIELDS,
      note: "Capture provider/method/confidence in dry-run report until v1.1.3 schema is approved. Do not create fields in this task.",
    },
    args: {
      fetch_limit: args.fetchLimit,
      geocode_limit: args.geocodeLimit,
      delay_ms: args.delayMs,
      families: args.families,
      provider_override: args.providerOverride,
    },
    summary: {
      total_records_scanned: censusRows.length,
      records_with_valid_coordinates: already.length,
      active_brand_missing_coordinates: missing.length,
      blocked_prefilter: blockedPre.length,
      records_with_official_address_found: officialAddrFound,
      census_street_address_among_missing: censusStreetAddr,
      vic_street_address_among_missing: vicStreetAddr,
      records_with_official_coordinates_found: officialCoordsFound,
      records_sent_to_geocoder: geocodeUsed,
      records_proposed_for_coordinate_update: proposed.length,
      geocode_proposed: geocodeProposed,
      records_blocked: blocked.length,
      deferred: deferred.length,
      pages_fetched: fetchUsed,
      exact_airtable_update_count_if_applied: proposed.length,
      confidence_distribution: confidenceDist,
      geocoding_cost_estimate: cost,
    },
    proposal_validation: proposalValidation,
    proposed_updates: proposed.map((p) => ({
      action: "propose",
      record_id: args.forAutopilot ? p.record_id : mask(p.record_id),
      identity_key: p.identity_key,
      property_name: p.property_name,
      brand: p.brand,
      family: p.family,
      latitude: p.proposal.Latitude ?? null,
      longitude: p.proposal.Longitude ?? null,
      official_address: p.official_address || null,
      address_source_url: p.address_source_url || p.source_url,
      coordinate_source_type: p.coordinate_source_type,
      confidence: p.confidence,
      geocode_provider: p.report_meta?.geocode_provider || null,
      geocode_method: p.report_meta?.geocode_method || null,
      storage_terms_ok_for_apply: p.storage_terms_ok_for_apply ?? null,
      fields_if_applied: Object.keys(p.proposal),
      proposal: p.proposal,
      report_meta: p.report_meta || null,
    })),
    blocked_sample: blocked.slice(0, 40).map((b) => ({
      record_id: mask(b.record_id),
      identity_key: b.identity_key,
      property_name: b.property_name,
      family: b.family,
      blocked_reason: b.blocked_reason,
      coordinate_source_type: b.coordinate_source_type,
      official_address: b.official_address || null,
    })),
    deferred_sample: deferred.slice(0, 20).map((d) => ({
      record_id: mask(d.record_id),
      reason: d.blocked_reason,
      action: d.action,
    })),
    fields_proposed_for_future_write: [...ALLOWED_FUTURE_WRITE_FIELDS],
    fields_not_touched: [
      ...FORBIDDEN_WRITE_FIELDS,
      "Brand Explorer fields",
      "(no Airtable writes in this dry-run)",
      ...RECOMMENDED_V113_FIELDS.map((f) => `${f.name} (not in schema yet)`),
    ],
    future_apply_command: [
      "npm run research-engine-v2:production-census-address-geocode-resolver -- --apply \\",
      ...APPLY_CONFIRM_FLAGS.map((f, i) =>
        i === APPLY_CONFIRM_FLAGS.length - 1 ? `  ${f}` : `  ${f} \\`
      ),
    ].join("\n"),
    webhound: {
      used: false,
      restarted: false,
      production_writes: 0,
      note: "Webhound sidecar remains closed; not used in this lane.",
    },
    brand_explorer_safety: {
      touched: false,
      writes: 0,
      gates_run: false,
      note: "Gates run by CLI after dry-run.",
    },
    next_step: null, // filled below
  };
}

export function finalizeNextStep(report) {
  if (report.status === STATUS.NEEDS_PROVIDER_OR_TERMS) {
    report.next_step =
      "Founder decision: (1) enable Mapbox permanent geocoding (MAPBOX_ACCESS_TOKEN + MAPBOX_PERMANENT_GEOCODING=1), or (2) confirm Google storage terms (GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED=1) if Google remains the provider. Then re-run dry-run. Do not apply until terms are confirmed. Optional: approve schema v1.1.3 provenance fields.";
  } else if (report.status === STATUS.BLOCKED_SOURCE_QUALITY) {
    report.next_step =
      "Improve official address coverage (Census Address / VIC / page extract) before geocoding. Raise --fetch-limit for address extraction on records without street addresses. Do not restart Webhound for full Census.";
  } else if (report.status === STATUS.NEEDS_SCHEMA_V113) {
    report.next_step =
      "Approve and add schema v1.1.3 supporting provenance fields, then re-run dry-run / apply.";
  } else {
    report.next_step =
      "Founder review proposed coordinate updates in dry-run report. If approved and geocoding storage terms confirmed, run the apply command with all confirm flags. Optionally add schema v1.1.3 provenance fields first.";
  }
  return report;
}

export function renderAddressGeocodeMarkdown(report) {
  const s = report.summary || {};
  const g = report.geocoding || {};
  return `# Production Census Address-First Geocode Resolver — Dry Run

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Apply executed:** false  
**Geocoding provider:** \`${g.provider}\` (${g.resolve_reason})

## 1. Executive summary

| Metric | Value |
| --- | ---: |
| Scanned | ${s.total_records_scanned} |
| Already valid coordinates | ${s.records_with_valid_coordinates} |
| Active missing coordinates | ${s.active_brand_missing_coordinates} |
| Official address found | ${s.records_with_official_address_found} |
| Official coordinates found | ${s.records_with_official_coordinates_found} |
| Sent to geocoder | ${s.records_sent_to_geocoder} |
| Proposed updates | ${s.records_proposed_for_coordinate_update} |
| Blocked | ${s.records_blocked} |
| Exact Airtable updates if applied | ${s.exact_airtable_update_count_if_applied} |
| Est. geocode API cost (USD) | ${s.geocoding_cost_estimate?.estimated_usd ?? "—"} |
| Webhound production writes | 0 |

## 2. Address-first resolution method

1. Existing Census Source URL / Official Property URL  
2. Official brand/property directory or hotel page (fetch when address missing)  
3. Structured page data (JSON-LD / schema.org / map payload / address block)  
4. Confirm official address (street-level; property/city/state/country checks)  
5. If official coordinates found → propose as \`official_coordinates\`  
6. Else geocode **property name + official street address only** via \`GEOCODING_PROVIDER\`  
7. High/Medium only; Low → blocked  

## 3. Geocoding provider strategy

\`\`\`json
${JSON.stringify(g, null, 2)}
\`\`\`

## 4. Terms / storage warning

${(g.terms_warnings || []).map((w) => `- ${w}`).join("\n")}

## 5. Official address coverage

- Census street-level among active missing: **${s.census_street_address_among_missing}**
- VIC street-level among active missing: **${s.vic_street_address_among_missing}**
- Official address found during resolution: **${s.records_with_official_address_found}**

## 6. Official coordinate coverage

- Official coordinates proposed: **${s.records_with_official_coordinates_found}**
- Official-address geocode proposed: **${s.geocode_proposed}**

## 7. Proposed coordinate updates (sample)

\`\`\`json
${JSON.stringify(report.proposed_updates?.slice(0, 40), null, 2)}
\`\`\`

## 8. Blocked records (sample)

\`\`\`json
${JSON.stringify(report.blocked_sample, null, 2)}
\`\`\`

## 9. Confidence distribution

\`\`\`json
${JSON.stringify(s.confidence_distribution, null, 2)}
\`\`\`

## 10. Estimated API cost

\`\`\`json
${JSON.stringify(s.geocoding_cost_estimate, null, 2)}
\`\`\`

## 11. Fields proposed for future write

${(report.fields_proposed_for_future_write || []).map((f) => `- ${f}`).join("\n")}

## 12. Fields not touched

${(report.fields_not_touched || []).map((f) => `- ${f}`).join("\n")}

## 13. Brand Explorer safety

\`\`\`json
${JSON.stringify(report.brand_explorer_safety, null, 2)}
\`\`\`

## 14. Schema v1.1.3 recommendation

\`\`\`json
${JSON.stringify(report.schema, null, 2)}
\`\`\`

## 15. Recommended next step

${report.next_step}

## Proposal validation

\`\`\`json
${JSON.stringify(report.proposal_validation, null, 2)}
\`\`\`

## Future apply (do not run until founder approval)

\`\`\`bash
${report.future_apply_command}
\`\`\`
`;
}
