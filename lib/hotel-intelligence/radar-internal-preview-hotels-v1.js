/**
 * Radar HPC internal-preview inclusion (dynamic Radar Status query).
 *
 * Hotel Property Census (HPC) hotels with Radar Status Open|Pipeline are
 * eligible for authorized internal Radar after a normal page refresh, subject
 * to the short in-process cache below. Hidden/blank are excluded.
 *
 * Eligibility is formula-driven — not a hotel-specific allowlist.
 * Legacy Hotel Census pins remain authoritative when identity collides;
 * HPC overlays are additive for hotels absent from that pool.
 *
 * Radar Status (HPC single-select): Open | Pipeline | Hidden
 *   - Open / Pipeline → include for authorized Radar (maps to DTO status)
 *   - Hidden / blank → exclude
 *   - Never maps Production Use Status / other governance labels
 *   - Does not grant public/customer access (auth remains independent)
 *
 * Cache policy (documented):
 *   - Default TTL: RADAR_HPC_STATUS_CACHE_TTL_MS_DEFAULT (45s)
 *   - Override: process.env.RADAR_HPC_STATUS_CACHE_TTL_MS
 *   - Scope: in-process memory only; private to this Node process
 *   - A full-page Radar refresh after TTL expiry fetches current HPC status
 *   - Cache never authorizes; auth is re-checked on every request
 *
 * Overview field projections (description, amenities, meeting space, address)
 * are additive display inputs for Hotel Explorer; they do not change eligibility.
 *
 * Security:
 *   - Never trusts `internalPreview=1` alone.
 *   - Caller must prove admin / local Dealality admin via dealalityUser + req.
 *   - Customer/share audiences must not receive these hotels.
 *   - Diagnostics are for authorized callers only (never for share/customer).
 */

import Airtable from "airtable";
import { resolveCanonicalHotelId } from "../hotel-census/adp-gdi-canonical-identity.js";
import { isAdpOwnerAppAdminEmail } from "../dealality/adp-owner-app-admin-emails.js";
import { hasLocalDealalityAdminAccess } from "../dealality/local-dealality-admin-access.js";
import { WORKSPACE_ADMIN } from "../company-workspace-access.js";

export const RADAR_INTERNAL_PREVIEW_HOTELS_VERSION =
  "radar_hpc_dynamic_status_v1";

export const HOTEL_PROPERTY_CENSUS_TABLE =
  process.env.AIRTABLE_HOTEL_PROPERTY_CENSUS_TABLE || "Hotel Property Census";

export const HPC_RADAR_STATUS_FIELD = "Radar Status";

/** Short in-process cache so a normal refresh after expiry sees current status. */
export const RADAR_HPC_STATUS_CACHE_TTL_MS_DEFAULT = 45_000;

export const RADAR_HPC_STATUS_CACHE_POLICY = Object.freeze({
  ttlMsDefault: RADAR_HPC_STATUS_CACHE_TTL_MS_DEFAULT,
  envKey: "RADAR_HPC_STATUS_CACHE_TTL_MS",
  scope: "in_process_memory",
  notes:
    "Authorized admin fetches are cached briefly. Full-page Radar refresh after TTL reflects current HPC Open/Pipeline status. Auth is always re-checked.",
});

const REC_ID_RE = /^rec[A-Za-z0-9]{14}$/;
const RADAR_STATUS_VISIBLE = new Set(["Open", "Pipeline"]);

/**
 * Airtable `.select({ fields })` must list ONLY real Hotel Property Census field
 * names. Alias / DTO fallback keys (name, city, Website, Total Meeting Space, …)
 * belong in formatters — never in this array — or Airtable rejects the whole query.
 *
 * Verified 2026-11-18 against live Meta API for AIRTABLE_BASE_ID_ALT /
 * table tbl9aY5ijiuIzzWam (fixtures/hotel-census/hpc-radar-select-fields-v1.json).
 */
export const HPC_SELECT_FIELDS = Object.freeze([
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Latitude",
  "Longitude",
  "City",
  "Country",
  "Continent",
  "State / Region",
  "Asset Context",
  "Rooms / Keys",
  "Hotel Class / Segment",
  "Market",
  "Market / Submarket",
  "Submarket",
  "Property Type",
  "Operator / Management Company",
  "Official Property URL",
  "Phone",
  "Address",
  "Postal Code",
  "Short Property Summary",
  "Property Positioning",
  "Hotel Description - AI Summary",
  "Hotel Description - Source Text",
  "Amenities - Source Text",
  "Amenities - Structured Tags",
  "Production Use Status",
  HPC_RADAR_STATUS_FIELD,
]);

/** @type {{ expiresAt: number, payload: object } | null} */
let _hpcStatusCache = null;

function readText(fields, keys) {
  for (const key of keys) {
    const raw = fields?.[key];
    if (raw == null || raw === "") continue;
    if (typeof raw === "string") {
      const t = raw.trim();
      if (t) return t;
      continue;
    }
    if (Array.isArray(raw)) {
      const joined = raw
        .map((item) =>
          typeof item === "string" ? item.trim() : item?.name || ""
        )
        .filter(Boolean)
        .join("; ");
      if (joined) return joined;
      continue;
    }
    if (typeof raw === "object" && raw.name) {
      const t = String(raw.name).trim();
      if (t) return t;
    }
  }
  return null;
}

function readNumber(fields, keys) {
  for (const key of keys) {
    const raw = fields?.[key];
    if (raw == null || raw === "") continue;
    const n = Number(raw);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isAirtableRateLimitError(err) {
  const status = err?.statusCode || err?.status || err?.error?.statusCode;
  if (status === 429) return true;
  const msg = String(err?.message || err || "");
  return /429|rate.?limit|too many requests/i.test(msg);
}

export function resolveRadarHpcStatusCacheTtlMs(env = process.env) {
  const raw = env?.RADAR_HPC_STATUS_CACHE_TTL_MS;
  if (raw == null || raw === "") return RADAR_HPC_STATUS_CACHE_TTL_MS_DEFAULT;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) return RADAR_HPC_STATUS_CACHE_TTL_MS_DEFAULT;
  return Math.min(Math.floor(n), 5 * 60_000);
}

export function clearRadarHpcStatusCache() {
  _hpcStatusCache = null;
}

export function buildHpcRadarStatusFilterFormula() {
  return `OR({${HPC_RADAR_STATUS_FIELD}}='Open',{${HPC_RADAR_STATUS_FIELD}}='Pipeline')`;
}

/**
 * Resolve HPC Radar Status → Open | Pipeline | null.
 * Hidden and blank are null (excluded). Never invents values from governance fields.
 */
export function resolveRadarStatusFromHpcFields(fields = {}) {
  const raw = readText(fields, [HPC_RADAR_STATUS_FIELD]);
  if (!raw) return null;
  if (raw === "Open" || raw === "Pipeline") return raw;
  if (raw === "Hidden") return null;
  const lower = String(raw).toLowerCase();
  if (lower === "open") return "Open";
  if (lower === "pipeline") return "Pipeline";
  if (lower === "hidden") return null;
  return null;
}

export function isHpcRadarStatusVisible(fields = {}) {
  return RADAR_STATUS_VISIBLE.has(resolveRadarStatusFromHpcFields(fields) || "");
}

export function normalizeHotelIdentityName(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

export function roundHotelIdentityCoord(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.round(n * 10000) / 10000;
}

/**
 * Canonical identity keys for dedupe across Hotel Census ↔ HPC.
 * Prefer record id / alias canonical id; fall back to name+coords fingerprint.
 */
export function buildHotelIdentityKeys(hotel = {}) {
  const keys = new Set();
  const id = String(hotel?.id || hotel?.recordId || "").trim();
  if (REC_ID_RE.test(id)) {
    keys.add(`id:${id}`);
    try {
      const canonical = resolveCanonicalHotelId(id);
      if (canonical && REC_ID_RE.test(canonical)) {
        keys.add(`canonical:${canonical}`);
      }
    } catch {
      /* alias map optional in unit contexts */
    }
  }
  const name = normalizeHotelIdentityName(hotel?.name);
  const lat = roundHotelIdentityCoord(hotel?.lat);
  const lng = roundHotelIdentityCoord(hotel?.lng);
  if (name && lat != null && lng != null && (lat !== 0 || lng !== 0)) {
    keys.add(`geo:${name}|${lat}|${lng}`);
  }
  return keys;
}

export function hotelHasMapCoordinates(hotel = {}) {
  const lat = Number(hotel?.lat);
  const lng = Number(hotel?.lng);
  return Number.isFinite(lat) && Number.isFinite(lng) && (lat !== 0 || lng !== 0);
}

/**
 * Map a Hotel Property Census record into brand-presence hotel DTO.
 * Caller must already ensure Radar Status is Open/Pipeline when including on map.
 * Overview fields are projected for Hotel Explorer display only.
 */
export function formatHotelPropertyCensusAsBrandPresenceHotel(record) {
  const fields = record?.fields || {};
  const lat = readNumber(fields, ["Latitude", "latitude"]);
  const lng = readNumber(fields, ["Longitude", "longitude"]);
  const hasCoords =
    Number.isFinite(lat) &&
    Number.isFinite(lng) &&
    (lat !== 0 || lng !== 0);
  const rooms = readNumber(fields, ["Rooms / Keys", "rooms", "Rooms"]);
  const productionUseStatus = readText(fields, ["Production Use Status"]);
  const radarStatus = resolveRadarStatusFromHpcFields(fields);
  const hotelDescription = readText(fields, [
    "Short Property Summary",
    "Property Positioning",
    "Hotel Description - AI Summary",
    "Hotel Description - Source Text",
    "Hotel Description",
    "hotelDescription",
  ]);
  const hotelHeadline = readText(fields, [
    "Property Positioning",
    "Hotel Headline",
    "hotelHeadline",
  ]);
  const amenities = readText(fields, [
    "Amenities - Source Text",
    "Amenities - Structured Tags",
    "Amenities",
    "amenities",
  ]);
  const totalMeetingSpace = readNumber(fields, [
    "Total Meeting Space",
    "Total Meeting Space Sq Ft",
    "Meeting Space Sq Ft",
  ]);

  return {
    id: record.id,
    name:
      readText(fields, ["Property Name", "Canonical Property Name", "name"]) ||
      "Unknown Hotel",
    brand:
      readText(fields, ["Current Brand", "Affiliation", "Brand Family"]) ||
      "Unknown Brand",
    parentCompany:
      readText(fields, ["Parent Company", "Brand Family", "Family / Source Family"]) ||
      "Unknown",
    status: radarStatus || "Hidden",
    radarStatus: radarStatus || "Hidden",
    productionUseStatus: productionUseStatus || null,
    lat: hasCoords ? lat : 0,
    lng: hasCoords ? lng : 0,
    city: readText(fields, ["City", "city"]) || "Unknown City",
    country: readText(fields, ["Country", "country"]) || "Unknown Country",
    region: readText(fields, ["Region", "Continent", "State / Region"]) || "Unknown Region",
    locationType: readText(fields, ["Location", "Asset Context"]) || null,
    rooms: Number.isFinite(rooms) ? rooms : 0,
    chainScale: readText(fields, ["Chain Scale", "Hotel Class / Segment"]) || null,
    market: readText(fields, ["Market", "Market / Submarket"]) || null,
    submarket: readText(fields, ["Submarket"]) || null,
    censusPropertyType: readText(fields, ["Property Type"]) || null,
    managementCompany: readText(fields, ["Operator / Management Company", "Management Company"]) || null,
    website: readText(fields, ["Official Property URL", "Website"]) || null,
    telephone: readText(fields, ["Phone", "Telephone"]) || null,
    address1: readText(fields, ["Address", "Address 1", "address1"]) || null,
    state: readText(fields, ["State / Region", "State", "state"]) || null,
    postalCode: readText(fields, ["Postal Code", "postalCode"]) || null,
    hotelDescription: hotelDescription || null,
    hotelHeadline:
      hotelHeadline && hotelHeadline !== hotelDescription ? hotelHeadline : null,
    amenities: amenities || null,
    totalMeetingSpace: Number.isFinite(totalMeetingSpace) ? totalMeetingSpace : null,
    internalPreviewOnly: true,
    censusSourceTable: HOTEL_PROPERTY_CENSUS_TABLE,
    radarInclusion: "AUTHORIZED_INTERNAL_PREVIEW",
  };
}

/**
 * True when the resolved Dealality user may receive internal-preview hotels.
 * Query flags alone never authorize.
 *
 * LOCAL_DEV_AUTH_BYPASS admin (memberstackId=dev_bypass) additionally requires
 * intentional owner-app header so bare unauthenticated curls with only
 * DEV_AUTH_BYPASS_EMAIL set cannot harvest restricted hotels. Production never
 * activates that bypass (NODE_ENV=production).
 */
export function isAuthorizedForRadarInternalPreviewHotels(dealalityUser, req = {}) {
  if (!dealalityUser || typeof dealalityUser !== "object") return false;

  const userSource = String(dealalityUser.userSource || "").trim();
  const isLocalDevBypassUser =
    userSource === "LOCAL_DEV_AUTH_BYPASS" ||
    String(dealalityUser.memberstackId || "").trim() === "dev_bypass";

  if (isLocalDevBypassUser) {
    // DEV bypass identity is synthetic — still require intentional owner-app header
    // so bare unauthenticated curls cannot harvest restricted hotels.
    const ownerApp = String(
      req?.headers?.["x-dealality-owner-app"] ||
        req?.headers?.["X-Dealality-Owner-App"] ||
        ""
    ).trim();
    if (ownerApp !== "1") return false;
  }

  if (dealalityUser.isAdmin === true || dealalityUser.flags?.isAdmin === true) {
    return true;
  }

  const workspaces = Array.isArray(dealalityUser.workspaceAccess)
    ? dealalityUser.workspaceAccess
    : [];
  if (workspaces.includes(WORKSPACE_ADMIN)) return true;

  // Server-side ADP owner-app admin allowlist (verified identity emails only).
  const identityEmails = [
    dealalityUser.email,
    req?.memberstackEmail,
    req?.memberstackAuth?.email,
  ];
  if (!isLocalDevBypassUser && isAdpOwnerAppAdminEmail(identityEmails)) {
    return true;
  }

  if (hasLocalDealalityAdminAccess(dealalityUser, req)) return true;
  return false;
}

/**
 * Reject insecure client-only unlock attempts.
 */
export function assertInternalPreviewQueryDoesNotAuthorize(query = {}) {
  const flag =
    query.internalPreview === "1" ||
    query.internalPreview === "true" ||
    query.radarInternalPreview === "1" ||
    query.includeInternalPreview === "1";
  return {
    queryRequestsInternalPreview: Boolean(flag),
    authorizedByQueryAlone: false,
  };
}

/**
 * @deprecated Eligibility is dynamic via Radar Status. Kept empty for API
 * compatibility — do not use as a hotel allowlist.
 */
export function listInternalPreviewCensusIds() {
  return [];
}

function emptyDiagnostics(overrides = {}) {
  return {
    openCount: 0,
    pipelineCount: 0,
    retrievedCount: 0,
    missingIdentity: [],
    missingCoordinates: [],
    duplicatesReconciled: [],
    apiErrors: [],
    paginationComplete: false,
    pageCount: 0,
    lastSuccessfulFetchAt: null,
    cacheHit: false,
    cacheTtlMs: resolveRadarHpcStatusCacheTtlMs(),
    formula: buildHpcRadarStatusFilterFormula(),
    ...overrides,
  };
}

/**
 * Paginated HPC select for Open|Pipeline with rate-limit retries.
 * @returns {Promise<{ records: object[], diagnostics: object, errors: object[] }>}
 */
export async function fetchHpcRadarStatusEligibleRecords({
  airtableBase,
  pageSize = 100,
  maxRetries = 4,
  selectFields = HPC_SELECT_FIELDS,
} = {}) {
  const diagnostics = emptyDiagnostics();
  const errors = [];
  if (!airtableBase) {
    errors.push({ reason: "missing_airtable_base" });
    diagnostics.apiErrors = errors.slice();
    return { records: [], diagnostics, errors };
  }

  const formula = buildHpcRadarStatusFilterFormula();
  const records = [];
  let lastErr = null;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      records.length = 0;
      diagnostics.pageCount = 0;
      await airtableBase(HOTEL_PROPERTY_CENSUS_TABLE)
        .select({
          filterByFormula: formula,
          pageSize: Math.min(Math.max(Number(pageSize) || 100, 1), 100),
          fields: [...selectFields],
        })
        .eachPage((page, next) => {
          diagnostics.pageCount += 1;
          for (const rec of page || []) records.push(rec);
          next();
        });
      diagnostics.paginationComplete = true;
      diagnostics.lastSuccessfulFetchAt = new Date().toISOString();
      lastErr = null;
      break;
    } catch (err) {
      lastErr = err;
      if (isAirtableRateLimitError(err) && attempt < maxRetries) {
        const backoffMs = Math.min(16_000, 500 * 2 ** attempt);
        await sleep(backoffMs);
        continue;
      }
      const message = String(err?.message || err).slice(0, 240);
      const unknownField = /UNKNOWN_FIELD_NAME|Unknown field|invalid.*(field|param)/i.test(
        message
      );
      errors.push({
        reason: unknownField
          ? "hpc_select_unknown_field"
          : "hpc_formula_fetch_failed",
        message,
        rateLimited: isAirtableRateLimitError(err),
        attempt: attempt + 1,
      });
      diagnostics.paginationComplete = false;
      break;
    }
  }

  if (lastErr && !errors.length) {
    const message = String(lastErr?.message || lastErr).slice(0, 240);
    errors.push({
      reason: /UNKNOWN_FIELD_NAME|Unknown field/i.test(message)
        ? "hpc_select_unknown_field"
        : "hpc_formula_fetch_failed",
      message,
    });
  }

  diagnostics.apiErrors = errors.slice();
  return { records, diagnostics, errors };
}

function classifyFormattedHotels(hotels) {
  const missingIdentity = [];
  const missingCoordinates = [];
  let openCount = 0;
  let pipelineCount = 0;
  const mapEligible = [];

  for (const hotel of hotels) {
    const id = String(hotel?.id || "").trim();
    if (!REC_ID_RE.test(id)) {
      missingIdentity.push({
        id: id || null,
        name: hotel?.name || null,
        reason: "invalid_or_missing_record_id",
      });
      continue;
    }
    if (!hotelHasMapCoordinates(hotel)) {
      missingCoordinates.push({
        id,
        name: hotel?.name || null,
        reason: "missing_or_zero_coordinates",
      });
      // Still returned for diagnostics; excluded from mapEligible below.
    }
    const st = String(hotel.status || hotel.radarStatus || "").trim();
    if (st === "Open") openCount += 1;
    else if (st === "Pipeline") pipelineCount += 1;

    if (hotelHasMapCoordinates(hotel) && (st === "Open" || st === "Pipeline")) {
      mapEligible.push(hotel);
    }
  }

  return {
    openCount,
    pipelineCount,
    missingIdentity,
    missingCoordinates,
    mapEligible,
  };
}

/**
 * Load Open|Pipeline HPC hotels for authorized callers via formula query.
 * Optional `records` injects fixtures (unit tests) — never used for auth bypass.
 *
 * @returns {Promise<object>}
 */
export async function loadAuthorizedInternalPreviewHotels({
  dealalityUser,
  req = {},
  airtableBase = null,
  records = null,
  bypassCache = false,
  pageSize = 100,
} = {}) {
  if (!isAuthorizedForRadarInternalPreviewHotels(dealalityUser, req)) {
    return {
      ok: false,
      unauthorized: true,
      hotels: [],
      errors: [{ reason: "forbidden_admin" }],
      skippedHidden: [],
      diagnostics: emptyDiagnostics({
        apiErrors: [{ reason: "forbidden_admin" }],
      }),
      cachePolicy: RADAR_HPC_STATUS_CACHE_POLICY,
    };
  }

  const ttlMs = resolveRadarHpcStatusCacheTtlMs();
  const now = Date.now();
  if (
    !bypassCache &&
    !Array.isArray(records) &&
    _hpcStatusCache &&
    _hpcStatusCache.expiresAt > now
  ) {
    const cached = _hpcStatusCache.payload;
    return {
      ...cached,
      diagnostics: {
        ...cached.diagnostics,
        cacheHit: true,
        cacheTtlMs: ttlMs,
      },
      cachePolicy: RADAR_HPC_STATUS_CACHE_POLICY,
    };
  }

  let rawRecords = [];
  let fetchDiagnostics = emptyDiagnostics({ cacheTtlMs: ttlMs });
  let errors = [];

  if (Array.isArray(records)) {
    rawRecords = records;
    fetchDiagnostics.paginationComplete = true;
    fetchDiagnostics.pageCount = 1;
    fetchDiagnostics.lastSuccessfulFetchAt = new Date().toISOString();
  } else {
    const key = process.env.AIRTABLE_API_KEY;
    const baseId = process.env.AIRTABLE_BASE_ID_ALT;
    const base =
      airtableBase ||
      (key && baseId ? new Airtable({ apiKey: key }).base(baseId) : null);

    if (!base) {
      return {
        ok: false,
        hotels: [],
        errors: [{ reason: "missing_airtable_config" }],
        skippedHidden: [],
        diagnostics: emptyDiagnostics({
          apiErrors: [{ reason: "missing_airtable_config" }],
          cacheTtlMs: ttlMs,
        }),
        cachePolicy: RADAR_HPC_STATUS_CACHE_POLICY,
      };
    }

    const fetched = await fetchHpcRadarStatusEligibleRecords({
      airtableBase: base,
      pageSize,
    });
    rawRecords = fetched.records;
    fetchDiagnostics = { ...fetched.diagnostics, cacheTtlMs: ttlMs, cacheHit: false };
    errors = fetched.errors;
  }

  const skippedHidden = [];
  const hotelsAll = [];
  for (const record of rawRecords) {
    const fields = record?.fields || {};
    if (!isHpcRadarStatusVisible(fields)) {
      skippedHidden.push(String(record?.id || "").trim() || "unknown");
      continue;
    }
    hotelsAll.push(formatHotelPropertyCensusAsBrandPresenceHotel(record));
  }

  const classified = classifyFormattedHotels(hotelsAll);
  const diagnostics = {
    ...fetchDiagnostics,
    openCount: classified.openCount,
    pipelineCount: classified.pipelineCount,
    retrievedCount: hotelsAll.length,
    missingIdentity: classified.missingIdentity,
    missingCoordinates: classified.missingCoordinates,
    duplicatesReconciled: [],
    apiErrors: errors.slice(),
    cacheHit: false,
    cacheTtlMs: ttlMs,
    formula: buildHpcRadarStatusFilterFormula(),
  };

  const payload = {
    ok: errors.length === 0,
    version: RADAR_INTERNAL_PREVIEW_HOTELS_VERSION,
    sourceTable: HOTEL_PROPERTY_CENSUS_TABLE,
    hotels: classified.mapEligible,
    hotelsAllStatuses: hotelsAll,
    errors,
    skippedHidden,
    diagnostics,
    cachePolicy: RADAR_HPC_STATUS_CACHE_POLICY,
  };

  if (!Array.isArray(records) && diagnostics.paginationComplete && errors.length === 0) {
    _hpcStatusCache = {
      expiresAt: now + ttlMs,
      payload: {
        ok: payload.ok,
        version: payload.version,
        sourceTable: payload.sourceTable,
        hotels: payload.hotels,
        hotelsAllStatuses: payload.hotelsAllStatuses,
        errors: payload.errors,
        skippedHidden: payload.skippedHidden,
        diagnostics: { ...diagnostics, cacheHit: false },
      },
    };
  }

  return payload;
}

/**
 * Merge internal-preview hotels into a brand-presence hotel array.
 * Preserves independently authorized legacy Hotel Census records on identity collision.
 */
export function mergeInternalPreviewHotelsIntoBrandPresenceList(
  existingHotels,
  previewHotels
) {
  const list = Array.isArray(existingHotels) ? existingHotels.slice() : [];
  const seenKeys = new Set();
  for (const hotel of list) {
    for (const key of buildHotelIdentityKeys(hotel)) seenKeys.add(key);
  }

  const added = [];
  const duplicatesReconciled = [];

  for (const hotel of Array.isArray(previewHotels) ? previewHotels : []) {
    const id = String(hotel?.id || "").trim();
    if (!REC_ID_RE.test(id)) continue;
    const status = String(hotel?.status || hotel?.radarStatus || "").trim();
    if (status !== "Open" && status !== "Pipeline") continue;
    if (!hotelHasMapCoordinates(hotel)) continue;

    const keys = [...buildHotelIdentityKeys(hotel)];
    const collision = keys.find((k) => seenKeys.has(k));
    if (collision) {
      duplicatesReconciled.push({
        hpcId: id,
        name: hotel?.name || null,
        matchedKey: collision,
        preservedSource: "hotel_census_or_existing",
      });
      continue;
    }

    list.push(hotel);
    for (const key of keys) seenKeys.add(key);
    added.push(id);
  }

  return {
    hotels: list,
    addedIds: added,
    duplicateSkipped: duplicatesReconciled.length > 0,
    duplicatesReconciled,
  };
}
