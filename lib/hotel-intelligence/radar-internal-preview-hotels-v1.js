/**
 * Radar internal-preview hotel inclusion (Phase 1.3).
 *
 * Bethesda Marriott (and future allowlisted hotels) may live on
 * Hotel Property Census while Radar map pins load from Hotel Census.
 * This module projects allowlisted HPC records into the brand-presence DTO
 * shape for authorized internal Radar only.
 *
 * Security:
 *   - Never trusts `internalPreview=1` alone.
 *   - Caller must prove admin / local Dealality admin via dealalityUser + req.
 *   - Customer/share audiences must not receive these hotels.
 */

import Airtable from "airtable";
import {
  INTERNAL_HE_PREVIEW_CENSUS_IDS,
  isBethesdaMarriottCensusId,
} from "./radar-hotel-explorer-progressive-enrichment-v1.js";
import { hasLocalDealalityAdminAccess } from "../dealality/local-dealality-admin-access.js";

export const RADAR_INTERNAL_PREVIEW_HOTELS_VERSION =
  "radar_internal_preview_hotels_v1";

export const HOTEL_PROPERTY_CENSUS_TABLE =
  process.env.AIRTABLE_HOTEL_PROPERTY_CENSUS_TABLE || "Hotel Property Census";

const REC_ID_RE = /^rec[A-Za-z0-9]{14}$/;

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

/**
 * Map a Hotel Property Census record into brand-presence hotel DTO.
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
    parentCompany: readText(fields, ["Parent Company"]) || "Unknown",
    status: readText(fields, ["status", "Status", "Production Use Status"]) || "unknown",
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

export function listInternalPreviewCensusIds() {
  return [...INTERNAL_HE_PREVIEW_CENSUS_IDS];
}

/**
 * Load allowlisted hotels from Hotel Property Census for authorized callers.
 * @returns {Promise<{ ok: boolean, hotels: object[], errors: object[] }>}
 */
export async function loadAuthorizedInternalPreviewHotels({
  dealalityUser,
  req = {},
  airtableBase = null,
  recordIds = null,
} = {}) {
  if (!isAuthorizedForRadarInternalPreviewHotels(dealalityUser, req)) {
    return {
      ok: false,
      unauthorized: true,
      hotels: [],
      errors: [{ reason: "forbidden_admin" }],
    };
  }

  const ids = (recordIds || listInternalPreviewCensusIds()).filter((id) =>
    REC_ID_RE.test(String(id || "").trim())
  );

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
    };
  }

  const hotels = [];
  const errors = [];
  for (const id of ids) {
    try {
      const record = await base(HOTEL_PROPERTY_CENSUS_TABLE).find(id);
      hotels.push(formatHotelPropertyCensusAsBrandPresenceHotel(record));
    } catch (err) {
      errors.push({
        id,
        reason: "hpc_fetch_failed",
        message: String(err?.message || err).slice(0, 200),
        isBethesda: isBethesdaMarriottCensusId(id),
      });
    }
  }

  return {
    ok: true,
    version: RADAR_INTERNAL_PREVIEW_HOTELS_VERSION,
    sourceTable: HOTEL_PROPERTY_CENSUS_TABLE,
    hotels,
    errors,
  };
}

/**
 * Merge internal-preview hotels into a brand-presence hotel array (by id).
 * Does not duplicate existing Hotel Census pins.
 */
export function mergeInternalPreviewHotelsIntoBrandPresenceList(
  existingHotels,
  previewHotels
) {
  const list = Array.isArray(existingHotels) ? existingHotels.slice() : [];
  const seen = new Set(list.map((h) => String(h?.id || "").trim()).filter(Boolean));
  const added = [];
  for (const hotel of Array.isArray(previewHotels) ? previewHotels : []) {
    const id = String(hotel?.id || "").trim();
    if (!REC_ID_RE.test(id) || seen.has(id)) continue;
    list.push(hotel);
    seen.add(id);
    added.push(id);
  }
  return { hotels: list, addedIds: added, duplicateSkipped: false };
}
