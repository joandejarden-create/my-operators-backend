/**
 * HBX Content API CALA Wave 1 dry-run ingest v1.
 * Candidate pack + write plan only. No Airtable writes.
 *
 * Objective: hbx-content-api-cala-wave1-dry-run-v1
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveHbxConfig,
  hbxFetchJson,
  contentUrl,
} from "./hbx-content-api-client.js";
import { normName } from "./census-autopilot-v2/identity-dedupe.js";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import {
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const HBX_WAVE1_OBJECTIVE = "hbx-content-api-cala-wave1-dry-run-v1";
export const HBX_WAVE1_VERSION = "hbx-content-api-cala-wave1-dry-run-v1";

export const HBX_WAVE1_STATUS = Object.freeze({
  COMPLETE_READY:
    "production_census_hbx_content_api_cala_wave1_dry_run_v1_complete_ready_for_apply_policy_review",
  PARTIAL_LICENSE:
    "production_census_hbx_content_api_cala_wave1_dry_run_v1_partial_license_policy_needed",
  PARTIAL_SOURCE:
    "production_census_hbx_content_api_cala_wave1_dry_run_v1_partial_source_remaining",
  BLOCKED: "production_census_hbx_content_api_cala_wave1_dry_run_v1_blocked",
});

/** ISO-2 codes for Wave 1 (verified via HBX countryCode filter). */
export const WAVE1_COUNTRY_MAP = Object.freeze({
  Mexico: "MX",
  "Dominican Republic": "DO",
  Colombia: "CO",
  "Costa Rica": "CR",
  Panama: "PA",
});

const allowedWave1Codes = new Set(Object.values(WAVE1_COUNTRY_MAP));

export const MATCH_CLASS = Object.freeze({
  EXISTING_MATCH_HIGH: "existing_match_high",
  EXISTING_MATCH_MEDIUM: "existing_match_medium",
  PROBABLE_DUPLICATE_HOLD: "probable_duplicate_hold",
  POSSIBLE_DUPLICATE_REVIEW: "possible_duplicate_review",
  NEW_CANDIDATE_HIGH: "new_candidate_high",
  NEW_CANDIDATE_MEDIUM: "new_candidate_medium",
  REJECT_NON_HOTEL: "reject_non_hotel",
  REJECT_INSUFFICIENT_IDENTITY: "reject_insufficient_identity",
  REJECT_WRONG_COUNTRY: "reject_wrong_country",
});

export const FIELD_LICENSE = Object.freeze({
  WRITE_CANDIDATE_NOW: "write_candidate_now",
  INTERNAL_ONLY_CANDIDATE: "internal_only_candidate",
  LICENSE_POLICY_NEEDED: "license_policy_needed",
  UNSUPPORTED: "unsupported",
  REJECT: "reject",
});

const NON_HOTEL_ACCOMMODATION = new Set([
  "A", // apartment often
  "V", // villa sometimes — keep as review, not hard reject unless clear
  "C", // camping
  "HOS",
  "HOSTEL",
]);

const CENSUS_READ_FIELDS = [
  "Property Identity Key",
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Country",
  "City",
  "Address",
  "Official Property URL",
  "Phone",
  "Latitude",
  "Longitude",
];

const CHECKPOINT_DIR = path.join(
  ROOT,
  "data/research-engine-v2/hbx-content-api-cala-wave1"
);
const CHECKPOINT_FILE = path.join(CHECKPOINT_DIR, "hbx-wave1-checkpoint.json");

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeMd(fp, md) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function blank(v) {
  return v == null || String(v).trim() === "";
}

function textContent(v) {
  if (v == null) return null;
  if (typeof v === "string") return v.trim() || null;
  if (typeof v === "object" && v.content != null) return String(v.content).trim() || null;
  return String(v).trim() || null;
}

function normCountry(c) {
  return String(c || "")
    .trim()
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "");
}

function countryNameFromCode(code) {
  const entry = Object.entries(WAVE1_COUNTRY_MAP).find(([, cc]) => cc === code);
  return entry ? entry[0] : code;
}

function domainOf(url) {
  if (!url) return null;
  try {
    const u = new URL(String(url).startsWith("http") ? url : `https://${url}`);
    return u.hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return null;
  }
}

function normPhone(p) {
  const d = String(p || "").replace(/\D/g, "");
  return d.length >= 8 ? d.slice(-10) : d || null;
}

function haversineM(lat1, lon1, lat2, lon2) {
  const toR = (d) => (Number(d) * Math.PI) / 180;
  const R = 6371000;
  const dLat = toR(lat2 - lat1);
  const dLon = toR(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toR(lat1)) * Math.cos(toR(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

function tokenOverlap(a, b) {
  const ta = new Set(String(a || "").split(/\s+/).filter((t) => t.length > 2));
  const tb = new Set(String(b || "").split(/\s+/).filter((t) => t.length > 2));
  if (!ta.size || !tb.size) return 0;
  let hit = 0;
  for (const t of ta) if (tb.has(t)) hit += 1;
  return hit / Math.max(ta.size, tb.size);
}

/**
 * Extract PHONEHOTEL only — reject PHONEBOOKING / PHONEMANAGEMENT.
 */
export function extractPhoneHotel(phones) {
  if (!Array.isArray(phones)) {
    return { phonehotel: null, rejected: [], all_types: [] };
  }
  const rejected = [];
  const allTypes = [];
  let phonehotel = null;
  for (const p of phones) {
    const typ = String(p?.phoneType || p?.phoneTypeCode || p?.type || "")
      .toUpperCase()
      .trim();
    const num = p?.phoneNumber || p?.number || null;
    if (typ) allTypes.push(typ);
    if (!num) continue;
    if (typ.includes("PHONEBOOKING") || typ.includes("BOOKING")) {
      rejected.push({ type: typ, reason: "PHONEBOOKING_rejected_as_hotel_phone" });
      continue;
    }
    if (typ.includes("PHONEMANAGEMENT") || typ.includes("MANAGEMENT")) {
      rejected.push({
        type: typ,
        reason: "PHONEMANAGEMENT_rejected_as_hotel_phone",
      });
      continue;
    }
    if (typ.includes("PHONEHOTEL") || typ === "HOTEL") {
      if (!phonehotel) phonehotel = String(num).trim();
    }
  }
  return { phonehotel, rejected, all_types: [...new Set(allTypes)] };
}

/**
 * Extract dry-run hotel record from HBX payload.
 */
export function extractHbxHotel(raw, expectedCountryName) {
  const code = raw?.code ?? raw?.hotelCode ?? null;
  const name = textContent(raw?.name);
  const countryCode = String(raw?.countryCode || raw?.country?.code || "").toUpperCase();
  const countryName = countryNameFromCode(countryCode) || expectedCountryName;
  const { phonehotel, rejected, all_types } = extractPhoneHotel(raw?.phones);
  const roomsArr = Array.isArray(raw?.rooms) ? raw.rooms : null;
  const roomsNumber =
    raw?.roomsNumber ?? raw?.roomCount ?? raw?.numberOfRooms ?? raw?.totalRooms ?? null;

  return {
    hbx_hotel_code: code,
    name,
    normalized_name: normName(name),
    accommodation_type: raw?.accommodationTypeCode || raw?.accommodationType?.code || null,
    category: raw?.categoryCode || raw?.category?.code || null,
    chain_code: raw?.chainCode || raw?.chain?.code || null,
    address: textContent(raw?.address),
    city: textContent(raw?.city),
    country: countryName,
    country_code: countryCode,
    postal_code: raw?.postalCode || raw?.postalcode || null,
    destination: raw?.destinationCode || raw?.destination?.code || null,
    zone: raw?.zoneCode || raw?.zone?.code || null,
    latitude: raw?.coordinates?.latitude ?? raw?.latitude ?? null,
    longitude: raw?.coordinates?.longitude ?? raw?.longitude ?? null,
    website: raw?.web || raw?.website || raw?.url || null,
    website_domain: domainOf(raw?.web || raw?.website || raw?.url),
    phonehotel,
    phone_rejected: rejected,
    phone_types_seen: all_types,
    description_present: Boolean(textContent(raw?.description)),
    facilities_present: Array.isArray(raw?.facilities) && raw.facilities.length > 0,
    images_present: Array.isArray(raw?.images) && raw.images.length > 0,
    room_types_present: Boolean(roomsArr && roomsArr.length > 0),
    room_types_count: roomsArr ? roomsArr.length : 0,
    rooms_total_supported: roomsNumber != null && Number(roomsNumber) > 0,
    rooms_total_value: roomsNumber != null && Number(roomsNumber) > 0 ? Number(roomsNumber) : null,
    // Explicit guard: never treat rooms[] length as keys
    rooms_array_length_not_used_as_keys: true,
    last_update: raw?.lastUpdate || null,
    raw_top_keys: Object.keys(raw || {}).slice(0, 30),
  };
}

export function classifyFieldLicenses(hotel) {
  return {
    address: hotel.address
      ? FIELD_LICENSE.INTERNAL_ONLY_CANDIDATE
      : FIELD_LICENSE.UNSUPPORTED,
    website: hotel.website
      ? FIELD_LICENSE.INTERNAL_ONLY_CANDIDATE
      : FIELD_LICENSE.UNSUPPORTED,
    phonehotel: hotel.phonehotel
      ? FIELD_LICENSE.INTERNAL_ONLY_CANDIDATE
      : FIELD_LICENSE.UNSUPPORTED,
    coordinates:
      hotel.latitude != null && hotel.longitude != null
        ? FIELD_LICENSE.LICENSE_POLICY_NEEDED
        : FIELD_LICENSE.UNSUPPORTED,
    images: hotel.images_present
      ? FIELD_LICENSE.LICENSE_POLICY_NEEDED
      : FIELD_LICENSE.UNSUPPORTED,
    description: hotel.description_present
      ? FIELD_LICENSE.LICENSE_POLICY_NEEDED
      : FIELD_LICENSE.UNSUPPORTED,
    facilities: hotel.facilities_present
      ? FIELD_LICENSE.LICENSE_POLICY_NEEDED
      : FIELD_LICENSE.UNSUPPORTED,
    rooms_keys: hotel.rooms_total_supported
      ? FIELD_LICENSE.INTERNAL_ONLY_CANDIDATE
      : FIELD_LICENSE.UNSUPPORTED,
    chain_code: hotel.chain_code
      ? FIELD_LICENSE.INTERNAL_ONLY_CANDIDATE
      : FIELD_LICENSE.UNSUPPORTED,
    category: hotel.category
      ? FIELD_LICENSE.INTERNAL_ONLY_CANDIDATE
      : FIELD_LICENSE.UNSUPPORTED,
  };
}

function isLikelyNonHotel(hotel) {
  const t = String(hotel.accommodation_type || "").toUpperCase();
  if (!t) return false;
  if (t === "H" || t === "HOTEL") return false;
  if (NON_HOTEL_ACCOMMODATION.has(t)) return true;
  if (/HOSTEL|CAMP|APARTMENT|APTHOTEL/.test(t) && t !== "H") return true;
  return false;
}

/**
 * Match one HBX hotel against census index.
 */
export function matchHbxToCensus(hotel, censusIndex) {
  const licenses = classifyFieldLicenses(hotel);
  const base = {
    hbx_hotel_code: hotel.hbx_hotel_code,
    name: hotel.name,
    country: hotel.country,
    city: hotel.city,
    field_licenses: licenses,
  };

  if (!hotel.country_code || !allowedWave1Codes.has(hotel.country_code)) {
    return {
      ...base,
      match_class: MATCH_CLASS.REJECT_WRONG_COUNTRY,
      reason: "country_not_in_wave1",
      census_record_id: null,
    };
  }

  if (!hotel.normalized_name || hotel.normalized_name.length < 3) {
    return {
      ...base,
      match_class: MATCH_CLASS.REJECT_INSUFFICIENT_IDENTITY,
      reason: "name_too_short",
      census_record_id: null,
    };
  }

  if (isLikelyNonHotel(hotel)) {
    return {
      ...base,
      match_class: MATCH_CLASS.REJECT_NON_HOTEL,
      reason: `accommodation_type=${hotel.accommodation_type}`,
      census_record_id: null,
    };
  }

  const countryKey = normCountry(hotel.country);
  const bucket = censusIndex.byCountry.get(countryKey) || [];
  const nName = hotel.normalized_name;
  const domain = hotel.website_domain;
  const phone = normPhone(hotel.phonehotel);

  let best = null;

  // Exact name|country
  const exactKey = `${nName}|${countryKey}`;
  const exact = censusIndex.byNameCountry.get(exactKey);
  if (exact) {
    best = {
      record: exact,
      score: 1,
      signals: ["exact_normalized_name_country"],
      class: MATCH_CLASS.EXISTING_MATCH_HIGH,
    };
  }

  // Domain match
  if (domain && censusIndex.byDomain.has(domain)) {
    const recs = censusIndex.byDomain.get(domain);
    for (const r of recs) {
      if (normCountry(r.country) !== countryKey) continue;
      if (!best || best.score < 0.95) {
        best = {
          record: r,
          score: 0.95,
          signals: ["website_domain"],
          class: MATCH_CLASS.EXISTING_MATCH_HIGH,
        };
      }
    }
  }

  // Phone match
  if (phone && censusIndex.byPhone.has(phone)) {
    const recs = censusIndex.byPhone.get(phone);
    for (const r of recs) {
      if (normCountry(r.country) !== countryKey) continue;
      if (!best || best.score < 0.92) {
        best = {
          record: r,
          score: 0.92,
          signals: ["phonehotel"],
          class: MATCH_CLASS.EXISTING_MATCH_HIGH,
        };
      }
    }
  }

  // Fuzzy within country
  if (!best || best.score < 0.9) {
    for (const r of bucket) {
      const overlap = tokenOverlap(nName, r.normalized_name);
      const sameCity =
        hotel.city &&
        r.city &&
        normName(hotel.city) === normName(r.city);
      let score = overlap;
      const signals = [];
      if (overlap >= 0.7) signals.push(`name_overlap=${overlap.toFixed(2)}`);
      if (sameCity) {
        score += 0.15;
        signals.push("same_city");
      }
      if (
        hotel.latitude != null &&
        hotel.longitude != null &&
        r.lat != null &&
        r.lng != null
      ) {
        const dist = haversineM(hotel.latitude, hotel.longitude, r.lat, r.lng);
        if (dist <= 150) {
          score += 0.2;
          signals.push(`coords_within_${Math.round(dist)}m`);
        } else if (dist <= 500 && overlap >= 0.5) {
          score += 0.1;
          signals.push(`coords_near_${Math.round(dist)}m`);
        }
      }
      if (hotel.chain_code && r.brand) {
        const b = normName(r.brand);
        if (b && nName.includes(b.slice(0, 6))) {
          score += 0.05;
          signals.push("brand_hint");
        }
      }
      if (score >= 0.85 && (!best || score > best.score)) {
        best = {
          record: r,
          score,
          signals,
          class:
            score >= 0.95 || (overlap >= 0.85 && sameCity)
              ? MATCH_CLASS.EXISTING_MATCH_HIGH
              : MATCH_CLASS.EXISTING_MATCH_MEDIUM,
        };
      } else if (score >= 0.65 && overlap >= 0.65 && (!best || score > best.score)) {
        best = {
          record: r,
          score,
          signals,
          class: sameCity
            ? MATCH_CLASS.POSSIBLE_DUPLICATE_REVIEW
            : MATCH_CLASS.PROBABLE_DUPLICATE_HOLD,
        };
      }
    }
  }

  if (best && best.class.startsWith("existing")) {
    return {
      ...base,
      match_class: best.class,
      match_score: best.score,
      match_signals: best.signals,
      census_record_id: best.record.id,
      census_property_name: best.record.name,
      census_city: best.record.city,
      census_has_address: Boolean(best.record.address),
      census_has_website: Boolean(best.record.website),
      census_has_phone: Boolean(best.record.phone),
      census_has_coords: best.record.lat != null && best.record.lng != null,
    };
  }

  if (best && (best.class === MATCH_CLASS.PROBABLE_DUPLICATE_HOLD || best.class === MATCH_CLASS.POSSIBLE_DUPLICATE_REVIEW)) {
    return {
      ...base,
      match_class: best.class,
      match_score: best.score,
      match_signals: best.signals,
      census_record_id: best.record.id,
      census_property_name: best.record.name,
    };
  }

  // New candidate quality
  const identityBits = [
    hotel.name,
    hotel.country,
    hotel.city,
    hotel.address,
    hotel.website,
    hotel.phonehotel,
  ].filter(Boolean).length;

  const high =
    Boolean(hotel.name && hotel.country && (hotel.address || hotel.website || hotel.phonehotel)) &&
    identityBits >= 4;

  return {
    ...base,
    match_class: high
      ? MATCH_CLASS.NEW_CANDIDATE_HIGH
      : MATCH_CLASS.NEW_CANDIDATE_MEDIUM,
    match_score: high ? 0.8 : 0.55,
    match_signals: [`identity_bits=${identityBits}`],
    census_record_id: null,
  };
}

function buildCensusIndex(records) {
  const byCountry = new Map();
  const byNameCountry = new Map();
  const byDomain = new Map();
  const byPhone = new Map();

  for (const r of records) {
    const f = r.fields || {};
    const name = f["Canonical Property Name"] || f["Property Name"] || "";
    const country = f.Country || "";
    const city = f.City || "";
    const row = {
      id: r.id,
      name,
      normalized_name: normName(name),
      country,
      city,
      address: f.Address || null,
      website: f["Official Property URL"] || null,
      domain: domainOf(f["Official Property URL"]),
      phone: f.Phone || null,
      phone_norm: normPhone(f.Phone),
      brand: f["Current Brand"] || f["Brand Family"] || null,
      lat: f.Latitude != null ? Number(f.Latitude) : null,
      lng: f.Longitude != null ? Number(f.Longitude) : null,
    };
    const ck = normCountry(country);
    if (!byCountry.has(ck)) byCountry.set(ck, []);
    byCountry.get(ck).push(row);
    if (row.normalized_name) {
      byNameCountry.set(`${row.normalized_name}|${ck}`, row);
    }
    if (row.domain) {
      if (!byDomain.has(row.domain)) byDomain.set(row.domain, []);
      byDomain.get(row.domain).push(row);
    }
    if (row.phone_norm) {
      if (!byPhone.has(row.phone_norm)) byPhone.set(row.phone_norm, []);
      byPhone.get(row.phone_norm).push(row);
    }
  }
  return { byCountry, byNameCountry, byDomain, byPhone, size: records.length };
}

async function listCensusWave1(baseId, token, tableId, countries) {
  const out = [];
  // Filter by country OR formula
  const orParts = countries.map(
    (c) => `{Country}='${String(c).replace(/'/g, "\\'")}'`
  );
  const filterByFormula = `OR(${orParts.join(",")})`;
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula });
    if (offset) params.set("offset", offset);
    for (const f of CENSUS_READ_FIELDS) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(
        `census_list_failed:${res.status}:${json?.error?.message || ""}`
      );
    }
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(110);
  } while (offset);
  return out;
}

function emptyCountryStats(country) {
  return {
    country,
    hbx_hotels_pulled: 0,
    hbx_total_reported: null,
    existing_match_high: 0,
    existing_match_medium: 0,
    new_candidate_high: 0,
    new_candidate_medium: 0,
    probable_duplicate_hold: 0,
    possible_duplicate_review: 0,
    rejected_non_hotel: 0,
    rejected_insufficient_identity: 0,
    rejected_wrong_country: 0,
    address_candidates: 0,
    website_candidates: 0,
    phonehotel_candidates: 0,
    coordinate_candidates_held: 0,
    image_description_facility_held: 0,
    room_count_unsupported: 0,
    estimated_write_yield_if_approved: 0,
  };
}

function bumpClass(stats, matchClass) {
  const map = {
    [MATCH_CLASS.EXISTING_MATCH_HIGH]: "existing_match_high",
    [MATCH_CLASS.EXISTING_MATCH_MEDIUM]: "existing_match_medium",
    [MATCH_CLASS.NEW_CANDIDATE_HIGH]: "new_candidate_high",
    [MATCH_CLASS.NEW_CANDIDATE_MEDIUM]: "new_candidate_medium",
    [MATCH_CLASS.PROBABLE_DUPLICATE_HOLD]: "probable_duplicate_hold",
    [MATCH_CLASS.POSSIBLE_DUPLICATE_REVIEW]: "possible_duplicate_review",
    [MATCH_CLASS.REJECT_NON_HOTEL]: "rejected_non_hotel",
    [MATCH_CLASS.REJECT_INSUFFICIENT_IDENTITY]: "rejected_insufficient_identity",
    [MATCH_CLASS.REJECT_WRONG_COUNTRY]: "rejected_wrong_country",
  };
  const k = map[matchClass];
  if (k) stats[k] += 1;
}

function buildWritePlanForMatch(hotel, match) {
  const plans = [];
  const licenses = match.field_licenses || classifyFieldLicenses(hotel);

  if (match.match_class === MATCH_CLASS.EXISTING_MATCH_HIGH) {
    plans.push({
      action: "update_candidate",
      census_record_id: match.census_record_id,
      hbx_hotel_code: hotel.hbx_hotel_code,
      fields: [],
    });
    const fields = plans[0].fields;

    fields.push({
      field: "HBX External ID / Hotelbeds Code",
      value: hotel.hbx_hotel_code,
      license: FIELD_LICENSE.INTERNAL_ONLY_CANDIDATE,
      note: "Only if schema field exists — verify before apply",
      schema_exists: "unknown_verify_before_apply",
    });

    if (hotel.address && !match.census_has_address) {
      fields.push({
        field: "Address",
        value: hotel.address,
        license: licenses.address,
        note: "blank census address → Medium internal candidate",
      });
    }
    if (hotel.website && !match.census_has_website) {
      fields.push({
        field: "Official Property URL",
        value: hotel.website,
        license: licenses.website,
        note: "blank census website → Medium internal candidate; validate non-OTA",
      });
    }
    if (hotel.phonehotel && !match.census_has_phone) {
      fields.push({
        field: "Phone",
        value: hotel.phonehotel,
        license: licenses.phonehotel,
        note: "PHONEHOTEL only; require provenance if schema supports",
      });
    }
    if (hotel.category) {
      fields.push({
        field: "Category / hotel class",
        value: hotel.category,
        license: licenses.category,
        note: "Only if Census schema supports",
        schema_exists: "unknown_verify_before_apply",
      });
    }
    if (hotel.chain_code) {
      fields.push({
        field: "Chain Code",
        value: hotel.chain_code,
        license: licenses.chain_code,
        note: "Map via Brand Setup before Current Brand write",
        schema_exists: "unknown_verify_before_apply",
      });
    }
    if (hotel.latitude != null && hotel.longitude != null) {
      fields.push({
        field: "Latitude/Longitude",
        value: { lat: hotel.latitude, lng: hotel.longitude },
        license: FIELD_LICENSE.LICENSE_POLICY_NEEDED,
        note: "Held — prefer Mapbox-after-validated-address unless HBX storage licensed",
      });
    }
    for (const [label, present] of [
      ["Description", hotel.description_present],
      ["Images", hotel.images_present],
      ["Facilities", hotel.facilities_present],
    ]) {
      if (present) {
        fields.push({
          field: label,
          value: "present_in_hbx",
          license: FIELD_LICENSE.LICENSE_POLICY_NEEDED,
          note: "Held pending license/storage review",
        });
      }
    }
    fields.push({
      field: "Rooms / Keys",
      value: null,
      license: FIELD_LICENSE.UNSUPPORTED,
      note: "HBX rooms[] is room-type catalog only — never write from array length",
    });
  }

  if (match.match_class === MATCH_CLASS.NEW_CANDIDATE_HIGH) {
    plans.push({
      action: "insert_candidate",
      hbx_hotel_code: hotel.hbx_hotel_code,
      proposed_fields: {
        "Property Name": hotel.name,
        Country: hotel.country,
        City: hotel.city,
        Address: hotel.address,
        "Official Property URL": hotel.website,
        Phone: hotel.phonehotel,
        "Production Use Status": "Census Only / Not Owner-Facing",
        "Public Display Review Status": "Hold",
        "Radar Display Status": "Hold",
        "Human Review Required": true,
        "Review Status": "Needs Review",
        _hbx_hotel_code: hotel.hbx_hotel_code,
        _chain_code: hotel.chain_code,
        _category: hotel.category,
        _coordinates_held: {
          lat: hotel.latitude,
          lng: hotel.longitude,
          license: FIELD_LICENSE.LICENSE_POLICY_NEEDED,
        },
        _rooms_keys: FIELD_LICENSE.UNSUPPORTED,
      },
      note: "No production inserts in this dry run",
    });
  }

  return plans;
}

function loadCheckpoint() {
  try {
    if (!fs.existsSync(CHECKPOINT_FILE)) return null;
    return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8"));
  } catch {
    return null;
  }
}

function saveCheckpoint(state) {
  writeJson(CHECKPOINT_FILE, { ...state, updated_at: new Date().toISOString() });
}

export async function pullCountryHotels(cfg, countryName, countryCode, opts) {
  const batchSize = Math.min(1000, Math.max(1, Number(opts.batchSize || 100)));
  const maxHotels = opts.maxHotelsPerCountry;
  const delayMs = opts.delayMs;
  const limiter = opts.rateLimiter || null;
  const fields = String(opts.fields || "all").trim() || "all";
  const hotels = [];
  let from = Math.max(1, Number(opts.startFrom || 1));
  let total = null;
  const seen = new Set();

  while (hotels.length < maxHotels) {
    const to = Math.min(from + batchSize - 1, from + (maxHotels - hotels.length) - 1);
    const qs = [
      `fields=${encodeURIComponent(fields)}`,
      "language=ENG",
      `from=${from}`,
      `to=${to}`,
      `countryCode=${encodeURIComponent(countryCode)}`,
      "useSecondaryLanguage=false",
    ].join("&");
    const fetchOnce = () => hbxFetchJson(contentUrl(cfg, `hotels?${qs}`), cfg);
    const res = limiter ? await limiter.schedule(fetchOnce) : await fetchOnce();
    if (!res.ok) {
      return {
        ok: false,
        hotels,
        total,
        partial: hotels.length > 0,
        error: {
          status: res.status,
          code: res.error_code,
          message: res.error_message,
          quota_exceeded: Boolean(res.quota_exceeded) || /quota/i.test(String(res.error_message || "")),
          response_headers: res.response_headers || {},
        },
        from,
        to,
        requests_note: limiter ? { count: limiter.requestCount } : null,
      };
    }
    total = res.body?.total ?? total;
    const page = Array.isArray(res.body?.hotels) ? res.body.hotels : [];
    if (!page.length) break;
    for (const h of page) {
      const code = h?.code ?? h?.hotelCode;
      if (code == null || seen.has(code)) continue;
      seen.add(code);
      hotels.push(h);
      if (hotels.length >= maxHotels) break;
    }
    opts.onBatch?.({
      country: countryName,
      countryCode,
      from,
      to,
      page: page.length,
      pulled: hotels.length,
      total,
    });
    if (page.length < batchSize) break;
    if (total != null && to >= total) break;
    from = to + 1;
    if (!limiter) await sleep(delayMs);
  }

  return { ok: true, hotels, total, error: null, partial: false };
}

function renderDryRunMarkdown(report) {
  const by = report.by_country || {};
  const countryRows = Object.values(by)
    .map(
      (s) =>
        `| ${s.country} | ${s.hbx_hotels_pulled} | ${s.existing_match_high} | ${s.existing_match_medium} | ${s.new_candidate_high} | ${s.new_candidate_medium} | ${s.probable_duplicate_hold + s.possible_duplicate_review} | ${s.rejected_non_hotel + s.rejected_insufficient_identity + s.rejected_wrong_country} | ${s.address_candidates} | ${s.website_candidates} | ${s.phonehotel_candidates} | ${s.coordinate_candidates_held} | ${s.estimated_write_yield_if_approved} |`
    )
    .join("\n");

  return `# HBX Content API CALA Wave 1 Dry-Run v1

**Status:** \`${report.status}\`
**Objective:** \`${HBX_WAVE1_OBJECTIVE}\`
**Generated:** ${report.generated_at}
**Mode:** candidate-only / dry-run

## No-write confirmation

- Airtable writes: **${report.airtable_writes}**
- Hotel Property Census updates: **${report.census_writes}**
- Inserts: **${report.inserts}**
- Brand Explorer / Brand Setup: **0**
- \`ENABLE_HBX_CENSUS_WRITES\`: **0**
- \`ENABLE_HBX_INSERTS\`: **0**
- Future target: ${productionHotelPropertyCensus.tableName} (\`${PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID}\`) — **not written**

## Scope

- Countries: ${(report.countries || []).join(", ")}
- Batch size: ${report.batch_size}
- Max hotels/country: ${report.max_hotels_per_country}
- Census records indexed (Wave 1 filter): ${report.census_records_indexed}

## Totals

| Metric | Count |
|--------|-------|
| HBX hotels pulled | ${report.totals?.hbx_hotels_pulled ?? 0} |
| existing_match_high | ${report.totals?.existing_match_high ?? 0} |
| existing_match_medium | ${report.totals?.existing_match_medium ?? 0} |
| new_candidate_high | ${report.totals?.new_candidate_high ?? 0} |
| new_candidate_medium | ${report.totals?.new_candidate_medium ?? 0} |
| duplicate holds/reviews | ${report.totals?.duplicate_holds ?? 0} |
| rejected | ${report.totals?.rejected ?? 0} |
| address candidates | ${report.totals?.address_candidates ?? 0} |
| website candidates | ${report.totals?.website_candidates ?? 0} |
| PHONEHOTEL candidates | ${report.totals?.phonehotel_candidates ?? 0} |
| coordinate candidates held | ${report.totals?.coordinate_candidates_held ?? 0} |
| image/description/facility held | ${report.totals?.image_description_facility_held ?? 0} |
| room count unsupported | ${report.totals?.room_count_unsupported ?? 0} |
| estimated write yield if approved | ${report.totals?.estimated_write_yield_if_approved ?? 0} |

## By country

| Country | Pulled | Exist High | Exist Med | New High | New Med | Dup holds | Rejected | Addr | Web | Phone | Coords held | Est. yield |
|---------|--------|------------|-----------|----------|---------|-----------|----------|------|-----|-------|-------------|------------|
${countryRows}

## Policy guards

- PHONEBOOKING / PHONEMANAGEMENT: **rejected as hotel phone**
- Rooms / Keys from \`rooms[]\`: **unsupported** (room-type catalog only)
- Coordinates / images / descriptions / facilities: **license_policy_needed**

## License policy decisions needed

${(report.license_policy_decisions_needed || []).map((x) => `- ${x}`).join("\n")}

## Artifacts

- \`reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json\`
- \`reports/research-engine-v2/hbx-cala-wave1-candidate-pack.md\`
- \`reports/research-engine-v2/hbx-cala-wave1-write-plan.json\`
- \`reports/research-engine-v2/hbx-cala-wave1-write-plan.md\`
- Checkpoint: \`data/research-engine-v2/hbx-content-api-cala-wave1/hbx-wave1-checkpoint.json\`

## Next step

${report.next_step || "Founder/policy review before any ENABLE_HBX_CENSUS_WRITES apply."}
`;
}

/**
 * @param {{ env?: NodeJS.ProcessEnv, log?: Function }} [opts]
 */
export async function runHbxCalaWave1DryRunV1(opts = {}) {
  const log = opts.log || console.log;
  const env = { ...(opts.env || process.env) };
  env.ENABLE_HBX_CENSUS_WRITES = "0";
  env.ENABLE_HBX_INSERTS = "0";
  env.HBX_DRY_RUN = "1";

  const countries = String(
    env.HBX_WAVE1_COUNTRIES ||
      "Mexico,Dominican Republic,Colombia,Costa Rica,Panama"
  )
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const batchSize = Math.max(10, Number(env.HBX_BATCH_SIZE || 100));
  const maxHotelsPerCountry = Math.max(
    batchSize,
    Number(env.HBX_MAX_HOTELS_PER_COUNTRY || 1000)
  );
  const delayMs = Number(env.HBX_BATCH_DELAY_MS || 150);

  const cfg = resolveHbxConfig(env);
  if (!cfg.ok) {
    const blocked = {
      ok: false,
      status: HBX_WAVE1_STATUS.BLOCKED,
      objective: HBX_WAVE1_OBJECTIVE,
      reason: "missing_hbx_credentials",
      missing: cfg.missing,
      airtable_writes: 0,
      census_writes: 0,
      inserts: 0,
      generated_at: new Date().toISOString(),
    };
    persistAll(blocked, { candidates: [], writePlans: [] });
    return blocked;
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases.target_base_id) {
    const blocked = {
      ok: false,
      status: HBX_WAVE1_STATUS.BLOCKED,
      objective: HBX_WAVE1_OBJECTIVE,
      reason: "missing_airtable_read_credentials",
      airtable_writes: 0,
      census_writes: 0,
      inserts: 0,
      generated_at: new Date().toISOString(),
    };
    persistAll(blocked, { candidates: [], writePlans: [] });
    return blocked;
  }

  log(
    `[hbx-wave1] dry-run countries=${countries.join("|")} batch=${batchSize} max/country=${maxHotelsPerCountry} writes=0`
  );

  log("[hbx-wave1] listing Hotel Property Census (Wave 1 countries, read-only)");
  const censusRecords = await listCensusWave1(
    bases.target_base_id,
    token,
    PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
    countries
  );
  const censusIndex = buildCensusIndex(censusRecords);
  log(`[hbx-wave1] census_indexed=${censusIndex.size}`);

  const checkpoint = {
    version: HBX_WAVE1_VERSION,
    objective: HBX_WAVE1_OBJECTIVE,
    dry_run: true,
    countries: {},
    started_at: new Date().toISOString(),
  };

  const byCountry = {};
  const candidates = [];
  const writePlans = [];
  const extractedByCountry = {};

  for (const countryName of countries) {
    const countryCode = WAVE1_COUNTRY_MAP[countryName];
    if (!countryCode) {
      log(`[hbx-wave1] skip unknown country mapping: ${countryName}`);
      continue;
    }
    const stats = emptyCountryStats(countryName);
    byCountry[countryName] = stats;
    extractedByCountry[countryName] = [];

    log(`[hbx-wave1] pull country=${countryName} code=${countryCode}`);
    const pulled = await pullCountryHotels(cfg, countryName, countryCode, {
      batchSize,
      maxHotelsPerCountry,
      delayMs,
      onBatch: (b) => {
        log(
          `[hbx-wave1] batch ${b.country} from=${b.from} to=${b.to} page=${b.page} pulled=${b.pulled}/${b.total ?? "?"}`
        );
        checkpoint.countries[countryName] = {
          countryCode,
          from: b.from,
          to: b.to,
          pulled: b.pulled,
          total: b.total,
        };
        saveCheckpoint(checkpoint);
      },
    });

    if (!pulled.ok) {
      stats.error = pulled.error;
      log(
        `[hbx-wave1] pull_failed country=${countryName} status=${pulled.error?.status}`
      );
      continue;
    }

    stats.hbx_hotels_pulled = pulled.hotels.length;
    stats.hbx_total_reported = pulled.total;
    checkpoint.countries[countryName] = {
      countryCode,
      pulled: pulled.hotels.length,
      total: pulled.total,
      complete: true,
    };
    saveCheckpoint(checkpoint);

    for (const raw of pulled.hotels) {
      const hotel = extractHbxHotel(raw, countryName);
      // Force country consistency
      if (hotel.country_code !== countryCode) {
        hotel.country_code = countryCode;
        hotel.country = countryName;
      }
      const match = matchHbxToCensus(hotel, censusIndex);
      bumpClass(stats, match.match_class);

      if (hotel.address) stats.address_candidates += 1;
      if (hotel.website) stats.website_candidates += 1;
      if (hotel.phonehotel) stats.phonehotel_candidates += 1;
      if (hotel.latitude != null && hotel.longitude != null) {
        stats.coordinate_candidates_held += 1;
      }
      if (
        hotel.description_present ||
        hotel.images_present ||
        hotel.facilities_present
      ) {
        stats.image_description_facility_held += 1;
      }
      if (!hotel.rooms_total_supported) stats.room_count_unsupported += 1;

      const row = {
        ...hotel,
        match_class: match.match_class,
        match_score: match.match_score ?? null,
        match_signals: match.match_signals || [],
        census_record_id: match.census_record_id,
        census_property_name: match.census_property_name || null,
        field_licenses: match.field_licenses,
      };
      candidates.push(row);
      extractedByCountry[countryName].push(row);

      const plans = buildWritePlanForMatch(hotel, match);
      for (const p of plans) {
        writePlans.push({ country: countryName, ...p });
        if (p.action === "update_candidate") {
          const writableNow = (p.fields || []).filter(
            (f) =>
              f.license === FIELD_LICENSE.INTERNAL_ONLY_CANDIDATE &&
              f.field !== "HBX External ID / Hotelbeds Code" &&
              f.field !== "Category / hotel class" &&
              f.field !== "Chain Code"
          ).length;
          stats.estimated_write_yield_if_approved += writableNow;
        }
        if (p.action === "insert_candidate") {
          stats.estimated_write_yield_if_approved += 1;
        }
      }
    }
  }

  const totals = Object.values(byCountry).reduce(
    (a, s) => {
      a.hbx_hotels_pulled += s.hbx_hotels_pulled;
      a.existing_match_high += s.existing_match_high;
      a.existing_match_medium += s.existing_match_medium;
      a.new_candidate_high += s.new_candidate_high;
      a.new_candidate_medium += s.new_candidate_medium;
      a.duplicate_holds +=
        s.probable_duplicate_hold + s.possible_duplicate_review;
      a.rejected +=
        s.rejected_non_hotel +
        s.rejected_insufficient_identity +
        s.rejected_wrong_country;
      a.address_candidates += s.address_candidates;
      a.website_candidates += s.website_candidates;
      a.phonehotel_candidates += s.phonehotel_candidates;
      a.coordinate_candidates_held += s.coordinate_candidates_held;
      a.image_description_facility_held += s.image_description_facility_held;
      a.room_count_unsupported += s.room_count_unsupported;
      a.estimated_write_yield_if_approved += s.estimated_write_yield_if_approved;
      return a;
    },
    {
      hbx_hotels_pulled: 0,
      existing_match_high: 0,
      existing_match_medium: 0,
      new_candidate_high: 0,
      new_candidate_medium: 0,
      duplicate_holds: 0,
      rejected: 0,
      address_candidates: 0,
      website_candidates: 0,
      phonehotel_candidates: 0,
      coordinate_candidates_held: 0,
      image_description_facility_held: 0,
      room_count_unsupported: 0,
      estimated_write_yield_if_approved: 0,
    }
  );

  const licenseDecisions = [
    "Confirm whether HBX coordinates may be permanently stored in Hotel Property Census (else Mapbox-after-validated-address only).",
    "Confirm whether HBX images may be stored or linked (likely internal-only / do-not-publish without license).",
    "Confirm whether HBX descriptions may be stored (likely internal-only).",
    "Confirm whether facilities tags may be stored as internal enrichment.",
    "Confirm schema field for Hotelbeds/HBX external hotel code before apply.",
  ];

  let status = HBX_WAVE1_STATUS.COMPLETE_READY;
  if (totals.hbx_hotels_pulled === 0) {
    status = HBX_WAVE1_STATUS.BLOCKED;
  } else if (totals.coordinate_candidates_held > 0 || totals.image_description_facility_held > 0) {
    status = HBX_WAVE1_STATUS.PARTIAL_LICENSE;
  }
  // Partial source if any country pulled less than reported total due to max cap
  const capped = Object.values(byCountry).some(
    (s) =>
      s.hbx_total_reported != null &&
      s.hbx_hotels_pulled < s.hbx_total_reported &&
      s.hbx_hotels_pulled >= maxHotelsPerCountry
  );
  if (capped && status === HBX_WAVE1_STATUS.COMPLETE_READY) {
    status = HBX_WAVE1_STATUS.PARTIAL_SOURCE;
  } else if (capped && status === HBX_WAVE1_STATUS.PARTIAL_LICENSE) {
    // keep license as primary but note source remaining
  }

  const report = {
    ok: status !== HBX_WAVE1_STATUS.BLOCKED,
    status,
    objective: HBX_WAVE1_OBJECTIVE,
    version: HBX_WAVE1_VERSION,
    generated_at: new Date().toISOString(),
    mode: "candidate-only",
    dry_run: true,
    countries,
    country_codes: Object.fromEntries(
      countries.map((c) => [c, WAVE1_COUNTRY_MAP[c]])
    ),
    batch_size: batchSize,
    max_hotels_per_country: maxHotelsPerCountry,
    census_records_indexed: censusIndex.size,
    by_country: byCountry,
    totals,
    license_policy_decisions_needed: licenseDecisions,
    policy_guards: {
      phonebooking_rejected: true,
      phonemanagement_rejected: true,
      rooms_array_not_used_as_keys: true,
      coordinates_license_policy_needed: true,
      images_license_policy_needed: true,
      descriptions_license_policy_needed: true,
      facilities_license_policy_needed: true,
    },
    airtable_writes: 0,
    census_writes: 0,
    inserts: 0,
    brand_explorer_writes: 0,
    brand_setup_writes: 0,
    owner_operator_date_writes: 0,
    recent_momentum_writes: 0,
    company_validated_writes: 0,
    brand_verified_writes: 0,
    secrets_logged: false,
    flags: {
      ENABLE_HBX_CENSUS_WRITES: "0",
      ENABLE_HBX_INSERTS: "0",
      HBX_DRY_RUN: "1",
    },
    future_write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
      used_this_run: false,
    },
    next_step:
      "Policy review (coords/images/descriptions/facilities + schema external ID). Then optional apply under ENABLE_HBX_CENSUS_WRITES with Medium-internal only for address/website/PHONEHOTEL.",
    candidate_pack_path:
      "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json",
    write_plan_path: "reports/research-engine-v2/hbx-cala-wave1-write-plan.json",
    checkpoint_path:
      "data/research-engine-v2/hbx-content-api-cala-wave1/hbx-wave1-checkpoint.json",
  };

  persistAll(report, { candidates, writePlans, extractedByCountry });
  saveCheckpoint({
    ...checkpoint,
    finished_at: new Date().toISOString(),
    status,
    totals,
  });

  log(
    `[hbx-wave1] done status=${status} pulled=${totals.hbx_hotels_pulled} new_high=${totals.new_candidate_high} exist_high=${totals.existing_match_high} airtable_writes=0`
  );
  return report;
}

function persistAll(report, { candidates = [], writePlans = [], extractedByCountry = {} }) {
  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const docsDir = path.join(ROOT, "docs/data-intelligence");

  writeJson(path.join(reportsDir, "hbx-content-api-cala-wave1-dry-run-v1.json"), report);
  writeMd(
    path.join(reportsDir, "hbx-content-api-cala-wave1-dry-run-v1.md"),
    renderDryRunMarkdown(report)
  );
  writeMd(
    path.join(docsDir, "hbx-content-api-cala-wave1-dry-run-v1.md"),
    renderDryRunMarkdown(report)
  );

  const pack = {
    objective: HBX_WAVE1_OBJECTIVE,
    generated_at: report.generated_at || new Date().toISOString(),
    dry_run: true,
    airtable_writes: 0,
    count: candidates.length,
    by_match_class: candidates.reduce((a, c) => {
      a[c.match_class] = (a[c.match_class] || 0) + 1;
      return a;
    }, {}),
    candidates: candidates.map((c) => ({
      hbx_hotel_code: c.hbx_hotel_code,
      name: c.name,
      country: c.country,
      city: c.city,
      address: c.address,
      website: c.website,
      phonehotel: c.phonehotel,
      chain_code: c.chain_code,
      category: c.category,
      latitude: c.latitude,
      longitude: c.longitude,
      rooms_total_supported: c.rooms_total_supported,
      room_types_count: c.room_types_count,
      match_class: c.match_class,
      match_score: c.match_score,
      match_signals: c.match_signals,
      census_record_id: c.census_record_id,
      field_licenses: c.field_licenses,
    })),
    by_country_counts: Object.fromEntries(
      Object.entries(extractedByCountry).map(([k, arr]) => [k, arr.length])
    ),
  };
  writeJson(path.join(reportsDir, "hbx-cala-wave1-candidate-pack.json"), pack);
  writeMd(
    path.join(reportsDir, "hbx-cala-wave1-candidate-pack.md"),
    `# HBX CALA Wave 1 Candidate Pack

**Generated:** ${pack.generated_at}
**Candidates:** ${pack.count}
**Airtable writes:** 0

## By match class

${Object.entries(pack.by_match_class)
  .map(([k, v]) => `- \`${k}\`: ${v}`)
  .join("\n")}

## Sample (first 25)

| Code | Name | Country | City | Class |
|------|------|---------|------|-------|
${pack.candidates
  .slice(0, 25)
  .map(
    (c) =>
      `| ${c.hbx_hotel_code} | ${String(c.name || "").slice(0, 40)} | ${c.country} | ${c.city || ""} | \`${c.match_class}\` |`
  )
  .join("\n")}

Full JSON: \`reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json\`
`
  );

  const planDoc = {
    objective: HBX_WAVE1_OBJECTIVE,
    generated_at: report.generated_at || new Date().toISOString(),
    dry_run: true,
    airtable_writes: 0,
    inserts: 0,
    updates: 0,
    plan_count: writePlans.length,
    update_candidates: writePlans.filter((p) => p.action === "update_candidate").length,
    insert_candidates: writePlans.filter((p) => p.action === "insert_candidate").length,
    plans: writePlans,
    held_fields: [
      "coordinates (license_policy_needed)",
      "images (license_policy_needed)",
      "descriptions (license_policy_needed)",
      "facilities (license_policy_needed)",
      "Rooms / Keys (unsupported from rooms[])",
    ],
    never_write: [
      "owner",
      "operator",
      "developer",
      "opening/renovation/affiliation dates",
      "Recent Momentum",
      "Company Validated",
      "Brand Verified",
      "Brand Status",
      "release fields",
    ],
  };
  writeJson(path.join(reportsDir, "hbx-cala-wave1-write-plan.json"), planDoc);
  writeMd(
    path.join(reportsDir, "hbx-cala-wave1-write-plan.md"),
    `# HBX CALA Wave 1 Write Plan (Dry-Run)

**Generated:** ${planDoc.generated_at}
**Plan rows:** ${planDoc.plan_count}
**Update candidates:** ${planDoc.update_candidates}
**Insert candidates:** ${planDoc.insert_candidates}
**Airtable writes this run:** 0

## Held

${planDoc.held_fields.map((x) => `- ${x}`).join("\n")}

## Never write

${planDoc.never_write.map((x) => `- ${x}`).join("\n")}

## Sample update plans (first 15)

${writePlans
  .filter((p) => p.action === "update_candidate")
  .slice(0, 15)
  .map(
    (p) =>
      `- census \`${p.census_record_id}\` ← HBX ${p.hbx_hotel_code}: ${(p.fields || [])
        .map((f) => `${f.field}[${f.license}]`)
        .join(", ")}`
  )
  .join("\n") || "- (none)"}

## Sample insert candidates (first 15)

${writePlans
  .filter((p) => p.action === "insert_candidate")
  .slice(0, 15)
  .map(
    (p) =>
      `- HBX ${p.hbx_hotel_code}: ${p.proposed_fields?.["Property Name"]} / ${p.proposed_fields?.Country} (Census Only / Hold)`
  )
  .join("\n") || "- (none)"}

Full JSON: \`reports/research-engine-v2/hbx-cala-wave1-write-plan.json\`
`
  );
}
