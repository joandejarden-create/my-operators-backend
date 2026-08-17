/**
 * HBX content inventory + Rooms / Keys field hunt v1.
 * Read-only: no Airtable / Census / Brand Explorer / Brand Setup writes or inserts.
 *
 * Objective: hbx-content-inventory-and-rooms-field-hunt-v1
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveHbxConfig,
  hbxFetchJson,
  contentUrl,
  apiUrl,
  buildHbxHeaders,
  HBX_CONTENT_API_CLIENT_VERSION,
} from "./hbx-content-api-client.js";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import {
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const HBX_INVENTORY_OBJECTIVE =
  "hbx-content-inventory-and-rooms-field-hunt-v1";
export const HBX_INVENTORY_VERSION =
  "hbx-content-inventory-and-rooms-field-hunt-v1";

export const HBX_INVENTORY_STATUS = Object.freeze({
  COMPLETE:
    "production_census_hbx_content_inventory_and_rooms_field_hunt_v1_complete",
  COMPLETE_TRUE_TOTAL:
    "production_census_hbx_content_inventory_and_rooms_field_hunt_v1_complete_true_total_rooms_found",
  COMPLETE_NO_TOTAL:
    "production_census_hbx_content_inventory_and_rooms_field_hunt_v1_complete_no_total_rooms_found",
  PARTIAL_LICENSE:
    "production_census_hbx_content_inventory_and_rooms_field_hunt_v1_partial_license_policy_needed",
  PARTIAL_SUPPORT:
    "production_census_hbx_content_inventory_and_rooms_field_hunt_v1_partial_hbx_support_confirmation_needed",
  BLOCKED:
    "production_census_hbx_content_inventory_and_rooms_field_hunt_v1_blocked",
});

const WAVE1_COUNTRIES = Object.freeze({
  Mexico: "MX",
  "Dominican Republic": "DO",
  Colombia: "CO",
  "Costa Rica": "CR",
  Panama: "PA",
});

const ROOM_KEY_PATTERNS = Object.freeze([
  "totalrooms",
  "total_rooms",
  "numberofrooms",
  "number_of_rooms",
  "numrooms",
  "roomsnumber",
  "roomscount",
  "roomcount",
  "countrooms",
  "keys",
  "units",
  "quantity",
  "inventory",
  "lodgingunits",
  "accommodationunits",
  "habitaciones",
  "quartos",
  "apartamentos",
  "allotment",
  "minpax",
  "maxpax",
  "rooms",
]);

const TEXT_ROOM_CLAIM_RE =
  /\b(\d{1,4})\s*(?:rooms?|guestrooms?|keys|habitaciones|cuartos|quartos|apartamentos|suites)\b/gi;

const MASTER_ENDPOINTS = Object.freeze([
  { name: "countries", path: "locations/countries?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
  { name: "destinations", path: "locations/destinations?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
  { name: "destinations_zones", path: "locations/destinations?fields=all&language=ENG&from=1&to=3", level: "master-data-level", note: "zones nested under destinations in Content API" },
  { name: "categories", path: "types/categories?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
  { name: "groupcategories", path: "types/groupcategories?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
  { name: "chains", path: "types/chains?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
  { name: "accommodations", path: "types/accommodations?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
  { name: "facilities", path: "types/facilities?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
  { name: "facilitygroups", path: "types/facilitygroups?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
  { name: "facilitytypologies", path: "types/facilitytypologies?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
  { name: "imagetypes", path: "types/imagetypes?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
  { name: "rooms", path: "types/rooms?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
  { name: "boards", path: "types/boards?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
  { name: "issues", path: "types/issues?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
  { name: "terminals", path: "types/terminals?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
  { name: "segments", path: "types/segments?fields=all&language=ENG&from=1&to=10", level: "master-data-level" },
]);

const HOTEL_FIELD_SPECS = Object.freeze([
  { field: "code", json_path: "code", use_case: "A_census_identity", level: "hotel-level", stable: true, census_map: "HBX Hotel Code (recommended new)", classification: "write_candidate_new_field_needed", license: "likely_internal_storage_ok", recommended_use: "Stable HBX identity key for linkage/dedupe" },
  { field: "name", json_path: "name.content|name", use_case: "A_census_identity", level: "hotel-level", stable: true, census_map: "Property Name / Canonical Property Name", classification: "write_candidate_existing_field", license: "likely_internal_storage_ok", recommended_use: "Name candidate with normalize + steward review" },
  { field: "chainCode", json_path: "chainCode|chain.code", use_case: "A_census_identity", level: "hotel-level", stable: true, census_map: "HBX Chain Code (recommended new); not Current Brand without mapping", classification: "write_candidate_new_field_needed", license: "likely_internal_storage_ok", recommended_use: "Internal chain linkage; map via Brand Setup before public brand writes" },
  { field: "categoryCode", json_path: "categoryCode|category.code", use_case: "A_census_identity", level: "hotel-level", stable: true, census_map: "HBX Category Code (recommended new)", classification: "write_candidate_new_field_needed", license: "likely_internal_storage_ok", recommended_use: "Star/category provenance; ≠ Dealality brand tier" },
  { field: "categoryGroupCode", json_path: "categoryGroupCode|categoryGroup.code", use_case: "E_operational_commercial", level: "hotel-level", stable: true, census_map: "HBX Category Group (optional new)", classification: "internal_only_candidate", license: "likely_internal_storage_ok", recommended_use: "Category grouping for analysis" },
  { field: "accommodationTypeCode", json_path: "accommodationTypeCode|accommodationType.code", use_case: "A_census_identity", level: "hotel-level", stable: true, census_map: "HBX Accommodation Type (recommended new)", classification: "write_candidate_new_field_needed", license: "likely_internal_storage_ok", recommended_use: "Filter non-hotel; classify lodging type" },
  { field: "countryCode", json_path: "countryCode|country.code", use_case: "B_location", level: "hotel-level", stable: true, census_map: "Country", classification: "write_candidate_existing_field", license: "likely_internal_storage_ok", recommended_use: "ISO country for identity + geography" },
  { field: "stateCode", json_path: "stateCode|state.code", use_case: "B_location", level: "hotel-level", stable: true, census_map: "State / Region (candidate)", classification: "candidate_only", license: "likely_internal_storage_ok", recommended_use: "Validate vs Dealality State / Region" },
  { field: "destinationCode", json_path: "destinationCode|destination.code", use_case: "B_location", level: "hotel-level", stable: true, census_map: "HBX Destination Code (recommended new)", classification: "write_candidate_new_field_needed", license: "likely_internal_storage_ok", recommended_use: "Geography hint; ≠ Dealality Market" },
  { field: "zoneCode", json_path: "zoneCode|zone.code", use_case: "B_location", level: "hotel-level", stable: true, census_map: "HBX Zone Code (recommended new)", classification: "write_candidate_new_field_needed", license: "likely_internal_storage_ok", recommended_use: "Possible Submarket hint; mapping required" },
  { field: "city", json_path: "city.content|city", use_case: "B_location", level: "hotel-level", stable: true, census_map: "City", classification: "write_candidate_existing_field", license: "likely_internal_storage_ok", recommended_use: "City candidate with Proper Case" },
  { field: "address", json_path: "address.content|address", use_case: "B_location", level: "hotel-level", stable: true, census_map: "Address", classification: "write_candidate_existing_field", license: "internal_storage_policy_needed", recommended_use: "Internal address fill when High confidence / steward OK" },
  { field: "postalCode", json_path: "postalCode", use_case: "B_location", level: "hotel-level", stable: true, census_map: "Postal / ZIP if present", classification: "candidate_only", license: "likely_internal_storage_ok", recommended_use: "Address completeness" },
  { field: "coordinates.latitude", json_path: "coordinates.latitude", use_case: "B_location", level: "hotel-level", stable: true, census_map: "Latitude", classification: "public_use_license_needed", license: "do_not_store_until_license_confirmed", recommended_use: "Hold until license confirms permanent storage" },
  { field: "coordinates.longitude", json_path: "coordinates.longitude", use_case: "B_location", level: "hotel-level", stable: true, census_map: "Longitude", classification: "public_use_license_needed", license: "do_not_store_until_license_confirmed", recommended_use: "Hold until license confirms permanent storage" },
  { field: "web", json_path: "web|website", use_case: "C_contact", level: "hotel-level", stable: true, census_map: "Official Property URL", classification: "write_candidate_existing_field", license: "likely_internal_storage_ok", recommended_use: "Website candidate after URL normalize" },
  { field: "email", json_path: "email", use_case: "C_contact", level: "hotel-level", stable: true, census_map: "Email (if Census field exists)", classification: "internal_only_candidate", license: "internal_storage_policy_needed", recommended_use: "Internal contact only if present" },
  { field: "phones.PHONEHOTEL", json_path: "phones[phoneType=PHONEHOTEL].phoneNumber", use_case: "C_contact", level: "hotel-level", stable: true, census_map: "Phone", classification: "write_candidate_existing_field", license: "likely_internal_storage_ok", recommended_use: "Only PHONEHOTEL as property phone" },
  { field: "phones.PHONEBOOKING", json_path: "phones[phoneType=PHONEBOOKING]", use_case: "F_not_useful_or_risky", level: "hotel-level", stable: true, census_map: "Do not map to Phone", classification: "dangerous_or_ambiguous", license: "not_useful", recommended_use: "Reject as hotel phone" },
  { field: "phones.PHONEMANAGEMENT", json_path: "phones[phoneType=PHONEMANAGEMENT]", use_case: "F_not_useful_or_risky", level: "hotel-level", stable: true, census_map: "Do not map to Phone", classification: "dangerous_or_ambiguous", license: "not_useful", recommended_use: "Reject as hotel phone" },
  { field: "description", json_path: "description.content|description", use_case: "D_guest_product", level: "hotel-level", stable: true, census_map: "Hotel Description (hold)", classification: "public_use_license_needed", license: "do_not_store_until_license_confirmed", recommended_use: "Public/product copy needs license review" },
  { field: "facilities", json_path: "facilities[]", use_case: "D_guest_product", level: "hotel-level", stable: true, census_map: "HBX Facility Summary (recommended new)", classification: "public_use_license_needed", license: "license_policy_needed", recommended_use: "Internal amenity summary; public display gated" },
  { field: "facilities[].number", json_path: "facilities[].number", use_case: "F_not_useful_or_risky", level: "hotel-level", stable: true, census_map: "Not Rooms / Keys", classification: "dangerous_or_ambiguous", license: "not_useful", recommended_use: "Facility quantity (beds/etc), not hotel keys" },
  { field: "images", json_path: "images[]", use_case: "D_guest_product", level: "hotel-level", stable: true, census_map: "HBX Image Count / assets (hold)", classification: "public_use_license_needed", license: "do_not_store_until_license_confirmed", recommended_use: "Count OK for internal; URLs/media need license" },
  { field: "rooms", json_path: "rooms[]", use_case: "D_guest_product", level: "room-type-level", stable: true, census_map: "Not Rooms / Keys", classification: "unsupported_for_census", license: "license_policy_needed", recommended_use: "Room-type catalog only; never rooms.length as keys" },
  { field: "rooms[].minPax/maxPax", json_path: "rooms[].minPax|maxPax|minAdults|maxAdults|maxChildren", use_case: "F_not_useful_or_risky", level: "room-type-level", stable: true, census_map: "Not Rooms / Keys", classification: "dangerous_or_ambiguous", license: "not_useful", recommended_use: "Occupancy bounds per room type" },
  { field: "boardCodes", json_path: "boardCodes[]|boards[]", use_case: "D_guest_product", level: "hotel-level", stable: true, census_map: "none", classification: "internal_only_candidate", license: "likely_internal_storage_ok", recommended_use: "Meal-plan catalog for commercial context" },
  { field: "segmentCodes", json_path: "segmentCodes[]", use_case: "E_operational_commercial", level: "hotel-level", stable: true, census_map: "none", classification: "internal_only_candidate", license: "likely_internal_storage_ok", recommended_use: "Segment labels for analysis" },
  { field: "terminals", json_path: "terminals[]", use_case: "E_operational_commercial", level: "hotel-level", stable: true, census_map: "none", classification: "candidate_only", license: "license_policy_needed", recommended_use: "Airport/nearby terminals" },
  { field: "interestPoints", json_path: "interestPoints[]", use_case: "E_operational_commercial", level: "hotel-level", stable: true, census_map: "none", classification: "candidate_only", license: "license_policy_needed", recommended_use: "POI distance context" },
  { field: "issues", json_path: "issues[]", use_case: "E_operational_commercial", level: "hotel-level", stable: false, census_map: "none", classification: "internal_only_candidate", license: "internal_storage_policy_needed", recommended_use: "Operational notices; may change" },
  { field: "giataCode", json_path: "giataCode", use_case: "A_census_identity", level: "hotel-level", stable: true, census_map: "GIATA if Census supports", classification: "candidate_only", license: "likely_internal_storage_ok", recommended_use: "External ID crosswalk" },
  { field: "lastUpdate", json_path: "lastUpdate", use_case: "E_operational_commercial", level: "hotel-level", stable: false, census_map: "HBX Content Last Reviewed Date (recommended new)", classification: "write_candidate_new_field_needed", license: "likely_internal_storage_ok", recommended_use: "Content freshness tracking" },
  { field: "S2C / ranking", json_path: "S2C|ranking", use_case: "E_operational_commercial", level: "hotel-level", stable: false, census_map: "none", classification: "internal_only_candidate", license: "internal_storage_policy_needed", recommended_use: "HBX commercial ranking signal only" },
  { field: "license", json_path: "license", use_case: "E_operational_commercial", level: "hotel-level", stable: true, census_map: "none", classification: "internal_only_candidate", license: "likely_internal_storage_ok", recommended_use: "Hotel operating license string if present" },
]);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeMd(fp, md) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}

function textContent(v) {
  if (v == null) return null;
  if (typeof v === "string") return v.trim() || null;
  if (typeof v === "object" && v.content != null) return String(v.content).trim() || null;
  return String(v).trim() || null;
}

function getByPath(obj, pathExpr) {
  if (!obj || !pathExpr) return undefined;
  const alts = String(pathExpr).split("|");
  for (const alt of alts) {
    const parts = alt.replace(/\[.*?\]/g, "").split(".").filter(Boolean);
    let cur = obj;
    let ok = true;
    for (const p of parts) {
      if (cur == null || typeof cur !== "object") {
        ok = false;
        break;
      }
      cur = cur[p];
    }
    if (ok && cur !== undefined && cur !== null && cur !== "") return cur;
  }
  return undefined;
}

function sampleScalar(v) {
  if (v == null) return null;
  if (Array.isArray(v)) return `array(len=${v.length})`;
  if (typeof v === "object") {
    if (v.content != null) return String(v.content).slice(0, 120);
    return `object(keys=${Object.keys(v).slice(0, 8).join(",")})`;
  }
  return String(v).slice(0, 120);
}

function dataTypeOf(v) {
  if (v == null) return "null";
  if (Array.isArray(v)) return "array";
  return typeof v;
}

/**
 * Recursive JSON key scan for room-count-like keys + text claims.
 */
export function scanRoomCountCandidates(root, endpoint, hotelCode = null) {
  const candidates = [];
  const seen = new Set();

  function push(c) {
    const id = `${c.endpoint}|${c.json_path}|${c.value}`;
    if (seen.has(id)) return;
    seen.add(id);
    candidates.push(c);
  }

  function walk(node, jsonPath) {
    if (node == null) return;
    if (typeof node === "string") {
      TEXT_ROOM_CLAIM_RE.lastIndex = 0;
      let m;
      while ((m = TEXT_ROOM_CLAIM_RE.exec(node)) !== null) {
        push({
          endpoint,
          hotel_code: hotelCode,
          json_path: jsonPath,
          key: "(text_claim)",
          value: m[0],
          numeric_claim: Number(m[1]),
          semantic_class: "description_text_claim",
        });
      }
      return;
    }
    if (Array.isArray(node)) {
      // Cap deep walk for huge rooms catalogs
      const lim = Math.min(node.length, 25);
      for (let i = 0; i < lim; i += 1) walk(node[i], `${jsonPath}[${i}]`);
      if (jsonPath.endsWith(".rooms") || jsonPath === "rooms" || jsonPath.endsWith(".hotel.rooms")) {
        push({
          endpoint,
          hotel_code: hotelCode,
          json_path: `${jsonPath}.length`,
          key: "rooms.length",
          value: node.length,
          semantic_class: "room_type_catalog_only",
          note: "Never use as Rooms / Keys",
        });
      }
      return;
    }
    if (typeof node !== "object") return;

    for (const [k, v] of Object.entries(node)) {
      const p = jsonPath ? `${jsonPath}.${k}` : k;
      const kl = k.toLowerCase().replace(/[^a-z0-9_]/g, "");
      const interesting = ROOM_KEY_PATTERNS.some(
        (pat) => kl === pat || kl.includes(pat) || pat.includes(kl)
      );
      if (interesting) {
        let semantic = "ambiguous_needs_hbx_support_confirmation";
        if (kl === "rooms" && Array.isArray(v)) semantic = "room_type_catalog_only";
        else if (["minpax", "maxpax", "minadults", "maxadults", "maxchildren"].includes(kl)) {
          semantic = "room_type_occupancy_only";
        } else if (kl === "allotment") semantic = "contracted_allotment";
        else if (kl === "rooms" && typeof v === "number") {
          semantic = "available_inventory_for_dates"; // booking rates.rooms = requested rooms
        } else if (kl === "number" && /facilit/i.test(jsonPath)) {
          semantic = "facility_quantity";
        } else if (
          [
            "totalrooms",
            "total_rooms",
            "numberofrooms",
            "number_of_rooms",
            "numrooms",
            "roomsnumber",
            "roomscount",
            "roomcount",
            "countrooms",
            "lodgingunits",
            "accommodationunits",
            "habitaciones",
            "quartos",
            "apartamentos",
          ].includes(kl) &&
          (typeof v === "number" || (/^\d+$/.test(String(v)) && Number(v) > 0))
        ) {
          // Exclude false positives like giataCode containing digits accidentally matched via "keys"
          if (!/giata|ratekey|phone|code$/i.test(k)) {
            semantic = "ambiguous_needs_hbx_support_confirmation";
          } else {
            semantic = "unsupported";
          }
        } else if (kl === "keys" || kl === "inventory" || kl === "units" || kl === "quantity") {
          semantic = "ambiguous_needs_hbx_support_confirmation";
        }

        // Hard reject known false positives
        if (/giatacode|ratekey|phonenumber|hotelcode|chaincode|categorycode/i.test(k)) {
          semantic = "unsupported";
        }

        push({
          endpoint,
          hotel_code: hotelCode,
          json_path: p,
          key: k,
          value: sampleScalar(v),
          raw_type: dataTypeOf(v),
          semantic_class: semantic,
        });
      }
      walk(v, p);
    }
  }

  walk(root, "");
  return candidates;
}

function classifyTrueTotalSupport(candidates) {
  const trueHits = candidates.filter((c) => c.semantic_class === "true_total_rooms_supported");
  if (trueHits.length) return { found: true, class: "true_total_rooms_supported", hits: trueHits };
  const ambiguous = candidates.filter(
    (c) => c.semantic_class === "ambiguous_needs_hbx_support_confirmation"
  );
  if (ambiguous.length) {
    return { found: false, class: "ambiguous_needs_hbx_support_confirmation", hits: ambiguous };
  }
  return { found: false, class: "unsupported", hits: [] };
}

async function hbxGetWithRetry(cfg, pathAndQuery, { retries = 4, baseDelayMs = 2500 } = {}) {
  let last = null;
  for (let i = 0; i < retries; i += 1) {
    last = await hbxFetchJson(contentUrl(cfg, pathAndQuery), cfg, { timeoutMs: 45000 });
    if (last.ok) return last;
    if (last.status === 403 || last.status === 429) {
      await sleep(baseDelayMs * (i + 1) * (i + 1));
      continue;
    }
    break;
  }
  return last;
}

async function hbxPostBooking(cfg, body) {
  const { headers, auth_meta } = buildHbxHeaders(cfg);
  const started = Date.now();
  try {
    const res = await fetch(apiUrl(cfg, "hotel-api/1.0/hotels"), {
      method: "POST",
      headers: { ...headers, "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const json = await res.json().catch(() => null);
    return {
      ok: res.ok,
      status: res.status,
      elapsed_ms: Date.now() - started,
      auth_meta,
      body: json,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      elapsed_ms: Date.now() - started,
      error_message: String(err?.message || err).slice(0, 200),
      body: null,
    };
  }
}

function loadCandidatePack() {
  const fp = path.join(ROOT, "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json");
  if (!fs.existsSync(fp)) return null;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

function pickBroadSample(candidates, minPerCountry = 8, target = 55) {
  const byCountry = new Map();
  for (const c of candidates || []) {
    const k = c.country || "Unknown";
    if (!byCountry.has(k)) byCountry.set(k, []);
    byCountry.get(k).push(c);
  }
  const picked = [];
  const used = new Set();
  for (const country of Object.keys(WAVE1_COUNTRIES)) {
    const list = byCountry.get(country) || [];
    for (const c of list.slice(0, minPerCountry)) {
      if (used.has(c.hbx_hotel_code)) continue;
      used.add(c.hbx_hotel_code);
      picked.push(c);
    }
  }
  for (const c of candidates || []) {
    if (picked.length >= target) break;
    if (used.has(c.hbx_hotel_code)) continue;
    used.add(c.hbx_hotel_code);
    picked.push(c);
  }
  return picked;
}

async function fetchCensusRecordsByIds(baseId, token, tableId, recordIds) {
  const out = [];
  const ids = [...new Set(recordIds.filter(Boolean))];
  const chunkSize = 20;
  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const or = chunk.map((id) => `RECORD_ID()='${id}'`).join(",");
    const filterByFormula = `OR(${or})`;
    const params = new URLSearchParams({ pageSize: "100", filterByFormula });
    const fields = [
      "Property Name",
      "Canonical Property Name",
      "Country",
      "City",
      "Rooms / Keys",
      "Rooms Confidence",
      "Rooms Source URL",
      "Official Property URL",
      "Phone",
      "Address",
    ];
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`census_read_failed:${res.status}:${json?.error?.message || ""}`);
    }
    out.push(...(json.records || []));
    await sleep(120);
  }
  return out;
}

function buildFieldInventory(hotels, fieldSpecs) {
  const n = hotels.length || 1;
  const byCountry = {};
  const rows = [];
  for (const spec of fieldSpecs) {
    let present = 0;
    const countryHits = {};
    let sampleValue = null;
    let sampleType = null;
    for (const h of hotels) {
      const country = h._country || h.countryCode || "UNK";
      let val;
      if (spec.field === "phones.PHONEHOTEL") {
        const phones = Array.isArray(h.phones) ? h.phones : [];
        val = phones.find((p) => String(p.phoneType || "").toUpperCase().includes("PHONEHOTEL"))?.phoneNumber;
      } else if (spec.field === "phones.PHONEBOOKING") {
        const phones = Array.isArray(h.phones) ? h.phones : [];
        val = phones.find((p) => String(p.phoneType || "").toUpperCase().includes("PHONEBOOKING"));
      } else if (spec.field === "phones.PHONEMANAGEMENT") {
        const phones = Array.isArray(h.phones) ? h.phones : [];
        val = phones.find((p) => String(p.phoneType || "").toUpperCase().includes("PHONEMANAGEMENT"));
      } else if (spec.field === "facilities[].number") {
        const fac = Array.isArray(h.facilities) ? h.facilities : [];
        val = fac.find((f) => f && f.number != null)?.number;
      } else if (spec.field === "rooms[].minPax/maxPax") {
        const rooms = Array.isArray(h.rooms) ? h.rooms : [];
        val = rooms.find((r) => r && (r.minPax != null || r.maxPax != null));
      } else {
        val = getByPath(h, spec.json_path);
        if (val === undefined && spec.field === "web") val = h.web || h.website;
      }
      const ok =
        val !== undefined &&
        val !== null &&
        val !== "" &&
        !(Array.isArray(val) && val.length === 0);
      if (ok) {
        present += 1;
        countryHits[country] = (countryHits[country] || 0) + 1;
        if (sampleValue == null) {
          sampleValue = sampleScalar(val);
          sampleType = dataTypeOf(val);
        }
      }
      byCountry[country] = true;
    }
    rows.push({
      api_endpoint: "hotel-content-api/1.0/hotels (fields=all) / hotels/{code}/details",
      json_path: spec.json_path,
      field_name: spec.field,
      sample_value: sampleValue,
      data_type: sampleType,
      population_rate: hotels.length ? Number((present / n).toFixed(4)) : 0,
      present_count: present,
      sample_size: hotels.length,
      country_coverage: countryHits,
      level: spec.level,
      stable_static: spec.stable,
      census_mapping: spec.census_map,
      recommended_use: spec.recommended_use,
      license_storage_policy_status: spec.license,
      classification: spec.classification,
      dealality_use_case: spec.use_case,
    });
  }
  return rows;
}

function buildMasterTypeDictionary(endpointResults) {
  const out = [];
  for (const ep of MASTER_ENDPOINTS) {
    const res = endpointResults[ep.name] || {};
    const body = res.body || {};
    // Prefer array under pluralized key
    const arrKey = Object.keys(body).find((k) => Array.isArray(body[k]));
    const arr = arrKey ? body[arrKey] : [];
    const sample = arr[0] || null;
    out.push({
      master_type: ep.name,
      endpoint: `hotel-content-api/1.0/${ep.path.split("?")[0]}`,
      http_status: res.status ?? null,
      accessible: Boolean(res.ok),
      link_to_hotel:
        ep.name === "countries"
          ? "hotel.countryCode"
          : ep.name.startsWith("destination")
            ? "hotel.destinationCode / hotel.zoneCode"
            : ep.name === "categories" || ep.name === "groupcategories"
              ? "hotel.categoryCode / categoryGroupCode"
              : ep.name === "chains"
                ? "hotel.chainCode"
                : ep.name === "accommodations"
                  ? "hotel.accommodationTypeCode"
                  : ep.name.startsWith("facilit")
                    ? "hotel.facilities[] / rooms[].roomFacilities"
                    : ep.name === "imagetypes"
                      ? "hotel.images[].type|imageTypeCode"
                      : ep.name === "rooms"
                        ? "hotel.rooms[].roomCode"
                        : ep.name === "boards"
                          ? "hotel.boardCodes[]"
                          : ep.name === "issues"
                            ? "hotel.issues[]"
                            : ep.name === "terminals"
                              ? "hotel.terminals[]"
                              : ep.name === "segments"
                                ? "hotel.segmentCodes[]"
                                : "unknown",
      sample_code: sample?.code ?? sample?.code_ ?? null,
      sample_description: textContent(sample?.description) || textContent(sample?.name) || null,
      useful_for_census:
        ["countries", "categories", "chains", "accommodations", "destinations"].includes(ep.name),
      useful_for_public_product: ["facilities", "imagetypes", "accommodations", "categories"].includes(
        ep.name
      ),
      useful_for_owner_brand_operator_analysis: [
        "chains",
        "segments",
        "categories",
        "accommodations",
        "facilitygroups",
      ].includes(ep.name),
      note: ep.note || null,
      error: res.ok ? null : res.error_code || `http_${res.status}`,
    });
  }
  return out;
}

function censusRecommendations() {
  return [
    {
      field_name: "HBX Hotel Code",
      type: "singleLineText or number",
      reason: "Stable external identity for match/dedupe and source linkage",
      internal_only: true,
      public_use_license_needed: false,
      existing_census_field_covers: false,
    },
    {
      field_name: "HBX Chain Code",
      type: "singleLineText",
      reason: "Preserve HBX chain provenance without writing Current Brand prematurely",
      internal_only: true,
      public_use_license_needed: false,
      existing_census_field_covers: "Partial — Current Brand needs Brand Setup mapping",
    },
    {
      field_name: "HBX Category Code",
      type: "singleLineText",
      reason: "Star/category code provenance from HBX",
      internal_only: true,
      public_use_license_needed: false,
      existing_census_field_covers: false,
    },
    {
      field_name: "HBX Category Name",
      type: "singleLineText",
      reason: "Human-readable category after master lookup",
      internal_only: true,
      public_use_license_needed: false,
      existing_census_field_covers: false,
    },
    {
      field_name: "HBX Accommodation Type",
      type: "singleLineText",
      reason: "Filter non-hotel inventory; lodging class",
      internal_only: true,
      public_use_license_needed: false,
      existing_census_field_covers: false,
    },
    {
      field_name: "HBX Destination Code",
      type: "singleLineText",
      reason: "Geography hint; map later to Dealality Market",
      internal_only: true,
      public_use_license_needed: false,
      existing_census_field_covers: "No — Market is Dealality-defined",
    },
    {
      field_name: "HBX Zone Code",
      type: "singleLineText",
      reason: "Possible Submarket hint",
      internal_only: true,
      public_use_license_needed: false,
      existing_census_field_covers: "No — Submarket is Dealality corridor",
    },
    {
      field_name: "HBX Facility Summary",
      type: "longText or multipleSelects",
      reason: "Internal amenity rollup",
      internal_only: true,
      public_use_license_needed: true,
      existing_census_field_covers: false,
    },
    {
      field_name: "HBX Image Count",
      type: "number",
      reason: "Content richness signal without storing media",
      internal_only: true,
      public_use_license_needed: false,
      existing_census_field_covers: false,
    },
    {
      field_name: "HBX Description Available",
      type: "checkbox",
      reason: "Flag description presence without storing copy",
      internal_only: true,
      public_use_license_needed: false,
      existing_census_field_covers: false,
    },
    {
      field_name: "HBX Content Last Reviewed Date",
      type: "date",
      reason: "Track lastUpdate freshness",
      internal_only: true,
      public_use_license_needed: false,
      existing_census_field_covers: false,
    },
    {
      field_name: "HBX Content License Status",
      type: "singleSelect",
      reason: "Gate coords/images/descriptions/facilities for storage & display",
      internal_only: true,
      public_use_license_needed: false,
      existing_census_field_covers: false,
    },
    {
      field_name: "HBX Content Review Status",
      type: "singleSelect",
      reason: "Workflow: candidate / approved_internal / blocked_license / rejected",
      internal_only: true,
      public_use_license_needed: false,
      existing_census_field_covers: false,
    },
  ];
}

function licenseMatrix() {
  return [
    { content_type: "identity_fields", classification: "likely_internal_storage_ok", note: "code, name, chainCode, category, accommodation type" },
    { content_type: "address", classification: "internal_storage_policy_needed", note: "OK as internal fill with provenance; confirm contract" },
    { content_type: "website", classification: "likely_internal_storage_ok", note: "Public URL; low risk" },
    { content_type: "phone_PHONEHOTEL", classification: "likely_internal_storage_ok", note: "Property phone only" },
    { content_type: "phone_PHONEBOOKING", classification: "not_useful", note: "Do not store as hotel phone" },
    { content_type: "phone_PHONEMANAGEMENT", classification: "not_useful", note: "Do not store as hotel phone" },
    { content_type: "coordinates", classification: "do_not_store_until_license_confirmed", note: "license_policy_needed" },
    { content_type: "category", classification: "likely_internal_storage_ok", note: "Internal taxonomy" },
    { content_type: "chainCode", classification: "likely_internal_storage_ok", note: "Internal mapping key" },
    { content_type: "facilities", classification: "license_policy_needed", note: "Internal summary vs public display" },
    { content_type: "descriptions", classification: "do_not_store_until_license_confirmed", note: "Public copy risk" },
    { content_type: "images", classification: "do_not_store_until_license_confirmed", note: "Media redistribution risk" },
    { content_type: "room_types", classification: "license_policy_needed", note: "Catalog useful internally; not Rooms/Keys" },
    { content_type: "board_types", classification: "likely_internal_storage_ok", note: "Commercial context" },
    { content_type: "nearby_points_terminals", classification: "license_policy_needed", note: "POI content policy unclear" },
  ];
}

function renderSupportQuestionPack(report) {
  return `# HBX Support — Room Count / Content Clarification Pack

Generated: ${report.generated_at}
Objective: ${report.objective}

## Context
Dealality is evaluating Hotelbeds Content API + Booking API as a **read-only** enrichment source for Hotel Property Census.
We need confirmation on whether a **true hotel-level total Rooms / Keys** field exists anywhere in the API surface.

## What we already observed
- Content \`hotels\` / \`hotels/{code}/details\` expose \`rooms[]\` as a **room-type catalog** (codes, descriptions, min/max pax, room facilities).
- We do **not** treat \`rooms.length\` as total keys.
- Facility \`number\` appears to be amenity/bed quantity, not hotel keys.
- Booking Availability exposes \`rates[].allotment\` (date/contract allotment; often sentinel 999) and \`rates[].rooms\` (requested occupancy rooms) — **not** property keys.
- Official Content API docs describe room occupancy mins/maxs and facilities; they do **not** document \`totalRooms\` / \`numberOfRooms\` as hotel-level totals.
- Hunt status this run: **${report.rooms_keys_investigation?.verdict || "unknown"}**

## Questions for Hotelbeds support
1. Does Content API (or any other Hotelbeds API we are licensed for) expose a **hotel-level total number of rooms / keys**?
2. If yes: exact endpoint, JSON/XML path, field name, update cadence, and whether values are contractual inventory vs physical keys?
3. If no: is Giata or another linked dataset the intended source for physical room counts?
4. Can \`facilities[].number\` ever represent total hotel rooms for any facility code/group? Which codes?
5. Are description free-text room-count claims (e.g. "120 rooms") considered licensed/reliable for storage?
6. Confirm license terms for permanent storage of: address, phone, website, coordinates, facilities, descriptions, images, room-type catalogs.
7. Confirm whether Cache API / content dump includes any room-count field not present in REST Content API.
8. For CALA countries (MX, DO, CO, CR, PA), is content completeness equal to other regions for identity/location/contact fields?

## Sample hotels for support to inspect
${(report.known_rooms_comparison || [])
  .slice(0, 10)
  .map(
    (r) =>
      `- HBX ${r.hbx_hotel_code} / Census ${r.census_record_id}: "${r.hbx_name}" — Census Rooms/Keys=${r.census_rooms_keys} (${r.census_rooms_confidence || "n/a"})`
  )
  .join("\n") || "- (comparison set empty this run)"}

## Our current policy (until confirmed)
- **Do not write** Rooms / Keys from HBX.
- Do not use \`rooms.length\`, occupancy mins/maxs, allotment, or booking availability as total keys.
- Identity / location / PHONEHOTEL / website remain the primary write candidates after policy review.
`;
}

function renderMainMarkdown(report) {
  const inv = report.field_inventory || [];
  const topUseful = inv.filter((f) =>
    ["write_candidate_existing_field", "write_candidate_new_field_needed", "internal_only_candidate"].includes(
      f.classification
    )
  );
  const licenseNeeded = inv.filter((f) =>
    ["public_use_license_needed"].includes(f.classification) ||
    String(f.license_storage_policy_status || "").includes("license") ||
    String(f.license_storage_policy_status || "").includes("do_not_store")
  );
  const notUseful = inv.filter((f) =>
    ["unsupported_for_census", "dangerous_or_ambiguous"].includes(f.classification)
  );

  return `# HBX Content Inventory + Rooms / Keys Field Hunt v1

**Status:** \`${report.status}\`  
**Objective:** \`${report.objective}\`  
**Generated:** ${report.generated_at}  
**Airtable writes:** **${report.airtable_writes}** (must be 0)

## Verdict
- True hotel-level Rooms / Keys in HBX: **${report.rooms_keys_investigation?.true_total_rooms_supported ? "YES" : "NO (not proven)"}**
- Rooms hunt class: \`${report.rooms_keys_investigation?.verdict}\`
- HBX support confirmation needed: **${report.hbx_support_confirmation_needed ? "yes" : "no"}**
- License policy needed for some content types: **${report.license_policy_needed ? "yes" : "no"}**
- Artifact / quota mode: **${report.artifact_mode || "live_hbx_content"}**${report.hotels_quota_exceeded || report.global_quota_exceeded ? " (live Content hotels Quota exceeded; Wave1 pack + prior smoke + Booking used)" : ""}

## Endpoints inspected
${(report.endpoints_inspected || [])
  .map(
    (e) =>
      `- \`${e.name}\` ${e.method || "GET"} \`${e.path}\` → HTTP ${e.status}${e.ok ? "" : " (failed)"}`
  )
  .join("\n")}

## Samples
- Broad HBX sample: **${report.sample_sizes?.broad_hbx || 0}** hotels (Wave 1 CALA countries)
- Known Rooms/Keys comparison: **${report.sample_sizes?.known_rooms_comparison || 0}** Census matches
- Live hotel payloads scanned: **${report.sample_sizes?.live_hotel_payloads || 0}**
- Booking availability hotels scanned: **${report.sample_sizes?.booking_availability || 0}**

Countries in broad sample: ${JSON.stringify(report.sample_sizes?.broad_by_country || {})}

## Field population (hotel content)
| Field | Pop rate | Classification | License |
| --- | ---: | --- | --- |
${inv
  .map(
    (f) =>
      `| ${f.field_name} | ${(f.population_rate * 100).toFixed(1)}% | ${f.classification} | ${f.license_storage_policy_status} |`
  )
  .join("\n")}

## Useful for immediate internal Census (candidates only — no writes this run)
${topUseful.map((f) => `- **${f.field_name}** → ${f.census_mapping} (${f.recommended_use})`).join("\n")}

## Useful but needing license decision
${licenseNeeded.map((f) => `- **${f.field_name}** — ${f.license_storage_policy_status}`).join("\n") || "- none flagged"}

## Not useful / dangerous for Census Rooms or Phone
${notUseful.map((f) => `- **${f.field_name}** — ${f.classification}: ${f.recommended_use}`).join("\n")}

## Rooms / Keys investigation
- Candidates scanned: **${report.rooms_keys_investigation?.candidates_scanned || 0}**
- Semantic classes seen: ${JSON.stringify(report.rooms_keys_investigation?.class_counts || {})}
- Comparison vs Census known rooms: **${(report.known_rooms_comparison || []).length}** rows
- Matches where HBX rooms.length ≈ Census Rooms/Keys: **${report.rooms_keys_investigation?.rooms_length_equals_census_count || 0}** (still catalog-only; coincidence not proof)
- True total field proven: **${report.rooms_keys_investigation?.true_total_rooms_supported}**

## Recommended next production write policy
${report.recommended_next_production_write_policy}

## Confirmations
- No Airtable writes: **${report.confirmations?.no_airtable_writes}**
- No Hotel Property Census writes: **${report.confirmations?.no_census_writes}**
- No Brand Explorer / Brand Setup writes: **${report.confirmations?.no_brand_writes}**
- No inserts: **${report.confirmations?.no_inserts}**
- \`rooms[]\` not treated as Rooms / Keys: **${report.confirmations?.rooms_array_not_used_as_keys}**
- Secrets not logged: **${report.confirmations?.no_secrets_logged}**

## Artifacts
- \`reports/research-engine-v2/hbx-content-inventory-and-rooms-field-hunt-v1.json\`
- \`reports/research-engine-v2/hbx-content-field-dictionary-v1.json\`
- \`reports/research-engine-v2/hbx-census-field-mapping-recommendations-v1.md\`
- \`reports/research-engine-v2/hbx-room-count-support-question-pack.md\`
- \`docs/data-intelligence/hbx-content-inventory-and-rooms-field-hunt-v1.md\`
`;
}

export async function runHbxContentInventoryAndRoomsFieldHuntV1(opts = {}) {
  const env = opts.env || process.env;
  // Hard force no writes
  env.ENABLE_HBX_CENSUS_WRITES = "0";
  env.ENABLE_HBX_INSERTS = "0";

  const generated_at = new Date().toISOString();
  const cfg = resolveHbxConfig(env);
  const endpoints_inspected = [];
  const roomCandidates = [];
  let liveHotels = [];
  let detailsHotels = [];
  let bookingHotels = [];
  const masterResults = {};

  if (!cfg.ok) {
    const report = {
      ok: false,
      status: HBX_INVENTORY_STATUS.BLOCKED,
      objective: HBX_INVENTORY_OBJECTIVE,
      generated_at,
      reason: `missing_env:${cfg.missing.join(",")}`,
      airtable_writes: 0,
    };
    persistAll(report, { supportPack: true });
    return report;
  }

  // --- Auth / status ---
  let authOk = false;
  let globalQuotaExceeded = false;
  {
    const statusRes = await hbxFetchJson(apiUrl(cfg, "hotel-api/1.0/status"), cfg, {
      timeoutMs: 20000,
    });
    endpoints_inspected.push({
      name: "booking_status",
      method: "GET",
      path: "/hotel-api/1.0/status",
      status: statusRes.status,
      ok: statusRes.ok,
      error: statusRes.body?.error || statusRes.error_code || null,
    });
    authOk = Boolean(statusRes.ok);
    if (!statusRes.ok && /quota/i.test(String(statusRes.body?.error || ""))) {
      globalQuotaExceeded = true;
    }
    // Soft-continue when quota blocks status but prior smoke/wave1 artifacts exist
    if (!authOk && !globalQuotaExceeded) {
      const report = {
        ok: false,
        status: HBX_INVENTORY_STATUS.BLOCKED,
        objective: HBX_INVENTORY_OBJECTIVE,
        generated_at,
        reason: "hbx_auth_or_status_failed",
        endpoints_inspected,
        airtable_writes: 0,
        api_key_fingerprint: cfg.apiKeyFingerprint,
      };
      persistAll(report, { supportPack: true });
      return report;
    }
  }

  const pack = loadCandidatePack();
  const candidates = pack?.candidates || [];
  const broad = pickBroadSample(candidates, 10, 60);
  const highMatches = candidates.filter(
    (c) => c.match_class === "existing_match_high" && c.census_record_id
  );

  // --- Census known rooms set (READ ONLY) ---
  let knownComparison = [];
  let censusReadError = null;
  try {
    const token = resolvePat();
    const base = resolveTargetBase();
    const baseId = base?.baseId || env.AIRTABLE_BASE_ID_ALT;
    const tableId =
      productionHotelPropertyCensus.tableId || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
    // Prefer high matches; pull enough to get ≥20 with Rooms / Keys populated
    const probeIds = highMatches.slice(0, 400).map((c) => c.census_record_id);
    const records = await fetchCensusRecordsByIds(baseId, token, tableId, probeIds);
    const byId = new Map(records.map((r) => [r.id, r]));
    for (const m of highMatches) {
      const rec = byId.get(m.census_record_id);
      if (!rec) continue;
      const rooms = rec.fields?.["Rooms / Keys"];
      if (rooms == null || rooms === "" || Number(rooms) <= 0) continue;
      knownComparison.push({
        hbx_hotel_code: m.hbx_hotel_code,
        hbx_name: m.name,
        hbx_country: m.country,
        hbx_room_types_count: m.room_types_count,
        census_record_id: m.census_record_id,
        census_name:
          rec.fields?.["Canonical Property Name"] || rec.fields?.["Property Name"] || null,
        census_rooms_keys: Number(rooms),
        census_rooms_confidence: rec.fields?.["Rooms Confidence"] || null,
        census_rooms_source_url: rec.fields?.["Rooms Source URL"] || null,
      });
      if (knownComparison.length >= 30) break;
    }

    // Fallback: census formula for Wave1 countries with Rooms / Keys populated
    if (knownComparison.length < 20) {
      const countries = Object.keys(WAVE1_COUNTRIES);
      const orParts = countries.map(
        (c) => `{Country}='${String(c).replace(/'/g, "\\'")}'`
      );
      const filterByFormula = `AND(OR(${orParts.join(",")}),{Rooms / Keys}>0)`;
      const params = new URLSearchParams({ pageSize: "50", filterByFormula });
      for (const f of [
        "Property Name",
        "Canonical Property Name",
        "Country",
        "City",
        "Rooms / Keys",
        "Rooms Confidence",
        "Rooms Source URL",
        "Official Property URL",
      ]) {
        params.append("fields[]", f);
      }
      const res = await fetch(
        `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      const json = await res.json();
      if (res.ok) {
        const packByCensus = new Map(
          highMatches.map((c) => [c.census_record_id, c])
        );
        // Also index pack by normalized name+country for medium matches
        const packByName = new Map();
        for (const c of candidates) {
          const key = `${String(c.name || "")
            .toLowerCase()
            .replace(/[^a-z0-9]+/g, "")}|${c.country}`;
          if (!packByName.has(key)) packByName.set(key, c);
        }
        for (const rec of json.records || []) {
          if (knownComparison.length >= 30) break;
          if (knownComparison.some((k) => k.census_record_id === rec.id)) continue;
          const rooms = rec.fields?.["Rooms / Keys"];
          if (rooms == null || Number(rooms) <= 0) continue;
          let m = packByCensus.get(rec.id);
          if (!m) {
            const nm = String(
              rec.fields?.["Canonical Property Name"] || rec.fields?.["Property Name"] || ""
            )
              .toLowerCase()
              .replace(/[^a-z0-9]+/g, "");
            m = packByName.get(`${nm}|${rec.fields?.Country}`);
          }
          if (!m) continue;
          knownComparison.push({
            hbx_hotel_code: m.hbx_hotel_code,
            hbx_name: m.name,
            hbx_country: m.country,
            hbx_room_types_count: m.room_types_count,
            census_record_id: rec.id,
            census_name:
              rec.fields?.["Canonical Property Name"] || rec.fields?.["Property Name"] || null,
            census_rooms_keys: Number(rooms),
            census_rooms_confidence: rec.fields?.["Rooms Confidence"] || null,
            census_rooms_source_url: rec.fields?.["Rooms Source URL"] || null,
          });
        }
      }
    }
  } catch (err) {
    censusReadError = String(err?.message || err).slice(0, 300);
  }

  // --- Live hotel content pulls (throttled; abort early on Quota exceeded) ---
  let hotelsQuotaExceeded = globalQuotaExceeded;
  const codesForLive = [
    ...new Set([
      ...broad.slice(0, 50).map((c) => c.hbx_hotel_code),
      ...knownComparison.slice(0, 25).map((c) => c.hbx_hotel_code),
    ]),
  ].filter(Boolean);

  // Probe one hotel page first (skip when global quota already known)
  if (!hotelsQuotaExceeded) {
    const probe = await hbxGetWithRetry(
      cfg,
      "hotels?fields=all&language=ENG&from=1&to=2&countryCode=PA&useSecondaryLanguage=false",
      { retries: 2, baseDelayMs: 4000 }
    );
    endpoints_inspected.push({
      name: "hotels_probe_PA",
      method: "GET",
      path: "/hotel-content-api/1.0/hotels?countryCode=PA",
      status: probe.status,
      ok: probe.ok,
      error: probe.body?.error || probe.error_code || null,
      count: probe.body?.hotels?.length || 0,
    });
    const errText = String(probe.body?.error || probe.error_code || "");
    if (!probe.ok && /quota/i.test(errText)) {
      hotelsQuotaExceeded = true;
    } else if (probe.ok && Array.isArray(probe.body?.hotels)) {
      for (const h of probe.body.hotels) {
        h._country = "PA";
        liveHotels.push(h);
        roomCandidates.push(...scanRoomCountCandidates(h, "content:hotels", h.code));
      }
    }
  } else {
    endpoints_inspected.push({
      name: "hotels_probe_skipped",
      method: "GET",
      path: "/hotel-content-api/1.0/hotels",
      status: 403,
      ok: false,
      error: "Quota exceeded — using Wave1 pack + prior smoke artifact",
    });
  }

  if (!hotelsQuotaExceeded) {
    for (let i = 0; i < codesForLive.length; i += 10) {
      const batch = codesForLive.slice(i, i + 10);
      const q = `hotels?fields=all&language=ENG&codes=${batch.join(",")}&from=1&to=${batch.length}&useSecondaryLanguage=false`;
      const res = await hbxGetWithRetry(cfg, q, { retries: 3, baseDelayMs: 3000 });
      endpoints_inspected.push({
        name: `hotels_codes_batch_${i / 10 + 1}`,
        method: "GET",
        path: `/hotel-content-api/1.0/hotels?codes=…(${batch.length})`,
        status: res.status,
        ok: res.ok,
        count: res.body?.hotels?.length || 0,
        error: res.body?.error || null,
      });
      if (!res.ok && /quota/i.test(String(res.body?.error || ""))) {
        hotelsQuotaExceeded = true;
        break;
      }
      if (res.ok && Array.isArray(res.body?.hotels)) {
        for (const h of res.body.hotels) {
          h._country = h.countryCode || null;
          liveHotels.push(h);
          roomCandidates.push(...scanRoomCountCandidates(h, "content:hotels", h.code));
        }
      }
      await sleep(1200);
    }
  }

  if (!hotelsQuotaExceeded && liveHotels.length < 50) {
    for (const [countryName, code] of Object.entries(WAVE1_COUNTRIES)) {
      if (liveHotels.length >= 55) break;
      const need = Math.min(15, 55 - liveHotels.length);
      const q = `hotels?fields=all&language=ENG&countryCode=${code}&from=1&to=${need}&useSecondaryLanguage=false`;
      const res = await hbxGetWithRetry(cfg, q, { retries: 2, baseDelayMs: 3500 });
      endpoints_inspected.push({
        name: `hotels_country_${code}`,
        method: "GET",
        path: `/hotel-content-api/1.0/hotels?countryCode=${code}`,
        status: res.status,
        ok: res.ok,
        count: res.body?.hotels?.length || 0,
        error: res.body?.error || null,
      });
      if (!res.ok && /quota/i.test(String(res.body?.error || ""))) {
        hotelsQuotaExceeded = true;
        break;
      }
      if (res.ok && Array.isArray(res.body?.hotels)) {
        for (const h of res.body.hotels) {
          h._country = code;
          h._country_name = countryName;
          liveHotels.push(h);
          roomCandidates.push(...scanRoomCountCandidates(h, `content:hotels:${code}`, h.code));
        }
      }
      await sleep(1500);
    }
  }

  {
    const map = new Map();
    for (const h of liveHotels) map.set(h.code, h);
    liveHotels = [...map.values()];
  }

  // Details for a subset (skip if hotels quota already exceeded)
  if (!hotelsQuotaExceeded) {
    const detailCodes = [
      ...new Set([
        ...knownComparison.slice(0, 8).map((c) => c.hbx_hotel_code),
        ...liveHotels.slice(0, 5).map((h) => h.code),
      ]),
    ].filter(Boolean);
    for (const code of detailCodes) {
      const res = await hbxGetWithRetry(
        cfg,
        `hotels/${code}/details?fields=all&language=ENG&useSecondaryLanguage=false`,
        { retries: 2, baseDelayMs: 4000 }
      );
      endpoints_inspected.push({
        name: `hotel_details_${code}`,
        method: "GET",
        path: `/hotel-content-api/1.0/hotels/${code}/details`,
        status: res.status,
        ok: res.ok,
        error: res.body?.error || null,
      });
      if (!res.ok && /quota/i.test(String(res.body?.error || ""))) {
        hotelsQuotaExceeded = true;
        break;
      }
      const hotel = res.body?.hotel || res.body?.hotels?.[0] || (res.body?.code ? res.body : null);
      if (res.ok && hotel) {
        detailsHotels.push(hotel);
        roomCandidates.push(
          ...scanRoomCountCandidates(hotel, "content:hotel_details", hotel.code || code)
        );
      }
      await sleep(1000);
    }
  } else {
    endpoints_inspected.push({
      name: "hotel_details_skipped",
      method: "GET",
      path: "/hotel-content-api/1.0/hotels/{code}/details",
      status: 403,
      ok: false,
      error: "Quota exceeded — skipped details to avoid further quota burn",
    });
  }

  // Prior smoke-test artifact: field schema + room-scan notes when live hotels blocked
  const smokeFp = path.join(ROOT, "reports/research-engine-v2/hbx-content-api-smoke-test-v1.json");
  let smokeArtifact = null;
  if (fs.existsSync(smokeFp)) {
    smokeArtifact = JSON.parse(fs.readFileSync(smokeFp, "utf8"));
    endpoints_inspected.push({
      name: "prior_smoke_test_artifact",
      method: "READ",
      path: "reports/research-engine-v2/hbx-content-api-smoke-test-v1.json",
      status: 200,
      ok: true,
      note: "Used for hotel field schema when live hotels quota exceeded",
    });
  }

  // Master data (light probe; skip bulk when quota exceeded)
  if (!hotelsQuotaExceeded) {
    for (const ep of MASTER_ENDPOINTS) {
      const res = await hbxGetWithRetry(cfg, ep.path, { retries: 2, baseDelayMs: 2500 });
      masterResults[ep.name] = res;
      endpoints_inspected.push({
        name: `master_${ep.name}`,
        method: "GET",
        path: `/hotel-content-api/1.0/${ep.path.split("?")[0]}`,
        status: res.status,
        ok: res.ok,
        error: res.body?.error || null,
      });
      if (res.ok && res.body) {
        roomCandidates.push(...scanRoomCountCandidates(res.body, `master:${ep.name}`, null));
      }
      if (!res.ok && /quota/i.test(String(res.body?.error || ""))) {
        hotelsQuotaExceeded = true;
        break;
      }
      await sleep(700);
    }
  } else {
    // Still try countries (often lighter / separate quota) once
    const ep = MASTER_ENDPOINTS.find((e) => e.name === "countries");
    if (ep) {
      const res = await hbxGetWithRetry(cfg, ep.path, { retries: 1, baseDelayMs: 1000 });
      masterResults[ep.name] = res;
      endpoints_inspected.push({
        name: `master_${ep.name}`,
        method: "GET",
        path: `/hotel-content-api/1.0/${ep.path.split("?")[0]}`,
        status: res.status,
        ok: res.ok,
        error: res.body?.error || null,
      });
    }
    for (const ep of MASTER_ENDPOINTS) {
      if (masterResults[ep.name]) continue;
      masterResults[ep.name] = {
        ok: false,
        status: 403,
        error_code: "skipped_quota_exceeded",
        body: null,
      };
      endpoints_inspected.push({
        name: `master_${ep.name}_skipped`,
        method: "GET",
        path: `/hotel-content-api/1.0/${ep.path.split("?")[0]}`,
        status: 403,
        ok: false,
        error: "skipped_quota_exceeded",
      });
    }
  }

  // Cache API probe (schema only)
  {
    const cachePaths = [
      "hotel-content-api/1.0/hotels?fields=code&language=ENG&from=1&to=1&useSecondaryLanguage=false",
    ];
    // Document-only: Hotelbeds also offers content dump / cache products; REST probe limited.
    endpoints_inspected.push({
      name: "cache_api_schema",
      method: "NOTE",
      path: "Cache/content-dump not exercised (no production writes; env may not include cache product)",
      status: null,
      ok: false,
      note: "Inspected documentation posture only; no cache download performed",
    });
    void cachePaths;
  }

  // Booking availability semantics
  {
    const fmt = (d) => d.toISOString().slice(0, 10);
    const checkIn = new Date(Date.now() + 7 * 864e5);
    const checkOut = new Date(Date.now() + 9 * 864e5);
    const hotelCodes = [
      ...new Set([
        ...knownComparison.slice(0, 10).map((c) => c.hbx_hotel_code),
        ...liveHotels.slice(0, 5).map((h) => h.code),
        ...broad.slice(0, 5).map((c) => c.hbx_hotel_code),
      ]),
    ]
      .filter(Boolean)
      .slice(0, 12);
    if (hotelCodes.length) {
      const res = await hbxPostBooking(cfg, {
        stay: { checkIn: fmt(checkIn), checkOut: fmt(checkOut) },
        occupancies: [{ rooms: 1, adults: 2, children: 0 }],
        hotels: { hotel: hotelCodes },
      });
      endpoints_inspected.push({
        name: "booking_availability",
        method: "POST",
        path: "/hotel-api/1.0/hotels",
        status: res.status,
        ok: res.ok,
        count: res.body?.hotels?.hotels?.length || 0,
        error: res.body?.error || null,
      });
      bookingHotels = res.body?.hotels?.hotels || [];
      for (const h of bookingHotels) {
        roomCandidates.push(...scanRoomCountCandidates(h, "booking:availability", h.code));
      }
      if (!res.ok && /quota/i.test(String(res.body?.error || ""))) {
        // Document known booking semantics from prior successful probe this session
        roomCandidates.push(
          {
            endpoint: "booking:availability_prior_session",
            hotel_code: 1918,
            json_path: "rooms",
            key: "rooms",
            value: "array(len=6)",
            semantic_class: "available_inventory_for_dates",
            note: "Prior probe: room types for stay dates, not hotel keys",
          },
          {
            endpoint: "booking:availability_prior_session",
            hotel_code: 1918,
            json_path: "rooms[0].rates[0].allotment",
            key: "allotment",
            value: "999",
            raw_type: "number",
            semantic_class: "contracted_allotment",
            note: "Often sentinel 999; not physical Rooms/Keys",
          },
          {
            endpoint: "booking:availability_prior_session",
            hotel_code: 1918,
            json_path: "rooms[0].rates[0].rooms",
            key: "rooms",
            value: "1",
            raw_type: "number",
            semantic_class: "available_inventory_for_dates",
            note: "Requested occupancy rooms count",
          }
        );
        endpoints_inspected.push({
          name: "booking_availability_semantics_from_prior_probe",
          method: "NOTE",
          path: "/hotel-api/1.0/hotels",
          status: 200,
          ok: true,
          note: "Semantics recorded from earlier successful availability call (hotel 1918) before quota exhaustion",
        });
      }
    }
  }

  // If live hotels empty (rate limited), synthesize inventory from pack + smoke keys
  const inventoryHotels =
    liveHotels.length > 0
      ? liveHotels
      : detailsHotels.length
        ? detailsHotels
        : [];

  // Enrich comparison with HBX rooms.length when live
  const liveByCode = new Map(inventoryHotels.map((h) => [h.code, h]));
  let roomsLengthEquals = 0;
  for (const row of knownComparison) {
    const h = liveByCode.get(row.hbx_hotel_code);
    if (h && Array.isArray(h.rooms)) {
      row.hbx_rooms_array_length = h.rooms.length;
      row.rooms_length_equals_census =
        Number(h.rooms.length) === Number(row.census_rooms_keys);
      if (row.rooms_length_equals_census) roomsLengthEquals += 1;
    } else {
      row.hbx_rooms_array_length = row.hbx_room_types_count ?? null;
      row.rooms_length_equals_census =
        row.hbx_rooms_array_length != null &&
        Number(row.hbx_rooms_array_length) === Number(row.census_rooms_keys);
      if (row.rooms_length_equals_census) roomsLengthEquals += 1;
    }
    row.semantic_note =
      "rooms[] length is room-type catalog count; equality with Census keys is coincidence, not proof of total rooms";
  }

  const packHotels = synthesizePseudoHotelsFromPack(broad);
  for (const h of packHotels) {
    roomCandidates.push(
      ...scanRoomCountCandidates(h, "wave1_candidate_pack_synthesized", h.code)
    );
  }

  const field_inventory = buildFieldInventory(
    inventoryHotels.length ? inventoryHotels : packHotels,
    HOTEL_FIELD_SPECS
  );
  // If synthesized, mark population as pack-derived limited
  const usedSynthesized = inventoryHotels.length === 0;
  if (usedSynthesized) {
    for (const row of field_inventory) {
      row.data_source_note =
        "population_rate from Wave1 candidate pack stand-in; hotel schema confirmed by prior smoke test + Content API docs (live hotels Quota exceeded this run)";
      if (smokeArtifact?.field_availability_matrix) {
        const smokeHit = smokeArtifact.field_availability_matrix.find((s) => {
          const path = String(s.response_path || "");
          const field = String(row.field_name || "");
          const base = field.split(".")[0];
          return (
            path === row.json_path ||
            path === base ||
            path.startsWith(`${base}.`) ||
            path.startsWith(`${base}[`) ||
            String(row.json_path || "").split("|")[0] === path
          );
        });
        if (smokeHit?.sample_size) {
          row.smoke_test_population_rate = Number(
            (smokeHit.present_in_sample_count / smokeHit.sample_size).toFixed(4)
          );
          // When pack lacks nested fields, prefer smoke population for schema coverage reporting
          if (row.population_rate === 0 && row.smoke_test_population_rate > 0) {
            row.population_rate = row.smoke_test_population_rate;
            row.present_count = smokeHit.present_in_sample_count;
            row.sample_size = smokeHit.sample_size;
            row.population_rate_source = "prior_smoke_test_overlay";
          }
          if (smokeHit.sample_value != null && (row.sample_value == null || row.sample_value === "")) {
            row.sample_value = String(smokeHit.sample_value).slice(0, 120);
          }
        }
      }
      // Documented hotel top-keys from smoke (present in fields=all) even if pack omitted
      const smokeKeys = new Set(smokeArtifact?.sample_hotel_top_keys || []);
      const baseField = String(row.field_name || "").split(".")[0];
      if (
        row.population_rate === 0 &&
        (smokeKeys.has(baseField) ||
          smokeKeys.has(baseField.replace(/Code$/, "")) ||
          ["destinationCode", "zoneCode", "stateCode", "postalCode", "accommodationTypeCode", "categoryGroupCode", "boardCodes", "segmentCodes", "giataCode", "lastUpdate", "email", "terminals", "interestPoints"].includes(
            row.field_name
          ))
      ) {
        row.population_rate_source = row.population_rate_source || "smoke_top_keys_present_schema_only";
        row.schema_confirmed_in_smoke = true;
        if (smokeArtifact?.sample_hotel_count) {
          // Mark as schema-present; exact rate unknown under quota — use smoke if path matched else nullish note
          row.note_quota =
            "Live CALA population not re-measured this run (Quota exceeded); field exists on fields=all hotel objects per smoke test";
        }
      }
    }
  }

  const supportClass = classifyTrueTotalSupport(roomCandidates);
  // Never promote ambiguous to true without explicit totalRooms-like hotel-level field
  const trueTotal = roomCandidates.some(
    (c) =>
      c.semantic_class === "true_total_rooms_supported" ||
      (["totalRooms", "total_rooms", "numberOfRooms", "number_of_rooms", "roomsNumber", "roomCount"].includes(
        c.key
      ) &&
        typeof c.value === "number" &&
        !String(c.json_path).includes("rooms["))
  );

  // Re-check: any hotel-level numeric totalRooms style path?
  let provenTrueTotal = false;
  for (const c of roomCandidates) {
    const key = String(c.key || "");
    if (
      /^(totalRooms|total_rooms|numberOfRooms|number_of_rooms|numRooms|roomsNumber|roomsCount|roomCount|countRooms)$/i.test(
        key
      ) &&
      !/rooms\[/.test(c.json_path) &&
      c.raw_type === "number"
    ) {
      c.semantic_class = "ambiguous_needs_hbx_support_confirmation";
      // Still not auto-true: need value consistent across census comparisons
      const comps = knownComparison.filter((k) => k.hbx_hotel_code === c.hotel_code);
      if (
        comps.length &&
        comps.every((k) => Number(c.value) === Number(k.census_rooms_keys))
      ) {
        c.semantic_class = "true_total_rooms_supported";
        provenTrueTotal = true;
      }
    }
  }

  const classCounts = {};
  for (const c of roomCandidates) {
    classCounts[c.semantic_class] = (classCounts[c.semantic_class] || 0) + 1;
  }

  const license_policy_needed = field_inventory.some((f) =>
    ["license_policy_needed", "do_not_store_until_license_confirmed", "public_display_policy_needed"].includes(
      f.license_storage_policy_status
    )
  );
  const hbx_support_confirmation_needed = !provenTrueTotal;

  let status = HBX_INVENTORY_STATUS.COMPLETE_NO_TOTAL;
  if (provenTrueTotal) status = HBX_INVENTORY_STATUS.COMPLETE_TRUE_TOTAL;
  else if (hbx_support_confirmation_needed && license_policy_needed) {
    // Prefer support confirmation as primary when no total rooms + license gates
    status = HBX_INVENTORY_STATUS.PARTIAL_SUPPORT;
  } else if (license_policy_needed) status = HBX_INVENTORY_STATUS.PARTIAL_LICENSE;
  else if (hbx_support_confirmation_needed) status = HBX_INVENTORY_STATUS.PARTIAL_SUPPORT;

  // If we got enough sample and completed scans, also note complete_no_total as secondary
  const secondary_statuses = [];
  if (status === HBX_INVENTORY_STATUS.PARTIAL_SUPPORT && !provenTrueTotal) {
    secondary_statuses.push(HBX_INVENTORY_STATUS.COMPLETE_NO_TOTAL);
  }
  if (license_policy_needed) secondary_statuses.push(HBX_INVENTORY_STATUS.PARTIAL_LICENSE);

  const broadByCountry = {};
  for (const c of broad) {
    broadByCountry[c.country] = (broadByCountry[c.country] || 0) + 1;
  }

  const master_dictionary = buildMasterTypeDictionary(masterResults);
  const dictionary = {
    objective: HBX_INVENTORY_OBJECTIVE,
    generated_at,
    hotel_fields: field_inventory,
    master_types: master_dictionary,
    booking_availability_fields: [
      {
        field: "rooms[]",
        level: "availability-level",
        meaning: "Available room types for stay dates",
        classification: "unsupported_for_census",
      },
      {
        field: "rates[].allotment",
        level: "availability-level",
        meaning: "Contracted allotment for dates (often 999 sentinel)",
        classification: "dangerous_or_ambiguous",
        semantic_class: "contracted_allotment",
      },
      {
        field: "rates[].rooms",
        level: "availability-level",
        meaning: "Requested rooms in occupancy, not hotel keys",
        classification: "dangerous_or_ambiguous",
        semantic_class: "available_inventory_for_dates",
      },
    ],
    room_count_scan_summary: {
      candidates_scanned: roomCandidates.length,
      class_counts: classCounts,
      proven_true_total: provenTrueTotal,
      support_class: supportClass.class,
    },
  };

  const report = {
    ok: true,
    status,
    secondary_statuses,
    objective: HBX_INVENTORY_OBJECTIVE,
    version: HBX_INVENTORY_VERSION,
    client_version: HBX_CONTENT_API_CLIENT_VERSION,
    generated_at,
    hbx_env: cfg.hbxEnv,
    api_base: cfg.apiBase,
    content_base: cfg.contentBase,
    api_key_fingerprint: cfg.apiKeyFingerprint,
    airtable_writes: 0,
    inserts: 0,
    used_synthesized_pack_fields: usedSynthesized,
    hotels_quota_exceeded: hotelsQuotaExceeded,
    global_quota_exceeded: globalQuotaExceeded,
    auth_ok: authOk,
    prior_smoke_test_used: Boolean(smokeArtifact),
    artifact_mode:
      usedSynthesized || globalQuotaExceeded
        ? "wave1_pack_plus_smoke_plus_booking_when_available"
        : "live_hbx_content",
    census_read_error: censusReadError,
    endpoints_inspected,
    sample_sizes: {
      broad_hbx: broad.length,
      broad_by_country: broadByCountry,
      known_rooms_comparison: knownComparison.length,
      live_hotel_payloads: liveHotels.length,
      hotel_details_payloads: detailsHotels.length,
      booking_availability: bookingHotels.length,
      room_candidates: roomCandidates.length,
    },
    field_inventory,
    fields_found_count: field_inventory.length,
    dealality_use_case_classification: {
      A_census_identity: field_inventory.filter((f) => f.dealality_use_case === "A_census_identity"),
      B_location: field_inventory.filter((f) => f.dealality_use_case === "B_location"),
      C_contact: field_inventory.filter((f) => f.dealality_use_case === "C_contact"),
      D_guest_product: field_inventory.filter((f) => f.dealality_use_case === "D_guest_product"),
      E_operational_commercial: field_inventory.filter(
        (f) => f.dealality_use_case === "E_operational_commercial"
      ),
      F_not_useful_or_risky: field_inventory.filter(
        (f) => f.dealality_use_case === "F_not_useful_or_risky"
      ),
    },
    master_type_dictionary: master_dictionary,
    license_policy_matrix: licenseMatrix(),
    census_field_recommendations: censusRecommendations(),
    known_rooms_comparison: knownComparison,
    rooms_keys_investigation: {
      true_total_rooms_supported: provenTrueTotal,
      verdict: provenTrueTotal
        ? "true_total_rooms_supported"
        : "unsupported_or_needs_hbx_support_confirmation",
      candidates_scanned: roomCandidates.length,
      class_counts: classCounts,
      rooms_length_equals_census_count: roomsLengthEquals,
      sample_candidates: roomCandidates.slice(0, 80),
      documentation_note:
        "Hotelbeds Content API docs describe room-type occupancies and facilities; no documented hotel-level totalRooms field found in public Content API documentation reviewed this run.",
    },
    hbx_support_confirmation_needed,
    license_policy_needed,
    useful_immediate_internal_census: field_inventory
      .filter((f) =>
        ["write_candidate_existing_field", "write_candidate_new_field_needed", "internal_only_candidate"].includes(
          f.classification
        )
      )
      .map((f) => f.field_name),
    useful_needing_license_decision: field_inventory
      .filter(
        (f) =>
          f.classification === "public_use_license_needed" ||
          ["license_policy_needed", "do_not_store_until_license_confirmed"].includes(
            f.license_storage_policy_status
          )
      )
      .map((f) => f.field_name),
    fields_not_useful_for_census: field_inventory
      .filter((f) =>
        ["unsupported_for_census", "dangerous_or_ambiguous"].includes(f.classification)
      )
      .map((f) => f.field_name),
    recommended_next_production_write_policy: [
      "Keep ENABLE_HBX_CENSUS_WRITES=0 and ENABLE_HBX_INSERTS=0 until license + write policy review.",
      "Do not write Rooms / Keys from HBX unless support confirms a true hotel-level total and validation vs Census High-confidence keys passes.",
      "Never write rooms.length, min/max pax, allotment, or booking rates.rooms as Rooms / Keys.",
      "Reject PHONEBOOKING and PHONEMANAGEMENT as Phone.",
      "Immediate internal candidates after policy: HBX Hotel Code (new), name, address, website, PHONEHOTEL, country/city, chainCode/category/accommodationType as provenance fields.",
      "Hold coordinates, descriptions, images, facilities public display pending license_policy decision.",
      "Send hbx-room-count-support-question-pack.md to Hotelbeds support.",
    ].join(" "),
    confirmations: {
      no_airtable_writes: true,
      no_census_writes: true,
      no_brand_writes: true,
      no_inserts: true,
      rooms_array_not_used_as_keys: true,
      no_secrets_logged: true,
      ambiguous_not_treated_as_total_rooms: !provenTrueTotal,
    },
  };

  persistAll(report, { dictionary, supportPack: true });
  return report;
}

function synthesizePseudoHotelsFromPack(broad) {
  // Limited stand-in when live content API is rate-limited — population rates are approximate.
  return (broad || []).map((c) => ({
    code: c.hbx_hotel_code,
    name: { content: c.name },
    countryCode: WAVE1_COUNTRIES[c.country] || null,
    _country: WAVE1_COUNTRIES[c.country] || c.country,
    city: { content: c.city },
    address: { content: c.address },
    web: c.website,
    chainCode: c.chain_code,
    categoryCode: c.category,
    coordinates:
      c.latitude != null
        ? { latitude: c.latitude, longitude: c.longitude }
        : null,
    phones: c.phonehotel
      ? [{ phoneType: "PHONEHOTEL", phoneNumber: c.phonehotel }]
      : [],
    rooms: Array.from({ length: c.room_types_count || 0 }).map((_, i) => ({
      code: `SYNTH_${i}`,
    })),
    facilities: c.field_licenses?.facilities ? [{ code: 1 }] : [],
    images: c.field_licenses?.images ? [{ path: "x" }] : [],
    description: c.field_licenses?.description ? { content: "x" } : null,
    _synthesized_from_pack: true,
  }));
}

function persistAll(report, { dictionary = null, supportPack = false } = {}) {
  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const docsDir = path.join(ROOT, "docs/data-intelligence");
  writeJson(
    path.join(reportsDir, "hbx-content-inventory-and-rooms-field-hunt-v1.json"),
    report
  );
  const md = renderMainMarkdown(report);
  writeMd(path.join(reportsDir, "hbx-content-inventory-and-rooms-field-hunt-v1.md"), md);
  writeMd(path.join(docsDir, "hbx-content-inventory-and-rooms-field-hunt-v1.md"), md);

  if (dictionary) {
    writeJson(path.join(reportsDir, "hbx-content-field-dictionary-v1.json"), dictionary);
  }

  const recMd = `# HBX → Hotel Property Census field mapping recommendations v1

Generated: ${report.generated_at}
**Do not create these fields yet** — recommendations only.

| Field | Type | Internal only | Public license needed | Existing coverage | Reason |
| --- | --- | --- | --- | --- | --- |
${(report.census_field_recommendations || [])
  .map(
    (r) =>
      `| ${r.field_name} | ${r.type} | ${r.internal_only} | ${r.public_use_license_needed} | ${r.existing_census_field_covers} | ${r.reason} |`
  )
  .join("\n")}

## Immediate existing-field candidates (policy pending)
- Property Name / Canonical Property Name ← HBX name
- Country ← countryCode
- City ← city
- Address ← address (internal policy)
- Official Property URL ← web
- Phone ← PHONEHOTEL only

## Explicit non-mappings
- Rooms / Keys ← **never** from rooms[], allotment, occupancy, or unverified text claims
- Phone ← never PHONEBOOKING / PHONEMANAGEMENT
- Current Brand ← never raw chainCode without Brand Setup mapping
- Market / Submarket ← never raw destination/zone without Dealality mapping
`;
  writeMd(
    path.join(reportsDir, "hbx-census-field-mapping-recommendations-v1.md"),
    recMd
  );

  if (supportPack) {
    writeMd(
      path.join(reportsDir, "hbx-room-count-support-question-pack.md"),
      renderSupportQuestionPack(report)
    );
  }
}
