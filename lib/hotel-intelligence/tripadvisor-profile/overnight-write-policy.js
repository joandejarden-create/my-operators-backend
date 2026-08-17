/**
 * Overnight wave write policy — null-fill only into retained KEEP_* census fields.
 * Schema delete/consolidate/move-out NOT approved.
 */

import { WRITE_TIER } from "./census-map.js";
import { MAP_CENSUS_FIELDS as MAP } from "../map_hotel_intelligence_fields.js";
import {
  blank,
  websitePlausible,
  phonePlausible,
  emailPlausible,
} from "./write-policy.js";

/** Dispositions allowed for Airtable writes tonight. */
export const WRITEABLE_DISPOSITIONS = new Set([
  "KEEP_CORE",
  "KEEP_SUPPORTING",
]);

/** Fields blocked from Tripadvisor census writes regardless. */
export const NEVER_WRITE_FIELDS = new Set([
  MAP.brandName,
  MAP.parentCompanyName,
  "Owner Name",
  "Owner Type",
  "Operator / Management Company",
  "Developer Name",
  MAP.roomCount, // candidate only
  "Country", // never overwrite/fill from TA as authoritative country change
]);

/**
 * Tripadvisor hotelClass (stars) ≠ Dealality Hotel Class / Segment (chain scale).
 * Always HI/evidence only for this field.
 */
export const HOTEL_CLASS_CENSUS_BLOCKED_REASON =
  "tripadvisor_hotelClass_is_star_rating_not_chain_scale_segment";

export const PROPERTY_TYPE_OPTIONS = Object.freeze([
  "Hotel",
  "Resort",
  "Boutique Hotel",
  "Extended Stay",
  "All-Inclusive",
  "Serviced Apartment",
  "Mixed-Use",
  "Other",
  "Unknown",
]);

/**
 * Map Tripadvisor type/subcategories → Dealality Property Type option.
 * @returns {{ value: string|null, confidence: string, reason: string }}
 */
export function mapTripadvisorPropertyType(taItem = {}) {
  const subs = [
    ...(Array.isArray(taItem.subcategories) ? taItem.subcategories : []),
    taItem.type,
    taItem.category,
  ]
    .filter(Boolean)
    .map((s) => String(s).toLowerCase());
  const blob = subs.join(" ");
  if (/all.?inclusive/.test(blob)) {
    return { value: "All-Inclusive", confidence: "HIGH", reason: "subcategory_all_inclusive" };
  }
  if (/resort/.test(blob)) {
    return { value: "Resort", confidence: "HIGH", reason: "subcategory_resort" };
  }
  if (/boutique/.test(blob)) {
    return { value: "Boutique Hotel", confidence: "MEDIUM", reason: "subcategory_boutique" };
  }
  if (/extended.?stay|aparthotel|serviced/.test(blob)) {
    return {
      value: "Extended Stay",
      confidence: "MEDIUM",
      reason: "subcategory_extended_stay",
    };
  }
  if (/hotel|lodge|inn/.test(blob) || taItem.type === "HOTEL") {
    return { value: "Hotel", confidence: "HIGH", reason: "type_hotel" };
  }
  return { value: null, confidence: "LOW", reason: "unmapped_property_type" };
}

/** Conceptual amenity taxonomy → normalized tag tokens for multiline Structured Tags. */
export const AMENITY_TAXONOMY = Object.freeze([
  { re: /free\s*wi-?fi|wifi|wireless internet/i, tag: "WIFI" },
  { re: /outdoor pool/i, tag: "OUTDOOR_POOL" },
  { re: /indoor pool/i, tag: "INDOOR_POOL" },
  { re: /\bpool\b|swimming pool/i, tag: "POOL" },
  { re: /fitness|gym/i, tag: "FITNESS_CENTER" },
  { re: /\bspa\b/i, tag: "SPA" },
  { re: /beach/i, tag: "BEACH_ACCESS" },
  { re: /all.?inclusive/i, tag: "ALL_INCLUSIVE" },
  { re: /restaurant/i, tag: "RESTAURANT" },
  { re: /\bbar\b|lounge/i, tag: "BAR_LOUNGE" },
  { re: /business center/i, tag: "BUSINESS_CENTER" },
  { re: /wheelchair|accessible/i, tag: "ACCESSIBLE" },
  { re: /kid|family|children/i, tag: "FAMILY_FRIENDLY" },
  { re: /parking/i, tag: "PARKING" },
  { re: /airport shuttle/i, tag: "AIRPORT_SHUTTLE" },
  { re: /pet.?friendly|pets allowed/i, tag: "PET_FRIENDLY" },
  { re: /room service/i, tag: "ROOM_SERVICE" },
  { re: /concierge/i, tag: "CONCIERGE" },
  { re: /meeting|conference/i, tag: "MEETING_SPACE" },
]);

export function normalizeTripadvisorAmenities(amenities = []) {
  const tags = new Set();
  for (const raw of amenities || []) {
    const s = String(raw || "");
    for (const rule of AMENITY_TAXONOMY) {
      if (rule.re.test(s)) tags.add(rule.tag);
    }
  }
  return [...tags].sort();
}

/**
 * @param {Map<string,string>|Record<string,string>} dispositionByField
 */
export function isDispositionWritable(fieldName, dispositionByField) {
  if (NEVER_WRITE_FIELDS.has(fieldName)) return false;
  const d =
    dispositionByField instanceof Map
      ? dispositionByField.get(fieldName)
      : dispositionByField?.[fieldName];
  if (!d) return false; // unknown ⇒ do not write
  if (!WRITEABLE_DISPOSITIONS.has(d)) return false;
  if (
    ["MOVE_OUT", "DEPRECATE", "CONSOLIDATE", "DELETE", "DELETE_CANDIDATE", "MIGRATE"].some(
      (x) => String(d).includes(x)
    )
  ) {
    return false;
  }
  // MOVE_* dispositions
  if (String(d).startsWith("MOVE_")) return false;
  return true;
}

/**
 * Overnight proposals — expands Tier A + approved geo/type/amenities when KEEP_*.
 * Hotel Class / Segment: NEVER from Tripadvisor stars.
 * Email / Postal: no census column → candidates only.
 */
export function proposeOvernightCensusWrites(opts = {}) {
  const census = opts.censusFields || {};
  const ta = opts.taItem || {};
  const match = opts.matchMeta || {};
  const dispositionByField = opts.dispositionByField || {};
  const proposals = [];
  const blocked = [];
  const candidates = [];
  const conflicts = [];

  const matchHigh =
    match.confidence === "high" ||
    (Number(match.score) || 0) >= 0.9;
  if (!matchHigh) {
    return {
      proposals: [],
      blocked: [
        {
          reason: "match_not_high_confidence",
          match_confidence: match.confidence || null,
          score: match.score ?? null,
        },
      ],
      candidates: [],
      conflicts: [],
    };
  }

  function tryPropose(field, newValue, extra = {}) {
    if (blank(newValue)) return;
    if (!isDispositionWritable(field, dispositionByField)) {
      blocked.push({
        field,
        new_value: newValue,
        reason: "blocked_schema_disposition",
        disposition:
          dispositionByField instanceof Map
            ? dispositionByField.get(field)
            : dispositionByField[field],
      });
      return;
    }
    const oldValue = census[field];
    if (!blank(oldValue)) {
      // compare / conflict only
      const same =
        String(oldValue).trim().toLowerCase() ===
        String(newValue).trim().toLowerCase();
      if (!same) {
        conflicts.push({
          field,
          old_value: oldValue,
          new_value: newValue,
          reason: "CONFLICT_EXISTING_CANONICAL",
        });
      }
      blocked.push({
        field,
        old_value: oldValue,
        new_value: newValue,
        reason: "existing_non_null_blocked",
      });
      return;
    }
    proposals.push({
      field,
      old_value: null,
      new_value: newValue,
      source: "tripadvisor_apify",
      provider_property_id: ta.id != null ? String(ta.id) : null,
      source_url: ta.webUrl || null,
      field_confidence: extra.field_confidence || "HIGH",
      match_confidence: match.confidence || "high",
      match_score: match.score ?? null,
      write_policy_tier: extra.tier || WRITE_TIER.A_SAFE_GAP_FILL,
      reason: extra.reason || "null_fill",
      schema_disposition:
        dispositionByField instanceof Map
          ? dispositionByField.get(field)
          : dispositionByField[field],
      companion_fields: extra.companion_fields || null,
      requires_env: extra.requires_env || null,
    });
  }

  // Website
  if (!blank(ta.website)) {
    const w = websitePlausible(ta.website);
    if (w.ok) {
      tryPropose(MAP.website, ta.website, {
        reason: "null_fill_official_property_url",
        field_confidence: "HIGH",
      });
    } else {
      blocked.push({
        field: MAP.website,
        new_value: ta.website,
        reason: w.reason,
      });
    }
  }

  // Phone
  if (!blank(ta.phone)) {
    const p = phonePlausible(ta.phone);
    if (p.ok) {
      tryPropose(MAP.phone, ta.phone, {
        reason: "null_fill_phone",
        companion_fields: {
          "Phone Source Type": "trusted_secondary_source",
          "Phone Source URL": ta.webUrl || null,
          "Phone Confidence": "High",
        },
      });
    } else {
      blocked.push({ field: MAP.phone, new_value: ta.phone, reason: p.reason });
    }
  }

  // Address
  if (!blank(ta.address)) {
    tryPropose(MAP.address, ta.address, {
      reason: "null_fill_address",
      companion_fields: {
        "Address Confidence": "High",
        "Address Source URL": ta.webUrl || null,
      },
    });
  }

  // Coords
  const lat = Number(ta.latitude);
  const lng = Number(ta.longitude);
  if (Number.isFinite(lat) && Number.isFinite(lng) && (Math.abs(lat) > 0.01 || Math.abs(lng) > 0.01)) {
    tryPropose(MAP.latitude, lat, {
      reason: "null_fill_latitude",
      requires_env: "ENABLE_COORDINATE_WRITES=1",
      companion_fields: {
        "Coordinate Confidence": "High",
        "Coordinate Source Type": "trusted_secondary_source",
      },
    });
    tryPropose(MAP.longitude, lng, {
      reason: "null_fill_longitude",
      requires_env: "ENABLE_COORDINATE_WRITES=1",
    });
  }

  // City / State — only if blank and country-compatible addressObj
  const taCountry = String(
    ta.addressObj?.country || ta.locationString || ""
  ).toLowerCase();
  const censusCountry = String(census[MAP.country] || "").toLowerCase();
  const countryOk =
    !censusCountry ||
    !taCountry ||
    taCountry.includes(censusCountry) ||
    censusCountry.includes(taCountry.split(",")[0]?.trim() || "___");

  if (countryOk) {
    if (!blank(ta.addressObj?.city)) {
      tryPropose(MAP.city, ta.addressObj.city, {
        reason: "null_fill_city",
        field_confidence: "HIGH",
        tier: WRITE_TIER.B_CONDITIONAL,
      });
    }
    if (!blank(ta.addressObj?.state)) {
      tryPropose(MAP.stateRegion, ta.addressObj.state, {
        reason: "null_fill_state_region",
        field_confidence: "MEDIUM",
        tier: WRITE_TIER.B_CONDITIONAL,
      });
    }
  } else {
    blocked.push({
      reason: "country_mismatch_geo_fill_blocked",
      census_country: census[MAP.country],
      ta_country: ta.addressObj?.country || null,
    });
  }

  // Property type
  const pt = mapTripadvisorPropertyType(ta);
  if (pt.value) {
    tryPropose(MAP.propertyType, pt.value, {
      reason: pt.reason,
      field_confidence: pt.confidence,
      tier: WRITE_TIER.B_CONDITIONAL,
    });
  }

  // Amenities structured tags — only if empty
  const amenityTags = normalizeTripadvisorAmenities(ta.amenities || []);
  if (amenityTags.length) {
    const joined = amenityTags.join(", ");
    tryPropose("Amenities - Structured Tags", joined, {
      reason: "null_fill_normalized_amenities",
      field_confidence: "MEDIUM",
      tier: WRITE_TIER.B_CONDITIONAL,
    });
  }

  // Hotel class — NEVER write stars into chain-scale segment field
  if (!blank(ta.hotelClass)) {
    blocked.push({
      field: "Hotel Class / Segment",
      new_value: ta.hotelClass,
      reason: HOTEL_CLASS_CENSUS_BLOCKED_REASON,
    });
    candidates.push({
      field: "tripadvisor_hotel_class_stars",
      candidate_value: ta.hotelClass,
      attribution: ta.hotelClassAttribution || null,
      write_policy_tier: WRITE_TIER.HI_ONLY,
      reason: HOTEL_CLASS_CENSUS_BLOCKED_REASON,
    });
  }

  // Rooms candidate
  const rooms =
    ta.numberOfRooms != null && Number.isFinite(Number(ta.numberOfRooms))
      ? Number(ta.numberOfRooms)
      : null;
  if (rooms != null && rooms > 0) {
    if (blank(census[MAP.roomCount])) {
      candidates.push({
        field: MAP.roomCount,
        candidate_value: rooms,
        source: "tripadvisor_apify",
        source_url: ta.webUrl || null,
        rooms_candidate_status: "CANDIDATE_SINGLE_SOURCE",
        field_confidence: "MEDIUM",
        reason: "rooms_candidate_only_no_authoritative_write",
      });
    } else {
      conflicts.push({
        field: MAP.roomCount,
        old_value: census[MAP.roomCount],
        new_value: rooms,
        reason: "CONFLICT_EXISTING_CANONICAL",
      });
      blocked.push({
        field: MAP.roomCount,
        reason: "compare_only_existing_rooms",
        old_value: census[MAP.roomCount],
        new_value: rooms,
      });
    }
  }

  // Email / postal — no census columns
  if (!blank(ta.email)) {
    const e = emailPlausible(ta.email);
    candidates.push({
      field: "property_email",
      census_column_missing: true,
      candidate_value: ta.email,
      validation: e,
      reason: "email_evidence_only_no_census_column",
      note: "PROPERTY CONTACT EMAIL — not owner contact",
    });
  }
  if (!blank(ta.addressObj?.postalcode)) {
    candidates.push({
      field: "postal_code",
      census_column_missing: true,
      candidate_value: ta.addressObj.postalcode,
      reason: "postal_evidence_only_no_census_column",
    });
  }

  return { proposals, blocked, candidates, conflicts };
}
