/**
 * Field-level write policy for Tripadvisor → Census (null-fill only).
 * Blocks any overwrite of existing non-null census values.
 */

import { WRITE_TIER, TIER_A_CENSUS_FIELDS } from "./census-map.js";
import { MAP_CENSUS_FIELDS } from "../map_hotel_intelligence_fields.js";
import { websiteHost } from "../tripadvisor-rooms/match.js";

function blank(v) {
  return v == null || String(v).trim() === "";
}

function isValidHttpUrl(url) {
  try {
    const u = new URL(String(url || "").trim());
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

function websitePlausible(url, hotelName) {
  if (!isValidHttpUrl(url)) return { ok: false, reason: "invalid_url" };
  const host = websiteHost(url);
  if (!host) return { ok: false, reason: "no_host" };
  if (
    /tripadvisor\.|booking\.|expedia\.|hotels\.|google\.|facebook\.|instagram\./i.test(
      host
    )
  ) {
    return { ok: false, reason: "aggregator_or_social_host" };
  }
  return { ok: true, host };
}

function phonePlausible(phone) {
  const digits = String(phone || "").replace(/\D/g, "");
  if (digits.length < 7 || digits.length > 15) {
    return { ok: false, reason: "phone_digit_length" };
  }
  return { ok: true };
}

function emailPlausible(email) {
  const e = String(email || "").trim().toLowerCase();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e)) {
    return { ok: false, reason: "invalid_email" };
  }
  if (/tripadvisor\.|booking\.|noreply|no-reply/i.test(e)) {
    return { ok: false, reason: "generic_or_platform_email" };
  }
  return { ok: true, note: "hotel_ops_email_not_owner_contact" };
}

/**
 * Propose census writes from a matched Tripadvisor item + current census fields.
 * Never proposes overwrite of non-null values.
 *
 * @param {object} opts
 * @param {object} opts.censusFields Airtable field map
 * @param {object} opts.taItem
 * @param {object} opts.matchMeta
 * @param {object} opts.profilePack
 */
export function proposeCensusWrites(opts = {}) {
  const census = opts.censusFields || {};
  const ta = opts.taItem || {};
  const match = opts.matchMeta || {};
  const proposals = [];
  const blocked = [];
  const candidates = [];

  const matchOk =
    match.confidence === "high" ||
    match.confidence === "medium" ||
    (Number(match.score) || 0) >= 0.82;
  if (!matchOk) {
    return {
      proposals: [],
      blocked: [
        {
          reason: "match_confidence_insufficient",
          match_confidence: match.confidence || null,
          score: match.score ?? null,
        },
      ],
      candidates: [],
    };
  }

  function propose(field, newValue, tier, reason, extra = {}) {
    const oldValue = census[field];
    if (!blank(oldValue)) {
      blocked.push({
        field,
        old_value: oldValue,
        new_value: newValue,
        reason: "existing_non_null_blocked",
        write_policy_tier: tier,
      });
      return;
    }
    if (blank(newValue)) return;
    proposals.push({
      field,
      old_value: null,
      new_value: newValue,
      source: "tripadvisor_apify",
      provider_property_id: ta.id != null ? String(ta.id) : null,
      source_url: ta.webUrl || null,
      field_confidence: extra.field_confidence || "HIGH",
      match_confidence: match.confidence || null,
      match_score: match.score ?? null,
      write_policy_tier: tier,
      reason,
      ...extra,
    });
  }

  // --- Tier A ---
  if (!blank(ta.website)) {
    const w = websitePlausible(ta.website, ta.name);
    if (w.ok) {
      propose(
        MAP_CENSUS_FIELDS.website,
        ta.website,
        WRITE_TIER.A_SAFE_GAP_FILL,
        "null_fill_official_property_url",
        { field_confidence: "HIGH", website_host: w.host }
      );
    } else {
      blocked.push({
        field: MAP_CENSUS_FIELDS.website,
        new_value: ta.website,
        reason: w.reason,
        write_policy_tier: WRITE_TIER.A_SAFE_GAP_FILL,
      });
    }
  }

  if (!blank(ta.phone)) {
    const p = phonePlausible(ta.phone);
    if (p.ok) {
      propose(
        MAP_CENSUS_FIELDS.phone,
        ta.phone,
        WRITE_TIER.A_SAFE_GAP_FILL,
        "null_fill_phone",
        {
          field_confidence: "HIGH",
          companion_fields: {
            "Phone Source Type": "trusted_secondary_source",
            "Phone Source URL": ta.webUrl || null,
            "Phone Confidence": "High",
          },
        }
      );
    } else {
      blocked.push({
        field: MAP_CENSUS_FIELDS.phone,
        new_value: ta.phone,
        reason: p.reason,
        write_policy_tier: WRITE_TIER.A_SAFE_GAP_FILL,
      });
    }
  }

  if (!blank(ta.address)) {
    propose(
      MAP_CENSUS_FIELDS.address,
      ta.address,
      WRITE_TIER.A_SAFE_GAP_FILL,
      "null_fill_address",
      {
        field_confidence: "HIGH",
        companion_fields: {
          "Address Confidence": "High",
          "Address Source URL": ta.webUrl || null,
        },
      }
    );
  }

  const lat = ta.latitude;
  const lng = ta.longitude;
  if (Number.isFinite(Number(lat)) && Number.isFinite(Number(lng))) {
    const la = Number(lat);
    const lo = Number(lng);
    if (Math.abs(la) > 0.01 || Math.abs(lo) > 0.01) {
      propose(
        MAP_CENSUS_FIELDS.latitude,
        la,
        WRITE_TIER.A_SAFE_GAP_FILL,
        "null_fill_latitude",
        {
          field_confidence: "HIGH",
          requires_env: "ENABLE_COORDINATE_WRITES=1",
          companion_fields: {
            "Coordinate Confidence": "High",
            "Coordinate Source Type": "trusted_secondary_source",
          },
        }
      );
      propose(
        MAP_CENSUS_FIELDS.longitude,
        lo,
        WRITE_TIER.A_SAFE_GAP_FILL,
        "null_fill_longitude",
        {
          field_confidence: "HIGH",
          requires_env: "ENABLE_COORDINATE_WRITES=1",
        }
      );
    }
  }

  // --- Tier B ---
  if (!blank(ta.hotelClass)) {
    propose(
      "Hotel Class / Segment",
      String(ta.hotelClass),
      WRITE_TIER.B_CONDITIONAL,
      "conditional_hotel_class",
      {
        field_confidence: "MEDIUM",
        attribution: ta.hotelClassAttribution || null,
        requires_manual_or_flag: true,
      }
    );
  }

  // --- Tier C rooms ---
  const rooms =
    ta.numberOfRooms != null && Number.isFinite(Number(ta.numberOfRooms))
      ? Number(ta.numberOfRooms)
      : null;
  if (rooms != null && rooms > 0) {
    const existing = census[MAP_CENSUS_FIELDS.roomCount];
    if (blank(existing)) {
      candidates.push({
        field: MAP_CENSUS_FIELDS.roomCount,
        old_value: null,
        candidate_value: rooms,
        source: "tripadvisor_apify",
        write_policy_tier: WRITE_TIER.C_CANDIDATE_ONLY,
        rooms_candidate_status: "CANDIDATE_SINGLE_SOURCE",
        field_confidence: "MEDIUM",
        reason: "rooms_candidate_only_no_authoritative_write",
        source_url: ta.webUrl || null,
      });
    } else {
      blocked.push({
        field: MAP_CENSUS_FIELDS.roomCount,
        old_value: existing,
        new_value: rooms,
        reason: "compare_only_existing_rooms",
        write_policy_tier: WRITE_TIER.C_CANDIDATE_ONLY,
      });
    }
  }

  // Email — no census column; record as HI observation
  if (!blank(ta.email)) {
    const e = emailPlausible(ta.email);
    candidates.push({
      field: "Email",
      census_column_missing: true,
      candidate_value: ta.email,
      validation: e,
      write_policy_tier: WRITE_TIER.B_CONDITIONAL,
      reason: "email_hi_only_until_schema_added",
      note: "hotel ops email — not owner contact",
    });
  }

  return { proposals, blocked, candidates };
}

/**
 * Production write gate — respects existing env safety switches.
 */
export function evaluateProductionWriteGate(env = process.env) {
  const hi = String(env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES || "0") === "1";
  const enrich = String(env.ENABLE_CENSUS_FIELD_ENRICHMENT || "0") === "1";
  const coords = String(env.ENABLE_COORDINATE_WRITES || "0") === "1";
  const rooms = String(env.ENABLE_ROOMS_WRITES || "0") === "1";
  return {
    allow_any_airtable_writes: hi && enrich,
    allow_coordinate_writes: hi && enrich && coords,
    allow_rooms_authoritative_writes: false, // never via Tripadvisor auto path
    allow_rooms_writes_flag: rooms,
    status: hi && enrich ? "WRITES_ENABLED_FOR_APPROVED_TIERS" : "MANIFEST_ONLY",
    message:
      hi && enrich
        ? "Hotel Intelligence Airtable writes enabled — still null-fill only; rooms remain candidate-only"
        : "ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES and ENABLE_CENSUS_FIELD_ENRICHMENT must both be 1 to execute Tier A; stopping at proposed-write manifest",
  };
}

export { TIER_A_CENSUS_FIELDS, blank, websitePlausible, phonePlausible, emailPlausible };
