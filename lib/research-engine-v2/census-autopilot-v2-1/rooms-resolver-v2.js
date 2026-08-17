/**
 * Rooms Resolver V2 — wraps V1.3 family resolvers with provenance contract.
 * NEVER uses SerpApi. NEVER infers from bedrooms/occupancy/room-types/meeting rooms.
 */

import { resolveFamilyRooms, ROOMS_RESOLVER_VERSION as V13 } from "../census-autopilot-v1/golden-gap-v13/rooms-family-resolvers.js";

export const ROOMS_RESOLVER_V2_VERSION = "census-autopilot-v2.1-rooms-resolver";

export const ROOMS_LADDER = Object.freeze([
  "official_brand_structured_data_api",
  "official_hotel_property_page",
  "official_hotel_fact_sheet_pdf",
  "official_owner_website",
  "official_operator_management_page",
  "official_opening_development_announcement",
  "tourism_authority_government_registry",
  "official_convention_tourism_profile",
  "first_party_brand_operator_validation",
  "deep_research_escalation",
]);

export const ROOMS_NEVER_FROM = Object.freeze([
  "room_types",
  "bedrooms",
  "meeting_rooms",
  "occupancy",
  "availability",
  "booking_inventory",
  "review_counts",
  "serpapi_google_hotels",
  "cvent",
  "legacy_census",
]);

/**
 * @param {object} property { name, family, website, property_ids, independent_record_id }
 * @param {{ delayMs?: number }} [opts]
 */
export async function resolveRoomsV2(property, opts = {}) {
  const family = String(property.family || "").trim();
  if (!["IHG", "Hilton", "Choice"].includes(family)) {
    return {
      ok: false,
      rooms_value: null,
      classification: family ? "PUBLIC-RESEARCH ESCALATION" : "UNKNOWN",
      reason: "family_not_in_v2_native_scope",
      rooms_source: null,
      rooms_source_type: null,
      retrieved_at: null,
      confidence: null,
      property_identity_match: property.independent_record_id || property.property_identity_id || null,
      evidence_quote_or_structured_field: null,
      rights_status: "n/a",
      serpapi_used: false,
      inferred: false,
      ladder: ROOMS_LADDER,
      never_from: ROOMS_NEVER_FROM,
      attempts: [],
    };
  }

  const result = await resolveFamilyRooms(property, opts);
  const retrieved_at = new Date().toISOString();

  if (result.ok && result.claim?.rooms != null) {
    return {
      ok: true,
      rooms_value: result.claim.rooms,
      classification: "NATIVE RESOLVABLE",
      rooms_source: result.claim.source,
      rooms_source_type: result.claim.source_type,
      retrieved_at,
      confidence: result.claim.confidence,
      property_identity_match: property.independent_record_id || property.property_identity_id || null,
      evidence_quote_or_structured_field: result.claim.method || null,
      rights_status: "Allowed with Constraints — official brand/property research",
      serpapi_used: false,
      inferred: false,
      cvent_used: false,
      legacy_used: false,
      resolver_version: ROOMS_RESOLVER_V2_VERSION,
      wrapped: V13,
      attempts: result.attempts || [],
    };
  }

  let classification = "UNKNOWN";
  const reason = result.reason || "not_found";
  if (/403|blocked/i.test(reason)) classification = "PUBLIC-RESEARCH ESCALATION";
  else if (family === "Choice") classification = "FIRST-PARTY VALIDATION";
  else classification = "PUBLIC-RESEARCH ESCALATION";

  return {
    ok: false,
    rooms_value: null,
    classification,
    reason,
    rooms_source: null,
    rooms_source_type: null,
    retrieved_at,
    confidence: null,
    property_identity_match: property.independent_record_id || property.property_identity_id || null,
    evidence_quote_or_structured_field: null,
    rights_status: "n/a",
    serpapi_used: false,
    inferred: false,
    resolver_version: ROOMS_RESOLVER_V2_VERSION,
    attempts: result.attempts || [],
  };
}
