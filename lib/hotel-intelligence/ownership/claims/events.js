/**
 * Packet 2.5 — Derive Intelligence Events from validated claims / candidates.
 * Events reuse claim evidence — do not duplicate facts independently.
 */

import { randomBytes } from "node:crypto";
import { INTELLIGENCE_EVENT_TYPES } from "./claim-types.js";

export const CLAIM_EVENTS_VERSION = "claim-events-v1";

function generateEventId() {
  return `die_${randomBytes(13).toString("hex")}`;
}

/**
 * @param {object} candidate
 * @param {object[]} claims
 */
export function createIntelligenceEvent(candidate, claims = []) {
  const type = eventTypeFor(candidate, claims);
  if (!type) return null;
  const supporting = (candidate.supporting_claim_ids || []).filter(Boolean);
  return {
    schema_version: "intelligence-event-v1",
    event_id: generateEventId(),
    event_type: type,
    hotel_context_id: candidate.hotel_context_id || candidate.subject_hotel_id,
    subject_canonical_id: candidate.subject_canonical_id,
    object_canonical_id: candidate.object_canonical_id,
    relationship_type: candidate.relationship_type,
    ownership_percentage: candidate.ownership_percentage,
    temporal_status: candidate.temporal_status,
    effective_from: candidate.effective_from,
    effective_to: candidate.effective_to,
    supporting_claim_ids: supporting,
    confidence: candidate.confidence,
    created_at: new Date().toISOString(),
  };
}

function eventTypeFor(candidate, claims) {
  const rel = candidate.relationship_type;
  const ctypes = new Set(claims.map((c) => c.claim_type));
  if (ctypes.has("ENTITY_ACQUIRED_ENTITY_OR_ASSET")) return "ACQUISITION";
  if (ctypes.has("ENTITY_SOLD_ENTITY_OR_ASSET")) return "SALE";
  if (rel === "JV_WITH" || ctypes.has("ENTITY_PARTICIPATES_IN_JV")) {
    return "JV_FORMATION";
  }
  if (rel === "OWNED_BY" && candidate.temporal_status === "FORMER") {
    return "OWNERSHIP_CHANGE";
  }
  if (rel === "OWNED_BY" && candidate.temporal_status === "CURRENT") {
    return "OWNERSHIP_CHANGE";
  }
  if (
    rel === "BRANDED_BY" ||
    ctypes.has("HOTEL_ANNOUNCED_BRAND") ||
    ctypes.has("PROPERTY_WAS_REBRANDED")
  ) {
    return "BRAND_CHANGE";
  }
  if (ctypes.has("ENTITY_DEVELOPED_PROPERTY")) return "DEVELOPMENT";
  if (ctypes.has("ENTITY_FINANCED_PROPERTY")) return "FINANCING";
  if (ctypes.has("PROPERTY_OPENED_ON_DATE")) return "OPENING";
  if (!INTELLIGENCE_EVENT_TYPES.includes("OWNERSHIP_CHANGE")) return null;
  return null;
}
