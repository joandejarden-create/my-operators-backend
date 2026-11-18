/**
 * Commercial payload durability V1 (CD-1 / CD-2 / CD-5 / CD-6 / CD-7).
 *
 * Ensures opportunityPayloadJson round-trips preserve commercial presentation,
 * CMI, Weekly Change annotations, room-night claim metadata, and sales-readiness
 * blocker codes without destructive compaction.
 *
 * No new Airtable schema. No production writes.
 */

export const GDI_COMMERCIAL_PAYLOAD_DURABILITY_V1 =
  "gdi-commercial-payload-durability-v1";

export const GDI_COMMERCIAL_PAYLOAD_CONTRACT_VERSION =
  "gdi-commercial-payload-contract-v1";

/** Keys that must survive compaction / minimal payload floors (CD-2 / CD-5 / CD-7). */
export const REQUIRED_COMMERCIAL_PAYLOAD_KEYS = Object.freeze([
  "salesActionKey",
  "salesActionPillLabel",
  "commercialStageKey",
  "commercialStagePillLabel",
  "revenueStreamClass",
  "revenueStreamIcons",
  "bookingWindowStatus",
  "bookingWindowLabel",
  "bookingWindowReason",
  "actionTimingFilterKey",
  "competitorHotel",
  "cmiCompetitorHotel",
  "competitorVenueStatus",
  "competitorVenueStatusLabel",
  "cmiEvidenceStatus",
  "cmiVerified",
  "weeklyDeltaState",
  "isNewThisWeek",
  "firstSeenRunId",
  "firstSeenAt",
  "lastMaterialChangeAt",
  "lastMaterialChangeRunId",
  "eligibilityGaps",
  "salesReadinessBlockers",
  "salesReadinessRuleVersion",
  "qualificationReasons",
  "estimatedRoomNights",
  "estimatedRoomNightsClaimKind",
  "estimatedRoomNightsMethodology",
  "estimatedRoomNightsConfidence",
  "peakRoomsClaimKind",
  "peakRoomsEvidenceUrl",
  "roomDemandStatus",
  "dgGeneratorId",
  "dgProgramId",
  "dgSignalId",
  "hotelGeneratorFitId",
  "eventSeriesId",
  "eventCycleId",
]);

/** Hotel-private keys that must never be copied onto shared DG / Program records. */
export const HOTEL_PRIVATE_PAYLOAD_KEYS = Object.freeze([
  "hotelSalesFeedback",
  "salesFeedback",
  "salesFeedbackNotes",
  "hotelTeamNotes",
  "assignedSalesperson",
  "salesOwner",
  "decisionId",
  "workingStatus",
  "rejectionReason",
  "notAFitReason",
  "alreadyKnown",
  "usefulnessRating",
  "nextFollowUpDate",
  "contactedAt",
  "lastSalesActivityAt",
]);

function nonempty(v) {
  if (v == null) return false;
  if (typeof v === "string") return v.trim().length > 0;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v).length > 0;
  return true;
}

/**
 * Extract the commercial durability slice for payload merge / reattach.
 */
export function extractCommercialPayloadSlice(opp = {}) {
  const out = {};
  for (const key of REQUIRED_COMMERCIAL_PAYLOAD_KEYS) {
    if (opp[key] !== undefined) out[key] = opp[key];
  }
  out.commercialPayloadContractVersion =
    opp.commercialPayloadContractVersion ||
    GDI_COMMERCIAL_PAYLOAD_CONTRACT_VERSION;
  return out;
}

/**
 * Re-attach commercial keys after compaction (CD-2).
 */
export function reattachRequiredCommercialKeys(compacted = {}, original = {}) {
  const next =
    compacted && typeof compacted === "object" ? { ...compacted } : {};
  const slice = extractCommercialPayloadSlice(original);
  for (const [k, v] of Object.entries(slice)) {
    if (v === undefined) continue;
    if (!nonempty(next[k]) && (nonempty(v) || v === false || v === 0)) {
      next[k] = v;
    } else if (next[k] === undefined) {
      next[k] = v;
    }
  }
  return next;
}

/**
 * Strip hotel-private sales feedback before writing shared DG/Program payloads.
 */
export function stripHotelPrivateKeys(payload = {}) {
  const next =
    payload && typeof payload === "object" ? { ...payload } : {};
  for (const key of HOTEL_PRIVATE_PAYLOAD_KEYS) {
    if (key in next) delete next[key];
  }
  return next;
}

/**
 * Assert commercial keys survive a compact→reattach round trip.
 */
export function assertCommercialPayloadRoundTrip(original = {}, compacted = {}) {
  const restored = reattachRequiredCommercialKeys(compacted, original);
  const missing = [];
  for (const key of REQUIRED_COMMERCIAL_PAYLOAD_KEYS) {
    if (!nonempty(original[key]) && original[key] !== false && original[key] !== 0) {
      continue;
    }
    if (
      restored[key] === undefined ||
      (nonempty(original[key]) && !nonempty(restored[key]) && original[key] !== restored[key])
    ) {
      missing.push(key);
    }
  }
  return {
    ok: missing.length === 0,
    missing,
    contractVersion: GDI_COMMERCIAL_PAYLOAD_CONTRACT_VERSION,
  };
}
