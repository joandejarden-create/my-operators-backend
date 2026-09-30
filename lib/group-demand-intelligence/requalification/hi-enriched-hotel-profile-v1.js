/**
 * HI-enriched hotel geography/capability profile for GDI requalification.
 * Prefer live Hotel Intelligence over stale static config capability fields.
 * Does not invent facts — only overlays supportable HI commercial/event values.
 */

import { buildHotelGeographyProfile } from "../market-opportunity-graph/hotel-geography-profile-v1.js";
import { loadHotelIntelligenceFromAirtable } from "../../hotel-intelligence/schema/hi-airtable-store.js";
import { loadDomainStatusLedger } from "../../hotel-intelligence/onboarding/domain-status-store.js";
import { HI_DOMAIN } from "../../hotel-intelligence/onboarding/domain-status-v1.js";
import { gdiEventSpaceCapabilitySemantics } from "../../hotel-intelligence/onboarding/hi-completeness-gate.js";

/**
 * @param {string} hotelId
 * @param {{ hiPacket?: object }} [opts]
 */
export async function buildHiEnrichedHotelProfile(hotelId, opts = {}) {
  const base = buildHotelGeographyProfile(hotelId);
  if (base.error) return base;

  let hi = opts.hiPacket || null;
  if (!hi) {
    try {
      hi = await loadHotelIntelligenceFromAirtable(hotelId);
    } catch (err) {
      hi = { error: err.message || String(err) };
    }
  }

  const commercial = hi?.commercial || null;
  const ledger = loadDomainStatusLedger(hotelId);
  const eventStatus =
    ledger?.domains?.[HI_DOMAIN.EVENT_SPACES]?.domainStatus ||
    base.eventSpaceDomainStatus;

  const hiRooms =
    commercial?.roomsKeys != null ? Number(commercial.roomsKeys) : null;
  const hiMeet =
    commercial?.totalMeetingSpaceSqFt != null
      ? Number(commercial.totalMeetingSpaceSqFt)
      : null;
  const hiMeetingRooms =
    commercial?.meetingRoomCount != null
      ? Number(commercial.meetingRoomCount)
      : null;
  const hiLargest =
    commercial?.largestMeetingSpaceSqFt != null
      ? Number(commercial.largestMeetingSpaceSqFt)
      : null;
  const hiCap =
    commercial?.largestEventCapacity != null
      ? Number(commercial.largestEventCapacity)
      : null;

  const rooms =
    hiRooms != null && Number.isFinite(hiRooms) && hiRooms > 0
      ? hiRooms
      : base.rooms;
  const meetingSqFt =
    hiMeet != null && Number.isFinite(hiMeet) && hiMeet > 0
      ? hiMeet
      : base.meetingSqFt;

  const hiOverlay = {
    roomsKeys: hiRooms,
    meetingSqFt: hiMeet,
    meetingRoomCount: hiMeetingRooms,
    largestMeetingSpaceSqFt: hiLargest,
    largestEventCapacity: hiCap,
    eventSpaceRowCount: Array.isArray(hi?.eventSpaces) ? hi.eventSpaces.length : 0,
    demandNodeRowCount: Array.isArray(hi?.demandNodes) ? hi.demandNodes.length : 0,
    source: hi?.error ? "hi_load_error" : commercial ? "hotel_intelligence_airtable" : "config_only",
    hiLoadError: hi?.error || null,
  };

  return {
    ...base,
    rooms,
    meetingSqFt,
    meetingRoomCount: hiMeetingRooms ?? base.meetingRoomCount ?? null,
    largestMeetingSpaceSqFt: hiLargest,
    largestEventCapacity: hiCap,
    eventSpaceDomainStatus: eventStatus,
    eventSpaceCapability: gdiEventSpaceCapabilitySemantics(eventStatus),
    hiOverlay,
    hiEnriched: Boolean(commercial && !hi?.error),
  };
}

/**
 * Propose config capability patches from HI (no invent; only fill null/weaker).
 */
export function proposeCapabilityConfigPatch(configCapability = {}, hiOverlay = {}) {
  const next = { ...configCapability };
  const changes = [];
  if (
    hiOverlay.roomsKeys != null &&
    (next.totalGuestrooms == null ||
      next.totalGuestrooms === 0 ||
      Math.abs(Number(next.totalGuestrooms) - hiOverlay.roomsKeys) / hiOverlay.roomsKeys > 0.25)
  ) {
    // Only overwrite if missing or wildly different and HI is populated
    if (next.totalGuestrooms == null || next.totalGuestrooms === 0) {
      changes.push({
        field: "totalGuestrooms",
        from: next.totalGuestrooms ?? null,
        to: hiOverlay.roomsKeys,
      });
      next.totalGuestrooms = hiOverlay.roomsKeys;
    }
  }
  if (hiOverlay.meetingSqFt != null && (next.totalMeetingSpaceSqFt == null || next.totalMeetingSpaceSqFt === 0)) {
    changes.push({
      field: "totalMeetingSpaceSqFt",
      from: next.totalMeetingSpaceSqFt ?? null,
      to: hiOverlay.meetingSqFt,
    });
    next.totalMeetingSpaceSqFt = hiOverlay.meetingSqFt;
  }
  if (
    hiOverlay.meetingRoomCount != null &&
    (next.meetingRoomCount == null || next.meetingRoomCount === 0)
  ) {
    changes.push({
      field: "meetingRoomCount",
      from: next.meetingRoomCount ?? null,
      to: hiOverlay.meetingRoomCount,
    });
    next.meetingRoomCount = hiOverlay.meetingRoomCount;
  }
  if (
    hiOverlay.largestMeetingSpaceSqFt != null &&
    (next.largestMeetingRoomSqFt == null || next.largestMeetingRoomSqFt === 0)
  ) {
    changes.push({
      field: "largestMeetingRoomSqFt",
      from: next.largestMeetingRoomSqFt ?? null,
      to: hiOverlay.largestMeetingSpaceSqFt,
    });
    next.largestMeetingRoomSqFt = hiOverlay.largestMeetingSpaceSqFt;
  }
  if (
    hiOverlay.largestEventCapacity != null &&
    (next.largestTheaterCapacity == null || next.largestTheaterCapacity === 0)
  ) {
    changes.push({
      field: "largestTheaterCapacity",
      from: next.largestTheaterCapacity ?? null,
      to: hiOverlay.largestEventCapacity,
    });
    next.largestTheaterCapacity = hiOverlay.largestEventCapacity;
  }
  return { capabilityProfile: next, changes };
}
