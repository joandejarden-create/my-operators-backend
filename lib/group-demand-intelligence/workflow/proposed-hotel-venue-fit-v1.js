/**
 * When Hotel Venue Fit rows are missing, propose evidence-backed fit assessments
 * for human review. Missing rows ≠ fit and ≠ no-fit.
 */

export const PROPOSED_HOTEL_VENUE_FIT_VERSION = "gdi-proposed-hotel-venue-fit-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function haversineMiles(a, b) {
  if (
    a?.lat == null ||
    a?.lng == null ||
    b?.lat == null ||
    b?.lng == null
  ) {
    return null;
  }
  const toRad = (d) => (Number(d) * Math.PI) / 180;
  const R = 3958.8;
  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLng / 2) ** 2;
  return Number((2 * R * Math.asin(Math.sqrt(h))).toFixed(2));
}

/**
 * Propose hotel–venue fits from hotel config + PE venues (read-only proposals).
 */
export function proposeHotelVenueFits({
  hotelId,
  hotelConfig = null,
  venues = [],
  existingFits = [],
  maxProposals = 12,
} = {}) {
  const id = text(hotelId);
  const existingVenueIds = new Set(
    (existingFits || []).map((f) => f.venueId).filter(Boolean)
  );
  const hotelLat =
    hotelConfig?.geo?.lat ??
    hotelConfig?.latitude ??
    hotelConfig?.capabilityProfile?.lat ??
    null;
  const hotelLng =
    hotelConfig?.geo?.lng ??
    hotelConfig?.longitude ??
    hotelConfig?.capabilityProfile?.lng ??
    null;
  const hotelPoint =
    hotelLat != null && hotelLng != null
      ? { lat: Number(hotelLat), lng: Number(hotelLng) }
      : null;
  const coreMiles = Number(
    hotelConfig?.practicalCatchment?.coreMiles ||
      hotelConfig?.demandTerritory?.coreMiles ||
      5
  );
  const stretchMiles = Number(
    hotelConfig?.practicalCatchment?.stretchMiles ||
      hotelConfig?.demandTerritory?.stretchMiles ||
      15
  );
  const rooms = hotelConfig?.capabilityProfile?.totalGuestrooms || null;
  const segments = hotelConfig?.commercialPriorities?.targetSegments || [];
  const socialLean = segments.some((s) =>
    /social|wedding|weekend|private|meeting/i.test(String(s))
  );

  const proposals = [];
  for (const venue of venues || []) {
    if (proposals.length >= maxProposals) break;
    if (!venue?.venueId || existingVenueIds.has(venue.venueId)) continue;
    const vLat = venue.lat ?? venue.latitude;
    const vLng = venue.lng ?? venue.longitude;
    const distanceMiles =
      hotelPoint && vLat != null && vLng != null
        ? haversineMiles(hotelPoint, { lat: Number(vLat), lng: Number(vLng) })
        : venue.distanceMiles ?? null;

    let catchment = "UNKNOWN";
    if (distanceMiles != null) {
      if (distanceMiles <= coreMiles) catchment = "CORE";
      else if (distanceMiles <= stretchMiles) catchment = "COMPETITIVE";
      else catchment = "OUTSIDE";
    }

    const noLodging =
      /NO_LODGING|NONE|false/i.test(String(venue.onSiteLodgingStatus || "")) ||
      venue.onSiteLodging === false;
    let proposedDisposition = "background_market_intelligence_only";
    let rationale = "Venue retained for review — lodging capture not yet evidenced.";
    if (catchment === "OUTSIDE") {
      proposedDisposition = "rejected_insufficient_travel_or_procurement";
      rationale = `Venue ~${distanceMiles} mi appears outside practical catchment (${stretchMiles} mi stretch).`;
    } else if (noLodging && (catchment === "CORE" || catchment === "COMPETITIVE")) {
      proposedDisposition = socialLean
        ? "venue_referral_partnership_account"
        : "potential_meeting_or_ancillary_hospitality";
      rationale = `Nearby venue without on-site lodging (${distanceMiles != null ? distanceMiles + " mi" : "distance unknown"}) — plausible referral/ancillary lodging path; human must confirm travel cohort and partnership openness.`;
    } else if (catchment === "CORE" || catchment === "COMPETITIVE") {
      proposedDisposition = "potential_meeting_or_ancillary_hospitality";
      rationale = `Venue within catchment — assess ancillary hospitality / overflow independently for this hotel.`;
    }

    proposals.push({
      hotelId: id,
      venueId: venue.venueId,
      venueName: venue.venueName || null,
      distanceMiles,
      lodgingCatchmentFit: catchment,
      proposedDisposition,
      rationale,
      hotelRoomsCapacity: rooms,
      status: "proposed_for_human_review",
      note: "Missing Hotel Venue Fit row — proposal only; not auto-written to Airtable",
    });
  }

  return {
    version: PROPOSED_HOTEL_VENUE_FIT_VERSION,
    hotelId: id,
    asOf: new Date().toISOString(),
    existingFitCount: (existingFits || []).length,
    venueCount: (venues || []).length,
    proposalCount: proposals.length,
    proposals,
    note: "Missing fit rows ≠ fit and ≠ no-fit; proposals require human review",
  };
}
