/**
 * Canonical identity for Preferred Lodging Partnership opportunities.
 * Primary display title = verified venue name only.
 */
import {
  PREFERRED_LODGING_CLASSIFICATION_LINE,
  PREFERRED_LODGING_SEGMENT_ID,
  PREFERRED_LODGING_SUBTYPE,
  PREFERRED_LODGING_PARTNERSHIP_V1,
  text,
} from "./preferred-lodging-partnership-contract-v1.js";

export const PREFERRED_LODGING_IDENTITY_V1 =
  "gdi-preferred-lodging-identity-v1";

/**
 * Strip legacy " — Preferred Lodging Partnership" suffixes and research prose.
 */
export function normalizeVenueDisplayTitle(rawTitle, venueName) {
  const venue = text(venueName);
  if (venue && venue.length <= 80 && !/preferred lodging/i.test(venue)) {
    return venue;
  }
  const raw = text(rawTitle);
  if (!raw) return venue || "Venue";
  const withoutSuffix = raw
    .replace(/\s*[—–-]\s*Preferred Lodging Partnership.*$/i, "")
    .trim();
  const left = withoutSuffix.split(/\s*[—–]\s*/)[0].trim();
  if (left && left.length <= 80 && !/without guestroom|event space/i.test(left)) {
    return left;
  }
  return venue || left || "Venue";
}

export function buildPreferredLodgingIdentity({
  venue = {},
  hotel = {},
  opportunity = {},
} = {}) {
  const venueName = text(
    venue.name ||
      venue.venueName ||
      opportunity.organizationName ||
      opportunity.venueName,
  );
  const displayTitle = normalizeVenueDisplayTitle(
    opportunity.displayTitle || opportunity.title,
    venueName,
  );
  const venueKey = displayTitle
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
  const hotelId = text(hotel.hotelId || opportunity.hotelId) || "unknown_hotel";
  const peVenueId =
    text(opportunity.peVenueId || venue.venueId || venue.peVenueId) || null;

  return {
    version: PREFERRED_LODGING_IDENTITY_V1,
    contract: PREFERRED_LODGING_PARTNERSHIP_V1,
    displayTitle,
    primaryOrganizationName: displayTitle,
    venueName: displayTitle,
    venueAliases: [venueName, opportunity.organizationName]
      .map(text)
      .filter((x, i, a) => x && a.indexOf(x) === i && x !== displayTitle),
    venueAddress: text(venue.address || opportunity.venueAddress) || null,
    hotelId,
    demandSegmentId: PREFERRED_LODGING_SEGMENT_ID,
    demandOpportunitySubtype: PREFERRED_LODGING_SUBTYPE,
    classificationLine: PREFERRED_LODGING_CLASSIFICATION_LINE,
    peVenueId,
    /** Stable within hotel — prevents duplicate partnership rows for same venue. */
    venuePartnershipFingerprint: `plp_${hotelId}_${peVenueId || venueKey}`,
    namingBasis: "verified_venue_name",
    opportunityLevel: "account",
    researchTitlePreserved: text(opportunity.title) || null,
  };
}
