/**
 * Preferred lodging partnership commercial eligibility (zero invention).
 */
import {
  VENUE_ONSITE_LODGING_STATUS,
  normalizeVenueOnsiteLodgingStatus,
  text,
} from "./preferred-lodging-partnership-contract-v1.js";
import { CATCHMENT_CLASS } from "./preferred-lodging-partnership-contract-v1.js";

export const PREFERRED_LODGING_ELIGIBILITY_V1 =
  "gdi-preferred-lodging-eligibility-v1";

function hasNamedPerson(contact) {
  const name = text(contact?.name || contact?.personName || contact?.fullName);
  if (!name || name.length < 4) return false;
  if (/^(info|events|sales|contact|venue|the\s+)/i.test(name)) return false;
  if (!/\s/.test(name) && name.length < 8) return false;
  // Org-as-name: "The Lane" alone is not a named buyer
  if (/^(the\s+)?[a-z0-9 '&.-]+$/i.test(name) && !/\s[A-Z]/.test(name) && name.split(/\s+/).length < 2) {
    return false;
  }
  const tokens = name.split(/\s+/);
  return tokens.length >= 2 && tokens.every((t) => t.length >= 2);
}

export function evaluatePreferredLodgingEligibility({
  venue = {},
  proximity = {},
  lodgingStatus = null,
  contacts = [],
  eventActivityVerified = false,
  knownBarrier = null,
} = {}) {
  const reasons = [];
  const gaps = [];

  const venueName = text(venue.name || venue.venueName || venue.organizationName);
  if (!venueName) {
    reasons.push("missing_venue_identity");
  }

  const locationOk = Boolean(
    text(venue.address || venue.venueAddress) ||
      proximity.venueCoordinates ||
      text(venue.city || venue.venueCity),
  );
  if (!locationOk) gaps.push("venue_location_incomplete");

  if (!eventActivityVerified && !venue.eventActivityEvidenceStatus) {
    gaps.push("event_activity_not_verified");
  }

  const lodging = normalizeVenueOnsiteLodgingStatus(
    lodgingStatus || venue.onSiteLodgingStatus,
  );
  if (lodging === VENUE_ONSITE_LODGING_STATUS.UNKNOWN) {
    gaps.push("onsite_lodging_status_unknown");
  }
  if (lodging === VENUE_ONSITE_LODGING_STATUS.ONSITE_LODGING_AVAILABLE) {
    reasons.push("venue_has_sufficient_onsite_lodging");
  }

  const catchment = proximity.catchmentClass || CATCHMENT_CLASS.UNKNOWN;
  if (
    catchment === CATCHMENT_CLASS.OUT_OF_CATCHMENT ||
    catchment === CATCHMENT_CLASS.UNKNOWN
  ) {
    gaps.push("catchment_not_core_or_secondary");
  }

  const named = (contacts || []).find(hasNamedPerson);
  const hasNamedBuyer = Boolean(named);
  if (!hasNamedBuyer) {
    gaps.push("named_commercially_relevant_person_missing");
  }

  if (knownBarrier) {
    reasons.push(`barrier:${text(knownBarrier)}`);
  }

  const hardFail = reasons.some((r) =>
    /missing_venue_identity|sufficient_onsite_lodging|barrier:/i.test(r),
  );

  const salesReady =
    !hardFail &&
    venueName &&
    locationOk &&
    eventActivityVerified &&
    lodging !== VENUE_ONSITE_LODGING_STATUS.ONSITE_LODGING_AVAILABLE &&
    lodging !== VENUE_ONSITE_LODGING_STATUS.UNKNOWN &&
    (catchment === CATCHMENT_CLASS.CORE ||
      catchment === CATCHMENT_CLASS.SECONDARY) &&
    hasNamedBuyer;

  const researchContinuation =
    !hardFail &&
    venueName &&
    (!salesReady || gaps.length > 0);

  return {
    version: PREFERRED_LODGING_ELIGIBILITY_V1,
    salesReady,
    researchContinuation,
    disposition: salesReady
      ? "ASSIGN_FOR_DISCOVERY"
      : researchContinuation
        ? "WATCHLIST"
        : "DO_NOT_PURSUE",
    lodgingStatus: lodging,
    hasNamedBuyer,
    namedContactName: named?.name || null,
    gaps,
    hardFailReasons: reasons.filter((r) =>
      /missing_venue|sufficient_onsite|barrier:/i.test(r),
    ),
    notes: [
      "Confirmed bookings / room-night commitments are not required.",
      "Do not label an existing preferred partnership without verifying evidence.",
      "General inbox alone does not meet the named-buyer requirement.",
    ],
  };
}
