/**
 * Classify hotel group-demand attributes from stored config and archived profiles.
 * Does not fetch, estimate, or write inventory.
 */

export const PROPERTY_ATTRIBUTE_COVERAGE_VERSION = "gdi-property-attribute-coverage-v1";

function numberOrNull(value) {
  if (value == null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function row(attribute, value, status, source, note) {
  return { attribute, value: value ?? null, status, source: source || null, note: note || null };
}

export function assessPropertyAttributeCoverage({ config = {}, archivedProfile = null } = {}) {
  const cp = config.capabilityProfile || {};
  const profile = archivedProfile || {};
  const stewardship = profile.attributeStewardship || {};
  const attributes = [];

  const rooms = numberOrNull(cp.totalGuestrooms);
  attributes.push(
    rooms != null
      ? row("totalGuestrooms", rooms, "VERIFIED_INVENTORY", "gdi_capability_profile", null)
      : row("totalGuestrooms", null, "UNKNOWN", profile.roomsNote ? "archived_profile" : null, profile.roomsNote || "No verified guestroom count is stored.")
  );

  const meetingSqFt = numberOrNull(cp.totalMeetingSpaceSqFt);
  attributes.push(
    meetingSqFt != null
      ? row("totalMeetingSpaceSqFt", meetingSqFt, "VERIFIED_INVENTORY", "gdi_capability_profile", null)
      : row("totalMeetingSpaceSqFt", null, "UNKNOWN", null, "No verified meeting square footage is stored.")
  );

  const claimSqFt = numberOrNull(cp.meetingSpaceClaimSqFt ?? profile.meetingSpace?.meetingSpaceClaimSqFt);
  if (claimSqFt != null) {
    attributes.push(
      row(
        "meetingSpaceClaimSqFt",
        claimSqFt,
        "MARKETING_CLAIM",
        "stored_claim",
        cp.meetingSpaceClaimLabel || profile.meetingSpace?.interpretationNote || "Marketing claim. Not a verified inventory figure."
      )
    );
  }

  const auditorium = numberOrNull(cp.auditoriumCapacityClaim ?? profile.meetingSpace?.auditoriumCapacity);
  if (auditorium != null) {
    attributes.push(
      row(
        "auditoriumCapacity",
        auditorium,
        "OFFICIAL_CLAIM",
        stewardship.sources?.[0] || config.gdiOnboarding?.officialUrl || profile.officialUrl || null,
        cp.auditoriumCapacityNote || profile.meetingSpace?.interpretationNote || "Capacity claim. Not a square-footage inventory."
      )
    );
  }

  for (const [name, value] of Object.entries(stewardship.verifiedFacts || {})) {
    attributes.push(
      row(name, value, "ARCHIVED_OFFICIAL_FACT", stewardship.sources?.[0] || null, `Retrieved ${stewardship.retrievalDate || "on an earlier preflight"}. Not copied into guestroom inventory.`)
    );
  }

  const tags = new Set(profile.attributes || []);
  const shuttle = profile.shuttle || {};
  if ((shuttle.airportShuttleDca === false || shuttle.airportShuttleIad === false) && tags.has("airport_shuttle")) {
    attributes.push(
      row(
        "airport_shuttle",
        false,
        "CONTRADICTION",
        "archived_profile",
        "A structured shuttle field is false while the attribute tag airport_shuttle is present. The tag is not treated as verified."
      )
    );
  }

  return {
    version: PROPERTY_ATTRIBUTE_COVERAGE_VERSION,
    hotelId: config.hotelId || profile.hpcHotelId || null,
    displayName: config.displayName || profile.displayName || null,
    country: config.country || profile.country || null,
    city: config.city || profile.city || null,
    currency: config.currency || cp.currency || null,
    timeZone: config.timeZone || null,
    primaryLanguage: config.locale?.primary || null,
    officialUrl: config.gdiOnboarding?.officialUrl || profile.officialUrl || null,
    fetchedThisRun: false,
    attributes,
  };
}
