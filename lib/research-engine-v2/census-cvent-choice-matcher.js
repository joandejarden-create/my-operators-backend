/**
 * Match a parsed Cvent venue to a Choice HPC census row and build Medium patches.
 * Writes Address / Rooms / Official URL / description / flags when blank.
 * Never maps meeting-room counts → Rooms / Keys.
 * Never writes Cvent lat/lng (not on Latitude source approvals) — steward note only.
 */
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import {
  CVENT_AIRPORT_ASSET_CONTEXT_MAX_MI,
  CVENT_FORBIDDEN_ROOMS,
  normCventText,
} from "./census-cvent-venue-client.js";
import { MAP_ROOMS_SOURCE_TYPE } from "./census-secondary-hotel-data-policy.js";

export const CVENT_CHOICE_MATCHER_VERSION = "census-cvent-choice-matcher-v2";

/** Stale / identity-collision keys — never autofill Address from another live code. */
export const CVENT_IDENTITY_STEWARD_KEYS = Object.freeze({
  ind_choice_mx_mx086: {
    reason: "identity_collision_live_mx226_queretaro_tecnologico",
    note: "Do not copy mx226 Cvent Address onto mx086",
  },
  ind_choice_mx_mx104: {
    reason: "likely_soft_brand_placeholder_no_cvent",
    note: "Ascend Zacatecas — steward only unless distinct Cvent venue found",
  },
});

const BRAND_TOKENS = Object.freeze([
  "sleep inn",
  "comfort inn",
  "quality inn",
  "radisson",
  "park inn",
  "ascend",
  "fiesta americana",
  "faranda",
  "cambria",
  "clarion",
  "econo lodge",
  "rodeway",
  "suburban",
  "woodspring",
]);

function blank(v) {
  return v == null || !String(v).trim();
}

function checkboxBlank(v) {
  return v !== true && v !== false && v !== 1 && v !== 0 && blank(v);
}

export function brandTokensFromFields(fields = {}) {
  const brand = normCventText(fields["Current Brand"]);
  const name = normCventText(fields["Property Name"]);
  const tokens = [];
  for (const t of BRAND_TOKENS) {
    if (brand.includes(t) || name.includes(t)) tokens.push(t);
  }
  return tokens;
}

export function cventBrandAligned(fields, venue) {
  const hay = normCventText(
    [venue?.title, venue?.brand, venue?.chain].filter(Boolean).join(" ")
  );
  const tokens = brandTokensFromFields(fields);
  if (!tokens.length) return false;
  return tokens.some((t) => hay.includes(t));
}

export function cventCityAligned(city, venue) {
  const cityCue = normCventText(city);
  if (!cityCue) return false;
  const hay = normCventText(
    [
      venue?.address,
      venue?.addressParts?.city,
      venue?.title,
      venue?.sourceUrl,
    ]
      .filter(Boolean)
      .join(" ")
  );
  if (hay.includes(cityCue)) return true;
  if (cityCue === "cabo san lucas" && (hay.includes("los cabos") || hay.includes("cabo")))
    return true;
  if (cityCue === "tuxtla gutierrez" && hay.includes("tuxtla")) return true;
  if (cityCue === "mexico city" && (hay.includes("mexico city") || hay.includes("cdmx")))
    return true;
  if (cityCue === "panama city" && hay.includes("panama")) return true;
  if (
    (cityCue === "santiago de queretaro" || cityCue === "queretaro") &&
    hay.includes("queretaro")
  ) {
    return true;
  }
  if (cityCue === "ciudad obregon" && hay.includes("obregon")) return true;
  if (
    (cityCue === "san pedro tlaquepaque" || cityCue === "tlaquepaque") &&
    (hay.includes("tlaquepaque") || hay.includes("guadalajara") || hay.includes("tapatio"))
  ) {
    return true;
  }
  if (cityCue === "delicias" || cityCue === "ciudad delicias") {
    if (hay.includes("delicias")) return true;
  }
  return false;
}

export function cventRoomsAcceptable(rooms) {
  const n = Number(rooms);
  if (!Number.isFinite(n) || n < 10 || n > 5000) return false;
  if (CVENT_FORBIDDEN_ROOMS.includes(n)) return false;
  return true;
}

function airportMiles(airportDistance) {
  if (!airportDistance || !Number.isFinite(Number(airportDistance.value))) return null;
  const v = Number(airportDistance.value);
  if (airportDistance.unit === "km") return v / 1.60934;
  return v;
}

/**
 * Structured steward note for Cvent extras we do not autofill onto schema fields.
 */
export function buildCventStewardNoteExtras(venue, sourceUrl) {
  const bits = [];
  if (venue?.addressParts?.postalCode) {
    bits.push(`postal=${venue.addressParts.postalCode}`);
  }
  if (venue?.airportDistance?.raw) {
    bits.push(`airport=${venue.airportDistance.raw}`);
  } else if (venue?.airportDistance?.value != null) {
    bits.push(
      `airport=${venue.airportDistance.value}${venue.airportDistance.unit || ""}`
    );
  }
  if (venue?.meetingRoomsCount != null) {
    bits.push(`meeting_rooms=${venue.meetingRoomsCount}`);
  }
  if (venue?.totalMeetingSpace?.raw) {
    bits.push(`meeting_space=${venue.totalMeetingSpace.raw}`);
  }
  if (venue?.suites != null) {
    bits.push(`suites=${venue.suites}`);
  }
  if (venue?.latitude != null && venue?.longitude != null) {
    bits.push(
      `cvent_coords=${venue.latitude},${venue.longitude} (not written — Latitude source policy)`
    );
  }
  if (venue?.sourceId) bits.push(`cvent_sourceId=${venue.sourceId}`);
  if (!bits.length) return null;
  return `cvent_extras ${bits.join("; ")} src=${sourceUrl || venue?.sourceUrl || ""}`.trim();
}

/**
 * Validate venue against census fields.
 * @returns {{ ok: true, reasons: string[] } | { ok: false, reason: string }}
 */
export function acceptCventVenueForRow(fields, venue) {
  if (!venue) return { ok: false, reason: "no_venue" };
  if (!venue.choiceAffiliated) {
    return { ok: false, reason: "not_choice_affiliated" };
  }
  if (!cventBrandAligned(fields, venue)) {
    return { ok: false, reason: "brand_mismatch" };
  }
  if (!cventCityAligned(fields.City, venue)) {
    return { ok: false, reason: "city_mismatch" };
  }
  const key = String(fields["Property Identity Key"] || "").trim();
  if (key === "ind_choice_mx_mx086" && venue.address) {
    const addr = normCventText(venue.address);
    if (addr.includes("tecnologico") || addr.includes("1001")) {
      return { ok: false, reason: "mx086_blocked_mx226_address" };
    }
  }
  return {
    ok: true,
    reasons: ["choice_chain", "brand_aligned", "city_aligned"],
  };
}

/**
 * Build Airtable patch for blank Census fields from Cvent venue parse.
 * @param {object} fields — census fields
 * @param {object} venue — parsed Cvent venue
 * @param {string} sourceUrl
 * @param {{ today?: string }} [opts]
 */
export function buildCventChoicePatch(fields, venue, sourceUrl, opts = {}) {
  const today = opts.today || new Date().toISOString().slice(0, 10);
  const key = String(fields["Property Identity Key"] || "").trim();

  const accept = acceptCventVenueForRow(fields, venue);
  if (!accept.ok) {
    return {
      ok: false,
      steward: true,
      reason: accept.reason,
      patch: null,
      reasons: [accept.reason],
    };
  }

  /** @type {Record<string, unknown>} */
  const patch = {};
  const reasons = [...accept.reasons];

  // Address — mx086 may fill non-Tecnológico addresses; only block Tecnológico/1001 collisions
  const addressCandidate =
    venue.addressParts?.street && isStreetLevelAddress(venue.addressParts.street)
      ? venue.addressParts.street
      : venue.address;
  if (blank(fields.Address) && addressCandidate && isStreetLevelAddress(addressCandidate)) {
    const addrNorm = normCventText(addressCandidate);
    if (
      key === "ind_choice_mx_mx086" &&
      (addrNorm.includes("tecnologico") || addrNorm.includes("1001"))
    ) {
      reasons.push("address_skipped:mx086_mx226_tecnologico_collision");
    } else {
      patch.Address = addressCandidate;
      patch["Address Confidence"] = "Medium";
      patch["Address Source URL"] = sourceUrl;
      patch["Last Reviewed Date"] = today;
      reasons.push("address_from_cvent_medium");
    }
  } else if (blank(fields.Address) && addressCandidate) {
    reasons.push("address_not_street_level");
  }

  // Rooms — guest/sleeping rooms only
  if (blank(fields["Rooms / Keys"])) {
    if (cventRoomsAcceptable(venue.guestRooms)) {
      patch["Rooms / Keys"] = venue.guestRooms;
      patch["Rooms Confidence"] = "Medium";
      patch["Rooms Source URL"] = sourceUrl;
      patch["Rooms Source Type"] = MAP_ROOMS_SOURCE_TYPE.trusted_secondary_source;
      patch["Last Reviewed Date"] = today;
      reasons.push(`rooms_from_cvent_medium:${venue.guestRooms}`);
    } else if (venue.guestRooms != null) {
      reasons.push(`rooms_rejected:${venue.guestRooms}`);
    } else if (venue.meetingRoomsCount != null) {
      reasons.push(
        `rooms_missing_on_venue_meeting_rooms_not_used:${venue.meetingRoomsCount}`
      );
    } else {
      reasons.push("rooms_missing_on_venue");
    }
  } else {
    const existing = Number(fields["Rooms / Keys"]);
    const conf = String(fields["Rooms Confidence"] || "");
    if (
      cventRoomsAcceptable(venue.guestRooms) &&
      Number.isFinite(existing) &&
      existing !== venue.guestRooms &&
      conf === "High"
    ) {
      return {
        ok: false,
        steward: true,
        reason: "rooms_conflict_existing_high",
        patch: {
          "Rooms Confidence": "Hold",
          "Rooms Source Type": MAP_ROOMS_SOURCE_TYPE.steward_review,
          "Rooms Notes": `cvent_conflict existing=${existing} cvent=${venue.guestRooms} ${sourceUrl}`,
          "Last Reviewed Date": today,
        },
        reasons: ["rooms_conflict_existing_high"],
        conflict: true,
      };
    }
    reasons.push("rooms_already_present");
  }

  // Official property website surfaced on Cvent listing (non-cvent host)
  if (blank(fields["Official Property URL"]) && venue.website) {
    patch["Official Property URL"] = venue.website;
    patch["Last Reviewed Date"] = today;
    reasons.push("official_url_from_cvent_listing_website");
  }

  // Source listing description (raw — not AI summary)
  if (
    blank(fields["Hotel Description - Source Text"]) &&
    venue.listingText &&
    venue.listingText.length >= 40
  ) {
    patch["Hotel Description - Source Text"] = venue.listingText;
    patch["Last Reviewed Date"] = today;
    reasons.push("description_source_text_from_cvent_listing");
  }

  // Meeting space flag only — never write meeting counts to Rooms / Keys
  if (checkboxBlank(fields["Meeting Space Flag"]) && venue.hasMeetingSignal) {
    patch["Meeting Space Flag"] = true;
    patch["Last Reviewed Date"] = today;
    reasons.push("meeting_space_flag_from_cvent");
  }

  // Property type from venueTypeId
  if (blank(fields["Property Type"]) && venue.propertyType) {
    patch["Property Type"] = venue.propertyType;
    patch["Last Reviewed Date"] = today;
    reasons.push(`property_type_from_cvent:${venue.propertyType}`);
  }

  // Asset Context Airport when Cvent distance is close
  const mi = airportMiles(venue.airportDistance);
  if (
    blank(fields["Asset Context"]) &&
    mi != null &&
    mi <= CVENT_AIRPORT_ASSET_CONTEXT_MAX_MI
  ) {
    patch["Asset Context"] = "Airport";
    patch["Last Reviewed Date"] = today;
    reasons.push(`asset_context_airport_from_cvent:${mi.toFixed(2)}mi`);
  }

  // Phone — rare on Cvent; Medium via Notes for Steward (no Phone Confidence field)
  if (blank(fields.Phone) && venue.phone) {
    patch.Phone = venue.phone;
    patch["Last Reviewed Date"] = today;
    reasons.push("phone_from_cvent_medium");
  }

  const extras = buildCventStewardNoteExtras(venue, sourceUrl);
  const primaryWritten =
    Boolean(patch.Address) ||
    Object.prototype.hasOwnProperty.call(patch, "Rooms / Keys") ||
    Boolean(patch["Official Property URL"]) ||
    Boolean(patch["Hotel Description - Source Text"]) ||
    Boolean(patch["Meeting Space Flag"]) ||
    Boolean(patch["Property Type"]) ||
    Boolean(patch["Asset Context"]) ||
    Boolean(patch.Phone);

  if (extras && primaryWritten) {
    const prev = String(fields["Notes for Steward"] || "").trim();
    const next = prev.includes("cvent_extras")
      ? prev
      : [prev, extras].filter(Boolean).join("\n").slice(0, 90000);
    if (next !== prev) {
      patch["Notes for Steward"] = next;
      reasons.push("steward_note_cvent_extras");
    }
  }

  if (!Object.keys(patch).length) {
    return {
      ok: false,
      steward: true,
      reason: CVENT_IDENTITY_STEWARD_KEYS[key]?.reason || "nothing_to_write",
      patch: null,
      reasons,
    };
  }

  return {
    ok: true,
    steward: false,
    patch,
    reasons,
    address_written: Boolean(patch.Address),
    rooms_written: Object.prototype.hasOwnProperty.call(patch, "Rooms / Keys"),
    official_url_written: Boolean(patch["Official Property URL"]),
    description_written: Boolean(patch["Hotel Description - Source Text"]),
    meeting_flag_written: Boolean(patch["Meeting Space Flag"]),
  };
}
