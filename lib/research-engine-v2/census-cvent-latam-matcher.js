/**
 * Match Cvent LATAM venues to Hotel Property Census + build Medium updates /
 * Census Only Hold inserts.
 *
 * Never: Opening Date, Renovation Date, meeting rooms → Rooms / Keys, Cvent lat/lng writes.
 */

import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import {
  CVENT_AIRPORT_ASSET_CONTEXT_MAX_MI,
  CVENT_FORBIDDEN_ROOMS,
  normCventText,
} from "./census-cvent-venue-client.js";
import { MAP_ROOMS_SOURCE_TYPE } from "./census-secondary-hotel-data-policy.js";
import { INTERNAL_ONLY_INSERT_DEFAULTS } from "./census-confidence-tiered-internal-completion.js";
import { isForbiddenAutopilotField } from "./census-autopilot-field-allowlist.js";
import {
  buildCventStewardNoteExtras,
} from "./census-cvent-choice-matcher.js";

export const CVENT_LATAM_MATCHER_VERSION = "census-cvent-latam-matcher-v1";

/** Fields allowed on Cvent Census Only inserts. */
export const CVENT_INSERT_ALLOWED_FIELDS = Object.freeze([
  "Property Name",
  "Canonical Property Name",
  "Property Identity Key",
  "Current Brand",
  "Brand Family",
  "City",
  "State / Region",
  "Country",
  "Market",
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Official Property URL",
  "Source URL",
  "Source Type",
  "Source Confidence",
  "Identity Confidence",
  "Data Confidence Tier",
  "Phone",
  "Notes for Steward",
  "Hotel Description - Source Text",
  "Amenities - Source Text",
  "Property Type",
  "Asset Context",
  "Meeting Space Flag",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Production Use Status",
  "Public Display Review Status",
  "Radar Display Status",
  "Public Census Eligibility",
  "Human Review Required",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
]);

function blank(v) {
  return v == null || !String(v).trim();
}

function checkboxBlank(v) {
  return v !== true && v !== false && v !== 1 && v !== 0 && blank(v);
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function airportMiles(airportDistance) {
  if (!airportDistance || !Number.isFinite(Number(airportDistance.value))) return null;
  const v = Number(airportDistance.value);
  if (airportDistance.unit === "km") return v / 1.60934;
  return v;
}

export function cventRoomsAcceptable(rooms) {
  const n = Number(rooms);
  if (!Number.isFinite(n) || n < 10 || n > 5000) return false;
  if (CVENT_FORBIDDEN_ROOMS.includes(n)) return false;
  return true;
}

/**
 * Token overlap score for name matching (0–1).
 */
export function nameSimilarity(a, b) {
  const ta = new Set(
    normCventText(a)
      .split(/\s+/)
      .filter((t) => t.length > 2)
  );
  const tb = new Set(
    normCventText(b)
      .split(/\s+/)
      .filter((t) => t.length > 2)
  );
  if (!ta.size || !tb.size) return 0;
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter += 1;
  return inter / Math.max(ta.size, tb.size);
}

export function countriesAligned(censusCountry, venueCountry, harvestCountry) {
  const a = normCventText(censusCountry);
  const b = normCventText(venueCountry || "");
  const h = normCventText(harvestCountry || "");
  if (a && h && (a === h || a.includes(h) || h.includes(a))) {
    // Census row already scoped to harvest country
    if (!b || a === b || a.includes(b) || b.includes(a)) return true;
  }
  if (!a || !b) return false;
  if (a === b) return true;
  if (a.includes("dominican") && b.includes("dominican")) return true;
  if (a.includes("virgin") && b.includes("virgin")) return true;
  if (a.includes("turks") && b.includes("turks")) return true;
  return a.includes(b) || b.includes(a);
}

export function citiesAligned(censusCity, venue) {
  const cityCue = normCventText(censusCity);
  if (!cityCue) return false;
  const hay = normCventText(
    [venue?.addressParts?.city, venue?.address, venue?.title, venue?.sourceUrl]
      .filter(Boolean)
      .join(" ")
  );
  if (hay.includes(cityCue)) return true;
  // Soft city aliases
  if (cityCue === "mexico city" && (hay.includes("mexico city") || hay.includes("cdmx")))
    return true;
  if (cityCue.includes("punta cana") && hay.includes("punta cana")) return true;
  if (cityCue.includes("santo domingo") && hay.includes("santo domingo")) return true;
  return false;
}

/**
 * Find best census row match for a parsed venue within a country index.
 * @param {object} venue
 * @param {Array<{ id: string, fields: Record<string, unknown> }>} censusRows
 * @param {{ harvestCountry?: string }} [opts]
 */
export function matchCventVenueToCensus(venue, censusRows = [], opts = {}) {
  const title = String(venue?.title || "").trim();
  if (!title) return { ok: false, reason: "no_venue_title", match: null };

  let best = null;
  for (const row of censusRows) {
    const f = row.fields || {};
    if (
      !countriesAligned(
        f.Country,
        venue.addressParts?.country,
        opts.harvestCountry
      )
    ) {
      continue;
    }
    const nameScore = Math.max(
      nameSimilarity(title, f["Property Name"]),
      nameSimilarity(title, f["Canonical Property Name"])
    );
    if (nameScore < 0.45) continue;
    const cityOk =
      citiesAligned(f.City, venue) ||
      (!f.City && Boolean(venue.addressParts?.city));
    // Require city when census has city
    if (f.City && !citiesAligned(f.City, venue)) continue;
    const score = nameScore + (cityOk ? 0.15 : 0);
    if (!best || score > best.score) {
      best = { score, nameScore, cityOk, row, fields: f };
    }
  }

  if (!best) return { ok: false, reason: "no_match", match: null };
  if (best.score < 0.55) {
    return { ok: false, reason: "weak_match", match: best };
  }
  return { ok: true, reason: "matched", match: best };
}

/**
 * Near-duplicate check before insert (same country index).
 */
export function hasNearDuplicateCensusRow(venue, censusRows = [], opts = {}) {
  const m = matchCventVenueToCensus(venue, censusRows, opts);
  if (m.ok) return { near: true, match: m.match, reason: "matched_existing" };
  if (m.match && m.match.score >= 0.45) {
    return { near: true, match: m.match, reason: "weak_near_duplicate" };
  }
  // Also check identity key collision on cvent uuid
  const uuid = venue?.venueUuid;
  if (uuid) {
    const key = `cvent_${uuid.replace(/-/g, "").slice(0, 48)}`;
    const hit = censusRows.find(
      (r) => String(r.fields?.["Property Identity Key"] || "") === key
    );
    if (hit) return { near: true, match: { row: hit, score: 1 }, reason: "identity_key_exists" };
  }
  return { near: false, match: null, reason: null };
}

/**
 * Blank-only Medium update patch for a matched census row.
 */
export function buildCventLatamUpdatePatch(fields, venue, sourceUrl, opts = {}) {
  const today = opts.today || todayIsoDate();
  /** @type {Record<string, unknown>} */
  const patch = {};
  const reasons = [];

  const addressCandidate =
    venue.addressParts?.street && isStreetLevelAddress(venue.addressParts.street)
      ? venue.addressParts.street
      : venue.address;

  if (blank(fields.Address) && addressCandidate && isStreetLevelAddress(addressCandidate)) {
    patch.Address = addressCandidate;
    patch["Address Confidence"] = "Medium";
    patch["Address Source URL"] = sourceUrl;
    patch["Last Reviewed Date"] = today;
    reasons.push("address_from_cvent_medium");
  }

  if (blank(fields["Rooms / Keys"]) && cventRoomsAcceptable(venue.guestRooms)) {
    patch["Rooms / Keys"] = venue.guestRooms;
    patch["Rooms Confidence"] = "Medium";
    patch["Rooms Source URL"] = sourceUrl;
    patch["Rooms Source Type"] = MAP_ROOMS_SOURCE_TYPE.trusted_secondary_source;
    patch["Last Reviewed Date"] = today;
    reasons.push(`rooms_from_cvent_medium:${venue.guestRooms}`);
  } else if (blank(fields["Rooms / Keys"]) && venue.meetingRoomsCount != null) {
    reasons.push(
      `rooms_missing_meeting_rooms_not_used:${venue.meetingRoomsCount}`
    );
  }

  if (blank(fields["Official Property URL"]) && venue.website) {
    patch["Official Property URL"] = venue.website;
    patch["Last Reviewed Date"] = today;
    reasons.push("official_url_from_cvent");
  }

  if (
    blank(fields["Hotel Description - Source Text"]) &&
    venue.listingText &&
    venue.listingText.length >= 40
  ) {
    patch["Hotel Description - Source Text"] = venue.listingText;
    patch["Last Reviewed Date"] = today;
    reasons.push("description_from_cvent");
  }

  if (
    blank(fields["Amenities - Source Text"]) &&
    venue.amenitiesSourceText &&
    venue.amenitiesSourceText.length >= 8
  ) {
    patch["Amenities - Source Text"] = venue.amenitiesSourceText;
    patch["Last Reviewed Date"] = today;
    reasons.push("amenities_from_cvent");
  }

  if (checkboxBlank(fields["Meeting Space Flag"]) && venue.hasMeetingSignal) {
    patch["Meeting Space Flag"] = true;
    patch["Last Reviewed Date"] = today;
    reasons.push("meeting_space_flag_from_cvent");
  }

  if (blank(fields["Property Type"]) && venue.propertyType) {
    patch["Property Type"] = venue.propertyType;
    patch["Last Reviewed Date"] = today;
    reasons.push(`property_type_from_cvent:${venue.propertyType}`);
  }

  const mi = airportMiles(venue.airportDistance);
  if (
    blank(fields["Asset Context"]) &&
    mi != null &&
    mi <= CVENT_AIRPORT_ASSET_CONTEXT_MAX_MI
  ) {
    patch["Asset Context"] = "Airport";
    patch["Last Reviewed Date"] = today;
    reasons.push(`asset_context_airport:${mi.toFixed(2)}mi`);
  }

  if (blank(fields.Phone) && venue.phone) {
    patch.Phone = venue.phone;
    patch["Last Reviewed Date"] = today;
    reasons.push("phone_from_cvent");
  }

  // Built/Renovated → Notes only (never Opening/Renovation Date)
  const yearBits = [];
  if (venue.builtYear) yearBits.push(`built=${venue.builtYear}`);
  if (venue.renovatedYear) yearBits.push(`renovated=${venue.renovatedYear}`);
  const extrasCore = buildCventStewardNoteExtras(venue, sourceUrl);
  const yearNote = yearBits.length
    ? `cvent_years ${yearBits.join("; ")} (not written to Opening/Renovation Date)`
    : null;

  const primaryWritten = Object.keys(patch).some(
    (k) => k !== "Last Reviewed Date" && k !== "Notes for Steward"
  );
  if (primaryWritten || yearNote) {
    const prev = String(fields["Notes for Steward"] || "").trim();
    const add = [extrasCore, yearNote].filter(Boolean).join(" | ");
    if (add && !prev.includes("cvent_extras") && !prev.includes("cvent_years")) {
      patch["Notes for Steward"] = [prev, add].filter(Boolean).join("\n").slice(0, 90000);
      reasons.push("steward_note_cvent_extras");
    }
  }

  if (!Object.keys(patch).length) {
    return { ok: false, reason: "nothing_to_write", patch: null, reasons };
  }

  // Strip forbidden
  for (const k of Object.keys(patch)) {
    if (isForbiddenAutopilotField(k)) delete patch[k];
  }

  return {
    ok: true,
    patch,
    reasons,
    address_written: Boolean(patch.Address),
    rooms_written: Object.prototype.hasOwnProperty.call(patch, "Rooms / Keys"),
  };
}

/**
 * Build Census Only / Hold insert fields from a Cvent venue.
 */
export function buildCventCensusOnlyInsertFields(venue, opts = {}) {
  const today = opts.today || todayIsoDate();
  const name = String(venue?.title || "").trim();
  const country =
    String(venue?.addressParts?.country || opts.harvestCountry || "").trim() ||
    "Unknown";
  const city = String(venue?.addressParts?.city || "").trim() || "Unknown";
  const uuid = venue?.venueUuid || "";
  const sourceId = venue?.sourceId || "";
  const identityKey = uuid
    ? `cvent_${uuid.replace(/-/g, "").slice(0, 48)}`
    : sourceId
      ? `cvent_sid_${String(sourceId).replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 40)}`
      : `cvent_new_${normCventText(name).replace(/\s+/g, "_").slice(0, 40)}_${normCventText(city).replace(/\s+/g, "_").slice(0, 20)}`;

  const sourceUrl = String(opts.sourceUrl || venue?.sourceUrl || "").trim();
  const addressCandidate =
    venue?.addressParts?.street && isStreetLevelAddress(venue.addressParts.street)
      ? venue.addressParts.street
      : venue?.address;

  /** @type {Record<string, unknown>} */
  const fields = {
    "Property Name": name,
    "Canonical Property Name": name,
    "Property Identity Key": identityKey,
    City: city,
    Country: country,
    ...INTERNAL_ONLY_INSERT_DEFAULTS,
    "Source Type": "other",
    "Source Confidence": "Medium",
    "Identity Confidence": "Medium",
    "Last Reviewed Date": today,
    "Source URL": sourceUrl || undefined,
  };

  if (venue?.brand) fields["Current Brand"] = venue.brand;
  if (venue?.addressParts?.region) {
    fields["State / Region"] = venue.addressParts.region;
  }

  if (addressCandidate && isStreetLevelAddress(addressCandidate)) {
    fields.Address = addressCandidate;
    fields["Address Confidence"] = "Medium";
    if (sourceUrl) fields["Address Source URL"] = sourceUrl;
  }

  if (venue?.website) {
    fields["Official Property URL"] = venue.website;
  }

  if (cventRoomsAcceptable(venue?.guestRooms)) {
    fields["Rooms / Keys"] = venue.guestRooms;
    fields["Rooms Confidence"] = "Medium";
    if (sourceUrl) fields["Rooms Source URL"] = sourceUrl;
    fields["Rooms Source Type"] = MAP_ROOMS_SOURCE_TYPE.trusted_secondary_source;
  }

  if (venue?.listingText && venue.listingText.length >= 40) {
    fields["Hotel Description - Source Text"] = venue.listingText;
  }
  if (venue?.amenitiesSourceText && venue.amenitiesSourceText.length >= 8) {
    fields["Amenities - Source Text"] = venue.amenitiesSourceText;
  }
  if (venue?.propertyType) fields["Property Type"] = venue.propertyType;
  if (venue?.hasMeetingSignal) fields["Meeting Space Flag"] = true;

  const mi = airportMiles(venue?.airportDistance);
  if (mi != null && mi <= CVENT_AIRPORT_ASSET_CONTEXT_MAX_MI) {
    fields["Asset Context"] = "Airport";
  }

  if (venue?.phone) fields.Phone = venue.phone;

  const yearBits = [];
  if (venue?.builtYear) yearBits.push(`built=${venue.builtYear}`);
  if (venue?.renovatedYear) yearBits.push(`renovated=${venue.renovatedYear}`);
  const extras = buildCventStewardNoteExtras(venue, sourceUrl);
  fields["Notes for Steward"] = [
    "insert_provenance",
    "source=cvent_supplier_network",
    `venue_uuid=${uuid || "n/a"}`,
    `sourceId=${sourceId || "n/a"}`,
    "public_exposure=false",
    yearBits.length
      ? `cvent_years ${yearBits.join("; ")} (not Opening/Renovation Date)`
      : null,
    extras,
  ]
    .filter(Boolean)
    .join(" | ")
    .slice(0, 90000);

  for (const [k, v] of Object.entries(fields)) {
    if (v === undefined || v === null || v === "") delete fields[k];
    if (isForbiddenAutopilotField(k) || !CVENT_INSERT_ALLOWED_FIELDS.includes(k)) {
      delete fields[k];
    }
  }

  return {
    ok: Boolean(fields["Property Name"] && fields["Property Identity Key"]),
    fields,
    identity_key: identityKey,
  };
}
