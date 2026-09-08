/**
 * Packet 2.6C publishing hotfix — canonical ReportHotelIdentity for all report types.
 * One resolver; adapters must not invent location strings.
 * Standalone module (no hotel-seed import) to avoid circular ESM init.
 */

import { KGPV_HOTEL_ID } from "../research/backfill-kgpv.js";

/** Customer-facing geography when no usable property location exists. */
export const LOCATION_NOT_AVAILABLE = "Location not available";

/** @deprecated Never use as production fallback — retained only for QA detection. */
export const LOCATION_PENDING_FORBIDDEN = "Location pending";

const HOTEL_ID_ALIASES = Object.freeze({
  kgpv: KGPV_HOTEL_ID,
  "kgpv-full": KGPV_HOTEL_ID,
  dossier_kgpv: KGPV_HOTEL_ID,
});

/** Canonical customer display locations (Hotel Explorer convention). */
const DISPLAY_LOCATION_BY_HOTEL = Object.freeze({
  [KGPV_HOTEL_ID]: {
    hotel_name: "Krystal Grand Puerto Vallarta",
    city: "Puerto Vallarta",
    locality: null,
    state_region: "Jalisco",
    country: "Mexico",
    display_location: "Puerto Vallarta, Mexico",
  },
  recIwaP1etgx2g9nA: {
    hotel_name: "Cambridge Beaches Resort & Spa",
    city: null,
    locality: "Sandys Parish",
    state_region: "Sandys",
    country: "Bermuda",
    display_location: "Sandys Parish, Bermuda",
  },
  // Mexico Explorer Full HI — cover location must never fall through to "Location not available"
  recsYJb2R1jarPpK3: {
    hotel_name: "Sheraton Guadalajara Expo",
    city: "Zapopan",
    locality: null,
    state_region: "Jalisco",
    country: "Mexico",
    display_location: "Zapopan, Mexico",
  },
  recTYaiA4S6fR6ixx: {
    hotel_name: "voco Cancún Zona Hotelera",
    city: "Cancun",
    locality: "Zona Hotelera",
    state_region: "Quintana Roo",
    country: "Mexico",
    display_location: "Cancun, Mexico",
  },
});

function normalizeHotelId(input) {
  const raw = String(input || "").trim();
  if (!raw) return "";
  if (HOTEL_ID_ALIASES[raw]) return HOTEL_ID_ALIASES[raw];
  return raw;
}

function isForbiddenLocationLabel(label) {
  const s = String(label || "").trim();
  if (!s) return true;
  return /^(location\s+pending|location\s+not\s+available|pending|unknown|n\/?a|—|-)$/i.test(s);
}

function composeDisplayLocation({ city, locality, state_region, country }) {
  const place = city || locality || state_region || null;
  if (place && country) return `${place}, ${country}`;
  if (place) return String(place);
  if (country) return String(country);
  return null;
}

function pickLocationParts(input = {}) {
  const existing = input.hotel_location || input.location || null;
  const profile = input.property_profile || input.hotel?.property_profile || null;
  return {
    city:
      existing?.city ||
      input.city ||
      profile?.city ||
      input.hotel?.city ||
      null,
    locality:
      existing?.locality ||
      input.locality ||
      profile?.locality ||
      profile?.submarket ||
      null,
    state_region:
      existing?.state_region ||
      existing?.region ||
      input.state_region ||
      profile?.state_region ||
      profile?.state ||
      null,
    country:
      existing?.country ||
      input.country ||
      profile?.country ||
      input.hotel?.country ||
      null,
  };
}

/**
 * Resolve customer-facing ReportHotelIdentity from hotel ids + optional dossier fields.
 */
export function resolveReportHotelIdentity(input = {}) {
  const hotelId = normalizeHotelId(
    input.hotel_id ||
      input.hotelId ||
      input.hotel_airtable_record_id ||
      input.airtable_record_id ||
      input.recordId ||
      ""
  );
  const known = DISPLAY_LOCATION_BY_HOTEL[hotelId] || null;
  const existing = input.hotel_location || input.location || null;
  const parts = pickLocationParts(input);

  const city = known?.city || parts.city || null;
  const locality = known?.locality || parts.locality || null;
  const state_region = known?.state_region || parts.state_region || null;
  const country = known?.country || parts.country || null;

  let display_location = known?.display_location || input.display_location || null;

  if (!display_location || isForbiddenLocationLabel(display_location)) {
    display_location = composeDisplayLocation({ city, locality, state_region, country });
  }

  if (
    (!display_location || isForbiddenLocationLabel(display_location)) &&
    existing?.label &&
    !isForbiddenLocationLabel(existing.label)
  ) {
    display_location = String(existing.label).trim();
  }

  if (!display_location || isForbiddenLocationLabel(display_location)) {
    display_location = LOCATION_NOT_AVAILABLE;
  }

  return {
    hotel_id: hotelId || null,
    hotel_name: input.hotel_name || input.hotelName || known?.hotel_name || null,
    city: city || null,
    locality: locality || null,
    state_region: state_region || null,
    country: country || null,
    display_location,
    location_available: display_location !== LOCATION_NOT_AVAILABLE,
  };
}

/**
 * Attach canonical identity + hotel_location onto a report dossier (returns copy).
 */
export function enrichReportForPublishing(dossier) {
  if (!dossier || typeof dossier !== "object") return dossier;
  const identity = resolveReportHotelIdentity({
    hotel_id: dossier.hotel_id,
    hotel_airtable_record_id: dossier.hotel_airtable_record_id,
    hotel_name: dossier.hotel_name,
    hotel_location: dossier.hotel_location,
    property_profile: dossier.property_profile,
    city: dossier.city,
    country: dossier.country,
    state_region: dossier.state_region,
    locality: dossier.locality,
    display_location: dossier.display_location,
  });

  const out = { ...dossier };
  out.report_hotel_identity = {
    hotel_id: identity.hotel_id,
    hotel_name: identity.hotel_name,
    city: identity.city,
    locality: identity.locality,
    state_region: identity.state_region,
    country: identity.country,
    display_location: identity.display_location,
  };
  out.hotel_location = {
    label: identity.display_location,
    city: identity.city || identity.locality || null,
    locality: identity.locality || null,
    state_region: identity.state_region || null,
    country: identity.country || null,
  };
  if (
    (!out.hotel_airtable_record_id || !String(out.hotel_airtable_record_id).startsWith("rec")) &&
    identity.hotel_id &&
    String(identity.hotel_id).startsWith("rec")
  ) {
    out.hotel_airtable_record_id = identity.hotel_id;
  }
  return out;
}

/**
 * QA: known geography must not resolve to pending/blank.
 */
export function assertReportLocationCanonical(dossier) {
  const identity = resolveReportHotelIdentity(dossier || {});
  const errors = [];
  const label = dossier?.hotel_location?.label || identity.display_location;
  if (/location\s+pending/i.test(String(label || ""))) {
    errors.push("NO_LOCATION_PENDING_WHEN_KNOWN");
  }
  const hotelId = normalizeHotelId(
    dossier?.hotel_id || dossier?.hotel_airtable_record_id || ""
  );
  if (DISPLAY_LOCATION_BY_HOTEL[hotelId]) {
    if (!identity.location_available) {
      errors.push("REPORT_LOCATION_CANONICAL");
    }
    if (identity.display_location !== DISPLAY_LOCATION_BY_HOTEL[hotelId].display_location) {
      errors.push(
        `REPORT_LOCATION_MISMATCH:${identity.display_location}!=${DISPLAY_LOCATION_BY_HOTEL[hotelId].display_location}`
      );
    }
  }
  return {
    ok: errors.length === 0,
    errors,
    display_location: identity.display_location,
    identity,
  };
}

export { DISPLAY_LOCATION_BY_HOTEL, HOTEL_ID_ALIASES, normalizeHotelId };
