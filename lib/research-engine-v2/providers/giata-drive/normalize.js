/**
 * Normalize GIATA Drive Open Content property → safe internal shapes.
 * Firewall: roomTypes NEVER become room_count.
 */

import { GIATA_DRIVE_ROOMS_CAPABILITY } from "./client.js";

export const GIATA_DRIVE_NORMALIZE_VERSION = "giata-drive-normalize-v1";

function pickLocalized(names, prefer = "en") {
  if (!Array.isArray(names) || !names.length) return null;
  const pref = names.find((n) =>
    String(n.locale || "")
      .toLowerCase()
      .startsWith(prefer)
  );
  const def = names.find((n) => n.isDefault);
  const v = pref?.value || def?.value || names[0]?.value || names[0];
  return v != null ? String(v).trim() || null : null;
}

/**
 * @param {object} raw — GIATA Drive property JSON
 * @returns {object|null}
 */
export function normalizeGiataDriveProperty(raw) {
  if (!raw || typeof raw !== "object") return null;
  const giataId = raw.giataId != null ? String(raw.giataId) : null;
  const addr = Array.isArray(raw.addresses) ? raw.addresses[0] : null;
  const geo = Array.isArray(raw.geoCodes) ? raw.geoCodes[0] : null;
  const chain = Array.isArray(raw.chains) ? raw.chains[0] : null;
  const phone =
    (Array.isArray(raw.phones) &&
      raw.phones.find((p) => p.tech === "phone" || !p.tech)?.phone) ||
    null;
  const website =
    (Array.isArray(raw.urls) && raw.urls[0]?.url) || null;
  const rating =
    (Array.isArray(raw.ratings) &&
      (raw.ratings.find((r) => r.isDefault)?.value || raw.ratings[0]?.value)) ||
    null;

  const street =
    addr?.street ||
    (Array.isArray(addr?.addressLines) ? addr.addressLines.filter(Boolean).join(", ") : null) ||
    null;
  const streetNum = addr?.streetNum ? String(addr.streetNum).trim() : "";
  const address =
    street && streetNum && !String(street).includes(streetNum)
      ? `${street} ${streetNum}`.trim()
      : street;

  // Texts: pick a short English description if present
  let description = null;
  if (raw.texts && typeof raw.texts === "object") {
    const textBlob = raw.texts.en || raw.texts["en-US"] || Object.values(raw.texts)[0];
    if (typeof textBlob === "string") description = textBlob.slice(0, 2000);
    else if (textBlob && typeof textBlob === "object") {
      const first = Object.values(textBlob).find((v) => typeof v === "string");
      if (first) description = String(first).slice(0, 2000);
    }
  }

  const amenityKeys = raw.facts && typeof raw.facts === "object" ? Object.keys(raw.facts) : [];

  return {
    normalize_version: GIATA_DRIVE_NORMALIZE_VERSION,
    rooms_capability: GIATA_DRIVE_ROOMS_CAPABILITY.status,
    giata_id: giataId,
    name: pickLocalized(raw.names),
    city: pickLocalized(raw.city?.names) || addr?.cityName || null,
    destination: pickLocalized(raw.destination?.names) || null,
    country: pickLocalized(raw.country?.names) || null,
    country_code: raw.country?.code ? String(raw.country.code).toUpperCase() : null,
    address: address || null,
    postal_code: addr?.zip ? String(addr.zip) : null,
    state_region: addr?.federalState?.name || null,
    latitude:
      geo?.latitude != null && Number.isFinite(Number(geo.latitude))
        ? Number(geo.latitude)
        : null,
    longitude:
      geo?.longitude != null && Number.isFinite(Number(geo.longitude))
        ? Number(geo.longitude)
        : null,
    geo_accuracy: geo?.accuracy || null,
    brand_name: pickLocalized(chain?.names) || null,
    parent_company_name: null, // not entitled / not in payload as parent
    star_rating: rating != null ? String(rating) : null,
    website: website || null,
    phone: phone ? String(phone) : null,
    description,
    amenity_fact_keys: amenityKeys.slice(0, 50),
    image_count: Array.isArray(raw.images) ? raw.images.length : 0,
    room_types_count: Array.isArray(raw.roomTypes) ? raw.roomTypes.length : 0,
    // HARD FIREWALL
    room_count: null,
    supplier_ids: null,
    booking_id: null,
    hotelbeds_id: null,
    expedia_id: null,
  };
}

/**
 * Extract giataId from a property detail URL.
 */
export function giataIdFromUrl(url) {
  const m = String(url || "").match(/\/properties\/(\d+)\s*$/);
  return m ? m[1] : null;
}
