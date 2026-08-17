/**
 * Canonical hotel identity + record shapes for Hotel Intelligence MCP.
 * hotel_id is opaque dhl_<ulid> — never HBX / Google / Airtable rec as canonical.
 */

import { randomBytes } from "node:crypto";

export const CANONICAL_HOTEL_VERSION = "canonical-hotel-v1";

const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

function encodeCrockford(bytes, length) {
  // Encode big-endian bits from bytes into Crockford Base32 of `length` chars.
  let bits = 0n;
  for (const b of bytes) bits = (bits << 8n) | BigInt(b);
  const totalBits = BigInt(bytes.length * 8);
  const need = BigInt(length * 5);
  if (totalBits < need) bits <<= need - totalBits;
  else if (totalBits > need) bits >>= totalBits - need;
  let out = "";
  for (let i = length - 1; i >= 0; i -= 1) {
    const idx = Number((bits >> BigInt(i * 5)) & 31n);
    out += CROCKFORD[idx];
  }
  return out;
}

/** Crockford Base32 ULID (26 chars) → dhl_<ulid>. */
export function generateDealalityHotelId(nowMs = Date.now()) {
  const time = Math.max(0, Number(nowMs) || Date.now());
  // 48-bit timestamp → 10 chars; 80-bit randomness → 16 chars
  let t = BigInt(time) & 0xffffffffffffn;
  const timeBuf = new Uint8Array(6);
  for (let i = 5; i >= 0; i -= 1) {
    timeBuf[i] = Number(t & 0xffn);
    t >>= 8n;
  }
  const timePart = encodeCrockford(timeBuf, 10);
  const randPart = encodeCrockford(randomBytes(10), 16);
  return `dhl_${timePart}${randPart}`;
}

export function isDealalityHotelId(value) {
  return /^dhl_[0-9A-HJKMNP-TV-Z]{26}$/i.test(String(value || "").trim());
}

/**
 * Empty canonical hotel shell.
 * @param {Partial<object>} [overrides]
 */
export function createEmptyCanonicalHotel(overrides = {}) {
  return {
    hotel_id: overrides.hotel_id || generateDealalityHotelId(),
    identity: {
      official_name: null,
      display_name: null,
      alternate_names: [],
      former_names: [],
      ...(overrides.identity || {}),
    },
    location: {
      address_line_1: null,
      address_line_2: null,
      city: null,
      state_region: null,
      postal_code: null,
      country: null,
      country_code: null,
      latitude: null,
      longitude: null,
      market: null,
      submarket: null,
      ...(overrides.location || {}),
    },
    property: {
      room_count: null,
      property_type: null,
      chain_scale: null,
      star_rating: null,
      opening_year: null,
      renovation_year: null,
      status: null,
      ...(overrides.property || {}),
    },
    brand: {
      brand_id: null,
      brand_name: null,
      parent_company_id: null,
      parent_company_name: null,
      independent: null,
      ...(overrides.brand || {}),
    },
    operator: {
      operator_id: null,
      operator_name: null,
      operating_structure: null,
      ...(overrides.operator || {}),
    },
    owner: {
      owner_id: null,
      owner_name: null,
      ...(overrides.owner || {}),
    },
    digital: {
      website: null,
      phone: null,
      ...(overrides.digital || {}),
    },
    verification: {
      record_confidence: null,
      last_verified_at: null,
      review_status: null,
      ...(overrides.verification || {}),
    },
    linkages: {
      airtable_record_id: null,
      property_identity_key: null,
      external_ids: [],
      ...(overrides.linkages || {}),
    },
  };
}

/**
 * Flatten canonical hotel for MVP response shape.
 * @param {object} hotel
 */
export function toMvpHotelSummary(hotel) {
  if (!hotel) return null;
  return {
    hotel_id: hotel.hotel_id,
    official_name: hotel.identity?.official_name || hotel.identity?.display_name || null,
    display_name: hotel.identity?.display_name || hotel.identity?.official_name || null,
    address_line_1: hotel.location?.address_line_1 || null,
    city: hotel.location?.city || null,
    country: hotel.location?.country || null,
    latitude: hotel.location?.latitude ?? null,
    longitude: hotel.location?.longitude ?? null,
    room_count: hotel.property?.room_count ?? null,
    brand_name: hotel.brand?.brand_name || null,
    parent_company_name: hotel.brand?.parent_company_name || null,
    website: hotel.digital?.website || null,
    phone: hotel.digital?.phone || null,
    status: hotel.property?.status || null,
    record_confidence: hotel.verification?.record_confidence ?? null,
    last_verified_at: hotel.verification?.last_verified_at || null,
    airtable_record_id: hotel.linkages?.airtable_record_id || null,
    property_identity_key: hotel.linkages?.property_identity_key || null,
  };
}
