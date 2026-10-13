/**
 * Contact Intelligence V1 — phone normalization (canonical export).
 * Reuses V2 E.164 helpers; does not invent country codes when ambiguous.
 */

import {
  normalizePhoneE164,
  classifyPhoneType as classifyPhoneTypeV2,
  isLikelyFalsePhone,
} from "./owner-contact-resolution-v2/phone-resolution.js";
import { CONTACT_TYPE } from "./vocabulary.js";

export const PHONE_NORMALIZE_VERSION = "phone-normalize-v1";

/**
 * @returns {{ raw_value, normalized_E164, country, extension, phone_type, safe, source, last_verified }}
 */
export function normalizeBusinessPhone(raw, {
  country = null,
  extension = null,
  phone_type = null,
  context = "",
  is_hotel_phone = false,
  person_tied = false,
  source = null,
  last_verified = null,
} = {}) {
  const original = String(raw || "").trim();
  if (!original || isLikelyFalsePhone(original)) {
    return {
      raw_value: original || null,
      normalized_E164: null,
      country: country || null,
      extension: extension || null,
      phone_type: phone_type || "UNKNOWN",
      safe: false,
      source,
      last_verified,
      version: PHONE_NORMALIZE_VERSION,
    };
  }

  const norm = normalizePhoneE164(original, { country });
  const type =
    phone_type ||
    classifyPhoneTypeV2({
      raw: original,
      context,
      is_hotel_phone,
      person_tied,
    });

  return {
    raw_value: original,
    normalized_E164: norm.e164,
    country: country || null,
    extension: extension || null,
    phone_type: type,
    safe: Boolean(norm.safe && norm.e164),
    source,
    last_verified,
    version: PHONE_NORMALIZE_VERSION,
    /** Ambiguous country: do not invent CC */
    country_assumed: Boolean(country) && !String(original).trim().startsWith("+"),
  };
}

/**
 * Map phone path priority for "best legitimate business contact path".
 * Lower = better.
 */
export function phonePathPriority(phone_type) {
  const order = {
    DIRECT_MOBILE: 1,
    DIRECT_OFFICE: 1,
    EXECUTIVE_OFFICE: 2,
    DEVELOPMENT_OFFICE: 2,
    CORPORATE_PHONE: 3,
    REGIONAL_OFFICE: 4,
    HOTEL_PHONE: 5,
    MAIN_ORGANIZATION: 5,
  };
  return order[String(phone_type || "").toUpperCase()] ?? 99;
}

export function contactTypeForHotelPhone(hint) {
  const h = String(hint || "").toLowerCase();
  if (/reserv|reserva/.test(h)) return CONTACT_TYPE.HOTEL_RESERVATIONS_PHONE;
  if (/sales|ventas|group|grupo/.test(h)) return CONTACT_TYPE.HOTEL_SALES_PHONE;
  return CONTACT_TYPE.HOTEL_MAIN_PHONE;
}

export { normalizePhoneE164, isLikelyFalsePhone };
