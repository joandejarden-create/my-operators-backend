/**
 * Phone resolution + classification (Contact Resolution V2).
 * HOTEL_PHONE must never be presented as owner phone.
 */

import { PHONE_TYPE, PHONE_METHOD, CACHE_TTL } from "./vocabulary.js";
import { createEvidenceFact } from "./roles-and-sources.js";
import { SOURCE_CLASS } from "./vocabulary.js";

export const PHONE_RESOLUTION_V2 = "phone-resolution-v2";

export function normalizePhoneE164(raw, { country = null } = {}) {
  const digits = String(raw || "").replace(/[^\d+]/g, "");
  if (!digits) return { e164: null, original: raw, safe: false };
  // Only claim E.164 when already international or country known with simple rules
  if (digits.startsWith("+") && digits.length >= 10) {
    return { e164: digits, original: raw, safe: true };
  }
  const d = digits.replace(/^\+/, "");
  const cc = {
    mexico: "52",
    mx: "52",
    colombia: "57",
    co: "57",
    "dominican republic": "1",
    do: "1",
    "costa rica": "506",
    cr: "506",
  };
  const key = String(country || "").toLowerCase();
  const prefix = cc[key];
  if (prefix && d.length >= 8 && d.length <= 12) {
    return { e164: `+${prefix}${d.replace(new RegExp(`^${prefix}`), "")}`, original: raw, safe: true };
  }
  return { e164: null, original: raw, safe: false };
}

export function classifyPhoneType({ raw, context = "", is_hotel_phone = false, person_tied = false } = {}) {
  if (is_hotel_phone) return PHONE_TYPE.HOTEL_PHONE;
  const ctx = `${context} ${raw}`.toLowerCase();
  if (/whatsapp|móvil|movil|celular|mobile|cell\b/.test(ctx) && person_tied) {
    return PHONE_TYPE.DIRECT_MOBILE;
  }
  if (person_tied && /direct|directo|extension|ext\b/.test(ctx)) {
    return PHONE_TYPE.DIRECT_OFFICE;
  }
  if (/executive|presidencia|director/.test(ctx)) return PHONE_TYPE.EXECUTIVE_OFFICE;
  if (/corporate|corporativ|oficinas|headquarters|hq\b/.test(ctx)) {
    return PHONE_TYPE.CORPORATE_PHONE;
  }
  if (/reservas|reservations|front desk|concierge/.test(ctx)) return PHONE_TYPE.HOTEL_PHONE;
  return person_tied ? PHONE_TYPE.DIRECT_OFFICE : PHONE_TYPE.CORPORATE_PHONE;
}

/** Reject numbers that look like years / dates / fax-only noise. */
export function isLikelyFalsePhone(raw) {
  const d = String(raw || "").replace(/\D/g, "");
  if (d.length < 8 || d.length > 15) return true;
  if (/^(19|20)\d{2}$/.test(d)) return true;
  if (/^0+$/.test(d)) return true;
  return false;
}

export function resolveOwnerPhones({
  person = null,
  org_phones = [],
  hotel_phone = null,
  country = null,
} = {}) {
  const out = [];
  const rejected = [];

  for (const raw of org_phones || []) {
    if (isLikelyFalsePhone(raw)) {
      rejected.push({ raw, reason: "malformed_or_date_like" });
      continue;
    }
    const type = classifyPhoneType({ raw, person_tied: false });
    if (type === PHONE_TYPE.HOTEL_PHONE) {
      rejected.push({ raw, reason: "classified_hotel_phone" });
      continue;
    }
    const norm = normalizePhoneE164(raw, { country });
    out.push({
      phone: norm.e164 || raw,
      phone_original: raw,
      phone_type: type,
      phone_method: PHONE_METHOD.OWNER_ORG_CORPORATE,
      phone_confidence: "MEDIUM",
      is_owner_phone: true,
      evidence: [
        createEvidenceFact({
          claim: "org_corporate_phone",
          source_type: SOURCE_CLASS.OFFICIAL_OWNER_SITE,
          extracted_text_or_fact: raw,
        }),
      ],
      cache_ttl_ms: CACHE_TTL.verified_phone_ms,
    });
  }

  if (hotel_phone) {
    rejected.push({
      raw: hotel_phone,
      reason: "hotel_phone_not_owner_phone",
      phone_type: PHONE_TYPE.HOTEL_PHONE,
    });
  }

  // Person-tied: none from deterministic site unless future extract
  if (person?.phone_hint && !isLikelyFalsePhone(person.phone_hint)) {
    const type = classifyPhoneType({
      raw: person.phone_hint,
      context: person.title || "",
      person_tied: true,
    });
    if (type !== PHONE_TYPE.HOTEL_PHONE) {
      const norm = normalizePhoneE164(person.phone_hint, { country });
      out.unshift({
        phone: norm.e164 || person.phone_hint,
        phone_original: person.phone_hint,
        phone_type: type,
        phone_method: PHONE_METHOD.EXPLICIT_OFFICIAL_PERSON,
        phone_confidence: "HIGH",
        is_owner_phone: true,
        evidence: [
          createEvidenceFact({
            claim: "person_phone_hint",
            source_type: SOURCE_CLASS.OFFICIAL_OWNER_SITE,
            extracted_text_or_fact: person.phone_hint,
          }),
        ],
      });
    }
  }

  return {
    phones: out.slice(0, 5),
    rejected,
    useful_owner_phone: out.find((p) =>
      [PHONE_TYPE.DIRECT_MOBILE, PHONE_TYPE.DIRECT_OFFICE, PHONE_TYPE.EXECUTIVE_OFFICE, PHONE_TYPE.CORPORATE_PHONE].includes(
        p.phone_type
      )
    ) || null,
  };
}
