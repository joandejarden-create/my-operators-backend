/**
 * Verified business / mobile phone classification for GTM owner contacts.
 *
 * Tiers (single best tier on contact — VP2 beats VP1 beats VP3):
 * - VP2: Person mobile/cell on proof URL (LinkedIn, IR bio, registry)
 * - VP1: Person direct office line on proof URL (not entity switchboard)
 * - VP3: Entity HQ / toll-free switchboard — useful reference, not person-verified
 */
import { MAP_GTM_CONTACT, VAL_GTM_PHONE_VERIFICATION_TIER } from "./contact-field-map.js";

export { VAL_GTM_PHONE_VERIFICATION_TIER };

const TOLL_FREE_RE =
  /^(?:0?0?\s*1[\s.-]?)?(?:800|888|877|866|855|844|833|880|881|882|883|884|885|886|887|889)|(?:01800|01\s*800)/;

/**
 * @param {string} phone
 */
export function normalizePhoneDigits(phone) {
  return String(phone || "").replace(/\D/g, "");
}

/**
 * @param {string} phone
 */
export function isTollFreePhone(phone) {
  const raw = String(phone || "").trim();
  if (!raw) return false;
  if (TOLL_FREE_RE.test(raw.replace(/\s/g, ""))) return true;
  const d = normalizePhoneDigits(raw);
  return /^(1?(800|888|877|866|855|844|833)|52?800)/.test(d);
}

/**
 * @param {string} phone
 * @param {string} [country]
 */
export function isLikelyMobilePhone(phone, country = "") {
  const d = normalizePhoneDigits(phone);
  if (!d || d.length < 10) return false;

  const c = String(country || "").toLowerCase();

  // Mexico: +52 1 XX XXXX XXXX
  if (c.includes("mexico") || d.startsWith("52")) {
    const mx = d.startsWith("52") ? d.slice(2) : d;
    if (/^1\d{10}$/.test(mx)) return true;
  }

  // Colombia: mobile often 3XX
  if (c.includes("colombia") || d.startsWith("57")) {
    const co = d.startsWith("57") ? d.slice(2) : d;
    if (/^3\d{9}$/.test(co)) return true;
  }

  // Spain: 6XX / 7XX mobile
  if (c.includes("spain") || d.startsWith("34")) {
    const es = d.startsWith("34") ? d.slice(2) : d;
    if (/^[67]\d{8}$/.test(es)) return true;
  }

  // US/Canada: no reliable heuristic — require explicit mobilePhone field
  return false;
}

/**
 * @param {string} a
 * @param {string} b
 */
export function phonesMatch(a, b) {
  const da = normalizePhoneDigits(a);
  const db = normalizePhoneDigits(b);
  if (!da || !db) return false;
  if (da === db) return true;
  const minLen = Math.min(da.length, db.length);
  if (minLen >= 10 && da.slice(-minLen) === db.slice(-minLen)) return true;
  return false;
}

/**
 * @param {string} phone
 * @param {string} entitySwitchboard
 */
export function isEntitySwitchboardPhone(phone, entitySwitchboard) {
  if (!phone || !entitySwitchboard) return false;
  return phonesMatch(phone, entitySwitchboard);
}

/**
 * @param {string | null | undefined} tier
 */
export function isVerifiedPersonPhoneTier(tier) {
  return tier === "VP1" || tier === "VP2";
}

/**
 * @param {object} input
 * @param {string} [input.phone]
 * @param {string} [input.businessPhone]
 * @param {string} [input.mobilePhone]
 * @param {string} [input.phoneType] "mobile" | "business" | "switchboard"
 * @param {string} [input.businessPhoneTier]
 * @param {string} [input.mobilePhoneTier]
 * @param {string} [input.phoneVerificationTier]
 * @param {string} [input.verificationUrl]
 * @param {string} [input.name]
 * @param {string} [input.country]
 * @param {string} [input.entitySwitchboardPhone]
 */
export function resolveContactPhoneFields(input = {}) {
  const verificationUrl = String(input.verificationUrl || input.phoneVerificationUrl || "").trim();
  const entitySwitchboard = String(input.entitySwitchboardPhone || "").trim();
  const country = String(input.country || "").trim();
  const contactName = String(input.name || "").trim();

  let businessPhone = String(input.businessPhone || "").trim() || null;
  let mobilePhone = String(input.mobilePhone || "").trim() || null;
  let legacyPhone = String(input.phone || "").trim() || null;

  if (!businessPhone && !mobilePhone && legacyPhone) {
    const explicitType = String(input.phoneType || "").toLowerCase();
    if (explicitType === "mobile" || isLikelyMobilePhone(legacyPhone, country)) {
      mobilePhone = legacyPhone;
    } else {
      businessPhone = legacyPhone;
    }
    legacyPhone = null;
  }

  let businessTier = input.businessPhoneTier || null;
  let mobileTier = input.mobilePhoneTier || null;

  if (businessPhone && !businessTier) {
    businessTier = inferBusinessPhoneTier(businessPhone, {
      entitySwitchboardPhone: entitySwitchboard,
      verificationUrl,
      contactName,
      country,
    });
  }

  if (mobilePhone && !mobileTier) {
    mobileTier = inferMobilePhoneTier(mobilePhone, {
      verificationUrl,
      contactName,
      country,
      explicitMobile:
        String(input.phoneType || "").toLowerCase() === "mobile" ||
        input.mobilePhoneTier === "VP2",
    });
  }

  let phoneVerificationTier = input.phoneVerificationTier || null;
  if (!phoneVerificationTier) {
    phoneVerificationTier = pickBestPhoneVerificationTier(mobileTier, businessTier);
  }

  const primaryOutreachPhone = pickPrimaryOutreachPhone({
    mobilePhone,
    mobileTier,
    businessPhone,
    businessTier,
    phone: legacyPhone,
  });

  /** @type {Record<string, unknown>} */
  const fields = {};

  if (businessPhone) {
    fields[MAP_GTM_CONTACT.businessPhone] = businessPhone;
  }
  if (mobilePhone) {
    fields[MAP_GTM_CONTACT.mobilePhone] = mobilePhone;
  }
  if (phoneVerificationTier && VAL_GTM_PHONE_VERIFICATION_TIER.includes(phoneVerificationTier)) {
    fields[MAP_GTM_CONTACT.phoneVerificationTier] = phoneVerificationTier;
  }
  if (primaryOutreachPhone) {
    fields[MAP_GTM_CONTACT.phone] = primaryOutreachPhone;
  }

  return {
    businessPhone,
    businessPhoneTier: businessTier,
    mobilePhone,
    mobilePhoneTier: mobileTier,
    phoneVerificationTier,
    primaryOutreachPhone,
    hasVerifiedBusinessPhone: businessTier === "VP1",
    hasVerifiedMobilePhone: mobileTier === "VP2",
    hasVerifiedPersonPhone: isVerifiedPersonPhoneTier(mobileTier) || isVerifiedPersonPhoneTier(businessTier),
    fields,
  };
}

/**
 * @param {string} phone
 * @param {object} ctx
 */
export function inferBusinessPhoneTier(phone, ctx = {}) {
  const verificationUrl = String(ctx.verificationUrl || "").trim();
  if (!phone) return null;
  if (!verificationUrl) return null;

  if (isTollFreePhone(phone)) return "VP3";

  const entitySwitchboard = String(ctx.entitySwitchboardPhone || "").trim();
  if (entitySwitchboard && isEntitySwitchboardPhone(phone, entitySwitchboard)) return "VP3";
  if (isLikelyMobilePhone(phone, ctx.country)) return null;

  // Without switchboard reference we cannot confirm a direct line — store as entity-class only.
  if (!entitySwitchboard) return "VP3";

  return "VP1";
}

/**
 * @param {string} phone
 * @param {object} ctx
 */
export function inferMobilePhoneTier(phone, ctx = {}) {
  const verificationUrl = String(ctx.verificationUrl || "").trim();
  if (!phone || !verificationUrl) return null;
  if (isTollFreePhone(phone)) return null;
  if (isEntitySwitchboardPhone(phone, ctx.entitySwitchboardPhone || "")) return null;

  const country = ctx.country || "";
  if (isLikelyMobilePhone(phone, country)) return "VP2";

  // Explicit mobile from trusted enrichment source
  if (ctx.explicitMobile) return "VP2";

  return null;
}

/**
 * @param {string | null} mobileTier
 * @param {string | null} businessTier
 */
export function pickBestPhoneVerificationTier(mobileTier, businessTier) {
  if (mobileTier === "VP2") return "VP2";
  if (businessTier === "VP1") return "VP1";
  if (businessTier === "VP3" || mobileTier === "VP3") return "VP3";
  return null;
}

/**
 * @param {object} phones
 */
export function pickPrimaryOutreachPhone(phones) {
  if (phones.mobilePhone && phones.mobileTier === "VP2") return phones.mobilePhone;
  if (phones.businessPhone && phones.businessPhoneTier === "VP1") return phones.businessPhone;
  if (phones.mobilePhone && phones.mobileTier === "VP2") return phones.mobilePhone;
  if (phones.businessPhone) return phones.businessPhone;
  return phones.phone || null;
}

/**
 * @param {object} contact
 */
export function formatContactPhonesForDisplay(contact = {}) {
  const parts = [];
  if (contact.mobilePhone && contact.mobilePhoneTier === "VP2") {
    parts.push(`mobile ${contact.mobilePhone} (VP2)`);
  }
  if (contact.businessPhone) {
    const label = contact.businessPhoneTier === "VP1" ? "direct" : "HQ";
    parts.push(`${label} ${contact.businessPhone} (${contact.businessPhoneTier || "?"})`);
  } else if (contact.phone && !parts.length) {
    parts.push(contact.phone);
  }
  return parts.join("; ");
}
