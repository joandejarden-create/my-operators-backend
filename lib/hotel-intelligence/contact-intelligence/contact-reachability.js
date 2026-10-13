/**
 * Contact reachability helpers — reusable across Dealality products (GDI, Owner Intel, etc.).
 * Classifies email/phone types and verification posture without inventing data.
 * Does not send test emails or use invasive verification.
 */

import {
  isGenericMailboxEmail,
  isNamedPersonEmail,
  isRoleMailboxEmail,
  emailLocalPart,
} from "./dimensions.js";

export const REACHABILITY_EMAIL_TYPE = Object.freeze({
  DIRECT_WORK: "DIRECT_WORK",
  ROLE_BASED: "ROLE_BASED",
  GENERIC_ORGANIZATION: "GENERIC_ORGANIZATION",
  INFERRED: "INFERRED",
  UNKNOWN: "UNKNOWN",
});

export const REACHABILITY_EMAIL_VERIFICATION = Object.freeze({
  OFFICIAL_SOURCE_VERIFIED: "OFFICIAL_SOURCE_VERIFIED",
  PROVIDER_VERIFIED: "PROVIDER_VERIFIED",
  MULTI_SOURCE_CONFIRMED: "MULTI_SOURCE_CONFIRMED",
  DOMAIN_PATTERN_INFERRED: "DOMAIN_PATTERN_INFERRED",
  UNVERIFIED: "UNVERIFIED",
  INVALID: "INVALID",
});

export const REACHABILITY_PHONE_TYPE = Object.freeze({
  DIRECT: "DIRECT",
  OFFICE: "OFFICE",
  MOBILE_PUBLIC_PROFESSIONAL: "MOBILE_PUBLIC_PROFESSIONAL",
  EVENT_LINE: "EVENT_LINE",
  MAIN_ORGANIZATION: "MAIN_ORGANIZATION",
  UNKNOWN: "UNKNOWN",
});

const ROLE_LOCAL_EXTRA =
  /^(tournament|registrar|meetings|events|conference|housing|hotel|group|gadinst|associatedirector|nice)$/i;

/**
 * @param {string|null|undefined} email
 * @param {{ contactName?: string|null, inferred?: boolean, claimedOfficial?: boolean }} [opts]
 */
export function classifyEmailType(email, opts = {}) {
  const em = String(email || "").trim().toLowerCase();
  if (!em || !em.includes("@")) return REACHABILITY_EMAIL_TYPE.UNKNOWN;
  if (opts.inferred) return REACHABILITY_EMAIL_TYPE.INFERRED;
  if (isGenericMailboxEmail(em)) return REACHABILITY_EMAIL_TYPE.GENERIC_ORGANIZATION;
  const local = emailLocalPart(em);
  if (isRoleMailboxEmail(em) || ROLE_LOCAL_EXTRA.test(local.replace(/[._-]/g, ""))) {
    return REACHABILITY_EMAIL_TYPE.ROLE_BASED;
  }
  if (opts.contactName && isNamedPersonEmail(em, opts.contactName)) {
    return REACHABILITY_EMAIL_TYPE.DIRECT_WORK;
  }
  // first.last@ domain without name match still treated as possible direct work if not generic
  if (/^[a-z]+[._-][a-z]+/.test(local) && !opts.inferred) {
    return REACHABILITY_EMAIL_TYPE.DIRECT_WORK;
  }
  return REACHABILITY_EMAIL_TYPE.UNKNOWN;
}

/**
 * @param {object} input
 * @param {string|null} [input.email]
 * @param {string|null} [input.contactName]
 * @param {boolean} [input.fromOfficialSource]
 * @param {boolean} [input.providerVerified]
 * @param {boolean} [input.multiSource]
 * @param {boolean} [input.inferred]
 * @param {boolean} [input.invalid]
 */
export function classifyEmailVerificationStatus(input = {}) {
  if (input.invalid) return REACHABILITY_EMAIL_VERIFICATION.INVALID;
  if (input.inferred) return REACHABILITY_EMAIL_VERIFICATION.DOMAIN_PATTERN_INFERRED;
  const type = classifyEmailType(input.email, {
    contactName: input.contactName,
    inferred: input.inferred,
  });
  if (type === REACHABILITY_EMAIL_TYPE.UNKNOWN && !input.email) {
    return REACHABILITY_EMAIL_VERIFICATION.UNVERIFIED;
  }
  if (input.multiSource && input.fromOfficialSource) {
    return REACHABILITY_EMAIL_VERIFICATION.MULTI_SOURCE_CONFIRMED;
  }
  if (input.fromOfficialSource) {
    return REACHABILITY_EMAIL_VERIFICATION.OFFICIAL_SOURCE_VERIFIED;
  }
  if (input.providerVerified) {
    return REACHABILITY_EMAIL_VERIFICATION.PROVIDER_VERIFIED;
  }
  if (type === REACHABILITY_EMAIL_TYPE.INFERRED) {
    return REACHABILITY_EMAIL_VERIFICATION.DOMAIN_PATTERN_INFERRED;
  }
  return REACHABILITY_EMAIL_VERIFICATION.UNVERIFIED;
}

/**
 * Never label a main switchboard as DIRECT.
 * @param {string|null|undefined} phone
 * @param {{ lineHint?: string|null, claimedDirect?: boolean }} [opts]
 */
export function classifyPhoneType(phone, opts = {}) {
  const raw = String(phone || "").trim();
  if (!raw) return REACHABILITY_PHONE_TYPE.UNKNOWN;
  const hint = String(opts.lineHint || "")
    .trim()
    .toUpperCase()
    .replace(/[\s/-]+/g, "_");
  if (
    hint === "MAIN" ||
    hint === "SWITCHBOARD" ||
    hint === "MAIN_ORGANIZATION" ||
    hint === "TOLL_FREE"
  ) {
    return REACHABILITY_PHONE_TYPE.MAIN_ORGANIZATION;
  }
  if (hint === "EVENT" || hint === "EVENT_LINE") {
    return REACHABILITY_PHONE_TYPE.EVENT_LINE;
  }
  if (hint === "MOBILE" || hint === "MOBILE_PUBLIC_PROFESSIONAL") {
    return REACHABILITY_PHONE_TYPE.MOBILE_PUBLIC_PROFESSIONAL;
  }
  if (hint === "OFFICE") {
    return REACHABILITY_PHONE_TYPE.OFFICE;
  }
  if (hint === "DIRECT" && opts.claimedDirect) {
    return REACHABILITY_PHONE_TYPE.DIRECT;
  }
  // Without an explicit direct claim, prefer OFFICE over inventing DIRECT
  if (opts.claimedDirect) return REACHABILITY_PHONE_TYPE.DIRECT;
  return REACHABILITY_PHONE_TYPE.OFFICE;
}

/** Prefer stronger channel when merging — never overwrite verified with inferred. */
const EMAIL_TYPE_STRENGTH = {
  DIRECT_WORK: 50,
  ROLE_BASED: 30,
  GENERIC_ORGANIZATION: 15,
  UNKNOWN: 10,
  INFERRED: 5,
};

const EMAIL_VERIFY_STRENGTH = {
  MULTI_SOURCE_CONFIRMED: 50,
  OFFICIAL_SOURCE_VERIFIED: 45,
  PROVIDER_VERIFIED: 35,
  UNVERIFIED: 10,
  DOMAIN_PATTERN_INFERRED: 5,
  INVALID: 0,
};

const PHONE_TYPE_STRENGTH = {
  DIRECT: 50,
  MOBILE_PUBLIC_PROFESSIONAL: 40,
  OFFICE: 30,
  EVENT_LINE: 25,
  MAIN_ORGANIZATION: 10,
  UNKNOWN: 5,
};

export function emailChannelStrength({ emailType, emailVerificationStatus } = {}) {
  return (
    (EMAIL_TYPE_STRENGTH[emailType] || 0) + (EMAIL_VERIFY_STRENGTH[emailVerificationStatus] || 0)
  );
}

export function phoneChannelStrength({ phoneType } = {}) {
  return PHONE_TYPE_STRENGTH[phoneType] || 0;
}

/**
 * Choose the stronger email without silently promoting inferred → verified.
 * @returns {{ email: string|null, emailType: string, emailVerificationStatus: string, source?: string|null }}
 */
export function preferStrongerEmail(current, candidate) {
  if (!candidate?.email) return current || null;
  if (!current?.email) return candidate;
  const curS = emailChannelStrength(current);
  const candS = emailChannelStrength(candidate);
  if (candS > curS) return candidate;
  return current;
}

export function preferStrongerPhone(current, candidate) {
  if (!candidate?.phone) return current || null;
  if (!current?.phone) return candidate;
  if (phoneChannelStrength(candidate) > phoneChannelStrength(current)) return candidate;
  return current;
}

/**
 * Lightweight person key for dedupe (nickname / middle initial tolerant).
 */
export function personDedupeKey(name, organization = "") {
  const n = String(name || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\b(jr|sr|ii|iii|iv)\b/g, "")
    .replace(/[^a-z\s]/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  if (!n.length) return "";
  const first = n[0].slice(0, 1);
  const last = n[n.length - 1];
  const org = String(organization || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "")
    .slice(0, 24);
  return `${first}|${last}|${org}`;
}
