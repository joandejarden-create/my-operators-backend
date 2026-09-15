/**
 * Contact Intelligence V1 — dimension helpers + mailbox classification.
 * Reuses GTM verification patterns without importing CoStar as product SoT.
 */

import {
  ATTRIBUTION,
  DELIVERABILITY,
  FRESHNESS,
  ROLE_CURRENCY,
  USAGE_RIGHTS,
} from "./vocabulary.js";

const GENERIC_MAILBOX_LOCAL_RE =
  /^(info|contact|hello|admin|office|reception|reservations|booking|hotel|mail|enquiries|inquiries|general|support|customerservice|sales|marketing|hr|careers|jobs|media|press|webmaster|postmaster|noreply|no-reply|donotreply|soporte|ventas|reservas|contacto|informes)$/i;

const ROLE_MAILBOX_LOCAL_RE =
  /^(ir|investorrelations|investors|investor\.relations|relaciones\.inversionistas|asg|esg|investor\.relations)$/i;

export function emailLocalPart(email) {
  return (
    String(email || "")
      .trim()
      .toLowerCase()
      .split("@")[0] || ""
  );
}

export function isGenericMailboxEmail(email) {
  const local = emailLocalPart(email).replace(/[._-]/g, "");
  if (!local) return false;
  return GENERIC_MAILBOX_LOCAL_RE.test(local);
}

export function isRoleMailboxEmail(email) {
  return ROLE_MAILBOX_LOCAL_RE.test(emailLocalPart(email));
}

export function isNamedPersonEmail(email, contactName) {
  const em = String(email || "")
    .trim()
    .toLowerCase();
  const name = String(contactName || "").trim();
  if (!em || !name || !em.includes("@")) return false;
  if (isGenericMailboxEmail(em) || isRoleMailboxEmail(em)) return false;

  const local = emailLocalPart(em).replace(/[^a-z0-9]/g, "");
  if (/^[a-z]+[._-][a-z]+/.test(emailLocalPart(em))) return true;

  const tokens = name
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .split(/\s+/)
    .filter((t) => t.length >= 3 && !/^(de|del|la|los|las|von|van|jr|sr|ii|iii)$/i.test(t));

  return tokens.some((token) => {
    const slice = token.slice(0, Math.min(token.length, 8));
    return slice.length >= 3 && local.includes(slice);
  });
}

/**
 * Classify email attribution. Inferred pattern-matches are NEVER OFFICIAL.
 */
export function classifyEmailAttribution(email, { contactName = null, claimedOfficial = false } = {}) {
  const em = String(email || "").trim();
  if (!em) return ATTRIBUTION.UNATTRIBUTED;
  if (isRoleMailboxEmail(em)) return ATTRIBUTION.ROLE_MAILBOX;
  if (isGenericMailboxEmail(em)) return ATTRIBUTION.ORGANIZATION;
  if (contactName && isNamedPersonEmail(em, contactName)) {
    return claimedOfficial ? ATTRIBUTION.NAMED_PERSON : ATTRIBUTION.NAMED_PERSON;
  }
  if (claimedOfficial) return ATTRIBUTION.INFERRED;
  return ATTRIBUTION.INFERRED;
}

/**
 * Phone line class — corporate switchboard ≠ direct personal.
 */
export function classifyPhoneAttribution(phone, { lineType = null, claimedDirectPersonal = false } = {}) {
  const raw = String(phone || "").trim();
  if (!raw) return ATTRIBUTION.UNATTRIBUTED;
  const lt = String(lineType || "")
    .trim()
    .toUpperCase();
  if (lt === "SWITCHBOARD" || lt === "TOLL_FREE" || lt === "MAIN") {
    return ATTRIBUTION.ORGANIZATION;
  }
  if (lt === "MOBILE" || lt === "DIRECT") {
    return claimedDirectPersonal ? ATTRIBUTION.NAMED_PERSON : ATTRIBUTION.ORGANIZATION;
  }
  if (claimedDirectPersonal) return ATTRIBUTION.INFERRED;
  return ATTRIBUTION.PROPERTY;
}

export function classifyDeliverability({ attribution, verified = false, bounced = false } = {}) {
  if (bounced) return DELIVERABILITY.LOW;
  if (verified && (attribution === ATTRIBUTION.OFFICIAL || attribution === ATTRIBUTION.NAMED_PERSON)) {
    return DELIVERABILITY.HIGH;
  }
  if (attribution === ATTRIBUTION.ROLE_MAILBOX || attribution === ATTRIBUTION.ORGANIZATION) {
    return verified ? DELIVERABILITY.MEDIUM : DELIVERABILITY.UNKNOWN;
  }
  if (attribution === ATTRIBUTION.INFERRED) return DELIVERABILITY.LOW;
  return DELIVERABILITY.UNKNOWN;
}

export function classifyRoleCurrency({ observedAt = null, maxFreshDays = 365 } = {}) {
  if (!observedAt) return ROLE_CURRENCY.UNKNOWN;
  const t = Date.parse(observedAt);
  if (!Number.isFinite(t)) return ROLE_CURRENCY.UNKNOWN;
  const ageDays = (Date.now() - t) / (1000 * 60 * 60 * 24);
  if (ageDays > maxFreshDays * 2) return ROLE_CURRENCY.STALE;
  if (ageDays > maxFreshDays) return ROLE_CURRENCY.UNKNOWN;
  return ROLE_CURRENCY.CURRENT;
}

export function classifyFreshness({ observedAt = null, freshDays = 90, agingDays = 365 } = {}) {
  if (!observedAt) return FRESHNESS.UNKNOWN;
  const t = Date.parse(observedAt);
  if (!Number.isFinite(t)) return FRESHNESS.UNKNOWN;
  const ageDays = (Date.now() - t) / (1000 * 60 * 60 * 24);
  if (ageDays <= freshDays) return FRESHNESS.FRESH;
  if (ageDays <= agingDays) return FRESHNESS.AGING;
  return FRESHNESS.STALE;
}

export function defaultUsageRights(attribution) {
  if (attribution === ATTRIBUTION.INFERRED) return USAGE_RIGHTS.INTERNAL_ONLY;
  if (attribution === ATTRIBUTION.UNATTRIBUTED) return USAGE_RIGHTS.RESTRICTED;
  return USAGE_RIGHTS.PUBLIC_SAFE;
}
