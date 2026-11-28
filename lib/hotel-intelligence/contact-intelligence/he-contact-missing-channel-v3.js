/**
 * HE Contact Enrichment V3 — dynamic missing-channel detection.
 *
 * Airtable / shared resolver state is authoritative for durable channels.
 * Surfe session values must never be treated as verified durable channels.
 */

export const HE_CONTACT_MISSING_CHANNEL_V3 = "he-contact-missing-channel-v3";

const VERIFIED_OK = new Set([
  "verified direct",
  "verified",
  "company validated",
  "validated",
  "public listed", // LinkedIn / public profile may use this
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function isEmailType(t) {
  return /email/i.test(String(t || ""));
}

function isPhoneType(t) {
  return /telephone|phone/i.test(String(t || ""));
}

function isLinkedInType(t) {
  return /linkedin/i.test(String(t || ""));
}

/**
 * Eligible durable channel — not session-only / not Surfe-derived.
 */
export function isEligibleVerifiedChannel(ch = {}) {
  if (!ch || typeof ch !== "object") return false;
  if (ch.sessionOnly === true || ch.surfeDerived === true) return false;
  if (ch.persist === "forbidden" || ch.persistence === "NOT_WRITTEN") return false;
  const value = text(ch.channelValue || ch.value || ch.email || ch.phone);
  if (!value) return false;
  const status = text(ch.verificationStatus || ch.verification_status || "").toLowerCase();
  // Presence in HI Contact Channels / resolver output is eligible unless explicitly rejected.
  if (!status) return true;
  if (/rejected|invalid|do not use|unverified|pending/i.test(status)) return false;
  if (VERIFIED_OK.has(status)) return true;
  // "Verified Direct" and similar already covered; allow other non-rejected durable statuses.
  return !/surfe/i.test(String(ch.provider || ""));
}

/**
 * @param {object} person — canonical shared person DTO or HCI person row
 * @returns {{
 *   hasVerifiedEmail: boolean,
 *   hasVerifiedPhone: boolean,
 *   hasVerifiedLinkedIn: boolean,
 *   needsEmail: boolean,
 *   needsPhone: boolean,
 *   requestChannel: 'none'|'email'|'phone'|'both',
 *   surfeInclude: { email: boolean, mobile: boolean },
 *   enrichAction: 'none'|'email'|'phone'|'both',
 *   buttonEnabled: boolean,
 *   dimmedComplete: boolean,
 *   approvedButtonLabel: string,
 *   contextualDescription: string|null,
 *   version: string
 * }}
 */
export function computeMissingContactChannels(person = {}) {
  const channels = Array.isArray(person.channels) ? person.channels : [];
  const emailContacts = Array.isArray(person.emailContacts)
    ? person.emailContacts
    : Array.isArray(person.emails)
      ? person.emails
      : [];
  const phoneContacts = Array.isArray(person.phoneContacts)
    ? person.phoneContacts
    : Array.isArray(person.phones)
      ? person.phones
      : [];

  const fromChannelsEmail = channels.some(
    (c) => isEmailType(c.channelType || c.kind) && isEligibleVerifiedChannel(c),
  );
  const fromChannelsPhone = channels.some(
    (c) => isPhoneType(c.channelType || c.kind) && isEligibleVerifiedChannel(c),
  );
  const fromChannelsLi = channels.some(
    (c) => isLinkedInType(c.channelType || c.kind) && isEligibleVerifiedChannel(c),
  );

  const scalarEmail = text(person.email || person.workEmail);
  const scalarPhone = text(person.phone || person.mobile);
  const scalarLi = text(person.linkedinUrl || person.linkedin || person.verifiedLinkedIn);

  const hasVerifiedEmail =
    fromChannelsEmail ||
    emailContacts.some((c) => isEligibleVerifiedChannel(c) || text(c.value || c.email)) ||
    Boolean(scalarEmail);

  const hasVerifiedPhone =
    fromChannelsPhone ||
    phoneContacts.some((c) => isEligibleVerifiedChannel(c) || text(c.value || c.phone)) ||
    Boolean(scalarPhone);

  const hasVerifiedLinkedIn =
    fromChannelsLi ||
    Boolean(scalarLi && /linkedin\.com\/in\//i.test(scalarLi));

  const needsEmail = !hasVerifiedEmail;
  const needsPhone = !hasVerifiedPhone;

  let requestChannel = "none";
  if (needsEmail && needsPhone) requestChannel = "both";
  else if (needsEmail) requestChannel = "email";
  else if (needsPhone) requestChannel = "phone";

  const surfeInclude = {
    email: needsEmail,
    mobile: needsPhone,
  };

  const approvedButtonLabel = "Find Email & Phone";
  let contextualDescription = null;
  if (needsPhone && !needsEmail) contextualDescription = "Find Missing Phone";
  else if (needsEmail && !needsPhone) contextualDescription = "Find Missing Email";
  else if (needsEmail && needsPhone) contextualDescription = "Find Email & Phone";

  return {
    version: HE_CONTACT_MISSING_CHANNEL_V3,
    hasVerifiedEmail,
    hasVerifiedPhone,
    hasVerifiedLinkedIn,
    needsEmail,
    needsPhone,
    requestChannel,
    enrichAction: requestChannel,
    surfeInclude,
    buttonEnabled: requestChannel !== "none",
    dimmedComplete: requestChannel === "none",
    approvedButtonLabel,
    contextualDescription,
    // LinkedIn is identity input only — never substitutes for email/phone.
    linkedInIsIdentityOnly: true,
  };
}

/**
 * Map missing-channel state → Surfe include flags (official contract).
 */
export function surfeIncludeFromMissingState(state = {}) {
  const needsEmail = state.needsEmail === true;
  const needsPhone = state.needsPhone === true;
  return {
    email: needsEmail,
    mobile: needsPhone,
    // linkedInUrl enrichment is optional profile context; keep true when any channel requested.
    linkedInUrl: needsEmail || needsPhone,
  };
}

export default {
  HE_CONTACT_MISSING_CHANNEL_V3,
  isEligibleVerifiedChannel,
  computeMissingContactChannels,
  surfeIncludeFromMissingState,
};
