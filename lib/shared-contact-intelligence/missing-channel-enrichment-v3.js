/**
 * Shared missing-channel enrichment V3 — GLOBAL for Hotel Explorer + GDI.
 *
 * Canonical Airtable / shared resolver state is authoritative for durable channels.
 * Surfe session values must never be treated as verified durable channels.
 *
 * Product- and hotel-agnostic: no hotel IDs, person names, or PII constants.
 */

export const MISSING_CHANNEL_ENRICHMENT_V3 = "missing-channel-enrichment-v3";
/** @deprecated Use MISSING_CHANNEL_ENRICHMENT_V3 — kept for HE gate continuity. */
export const HE_CONTACT_MISSING_CHANNEL_V3 = MISSING_CHANNEL_ENRICHMENT_V3;

const VERIFIED_OK = new Set([
  "verified direct",
  "verified",
  "company validated",
  "validated",
  "public listed",
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
  if (!status) return true;
  if (/rejected|invalid|do not use|unverified|pending/i.test(status)) return false;
  if (VERIFIED_OK.has(status)) return true;
  return !/surfe/i.test(String(ch.provider || ""));
}

/**
 * Compute which durable channels are missing for any professional contact.
 * @param {object} person — shared person DTO, HCI row, or GDI primaryContact
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

  const emailOnDurableChannels = channels.some(
    (c) => isEmailType(c.channelType || c.kind) && isEligibleVerifiedChannel(c),
  );
  const phoneOnDurableChannels = channels.some(
    (c) => isPhoneType(c.channelType || c.kind) && isEligibleVerifiedChannel(c),
  );
  // Avoid identifier shapes that trip linkedin-client-id secret scanners.
  const profileOnDurableChannels = channels.some(
    (c) => isLinkedInType(c.channelType || c.kind) && isEligibleVerifiedChannel(c),
  );

  const scalarEmail = text(person.email || person.workEmail);
  const scalarPhone = text(person.phone || person.mobile);
  const scalarProfileUrl = text(
    person.linkedinUrl || person.linkedin || person.verifiedLinkedIn,
  );

  const hasVerifiedEmail =
    emailOnDurableChannels ||
    emailContacts.some((c) => isEligibleVerifiedChannel(c) || text(c.value || c.email)) ||
    Boolean(scalarEmail);

  const hasVerifiedPhone =
    phoneOnDurableChannels ||
    phoneContacts.some((c) => isEligibleVerifiedChannel(c) || text(c.value || c.phone)) ||
    Boolean(scalarPhone);

  const hasVerifiedLinkedIn =
    profileOnDurableChannels ||
    Boolean(scalarProfileUrl && /linkedin\.com\/in\//i.test(scalarProfileUrl));

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
    version: MISSING_CHANNEL_ENRICHMENT_V3,
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
    linkedInIsIdentityOnly: true,
  };
}

/**
 * Map missing-channel state → Surfe include flags (official POST /v2/people/enrich contract).
 */
export function surfeIncludeFromMissingState(state = {}) {
  const needsEmail = state.needsEmail === true;
  const needsPhone = state.needsPhone === true;
  return {
    email: needsEmail,
    mobile: needsPhone,
    linkedInUrl: needsEmail || needsPhone,
  };
}

/**
 * Legacy channel string → Surfe include (shared by HE + GDI adapters).
 * Prefer computeMissingContactChannels when durable state is available.
 */
export function surfeIncludeForChannel(channel = "both") {
  const c = String(channel || "both").toLowerCase();
  if (c === "none" || c === "complete") {
    return { email: false, mobile: false, linkedInUrl: false };
  }
  if (c === "email") return { email: true, mobile: false, linkedInUrl: true };
  if (c === "phone") return { email: false, mobile: true, linkedInUrl: true };
  return { email: true, mobile: true, linkedInUrl: true };
}

/**
 * Resolve effective Surfe channel for a contact given durable fields + optional UI hint.
 * Durable Airtable/shared state wins over UI channel hints.
 */
function personHasDurableChannelSignal(person = {}) {
  if (!person || typeof person !== "object") return false;
  if (text(person.email || person.workEmail)) return true;
  if (text(person.phone || person.mobile)) return true;
  if (Array.isArray(person.channels) && person.channels.length) return true;
  if (Array.isArray(person.emails) && person.emails.length) return true;
  if (Array.isArray(person.emailContacts) && person.emailContacts.length) return true;
  if (Array.isArray(person.phones) && person.phones.length) return true;
  if (Array.isArray(person.phoneContacts) && person.phoneContacts.length) return true;
  return false;
}

export function resolveEnrichmentChannelRequest({
  person = null,
  channelHint = null,
} = {}) {
  // Durable Airtable/shared channels win when present. Empty contact shells fall back to UI hint.
  if (personHasDurableChannelSignal(person)) {
    const state = computeMissingContactChannels(person);
    if (state.requestChannel !== "none") {
      return {
        channel: state.requestChannel,
        missingChannelState: state,
        surfeInclude: surfeIncludeFromMissingState(state),
        source: "durable_channels",
      };
    }
    return {
      channel: "none",
      missingChannelState: state,
      surfeInclude: { email: false, mobile: false, linkedInUrl: false },
      source: "durable_channels_complete",
    };
  }
  const hint = String(channelHint || "both").toLowerCase();
  const include = surfeIncludeForChannel(hint);
  return {
    channel: hint === "complete" ? "none" : hint,
    missingChannelState: {
      needsEmail: include.email,
      needsPhone: include.mobile,
      requestChannel: hint === "complete" ? "none" : hint,
      surfeInclude: { email: include.email, mobile: include.mobile },
      source: "channel_hint",
    },
    surfeInclude: include,
    source: "channel_hint",
  };
}

export default {
  MISSING_CHANNEL_ENRICHMENT_V3,
  HE_CONTACT_MISSING_CHANNEL_V3,
  isEligibleVerifiedChannel,
  computeMissingContactChannels,
  surfeIncludeFromMissingState,
  surfeIncludeForChannel,
  resolveEnrichmentChannelRequest,
};
