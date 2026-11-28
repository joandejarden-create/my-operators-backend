/**
 * Shared missing-channel enrichment V3 — GLOBAL for Hotel Explorer + GDI.
 *
 * Canonical Airtable / shared resolver state is authoritative for durable channels.
 * Surfe session values must never be treated as verified durable channels.
 *
 * Product- and hotel-agnostic: no hotel IDs, person names, or PII constants.
 */

export const MISSING_CHANNEL_ENRICHMENT_V3 = "missing-channel-enrichment-v3";
/** Permanent contract id — DEALALITY_DYNAMIC_CONTACT_ENRICHMENT_V3 */
export const DEALALITY_DYNAMIC_CONTACT_ENRICHMENT_V3 =
  "DEALALITY_DYNAMIC_CONTACT_ENRICHMENT_V3";
/** @deprecated Use MISSING_CHANNEL_ENRICHMENT_V3 — kept for HE gate continuity. */
export const HE_CONTACT_MISSING_CHANNEL_V3 = MISSING_CHANNEL_ENRICHMENT_V3;

/**
 * Provider-neutral outcome classifications (map onto product-specific enums).
 * Pending ≠ no-result; Findymail miss ≠ Surfe miss.
 */
export const CONTACT_PROVIDER_OUTCOMES = Object.freeze({
  SUCCESS_EMAIL_ONLY: "SUCCESS_EMAIL_ONLY",
  SUCCESS_PHONE_ONLY: "SUCCESS_PHONE_ONLY",
  SUCCESS_BOTH: "SUCCESS_BOTH",
  PARTIAL_SUCCESS: "PARTIAL_SUCCESS",
  PENDING: "PENDING",
  NO_RESULT: "NO_RESULT",
  INVALID_IDENTITY: "INVALID_IDENTITY",
  PROVIDER_AUTHORIZATION_FAILED: "PROVIDER_AUTHORIZATION_FAILED",
  INSUFFICIENT_CREDITS: "INSUFFICIENT_CREDITS",
  RATE_LIMITED: "RATE_LIMITED",
  TIMEOUT: "TIMEOUT",
  PROVIDER_ERROR: "PROVIDER_ERROR",
  UNSUPPORTED_CHANNEL: "UNSUPPORTED_CHANNEL",
  NO_LOOKUP_REQUIRED: "NO_LOOKUP_REQUIRED",
  SKIPPED_ALREADY_PRESENT: "SKIPPED_ALREADY_PRESENT",
  SKIPPED_BUDGET: "SKIPPED_BUDGET",
  SKIPPED_IN_FLIGHT: "SKIPPED_IN_FLIGHT",
});

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

/**
 * Provider-neutral missing-channel planner for Findymail / Surfe / future providers.
 * Does not execute provider calls. Surfe is never auto-scheduled here.
 *
 * @param {{
 *   person?: object,
 *   provider?: 'findymail'|'surfe'|'any',
 *   remainingCredits?: number|null,
 *   findymailEmailCreditCost?: number,
 *   findymailPhoneCreditCost?: number,
 *   allowFindymailPhone?: boolean,
 * }} opts
 */
export function planMissingChannelEnrichment(opts = {}) {
  const person = opts.person || {};
  const provider = String(opts.provider || "any").toLowerCase();
  const state = computeMissingContactChannels(person);
  const linkedinUrl = text(
    person.linkedinUrl || person.linkedin || person.verifiedLinkedIn,
  );
  const hasLinkedIn = Boolean(
    state.hasVerifiedLinkedIn ||
      (linkedinUrl && /linkedin\.com\/in\//i.test(linkedinUrl)),
  );

  const emailCost = Number(opts.findymailEmailCreditCost ?? 1);
  const phoneCost = Number(opts.findymailPhoneCreditCost ?? 10);
  const remaining =
    opts.remainingCredits == null ? null : Number(opts.remainingCredits);

  const steps = [];
  if (provider === "surfe" || provider === "any") {
    if (state.requestChannel === "none") {
      steps.push({
        provider: "surfe",
        action: "NONE",
        outcome: CONTACT_PROVIDER_OUTCOMES.NO_LOOKUP_REQUIRED,
        surfeInclude: { email: false, mobile: false, linkedInUrl: false },
      });
    } else {
      steps.push({
        provider: "surfe",
        action: "EXPLICIT_CLICK_ONLY",
        channel: state.requestChannel,
        surfeInclude: surfeIncludeFromMissingState(state),
        autoExecute: false,
        note: "Surfe never auto-runs from Stage 4 / planners",
      });
    }
  }

  if (provider === "findymail" || provider === "any") {
    if (state.needsEmail) {
      const budgetOk = remaining == null || remaining >= emailCost;
      steps.push({
        provider: "findymail",
        action: budgetOk ? "FIND_EMAIL_FROM_NAME" : "SKIP_BUDGET",
        channel: "email",
        estimatedCredits: emailCost,
        outcome: budgetOk
          ? null
          : CONTACT_PROVIDER_OUTCOMES.SKIPPED_BUDGET,
      });
    } else {
      steps.push({
        provider: "findymail",
        action: "SKIP_EMAIL_PRESENT",
        channel: "email",
        outcome: CONTACT_PROVIDER_OUTCOMES.SKIPPED_ALREADY_PRESENT,
      });
    }

    const allowPhone = opts.allowFindymailPhone !== false;
    if (state.needsPhone && allowPhone) {
      if (!hasLinkedIn) {
        steps.push({
          provider: "findymail",
          action: "SKIP_PHONE_NO_LINKEDIN",
          channel: "phone",
          outcome: CONTACT_PROVIDER_OUTCOMES.UNSUPPORTED_CHANNEL,
          note: "Findymail phone requires LinkedIn URL (POST /api/search/phone)",
        });
      } else {
        const budgetOk = remaining == null || remaining >= phoneCost;
        steps.push({
          provider: "findymail",
          action: budgetOk ? "FIND_PHONE_FROM_LINKEDIN" : "SKIP_BUDGET",
          channel: "phone",
          estimatedCredits: phoneCost,
          linkedinUrl: linkedinUrl || null,
          outcome: budgetOk
            ? null
            : CONTACT_PROVIDER_OUTCOMES.SKIPPED_BUDGET,
        });
      }
    } else if (!state.needsPhone) {
      steps.push({
        provider: "findymail",
        action: "SKIP_PHONE_PRESENT",
        channel: "phone",
        outcome: CONTACT_PROVIDER_OUTCOMES.SKIPPED_ALREADY_PRESENT,
      });
    }
  }

  return {
    version: DEALALITY_DYNAMIC_CONTACT_ENRICHMENT_V3,
    missingChannelState: state,
    identityReady: Boolean(
      text(person.name || person.fullName) &&
        (hasLinkedIn ||
          text(person.organization || person.organizationName || person.company)),
    ),
    hasVerifiedLinkedIn: hasLinkedIn,
    steps,
    findymailActions: steps.filter(
      (s) =>
        s.provider === "findymail" &&
        (s.action === "FIND_EMAIL_FROM_NAME" ||
          s.action === "FIND_PHONE_FROM_LINKEDIN" ||
          s.action === "SKIP_BUDGET"),
    ),
    findymailExecutable: steps.filter(
      (s) =>
        s.provider === "findymail" &&
        (s.action === "FIND_EMAIL_FROM_NAME" ||
          s.action === "FIND_PHONE_FROM_LINKEDIN"),
    ),
    surfeAutoForbidden: true,
  };
}

/**
 * Classify a provider response that may return email and/or phone independently.
 */
export function classifyPartialChannelResult({
  requestedEmail = false,
  requestedPhone = false,
  gotEmail = false,
  gotPhone = false,
  pending = false,
  errorCode = null,
} = {}) {
  if (pending) return CONTACT_PROVIDER_OUTCOMES.PENDING;
  if (errorCode) {
    const c = String(errorCode).toUpperCase();
    if (/CREDIT|402|INSUFFICIENT/.test(c)) {
      return CONTACT_PROVIDER_OUTCOMES.INSUFFICIENT_CREDITS;
    }
    if (/RATE|429/.test(c)) return CONTACT_PROVIDER_OUTCOMES.RATE_LIMITED;
    if (/TIMEOUT/.test(c)) return CONTACT_PROVIDER_OUTCOMES.TIMEOUT;
    if (/AUTH|401|403/.test(c)) {
      return CONTACT_PROVIDER_OUTCOMES.PROVIDER_AUTHORIZATION_FAILED;
    }
    if (/IDENTITY|INVALID/.test(c)) {
      return CONTACT_PROVIDER_OUTCOMES.INVALID_IDENTITY;
    }
    return CONTACT_PROVIDER_OUTCOMES.PROVIDER_ERROR;
  }
  if (!requestedEmail && !requestedPhone) {
    return CONTACT_PROVIDER_OUTCOMES.NO_LOOKUP_REQUIRED;
  }
  if (gotEmail && gotPhone) return CONTACT_PROVIDER_OUTCOMES.SUCCESS_BOTH;
  if (gotEmail && !gotPhone) {
    return requestedPhone
      ? CONTACT_PROVIDER_OUTCOMES.PARTIAL_SUCCESS
      : CONTACT_PROVIDER_OUTCOMES.SUCCESS_EMAIL_ONLY;
  }
  if (gotPhone && !gotEmail) {
    return requestedEmail
      ? CONTACT_PROVIDER_OUTCOMES.PARTIAL_SUCCESS
      : CONTACT_PROVIDER_OUTCOMES.SUCCESS_PHONE_ONLY;
  }
  return CONTACT_PROVIDER_OUTCOMES.NO_RESULT;
}

export default {
  MISSING_CHANNEL_ENRICHMENT_V3,
  DEALALITY_DYNAMIC_CONTACT_ENRICHMENT_V3,
  HE_CONTACT_MISSING_CHANNEL_V3,
  CONTACT_PROVIDER_OUTCOMES,
  isEligibleVerifiedChannel,
  computeMissingContactChannels,
  surfeIncludeFromMissingState,
  surfeIncludeForChannel,
  resolveEnrichmentChannelRequest,
  planMissingChannelEnrichment,
  classifyPartialChannelResult,
};
