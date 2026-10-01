/**
 * Contact Intelligence V1 — provider-neutral email verification interface.
 * Research discovery and verification are separate stages.
 * Does not send mail; does not claim definitive mailbox proof without a wired adapter.
 *
 * V1.1 wired path: native DNS MX (domain existence only).
 * MX presence ≠ MAILBOX_VALID / VERIFIED_MAILBOX.
 */

import dns from "node:dns/promises";
import { VERIFICATION_STATUS } from "./vocabulary.js";
import { isGenericMailboxEmail, isRoleMailboxEmail } from "./dimensions.js";

export const EMAIL_VERIFICATION_INTERFACE_VERSION = "email-verification-interface-v1.1";

export const MAILBOX_CHECK_RESULT = Object.freeze({
  VALID: "VALID",
  INVALID: "INVALID",
  CATCH_ALL: "CATCH_ALL",
  UNKNOWN: "UNKNOWN",
  PUBLICLY_PUBLISHED: "PUBLICLY_PUBLISHED",
  /** Domain has MX — not mailbox validation */
  MX_PRESENT: "MX_PRESENT",
  MX_ABSENT: "MX_ABSENT",
});

const DISPOSABLE_HOST_RE =
  /mailinator\.com|guerrillamail\.|tempmail\.|10minutemail\.|yopmail\.com|trashmail\./i;

const PERSONAL_HOST_RE =
  /^(gmail|googlemail|yahoo|hotmail|outlook|live|icloud|me|aol|protonmail|proton)\./i;

/**
 * Syntax + disposable + personal-domain checks (native, safe).
 */
export function checkEmailSyntax(email) {
  const em = String(email || "")
    .trim()
    .toLowerCase();
  if (!em || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(em)) {
    return { ok: false, result: MAILBOX_CHECK_RESULT.INVALID, reason: "syntax" };
  }
  const host = em.split("@")[1] || "";
  if (DISPOSABLE_HOST_RE.test(host)) {
    return { ok: false, result: MAILBOX_CHECK_RESULT.INVALID, reason: "disposable_domain" };
  }
  return { ok: true, result: MAILBOX_CHECK_RESULT.UNKNOWN, reason: null, host, email: em };
}

export function isPersonalEmailDomain(email) {
  const host = String(email || "")
    .trim()
    .toLowerCase()
    .split("@")[1] || "";
  return PERSONAL_HOST_RE.test(host);
}

/**
 * Native MX lookup — fail-closed on errors (UNKNOWN, never VALID).
 */
export async function checkDomainMx(host) {
  const h = String(host || "")
    .trim()
    .toLowerCase()
    .replace(/\.$/, "");
  if (!h) {
    return { ok: false, mailbox_result: MAILBOX_CHECK_RESULT.INVALID, mx: [], error: "empty_host" };
  }
  try {
    const records = await dns.resolveMx(h);
    const mx = (records || [])
      .map((r) => ({ exchange: r.exchange, priority: r.priority }))
      .sort((a, b) => a.priority - b.priority);
    if (!mx.length) {
      return {
        ok: false,
        mailbox_result: MAILBOX_CHECK_RESULT.MX_ABSENT,
        mx: [],
        error: "no_mx",
      };
    }
    return {
      ok: true,
      mailbox_result: MAILBOX_CHECK_RESULT.MX_PRESENT,
      mx,
      error: null,
    };
  } catch (err) {
    const code = String(err?.code || err?.message || err);
    // Fail closed: treat lookup failure as UNKNOWN, not INVALID (unless clearly no such domain)
    if (/ENOTFOUND|ENODATA|ESERVFAIL/i.test(code)) {
      return {
        ok: false,
        mailbox_result: /ENOTFOUND/i.test(code)
          ? MAILBOX_CHECK_RESULT.MX_ABSENT
          : MAILBOX_CHECK_RESULT.UNKNOWN,
        mx: [],
        error: code.slice(0, 120),
      };
    }
    return {
      ok: false,
      mailbox_result: MAILBOX_CHECK_RESULT.UNKNOWN,
      mx: [],
      error: code.slice(0, 120),
    };
  }
}

/**
 * Default V1.1 adapter: native MX only.
 * Never returns VALID / CATCH_ALL (would require SMTP or paid verifier).
 */
export function createNativeMxVerificationAdapter() {
  return async function nativeMxAdapter(email) {
    const syntax = checkEmailSyntax(email);
    if (!syntax.ok) {
      return {
        mailbox_result: MAILBOX_CHECK_RESULT.INVALID,
        method: "native_mx_syntax",
        evidence: [],
      };
    }
    const mx = await checkDomainMx(syntax.host);
    return {
      mailbox_result: mx.mailbox_result,
      method: "native_dns_mx",
      evidence: mx.mx?.length
        ? [{ source_type: "dns_mx", excerpt: mx.mx.map((m) => m.exchange).join(", ") }]
        : [],
      mx_error: mx.error || null,
      /** Explicit: MX must never be treated as mailbox-valid by callers */
      claims_mailbox_valid: false,
    };
  };
}

export function resolveVerificationAdapter(opts = {}) {
  if (typeof opts.adapter === "function") return { adapter: opts.adapter, source: "injected" };
  if (opts.use_native_mx === false) return { adapter: null, source: "disabled" };
  return { adapter: createNativeMxVerificationAdapter(), source: "native_mx_default" };
}

/**
 * Map mailbox check + publication context → VERIFICATION_STATUS.
 * INFERRED never becomes VERIFIED_* here.
 * MX_PRESENT never becomes VERIFIED_MAILBOX.
 */
export function mapMailboxResultToVerificationStatus({
  mailbox_result = MAILBOX_CHECK_RESULT.UNKNOWN,
  publicly_published = false,
  from_official_source = false,
  inferred = false,
  pattern_supported = false,
} = {}) {
  if (inferred && !publicly_published && !from_official_source) {
    if (pattern_supported) return VERIFICATION_STATUS.DOMAIN_PATTERN_SUPPORTED;
    return VERIFICATION_STATUS.INFERRED;
  }
  if (
    mailbox_result === MAILBOX_CHECK_RESULT.INVALID ||
    mailbox_result === MAILBOX_CHECK_RESULT.MX_ABSENT
  ) {
    return VERIFICATION_STATUS.INVALID;
  }
  if (mailbox_result === MAILBOX_CHECK_RESULT.CATCH_ALL) {
    return VERIFICATION_STATUS.CATCH_ALL_PROBABLE;
  }
  // MX alone is never mailbox-verified
  if (mailbox_result === MAILBOX_CHECK_RESULT.MX_PRESENT) {
    if (from_official_source) return VERIFICATION_STATUS.VERIFIED_OFFICIAL;
    if (publicly_published) return VERIFICATION_STATUS.PUBLICLY_PUBLISHED;
    if (pattern_supported) return VERIFICATION_STATUS.DOMAIN_PATTERN_SUPPORTED;
    return VERIFICATION_STATUS.UNRESOLVED;
  }
  if (mailbox_result === MAILBOX_CHECK_RESULT.VALID && from_official_source) {
    return VERIFICATION_STATUS.VERIFIED_MAILBOX;
  }
  if (mailbox_result === MAILBOX_CHECK_RESULT.VALID) return VERIFICATION_STATUS.VERIFIED_MAILBOX;
  if (from_official_source) return VERIFICATION_STATUS.VERIFIED_OFFICIAL;
  if (publicly_published) return VERIFICATION_STATUS.PUBLICLY_PUBLISHED;
  if (mailbox_result === MAILBOX_CHECK_RESULT.PUBLICLY_PUBLISHED) {
    return VERIFICATION_STATUS.PUBLICLY_PUBLISHED;
  }
  if (pattern_supported) return VERIFICATION_STATUS.DOMAIN_PATTERN_SUPPORTED;
  return VERIFICATION_STATUS.UNRESOLVED;
}

/**
 * Provider-neutral verifyEmailCandidate.
 * V1.1 default: native MX adapter (fail-closed; never upgrades MX→VERIFIED_MAILBOX).
 */
export async function verifyEmailCandidate(email, opts = {}) {
  try {
    const syntax = checkEmailSyntax(email);
    if (!syntax.ok) {
      return {
        version: EMAIL_VERIFICATION_INTERFACE_VERSION,
        email: String(email || "").trim().toLowerCase(),
        mailbox_result: MAILBOX_CHECK_RESULT.INVALID,
        verification_status: VERIFICATION_STATUS.INVALID,
        method: "syntax",
        checked_at: new Date().toISOString(),
        evidence: [],
        adapter_wired: false,
        fail_closed: true,
      };
    }

    if (isPersonalEmailDomain(syntax.email) && opts.require_business !== false) {
      return {
        version: EMAIL_VERIFICATION_INTERFACE_VERSION,
        email: syntax.email,
        mailbox_result: MAILBOX_CHECK_RESULT.UNKNOWN,
        verification_status: VERIFICATION_STATUS.UNRESOLVED,
        method: "personal_domain_screen",
        checked_at: new Date().toISOString(),
        evidence: [],
        adapter_wired: false,
        negative_screen: "PERSONAL_EMAIL_NOT_BUSINESS_CONTACT",
        fail_closed: true,
      };
    }

    const { adapter, source: adapterSource } = resolveVerificationAdapter(opts);

    if (typeof adapter === "function") {
      let remote;
      try {
        remote = await adapter(syntax.email, opts);
      } catch (err) {
        return {
          version: EMAIL_VERIFICATION_INTERFACE_VERSION,
          email: syntax.email,
          mailbox_result: MAILBOX_CHECK_RESULT.UNKNOWN,
          verification_status: VERIFICATION_STATUS.UNRESOLVED,
          method: "adapter_exception_fail_closed",
          checked_at: new Date().toISOString(),
          evidence: [],
          adapter_wired: true,
          adapter_source: adapterSource,
          fail_closed: true,
          error: String(err?.message || err).slice(0, 160),
        };
      }

      let mailbox_result = remote?.mailbox_result || MAILBOX_CHECK_RESULT.UNKNOWN;
      // Hard rule: native MX must never claim VALID
      if (
        remote?.claims_mailbox_valid === false &&
        mailbox_result === MAILBOX_CHECK_RESULT.VALID
      ) {
        mailbox_result = MAILBOX_CHECK_RESULT.UNKNOWN;
      }

      return {
        version: EMAIL_VERIFICATION_INTERFACE_VERSION,
        email: syntax.email,
        mailbox_result,
        verification_status: mapMailboxResultToVerificationStatus({
          mailbox_result,
          publicly_published: opts.publicly_published === true,
          from_official_source: opts.from_official_source === true,
          inferred: opts.inferred === true,
          pattern_supported: opts.pattern_supported === true,
        }),
        method: remote?.method || "provider_adapter",
        checked_at: new Date().toISOString(),
        evidence: remote?.evidence || [],
        adapter_wired: true,
        adapter_source: adapterSource,
        catch_all: mailbox_result === MAILBOX_CHECK_RESULT.CATCH_ALL,
        is_generic: isGenericMailboxEmail(syntax.email) || isRoleMailboxEmail(syntax.email),
        fail_closed: true,
        mx_is_not_mailbox_valid: mailbox_result === MAILBOX_CHECK_RESULT.MX_PRESENT,
      };
    }

    const verification_status = mapMailboxResultToVerificationStatus({
      mailbox_result: opts.publicly_published
        ? MAILBOX_CHECK_RESULT.PUBLICLY_PUBLISHED
        : MAILBOX_CHECK_RESULT.UNKNOWN,
      publicly_published: opts.publicly_published === true,
      from_official_source: opts.from_official_source === true,
      inferred: opts.inferred === true,
      pattern_supported: opts.pattern_supported === true,
    });

    return {
      version: EMAIL_VERIFICATION_INTERFACE_VERSION,
      email: syntax.email,
      mailbox_result: opts.publicly_published
        ? MAILBOX_CHECK_RESULT.PUBLICLY_PUBLISHED
        : MAILBOX_CHECK_RESULT.UNKNOWN,
      verification_status,
      method: "syntax_and_publication_flags_only",
      checked_at: new Date().toISOString(),
      evidence: opts.evidence || [],
      adapter_wired: false,
      fail_closed: true,
      note: "No verification adapter; never claims VERIFIED_MAILBOX.",
    };
  } catch (err) {
    return {
      version: EMAIL_VERIFICATION_INTERFACE_VERSION,
      email: String(email || "").trim().toLowerCase(),
      mailbox_result: MAILBOX_CHECK_RESULT.UNKNOWN,
      verification_status: VERIFICATION_STATUS.UNRESOLVED,
      method: "verify_exception_fail_closed",
      checked_at: new Date().toISOString(),
      evidence: [],
      adapter_wired: false,
      fail_closed: true,
      error: String(err?.message || err).slice(0, 160),
    };
  }
}

/** Conservative SMTP policy notes (no probing implemented here). */
export const SMTP_SAFETY_POLICY = Object.freeze({
  send_email: false,
  aggressive_probing: false,
  repeated_mailbox_probing: false,
  prefer_dedicated_provider: true,
  max_probes_per_domain_per_day: 0,
  native_mx_wired: true,
  native_mx_claims_mailbox_valid: false,
});
