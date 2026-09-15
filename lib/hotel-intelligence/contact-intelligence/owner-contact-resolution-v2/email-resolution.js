/**
 * Email resolution lane — explicit → pattern → verify → paid fallback.
 * Never publish INFERRED_UNVERIFIED as a valid contact.
 */

import { generateInferredEmailCandidates, getEmailVerificationStatus } from "../person-email-discovery.js";
import { EMAIL_STATUS, EMAIL_METHOD, CACHE_TTL } from "./vocabulary.js";
import { createEvidenceFact } from "./roles-and-sources.js";
import { SOURCE_CLASS } from "./vocabulary.js";

export const EMAIL_RESOLUTION_V2 = "email-resolution-v2";

/** Simple in-process verification cache. */
const verifyCache = new Map();

export function normalizePersonNameForEmail(fullName) {
  const raw = String(fullName || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z\s'-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const parts = raw.split(" ").filter(Boolean);
  // Spanish/Portuguese: keep first + last significant token (drop particles)
  const particles = new Set(["de", "del", "la", "las", "los", "da", "do", "dos", "das", "y", "e"]);
  const significant = parts.filter((p) => !particles.has(p.toLowerCase()));
  const first = significant[0] || parts[0] || "";
  const last = significant[significant.length - 1] || parts[parts.length - 1] || "";
  return {
    original: fullName,
    normalized: raw,
    first_name: first,
    last_name: last,
    first_lower: first.toLowerCase(),
    last_lower: last.toLowerCase(),
  };
}

export function buildPatternCandidates(nameParts, domain) {
  if (!domain || !nameParts.first_lower || !nameParts.last_lower) return [];
  const f = nameParts.first_lower;
  const l = nameParts.last_lower;
  const locals = [
    `${f}.${l}`,
    f,
    `${f[0]}${l}`,
    `${f}${l[0]}`,
    `${f}_${l}`,
    `${f}-${l}`,
    l,
  ];
  return [...new Set(locals)].map((local) => ({
    email: `${local}@${domain}`,
    pattern: local.includes(".") ? "first.last" : local === f ? "first" : "other",
    name_transform: nameParts,
  }));
}

/**
 * Provider abstraction — no unsafe SMTP.
 * Returns deliverable | undeliverable | risky | unknown | catch_all
 */
export async function verifyEmail(email, { force = false } = {}) {
  const key = String(email || "").trim().toLowerCase();
  if (!key) {
    return { status: "unknown", provider: "none", evidence: { reason: "empty" } };
  }
  if (!force && verifyCache.has(key)) {
    return { ...verifyCache.get(key), cached: true };
  }

  const wiring = getEmailVerificationStatus();
  // No abusive SMTP; stub until Hunter/NeverBounce adapter authorized
  const result = {
    status: "unknown",
    provider: "none",
    evidence: {
      wiring: wiring.status,
      note: "Mailbox verification adapter not wired — treat EXPLICIT as EXPLICIT_UNVERIFIED until verify provider enabled.",
    },
    verified_at: new Date().toISOString(),
  };
  verifyCache.set(key, result);
  return { ...result, cached: false };
}

export function mapVerifyToEmailStatus(baseStatus, verify) {
  const v = String(verify?.status || "unknown").toLowerCase();
  if (baseStatus === EMAIL_STATUS.REJECTED) return EMAIL_STATUS.REJECTED;
  if (baseStatus === EMAIL_STATUS.EXPLICIT_UNVERIFIED || baseStatus === EMAIL_STATUS.EXPLICIT_VERIFIED) {
    if (v === "deliverable") return EMAIL_STATUS.EXPLICIT_VERIFIED;
    if (v === "undeliverable") return EMAIL_STATUS.REJECTED;
    return EMAIL_STATUS.EXPLICIT_UNVERIFIED;
  }
  if (baseStatus === EMAIL_STATUS.INFERRED_UNVERIFIED || baseStatus === EMAIL_STATUS.INFERRED_VERIFIED) {
    if (v === "deliverable") return EMAIL_STATUS.INFERRED_VERIFIED;
    if (v === "undeliverable") return EMAIL_STATUS.REJECTED;
    return EMAIL_STATUS.INFERRED_UNVERIFIED;
  }
  return baseStatus;
}

/** Publishable? INFERRED_UNVERIFIED never. */
export function isPublishableEmailStatus(status) {
  return (
    status === EMAIL_STATUS.EXPLICIT_VERIFIED ||
    status === EMAIL_STATUS.EXPLICIT_UNVERIFIED ||
    status === EMAIL_STATUS.INFERRED_VERIFIED
  );
}

/**
 * Resolve email for one person against domain + website finds.
 */
export async function resolvePersonEmail({
  person,
  domain,
  explicit_emails = [],
  peer_emails = [],
  allow_inference = true,
} = {}) {
  const nameParts = normalizePersonNameForEmail(person?.full_name);
  const evidence = [];

  // A. Explicit from official page / hint
  const hint = person?.email_hint;
  if (hint) {
    const verify = await verifyEmail(hint);
    const status = mapVerifyToEmailStatus(EMAIL_STATUS.EXPLICIT_UNVERIFIED, verify);
    evidence.push(
      createEvidenceFact({
        claim: "explicit_email_hint",
        source_url: person?.evidence?.[0]?.source_url || null,
        source_type: SOURCE_CLASS.OFFICIAL_OWNER_SITE,
        extracted_text_or_fact: hint,
      })
    );
    return {
      email: hint,
      email_status: status,
      email_method: EMAIL_METHOD.EXPLICIT_OFFICIAL,
      email_confidence: status === EMAIL_STATUS.EXPLICIT_VERIFIED ? "HIGH" : "MEDIUM",
      publishable: isPublishableEmailStatus(status),
      name_transform: nameParts,
      evidence,
      verify,
      cache_ttl_ms: CACHE_TTL.verified_email_ms,
    };
  }

  for (const e of explicit_emails) {
    const local = String(e).split("@")[0] || "";
    const matchesName =
      local.toLowerCase().includes(nameParts.first_lower) ||
      (nameParts.last_lower && local.toLowerCase().includes(nameParts.last_lower));
    if (!matchesName) continue;
    const verify = await verifyEmail(e);
    const status = mapVerifyToEmailStatus(EMAIL_STATUS.EXPLICIT_UNVERIFIED, verify);
    evidence.push(
      createEvidenceFact({
        claim: "explicit_email_name_match",
        source_type: SOURCE_CLASS.OFFICIAL_OWNER_SITE,
        extracted_text_or_fact: e,
      })
    );
    return {
      email: e,
      email_status: status,
      email_method: EMAIL_METHOD.EXPLICIT_OFFICIAL,
      email_confidence: "MEDIUM",
      publishable: isPublishableEmailStatus(status),
      name_transform: nameParts,
      evidence,
      verify,
    };
  }

  // C/D. Pattern from peers or generation
  if (!allow_inference || !domain) {
    return {
      email: null,
      email_status: EMAIL_STATUS.NOT_FOUND,
      email_method: EMAIL_METHOD.NONE,
      email_confidence: "LOW",
      publishable: false,
      name_transform: nameParts,
      evidence,
    };
  }

  let method = EMAIL_METHOD.PATTERN_GENERATED;
  if (peer_emails.length >= 2) {
    method = EMAIL_METHOD.PATTERN_FROM_PEERS;
  }

  const generated = generateInferredEmailCandidates(
    person?.full_name || `${nameParts.first_name} ${nameParts.last_name}`,
    domain,
    peer_emails
  );

  const candidates = Array.isArray(generated)
    ? generated.map((g) => (typeof g === "string" ? g : g.email || g.address)).filter(Boolean)
    : buildPatternCandidates(nameParts, domain).map((c) => c.email);

  const primary = candidates[0] || null;
  if (!primary) {
    return {
      email: null,
      email_status: EMAIL_STATUS.NOT_FOUND,
      email_method: EMAIL_METHOD.NONE,
      email_confidence: "LOW",
      publishable: false,
      name_transform: nameParts,
      evidence,
    };
  }

  const verify = await verifyEmail(primary);
  const status = mapVerifyToEmailStatus(EMAIL_STATUS.INFERRED_UNVERIFIED, verify);
  evidence.push(
    createEvidenceFact({
      claim: "inferred_email_pattern",
      source_type: SOURCE_CLASS.OTHER_PUBLIC_SOURCE,
      extracted_text_or_fact: `${primary} via ${method}`,
    })
  );

  return {
    email: primary,
    email_status: status,
    email_method: method,
    email_confidence: status === EMAIL_STATUS.INFERRED_VERIFIED ? "MEDIUM" : "LOW",
    publishable: isPublishableEmailStatus(status),
    name_transform: nameParts,
    candidates: candidates.slice(0, 5),
    evidence,
    verify,
  };
}
