/**
 * Person identity collision protection (V2.3).
 *
 * personIdentityKey = sha256(name|org).slice(0,16) is a candidate grouping key only.
 * Same name + same organization MUST NOT auto-merge distinct people.
 * Different organizations MUST NOT auto-merge without verified employment continuity.
 *
 * No speculative merges. Ambiguous matches stay UNRESOLVED.
 */

import { createHash } from "node:crypto";
import { publicContactPersonKey } from "./gdi-public-contact-verification-v1.js";
import { looksLikePersonName } from "../secondary-named-contact-v1.js";

export const GDI_PERSON_IDENTITY_COLLISION_V1 =
  "gdi-person-identity-collision-v1";

export const IDENTITY_MATCH = Object.freeze({
  SAME_PERSON_CONFIRMED: "SAME_PERSON_CONFIRMED",
  DISTINCT_PERSONS: "DISTINCT_PERSONS",
  EMPLOYMENT_CONTINUITY_CANDIDATE: "EMPLOYMENT_CONTINUITY_CANDIDATE",
  AMBIGUOUS_UNRESOLVED: "AMBIGUOUS_UNRESOLVED",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function normUrl(url) {
  const s = text(url).toLowerCase().replace(/\/$/, "");
  if (!s) return null;
  try {
    const u = new URL(/^https?:\/\//i.test(s) ? s : `https://${s}`);
    return `${u.hostname}${u.pathname}`.replace(/\/$/, "");
  } catch {
    return s;
  }
}

function isSafeHttpUrl(url) {
  try {
    const u = new URL(String(url || ""));
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

/**
 * Stable disambiguation fingerprint from independent evidence (non-PII hash).
 * Includes profile URL / person-specific source / employer domain when present.
 */
export function buildIdentityDisambiguationFingerprint(attrs = {}) {
  // Role is intentionally excluded — titles change without changing identity.
  const parts = [
    normUrl(attrs.professionalProfileUrl) || "",
    normUrl(attrs.organizationalProfileUrl) || "",
    normUrl(attrs.personSpecificSourceUrl) || "",
    text(attrs.employerDomain).toLowerCase(),
    text(attrs.externalStableId).toLowerCase(),
  ];
  const material = parts.filter(Boolean);
  if (!material.length) return null;
  return createHash("sha256").update(material.join("|")).digest("hex").slice(0, 20);
}

/**
 * Candidate grouping key (legacy) — NOT sufficient alone for merge.
 */
export function candidatePersonGroupingKey({ name, organization } = {}) {
  return publicContactPersonKey({ name, organization });
}

/**
 * Compare two person identity records for merge eligibility.
 */
export function assessPersonIdentityMatch(a = {}, b = {}) {
  const nameA = text(a.name);
  const nameB = text(b.name);
  const orgA = text(a.organization);
  const orgB = text(b.organization);

  if (!looksLikePersonName(nameA) || !looksLikePersonName(nameB)) {
    return {
      version: GDI_PERSON_IDENTITY_COLLISION_V1,
      match: IDENTITY_MATCH.INSUFFICIENT_EVIDENCE,
      mergeAllowed: false,
      reason: "NAMED_PERSON_GATE_FAILED",
    };
  }

  const groupA = candidatePersonGroupingKey({ name: nameA, organization: orgA });
  const groupB = candidatePersonGroupingKey({ name: nameB, organization: orgB });
  const sameGroupKey = Boolean(groupA && groupB && groupA === groupB);

  const fpA =
    text(a.disambiguationFingerprint) ||
    buildIdentityDisambiguationFingerprint(a);
  const fpB =
    text(b.disambiguationFingerprint) ||
    buildIdentityDisambiguationFingerprint(b);

  const profileA = normUrl(a.professionalProfileUrl);
  const profileB = normUrl(b.professionalProfileUrl);
  const sourceA = normUrl(a.personSpecificSourceUrl || a.sourceUrl);
  const sourceB = normUrl(b.personSpecificSourceUrl || b.sourceUrl);

  // Conflicting independent profiles → distinct even if same name+org
  if (profileA && profileB && profileA !== profileB) {
    return {
      version: GDI_PERSON_IDENTITY_COLLISION_V1,
      match: IDENTITY_MATCH.DISTINCT_PERSONS,
      mergeAllowed: false,
      reason: "CONFLICTING_PROFESSIONAL_PROFILE_URLS",
      sameGroupKey,
      groupingKeyA: groupA,
      groupingKeyB: groupB,
    };
  }

  // Conflicting fingerprints under same name+org → distinct / unresolved
  if (sameGroupKey && fpA && fpB && fpA !== fpB) {
    return {
      version: GDI_PERSON_IDENTITY_COLLISION_V1,
      match: IDENTITY_MATCH.DISTINCT_PERSONS,
      mergeAllowed: false,
      reason: "CONFLICTING_DISAMBIGUATION_FINGERPRINTS",
      sameGroupKey,
      fingerprintA: fpA,
      fingerprintB: fpB,
    };
  }

  // Same name+org with matching independent evidence → confirmed
  if (
    sameGroupKey &&
    ((profileA && profileB && profileA === profileB) ||
      (fpA && fpB && fpA === fpB) ||
      (sourceA &&
        sourceB &&
        sourceA === sourceB &&
        isSafeHttpUrl(a.personSpecificSourceUrl || a.sourceUrl)))
  ) {
    return {
      version: GDI_PERSON_IDENTITY_COLLISION_V1,
      match: IDENTITY_MATCH.SAME_PERSON_CONFIRMED,
      mergeAllowed: true,
      reason: "GROUP_KEY_PLUS_INDEPENDENT_EVIDENCE_MATCH",
      sameGroupKey,
      fingerprint: fpA || fpB || null,
    };
  }

  // Same name+org but no independent evidence → do NOT auto-merge
  // Exception: identical person-specific source URL counts as weak confirmation
  // for updating the *same* durable record, not for merging two unknowns.
  if (sameGroupKey) {
    if (sourceA && sourceB && sourceA === sourceB) {
      return {
        version: GDI_PERSON_IDENTITY_COLLISION_V1,
        match: IDENTITY_MATCH.SAME_PERSON_CONFIRMED,
        mergeAllowed: true,
        reason: "GROUP_KEY_PLUS_IDENTICAL_SOURCE_URL",
        sameGroupKey,
      };
    }
    return {
      version: GDI_PERSON_IDENTITY_COLLISION_V1,
      match: IDENTITY_MATCH.AMBIGUOUS_UNRESOLVED,
      mergeAllowed: false,
      reason: "SAME_NAME_ORG_WITHOUT_DISAMBIGUATION_EVIDENCE",
      sameGroupKey,
      note: "personIdentityKey alone is insufficient for merge",
    };
  }

  // Different orgs: employment continuity only with explicit verified link
  const continuity =
    a.employmentContinuityToPersonKey === groupB ||
    b.employmentContinuityToPersonKey === groupA ||
    (text(a.successorPersonIdentityKey) &&
      text(a.successorPersonIdentityKey) === groupB) ||
    (text(b.priorPersonIdentityKey) &&
      text(b.priorPersonIdentityKey) === groupA);

  if (
    nameA.toLowerCase() === nameB.toLowerCase() &&
    orgA.toLowerCase() !== orgB.toLowerCase()
  ) {
    if (continuity && (fpA || profileA) && (fpB || profileB)) {
      // Still not an auto-merge of channels — continuity candidate only
      return {
        version: GDI_PERSON_IDENTITY_COLLISION_V1,
        match: IDENTITY_MATCH.EMPLOYMENT_CONTINUITY_CANDIDATE,
        mergeAllowed: false,
        reason: "VERIFIED_EMPLOYMENT_CONTINUITY_WITHOUT_CHANNEL_TRANSFER",
        sameGroupKey: false,
        note: "Preserve history; invalidate unsupported channel assumptions",
      };
    }
    return {
      version: GDI_PERSON_IDENTITY_COLLISION_V1,
      match: IDENTITY_MATCH.DISTINCT_PERSONS,
      mergeAllowed: false,
      reason: "DIFFERENT_ORGANIZATIONS_NO_CONTINUITY_EVIDENCE",
      sameGroupKey: false,
    };
  }

  return {
    version: GDI_PERSON_IDENTITY_COLLISION_V1,
    match: IDENTITY_MATCH.INSUFFICIENT_EVIDENCE,
    mergeAllowed: false,
    reason: "NO_MATCH_CRITERIA_MET",
    sameGroupKey,
  };
}

/**
 * Decide whether an incoming record may upsert into an existing durable person.
 */
export function decideDurablePersonUpsert(existing, incoming) {
  if (!existing) {
    return {
      action: "CREATE",
      mergeAllowed: true,
      match: null,
    };
  }
  const assessment = assessPersonIdentityMatch(existing, incoming);
  if (assessment.mergeAllowed) {
    return { action: "UPDATE_SAME_PERSON", mergeAllowed: true, match: assessment };
  }
  if (assessment.match === IDENTITY_MATCH.DISTINCT_PERSONS) {
    return {
      action: "CREATE_COLLISION_BRANCH",
      mergeAllowed: false,
      match: assessment,
      note: "Require disambiguationFingerprint suffix; do not overwrite existing",
    };
  }
  return {
    action: "REFUSE_AMBIGUOUS",
    mergeAllowed: false,
    match: assessment,
  };
}

/**
 * Collision-safe durable person key: grouping key + optional fingerprint suffix.
 */
export function durablePersonRecordKey({
  name,
  organization,
  disambiguationFingerprint = null,
  professionalProfileUrl = null,
  personSpecificSourceUrl = null,
  employerDomain = null,
  verifiedRole = null,
  externalStableId = null,
} = {}) {
  const group = candidatePersonGroupingKey({ name, organization });
  if (!group) return null;
  const fp =
    text(disambiguationFingerprint) ||
    buildIdentityDisambiguationFingerprint({
      professionalProfileUrl,
      personSpecificSourceUrl,
      employerDomain,
      verifiedRole,
      externalStableId,
    });
  if (!fp) return `${group}::ambiguous`;
  return `${group}::${fp}`;
}

export default {
  GDI_PERSON_IDENTITY_COLLISION_V1,
  IDENTITY_MATCH,
  buildIdentityDisambiguationFingerprint,
  candidatePersonGroupingKey,
  assessPersonIdentityMatch,
  decideDurablePersonUpsert,
  durablePersonRecordKey,
};
