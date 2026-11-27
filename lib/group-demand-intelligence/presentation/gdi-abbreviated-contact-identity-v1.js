/**
 * Abbreviated / incomplete professional contact identity (permanent).
 *
 * "Lily Y." / "Suzanne K." style surnames are NOT fully verified identities.
 * They must not auto-pass WHO/HOW as complete person identity or be submitted
 * for ambiguous Findymail enrichment.
 *
 * Does not invent surnames. LinkedIn privacy-abbreviated profiles remain PARTIAL.
 */
export const GDI_ABBREVIATED_CONTACT_IDENTITY_V1 =
  "gdi-abbreviated-contact-identity-v1";

export const PERSON_IDENTITY_COMPLETENESS = Object.freeze({
  FULL: "FULL",
  ABBREVIATED_SURNAME: "ABBREVIATED_SURNAME",
  INITIALS_ONLY: "INITIALS_ONLY",
  SINGLE_TOKEN: "SINGLE_TOKEN",
  EMPTY: "EMPTY",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * True for first+initial surname patterns: "Lily Y.", "Suzanne K", "J. Smith" (leading initial given).
 */
export function isAbbreviatedPersonName(name = "") {
  const s = text(name);
  if (!s) return true;
  // First + single-letter surname (optional period)
  if (/^[A-Za-z][A-Za-z'’\-]+\s+[A-Za-z]\.?$/i.test(s)) return true;
  // Trailing single-letter surname token
  if (/\b[A-Za-z]\.$/.test(s) && s.split(/\s+/).length <= 3) return true;
  // Leading initial given name with short rest: "J. Doe" is OK (full surname);
  // "J. K." is incomplete
  if (/^[A-Za-z]\.\s+[A-Za-z]\.?$/i.test(s)) return true;
  return false;
}

export function classifyPersonIdentityCompleteness(name = "") {
  const s = text(name);
  if (!s) return PERSON_IDENTITY_COMPLETENESS.EMPTY;
  const parts = s.split(/\s+/).filter(Boolean);
  if (parts.length < 2) return PERSON_IDENTITY_COMPLETENESS.SINGLE_TOKEN;
  if (/^[A-Za-z]\.\s+[A-Za-z]\.?$/i.test(s)) {
    return PERSON_IDENTITY_COMPLETENESS.INITIALS_ONLY;
  }
  const last = parts[parts.length - 1];
  if (/^[A-Za-z]\.?$/.test(last)) {
    return PERSON_IDENTITY_COMPLETENESS.ABBREVIATED_SURNAME;
  }
  if (isAbbreviatedPersonName(s)) {
    return PERSON_IDENTITY_COMPLETENESS.ABBREVIATED_SURNAME;
  }
  return PERSON_IDENTITY_COMPLETENESS.FULL;
}

export function isFullyVerifiedPersonName(name = "") {
  return (
    classifyPersonIdentityCompleteness(name) === PERSON_IDENTITY_COMPLETENESS.FULL
  );
}

/**
 * Prefer the richer of two display names without inventing tokens.
 * Returns the candidate that is FULL when the other is abbreviated.
 */
export function preferCompletePersonName(primary, fallback) {
  const a = text(primary);
  const b = text(fallback);
  if (!a) return b || null;
  if (!b) return a;
  const ca = classifyPersonIdentityCompleteness(a);
  const cb = classifyPersonIdentityCompleteness(b);
  if (
    ca === PERSON_IDENTITY_COMPLETENESS.FULL &&
    cb !== PERSON_IDENTITY_COMPLETENESS.FULL
  ) {
    return a;
  }
  if (
    cb === PERSON_IDENTITY_COMPLETENESS.FULL &&
    ca !== PERSON_IDENTITY_COMPLETENESS.FULL
  ) {
    return b;
  }
  // Same completeness — prefer longer (usually fuller spelling)
  return a.length >= b.length ? a : b;
}

/**
 * Sync stale presentation name fields onto the canonical primary contact name.
 */
export function syncOpportunityContactPresentationNames(opportunity = {}) {
  const o = { ...opportunity };
  const primary = o.primaryContact ? { ...o.primaryContact } : null;
  if (!primary || !text(primary.name || primary.fullName)) {
    return { opportunity: o, changed: false, repairs: [] };
  }
  const canonical = preferCompletePersonName(
    primary.fullName || primary.name,
    primary.name,
  );
  const repairs = [];
  if (text(primary.name) !== canonical) {
    repairs.push({
      field: "primaryContact.name",
      from: primary.name,
      to: canonical,
    });
    primary.name = canonical;
  }
  if (text(primary.fullName) !== canonical) {
    repairs.push({
      field: "primaryContact.fullName",
      from: primary.fullName,
      to: canonical,
    });
    primary.fullName = canonical;
  }
  primary.identityCompleteness = classifyPersonIdentityCompleteness(canonical);
  if (
    primary.identityCompleteness !== PERSON_IDENTITY_COMPLETENESS.FULL &&
    !primary.identityStatus
  ) {
    primary.identityStatus = "PARTIAL_IDENTITY_ABBREVIATED_SURNAME";
  }

  o.primaryContact = primary;

  if (text(o.contactName) && text(o.contactName) !== canonical) {
    const preferred = preferCompletePersonName(canonical, o.contactName);
    if (preferred === canonical && text(o.contactName) !== canonical) {
      repairs.push({
        field: "contactName",
        from: o.contactName,
        to: canonical,
      });
      o.contactName = canonical;
    }
  } else if (!text(o.contactName)) {
    o.contactName = canonical;
  }

  if (o.tileContactPreview && typeof o.tileContactPreview === "object") {
    const tile = { ...o.tileContactPreview };
    const tileName = text(tile.name || tile.displayName);
    const preferred = preferCompletePersonName(canonical, tileName);
    if (preferred === canonical && tileName !== canonical) {
      repairs.push({
        field: "tileContactPreview.name",
        from: tileName,
        to: canonical,
      });
      tile.name = canonical;
      if (tile.displayName) tile.displayName = canonical;
      o.tileContactPreview = tile;
    }
  }

  // Replace abbreviated mentions in customer-facing action copy when we have FULL identity
  if (classifyPersonIdentityCompleteness(canonical) === PERSON_IDENTITY_COMPLETENESS.FULL) {
    for (const key of ["recommendedNextStep", "recommendedAction"]) {
      const raw = text(o[key]);
      if (!raw) continue;
      // Replace "First X." patterns that match the same first name
      const first = canonical.split(/\s+/)[0];
      // Do not require \b after the period — "." and ";" are both non-word chars.
      const re = new RegExp(
        `\\b${first.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s+[A-Za-z]\\.(?=$|\\s|[;,!?])`,
        "g",
      );
      const next = raw.replace(re, canonical);
      if (next !== raw) {
        repairs.push({ field: key, from: raw.slice(0, 80), to: next.slice(0, 80) });
        o[key] = next;
      }
    }
  }

  return { opportunity: o, changed: repairs.length > 0, repairs };
}

/**
 * Audit an opportunity inventory for abbreviated / incomplete identities.
 */
export function auditAbbreviatedContactIdentities(opportunities = []) {
  const rows = [];
  for (const o of opportunities) {
    const c = o.primaryContact || {};
    const name = text(c.name || c.fullName || o.contactName || o.tileContactPreview?.name);
    const completeness = classifyPersonIdentityCompleteness(name);
    const tileName = text(o.tileContactPreview?.name);
    const drift =
      tileName &&
      name &&
      tileName !== name &&
      classifyPersonIdentityCompleteness(tileName) !== PERSON_IDENTITY_COMPLETENESS.FULL &&
      completeness === PERSON_IDENTITY_COMPLETENESS.FULL;
    if (
      completeness !== PERSON_IDENTITY_COMPLETENESS.FULL ||
      drift ||
      (tileName && isAbbreviatedPersonName(tileName))
    ) {
      rows.push({
        opportunityId: o.id || o.opportunityId,
        organization: o.organizationName || o.displayCompanyName || null,
        primaryName: text(c.name) || null,
        fullName: text(c.fullName) || null,
        tileName: tileName || null,
        contactName: text(o.contactName) || null,
        completeness,
        presentationDrift: Boolean(drift),
        linkedinUrl: text(c.linkedinUrl || c.sourceUrl) || null,
        title: text(c.title || c.role) || null,
      });
    }
  }
  return {
    version: GDI_ABBREVIATED_CONTACT_IDENTITY_V1,
    checked: opportunities.length,
    flagged: rows.length,
    rows,
  };
}
