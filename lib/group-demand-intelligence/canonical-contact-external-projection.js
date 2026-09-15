/**
 * Safe external projection of canonical contact intelligence for GDI share.
 *
 * GDI Operating Law: reusable product logic only — no hotel-specific constants.
 * Displays CURRENT canonical fields only. Never proposal history, rollbacks,
 * rejected/ambiguous/held values, or provider internals.
 */

export const EXTERNAL_PHONE_TYPE = Object.freeze({
  MOBILE: "MOBILE",
  DIRECT: "DIRECT",
  OFFICE: "OFFICE",
  MAIN_SHARED: "MAIN_SHARED",
  UNKNOWN: "UNKNOWN",
});

export const EXTERNAL_PHONE_TYPE_LABEL = Object.freeze({
  MOBILE: "Mobile",
  DIRECT: "Direct",
  OFFICE: "Office",
  MAIN_SHARED: "Main / Shared",
  UNKNOWN: "Phone",
});

const PROVIDER_LEAK_RE =
  /\b(surfe|webhound|apollo|zoominfo|lusha|credits?\s*spent|provider\s*cost)\b/i;

/**
 * Map internal phone outcome / field kind → client-safe phone type.
 */
export function mapExternalPhoneType({ fieldKind, phoneOutcome, phoneType } = {}) {
  const outcome = String(phoneOutcome || "").toUpperCase();
  const kind = String(fieldKind || phoneType || "").toUpperCase();
  if (
    outcome.includes("MAIN") ||
    outcome.includes("SHARED") ||
    kind === "MAIN_ORG_PHONE"
  ) {
    return EXTERNAL_PHONE_TYPE.MAIN_SHARED;
  }
  if (outcome.includes("MOBILE") || kind === "MOBILE") {
    return EXTERNAL_PHONE_TYPE.MOBILE;
  }
  if (outcome.includes("OFFICE") || kind === "PHONE") {
    return EXTERNAL_PHONE_TYPE.OFFICE;
  }
  if (outcome.includes("DIRECT")) {
    return EXTERNAL_PHONE_TYPE.DIRECT;
  }
  if (phoneType && EXTERNAL_PHONE_TYPE[phoneType]) return phoneType;
  return EXTERNAL_PHONE_TYPE.UNKNOWN;
}

function fieldIsExternallyEligible(field) {
  if (!field || field.value == null || String(field.value).trim() === "") return false;
  if (field.canonicalStatus && field.canonicalStatus !== "CANONICAL") return false;
  if (field.status === "ROLLED_BACK" || field.rolledBack === true) return false;
  if (field.blockedByFeedback === true || field.reuseBlocked === true) return false;
  if (field.identityDecision === "AMBIGUOUS" || field.identityDecision === "REJECTED") {
    return false;
  }
  if (field.mergeDecision === "REJECT_FIELD" || field.mergeDecision === "HOLD_FOR_REVIEW") {
    return false;
  }
  return true;
}

function scrubClientText(text) {
  if (text == null) return null;
  const s = String(text).trim();
  if (!s) return null;
  if (PROVIDER_LEAK_RE.test(s)) return null;
  return s;
}

/**
 * Project a canonical person + opportunity relationship into client-safe contact fields.
 *
 * @param {object} opts
 * @param {object} opts.canonicalPerson — person with fields.EMAIL / MOBILE / PHONE
 * @param {object} [opts.relationship] — hotel/opportunity relationship (role, why)
 * @param {object} [opts.fieldMeta] — optional per-field eligibility metadata
 * @returns {object|null} external projection or null if nothing safe to show
 */
export function projectCanonicalContactForExternal({
  canonicalPerson,
  relationship = null,
  fieldMeta = {},
} = {}) {
  if (!canonicalPerson) return null;

  const emailField = canonicalPerson.fields?.EMAIL || null;
  const mobileField = canonicalPerson.fields?.MOBILE || null;
  const phoneField = canonicalPerson.fields?.PHONE || null;
  const roleEmailField = canonicalPerson.fields?.ROLE_EMAIL || null;

  const emailEligible = fieldIsExternallyEligible(emailField) &&
    fieldMeta.email?.eligible !== false;
  const mobileEligible = fieldIsExternallyEligible(mobileField) &&
    fieldMeta.mobile?.eligible !== false;
  const phoneEligible = fieldIsExternallyEligible(phoneField) &&
    fieldMeta.phone?.eligible !== false;
  const roleEmailEligible = fieldIsExternallyEligible(roleEmailField) &&
    fieldMeta.roleEmail?.eligible !== false;

  // Prefer direct work email; never show rolled-back / rejected.
  const email = emailEligible
    ? emailField.value
    : roleEmailEligible
      ? roleEmailField.value
      : null;

  // Prefer mobile over office phone for primary display.
  let phone = null;
  let phoneType = null;
  let phoneTypeLabel = null;
  if (mobileEligible) {
    phone = mobileField.value;
    phoneType = mapExternalPhoneType({
      fieldKind: "MOBILE",
      phoneOutcome: mobileField.provenance?.surfeOutcome || fieldMeta.mobile?.phoneOutcome,
      phoneType: mobileField.phoneType,
    });
  } else if (phoneEligible) {
    phone = phoneField.value;
    phoneType = mapExternalPhoneType({
      fieldKind: "PHONE",
      phoneOutcome: phoneField.provenance?.surfeOutcome || fieldMeta.phone?.phoneOutcome,
      phoneType: phoneField.phoneType,
    });
  }
  if (phoneType) phoneTypeLabel = EXTERNAL_PHONE_TYPE_LABEL[phoneType] || "Phone";

  // Do not imply main/shared is direct
  if (phoneType === EXTERNAL_PHONE_TYPE.MAIN_SHARED) {
    phoneTypeLabel = EXTERNAL_PHONE_TYPE_LABEL.MAIN_SHARED;
  }

  const name = scrubClientText(canonicalPerson.displayName);
  const organization = scrubClientText(canonicalPerson.organization);
  const title =
    scrubClientText(relationship?.eventRole) ||
    scrubClientText(canonicalPerson.title) ||
    scrubClientText(relationship?.role) ||
    null;
  const whyThisContact =
    scrubClientText(relationship?.whyThisContact) ||
    scrubClientText(fieldMeta.whyThisContact) ||
    (name && title
      ? `${name} (${title}) is the primary outreach contact for this opportunity.`
      : name
        ? `${name} is the primary outreach contact for this opportunity.`
        : null);

  if (!name && !email && !phone) return null;

  return {
    name: name || null,
    title: title || null,
    role: title || null,
    organization: organization || null,
    email: email || null,
    phone: phone || null,
    phoneType,
    phoneTypeLabel,
    whyThisContact,
    // Client-safe presentation flags only — no provider / credits / audit internals
    emailEligible: Boolean(email),
    phoneEligible: Boolean(phone),
    sourceClass: "CANONICAL",
    claimKind: "FACT",
  };
}

/**
 * Strip provider / cost internals from any object before external serialization.
 */
export function assertNoProviderLeak(payload) {
  const json = JSON.stringify(payload);
  if (PROVIDER_LEAK_RE.test(json)) {
    const err = new Error("provider_internals_leak");
    err.code = "provider_internals_leak";
    throw err;
  }
  return true;
}

/**
 * Apply external projection onto an opportunity for share rendering.
 * Does NOT mutate research source package — returns a shallow overlay copy.
 */
export function overlayCanonicalContactOnOpportunity(opportunity, projection, opts = {}) {
  if (!opportunity) return null;
  if (!projection) return { ...opportunity };

  const keepBackup =
    opts.preserveGenericAsBackup === true &&
    opportunity.primaryContact &&
    opportunity.primaryContact.email &&
    projection.email &&
    String(opportunity.primaryContact.email).toLowerCase() !==
      String(projection.email).toLowerCase();

  const backupContacts = [...(opportunity.backupContacts || [])];
  if (keepBackup) {
    backupContacts.unshift({
      name: opportunity.primaryContact.name,
      role: opportunity.primaryContact.role,
      organization: opportunity.primaryContact.organization,
      email: opportunity.primaryContact.email,
      phone: opportunity.primaryContact.phone || null,
      phoneTypeLabel: opportunity.primaryContact.phoneTypeLabel || null,
      whyThisContact: opportunity.primaryContact.whyThisContact || null,
    });
  }

  return {
    ...opportunity,
    primaryContact: {
      name: projection.name,
      title: projection.title,
      role: projection.role,
      organization: projection.organization,
      email: projection.email,
      phone: projection.phone,
      phoneType: projection.phoneType,
      phoneTypeLabel: projection.phoneTypeLabel,
      whyThisContact: projection.whyThisContact,
      claimKind: "FACT",
      contactGrade: opportunity.contactGrade || null,
      contactGradeLabel: opportunity.contactGradeLabel || null,
      contactConfidence: opportunity.contactConfidence ?? null,
      lastVerifiedAt: null,
    },
    whyThisContact: projection.whyThisContact || opportunity.whyThisContact || null,
    backupContacts: backupContacts.slice(0, 3),
    contactQuality: opportunity.contactQuality || null,
    contactQualityLabel: opportunity.contactQualityLabel || null,
  };
}
