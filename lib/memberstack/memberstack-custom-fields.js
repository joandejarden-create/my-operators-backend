/**
 * Map signup / Airtable data to Memberstack custom field IDs (not display labels).
 *
 * Memberstack generates a stable field ID when you create a custom field in the dashboard.
 * API writes must use that ID (often a slug of the original name, e.g. "First Name" → first-name).
 *
 * Override any key via env: MEMBERSTACK_CF_FIRST_NAME, MEMBERSTACK_CF_AIRTABLE_USER_ID, etc.
 */

function slugFromLabel(label) {
  return String(label || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-+|-+$)/g, "");
}

function envKey(name, fallbackLabel) {
  const v = (process.env[name] || "").trim();
  return v || slugFromLabel(fallbackLabel);
}

/** Memberstack custom field IDs (API keys) for this project. */
export const MS_CF = {
  firstName: envKey("MEMBERSTACK_CF_FIRST_NAME", "First Name"),
  lastName: envKey("MEMBERSTACK_CF_LAST_NAME", "Last Name"),
  companyName: envKey("MEMBERSTACK_CF_COMPANY_NAME", "Company Name"),
  companyProfileId: envKey("MEMBERSTACK_CF_COMPANY_PROFILE_ID", "Company Profile ID"),
  airtableUserId: envKey("MEMBERSTACK_CF_AIRTABLE_USER_ID", "AirTable User ID"),
  phone: envKey("MEMBERSTACK_CF_PHONE", "Phone"),
  companyType: envKey("MEMBERSTACK_CF_COMPANY_TYPE", "Company Type"),
  reasonToJoin: envKey("MEMBERSTACK_CF_REASON_TO_JOIN", "Reason to Join"),
  howDidYouHear: envKey("MEMBERSTACK_CF_HOW_DID_YOU_HEAR", "How Did You Hear"),
};

/** Legacy camelCase keys (wrong for most MS dashboards) — used only when reading inbound data. */
const LEGACY_READ_ALIASES = {
  firstName: ["firstName", MS_CF.firstName, "first-name"],
  lastName: ["lastName", MS_CF.lastName, "last-name"],
  companyName: ["companyName", MS_CF.companyName, "company-name"],
  companyProfileId: ["companyProfileId", MS_CF.companyProfileId, "company-profile-id"],
  airtableUserId: ["airtableRecordId", "airtableUserId", MS_CF.airtableUserId, "air-table-user-id"],
  phone: ["phone", MS_CF.phone],
  companyType: ["companyType", "userType", MS_CF.companyType],
  reasonToJoin: ["reasonToJoin", MS_CF.reasonToJoin],
  howDidYouHear: ["howDidYouHear", MS_CF.howDidYouHear],
};

function pickFirst(cf, keys) {
  if (!cf || typeof cf !== "object") return "";
  for (const k of keys) {
    const v = cf[k];
    if (typeof v === "string" && v.trim()) return v.trim();
    if (typeof v === "number" && Number.isFinite(v)) return String(v);
  }
  return "";
}

/**
 * Build customFields object for Memberstack Admin API / DOM signup.
 * @param {object} body - signup form body
 * @param {{ airtableRecordId?: string, companyProfileId?: string }} [extras]
 */
export function buildMemberstackCustomFields(body, extras = {}) {
  const firstName = typeof body?.firstName === "string" ? body.firstName.trim() : "";
  const lastName = typeof body?.lastName === "string" ? body.lastName.trim() : "";
  const companyName = typeof body?.companyName === "string" ? body.companyName.trim() : "";
  const phone = typeof body?.phone === "string" ? body.phone.trim() : "";
  const companyType = typeof body?.companyType === "string" ? body.companyType.trim() : "";
  const reasonToJoin = typeof body?.reasonToJoin === "string" ? body.reasonToJoin.trim() : "";
  const howDidYouHear = typeof body?.howDidYouHear === "string" ? body.howDidYouHear.trim() : "";

  const out = {};
  if (firstName) out[MS_CF.firstName] = firstName;
  if (lastName) out[MS_CF.lastName] = lastName;
  if (companyName) out[MS_CF.companyName] = companyName;
  if (phone) out[MS_CF.phone] = phone;
  if (companyType) out[MS_CF.companyType] = companyType;
  if (reasonToJoin) out[MS_CF.reasonToJoin] = reasonToJoin;
  if (howDidYouHear) out[MS_CF.howDidYouHear] = howDidYouHear;

  const airtableRecordId =
    typeof extras.airtableRecordId === "string" ? extras.airtableRecordId.trim() : "";
  if (airtableRecordId) out[MS_CF.airtableUserId] = airtableRecordId;

  const companyProfileId =
    typeof extras.companyProfileId === "string" ? extras.companyProfileId.trim() : "";
  if (companyProfileId) out[MS_CF.companyProfileId] = companyProfileId;

  return out;
}

/** Read logical signup fields from a Memberstack customFields blob (webhook / GET member). */
export function readLogicalCustomFields(cf) {
  return {
    firstName: pickFirst(cf, LEGACY_READ_ALIASES.firstName),
    lastName: pickFirst(cf, LEGACY_READ_ALIASES.lastName),
    companyName: pickFirst(cf, LEGACY_READ_ALIASES.companyName),
    companyProfileId: pickFirst(cf, LEGACY_READ_ALIASES.companyProfileId),
    airtableRecordId: pickFirst(cf, LEGACY_READ_ALIASES.airtableUserId),
    phone: pickFirst(cf, LEGACY_READ_ALIASES.phone),
    companyType: pickFirst(cf, LEGACY_READ_ALIASES.companyType),
    reasonToJoin: pickFirst(cf, LEGACY_READ_ALIASES.reasonToJoin),
    howDidYouHear: pickFirst(cf, LEGACY_READ_ALIASES.howDidYouHear),
  };
}
