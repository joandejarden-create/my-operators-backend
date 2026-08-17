/**
 * Pilot provisioning — Airtable field names / env aliases (no schema mutations).
 */
import {
  INTAKE_USERS_UNIQUE_WEBFLOW_ID,
  INTAKE_DEALS_USER_LINK_NAME,
} from "../../api/schemas/intake-deal-fields.js";

/** Legacy Airtable names — both mirror Memberstack member id until rename migration. */
export const MEMBERSTACK_MEMBER_ID_FIELD_NAMES = {
  primary: process.env.AIRTABLE_USERS_MEMBERSTACK_ID_FIELD || "Unique Webflow ID",
  primaryAliases: (
    process.env.AIRTABLE_USERS_MEMBERSTACK_ID_FIELD_ALIASES || "Unique_Webflow_ID,Unique Webflow ID"
  )
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean),
  mirror: process.env.AIRTABLE_USERS_SLUG_FIELD_NAME || "Slug",
  mirrorAliases: (process.env.AIRTABLE_USERS_SLUG_FIELD_ALIASES || "Slug,slug")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean),
  primaryFieldId:
    process.env.AIRTABLE_INTAKE_USERS_UNIQUE_WEBFLOW_ID_FIELD || INTAKE_USERS_UNIQUE_WEBFLOW_ID,
  mirrorFieldId: process.env.AIRTABLE_USERS_SLUG_FIELD || "fldEgbHu5MvfyrxgE",
};

export function readMemberstackPrimaryFromFields(userFields) {
  const f = userFields || {};
  for (const name of MEMBERSTACK_MEMBER_ID_FIELD_NAMES.primaryAliases) {
    const val = f[name];
    if (val != null && String(val).trim()) return String(val).trim();
  }
  const byId = f[MEMBERSTACK_MEMBER_ID_FIELD_NAMES.primaryFieldId];
  if (byId != null && String(byId).trim()) return String(byId).trim();
  return "";
}

export function readMemberstackMirrorFromFields(userFields) {
  const f = userFields || {};
  for (const name of MEMBERSTACK_MEMBER_ID_FIELD_NAMES.mirrorAliases) {
    const val = f[name];
    if (val != null && String(val).trim()) return String(val).trim();
  }
  const byId = f[MEMBERSTACK_MEMBER_ID_FIELD_NAMES.mirrorFieldId];
  if (byId != null && String(byId).trim()) return String(byId).trim();
  return "";
}

/** Live base (appvtnDurnMSjINP6): no Workspace Access column on Users (tbl6shiyz2wdUqE5F). */
export const WORKSPACE_ACCESS_SOURCE_TABLE = "Company Profile";

/** Canonical workspace permissions field — Company Profile only. */
export const COMPANY_WORKSPACE_ACCESS_FIELD =
  process.env.AIRTABLE_COMPANY_WORKSPACE_ACCESS_FIELD || "Workspace Access";

/** Live field id on Company Profile (base appvtnDurnMSjINP6) — documentation / probes only. */
export const COMPANY_WORKSPACE_ACCESS_FIELD_ID =
  process.env.AIRTABLE_COMPANY_WORKSPACE_ACCESS_FIELD_ID || "fldhZqzi0LskI0MpK";

export function workspaceAccessSourceLabel() {
  return `${WORKSPACE_ACCESS_SOURCE_TABLE} → ${COMPANY_WORKSPACE_ACCESS_FIELD}`;
}

/**
 * Legacy resolve-user merge fallback only (lib/dealality/resolve-user.js).
 * Not provisioned on Users in the current base — do not use for pilot checks.
 */
export const USERS_WORKSPACE_ACCESS_FIELD =
  process.env.AIRTABLE_USERS_WORKSPACE_ACCESS_FIELD || "Workspace Access";

export const DEALS_COMPANY_LINK_FIELD =
  process.env.AIRTABLE_DEALS_COMPANY_LINK_FIELD || "Company Profile";

export const DEALS_USER_LINK_FIELD = INTAKE_DEALS_USER_LINK_NAME;

export const USERS_COMPANY_LINK_FIELD =
  process.env.AIRTABLE_USERS_COMPANY_LINK_FIELD || "Company Profile";

export function getUsersStatusFieldCandidates() {
  const raw =
    process.env.AIRTABLE_USERS_STATUS_FIELDS ||
    process.env.SIGNUP_AIRTABLE_STATUS_FIELD ||
    "Account Status,Status";
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

export const INACTIVE_ACCOUNT_STATUS_VALUES = (
  process.env.AIRTABLE_USERS_INACTIVE_STATUS_VALUES ||
  "inactive,disabled,suspended,archived"
)
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

export const PENDING_ACCOUNT_STATUS_VALUES = String(
  process.env.SIGNUP_AIRTABLE_PENDING_STATUS || "Pending"
)
  .split("|")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

export const ACTIVE_ACCOUNT_STATUS_VALUES = String(
  process.env.SIGNUP_AIRTABLE_APPROVED_STATUS ||
    process.env.SIGNUP_AIRTABLE_ACTIVE_STATUS ||
    "Active"
)
  .split("|")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

export function memberstackIdLabel() {
  return `${MEMBERSTACK_MEMBER_ID_FIELD_NAMES.primary} (Memberstack Member ID)`;
}

export function memberstackSlugLabel() {
  return `${MEMBERSTACK_MEMBER_ID_FIELD_NAMES.mirror} (Memberstack Member ID mirror)`;
}
