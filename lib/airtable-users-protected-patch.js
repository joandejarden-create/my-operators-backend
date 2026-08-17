/**
 * Airtable Users write guards — identity/onboarding only; authorization preserved.
 * Used by Memberstack sync and public signup upsert.
 */
import { extractLinkedRecordIds } from "./airtable-utils.js";

export const AUTH_ROLE_HINT_FIELD_CANDIDATES = (
  process.env.AIRTABLE_USERS_AUTH_ROLE_HINT_FIELDS ||
  "Auth Role Hint,Memberstack Role Hint,memberstack_role_hint"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

export const ROLE_HINT_SOURCE_FIELDS = (
  process.env.AIRTABLE_USERS_ROLE_FIELDS ||
  process.env.AIRTABLE_USERS_ROLE_FIELD ||
  "Platform Role,User Type,Role"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

/**
 * Never written from signup or Memberstack sync (Airtable authorizes).
 * Workspace Access on Users is legacy/read-only — set on Company Profile only.
 */
export const USERS_PROTECTED_NEVER_WRITE = new Set([
  "Workspace Access",
  "workspace_access",
  "workspaceAccess",
  "Company Type",
  "company_type",
  "companyType",
  "Company Type Tags",
  "company_type_tags",
  "companyTypeTags",
  "Third-Party Management Availability",
  "Operating Model",
  "Region Access",
  "region_access",
  "regionAccess",
  "Deal Access",
  "deal_access",
  "dealAccess",
  "Permission Level",
  "permission_level",
  "permissionLevel",
  "Demo",
  "Admin",
  "Owner",
  "Operator",
  "Brand",
]);

/** Memberstack sync alias (same set). */
export const MS_SYNC_NEVER_WRITE = USERS_PROTECTED_NEVER_WRITE;

export const USERS_ONBOARDING_FILL_IF_BLANK = new Set([
  ...ROLE_HINT_SOURCE_FIELDS,
  ...AUTH_ROLE_HINT_FIELD_CANDIDATES,
  "User Type",
  "Company Name",
  "Title",
  "Phone Number",
  "Reason to Join Platform",
  "How Did You Hear About Us",
]);

export const MS_SYNC_FILL_IF_BLANK = USERS_ONBOARDING_FILL_IF_BLANK;

/** Role-like value from signup form — hint only, not Workspace Access. */
export function resolveSignupRoleHint(body) {
  const b = body || {};
  const raw =
    (typeof b.role === "string" && b.role) ||
    (typeof b.companyType === "string" && b.companyType) ||
    "";
  return raw.trim();
}

export function isEmptyAirtableValue(value) {
  if (value == null || value === "") return true;
  if (Array.isArray(value) && !value.length) return true;
  return false;
}

function isRoleAuthorizationField(key) {
  if (key === "User Type") return true;
  return ROLE_HINT_SOURCE_FIELDS.includes(key);
}

function isAuthRoleHintField(key) {
  return AUTH_ROLE_HINT_FIELD_CANDIDATES.includes(key);
}

/**
 * Memberstack webhook sync — preserve authorization; fill-if-blank onboarding fields.
 * @param {object} existingFields
 * @param {object} candidatePatch
 * @returns {{ patch: object, skipped: string[] }}
 */
export function buildMemberstackSyncPatch(existingFields, candidatePatch) {
  const patch = {};
  const skipped = [];
  const existing = existingFields || {};

  for (const [key, value] of Object.entries(candidatePatch)) {
    if (value == null || value === "") continue;
    if (USERS_PROTECTED_NEVER_WRITE.has(key)) {
      skipped.push(key);
      continue;
    }
    if (key === "Company Profile") {
      const linked = extractLinkedRecordIds(existing[key]);
      if (linked.length) {
        skipped.push(key);
        continue;
      }
    }
    if (USERS_ONBOARDING_FILL_IF_BLANK.has(key) || USERS_ONBOARDING_FILL_IF_BLANK.has(key.trim())) {
      if (!isEmptyAirtableValue(existing[key])) {
        skipped.push(key);
        continue;
      }
    }
    patch[key] = value;
  }
  return { patch, skipped };
}

/**
 * Public signup — role from form is a hint, not permission.
 * @param {object} existingFields
 * @param {object} candidatePatch
 * @param {{ isCreate?: boolean, roleHint?: string }} [options]
 * @returns {{ patch: object, skipped: string[] }}
 */
export function buildSignupUsersPatch(existingFields, candidatePatch, options = {}) {
  const isCreate = Boolean(options.isCreate);
  const roleHint = typeof options.roleHint === "string" ? options.roleHint.trim() : "";
  const existing = existingFields || {};
  const merged = { ...candidatePatch };
  const patch = {};
  const skipped = [];

  if (roleHint) {
    for (const hintField of AUTH_ROLE_HINT_FIELD_CANDIDATES) {
      merged[hintField] = roleHint;
    }
    if (!AUTH_ROLE_HINT_FIELD_CANDIDATES.length && isCreate && isEmptyAirtableValue(existing["User Type"])) {
      merged["User Type"] = roleHint;
    }
  }

  for (const [key, value] of Object.entries(merged)) {
    if (value == null || value === "") continue;
    if (USERS_PROTECTED_NEVER_WRITE.has(key)) {
      skipped.push(key);
      continue;
    }
    if (key === "Company Profile") {
      const linked = extractLinkedRecordIds(existing[key]);
      if (linked.length) {
        skipped.push(key);
        continue;
      }
    }
    if (!isCreate) {
      if (isAuthRoleHintField(key)) {
        if (roleHint) patch[key] = roleHint;
        else skipped.push(key);
        continue;
      }
      if (isRoleAuthorizationField(key)) {
        skipped.push(key);
        continue;
      }
    } else if (isRoleAuthorizationField(key) && !isAuthRoleHintField(key)) {
      if (!isEmptyAirtableValue(existing[key])) {
        skipped.push(key);
        continue;
      }
    }
    if (!isCreate && USERS_ONBOARDING_FILL_IF_BLANK.has(key)) {
      if (!isAuthRoleHintField(key) && !isEmptyAirtableValue(existing[key])) {
        skipped.push(key);
        continue;
      }
    }
    if (isCreate && USERS_ONBOARDING_FILL_IF_BLANK.has(key) && !isAuthRoleHintField(key)) {
      if (!isEmptyAirtableValue(existing[key])) {
        skipped.push(key);
        continue;
      }
    }
    patch[key] = value;
  }
  return { patch, skipped };
}

export function isAirtableFieldError(err) {
  return (
    err?.statusCode === 422 &&
    err?.message &&
    (err.message.includes("Unknown field") || err.message.includes("invalid"))
  );
}

/**
 * Create/update with unknown-field stripping (Auth Role Hint safe when missing in base).
 */
export async function writeUsersRecordWithFieldFallback(base, tableId, { isCreate, recordId, fields }) {
  const working = { ...fields };

  for (let attempt = 0; attempt < 40; attempt++) {
    try {
      if (isCreate) return await base(tableId).create(working, { typecast: true });
      return await base(tableId).update(recordId, working, { typecast: true });
    } catch (err) {
      if (!isAirtableFieldError(err)) throw err;
      const m =
        String(err.message).match(/Unknown field name: "?([^"]+)"?/i) ||
        String(err.message).match(/Field "?([^"]+)"? cannot accept/i);
      const bad = m && m[1] ? m[1].trim() : null;
      if (bad && Object.prototype.hasOwnProperty.call(working, bad)) {
        delete working[bad];
        if (!Object.keys(working).length) {
          const emptyErr = new Error("No writable Users fields remain after schema fallback");
          emptyErr.statusCode = 422;
          throw emptyErr;
        }
        continue;
      }
      throw err;
    }
  }
  const err = new Error("Users write fallback exceeded retry limit");
  err.statusCode = 422;
  throw err;
}
