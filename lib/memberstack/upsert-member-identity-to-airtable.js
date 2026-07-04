/**
 * Memberstack webhook / sync → Airtable Users (identity only).
 * Airtable Company Profile + Workspace Access remain authoritative — never overwritten from Memberstack plans.
 */
import Airtable from "airtable";
import {
  AUTH_ROLE_HINT_FIELD_CANDIDATES,
  buildMemberstackSyncPatch,
  isAirtableFieldError,
  MS_SYNC_FILL_IF_BLANK,
  MS_SYNC_NEVER_WRITE,
  writeUsersRecordWithFieldFallback,
} from "../airtable-users-protected-patch.js";
import {
  INTAKE_USERS_EMAIL,
  INTAKE_USERS_FIRST_NAME,
  INTAKE_USERS_LAST_NAME,
  INTAKE_USERS_UNIQUE_WEBFLOW_ID,
} from "../../api/schemas/intake-deal-fields.js";
import { readLogicalCustomFields } from "./memberstack-custom-fields.js";

export { buildMemberstackSyncPatch, MS_SYNC_NEVER_WRITE, MS_SYNC_FILL_IF_BLANK };

const USERS_TABLE = process.env.AIRTABLE_ME_USERS_TABLE || "tbl6shiyz2wdUqE5F";

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    const err = new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
    err.statusCode = 500;
    throw err;
  }
  return new Airtable({ apiKey }).base(baseId);
}

function escapeEmail(email) {
  return String(email || "")
    .trim()
    .toLowerCase()
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "\\'");
}

async function findUserByEmailOrMemberstackId(base, email, memberstackId) {
  const normalizedEmail = String(email || "")
    .trim()
    .toLowerCase();
  if (normalizedEmail) {
    const esc = escapeEmail(normalizedEmail);
    const byEmail = await base(USERS_TABLE)
      .select({ filterByFormula: `{Email} = '${esc}'`, maxRecords: 1 })
      .firstPage();
    if (byEmail.length) return { record: byEmail[0], matchedBy: "email" };
  }
  if (memberstackId) {
    const escId = String(memberstackId).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
    const formula = `OR({${INTAKE_USERS_UNIQUE_WEBFLOW_ID}} = '${escId}', {slug} = '${escId}', {Slug} = '${escId}')`;
    const byId = await base(USERS_TABLE)
      .select({ filterByFormula: formula, maxRecords: 1 })
      .firstPage();
    if (byId.length) return { record: byId[0], matchedBy: "memberstack_id" };
  }
  return { record: null, matchedBy: null };
}

/**
 * @param {{
 *   memberstackId: string|null,
 *   email: string|null,
 *   customFields?: object|null,
 *   memberstackRoleHint?: string|null,
 *   statusOnWrite?: string,
 * }} input
 */
export async function upsertMemberIdentityToAirtable(input) {
  const memberstackId = input.memberstackId ? String(input.memberstackId).trim() : "";
  let email = input.email ? String(input.email).trim().toLowerCase() : "";
  const customFields =
    input.customFields && typeof input.customFields === "object" ? input.customFields : null;
  const logical = readLogicalCustomFields(customFields);
  if (!email && logical.airtableRecordId) {
    /* email required for create — caller should fetch member from API first */
  }

  if (!email && !memberstackId) {
    const err = new Error("Memberstack sync requires email or memberstack id");
    err.statusCode = 400;
    throw err;
  }

  const base = getBase();
  const found = await findUserByEmailOrMemberstackId(base, email, memberstackId);
  const isCreate = !found.record;
  const existingFields = found.record?.fields || {};

  const roleHint =
    (input.memberstackRoleHint && String(input.memberstackRoleHint).trim()) ||
    logical.companyType ||
    "";

  const identityPatch = {
    [INTAKE_USERS_EMAIL]: email || undefined,
    [INTAKE_USERS_UNIQUE_WEBFLOW_ID]: memberstackId || undefined,
    [INTAKE_USERS_FIRST_NAME]: logical.firstName || undefined,
    [INTAKE_USERS_LAST_NAME]: logical.lastName || undefined,
    "Phone Number": logical.phone || undefined,
    "Company Name": logical.companyName || undefined,
  };

  if (roleHint) {
    for (const hintField of AUTH_ROLE_HINT_FIELD_CANDIDATES) {
      identityPatch[hintField] = roleHint;
    }
    if (!AUTH_ROLE_HINT_FIELD_CANDIDATES.length) {
      identityPatch["User Type"] = roleHint;
    }
  }

  const companyProfileId = logical.companyProfileId;
  if (companyProfileId && /^rec[a-zA-Z0-9]{10,}$/.test(companyProfileId)) {
    identityPatch["Company Profile"] = [companyProfileId];
  }

  const statusFieldName = (process.env.SIGNUP_AIRTABLE_STATUS_FIELD || "").trim();
  const statusOnWrite = input.statusOnWrite || "";
  if (statusFieldName && statusOnWrite) {
    identityPatch[statusFieldName] = statusOnWrite;
  } else if (statusFieldName && isCreate && process.env.SIGNUP_AIRTABLE_SET_PENDING_STATUS !== "false") {
    identityPatch[statusFieldName] = (
      process.env.SIGNUP_AIRTABLE_PENDING_STATUS || "Pending"
    ).trim();
  }

  let patch = identityPatch;
  let skipped = [];
  if (!isCreate) {
    const built = buildMemberstackSyncPatch(existingFields, identityPatch);
    patch = built.patch;
    skipped = built.skipped;
  }

  Object.keys(patch).forEach((k) => {
    if (patch[k] === undefined) delete patch[k];
  });

  if (!Object.keys(patch).length && !isCreate) {
    return {
      recordId: found.record.id,
      email: email || existingFields.Email,
      created: false,
      matchedBy: found.matchedBy,
      skipped,
      warnings: ["no_identity_fields_to_update"],
    };
  }

  const warnings = [];

  const record = await writeUsersRecordWithFieldFallback(base, USERS_TABLE, {
    isCreate,
    recordId: found.record?.id,
    fields: patch,
  });
  if (skipped.length) warnings.push(`preserved_existing_fields:${skipped.join(",")}`);

  return {
    recordId: record.id,
    email: email || record.fields?.Email,
    created: isCreate,
    matchedBy: found.matchedBy || (isCreate ? "created" : null),
    skipped,
    warnings,
  };
}
