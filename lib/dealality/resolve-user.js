/**
 * Resolve Dealality user from Airtable Users (Memberstack id, then email).
 */

import Airtable from "airtable";
import {
  INTAKE_USERS_TABLE,
  INTAKE_USERS_EMAIL,
  INTAKE_USERS_UNIQUE_WEBFLOW_ID,
} from "../../api/schemas/intake-deal-fields.js";
import { escapeAirtableFormulaValue, cellToString, extractLinkedRecordIds } from "../airtable-utils.js";

const USERS_TABLE = process.env.AIRTABLE_ME_USERS_TABLE || INTAKE_USERS_TABLE;

const MEMBERSTACK_MATCH_FIELDS = (
  process.env.AIRTABLE_ME_USERS_MEMBERSTACK_FIELDS || INTAKE_USERS_UNIQUE_WEBFLOW_ID
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const ROLE_FIELD_CANDIDATES = (
  process.env.AIRTABLE_USERS_ROLE_FIELDS ||
  process.env.AIRTABLE_USERS_ROLE_FIELD ||
  "Platform Role,User Type,Role"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const STATUS_FIELD_CANDIDATES = (
  process.env.AIRTABLE_USERS_STATUS_FIELDS ||
  process.env.AIRTABLE_USERS_STATUS_FIELD ||
  "Status,Account Status"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const USERS_COMPANY_FIELD =
  process.env.AIRTABLE_USERS_COMPANY_LINK_FIELD || "Company Profile";

const COMPANY_NAME_LOOKUP_FIELD =
  process.env.AIRTABLE_COMPANY_PROFILE_NAME_FIELD || "Company Name";

const COMPANY_TABLE =
  process.env.AIRTABLE_COMPANY_PROFILE_TABLE || "tblItyfH6MlOnMKZ9";

const COMPANY_TYPE_FIELD =
  process.env.AIRTABLE_COMPANY_PROFILE_TYPE_FIELD || "Company Type";

const INACTIVE_STATUS_VALUES = (
  process.env.AIRTABLE_USERS_INACTIVE_STATUS_VALUES || "inactive,disabled,suspended,archived"
)
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

const ADMIN_ROLE_TOKENS = (process.env.DEALITY_ADMIN_ROLES || "admin,superadmin,platform admin")
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

const OWNER_ROLE_TOKENS = (process.env.DEALITY_OWNER_ROLES || "owner,hotel owner,hotel owners")
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

const BRAND_ROLE_TOKENS = (process.env.DEALITY_BRAND_ROLES || "brand,franchise,hotel brand")
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

const OPERATOR_ROLE_TOKENS = (process.env.DEALITY_OPERATOR_ROLES || "operator,management,mgmt,hotel management")
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

function firstFieldValue(fields, candidates) {
  for (const key of candidates) {
    const v = cellToString(fields[key]);
    if (v) return v;
  }
  return "";
}

function normalizeRoleToken(role) {
  return String(role || "")
    .trim()
    .toLowerCase();
}

export function classifyRole(roleRaw) {
  const r = normalizeRoleToken(roleRaw);
  if (!r) return { role: "unknown", isAdmin: false, isOwner: false, isBrand: false, isOperator: false };
  const isAdmin = ADMIN_ROLE_TOKENS.some((t) => r.includes(t));
  const isOwner = OWNER_ROLE_TOKENS.some((t) => r.includes(t));
  const isBrand = BRAND_ROLE_TOKENS.some((t) => r.includes(t));
  const isOperator = OPERATOR_ROLE_TOKENS.some((t) => r.includes(t));
  let role = roleRaw || "unknown";
  if (isAdmin) role = "admin";
  else if (isOwner) role = "owner";
  else if (isBrand) role = "brand";
  else if (isOperator) role = "operator";
  return { role, isAdmin, isOwner, isBrand, isOperator };
}

function roleIsKnown(info) {
  return !!(info && (info.isAdmin || info.isOwner || info.isBrand || info.isOperator));
}

/** @param {object} fields Airtable Users fields */
export function roleInfoFromUserFields(fields) {
  const roleRaw = firstFieldValue(fields || {}, ROLE_FIELD_CANDIDATES);
  const info = classifyRole(roleRaw);
  return { roleRaw: roleRaw || null, roleSource: roleRaw ? "user" : null, ...info };
}

async function fetchCompanyTypeRaw(base, companyRecordId) {
  if (!companyRecordId) return null;
  try {
    const rec = await base(COMPANY_TABLE).find(companyRecordId);
    return cellToString(rec.fields && rec.fields[COMPANY_TYPE_FIELD]) || null;
  } catch {
    return null;
  }
}

/**
 * Role from Users (Platform Role / User Type / Role), then Company Profile → Company Type.
 * @param {import('airtable').Base} base
 * @param {object} fields Airtable Users fields
 */
export async function roleInfoFromUserFieldsAsync(base, fields) {
  const userInfo = roleInfoFromUserFields(fields);
  if (roleIsKnown(userInfo)) return userInfo;

  const companyIds = extractLinkedRecordIds(fields && fields[USERS_COMPANY_FIELD]);
  const companyTypeRaw = await fetchCompanyTypeRaw(base, companyIds[0] || null);
  if (!companyTypeRaw) return userInfo;

  const fromCompany = classifyRole(companyTypeRaw);
  if (!roleIsKnown(fromCompany)) {
    return { ...userInfo, companyTypeRaw };
  }
  return {
    ...fromCompany,
    roleRaw: companyTypeRaw,
    roleSource: "company",
    userRoleRaw: userInfo.roleRaw || null,
    companyTypeRaw,
  };
}

function isInactiveStatus(statusRaw) {
  const s = normalizeRoleToken(statusRaw);
  if (!s) return false;
  return INACTIVE_STATUS_VALUES.some((v) => s === v || s.includes(v));
}

function buildMemberstackLookupFormula(memberstackId) {
  const lit = escapeAirtableFormulaValue(memberstackId);
  const parts = MEMBERSTACK_MATCH_FIELDS.map((field) => `{${field}} = '${lit}'`);
  if (parts.length === 1) return parts[0];
  return `OR(${parts.join(",")})`;
}

async function fetchCompanyName(base, companyRecordId) {
  if (!companyRecordId) return null;
  try {
    const rec = await base(COMPANY_TABLE).find(companyRecordId);
    return cellToString(rec.fields && rec.fields[COMPANY_NAME_LOOKUP_FIELD]) || null;
  } catch {
    return null;
  }
}

/**
 * @param {{ memberstackId: string, email?: string|null }} input
 */
export async function resolveDealalityUser(input) {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    const err = new Error("Airtable not configured");
    err.code = "server_misconfigured";
    throw err;
  }

  const memberstackId = String(input.memberstackId || "").trim();
  const emailHint = input.email ? String(input.email).trim().toLowerCase() : "";

  const base = new Airtable({ apiKey }).base(baseId);
  let rows = [];

  if (memberstackId) {
    const formula = buildMemberstackLookupFormula(memberstackId);
    rows = await base(USERS_TABLE).select({ filterByFormula: formula, maxRecords: 1 }).firstPage();
  }

  if (!rows.length && emailHint) {
    const lit = escapeAirtableFormulaValue(emailHint);
    const formula = `LOWER({${INTAKE_USERS_EMAIL}}) = '${lit.toLowerCase()}'`;
    rows = await base(USERS_TABLE).select({ filterByFormula: formula, maxRecords: 1 }).firstPage();
  }

  if (!rows.length) {
    return { found: false, reason: "user_not_found" };
  }

  const rec = rows[0];
  const fields = rec.fields || {};
  const email = cellToString(fields[INTAKE_USERS_EMAIL]) || input.email || null;
  const status = firstFieldValue(fields, STATUS_FIELD_CANDIDATES) || "active";
  const companyIds = extractLinkedRecordIds(fields[USERS_COMPANY_FIELD]);
  const companyId = companyIds[0] || null;
  const roleInfo = await roleInfoFromUserFieldsAsync(base, fields);
  const roleRaw = roleInfo.roleRaw || null;
  let companyName = null;
  if (companyId) {
    companyName = await fetchCompanyName(base, companyId);
  }

  if (isInactiveStatus(status)) {
    return {
      found: false,
      reason: "user_inactive",
      userRecordId: rec.id,
      status,
    };
  }

  return {
    found: true,
    userRecordId: rec.id,
    email,
    memberstackId: memberstackId || null,
    role: roleInfo.role,
    roleRaw: roleRaw || null,
    status,
    companyId,
    companyIds,
    companyName,
    isAdmin: roleInfo.isAdmin,
    isOwner: roleInfo.isOwner,
    isBrand: roleInfo.isBrand,
    isOperator: roleInfo.isOperator,
  };
}
