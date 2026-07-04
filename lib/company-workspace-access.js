/**
 * Company Type, Workspace Access, and permission flags (Owner-Operator hybrid model).
 *
 * Permission rule: use workspaceAccess / flags — not legacy role string alone.
 * legacyRole exists only for consumers that still read dealality.role (e.g. app shell nav).
 */

import {
  COMPANY_TYPE_AIRTABLE_FIELD,
  getAirtableFieldValue,
  isOwnerOperatorCompanyTypeString,
  normalizeCompanyTypeToFilterKey,
} from "./company-type-normalize.js";
import {
  COMPANY_ROLE_AIRTABLE_FIELD,
  companyRoleFromEcosystemField,
  normalizeCompanyRoleToForm,
} from "./company-role-normalize.js";

export const WORKSPACE_OWNER = "Owner";
export const WORKSPACE_OPERATOR = "Operator";
export const WORKSPACE_BRAND = "Brand";
export const WORKSPACE_DEMO = "Demo";
export const WORKSPACE_ADMIN = "Admin";

/** Preview workspaces for Demo users (read/switcher only — not production write permission). */
export const DEMO_PREVIEW_WORKSPACES = [
  WORKSPACE_OWNER,
  WORKSPACE_OPERATOR,
  WORKSPACE_BRAND,
];

const WORKSPACE_ACCESS_FIELD_NAMES = [
  process.env.AIRTABLE_COMPANY_WORKSPACE_ACCESS_FIELD || "Workspace Access",
  "workspace_access",
  "workspaceAccess",
];

const COMPANY_TYPE_TAGS_FIELD =
  process.env.AIRTABLE_COMPANY_TYPE_TAGS_FIELD || "Company Type Tags";

const THIRD_PARTY_MGMT_FIELD =
  process.env.AIRTABLE_COMPANY_THIRD_PARTY_MGMT_FIELD || "Third-Party Management Availability";

const OPERATING_MODEL_FIELD =
  process.env.AIRTABLE_COMPANY_OPERATING_MODEL_FIELD || "Operating Model";

const USER_ROLE_FIELD_CANDIDATES = (
  process.env.AIRTABLE_USERS_ROLE_FIELDS ||
  process.env.AIRTABLE_USERS_ROLE_FIELD ||
  "Platform Role,User Type,Role"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const ADMIN_ROLE_TOKENS = (process.env.DEALITY_ADMIN_ROLES || "admin,superadmin,platform admin")
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

const OWNER_ROLE_TOKENS = (
  process.env.DEALITY_OWNER_ROLES ||
  "owner,hotel owner,hotel owners,owner-operator,owner operator,owner_operator"
)
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

const BRAND_ROLE_TOKENS = (process.env.DEALITY_BRAND_ROLES || "brand,franchise,hotel brand")
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

const OPERATOR_ROLE_TOKENS = (
  process.env.DEALITY_OPERATOR_ROLES ||
  "operator,management,mgmt,hotel management,owner-operator,owner operator,owner_operator"
)
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

const OPERATE_TAGS = new Set([
  "operates own portfolio",
  "operates affiliated-owned hotels",
  "operates third-party hotels",
]);

/** Lowercase keys for eligibility checks (accepts legacy + canonical Airtable labels). */
const THIRD_PARTY_AVAILABLE_KEYS = new Set(["yes", "selectively", "case-by-case"]);

const THIRD_PARTY_UNAVAILABLE_KEYS = new Set(["no"]);

const THIRD_PARTY_UNKNOWN_KEYS = new Set(["unknown / to confirm", "unknown", ""]);

const PLATFORM_VISIBILITY_FIELD_NAMES = [
  "Company Platform Visibility",
  "Platform Visibility",
  "platform_visibility",
];

export function normalizeString(value) {
  if (value == null) return "";
  if (typeof value === "string") return value.trim();
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  if (typeof value === "object" && value !== null && typeof value.name === "string") {
    return value.name.trim();
  }
  return String(value).trim();
}

/** @param {unknown} value */
export function normalizeList(value) {
  if (value == null) return [];
  if (Array.isArray(value)) {
    return value.map((item) => normalizeString(item)).filter(Boolean);
  }
  const raw = normalizeString(value);
  if (!raw) return [];
  return raw
    .split(/[,;]+/)
    .map((part) => part.trim())
    .filter(Boolean);
}

export function normalizeCompanyType(value) {
  return normalizeString(value).toLowerCase();
}

function companyTypeRawFromFields(fields) {
  if (!fields || typeof fields !== "object") return "";
  return (
    normalizeString(getAirtableFieldValue(fields, COMPANY_TYPE_AIRTABLE_FIELD)) ||
    normalizeString(getAirtableFieldValue(fields, "company_type")) ||
    normalizeString(getAirtableFieldValue(fields, "companyType")) ||
    normalizeString(getAirtableFieldValue(fields, "User Type")) ||
    ""
  );
}

export function normalizeCompanyTypeTags(fields) {
  if (!fields || typeof fields !== "object") return [];
  for (const name of [COMPANY_TYPE_TAGS_FIELD, "company_type_tags", "companyTypeTags"]) {
    const list = normalizeList(getAirtableFieldValue(fields, name));
    if (list.length) return list;
  }
  return [];
}

export function normalizeEcosystemRole(fields) {
  if (!fields || typeof fields !== "object") return "";
  const raw =
    getAirtableFieldValue(fields, COMPANY_ROLE_AIRTABLE_FIELD) ||
    getAirtableFieldValue(fields, "ecosystemRole") ||
    "";
  return normalizeCompanyRoleToForm(raw) || "";
}

function isOwnerOperatorCompanyTypeValue(raw) {
  return isOwnerOperatorCompanyTypeString(raw);
}

export function isOwnerOperatorCompany(fields) {
  if (!fields || typeof fields !== "object") return false;

  const typeRaw = companyTypeRawFromFields(fields);
  if (isOwnerOperatorCompanyTypeValue(typeRaw)) return true;

  const filterKey = normalizeCompanyTypeToFilterKey(typeRaw);
  if (filterKey === "OWNER_OPERATOR") return true;

  const tags = normalizeCompanyTypeTags(fields).map((t) => t.toLowerCase());
  const ownsHotels = tags.some((t) => t === "owns hotels" || t.includes("owns hotel"));
  const operates = tags.some((t) => OPERATE_TAGS.has(t) || t.includes("operates "));
  if (ownsHotels && operates) return true;

  const ecosystem = normalizeEcosystemRole(fields);
  if (ecosystem === "OwnerOperator") return true;

  const ecosystemRaw = normalizeString(getAirtableFieldValue(fields, COMPANY_ROLE_AIRTABLE_FIELD)).toLowerCase();
  if (
    isOwnerOperatorCompanyTypeString(ecosystemRaw) ||
    ecosystemRaw.includes("owner-operator") ||
    ecosystemRaw.includes("owner operator")
  ) {
    return true;
  }

  return false;
}

function userRoleTokenFromFields(fields) {
  if (!fields || typeof fields !== "object") return "";
  for (const name of USER_ROLE_FIELD_CANDIDATES) {
    const v = normalizeString(getAirtableFieldValue(fields, name));
    if (v) return v.toLowerCase();
  }
  if (fields.dealalityRole) return normalizeString(fields.dealalityRole).toLowerCase();
  if (fields.role) return normalizeString(fields.role).toLowerCase();
  return "";
}

function tokenIncludesRole(token, list) {
  if (!token) return false;
  return list.some((t) => token.includes(t));
}

function explicitWorkspaceAccessFromFields(fields) {
  if (!fields || typeof fields !== "object") return [];
  for (const name of WORKSPACE_ACCESS_FIELD_NAMES) {
    const list = normalizeList(getAirtableFieldValue(fields, name));
    if (list.length) {
      return list.map((w) => normalizeWorkspaceLabel(w)).filter(Boolean);
    }
  }
  return [];
}

export function normalizeWorkspaceLabel(raw) {
  const s = normalizeString(raw);
  if (!s) return "";
  const lower = s.toLowerCase();
  if (lower === "owner" || lower.includes("owner-side")) return WORKSPACE_OWNER;
  if (lower === "operator" || lower.includes("operator-side")) return WORKSPACE_OPERATOR;
  if (lower === "brand") return WORKSPACE_BRAND;
  if (lower === "demo" || lower.includes("sandbox")) return WORKSPACE_DEMO;
  if (lower === "admin") return WORKSPACE_ADMIN;
  if (/^owner$/i.test(s)) return WORKSPACE_OWNER;
  if (/^operator$/i.test(s)) return WORKSPACE_OPERATOR;
  if (/^brand$/i.test(s)) return WORKSPACE_BRAND;
  if (/^demo$/i.test(s)) return WORKSPACE_DEMO;
  if (/^admin$/i.test(s)) return WORKSPACE_ADMIN;
  return "";
}

/** Normalize workspace list for Airtable write (exact select labels). */
export function normalizeWorkspaceAccessForAirtableWrite(list) {
  return [...new Set((list || []).map((w) => normalizeWorkspaceLabel(w)).filter(Boolean))];
}

/** Lowercase eligibility key for Third-Party Management Availability. */
export function thirdPartyAvailabilityEligibilityKey(raw) {
  const s = normalizeString(raw).toLowerCase();
  if (!s) return "";
  if (s === "case by case") return "case-by-case";
  return s;
}

function inferWorkspaceAccessFromFields(fields) {
  const access = new Set();
  if (!fields || typeof fields !== "object") return [];

  if (isOwnerOperatorCompany(fields)) {
    access.add(WORKSPACE_OWNER);
    access.add(WORKSPACE_OPERATOR);
    return [...access];
  }

  const filterKey = normalizeCompanyTypeToFilterKey(companyTypeRawFromFields(fields));
  if (filterKey === "OWNER_OPERATOR") {
    access.add(WORKSPACE_OWNER);
    access.add(WORKSPACE_OPERATOR);
  } else if (filterKey === "HOTEL OWNERS") {
    access.add(WORKSPACE_OWNER);
  } else if (filterKey === "HOTEL MGMT. COMPANY") {
    access.add(WORKSPACE_OPERATOR);
  } else if (filterKey === "HOTEL BRANDS (FRANCHISE)") {
    access.add(WORKSPACE_BRAND);
  }

  const ecosystem = normalizeEcosystemRole(fields);
  if (ecosystem === "Owner") access.add(WORKSPACE_OWNER);
  else if (ecosystem === "Operator") access.add(WORKSPACE_OPERATOR);
  else if (ecosystem === "Brand") access.add(WORKSPACE_BRAND);
  else if (ecosystem === "OwnerOperator") {
    access.add(WORKSPACE_OWNER);
    access.add(WORKSPACE_OPERATOR);
  } else if (ecosystem === "Both") {
    // Brand + Operator only — not Owner + Operator (per product audit).
    access.add(WORKSPACE_BRAND);
    access.add(WORKSPACE_OPERATOR);
  }

  const userToken = userRoleTokenFromFields(fields);
  if (tokenIncludesRole(userToken, ADMIN_ROLE_TOKENS)) access.add(WORKSPACE_ADMIN);
  if (tokenIncludesRole(userToken, OWNER_ROLE_TOKENS)) access.add(WORKSPACE_OWNER);
  if (tokenIncludesRole(userToken, BRAND_ROLE_TOKENS)) access.add(WORKSPACE_BRAND);
  if (tokenIncludesRole(userToken, OPERATOR_ROLE_TOKENS)) access.add(WORKSPACE_OPERATOR);

  return [...access];
}

/**
 * Effective workspace list: explicit Workspace Access on fields wins; else legacy inference.
 * @param {object} fields Merged user + company Airtable-shaped fields
 */
export function normalizeWorkspaceAccess(fields) {
  const explicit = explicitWorkspaceAccessFromFields(fields);
  if (explicit.length) return [...new Set(explicit)];
  return inferWorkspaceAccessFromFields(fields);
}

export function hasWorkspaceAccess(fields, workspace) {
  const target = normalizeWorkspaceLabel(workspace);
  if (!target) return false;
  return normalizeWorkspaceAccess(fields).includes(target);
}

export function canAccessOwnerWorkspace(fields) {
  return hasWorkspaceAccess(fields, WORKSPACE_OWNER);
}

export function canAccessOperatorWorkspace(fields) {
  return hasWorkspaceAccess(fields, WORKSPACE_OPERATOR);
}

export function canAccessBrandWorkspace(fields) {
  return hasWorkspaceAccess(fields, WORKSPACE_BRAND);
}

/** Demo = preview/sandbox; not Admin and not production Owner/Operator/Brand unless also listed. */
export function canAccessDemoWorkspace(fields) {
  return hasWorkspaceAccess(fields, WORKSPACE_DEMO);
}

export function getThirdPartyManagementAvailabilityRaw(fields) {
  if (!fields || typeof fields !== "object") return "";
  return thirdPartyAvailabilityEligibilityKey(
    getAirtableFieldValue(fields, THIRD_PARTY_MGMT_FIELD)
  );
}

export function isThirdPartyManagementAvailable(fields) {
  const key = getThirdPartyManagementAvailabilityRaw(fields);
  if (!key) return false;
  return THIRD_PARTY_AVAILABLE_KEYS.has(key);
}

export function isThirdPartyManagementUnavailable(fields) {
  const key = getThirdPartyManagementAvailabilityRaw(fields);
  return THIRD_PARTY_UNAVAILABLE_KEYS.has(key);
}

export function isThirdPartyManagementUnknown(fields) {
  const key = getThirdPartyManagementAvailabilityRaw(fields);
  return THIRD_PARTY_UNKNOWN_KEYS.has(key) || key.includes("unknown");
}

export function isCompanyHiddenFromMarketplace(fields) {
  if (!fields || typeof fields !== "object") return false;
  for (const name of PLATFORM_VISIBILITY_FIELD_NAMES) {
    const v = normalizeString(getAirtableFieldValue(fields, name)).toLowerCase();
    if (!v) continue;
    if (v.includes("anonymous") || v.includes("hidden")) return true;
  }
  return false;
}

/**
 * Operator Explorer / deal-request marketplace eligibility (future-safe + migration fallback).
 * @param {object|null} companyFields Company Profile fields (null if unmatched by name)
 * @param {{ isActiveSetup?: boolean, companyNameMatched?: boolean }} [options]
 */
export function evaluateOperatorMarketplaceEligibility(companyFields, options = {}) {
  const isActiveSetup = options.isActiveSetup !== false;
  const companyNameMatched = options.companyNameMatched !== false;

  const fields = companyFields && typeof companyFields === "object" ? companyFields : {};
  const companyType = companyTypeRawFromFields(fields);
  const normalizedCompanyType = normalizeCompanyTypeToFilterKey(companyType);
  const flags = getWorkspaceFlags(fields);
  const isOwnerOp = isOwnerOperatorCompany(fields);
  const workspaceAccess = flags.workspaceAccess;
  const companyDisplayBadges = getCompanyDisplayBadges(fields);

  const availRaw = getThirdPartyManagementAvailabilityRaw(fields);
  const thirdPartyManagementAvailability = availRaw || null;
  let thirdPartyManagementAvailabilityStatus = availRaw || "Unknown / Legacy";

  const hasOperatorWorkspace =
    canAccessOperatorWorkspace(fields) ||
    normalizedCompanyType === "HOTEL MGMT. COMPANY" ||
    (!companyNameMatched && isActiveSetup);

  const base = {
    isOwnerOperator: isOwnerOp,
    companyType: companyType || null,
    normalizedCompanyType: normalizedCompanyType || "",
    workspaceAccess,
    operatorExplorerEligible: false,
    operatorDealRequestEligible: false,
    thirdPartyManagementAvailability,
    thirdPartyManagementAvailabilityStatus,
    reviewBeforeOutreach: false,
    eligibilitySource: "excluded",
    companyDisplayBadges,
  };

  if (!isActiveSetup) {
    return { ...base, eligibilitySource: "inactive-operator-setup" };
  }
  if (isCompanyHiddenFromMarketplace(fields)) {
    return { ...base, eligibilitySource: "hidden-profile" };
  }
  if (!hasOperatorWorkspace) {
    return { ...base, eligibilitySource: "no-operator-workspace" };
  }
  if (THIRD_PARTY_UNAVAILABLE_KEYS.has(availRaw)) {
    return {
      ...base,
      thirdPartyManagementAvailabilityStatus: availRaw,
      eligibilitySource: "third-party-unavailable",
    };
  }

  if (!availRaw || availRaw.includes("unknown")) {
    return {
      ...base,
      operatorExplorerEligible: true,
      operatorDealRequestEligible: true,
      reviewBeforeOutreach: false,
      eligibilitySource: companyNameMatched
        ? isOwnerOp
          ? "owner-operator-legacy-migration"
          : "legacy-active-operator-setup"
        : "legacy-active-operator-setup",
    };
  }

  if (availRaw === "yes") {
    return {
      ...base,
      operatorExplorerEligible: true,
      operatorDealRequestEligible: true,
      reviewBeforeOutreach: false,
      eligibilitySource: "third-party-available",
    };
  }

  if (availRaw === "selectively" || availRaw === "case-by-case") {
    return {
      ...base,
      operatorExplorerEligible: true,
      operatorDealRequestEligible: true,
      reviewBeforeOutreach: true,
      thirdPartyManagementAvailabilityStatus: availRaw,
      eligibilitySource: "third-party-review-required",
    };
  }

  return {
    ...base,
    operatorExplorerEligible: true,
    operatorDealRequestEligible: true,
    eligibilitySource: "legacy-active-operator-setup",
  };
}

/** Attach eligibility fields to an operator list row. */
export function enrichOperatorListRowWithEligibility(listRow, companyFields, options = {}) {
  const isActiveSetup =
    String(listRow?.dealStatus || "")
      .trim()
      .toLowerCase() === "active";
  const eligibility = evaluateOperatorMarketplaceEligibility(companyFields, {
    isActiveSetup,
    companyNameMatched: Boolean(companyFields),
    ...options,
  });
  return { ...listRow, ...eligibility };
}

/**
 * @param {object} fields
 * @returns {{
 *   isOwner: boolean,
 *   isOperator: boolean,
 *   isBrand: boolean,
 *   isDemo: boolean,
 *   isAdmin: boolean,
 *   isOwnerOperator: boolean,
 *   workspaceAccess: string[],
 * }}
 */
export function getWorkspaceFlags(fields) {
  const workspaceAccess = normalizeWorkspaceAccess(fields);
  const userToken = userRoleTokenFromFields(fields);
  const isAdmin =
    workspaceAccess.includes(WORKSPACE_ADMIN) ||
    tokenIncludesRole(userToken, ADMIN_ROLE_TOKENS);
  const isOwnerOperator = isOwnerOperatorCompany(fields);
  const isOwner = workspaceAccess.includes(WORKSPACE_OWNER);
  const isOperator = workspaceAccess.includes(WORKSPACE_OPERATOR);
  const isBrand = workspaceAccess.includes(WORKSPACE_BRAND);
  const isDemo = workspaceAccess.includes(WORKSPACE_DEMO);

  return {
    isOwner,
    isOperator,
    isBrand,
    isDemo,
    isAdmin,
    isOwnerOperator,
    workspaceAccess,
  };
}

export function getDemoPreviewWorkspaces(flags) {
  if (!flags?.isDemo) return [];
  return [...DEMO_PREVIEW_WORKSPACES];
}

/** Canonical primary role for API consumers (may be owner-operator). */
export function getPrimaryRoleFromFlags(flags) {
  if (!flags) return "unknown";
  if (flags.isAdmin) return "admin";
  if (flags.isOwnerOperator || (flags.isOwner && flags.isOperator)) return "owner-operator";
  if (flags.isOwner) return "owner";
  if (flags.isBrand) return "brand";
  if (flags.isOperator) return "operator";
  return "unknown";
}

/**
 * Single role string for legacy UI (app shell, old nav).
 * Prefer owner when both owner+operator so My Deals nav stays available until workspace switcher ships.
 */
export function getLegacyRoleFromFlags(flags) {
  if (!flags) return "unknown";
  if (flags.isAdmin) return "admin";
  if (flags.isOwnerOperator || (flags.isOwner && flags.isOperator)) return "owner";
  if (flags.isOwner) return "owner";
  if (flags.isBrand) return "brand";
  if (flags.isOperator) return "operator";
  return "unknown";
}

export function getCompanyDisplayBadges(fields) {
  const badges = [];
  if (!fields || typeof fields !== "object") return badges;

  if (isOwnerOperatorCompany(fields)) badges.push("Hotel Owner - Operator");

  const operatingModel = normalizeString(getAirtableFieldValue(fields, OPERATING_MODEL_FIELD));
  if (operatingModel) badges.push(operatingModel);

  const availability = normalizeString(getAirtableFieldValue(fields, THIRD_PARTY_MGMT_FIELD));
  if (availability) badges.push(`Third-Party Management: ${availability}`);

  return badges;
}

/**
 * Build full access context from merged Airtable fields (user + company).
 * @param {object} fields
 * @param {{ roleSource?: string|null, roleRaw?: string|null, userRoleRaw?: string|null, companyTypeRaw?: string|null }} [meta]
 */
export function buildDealalityAccessContext(fields, meta = {}) {
  const flags = getWorkspaceFlags(fields);
  const primaryRole = getPrimaryRoleFromFlags(flags);
  const legacyRole = getLegacyRoleFromFlags(flags);
  const companyType = companyTypeRawFromFields(fields) || meta.companyTypeRaw || null;

  return {
    role: legacyRole,
    primaryRole,
    legacyRole,
    workspaceAccess: flags.workspaceAccess,
    flags: {
      isOwner: flags.isOwner,
      isOperator: flags.isOperator,
      isBrand: flags.isBrand,
      isDemo: flags.isDemo,
      isAdmin: flags.isAdmin,
      isOwnerOperator: flags.isOwnerOperator,
    },
    companyType,
    isOwner: flags.isOwner,
    isBrand: flags.isBrand,
    isOperator: flags.isOperator,
    isDemo: flags.isDemo,
    isAdmin: flags.isAdmin,
    isOwnerOperator: flags.isOwnerOperator,
    canAccessOwnerWorkspace: canAccessOwnerWorkspace(fields),
    canAccessOperatorWorkspace: canAccessOperatorWorkspace(fields),
    canAccessBrandWorkspace: canAccessBrandWorkspace(fields),
    canAccessDemoWorkspace: canAccessDemoWorkspace(fields),
    demoPreviewWorkspaces: getDemoPreviewWorkspaces(flags),
    thirdPartyManagementAvailable: isThirdPartyManagementAvailable(fields),
    companyDisplayBadges: getCompanyDisplayBadges(fields),
    activeWorkspace: null,
    roleRaw: meta.roleRaw ?? null,
    roleSource: meta.roleSource ?? null,
    userRoleRaw: meta.userRoleRaw ?? null,
    companyTypeRaw: meta.companyTypeRaw ?? companyType,
  };
}
