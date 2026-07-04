/**
 * Company Profile Owner-Operator / workspace field mappings and capability derivation.
 * Safe when Airtable columns are missing (unknown fields stripped on write).
 */

import {
  COMPANY_TYPE_FILTER_TO_AIRTABLE,
  COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE,
  COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE_ALIASES,
  isOwnerOperatorCompanyTypeString,
} from "./company-type-normalize.js";
import {
  normalizeWorkspaceAccessForAirtableWrite,
  WORKSPACE_BRAND,
  WORKSPACE_OPERATOR,
  WORKSPACE_OWNER,
} from "./company-workspace-access.js";

export const MAP_CP_AIRTABLE = {
  companyType: "Company Type",
  companyTypeTags: "Company Type Tags",
  workspaceAccess: "Workspace Access",
  coreProfileStatus: "Core Profile Status",
  ownerProfileStatus: "Owner Profile Status",
  operatorProfileStatus: "Operator Profile Status",
  developerProfileStatus: "Developer Profile Status",
  operatingModel: "Operating Model",
  thirdPartyManagementAvailability: "Third-Party Management Availability",
  potentialConflictFlags: "Potential Conflict Flags",
  competitiveSensitivityNotes: "Competitive Sensitivity Notes",
};

/** Form/API key → Airtable Company Type (exact select strings). */
export const COMPANY_TYPE_FORM_TO_AIRTABLE = {
  Brand: "Hotel Brands (Franchise)",
  Operator: "Hotel Management Company",
  Owner: "Hotel Owner",
  hotel_owner: "Hotel Owner",
  hotel_management_company: "Hotel Management Company",
  hotel_brands_franchise: "Hotel Brands (Franchise)",
  hospitality_consultants: "Hospitality Consultants",
  owner_operator: COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE,
  hotel_owner_operator: COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE,
  "owner-operator": COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE,
  OwnerOperator: COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE,
  Advisor: "Hospitality Consultants",
  Lender: "Hospitality Consultants",
  other: "Other",
  Other: "Other",
};

/** Canonical Airtable Company Type → Company Settings form key. */
export const COMPANY_TYPE_AIRTABLE_TO_FORM_CANONICAL = {
  "Hotel Owner": "Owner",
  "Hotel Management Company": "Operator",
  "Hotel Brands (Franchise)": "Brand",
  "Hospitality Consultants": "Advisor",
  [COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE]: "owner_operator",
  Other: "Other",
  "Owner-Operator": "owner_operator",
  "Owner Operator": "owner_operator",
  "Hotel Owner Operator": "owner_operator",
};

export function airtableCompanyTypeToFormKey(airtableValue) {
  const v = toStr(airtableValue);
  if (!v) return "";
  if (COMPANY_TYPE_AIRTABLE_TO_FORM_CANONICAL[v]) return COMPANY_TYPE_AIRTABLE_TO_FORM_CANONICAL[v];
  for (const [form, airtable] of Object.entries(COMPANY_TYPE_FORM_TO_AIRTABLE)) {
    if (airtable === v) return form;
  }
  return v;
}

/** @alias */
export const fromAirtableCompanyType = airtableCompanyTypeToFormKey;

export function pickFirstCompanyTypeInput(value) {
  if (value == null) return "";
  if (Array.isArray(value)) {
    for (const item of value) {
      const s = toStr(item);
      if (s) return s;
    }
    return "";
  }
  return toStr(value).replace(/^\[|\]$/g, "");
}

/** True when value is an internal form/API key — must not be written to Airtable. */
export function isInternalCompanyTypeKey(value) {
  const v = pickFirstCompanyTypeInput(value).toLowerCase().replace(/[\[\]]/g, "");
  if (!v) return false;
  return (
    v === "owner_operator" ||
    v === "hotel_owner_operator" ||
    v === "owner-operator" ||
    v === "owneroperator" ||
    v === "owner_operator[]" ||
    v === "ownoperator"
  );
}

const KNOWN_AIRTABLE_COMPANY_TYPES = new Set([
  "Hotel Owner",
  "Hotel Management Company",
  "Hotel Brands (Franchise)",
  "Hospitality Consultants",
  COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE,
  "Other",
]);

/**
 * Form/API/filter value → exact Airtable Company Type single-select string.
 * Never returns internal keys (e.g. owner_operator).
 */
export function toAirtableCompanyType(value) {
  const v = pickFirstCompanyTypeInput(value);
  if (!v) return "";

  const direct = COMPANY_TYPE_FORM_TO_AIRTABLE[v];
  if (direct) return direct;

  const lower = v.toLowerCase();
  for (const [form, airtable] of Object.entries(COMPANY_TYPE_FORM_TO_AIRTABLE)) {
    if (form.toLowerCase() === lower) return airtable;
  }

  if (isOwnerOperatorCompanyTypeString(v)) return COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE;

  if (KNOWN_AIRTABLE_COMPANY_TYPES.has(v)) return v;

  const filterMapped = COMPANY_TYPE_FILTER_TO_AIRTABLE[v];
  if (filterMapped) return filterMapped;

  if (
    lower === "owner_operator" ||
    lower === "hotel_owner_operator" ||
    lower === "owner-operator" ||
    lower === "owneroperator"
  ) {
    return COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE;
  }

  return "";
}

export function sanitizeCompanyTypeFieldOnPayload(fields) {
  return finalizeCompanyProfileFieldsForAirtableWrite(fields, { loud: false });
}

const STRAY_FORM_KEYS_ON_AIRTABLE_PAYLOAD = [
  "companyType",
  "company_type",
  "derivedCompanyType",
  "companyTypeKey",
  "companyTypeDisplay",
];

/**
 * Last-line guard immediately before Airtable Company Profile create/update.
 * - Maps Company Type through toAirtableCompanyType
 * - Strips internal form keys from payload
 * - Blocks owner_operator (and aliases) from reaching Airtable
 */
export function finalizeCompanyProfileFieldsForAirtableWrite(fields, options = {}) {
  const loud = options.loud !== false;
  if (!fields || typeof fields !== "object") return fields;

  for (const stray of STRAY_FORM_KEYS_ON_AIRTABLE_PAYLOAD) {
    if (Object.prototype.hasOwnProperty.call(fields, stray)) {
      delete fields[stray];
    }
  }

  const ctKey = MAP_CP_AIRTABLE.companyType;
  if (!Object.prototype.hasOwnProperty.call(fields, ctKey)) {
    return fields;
  }

  const raw = fields[ctKey];
  let safe = toAirtableCompanyType(raw);
  if (!safe && isInternalCompanyTypeKey(raw)) {
    safe = COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE;
  }

  if (!safe) {
    delete fields[ctKey];
    return fields;
  }

  fields[ctKey] = safe;

  if (isInternalCompanyTypeKey(fields[ctKey])) {
    const msg =
      `Company Profile BLOCKED: internal Company Type key would be written to Airtable: ${JSON.stringify(fields[ctKey])}`;
    console.error(msg, { raw, keys: Object.keys(fields) });
    if (process.env.NODE_ENV !== "production" && loud) {
      throw new Error(msg);
    }
    fields[ctKey] = COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE;
  }

  return fields;
}

export const CAPABILITY_OPTIONS = [
  { id: "owns_assets", label: "We own or control hotel assets", tag: "Owns Hotels" },
  { id: "develops", label: "We develop hotels", tag: "Develops Hotels" },
  { id: "operates_own", label: "We operate hotels for our own portfolio", tag: "Operates Own Portfolio" },
  {
    id: "operates_affiliated",
    label: "We operate hotels for affiliated-owned hotels",
    tag: "Operates Affiliated-Owned Hotels",
  },
  {
    id: "operates_third_party",
    label: "We operate hotels for third-party owners",
    tag: "Operates Third-Party Hotels",
  },
  { id: "brand", label: "We are a hotel brand / franchisor", tag: "Brand / Franchisor" },
  { id: "investor", label: "We invest in hotel assets", tag: "Capital Provider" },
  {
    id: "advisor",
    label: "We advise, broker, finance, or support hotel transactions",
    tag: "Consultant / Advisor",
  },
];

/** Exact Airtable single-select labels (Company Profile > Operating Model). */
export const OPERATING_MODEL_AIRTABLE_OPTIONS = [
  "Own-and-Operate Only",
  "Affiliated-Owned Hotels Only",
  "Third-Party Management",
  "Mixed Owner/Operator Model",
  "Asset-Light Management Platform",
  "Franchisee/Operator Model",
  "Unknown / To Confirm",
];

/** Input aliases → canonical Airtable Operating Model label (lowercase keys). */
const OPERATING_MODEL_ALIAS_TO_AIRTABLE = {
  "own-and-operate only": "Own-and-Operate Only",
  "own and operate only": "Own-and-Operate Only",
  own_and_operate_only: "Own-and-Operate Only",
  "affiliated-owned hotels only": "Affiliated-Owned Hotels Only",
  "affiliated owned hotels only": "Affiliated-Owned Hotels Only",
  "third-party management": "Third-Party Management",
  "third party management": "Third-Party Management",
  "mixed owner/operator model": "Mixed Owner/Operator Model",
  "mixed owner-operator model": "Mixed Owner/Operator Model",
  "mixed owner operator model": "Mixed Owner/Operator Model",
  "asset-light management platform": "Asset-Light Management Platform",
  "asset light management platform": "Asset-Light Management Platform",
  "franchisee/operator model": "Franchisee/Operator Model",
  "franchisee operator model": "Franchisee/Operator Model",
};

function operatingModelLookupKey(value) {
  return toStr(value)
    .toLowerCase()
    .replace(/_/g, "-")
    .replace(/\s+/g, " ")
    .trim();
}

/** Exact Airtable single-select labels (Third-Party Management Availability). */
export const THIRD_PARTY_MGMT_AIRTABLE_OPTIONS = [
  "Yes",
  "No",
  "Selectively",
  "Case-by-Case",
  "Unknown / To Confirm",
];

const THIRD_PARTY_ALIAS_TO_AIRTABLE = {
  "case-by-case": "Case-by-Case",
  "case by case": "Case-by-Case",
};

export const PROFILE_STATUS_AIRTABLE_OPTIONS = [
  "Not Started",
  "In Progress",
  "Complete",
  "Needs Review",
  "Not Applicable",
];

export const CONFLICT_FLAG_AIRTABLE_OPTIONS = [
  "Owns competing hotels in market",
  "Operates competing hotels in market",
  "Brand conflict possible",
  "Requires NDA before disclosure",
  "Related-party ownership",
  "No known conflict",
  "To Be Reviewed",
];

const OWN_TAGS = new Set(["Owns Hotels", "Develops Hotels"]);
const OPERATE_TAGS = new Set([
  "Operates Own Portfolio",
  "Operates Affiliated-Owned Hotels",
  "Operates Third-Party Hotels",
]);

const PROFILE_STATUS_LOCKED = new Set(["Complete", "Needs Review"]);

function toStr(v) {
  return v == null ? "" : String(v).trim();
}

export function parseJsonArray(raw) {
  if (raw == null || raw === "") return [];
  if (Array.isArray(raw)) return raw.map(toStr).filter(Boolean);
  const s = toStr(raw);
  if (!s) return [];
  if (s.startsWith("[") && s.endsWith("]")) {
    try {
      const parsed = JSON.parse(s);
      return Array.isArray(parsed) ? parsed.map(toStr).filter(Boolean) : [];
    } catch {
      return [];
    }
  }
  return s
    .split(/[,;]+/)
    .map((p) => p.trim())
    .filter(Boolean);
}

export function capabilitiesToTags(capabilityIdsOrTags) {
  const inputs = (capabilityIdsOrTags || []).map((c) => toStr(c)).filter(Boolean);
  const idToTag = Object.fromEntries(CAPABILITY_OPTIONS.map((o) => [o.id, o.tag]));
  const tagSet = new Set(CAPABILITY_OPTIONS.map((o) => o.tag));
  const tags = [];
  for (const inp of inputs) {
    if (idToTag[inp]) tags.push(idToTag[inp]);
    else if (tagSet.has(inp)) tags.push(inp);
  }
  return [...new Set(tags)];
}

export function tagsIncludeOwnership(tags) {
  return (tags || []).some((t) => OWN_TAGS.has(t));
}

export function tagsIncludeOperations(tags) {
  return (tags || []).some((t) => OPERATE_TAGS.has(t));
}

/**
 * Derive company type + workspace from tags (capabilities).
 * @returns {{ companyTypeForm: string, companyTypeAirtable: string, workspaceAccess: string[] }}
 */
export function deriveCompanyClassificationFromTags(tags) {
  const list = [...new Set((tags || []).map(toStr).filter(Boolean))];
  const owns = tagsIncludeOwnership(list);
  const operates = tagsIncludeOperations(list);
  const isBrand = list.includes("Brand / Franchisor");
  const isAdvisorOnly =
    list.includes("Consultant / Advisor") &&
    !owns &&
    !operates &&
    !isBrand &&
    !list.includes("Capital Provider");

  if (owns && operates) {
    return {
      companyTypeForm: "owner_operator",
      companyTypeAirtable: COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE,
      workspaceAccess: [WORKSPACE_OWNER, WORKSPACE_OPERATOR],
    };
  }
  if (isBrand) {
    return {
      companyTypeForm: "Brand",
      companyTypeAirtable: COMPANY_TYPE_FORM_TO_AIRTABLE.Brand,
      workspaceAccess: [WORKSPACE_BRAND],
    };
  }
  if (operates && !owns) {
    return {
      companyTypeForm: "Operator",
      companyTypeAirtable: COMPANY_TYPE_FORM_TO_AIRTABLE.Operator,
      workspaceAccess: [WORKSPACE_OPERATOR],
    };
  }
  if (owns && !operates) {
    return {
      companyTypeForm: "Owner",
      companyTypeAirtable: COMPANY_TYPE_FORM_TO_AIRTABLE.Owner,
      workspaceAccess: [WORKSPACE_OWNER],
    };
  }
  if (isAdvisorOnly) {
    return {
      companyTypeForm: "Advisor",
      companyTypeAirtable: COMPANY_TYPE_FORM_TO_AIRTABLE.Advisor,
      workspaceAccess: [],
    };
  }
  return {
    companyTypeForm: "Other",
    companyTypeAirtable: COMPANY_TYPE_FORM_TO_AIRTABLE.Other,
    workspaceAccess: [],
  };
}

/**
 * Default third-party availability from capabilities (does not overwrite existing user value).
 */
export function suggestThirdPartyManagementAvailability(tags, existingValue, operatingModel) {
  const existing = toStr(existingValue);
  if (existing) return existing;

  const list = tags || [];
  if (list.includes("Operates Third-Party Hotels")) return "Yes";
  if (toStr(operatingModel) === "Mixed Owner/Operator Model") return "Case-by-Case";
  if (toStr(operatingModel).toLowerCase() === "mixed owner/operator model") return "Case-by-Case";
  if (
    list.includes("Operates Own Portfolio") ||
    list.includes("Operates Affiliated-Owned Hotels")
  ) {
    if (!list.includes("Operates Third-Party Hotels")) return "No";
  }
  if (tagsIncludeOwnership(list) && tagsIncludeOperations(list)) {
    return "Case-by-Case";
  }
  return "";
}

export function suggestOperatingModel(tags, existingValue) {
  const existing = toStr(existingValue);
  if (existing) return toAirtableOperatingModel(existing) || existing;
  const list = tags || [];
  if (list.includes("Operates Third-Party Hotels") && tagsIncludeOwnership(list)) {
    return "Mixed Owner/Operator Model";
  }
  return "";
}

const STATUS_NOT_APPLICABLE = "Not Applicable";
const STATUS_IN_PROGRESS = "In Progress";

/**
 * Profile status defaults when blank — never overwrites Complete / Needs Review.
 */
export function applyProfileStatusDefaults(companyTypeAirtable, existingStatuses = {}) {
  const out = { ...existingStatuses };
  const isLocked = (key) => PROFILE_STATUS_LOCKED.has(toStr(out[key]));

  const setIfBlank = (key, value) => {
    if (isLocked(key)) return;
    if (!toStr(out[key])) out[key] = value;
  };

  if (companyTypeAirtable === COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE) {
    setIfBlank("ownerProfileStatus", STATUS_IN_PROGRESS);
    setIfBlank("operatorProfileStatus", STATUS_IN_PROGRESS);
    setIfBlank("developerProfileStatus", STATUS_NOT_APPLICABLE);
  } else if (companyTypeAirtable === COMPANY_TYPE_FORM_TO_AIRTABLE.Owner) {
    setIfBlank("ownerProfileStatus", STATUS_IN_PROGRESS);
    setIfBlank("operatorProfileStatus", STATUS_NOT_APPLICABLE);
    setIfBlank("developerProfileStatus", STATUS_NOT_APPLICABLE);
  } else if (companyTypeAirtable === COMPANY_TYPE_FORM_TO_AIRTABLE.Operator) {
    setIfBlank("operatorProfileStatus", STATUS_IN_PROGRESS);
    setIfBlank("ownerProfileStatus", STATUS_NOT_APPLICABLE);
    setIfBlank("developerProfileStatus", STATUS_NOT_APPLICABLE);
  }

  setIfBlank("coreProfileStatus", STATUS_IN_PROGRESS);
  return out;
}

function mapSelectValue(formVal, allowed, fallback) {
  const v = toStr(formVal);
  if (!v) return "";
  if (allowed.includes(v)) return v;
  const lower = v.toLowerCase();
  const match = allowed.find((a) => a.toLowerCase() === lower);
  return match || fallback || "";
}

/** Form/UI alias → exact Airtable Operating Model label. */
export function toAirtableOperatingModel(value) {
  const v = pickFirstCompanyTypeInput(value);
  if (!v) return "";
  if (OPERATING_MODEL_AIRTABLE_OPTIONS.includes(v)) return v;
  const key = operatingModelLookupKey(v);
  if (OPERATING_MODEL_ALIAS_TO_AIRTABLE[key]) return OPERATING_MODEL_ALIAS_TO_AIRTABLE[key];
  const keySpaces = key.replace(/-/g, " ");
  if (OPERATING_MODEL_ALIAS_TO_AIRTABLE[keySpaces]) {
    return OPERATING_MODEL_ALIAS_TO_AIRTABLE[keySpaces];
  }
  return mapSelectValue(v, OPERATING_MODEL_AIRTABLE_OPTIONS, "");
}

/** Form/UI alias → exact Airtable Third-Party Management Availability label. */
export function toAirtableThirdPartyManagement(value) {
  const v = pickFirstCompanyTypeInput(value);
  if (!v) return "";
  if (THIRD_PARTY_MGMT_AIRTABLE_OPTIONS.includes(v)) return v;
  const lower = v.toLowerCase().trim();
  if (THIRD_PARTY_ALIAS_TO_AIRTABLE[lower]) return THIRD_PARTY_ALIAS_TO_AIRTABLE[lower];
  return mapSelectValue(v, THIRD_PARTY_MGMT_AIRTABLE_OPTIONS, "");
}

/**
 * Merge Owner-Operator extension fields into Airtable payload from form body.
 * @param {Record<string, unknown>} body
 * @param {Record<string, unknown>} fields
 * @param {{ prefill?: Record<string, unknown> }} [ctx]
 */
export function mergeOwnerOperatorExtensionFields(body, fields, ctx = {}) {
  const prefill = ctx.prefill || {};
  const warnings = [];

  let tags = parseJsonArray(body.companyTypeTagsJson ?? body.companyTypeTags);
  const capabilityIds = parseJsonArray(body.companyCapabilitiesJson ?? body.companyCapabilities);
  if (capabilityIds.length) {
    tags = capabilitiesToTags(capabilityIds);
  }

  let companyTypeForm = toStr(body.companyType);
  let workspaceAccess = parseJsonArray(body.workspaceAccessJson ?? body.workspaceAccess);

  let derivedClassification = null;
  if (tags.length) {
    derivedClassification = deriveCompanyClassificationFromTags(tags);
    if (!companyTypeForm || body.deriveCompanyTypeFromCapabilities === "1") {
      companyTypeForm = derivedClassification.companyTypeForm;
    }
    if (!workspaceAccess.length || body.deriveWorkspaceFromCapabilities === "1") {
      workspaceAccess = derivedClassification.workspaceAccess;
    }
    fields[MAP_CP_AIRTABLE.companyTypeTags] = tags;
  } else if (tags.length === 0 && body.companyTypeTagsJson != null) {
    fields[MAP_CP_AIRTABLE.companyTypeTags] = [];
  }

  const companyTypeRaw =
    pickFirstCompanyTypeInput(body.derivedCompanyType) ||
    pickFirstCompanyTypeInput(body.companyTypeKey) ||
    companyTypeForm;
  let companyTypeAirtable = "";
  if (
    derivedClassification &&
    (body.deriveCompanyTypeFromCapabilities === "1" || !pickFirstCompanyTypeInput(body.companyType))
  ) {
    companyTypeAirtable = derivedClassification.companyTypeAirtable;
  } else {
    companyTypeAirtable = toAirtableCompanyType(companyTypeRaw);
  }
  if (companyTypeAirtable) {
    fields[MAP_CP_AIRTABLE.companyType] = companyTypeAirtable;
  }

  if (workspaceAccess.length) {
    fields[MAP_CP_AIRTABLE.workspaceAccess] =
      normalizeWorkspaceAccessForAirtableWrite(workspaceAccess);
  }

  let operatingModel = toStr(body.operatingModel);
  if (!operatingModel && tags.length) {
    operatingModel = suggestOperatingModel(tags, toStr(prefill.operatingModel));
  }

  const existingThirdParty =
    toStr(prefill.thirdPartyManagementAvailability) ||
    toStr(body.existingThirdPartyManagementAvailability);
  let thirdParty = "";
  const submittedThird = body.thirdPartyManagementAvailability;
  if (submittedThird != null && submittedThird !== "") {
    thirdParty = toStr(submittedThird);
  } else if (existingThirdParty) {
    thirdParty = existingThirdParty;
  } else if (tags.length) {
    thirdParty = suggestThirdPartyManagementAvailability(
      tags,
      "",
      operatingModel || toStr(prefill.operatingModel)
    );
  }
  if (thirdParty) {
    const mapped = toAirtableThirdPartyManagement(thirdParty);
    if (mapped) fields[MAP_CP_AIRTABLE.thirdPartyManagementAvailability] = mapped;
    else warnings.push("invalid_third_party_management_availability");
  }

  if (operatingModel) {
    const mapped = toAirtableOperatingModel(operatingModel);
    if (mapped) fields[MAP_CP_AIRTABLE.operatingModel] = mapped;
    else warnings.push("invalid_operating_model");
  }

  const conflictFlags = parseJsonArray(
    body.potentialConflictFlagsJson ?? body.potentialConflictFlags
  );
  if (conflictFlags.length) {
    const valid = conflictFlags.filter((f) => CONFLICT_FLAG_AIRTABLE_OPTIONS.includes(f));
    if (valid.length) fields[MAP_CP_AIRTABLE.potentialConflictFlags] = valid;
  }

  if (body.competitiveSensitivityNotes != null && body.competitiveSensitivityNotes !== "") {
    fields[MAP_CP_AIRTABLE.competitiveSensitivityNotes] = toStr(
      body.competitiveSensitivityNotes
    );
  }

  const statusInputs = {
    coreProfileStatus: body.coreProfileStatus ?? prefill.coreProfileStatus,
    ownerProfileStatus: body.ownerProfileStatus ?? prefill.ownerProfileStatus,
    operatorProfileStatus: body.operatorProfileStatus ?? prefill.operatorProfileStatus,
    developerProfileStatus: body.developerProfileStatus ?? prefill.developerProfileStatus,
  };

  const companyTypeForStatus =
    toAirtableCompanyType(fields[MAP_CP_AIRTABLE.companyType]) ||
    toAirtableCompanyType(companyTypeForm) ||
    "";
  const withDefaults = applyProfileStatusDefaults(companyTypeForStatus, {
    coreProfileStatus: toStr(statusInputs.coreProfileStatus),
    ownerProfileStatus: toStr(statusInputs.ownerProfileStatus),
    operatorProfileStatus: toStr(statusInputs.operatorProfileStatus),
    developerProfileStatus: toStr(statusInputs.developerProfileStatus),
  });

  for (const [formKey, airtableKey] of [
    ["coreProfileStatus", MAP_CP_AIRTABLE.coreProfileStatus],
    ["ownerProfileStatus", MAP_CP_AIRTABLE.ownerProfileStatus],
    ["operatorProfileStatus", MAP_CP_AIRTABLE.operatorProfileStatus],
    ["developerProfileStatus", MAP_CP_AIRTABLE.developerProfileStatus],
  ]) {
    const v = toStr(withDefaults[formKey]);
    if (!v) continue;
    const mapped = mapSelectValue(v, PROFILE_STATUS_AIRTABLE_OPTIONS, "");
    if (mapped) fields[airtableKey] = mapped;
  }

  if (fields[MAP_CP_AIRTABLE.operatingModel]) {
    fields[MAP_CP_AIRTABLE.operatingModel] = toAirtableOperatingModel(
      fields[MAP_CP_AIRTABLE.operatingModel]
    );
  }
  if (fields[MAP_CP_AIRTABLE.thirdPartyManagementAvailability]) {
    fields[MAP_CP_AIRTABLE.thirdPartyManagementAvailability] = toAirtableThirdPartyManagement(
      fields[MAP_CP_AIRTABLE.thirdPartyManagementAvailability]
    );
  }
  if (Array.isArray(fields[MAP_CP_AIRTABLE.workspaceAccess])) {
    fields[MAP_CP_AIRTABLE.workspaceAccess] = normalizeWorkspaceAccessForAirtableWrite(
      fields[MAP_CP_AIRTABLE.workspaceAccess]
    );
  }

  finalizeCompanyProfileFieldsForAirtableWrite(fields, { loud: false });

  return { warnings, derived: { companyTypeForm, tags, workspaceAccess } };
}

/** Read Airtable fields → form prefill extension. */
export function airtableFieldsToOwnerOperatorPrefill(f) {
  const fields = f || {};
  const prefill = {
    companyTypeTags: parseJsonArray(fields[MAP_CP_AIRTABLE.companyTypeTags]),
    workspaceAccess: parseJsonArray(fields[MAP_CP_AIRTABLE.workspaceAccess]),
    operatingModel: toStr(fields[MAP_CP_AIRTABLE.operatingModel]),
    thirdPartyManagementAvailability: toStr(
      fields[MAP_CP_AIRTABLE.thirdPartyManagementAvailability]
    ),
    coreProfileStatus: toStr(fields[MAP_CP_AIRTABLE.coreProfileStatus]),
    ownerProfileStatus: toStr(fields[MAP_CP_AIRTABLE.ownerProfileStatus]),
    operatorProfileStatus: toStr(fields[MAP_CP_AIRTABLE.operatorProfileStatus]),
    developerProfileStatus: toStr(fields[MAP_CP_AIRTABLE.developerProfileStatus]),
    potentialConflictFlags: parseJsonArray(fields[MAP_CP_AIRTABLE.potentialConflictFlags]),
    competitiveSensitivityNotes: toStr(fields[MAP_CP_AIRTABLE.competitiveSensitivityNotes]),
    companyCapabilities: [],
  };

  const tagToId = Object.fromEntries(CAPABILITY_OPTIONS.map((o) => [o.tag, o.id]));
  prefill.companyCapabilities = prefill.companyTypeTags
    .map((t) => tagToId[t])
    .filter(Boolean);

  return prefill;
}

export function companyTypeDisplayLabel(formKeyOrAirtable) {
  const key = toStr(formKeyOrAirtable);
  if (!key) return "";
  if (COMPANY_TYPE_FORM_TO_AIRTABLE[key]) return COMPANY_TYPE_FORM_TO_AIRTABLE[key];
  if (COMPANY_TYPE_AIRTABLE_TO_FORM_CANONICAL[key]) return key;
  return key;
}
