/**
 * Infer Owner-Operator extension field values for Company Profile backfill.
 * Only proposes values for empty fields; uses exact Airtable select labels.
 */

import { COMPANY_ROLE_AIRTABLE_FIELD, normalizeCompanyRoleToForm } from "./company-role-normalize.js";
import {
  COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE,
  isOwnerOperatorCompanyTypeString,
  normalizeCompanyTypeToFilterKey,
} from "./company-type-normalize.js";
import { normalizeWorkspaceAccess, WORKSPACE_DEMO } from "./company-workspace-access.js";
import {
  applyProfileStatusDefaults,
  deriveCompanyClassificationFromTags,
  MAP_CP_AIRTABLE,
  suggestOperatingModel,
  suggestThirdPartyManagementAvailability,
  toAirtableCompanyType,
  toAirtableOperatingModel,
  toAirtableThirdPartyManagement,
} from "./company-profile-owner-operator-fields.js";

const EXTENSION_FIELDS = [
  MAP_CP_AIRTABLE.companyType,
  MAP_CP_AIRTABLE.companyTypeTags,
  MAP_CP_AIRTABLE.workspaceAccess,
  MAP_CP_AIRTABLE.operatingModel,
  MAP_CP_AIRTABLE.thirdPartyManagementAvailability,
  MAP_CP_AIRTABLE.coreProfileStatus,
  MAP_CP_AIRTABLE.ownerProfileStatus,
  MAP_CP_AIRTABLE.operatorProfileStatus,
  MAP_CP_AIRTABLE.developerProfileStatus,
  MAP_CP_AIRTABLE.potentialConflictFlags,
  MAP_CP_AIRTABLE.competitiveSensitivityNotes,
];

function toStr(v) {
  return v == null ? "" : String(v).trim();
}

function listEmpty(raw) {
  if (raw == null) return true;
  if (Array.isArray(raw)) return raw.length === 0;
  return !toStr(raw);
}

function companyTypeRawFromFields(fields) {
  return toStr(fields[MAP_CP_AIRTABLE.companyType]);
}

function ecosystemFormKey(fields) {
  return normalizeCompanyRoleToForm(toStr(fields[COMPANY_ROLE_AIRTABLE_FIELD]));
}

function textBlob(fields) {
  return [
    fields["Company Overview"],
    fields["Company Description"],
    fields["Differentiators"],
    fields["Primary Services"],
  ]
    .flat()
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function ownerOperatesPortfolioSignal(blob) {
  return (
    /own and operate|own-and-operate|vertically integrated|self[- ]?operat/i.test(blob) ||
    /operates (?:its |our |their |a )?portfolio/i.test(blob) ||
    /owner[- ]?operator/i.test(blob)
  );
}

function thirdPartyOperateSignal(blob) {
  return /third[- ]?party|management contract|manages hotels for|hotel management company/i.test(
    blob
  );
}

/**
 * Infer capability tags when Company Type Tags is empty.
 * @returns {string[]}
 */
export function inferCompanyTypeTagsFromLegacy(fields) {
  const existing = fields[MAP_CP_AIRTABLE.companyTypeTags];
  if (!listEmpty(existing)) return [];

  const tags = new Set();
  const typeRaw = companyTypeRawFromFields(fields);
  const filterKey = normalizeCompanyTypeToFilterKey(typeRaw);
  const eco = ecosystemFormKey(fields);
  const blob = textBlob(fields);

  if (
    filterKey === "OWNER_OPERATOR" ||
    isOwnerOperatorCompanyTypeString(typeRaw) ||
    eco === "OwnerOperator"
  ) {
    tags.add("Owns Hotels");
    tags.add("Operates Own Portfolio");
    if (/third[- ]?party|management contract|manages hotels for/i.test(blob)) {
      tags.add("Operates Third-Party Hotels");
    }
  } else if (filterKey === "HOTEL OWNERS" || eco === "Owner") {
    tags.add("Owns Hotels");
    if (/develop|development|ground[- ]?up/i.test(blob)) tags.add("Develops Hotels");
    if (ownerOperatesPortfolioSignal(blob)) tags.add("Operates Own Portfolio");
    if (thirdPartyOperateSignal(blob)) tags.add("Operates Third-Party Hotels");
  } else if (filterKey === "HOTEL MGMT. COMPANY" || eco === "Operator") {
    tags.add("Operates Third-Party Hotels");
    if (/own portfolio|affiliated|own and operate/i.test(blob)) {
      tags.add("Operates Own Portfolio");
      tags.add("Owns Hotels");
    }
  } else if (filterKey === "HOTEL BRANDS (FRANCHISE)" || eco === "Brand") {
    tags.add("Brand / Franchisor");
  } else if (eco === "Both") {
    tags.add("Brand / Franchisor");
    tags.add("Operates Third-Party Hotels");
  } else if (eco === "Advisor" || filterKey === "HOSPITALITY CONSULTANTS") {
    tags.add("Consultant / Advisor");
  } else if (/invest|capital|lender/i.test(blob)) {
    tags.add("Capital Provider");
  }

  if (
    (filterKey === "HOTEL BRANDS (FRANCHISE)" || eco === "Brand" || eco === "Both") &&
    /franchisor|brand platform|licensing/i.test(blob)
  ) {
    tags.add("Brand / Franchisor");
  }
  if (/broker|consultant|advisory firm|advisory services/i.test(blob)) {
    tags.add("Consultant / Advisor");
  }

  return [...tags];
}

/**
 * Build patch object (Airtable field names) for empty extension columns only.
 * @param {Record<string, unknown>} fields — Airtable record.fields
 * @param {{ allowCompanyTypeFix?: boolean }} [options]
 * @returns {{ patch: Record<string, unknown>, reasons: string[], skipped: string[] }}
 */
export function buildCompanyProfileOwnerOperatorBackfillPatch(fields, options = {}) {
  const f = fields || {};
  const patch = {};
  const reasons = [];
  const skipped = [];
  const allowCompanyTypeFix = options.allowCompanyTypeFix !== false;

  const typeRaw = companyTypeRawFromFields(f);
  const safeType = toAirtableCompanyType(typeRaw);
  if (allowCompanyTypeFix && typeRaw && safeType && typeRaw !== safeType) {
    patch[MAP_CP_AIRTABLE.companyType] = safeType;
    reasons.push(`company_type_normalized:${typeRaw}→${safeType}`);
  }

  const tags = inferCompanyTypeTagsFromLegacy(f);
  if (listEmpty(f[MAP_CP_AIRTABLE.companyTypeTags]) && tags.length) {
    patch[MAP_CP_AIRTABLE.companyTypeTags] = tags;
    reasons.push(`tags_inferred:${tags.join(",")}`);
  }

  const effectiveTags =
    patch[MAP_CP_AIRTABLE.companyTypeTags] ||
    (Array.isArray(f[MAP_CP_AIRTABLE.companyTypeTags]) ? f[MAP_CP_AIRTABLE.companyTypeTags] : tags);

  const tagList = Array.isArray(effectiveTags) ? effectiveTags : tags;

  if (listEmpty(f[MAP_CP_AIRTABLE.workspaceAccess])) {
    let ws = [];
    if (tagList.length) {
      const derived = deriveCompanyClassificationFromTags(tagList);
      ws = derived.workspaceAccess.slice();
    } else {
      const mergedFields = { ...f, [MAP_CP_AIRTABLE.companyTypeTags]: tagList };
      if (patch[MAP_CP_AIRTABLE.companyType]) {
        mergedFields[MAP_CP_AIRTABLE.companyType] = patch[MAP_CP_AIRTABLE.companyType];
      }
      ws = normalizeWorkspaceAccess(mergedFields);
    }
    ws = ws.filter((w) => w !== WORKSPACE_DEMO);
    if (ws.length) {
      patch[MAP_CP_AIRTABLE.workspaceAccess] = ws;
      reasons.push(`workspace_access:${ws.join(",")}`);
    }
  }

  const companyTypeForDerivation =
    patch[MAP_CP_AIRTABLE.companyType] || safeType || typeRaw;

  if (listEmpty(f[MAP_CP_AIRTABLE.operatingModel])) {
    let opModel = suggestOperatingModel(tagList, "");
    if (!opModel && companyTypeForDerivation === COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE) {
      opModel = "Mixed Owner/Operator Model";
    }
    if (!opModel && normalizeCompanyTypeToFilterKey(companyTypeForDerivation) === "HOTEL MGMT. COMPANY") {
      opModel = "Third-Party Management";
    }
    if (!opModel && normalizeCompanyTypeToFilterKey(companyTypeForDerivation) === "HOTEL OWNERS") {
      if (tagList.includes("Operates Own Portfolio")) opModel = "Own-and-Operate Only";
      else if (tagList.includes("Operates Third-Party Hotels")) opModel = "Third-Party Management";
    }
    const mapped = toAirtableOperatingModel(opModel);
    if (mapped) {
      patch[MAP_CP_AIRTABLE.operatingModel] = mapped;
      reasons.push(`operating_model:${mapped}`);
    }
  }

  if (listEmpty(f[MAP_CP_AIRTABLE.thirdPartyManagementAvailability])) {
    const opForSuggest = patch[MAP_CP_AIRTABLE.operatingModel] || toStr(f[MAP_CP_AIRTABLE.operatingModel]);
    let suggested = suggestThirdPartyManagementAvailability(tagList, "", opForSuggest);
    if (
      !suggested &&
      tagList.includes("Owns Hotels") &&
      !tagList.includes("Operates Third-Party Hotels")
    ) {
      suggested = "No";
    }
    const mapped = toAirtableThirdPartyManagement(suggested);
    if (mapped) {
      patch[MAP_CP_AIRTABLE.thirdPartyManagementAvailability] = mapped;
      reasons.push(`third_party:${mapped}`);
    }
  }

  const statusKeys = [
    "coreProfileStatus",
    "ownerProfileStatus",
    "operatorProfileStatus",
    "developerProfileStatus",
  ];
  const existingStatuses = {};
  for (const key of statusKeys) {
    const airtableKey = MAP_CP_AIRTABLE[key];
    existingStatuses[key] = toStr(f[airtableKey]);
  }
  const withDefaults = applyProfileStatusDefaults(companyTypeForDerivation, existingStatuses);
  for (const key of statusKeys) {
    const airtableKey = MAP_CP_AIRTABLE[key];
    if (!listEmpty(f[airtableKey])) continue;
    const next = toStr(withDefaults[key]);
    if (next) {
      patch[airtableKey] = next;
      reasons.push(`${key}:${next}`);
    }
  }

  if (listEmpty(f[MAP_CP_AIRTABLE.potentialConflictFlags])) {
    patch[MAP_CP_AIRTABLE.potentialConflictFlags] = ["To Be Reviewed"];
    reasons.push("conflict_flags:To Be Reviewed");
  }

  for (const fieldName of EXTENSION_FIELDS) {
    if (!listEmpty(f[fieldName]) && !patch[fieldName]) {
      skipped.push(fieldName);
    }
  }

  return { patch, reasons, skipped };
}

export { EXTENSION_FIELDS };
