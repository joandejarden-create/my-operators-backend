/**
 * Operator company scope from Users → Operator Setup - Master → company_name.
 * Mirrors intended brand allow-list model (not unscoped BDR).
 */

import Airtable from "airtable";
import {
  INTAKE_USERS_TABLE,
} from "../../api/schemas/intake-deal-fields.js";
import {
  escapeAirtableFormulaValue,
  cellToString,
  extractLinkedRecordIds,
} from "../airtable-utils.js";
import { NEW_BASE_MASTER_TABLE } from "../../api/lib/operator-setup-new-base-read.js";
import {
  DEMO_BYPASS_WARNING,
  getOperatorDealsDemoCompanyName,
  isOperatorDealsDemoBypassEnabled,
} from "./operator-deals-demo-bypass.js";

export const MAP_OPERATOR_SCOPE = {
  usersOperatorSetupLink:
    process.env.AIRTABLE_ME_USERS_OPERATOR_SETUP_LINK || "Operator Setup - Master",
  operatorMasterTable:
    process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || NEW_BASE_MASTER_TABLE,
  operatorCompanyNameField:
    process.env.AIRTABLE_OPERATOR_COMPANY_NAME_FIELD || "company_name",
  operatorSubmissionStatusField:
    process.env.AIRTABLE_OPERATOR_SETUP_SUBMISSION_STATUS_FIELD || "submission_status",
};

const ACTIVE_STATUS_VALUES = (
  process.env.AIRTABLE_OPERATOR_SETUP_ACTIVE_STATUS_VALUES || "Active"
)
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

export function normalizeOperatingCompanyName(value) {
  return String(value ?? "")
    .replace(/\s+/g, " ")
    .trim();
}

/** Case-insensitive equality for allow-list checks. */
export function operatingCompanyNamesMatch(a, b) {
  const x = normalizeOperatingCompanyName(a).toLowerCase();
  const y = normalizeOperatingCompanyName(b).toLowerCase();
  return !!x && !!y && x === y;
}

export function isOperatingCompanyNameAllowed(scope, name) {
  if (!scope) return false;
  if (scope.isAdmin) return true;
  const target = normalizeOperatingCompanyName(name);
  if (!target) return false;
  return (scope.allowedOperatingCompanyNames || []).some((n) =>
    operatingCompanyNamesMatch(n, target),
  );
}

export function assertOperatingCompanyAllowed(scope, name) {
  if (isOperatingCompanyNameAllowed(scope, name)) return null;
  return {
    status: 403,
    body: {
      success: false,
      error: "forbidden_operator_scope",
      message: "This operating opportunity is outside your allowed company scope.",
    },
  };
}

/**
 * @param {object} scope
 * @param {import('airtable').Record | { fields: object }} record
 * @param {object} mapFields MAP_ODR_AIRTABLE or field names
 */
export function assertOperatorRequestRecordAccess(scope, record, mapFields) {
  const fields = record?.fields || {};
  const companyField = mapFields?.operatingCompanyName || "Operating Company Name";
  const companyName = fields[companyField] || "";
  return assertOperatingCompanyAllowed(scope, companyName);
}

function getAirtableBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    const err = new Error("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not configured");
    err.code = "server_misconfigured";
    throw err;
  }
  return new Airtable({ apiKey }).base(baseId);
}

function parseActiveStatusValuesConfigured() {
  return ACTIVE_STATUS_VALUES.length > 0;
}

function isActiveSubmissionStatus(raw) {
  if (!parseActiveStatusValuesConfigured()) return true;
  const s = normalizeOperatingCompanyName(raw).toLowerCase();
  if (!s) return false;
  return ACTIVE_STATUS_VALUES.some((v) => s === v || s.includes(v));
}

/**
 * Fetch Master rows and resolve company names for linked ids.
 * @param {import('airtable').Base} base
 * @param {string[]} masterIds
 */
export async function fetchOperatingCompaniesForMasterIds(base, masterIds) {
  const ids = [...new Set((masterIds || []).filter((id) => typeof id === "string" && id.startsWith("rec")))];
  const warnings = [];
  const companies = [];
  const companyRecords = [];

  if (!ids.length) {
    return {
      allowedOperatingCompanyNames: [],
      allowedOperatorSetupIds: [],
      warnings,
      skippedInactiveIds: [],
    };
  }

  const table = MAP_OPERATOR_SCOPE.operatorMasterTable;
  const nameField = MAP_OPERATOR_SCOPE.operatorCompanyNameField;
  const statusField = MAP_OPERATOR_SCOPE.operatorSubmissionStatusField;
  const batchSize = 8;
  const skippedInactiveIds = [];

  for (let i = 0; i < ids.length; i += batchSize) {
    const chunk = ids.slice(i, i + batchSize);
    const orParts = chunk.map((id) => `RECORD_ID() = '${escapeAirtableFormulaValue(id)}'`);
    const formula = orParts.length === 1 ? orParts[0] : `OR(${orParts.join(",")})`;
    let rows;
    try {
      rows = await base(table).select({ filterByFormula: formula, maxRecords: chunk.length }).firstPage();
    } catch (err) {
      warnings.push("operator_master_fetch_failed");
      if (process.env.NODE_ENV !== "production") {
        console.warn("[resolve-operator-scope] master fetch failed:", err.message);
      }
      continue;
    }

    for (const row of rows) {
      const f = row.fields || {};
      const statusRaw = cellToString(f[statusField]);
      const name = normalizeOperatingCompanyName(cellToString(f[nameField]));

      if (!parseActiveStatusValuesConfigured()) {
        warnings.push("operator_active_status_filter_not_configured");
      }

      if (parseActiveStatusValuesConfigured() && statusField && f[statusField] != null && statusRaw) {
        if (!isActiveSubmissionStatus(statusRaw)) {
          skippedInactiveIds.push(row.id);
          continue;
        }
      } else if (parseActiveStatusValuesConfigured() && statusField && (f[statusField] == null || !statusRaw)) {
        warnings.push("operator_submission_status_missing");
      }

      if (!name) {
        warnings.push("operator_master_linked_but_company_name_missing");
        continue;
      }

      const exists = companies.some((c) => operatingCompanyNamesMatch(c, name));
      if (!exists) companies.push(name);
      companyRecords.push({ id: row.id, companyName: name, submissionStatus: statusRaw || null });
    }
  }

  companies.sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));

  return {
    allowedOperatingCompanyNames: companies,
    allowedOperatorSetupIds: companyRecords.map((r) => r.id),
    warnings: [...new Set(warnings)],
    skippedInactiveIds,
    companyRecords,
  };
}

/**
 * Load operator scope for the signed-in Dealality user.
 * @param {object} dealalityUser req.dealalityUser
 */
export async function resolveOperatorScope(dealalityUser) {
  const user = dealalityUser && typeof dealalityUser === "object" ? dealalityUser : {};
  const isAdmin = !!user.isAdmin;
  const isOperator = !!user.isOperator;

  const baseScope = {
    allowedOperatingCompanyNames: [],
    allowedOperatorSetupIds: [],
    primaryOperatingCompanyName: null,
    isAdmin,
    isOperator,
    mappingStatus: "ok",
    warnings: [],
    skippedInactiveIds: [],
  };

  if (
    isOperatorDealsDemoBypassEnabled() &&
    !isAdmin &&
    !isOperator &&
    user.userRecordId
  ) {
    const demoCompany = getOperatorDealsDemoCompanyName();
    return {
      ...baseScope,
      allowedOperatingCompanyNames: demoCompany ? [demoCompany] : [],
      primaryOperatingCompanyName: demoCompany || null,
      mappingStatus: demoCompany ? "ok" : "no_operator_link",
      warnings: [DEMO_BYPASS_WARNING],
      demoBypassActive: true,
    };
  }

  if (isAdmin) {
    return {
      ...baseScope,
      mappingStatus: "admin_unrestricted",
    };
  }

  if (!user.userRecordId) {
    return {
      ...baseScope,
      mappingStatus: "no_user_record",
      warnings: ["operator_user_record_missing"],
    };
  }

  const usersTable = process.env.AIRTABLE_ME_USERS_TABLE || INTAKE_USERS_TABLE;
  const linkField = MAP_OPERATOR_SCOPE.usersOperatorSetupLink;

  let userFields = {};
  try {
    const base = getAirtableBase();
    const rec = await base(usersTable).find(user.userRecordId);
    userFields = rec?.fields || {};
    const masterIds = extractLinkedRecordIds(userFields[linkField]);

    if (!masterIds.length) {
      return {
        ...baseScope,
        mappingStatus: "no_operator_link",
        warnings: ["operator_link_empty"],
      };
    }

    const resolved = await fetchOperatingCompaniesForMasterIds(base, masterIds);
    const names = resolved.allowedOperatingCompanyNames || [];

    if (!names.length) {
      return {
        ...baseScope,
        allowedOperatorSetupIds: resolved.allowedOperatorSetupIds || [],
        mappingStatus: masterIds.length ? "names_unresolved" : "no_operator_link",
        warnings: resolved.warnings?.length ? resolved.warnings : ["operator_names_unresolved"],
        skippedInactiveIds: resolved.skippedInactiveIds || [],
      };
    }

    return {
      ...baseScope,
      allowedOperatingCompanyNames: names,
      allowedOperatorSetupIds: resolved.allowedOperatorSetupIds || [],
      primaryOperatingCompanyName: names.length === 1 ? names[0] : names[0],
      mappingStatus: "ok",
      warnings: resolved.warnings || [],
      skippedInactiveIds: resolved.skippedInactiveIds || [],
    };
  } catch (err) {
    if (err.code === "server_misconfigured") throw err;
    console.error("[resolve-operator-scope] lookup error:", err.message);
    return {
      ...baseScope,
      mappingStatus: "lookup_error",
      warnings: ["operator_scope_lookup_failed"],
    };
  }
}

/**
 * Companies to use for list/activity filter (non-admin).
 * @param {object} scope
 * @param {string} [requestedOperator] query ?operator=
 */
export function resolveEffectiveCompanyFilter(scope, requestedOperator) {
  if (!scope) return [];
  if (scope.isAdmin) {
    const q = normalizeOperatingCompanyName(requestedOperator);
    return q ? [q] : [];
  }

  const allowed = scope.allowedOperatingCompanyNames || [];
  if (!allowed.length) return [];

  const q = normalizeOperatingCompanyName(requestedOperator);
  if (!q) return allowed;

  const match = allowed.find((n) => operatingCompanyNamesMatch(n, q));
  return match ? [match] : [];
}

/**
 * Build Airtable formula OR for Operating Company Name ∈ names.
 * @param {string[]} names
 * @param {string} fieldName
 */
export function buildOperatingCompanyNameOrFormula(names, fieldName = "Operating Company Name") {
  const list = (names || []).map(normalizeOperatingCompanyName).filter(Boolean);
  if (!list.length) return null;
  if (list.length === 1) {
    return `{${fieldName}} = '${escapeAirtableFormulaValue(list[0])}'`;
  }
  const parts = list.map((n) => `{${fieldName}} = '${escapeAirtableFormulaValue(n)}'`);
  return `OR(${parts.join(", ")})`;
}
