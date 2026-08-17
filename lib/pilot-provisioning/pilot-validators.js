/**
 * Pilot provisioning validators — read-only checks; no Airtable writes.
 */
import { normalizeList } from "../company-workspace-access.js";
import { cellToString, extractLinkedRecordIds } from "../airtable-utils.js";
import {
  USERS_WORKSPACE_ACCESS_FIELD,
  COMPANY_WORKSPACE_ACCESS_FIELD,
  workspaceAccessSourceLabel,
  DEALS_COMPANY_LINK_FIELD,
  DEALS_USER_LINK_FIELD,
  getUsersStatusFieldCandidates,
  INACTIVE_ACCOUNT_STATUS_VALUES,
  PENDING_ACCOUNT_STATUS_VALUES,
  ACTIVE_ACCOUNT_STATUS_VALUES,
  readMemberstackPrimaryFromFields,
  readMemberstackMirrorFromFields,
} from "./pilot-field-registry.js";

export function isTestMemberstackId(id) {
  return String(id || "").trim().startsWith("mem_sb_");
}

export function isLiveMemberstackId(id) {
  const s = String(id || "").trim();
  return s.startsWith("mem_") && !s.startsWith("mem_sb_");
}

export function readMemberstackIdsFromUserFields(userFields) {
  return {
    primary: readMemberstackPrimaryFromFields(userFields),
    mirror: readMemberstackMirrorFromFields(userFields),
  };
}

export function validateMemberstackIdPair({ primary, mirror }, { allowTestId = false } = {}) {
  const problems = [];
  const warnings = [];

  if (!primary) problems.push("missing_memberstack_member_id");
  if (!mirror) warnings.push("missing_memberstack_member_id_mirror_slug");

  if (primary && mirror && primary !== mirror) {
    problems.push("memberstack_id_slug_mismatch");
  }

  if (primary && isTestMemberstackId(primary) && !allowTestId) {
    problems.push("test_memberstack_id_on_production_users_row");
  }
  if (mirror && isTestMemberstackId(mirror) && !allowTestId) {
    problems.push("test_memberstack_id_mirror_on_production_users_row");
  }

  if (primary && !isLiveMemberstackId(primary) && !isTestMemberstackId(primary)) {
    warnings.push("memberstack_id_unrecognized_format");
  }

  return { problems, warnings, ok: problems.length === 0 };
}

export function readUsersWorkspaceAccess(userFields) {
  return normalizeList(userFields?.[USERS_WORKSPACE_ACCESS_FIELD]);
}

export function readCompanyWorkspaceAccess(companyFields) {
  return normalizeList(companyFields?.[COMPANY_WORKSPACE_ACCESS_FIELD]);
}

/** @returns {{ fieldName: string|null, value: string|null, raw: unknown }} */
export function detectUsersAccountStatus(userFields) {
  const f = userFields || {};
  for (const name of getUsersStatusFieldCandidates()) {
    const val = cellToString(f[name]);
    if (val) return { fieldName: name, value: val, raw: f[name] };
  }
  return { fieldName: null, value: null, raw: null };
}

export function validateWorkspaceAccessSource(userFields, companyFields) {
  const problems = [];
  const warnings = [];

  const usersWs = readUsersWorkspaceAccess(userFields);
  const companyWs = readCompanyWorkspaceAccess(companyFields);
  const workspaceAccessSource = workspaceAccessSourceLabel();

  // Current base has no Workspace Access field on Users — only Company Profile is checked.
  if (!companyWs.length) {
    problems.push("company_profile_missing_workspace_access");
  } else if (!companyWs.includes("Owner") && !companyWs.includes("Admin")) {
    problems.push("company_profile_missing_owner_workspace_for_pilot");
  }

  return { problems, warnings, usersWs, companyWs, workspaceAccessSource };
}

export function validateAccountStatus(statusInfo, { allowPending = false } = {}) {
  const problems = [];
  const warnings = [];

  if (!statusInfo?.fieldName) {
    warnings.push("account_status_field_not_found");
    return { problems, warnings };
  }

  const val = String(statusInfo.value || "").trim();
  if (!val) {
    problems.push("account_status_empty");
    return { problems, warnings };
  }

  const lower = val.toLowerCase();
  if (INACTIVE_ACCOUNT_STATUS_VALUES.includes(lower)) {
    problems.push(`account_status_inactive:${val}`);
  } else if (PENDING_ACCOUNT_STATUS_VALUES.includes(lower)) {
    if (!allowPending) problems.push(`account_status_pending:${val}`);
    else warnings.push(`account_status_pending_allowed:${val}`);
  } else if (!ACTIVE_ACCOUNT_STATUS_VALUES.includes(lower)) {
    warnings.push(`account_status_unrecognized:${val}`);
  }

  return { problems, warnings };
}

/**
 * @param {object} dealFields
 * @param {string} userRecordId
 * @param {string[]} companyIds
 */
export function classifyDealAccessPath(dealFields, userRecordId, companyIds) {
  const companySet = new Set(companyIds || []);
  const dealCompanyIds = extractLinkedRecordIds(dealFields?.[DEALS_COMPANY_LINK_FIELD]);
  const dealUserIds = extractLinkedRecordIds(dealFields?.[DEALS_USER_LINK_FIELD]);

  const viaCompany =
    dealCompanyIds.length > 0 &&
    companySet.size > 0 &&
    dealCompanyIds.some((id) => companySet.has(id));
  const viaUser = dealUserIds.includes(userRecordId);

  return { viaCompany, viaUser, dealCompanyIds, dealUserIds };
}

/** Probe whether Deals table has Company Profile field (meta API; empty cells omitted from record API). */
export async function detectDealsCompanyProfileField(base, dealsTableId, dealsCompanyField, opts = {}) {
  const fieldName = dealsCompanyField || DEALS_COMPANY_LINK_FIELD;
  const apiKey = opts.apiKey || process.env.AIRTABLE_API_KEY;
  const baseId = opts.baseId || process.env.AIRTABLE_BASE_ID;

  if (apiKey && baseId) {
    try {
      const metaRes = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
        headers: { Authorization: `Bearer ${apiKey}` },
      });
      const meta = await metaRes.json();
      if (!meta.error) {
        const table = (meta.tables || []).find(
          (t) => t.id === dealsTableId || t.name === dealsTableId
        );
        if (table) {
          const field = (table.fields || []).find((f) => f.name === fieldName);
          if (field) {
            return {
              fieldName,
              present: true,
              reason: "field_in_schema",
              fieldId: field.id,
              tableName: table.name,
            };
          }
          return { fieldName, present: false, reason: "field_not_in_schema", tableName: table.name };
        }
      }
    } catch {
      /* fall through to record probe */
    }
  }

  try {
    const rows = await base(dealsTableId).select({ maxRecords: 1, pageSize: 1 }).firstPage();
    if (!rows.length) {
      return { fieldName, present: null, reason: "no_deal_rows_to_probe" };
    }
    const hasKey = Object.prototype.hasOwnProperty.call(rows[0].fields || {}, fieldName);
    return {
      fieldName,
      present: hasKey,
      reason: hasKey ? "field_on_record" : "field_missing_on_record_empty_cells_omitted",
    };
  } catch (err) {
    return {
      fieldName,
      present: false,
      reason: err?.message || String(err),
    };
  }
}
