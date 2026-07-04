/**
 * Owner-side Operator Deal Request create helpers (Phase 3).
 */

import Airtable from "airtable";
import { MAP_ODR_AIRTABLE } from "../../api/operator-deal-requests-fields.js";
import { mapOdrToResponse } from "../../api/operator-deal-requests.js";
import {
  MAP_OPERATOR_SCOPE,
  normalizeOperatingCompanyName,
  operatingCompanyNamesMatch,
} from "./resolve-operator-scope.js";
import { cellToString } from "../airtable-utils.js";
import { NEW_BASE_MASTER_TABLE } from "../../api/lib/operator-setup-new-base-read.js";

export const ODR_DEFAULT_CREATE_STATUS = "Sent / Awaiting Response";

export const ODR_TERMINAL_STATUSES = new Set([
  "Declined",
  "Responded - Declined",
  "Archived",
]);

export const ODR_ACTIVE_STATUSES = new Set([
  "New",
  "Sent / Awaiting Response",
  "Viewed",
  "Operator Viewed",
  "More Info Requested",
  "Accepted",
  "Responded - Accepted",
  "Pre-LOI",
  "Pre-LOI / Term Comparison",
  "Finalist",
  "Deal Room Active",
  "Feasibility",
  "Feasibility In Progress",
  "LOI Signed",
  "LOI Signed / Platform Exit",
]);

const ACTIVE_MASTER_STATUS_VALUES = (
  process.env.AIRTABLE_OPERATOR_SETUP_ACTIVE_STATUS_VALUES || "Active"
)
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

const ODR_TABLE = MAP_ODR_AIRTABLE.table;
const ACTIVITY_LOG_TABLE = process.env.AIRTABLE_TABLE_DEAL_ACTIVITY_LOG || "Deal Activity Log";

const CREATE_ACTIVITY_ACTIONS = ["Request Sent", "Operator contacted", "Operator Contacted"];

export function getOdrAirtableBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    const err = new Error("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not configured");
    err.code = "server_misconfigured";
    throw err;
  }
  return new Airtable({ apiKey }).base(baseId);
}

export function isOdrStatusActive(status) {
  const s = String(status || "").trim();
  if (!s) return true;
  if (ODR_TERMINAL_STATUSES.has(s)) return false;
  if (ODR_ACTIVE_STATUSES.has(s)) return true;
  return !ODR_TERMINAL_STATUSES.has(s);
}

export function ownerOutreachStatusLabel(status) {
  const s = String(status || "").trim();
  if (!s) return "Not contacted";
  if (s === "New" || s === "Sent / Awaiting Response") return "Request sent";
  if (s === "Viewed" || s === "Operator Viewed" || s === "Brand Viewed") return "Viewed";
  if (s === "More Info Requested") return "More info requested";
  if (s === "Accepted" || s === "Responded - Accepted") return "Accepted";
  if (s === "Declined" || s === "Responded - Declined") return "Declined";
  if (s === "Archived") return "Archived";
  return s;
}

function isActiveMasterSubmissionStatus(raw) {
  if (!ACTIVE_MASTER_STATUS_VALUES.length) return true;
  const s = normalizeOperatingCompanyName(raw).toLowerCase();
  if (!s) return false;
  return ACTIVE_MASTER_STATUS_VALUES.some((v) => s === v || s.includes(v));
}

export async function resolveMasterForOdrCreate(base, operatorSetupId, clientCompanyName) {
  const id = String(operatorSetupId || "").trim();
  if (!id.startsWith("rec")) {
    const err = new Error("operatorSetupId must be a valid Operator Setup - Master record id");
    err.code = "validation_error";
    throw err;
  }

  const table = MAP_OPERATOR_SCOPE.operatorMasterTable || NEW_BASE_MASTER_TABLE;
  const nameField = MAP_OPERATOR_SCOPE.operatorCompanyNameField || "company_name";
  const statusField = MAP_OPERATOR_SCOPE.operatorSubmissionStatusField || "submission_status";

  let rec;
  try {
    rec = await base(table).find(id);
  } catch (e) {
    const err = new Error("Operator Setup - Master record not found");
    err.code = "operator_setup_not_found";
    throw err;
  }

  const f = rec?.fields || {};
  const companyName = normalizeOperatingCompanyName(cellToString(f[nameField]));
  if (!companyName) {
    const err = new Error("Operator Setup - Master is missing company_name");
    err.code = "operator_company_name_missing";
    throw err;
  }

  const statusRaw = cellToString(f[statusField]);
  if (ACTIVE_MASTER_STATUS_VALUES.length && statusRaw && !isActiveMasterSubmissionStatus(statusRaw)) {
    const err = new Error("Operator Setup - Master is not in an active submission status");
    err.code = "operator_setup_inactive";
    throw err;
  }

  const clientName = normalizeOperatingCompanyName(clientCompanyName);
  if (clientName && !operatingCompanyNamesMatch(clientName, companyName)) {
    const err = new Error("operatingCompanyName does not match Operator Setup - Master company_name");
    err.code = "company_name_mismatch";
    throw err;
  }

  return { operatorSetupId: rec.id, operatingCompanyName: companyName, masterFields: f };
}

export function dealLinkToRecordId(raw) {
  if (raw == null) return null;
  if (typeof raw === "string") {
    const s = raw.trim();
    return s.startsWith("rec") ? s : null;
  }
  if (typeof raw === "object" && raw !== null && raw.id != null) {
    const s = String(raw.id).trim();
    return s.startsWith("rec") ? s : null;
  }
  return null;
}

export function firstLinkedDealIdFromOdrFields(fields) {
  const deal = fields && fields[MAP_ODR_AIRTABLE.deal];
  if (deal == null) return null;
  const first = Array.isArray(deal) ? deal[0] : deal;
  return dealLinkToRecordId(first);
}

export async function findOdrRowsForDeal(base, dealId) {
  const targetId = String(dealId || "").trim();
  if (!targetId.startsWith("rec")) return [];
  // TODO(odr-pagination): When Operator Deal Requests exceeds ~100 rows, paginate
  // this select (or add a deal-scoped Airtable view/filter) instead of a single
  // pageSize:100 scan — duplicate detection and by-deals read share this pattern.
  try {
    const records = await base(ODR_TABLE)
      .select({
        sort: [{ field: MAP_ODR_AIRTABLE.requestSentAt, direction: "desc" }],
        pageSize: 100,
      })
      .all();
    return records.filter((r) => firstLinkedDealIdFromOdrFields(r.fields) === targetId);
  } catch (err) {
    if (/Could not find table|NOT_FOUND|does not exist/i.test(String(err.message || ""))) {
      const e = new Error("Operator Deal Requests table is not configured in Airtable.");
      e.code = "odr_table_missing";
      throw e;
    }
    throw err;
  }
}

export function pickLatestOdrForDealAndCompany(records, dealId, operatingCompanyName) {
  const target = normalizeOperatingCompanyName(operatingCompanyName);
  let best = null;
  for (const rec of records || []) {
    const rowDealId = firstLinkedDealIdFromOdrFields(rec.fields);
    if (rowDealId !== dealId) continue;
    const rowName = normalizeOperatingCompanyName(rec.fields?.[MAP_ODR_AIRTABLE.operatingCompanyName]);
    if (!operatingCompanyNamesMatch(rowName, target)) continue;
    if (!best) {
      best = rec;
      continue;
    }
    const bestSent = best.fields?.[MAP_ODR_AIRTABLE.requestSentAt] || "";
    const curSent = rec.fields?.[MAP_ODR_AIRTABLE.requestSentAt] || "";
    if (String(curSent) > String(bestSent)) best = rec;
  }
  return best;
}

export function mapOdrToContactedRow(record) {
  const base = mapOdrToResponse(record);
  const status = base.status || "New";
  return {
    ...base,
    outreachStatusLabel: ownerOutreachStatusLabel(status),
    isActiveRequest: isOdrStatusActive(status),
  };
}

async function createOperatorActivityRow(base, logFields) {
  try {
    await base(ACTIVITY_LOG_TABLE).create([{ fields: logFields }]);
  } catch (e) {
    const msg = String(e?.message || "");
    if (/Unknown field|does not exist/i.test(msg) && logFields.Stakeholder != null) {
      const fallback = { ...logFields };
      delete fallback.Stakeholder;
      await base(ACTIVITY_LOG_TABLE).create([{ fields: fallback }]);
      return;
    }
    throw e;
  }
}

function isActivitySelectError(msg) {
  return /select option|UNKNOWN_MULTIPLE_CHOICE|invalid.*choice|INVALID_MULTIPLE_CHOICE/i.test(String(msg || ""));
}

export async function logOdrOwnerOutreachActivity(base, dealId, operatingCompanyName, ownerNotes) {
  if (!dealId || !operatingCompanyName) return;
  const detailsBase = `Owner initiated operator outreach for ${operatingCompanyName}.`;
  const notes = String(ownerNotes || "").trim();
  const details = notes ? `${detailsBase} ${notes.slice(0, 500)}` : detailsBase;
  const now = new Date().toISOString();
  let lastErr = null;

  for (const action of CREATE_ACTIVITY_ACTIONS) {
    const logFields = {
      Deal: [dealId],
      [MAP_ODR_AIRTABLE.operatingCompanyName]: String(operatingCompanyName).trim(),
      Stakeholder: "Operator",
      Action: action,
      Details: details,
      "Created At": now,
    };
    try {
      await createOperatorActivityRow(base, logFields);
      return;
    } catch (e) {
      lastErr = e;
      if (isActivitySelectError(String(e?.message || ""))) continue;
      console.warn("[odr-owner-create] activity log failed:", e.message);
      return;
    }
  }
  console.warn("[odr-owner-create] activity log exhausted fallbacks:", lastErr?.message);
}

export async function createOdrRow(base, payload) {
  const now = new Date().toISOString();
  const fields = {
    [MAP_ODR_AIRTABLE.deal]: [payload.dealId],
    [MAP_ODR_AIRTABLE.operatingCompanyName]: payload.operatingCompanyName,
    [MAP_ODR_AIRTABLE.status]: payload.status || ODR_DEFAULT_CREATE_STATUS,
    [MAP_ODR_AIRTABLE.requestSentAt]: now,
    [MAP_ODR_AIRTABLE.createdAt]: now,
    [MAP_ODR_AIRTABLE.lastUpdated]: now,
  };

  if (payload.operatorSetupId) {
    fields[MAP_ODR_AIRTABLE.operatorSetup] = [payload.operatorSetupId];
  }
  if (payload.alignmentScore != null && payload.alignmentScore !== "") {
    const n = Number(payload.alignmentScore);
    if (!Number.isNaN(n)) fields[MAP_ODR_AIRTABLE.alignmentScore] = n;
  }
  if (payload.alignmentBand) fields[MAP_ODR_AIRTABLE.alignmentBand] = String(payload.alignmentBand).trim();
  if (payload.dataConfidence) fields[MAP_ODR_AIRTABLE.dataConfidence] = String(payload.dataConfidence).trim();
  if (payload.ownerNotes != null) {
    fields[MAP_ODR_AIRTABLE.ownerNotes] = String(payload.ownerNotes || "").trim() || null;
  }

  const [record] = await base(ODR_TABLE).create([{ fields }]);
  return record;
}
