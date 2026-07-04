/**
 * Dealality ChatGPT Airtable service — read/write/summarize on allowed GTM tables only.
 */
import {
  ALLOWED_TABLE_IDS,
  DEFAULT_GTM_BASE_ID,
  INCOMPLETE_STATUS_VALUES,
  LIST_MAX_RECORDS_CAP,
  LIST_MAX_RECORDS_DEFAULT,
  SUMMARIZE_MAX_RECORDS_CAP,
  SUMMARIZE_MAX_RECORDS_DEFAULT,
  WRITE_BATCH_MAX,
  WRITE_BATCH_MIN,
} from "./dealality-airtable-chatgpt-config.js";

const DEV =
  process.env.NODE_ENV !== "production" || process.env.DEBUG_DEALALITY_AIRTABLE_CHATGPT === "true";

export function getGtmBaseId() {
  return (process.env.AIRTABLE_GTM_BASE_ID || DEFAULT_GTM_BASE_ID).trim();
}

export function getAirtableToken() {
  return (
    process.env.AIRTABLE_TOKEN ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_GTM_API_KEY ||
    ""
  ).trim();
}

export function assertConfigured() {
  const token = getAirtableToken();
  const baseId = getGtmBaseId();
  if (!token) {
    throw new ServiceError(500, "Server misconfiguration: Airtable token not set (AIRTABLE_TOKEN or AIRTABLE_PAT).");
  }
  if (!baseId) {
    throw new ServiceError(500, "Server misconfiguration: GTM base ID not set.");
  }
  return { token, baseId };
}

export class ServiceError extends Error {
  constructor(status, message, details = null) {
    super(message);
    this.name = "ServiceError";
    this.status = status;
    this.details = details;
  }
}

export function validateTableId(tableId) {
  const id = String(tableId || "").trim();
  if (!id) {
    throw new ServiceError(400, "tableId is required.");
  }
  if (!ALLOWED_TABLE_IDS.has(id)) {
    throw new ServiceError(400, `tableId "${id}" is not allowed. Use listDealalityTables to see allowed table IDs.`);
  }
  return id;
}

function clampInt(value, defaultVal, min, max) {
  if (value == null || value === "") return defaultVal;
  const n = Number(value);
  if (!Number.isFinite(n)) return defaultVal;
  return Math.min(max, Math.max(min, Math.floor(n)));
}

function sanitizeAirtableError(json, status) {
  const err = json?.error;
  if (!err) return { status, message: `Airtable request failed (${status}).` };
  return {
    status,
    message: err.message || `Airtable error (${err.type || status}).`,
    type: err.type || null,
  };
}

async function airtableFetch(path, { token, init = {}, searchParams = null } = {}) {
  const url = new URL(`https://api.airtable.com/v0${path}`);
  if (searchParams) {
    for (const [key, value] of searchParams.entries()) {
      url.searchParams.append(key, value);
    }
  }
  const res = await fetch(url.toString(), {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

function mapRecord(rec) {
  return {
    id: rec.id,
    createdTime: rec.createdTime,
    fields: rec.fields || {},
  };
}

let tablesCache = { at: 0, baseId: null, tables: null };
const TABLES_CACHE_TTL_MS = 5 * 60 * 1000;

async function fetchAllTables(token, baseId) {
  const now = Date.now();
  if (
    tablesCache.tables &&
    tablesCache.baseId === baseId &&
    now - tablesCache.at < TABLES_CACHE_TTL_MS
  ) {
    return tablesCache.tables;
  }
  const { res, json } = await airtableFetch(`/meta/bases/${baseId}/tables`, { token });
  if (!res.ok) {
    const err = sanitizeAirtableError(json, res.status);
    throw new ServiceError(res.status === 403 ? 403 : 502, err.message, err);
  }
  const all = json.tables || [];
  const allowed = all.filter((t) => ALLOWED_TABLE_IDS.has(t.id));
  tablesCache = { at: now, baseId, tables: allowed };
  return allowed;
}

export async function listDealalityTables() {
  const { token, baseId } = assertConfigured();
  const tables = await fetchAllTables(token, baseId);
  return {
    baseId,
    tables: tables.map((t) => ({
      id: t.id,
      name: t.name,
      fields: (t.fields || []).map((f) => ({
        id: f.id,
        name: f.name,
        type: f.type,
      })),
    })),
  };
}

function buildListSearchParams(input) {
  const maxRecords = clampInt(input.maxRecords, LIST_MAX_RECORDS_DEFAULT, 1, LIST_MAX_RECORDS_CAP);
  const params = new URLSearchParams();
  params.set("pageSize", String(Math.min(maxRecords, 100)));
  if (input.view) params.set("view", String(input.view).trim());
  if (input.filterByFormula) params.set("filterByFormula", String(input.filterByFormula));
  if (Array.isArray(input.fields)) {
    for (const f of input.fields) {
      if (f) params.append("fields[]", String(f));
    }
  }
  if (Array.isArray(input.sort)) {
    input.sort.slice(0, 3).forEach((s, i) => {
      if (!s?.field) return;
      params.set(`sort[${i}][field]`, String(s.field));
      params.set(`sort[${i}][direction]`, s.direction === "desc" ? "desc" : "asc");
    });
  }
  return { maxRecords, params };
}

export async function listRecordsByTableId(input = {}) {
  const { token, baseId } = assertConfigured();
  const tableId = validateTableId(input.tableId);
  const { maxRecords, params: baseParams } = buildListSearchParams(input);

  const records = [];
  let offset;
  do {
    const params = new URLSearchParams(baseParams);
    if (offset) params.set("offset", offset);
    const { res, json } = await airtableFetch(`/${baseId}/${tableId}`, {
      token,
      searchParams: params,
    });
    if (!res.ok) {
      const err = sanitizeAirtableError(json, res.status);
      throw new ServiceError(res.status >= 400 && res.status < 500 ? res.status : 502, err.message, err);
    }
    records.push(...(json.records || []).map(mapRecord));
    offset = json.offset;
  } while (offset && records.length < maxRecords);

  return { tableId, records: records.slice(0, maxRecords) };
}

export async function getRecordById(input = {}) {
  const { token, baseId } = assertConfigured();
  const tableId = validateTableId(input.tableId);
  const recordId = String(input.recordId || "").trim();
  if (!recordId) throw new ServiceError(400, "recordId is required.");

  const { res, json } = await airtableFetch(`/${baseId}/${tableId}/${recordId}`, { token });
  if (!res.ok) {
    const err = sanitizeAirtableError(json, res.status);
    throw new ServiceError(res.status === 404 ? 404 : res.status >= 400 && res.status < 500 ? res.status : 502, err.message, err);
  }
  return mapRecord(json);
}

function validateWriteRecords(records, requireIds = false) {
  if (!Array.isArray(records) || records.length < WRITE_BATCH_MIN || records.length > WRITE_BATCH_MAX) {
    throw new ServiceError(
      400,
      `records must be an array with ${WRITE_BATCH_MIN} to ${WRITE_BATCH_MAX} items.`
    );
  }
  return records.map((rec, idx) => {
    if (!rec || typeof rec !== "object") {
      throw new ServiceError(400, `records[${idx}] must be an object.`);
    }
    if (requireIds) {
      const id = String(rec.id || "").trim();
      if (!id) throw new ServiceError(400, `records[${idx}].id is required.`);
      if (!rec.fields || typeof rec.fields !== "object" || Array.isArray(rec.fields)) {
        throw new ServiceError(400, `records[${idx}].fields must be an object.`);
      }
      return { id, fields: { ...rec.fields } };
    }
    if (!rec.fields || typeof rec.fields !== "object" || Array.isArray(rec.fields)) {
      throw new ServiceError(400, `records[${idx}].fields must be an object with Airtable field names as keys.`);
    }
    return { fields: { ...rec.fields } };
  });
}

export async function createRecordsByTableId(input = {}) {
  const { token, baseId } = assertConfigured();
  const tableId = validateTableId(input.tableId);
  const records = validateWriteRecords(input.records, false);

  const { res, json } = await airtableFetch(`/${baseId}/${tableId}`, {
    token,
    init: {
      method: "POST",
      body: JSON.stringify({ records, typecast: true }),
    },
  });
  if (!res.ok) {
    const err = sanitizeAirtableError(json, res.status);
    if (DEV) console.error("[dealality-airtable-chatgpt] create failed", err);
    throw new ServiceError(res.status >= 400 && res.status < 500 ? res.status : 502, err.message, err);
  }
  return { tableId, records: (json.records || []).map(mapRecord) };
}

export async function updateRecordByTableId(input = {}) {
  const { token, baseId } = assertConfigured();
  const tableId = validateTableId(input.tableId);
  const recordId = String(input.recordId || "").trim();
  if (!recordId) throw new ServiceError(400, "recordId is required.");
  if (!input.fields || typeof input.fields !== "object" || Array.isArray(input.fields)) {
    throw new ServiceError(400, "fields must be an object with Airtable field names as keys.");
  }

  const { res, json } = await airtableFetch(`/${baseId}/${tableId}`, {
    token,
    init: {
      method: "PATCH",
      body: JSON.stringify({
        records: [{ id: recordId, fields: { ...input.fields } }],
        typecast: true,
      }),
    },
  });
  if (!res.ok) {
    const err = sanitizeAirtableError(json, res.status);
    if (DEV) console.error("[dealality-airtable-chatgpt] update failed", err);
    throw new ServiceError(res.status >= 400 && res.status < 500 ? res.status : 502, err.message, err);
  }
  const updated = (json.records || [])[0];
  if (!updated) throw new ServiceError(502, "Airtable returned no updated record.");
  return mapRecord(updated);
}

export async function updateRecordsByTableId(input = {}) {
  const { token, baseId } = assertConfigured();
  const tableId = validateTableId(input.tableId);
  const records = validateWriteRecords(input.records, true);

  const { res, json } = await airtableFetch(`/${baseId}/${tableId}`, {
    token,
    init: {
      method: "PATCH",
      body: JSON.stringify({ records, typecast: true }),
    },
  });
  if (!res.ok) {
    const err = sanitizeAirtableError(json, res.status);
    if (DEV) console.error("[dealality-airtable-chatgpt] bulk update failed", err);
    throw new ServiceError(res.status >= 400 && res.status < 500 ? res.status : 502, err.message, err);
  }
  return { tableId, records: (json.records || []).map(mapRecord) };
}

function incrementCount(map, value) {
  const key = value == null || String(value).trim() === "" ? "(blank)" : String(value);
  map[key] = (map[key] || 0) + 1;
}

function isIncompleteStatus(value) {
  if (value == null || String(value).trim() === "") return true;
  return !INCOMPLETE_STATUS_VALUES.has(String(value).trim().toLowerCase());
}

function pickSummaryFields(fields, keys) {
  const out = {};
  for (const k of keys) {
    if (fields[k] != null && fields[k] !== "") out[k] = fields[k];
  }
  return out;
}

export async function summarizeRecordsByTableId(input = {}) {
  const { token, baseId } = assertConfigured();
  const tableId = validateTableId(input.tableId);
  const maxRecords = clampInt(
    input.maxRecords,
    SUMMARIZE_MAX_RECORDS_DEFAULT,
    1,
    SUMMARIZE_MAX_RECORDS_CAP
  );

  const listInput = {
    tableId,
    maxRecords,
    view: input.view,
    filterByFormula: input.filterByFormula,
  };
  const { records } = await listRecordsByTableId(listInput);

  const statusField = input.statusField ? String(input.statusField).trim() : null;
  const phaseField = input.phaseField ? String(input.phaseField).trim() : null;
  const priorityField = input.priorityField ? String(input.priorityField).trim() : null;
  const groupByField = input.groupBy ? String(input.groupBy).trim() : null;

  const counts = {
    byStatus: {},
    byPhase: {},
    byPriority: {},
    byGroup: {},
  };
  const issues = [];
  const incomplete = [];

  for (const rec of records) {
    const f = rec.fields || {};
    if (statusField) incrementCount(counts.byStatus, f[statusField]);
    if (phaseField) incrementCount(counts.byPhase, f[phaseField]);
    if (priorityField) incrementCount(counts.byPriority, f[priorityField]);
    if (groupByField) incrementCount(counts.byGroup, f[groupByField]);

    if (tableId === "tblpCg0QZ0kIPXihE" && !f.Task) {
      issues.push({ recordId: rec.id, issue: "Missing Task" });
    }
    if (statusField && isIncompleteStatus(f[statusField])) {
      incomplete.push(rec);
    }
  }

  const sampleKeys = [
    "Task",
    "Workstream",
    "Status",
    "Phase",
    "Priority",
    "Start",
    "End",
    "Assigned To",
  ].filter(Boolean);

  const sampleRecords = incomplete.slice(0, 10).map((rec) => ({
    id: rec.id,
    fields: pickSummaryFields(rec.fields || {}, sampleKeys),
  }));

  return {
    tableId,
    totalRecords: records.length,
    counts,
    sampleRecords,
    issues: issues.slice(0, 25),
  };
}
