/**
 * Leak Audit Airtable REST client — writes only to AIRTABLE_LEAK_AUDIT_BASE_ID.
 * Never targets production ADP / Census / Explorer bases.
 */

import {
  LEAK_AUDIT_TABLE_NAMES,
  resolveLeakAuditTableName,
} from "./airtable-schema.js";

const AIRTABLE_API = "https://api.airtable.com/v0";

export function isLeakAuditAirtableConfigured(env = process.env) {
  const baseId = String(env.AIRTABLE_LEAK_AUDIT_BASE_ID || "").trim();
  const apiKey = String(
    env.AIRTABLE_LEAK_AUDIT_API_KEY || env.AIRTABLE_API_KEY || env.AIRTABLE_PAT || ""
  ).trim();
  return Boolean(baseId && apiKey);
}

export function getLeakAuditAirtableConfig(env = process.env) {
  const baseId = String(env.AIRTABLE_LEAK_AUDIT_BASE_ID || "").trim();
  const apiKey = String(
    env.AIRTABLE_LEAK_AUDIT_API_KEY || env.AIRTABLE_API_KEY || env.AIRTABLE_PAT || ""
  ).trim();
  const tablePrefix = String(env.AIRTABLE_LEAK_AUDIT_TABLE_PREFIX || "").trim();
  if (!baseId || !apiKey) {
    return {
      configured: false,
      baseId: null,
      apiKey: null,
      tablePrefix,
      reason: "missing_AIRTABLE_LEAK_AUDIT_BASE_ID_or_API_KEY",
    };
  }
  // Hard isolation: refuse if misconfigured to known ADP publish base alias
  if (
    env.ADP_AIRTABLE_BASE_ID &&
    baseId === String(env.ADP_AIRTABLE_BASE_ID).trim() &&
    env.AIRTABLE_LEAK_AUDIT_ALLOW_SHARED_BASE !== "1"
  ) {
    return {
      configured: false,
      baseId: null,
      apiKey: null,
      tablePrefix,
      reason: "leak_audit_base_must_not_equal_ADP_AIRTABLE_BASE_ID",
    };
  }
  return { configured: true, baseId, apiKey, tablePrefix };
}

function toAirtableFields(record) {
  const fields = { ...record };
  delete fields._airtableRecordId;
  // Keep id as a field for round-trip; Airtable also has its own record id
  return fields;
}

function fromAirtableRecord(row) {
  if (!row) return null;
  return {
    ...(row.fields || {}),
    _airtableRecordId: row.id,
  };
}

export function createLeakAuditAirtableClient(options = {}) {
  const env = options.env || process.env;
  const cfg = getLeakAuditAirtableConfig(env);
  const fetchImpl = options.fetchImpl || globalThis.fetch;

  async function request(method, tableLogicalName, { pathSuffix = "", body, query } = {}) {
    if (!cfg.configured) {
      const err = new Error(`leak_audit_airtable_not_configured:${cfg.reason}`);
      err.code = "LEAK_AUDIT_AIRTABLE_NOT_CONFIGURED";
      throw err;
    }
    if (!LEAK_AUDIT_TABLE_NAMES[tableLogicalName] && !tableLogicalName.startsWith("AdpLeakAudit")) {
      const err = new Error(`leak_audit_unknown_table:${tableLogicalName}`);
      err.code = "LEAK_AUDIT_UNKNOWN_TABLE";
      throw err;
    }
    const table = resolveLeakAuditTableName(tableLogicalName, cfg.tablePrefix);
    const qs = query
      ? `?${new URLSearchParams(
          Object.entries(query).flatMap(([k, v]) => {
            if (Array.isArray(v)) return v.map((item) => [k, item]);
            return [[k, String(v)]];
          })
        ).toString()}`
      : "";
    const url = `${AIRTABLE_API}/${cfg.baseId}/${encodeURIComponent(table)}${pathSuffix}${qs}`;
    const res = await fetchImpl(url, {
      method,
      headers: {
        Authorization: `Bearer ${cfg.apiKey}`,
        "Content-Type": "application/json",
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    const text = await res.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = { raw: text };
    }
    if (!res.ok) {
      const err = new Error(
        `leak_audit_airtable_${res.status}:${json?.error?.message || text.slice(0, 200)}`
      );
      err.status = res.status;
      err.body = json;
      throw err;
    }
    return json;
  }

  return {
    configured: cfg.configured,
    config: {
      configured: cfg.configured,
      baseId: cfg.configured ? cfg.baseId : null,
      tablePrefix: cfg.tablePrefix,
      reason: cfg.reason || null,
    },

    async createRecord(tableLogicalName, record) {
      const json = await request("POST", tableLogicalName, {
        body: { fields: toAirtableFields(record), typecast: true },
      });
      return fromAirtableRecord(json);
    },

    async updateRecord(tableLogicalName, airtableRecordId, patch) {
      const json = await request("PATCH", tableLogicalName, {
        pathSuffix: `/${encodeURIComponent(airtableRecordId)}`,
        body: { fields: toAirtableFields(patch), typecast: true },
      });
      return fromAirtableRecord(json);
    },

    async getRecord(tableLogicalName, airtableRecordId) {
      const json = await request("GET", tableLogicalName, {
        pathSuffix: `/${encodeURIComponent(airtableRecordId)}`,
      });
      return fromAirtableRecord(json);
    },

    async listRecords(tableLogicalName, { maxRecords = 100, filterByFormula } = {}) {
      const query = { pageSize: String(Math.min(maxRecords, 100)) };
      if (filterByFormula) query.filterByFormula = filterByFormula;
      const json = await request("GET", tableLogicalName, { query });
      return (json.records || []).map(fromAirtableRecord);
    },

    async upsertByLeakId(tableLogicalName, record) {
      if (!record?.id) throw new Error("leak_id_required_for_upsert");
      const existing = await this.listRecords(tableLogicalName, {
        maxRecords: 1,
        filterByFormula: `{id}='${String(record.id).replace(/'/g, "\\'")}'`,
      });
      if (existing[0]?._airtableRecordId) {
        return this.updateRecord(tableLogicalName, existing[0]._airtableRecordId, record);
      }
      return this.createRecord(tableLogicalName, record);
    },
  };
}
