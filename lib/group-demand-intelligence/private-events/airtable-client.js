/**
 * Shared Airtable helpers for Private Events V1.1 graph tables.
 */

import Airtable from "airtable";
import {
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
} from "../../decision-outcomes/airtable-base.js";
import { airtableRequestTimeoutMs } from "../../http/with-timeout.js";
import { PE_AIRTABLE_SCHEMA_VERSION } from "./airtable-field-map.js";

export function getPeAirtableToken() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

export function getPeAirtableBaseId() {
  return getGdiOpportunitiesAirtableBaseId();
}

export function isPeAirtableConfigured() {
  return Boolean(getPeAirtableToken() && getPeAirtableBaseId());
}

export function getPeBase() {
  const apiKey = getPeAirtableToken();
  const baseId = getPeAirtableBaseId();
  if (!apiKey || !baseId) {
    const err = new Error("gdi_private_events_airtable_not_configured");
    err.code = "gdi_private_events_airtable_not_configured";
    throw err;
  }
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "gdi_private_events" });
  return new Airtable({
    apiKey,
    requestTimeout: airtableRequestTimeoutMs(),
  }).base(baseId);
}

export function omitEmpty(fields) {
  const out = {};
  for (const [k, v] of Object.entries(fields)) {
    if (v === undefined || v === null || v === "") continue;
    out[k] = v;
  }
  return out;
}

export function asSelect(value, allowed) {
  const v = value == null ? "" : String(value).trim();
  if (!v) return undefined;
  return allowed.includes(v) ? v : undefined;
}

export function jsonText(value) {
  if (value == null) return undefined;
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch {
    return undefined;
  }
}

export function parseJson(value, fallback) {
  if (value == null || value === "") return fallback;
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

export function nowIso() {
  return new Date().toISOString();
}

export function schemaVersion() {
  return PE_AIRTABLE_SCHEMA_VERSION;
}

export function escapeFormula(value) {
  return String(value || "").replace(/'/g, "\\'");
}

export async function findByField(table, fieldName, value, max = 5) {
  if (!value) return [];
  const formula = `{${fieldName}} = '${escapeFormula(value)}'`;
  const rows = [];
  await table
    .select({ filterByFormula: formula, maxRecords: max })
    .eachPage((page, next) => {
      rows.push(...page);
      next();
    });
  return rows;
}

export async function createOrUpdate(table, { recordId, fields, dryRun }) {
  if (dryRun) {
    return {
      dryRun: true,
      recordId: recordId || null,
      action: recordId ? "update" : "create",
      fields,
    };
  }
  if (!table) {
    const err = new Error("airtable_table_required_for_write");
    err.code = "airtable_table_required_for_write";
    throw err;
  }
  if (recordId) {
    const rec = await table.update(recordId, fields);
    return { dryRun: false, recordId: rec.id, action: "update", record: rec };
  }
  const rec = await table.create(fields);
  return { dryRun: false, recordId: rec.id, action: "create", record: rec };
}
