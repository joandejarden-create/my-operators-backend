/**
 * Load live Airtable select/multi-select options for Operator Alignment fields.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  OAS_AUDIT_TABLES,
  OAS_AUDIT_FIELD_SPECS,
  fieldRegistryKey,
} from "./operator-alignment-airtable-options-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
export const LIVE_OPTIONS_JSON = path.join(ROOT, "reports", "operator-alignment-live-airtable-options.json");

let _cache = null;

export function stripAccents(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

export function normalizeOptionKey(s) {
  return stripAccents(String(s || ""))
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[–—]/g, "-");
}

function choicesFromField(field) {
  if (!field) return [];
  const t = field.type;
  if (t === "singleSelect" || t === "multipleSelects") {
    return (field.options?.choices || []).map((c) => c.name).filter(Boolean);
  }
  return [];
}

export async function fetchLiveAirtableTables(baseId, apiKey) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const json = await res.json();
  if (!res.ok) throw new Error(json.error?.message || `meta ${res.status}`);
  return json.tables || [];
}

/**
 * Build live options index from Airtable meta API.
 */
export function buildLiveOptionsIndex(tables) {
  const tableByName = new Map((tables || []).map((t) => [t.name, t]));
  const fields = {};
  const missingFields = [];
  const missingTables = [];

  for (const spec of OAS_AUDIT_FIELD_SPECS) {
    const tableName = OAS_AUDIT_TABLES[spec.tableKey];
    const key = fieldRegistryKey(spec.tableKey, spec.fieldName);
    const t = tableByName.get(tableName);
    if (!t) {
      missingTables.push(tableName);
      fields[key] = { tableKey: spec.tableKey, tableName, fieldName: spec.fieldName, status: "table_missing" };
      continue;
    }
    const f = (t.fields || []).find((x) => x.name === spec.fieldName);
    if (!f) {
      missingFields.push({ table: tableName, field: spec.fieldName });
      fields[key] = {
        tableKey: spec.tableKey,
        tableName,
        fieldName: spec.fieldName,
        status: "field_missing",
        plannedOptions: spec.plannedOptions || [],
      };
      continue;
    }
    const liveOptions = choicesFromField(f);
    fields[key] = {
      tableKey: spec.tableKey,
      tableName,
      fieldName: spec.fieldName,
      fieldType: f.type,
      fieldId: f.id,
      status: "ok",
      liveOptions,
      plannedOptions: spec.plannedOptions || [],
    };
  }

  return {
    exportedAt: new Date().toISOString(),
    fields,
    missingFields,
    missingTables: [...new Set(missingTables)],
  };
}

export function loadLiveOptionsFromFile(filePath = LIVE_OPTIONS_JSON) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

export async function getLiveOperatorAlignmentOptions({ refresh = false, baseId, apiKey } = {}) {
  if (!refresh && _cache) return _cache;

  if (!refresh) {
    const fromFile = loadLiveOptionsFromFile();
    if (fromFile?.fields) {
      _cache = fromFile;
      return _cache;
    }
  }

  const bid = baseId || process.env.AIRTABLE_BASE_ID;
  const key = apiKey || process.env.AIRTABLE_API_KEY;
  if (!bid || !key) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required for live options");
  const tables = await fetchLiveAirtableTables(bid, key);
  _cache = buildLiveOptionsIndex(tables);
  return _cache;
}

export function getFieldLiveMeta(liveIndex, tableKey, fieldName) {
  return liveIndex?.fields?.[fieldRegistryKey(tableKey, fieldName)] || null;
}

export function getLiveOptionsList(liveIndex, tableKey, fieldName) {
  const m = getFieldLiveMeta(liveIndex, tableKey, fieldName);
  return m?.liveOptions || [];
}

/** Build lookup: normalized key → exact Airtable label */
export function buildLiveOptionLookup(liveOptions) {
  const map = new Map();
  for (const label of liveOptions || []) {
    const k = normalizeOptionKey(label);
    if (!k) continue;
    if (!map.has(k)) map.set(k, label);
  }
  return map;
}

export function comparePlannedToLive(planned, live) {
  const plannedSet = new Set((planned || []).map(normalizeOptionKey));
  const liveSet = new Set((live || []).map(normalizeOptionKey));
  const exact = [];
  const missing = [];
  const extra = [];
  for (const p of planned || []) {
    if (liveSet.has(normalizeOptionKey(p))) exact.push(p);
    else missing.push(p);
  }
  for (const l of live || []) {
    if (!plannedSet.has(normalizeOptionKey(l))) extra.push(l);
  }
  let matchStatus = "Exact";
  if (!live?.length && planned?.length) matchStatus = "Missing";
  else if (missing.length && extra.length) matchStatus = "Partial";
  else if (missing.length) matchStatus = "Partial";
  else if (extra.length) matchStatus = "Extra";
  else if (!planned?.length && live?.length) matchStatus = "Extra";
  return { exact, missing, extra, matchStatus };
}
