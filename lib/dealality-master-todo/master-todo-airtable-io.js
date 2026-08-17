/**
 * Shared Airtable I/O for Dealality master to-do scripts.
 */
import {
  MASTER_TODO_DEFAULT_TABLE_ID,
  MASTER_TODO_GTM_BASE_ID,
} from "./master-todo-field-map.js";

const TASK_TABLE_KEYWORDS =
  /action|task|todo|master|project\s*plan|workstream|gtm\s*task/i;

export function getGtmConfig() {
  const token = (
    process.env.AIRTABLE_TOKEN ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_GTM_API_KEY ||
    ""
  ).trim();
  const baseId = (process.env.AIRTABLE_GTM_BASE_ID || MASTER_TODO_GTM_BASE_ID).trim();
  if (!token) throw new Error("Set AIRTABLE_TOKEN or AIRTABLE_PAT.");
  if (!baseId) throw new Error("Set AIRTABLE_GTM_BASE_ID.");
  return { token, baseId };
}

export async function metaFetch(baseId, token, metaPath, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${metaPath}`;
  const res = await fetch(url, {
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

export async function fetchAllTables(baseId, token) {
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`List tables failed (${res.status}): ${JSON.stringify(json)}`);
  return json.tables || [];
}

export function isCandidateTaskTable(table) {
  return TASK_TABLE_KEYWORDS.test(table.name || "");
}

export function serializeTableForAudit(table) {
  const fields = (table.fields || []).map((f) => ({
    id: f.id,
    name: f.name,
    type: f.type,
    choices:
      f.type === "singleSelect" || f.type === "multipleSelects"
        ? (f.options?.choices || []).map((c) => c.name)
        : null,
  }));
  const views = (table.views || []).map((v) => ({
    id: v.id,
    name: v.name,
    type: v.type || "grid",
  }));
  return {
    id: table.id,
    name: table.name,
    fieldCount: fields.length,
    viewCount: views.length,
    fields,
    views,
  };
}

export async function fetchAllRecords(baseId, token, tableId) {
  const records = [];
  let offset;
  do {
    const url = `https://api.airtable.com/v0/${baseId}/${tableId}?pageSize=100${offset ? `&offset=${offset}` : ""}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json();
    if (!res.ok) throw new Error(`List records failed (${res.status}): ${JSON.stringify(json)}`);
    records.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return records;
}

export function normalizeKey(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[’'"]/g, "")
    .replace(/[^\w\s/+-]/g, "");
}

export function buildMatchKey(taskName, workstream) {
  return `${normalizeKey(taskName)}|${normalizeKey(workstream)}`;
}

export const MASTER_TODO_VIEW_CONFIGS = [
  {
    name: "Master To-Do — Active",
    filterFormula: "AND({Status} != 'Completed', {Status} != 'Deferred', {Status} != 'Not Needed')",
    notes: "Also hide legacy founder-plan rows by adding Source = ChatGPT Master To-Do when that field exists.",
  },
  {
    name: "P1 Today / This Week",
    filterFormula:
      "AND({Priority} = 'P1 = Important Near-Term', {Status} != 'Completed')",
    sort: ["Priority asc", "End asc"],
  },
  {
    name: "GTM / Outreach",
    filterFormula:
      "OR({Phase} = 'GTM / Outreach', {Workstream} = 'Outreach Execution', {Workstream} = 'Pilot Target List', {Workstream} = 'Reply Handling')",
  },
  {
    name: "Pilot Delivery",
    filterFormula: "{Phase} = 'Pilot Delivery'",
  },
  {
    name: "Access Hygiene",
    filterFormula: "{Workstream} = 'Access Hygiene'",
  },
  {
    name: "Completed",
    filterFormula: "{Status} = 'Completed'",
  },
  {
    name: "Deferred / Later",
    filterFormula: "OR({Status} = 'Deferred', {Phase} = 'Later')",
  },
];

export function recommendMasterTable(candidates) {
  const founder = candidates.find((t) => t.id === MASTER_TODO_DEFAULT_TABLE_ID);
  if (founder) {
    return {
      tableId: founder.id,
      tableName: founder.name,
      reason:
        "Only task-shaped table in GTM base. Supports Task, Workstream, Status, Phase, Priority, and operational fields. Use Source = ChatGPT Master To-Do to distinguish master tasks from founder roadmap rows.",
      newTableNeeded: false,
    };
  }
  if (candidates.length === 1) {
    return {
      tableId: candidates[0].id,
      tableName: candidates[0].name,
      reason: "Single candidate task table in base.",
      newTableNeeded: false,
    };
  }
  return {
    tableId: null,
    tableName: null,
    reason: "No safe existing task table found; Joan approval required before creating Dealality Master To-Do table.",
    newTableNeeded: true,
  };
}

export function compareFields(existingFields, recommendedFields) {
  const byName = new Map((existingFields || []).map((f) => [f.name.toLowerCase(), f]));
  const missing = [];
  const present = [];
  for (const rec of recommendedFields) {
    const found = byName.get(rec.name.toLowerCase());
    if (found) {
      present.push({ ...rec, existingType: found.type, fieldId: found.id });
    } else {
      missing.push(rec);
    }
  }
  return { missing, present };
}

export function missingSelectOptions(field, targetOptions) {
  if (!field || !targetOptions?.length) return [];
  const current = new Set(field.choices || []);
  return targetOptions.filter((o) => !current.has(o));
}
