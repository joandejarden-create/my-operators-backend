#!/usr/bin/env node
/**
 * Add Leadership Team Members profile-detail columns (multiple selects + numbers + text).
 * Safe to re-run — skips existing fields.
 *
 * Env: AIRTABLE_API_KEY (schema.bases:write), AIRTABLE_BASE_ID
 *
 * Usage:
 *   node scripts/ensure-operator-leadership-member-profile-fields.mjs
 *   node scripts/ensure-operator-leadership-member-profile-fields.mjs --apply
 */
import "../load-env.js";
import {
  LEADERSHIP_MEMBER_SELECT_OPTIONS,
  MAP_LEADERSHIP_MEMBER,
} from "../api/lib/operator-leadership-member-map.js";

const TABLE_NAME = "Operator Setup - Leadership Team Members";
const APPLY = process.argv.includes("--apply");
const SYNC_OPTIONS = process.argv.includes("--sync-options") || APPLY;

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}

/** Merge canonical option names onto existing Airtable choices (preserve ids + extras). */
function mergeSelectChoices(existingChoices, expectedNames) {
  const byName = new Map();
  for (const c of existingChoices || []) {
    if (c?.name) byName.set(c.name, c);
  }
  const merged = [];
  const seen = new Set();
  for (const name of expectedNames) {
    seen.add(name);
    merged.push(byName.get(name) || { name });
  }
  for (const c of existingChoices || []) {
    if (c?.name && !seen.has(c.name)) merged.push(c);
  }
  return merged;
}

const MULTI_SELECT_SPECS = [
  { mapKey: "languages", optionsKey: "languages" },
  { mapKey: "marketExperience", optionsKey: "marketExperience" },
  { mapKey: "coreExpertise", optionsKey: "coreExpertise" },
  { mapKey: "relevantAssetTypes", optionsKey: "relevantAssetTypes" },
];

const FIELD_SPECS = [
  {
    name: MAP_LEADERSHIP_MEMBER.hospitalityExperienceYears,
    type: "number",
    options: { precision: 1 },
    description: "Years of hospitality industry experience (Explorer profile card).",
  },
  {
    name: MAP_LEADERSHIP_MEMBER.companyTenureYears,
    type: "number",
    options: { precision: 1 },
    description: "Years with current operator (Explorer profile card).",
  },
  {
    name: MAP_LEADERSHIP_MEMBER.priorBackground,
    type: "singleLineText",
    description: "Prior employers / background line for Explorer profile card.",
  },
  {
    name: MAP_LEADERSHIP_MEMBER.languages,
    type: "multipleSelects",
    options: choices(LEADERSHIP_MEMBER_SELECT_OPTIONS.languages),
    description: "Languages spoken (per executive).",
  },
  {
    name: MAP_LEADERSHIP_MEMBER.marketExperience,
    type: "multipleSelects",
    options: choices(LEADERSHIP_MEMBER_SELECT_OPTIONS.marketExperience),
    description: "Markets where this executive has credible experience.",
  },
  {
    name: MAP_LEADERSHIP_MEMBER.coreExpertise,
    type: "multipleSelects",
    options: choices(LEADERSHIP_MEMBER_SELECT_OPTIONS.coreExpertise),
    description: "Core functional expertise (per executive).",
  },
  {
    name: MAP_LEADERSHIP_MEMBER.relevantAssetTypes,
    type: "multipleSelects",
    options: choices(LEADERSHIP_MEMBER_SELECT_OPTIONS.relevantAssetTypes),
    description: "Asset types this executive knows well.",
  },
];

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}${path}`;
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

async function syncMultiSelectOptions(table, dryRun) {
  const report = [];
  for (const spec of MULTI_SELECT_SPECS) {
    const fieldName = MAP_LEADERSHIP_MEMBER[spec.mapKey];
    const expected = LEADERSHIP_MEMBER_SELECT_OPTIONS[spec.optionsKey];
    const field = (table.fields || []).find((f) => f.name === fieldName);
    if (!field) {
      report.push({ field: fieldName, status: "missing_field" });
      console.warn("SKIP sync (field missing):", fieldName);
      continue;
    }
    if (field.type !== "multipleSelects") {
      report.push({ field: fieldName, status: "wrong_type", type: field.type });
      continue;
    }
    const current = field.options?.choices || [];
    const currentNames = new Set(current.map((c) => c.name));
    const missing = expected.filter((n) => !currentNames.has(n));
    if (!missing.length) {
      report.push({ field: fieldName, status: "ok", count: current.length });
      console.log("OPTIONS OK", fieldName, `(${current.length} choices)`);
      continue;
    }
    const merged = mergeSelectChoices(current, expected);
    console.log("SYNC", fieldName, "add:", missing.join(", "));
    if (dryRun) {
      report.push({ field: fieldName, status: "would_sync", added: missing });
      continue;
    }
    const { res, json } = await metaFetch(
      process.env.AIRTABLE_BASE_ID,
      process.env.AIRTABLE_API_KEY,
      `/tables/${table.id}/fields/${field.id}`,
      {
        method: "PATCH",
        body: JSON.stringify({ options: { choices: merged } }),
      }
    );
    if (!res.ok) {
      report.push({ field: fieldName, status: "failed", error: json });
      console.error("FAILED", fieldName, res.status, JSON.stringify(json));
      console.error("  Hint: run scripts/restore-leadership-member-select-options.mjs --apply");
      continue;
    }
    report.push({ field: fieldName, status: "synced", added: missing, total: merged.length });
    console.log("SYNCED", fieldName, `→ ${merged.length} choices`);
    await new Promise((r) => setTimeout(r, 250));
  }
  return report;
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed ${listRes.status}: ${JSON.stringify(listJson)}`);

  const table = (listJson.tables || []).find((t) => t.name === TABLE_NAME);
  if (!table) throw new Error(`Table not found: ${TABLE_NAME}`);

  const existing = new Set((table.fields || []).map((f) => f.name));
  const planned = FIELD_SPECS.filter((s) => !existing.has(s.name));

  console.log(`Table: ${TABLE_NAME} (${table.id})`);
  console.log(`Existing fields: ${existing.size}; planned creates: ${planned.length}`);
  planned.forEach((s) => console.log("  -", s.name, `(${s.type})`));

  if (!APPLY) {
    console.log("\nDry run. Re-run with --apply to create fields.");
    if (SYNC_OPTIONS) {
      console.log("\n--- Select option sync (dry-run) ---");
      await syncMultiSelectOptions(table, true);
    }
    return;
  }

  if (!planned.length) {
    console.log("All profile fields already exist.");
  }

  for (const spec of planned) {
    const body = {
      name: spec.name,
      type: spec.type,
      description: spec.description || "",
      ...(spec.options ? { options: spec.options } : {}),
    };
    const { res, json } = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      console.error("FAILED", spec.name, res.status, JSON.stringify(json));
      continue;
    }
    console.log("CREATED", spec.name, json.id);
    await new Promise((r) => setTimeout(r, 250));
  }

  if (SYNC_OPTIONS) {
    console.log("\n--- Select option sync ---");
    const { res: refreshRes, json: refreshJson } = await metaFetch(baseId, token, "/tables");
    if (!refreshRes.ok) throw new Error(`List tables failed ${refreshRes.status}`);
    const refreshed = (refreshJson.tables || []).find((t) => t.name === TABLE_NAME);
    await syncMultiSelectOptions(refreshed || table, false);
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
