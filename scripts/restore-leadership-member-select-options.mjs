/**
 * Restore Leadership Team Members multiple-select options via Records API typecast
 * (Meta API PATCH on multipleSelects choices returns 422 in this base).
 *
 * Usage:
 *   node scripts/restore-leadership-member-select-options.mjs
 *   node scripts/restore-leadership-member-select-options.mjs --apply
 *   node scripts/restore-leadership-member-select-options.mjs --apply --field relevant_asset_types
 */
import "../load-env.js";
import {
  MAP_LEADERSHIP_MEMBER,
  LEADERSHIP_MEMBER_SELECT_OPTIONS,
} from "../api/lib/operator-leadership-member-map.js";

const TABLE_NAME = "Operator Setup - Leadership Team Members";
const APPLY = process.argv.includes("--apply");

function parseFieldArg() {
  const i = process.argv.indexOf("--field");
  if (i >= 0 && process.argv[i + 1]) return process.argv[i + 1].trim();
  return "";
}

const FIELD_SPECS = [
  { fieldName: MAP_LEADERSHIP_MEMBER.languages, options: LEADERSHIP_MEMBER_SELECT_OPTIONS.languages },
  { fieldName: MAP_LEADERSHIP_MEMBER.marketExperience, options: LEADERSHIP_MEMBER_SELECT_OPTIONS.marketExperience },
  { fieldName: MAP_LEADERSHIP_MEMBER.coreExpertise, options: LEADERSHIP_MEMBER_SELECT_OPTIONS.coreExpertise },
  { fieldName: MAP_LEADERSHIP_MEMBER.relevantAssetTypes, options: LEADERSHIP_MEMBER_SELECT_OPTIONS.relevantAssetTypes },
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

async function recordsFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE_NAME)}${path}`;
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

async function getFirstRecord(baseId, token) {
  const { res, json } = await recordsFetch(baseId, token, "?pageSize=1");
  if (!res.ok) throw new Error(`List records failed ${res.status}: ${JSON.stringify(json)}`);
  return (json.records || [])[0] || null;
}

async function restoreFieldOptions(baseId, token, tableMeta, fieldName, expectedNames, dryRun) {
  const field = (tableMeta.fields || []).find((f) => f.name === fieldName);
  if (!field) {
    return { fieldName, status: "missing_field" };
  }
  const currentNames = new Set((field.options?.choices || []).map((c) => c.name));
  const extras = [...currentNames].filter((n) => !expectedNames.includes(n));
  const targetNames = [...expectedNames, ...extras];
  const missing = expectedNames.filter((n) => !currentNames.has(n));
  if (!missing.length) {
    return { fieldName, status: "ok", total: currentNames.size };
  }

  const record = await getFirstRecord(baseId, token);
  if (!record) {
    return { fieldName, status: "no_records", missing };
  }

  const original = record.fields?.[fieldName] || [];

  if (dryRun) {
    return {
      fieldName,
      status: "would_restore",
      missing,
      extras,
      scaffoldRecordId: record.id,
    };
  }

  const { res: patchRes, json: patchJson } = await recordsFetch(
    baseId,
    token,
    `/${record.id}`,
    {
      method: "PATCH",
      body: JSON.stringify({
        fields: { [fieldName]: targetNames },
        typecast: true,
      }),
    }
  );
  if (!patchRes.ok) {
    return { fieldName, status: "failed", missing, error: patchJson };
  }

  const { res: restoreRes, json: restoreJson } = await recordsFetch(
    baseId,
    token,
    `/${record.id}`,
    {
      method: "PATCH",
      body: JSON.stringify({
        fields: { [fieldName]: original },
        typecast: true,
      }),
    }
  );
  if (!restoreRes.ok) {
    return {
      fieldName,
      status: "partial",
      missing,
      added: true,
      restoreError: restoreJson,
      scaffoldRecordId: record.id,
    };
  }

  const { json: metaAfter } = await metaFetch(baseId, token, "/tables");
  const tableAfter = (metaAfter.tables || []).find((t) => t.name === TABLE_NAME);
  const fieldAfter = (tableAfter?.fields || []).find((f) => f.name === fieldName);
  const afterNames = (fieldAfter?.options?.choices || []).map((c) => c.name);

  return {
    fieldName,
    status: "restored",
    missing,
    extras,
    total: afterNames.length,
    choices: afterNames,
    scaffoldRecordId: record.id,
  };
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const onlyField = parseFieldArg();
  const specs = onlyField
    ? FIELD_SPECS.filter((s) => s.fieldName === onlyField)
    : FIELD_SPECS;
  if (onlyField && !specs.length) {
    throw new Error(`Unknown field: ${onlyField}`);
  }

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`List tables failed ${res.status}: ${JSON.stringify(json)}`);
  const table = (json.tables || []).find((t) => t.name === TABLE_NAME);
  if (!table) throw new Error(`Table not found: ${TABLE_NAME}`);

  console.log(`Mode: ${APPLY ? "APPLY" : "dry-run"}`);
  console.log(`Table: ${TABLE_NAME}\n`);

  const report = [];
  for (const spec of specs) {
    const result = await restoreFieldOptions(
      baseId,
      token,
      table,
      spec.fieldName,
      spec.options,
      !APPLY
    );
    report.push(result);
    console.log(JSON.stringify(result, null, 2));
  }

  if (!APPLY) {
    console.log("\nRe-run with --apply to restore missing select options.");
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
