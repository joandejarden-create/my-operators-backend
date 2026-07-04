/**
 * Create or verify "Brand Alias Mapping" on Deal Capture Platform (AIRTABLE_BASE_ID_ALT).
 *
 * Requires AIRTABLE_API_KEY with schema.bases:write on the Platform base.
 *
 * Usage: node scripts/ensure-brand-alias-mapping-table.mjs
 *        node scripts/ensure-brand-alias-mapping-table.mjs --dry-run
 */
import "../load-env.js";
import { ALIAS_FIELDS } from "../lib/hotel-census/fields.js";

const TABLE_NAME = process.env.AIRTABLE_BRAND_ALIAS_TABLE || "Brand Alias Mapping";

const REQUIRED_FIELDS = [
  {
    name: ALIAS_FIELDS.canonicalBrandName,
    type: "singleLineText",
    description: "Dealality / Brand Explorer display name (canonical).",
  },
  {
    name: ALIAS_FIELDS.aliasSourceBrandName,
    type: "singleLineText",
    description: "Exact Hotel Census Affiliation value(s).",
  },
  {
    name: ALIAS_FIELDS.parentCompany,
    type: "singleLineText",
    description: "Optional parent company scope for alias rows.",
  },
  {
    name: ALIAS_FIELDS.active,
    type: "checkbox",
    options: { icon: "check", color: "greenBright" },
  },
  {
    name: ALIAS_FIELDS.matchConfidence,
    type: "singleSelect",
    options: {
      choices: [{ name: "High" }, { name: "Medium" }, { name: "Low" }],
    },
  },
  {
    name: ALIAS_FIELDS.notes,
    type: "multilineText",
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

function findTable(tables, name) {
  return (tables || []).find((t) => t.name === name);
}

function fieldNames(table) {
  return new Set((table?.fields || []).map((f) => f.name));
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;

  if (!token) throw new Error("Set AIRTABLE_API_KEY (writable PAT with schema.bases:write)");
  if (!baseId) throw new Error("Set AIRTABLE_BASE_ID_ALT");

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) {
    throw new Error(
      `List tables failed ${listRes.status}: ${JSON.stringify(listJson)}. ` +
        "PAT may need schema.bases:read and schema.bases:write on Platform base."
    );
  }

  const tables = listJson.tables || [];
  let aliasTable = findTable(tables, TABLE_NAME);
  const report = {
    tableName: TABLE_NAME,
    baseId,
    createdTable: false,
    fieldsCreated: [],
    fieldsPresent: [],
    fieldMismatches: [],
  };

  if (!aliasTable) {
    if (dryRun) {
      console.log(`[dry-run] Would create table "${TABLE_NAME}" with ${REQUIRED_FIELDS.length} fields.`);
      return;
    }

    const body = {
      name: TABLE_NAME,
      description:
        "Maps Dealality canonical brand names to exact Hotel Census Affiliation strings (Phase 1 read-only from app).",
      fields: REQUIRED_FIELDS,
    };

    const { res, json } = await metaFetch(baseId, token, "/tables", {
      method: "POST",
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      throw new Error(`Create table failed ${res.status}: ${JSON.stringify(json)}`);
    }

    report.createdTable = true;
    aliasTable = json;
    console.log(`Created table "${TABLE_NAME}" (${json.id}).`);
    for (const f of REQUIRED_FIELDS) report.fieldsCreated.push(f.name);
  } else {
    console.log(`Table "${TABLE_NAME}" already exists (${aliasTable.id}).`);
    const existing = fieldNames(aliasTable);
    const missing = REQUIRED_FIELDS.filter((f) => !existing.has(f.name));

    for (const f of REQUIRED_FIELDS) {
      if (existing.has(f.name)) report.fieldsPresent.push(f.name);
      else report.fieldMismatches.push({ expected: f.name, status: "missing" });
    }

    const unexpected = [...existing].filter(
      (n) => !REQUIRED_FIELDS.some((f) => f.name === n)
    );
    if (unexpected.length) {
      console.log("Extra fields on table (left unchanged):", unexpected.join(", "));
    }

    for (const fieldDef of missing) {
      if (dryRun) {
        console.log(`[dry-run] Would add field "${fieldDef.name}"`);
        continue;
      }
      const { res, json } = await metaFetch(baseId, token, `/tables/${aliasTable.id}/fields`, {
        method: "POST",
        body: JSON.stringify(fieldDef),
      });
      if (!res.ok) {
        throw new Error(`Add field "${fieldDef.name}" failed ${res.status}: ${JSON.stringify(json)}`);
      }
      report.fieldsCreated.push(fieldDef.name);
      console.log(`Added field "${fieldDef.name}" (${json.id}).`);
    }
  }

  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
