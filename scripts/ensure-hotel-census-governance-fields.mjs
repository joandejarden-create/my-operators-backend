/**
 * Add optional governance fields on Hotel Census (Deal Capture Platform).
 *
 * Requires AIRTABLE_API_KEY with schema.bases:write, AIRTABLE_BASE_ID_ALT.
 *
 * Usage: node scripts/ensure-hotel-census-governance-fields.mjs
 *        node scripts/ensure-hotel-census-governance-fields.mjs --dry-run
 */
import "../load-env.js";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const GOVERNANCE_FIELDS = [
  {
    name: CENSUS_FIELDS.includeInBrandExplorer,
    type: "checkbox",
    description:
      "When false, exclude property from Brand Explorer census rollups. Blank counts in Phase 1B.",
    options: { icon: "check", color: "greenBright" },
  },
  {
    name: CENSUS_FIELDS.dataConfidence,
    type: "singleSelect",
    description: "Ops QA label for census row quality (not a filter in Phase 1B).",
    options: {
      choices: [
        { name: "High" },
        { name: "Medium" },
        { name: "Low" },
        { name: "Needs Review" },
      ],
    },
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

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;

  if (!token) throw new Error("Set AIRTABLE_API_KEY (schema.bases:write)");
  if (!baseId) throw new Error("Set AIRTABLE_BASE_ID_ALT");

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) {
    throw new Error(`List tables failed ${listRes.status}: ${JSON.stringify(listJson)}`);
  }

  const censusTable = (listJson.tables || []).find((t) => t.name === HOTEL_CENSUS_TABLE);
  if (!censusTable) {
    throw new Error(`Table not found: ${HOTEL_CENSUS_TABLE}`);
  }

  const existing = new Set((censusTable.fields || []).map((f) => f.name));
  const report = { table: HOTEL_CENSUS_TABLE, tableId: censusTable.id, fieldsCreated: [], fieldsPresent: [] };

  for (const fieldDef of GOVERNANCE_FIELDS) {
    if (existing.has(fieldDef.name)) {
      report.fieldsPresent.push(fieldDef.name);
      console.log(`Field already exists: "${fieldDef.name}"`);
      continue;
    }
    if (dryRun) {
      console.log(`[dry-run] Would add field "${fieldDef.name}"`);
      report.fieldsCreated.push(fieldDef.name);
      continue;
    }
    const { res, json } = await metaFetch(baseId, token, `/tables/${censusTable.id}/fields`, {
      method: "POST",
      body: JSON.stringify(fieldDef),
    });
    if (!res.ok) {
      throw new Error(`Add field "${fieldDef.name}" failed ${res.status}: ${JSON.stringify(json)}`);
    }
    report.fieldsCreated.push(fieldDef.name);
    console.log(`Added field "${fieldDef.name}" (${json.id})`);
  }

  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
