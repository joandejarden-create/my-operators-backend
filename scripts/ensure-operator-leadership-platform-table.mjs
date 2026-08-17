/**
 * Create / verify Airtable table "Operator Setup - Leadership Platform" (Metadata API).
 *
 * Child rows for Explorer Organization Structure, Team Depth, Languages, Cadence, Markets, Owner Relationship.
 *
 * Requirements: AIRTABLE_API_KEY with schema.bases:read + schema.bases:write, AIRTABLE_BASE_ID
 *
 * Usage:
 *   node scripts/ensure-operator-leadership-platform-table.mjs
 *   node scripts/ensure-operator-leadership-platform-table.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  LEADERSHIP_PLATFORM_TABLE,
  LEADERSHIP_PLATFORM_SECTIONS,
  TEAM_DEPTH_OPTIONS,
} from "../api/lib/operator-leadership-platform-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const MASTER_TABLE =
  process.env.OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";

const APPLY = process.argv.includes("--apply");

const SECTION_CHOICES = Object.values(LEADERSHIP_PLATFORM_SECTIONS).map((name) => ({ name }));
const DEPTH_CHOICES = TEAM_DEPTH_OPTIONS.map((name) => ({ name }));

/** Scalar fields (excluding Operator link — added after table exists or with master id). */
function scalarFieldSpecs() {
  return [
    {
      name: "title",
      type: "singleLineText",
      description: "Primary label (layer title, function, language, market, cadence, touchpoint).",
    },
    {
      name: "section",
      type: "singleSelect",
      description: "Which Explorer subsection this row belongs to.",
      options: { choices: SECTION_CHOICES },
    },
    {
      name: "display_order",
      type: "number",
      options: { precision: 0 },
      description: "Sort order within section for one operator.",
    },
    {
      name: "subtitle",
      type: "singleLineText",
      description: "Secondary label (lead role, proficiency, owner-relationship lead).",
    },
    {
      name: "body",
      type: "multilineText",
      description: "Description, relevance, support text, or experience narrative.",
    },
    {
      name: "extra",
      type: "multilineText",
      description: "Comma-separated org tags or relevant leader names.",
    },
    {
      name: "depth",
      type: "singleSelect",
      description: "Team Depth rows only.",
      options: { choices: DEPTH_CHOICES },
    },
  ];
}

function operatorLinkField(linkedTableId) {
  return {
    name: "Operator",
    type: "multipleRecordLinks",
    description: "Link to Operator Setup - Master.",
    options: { linkedTableId },
  };
}

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${path}`;
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

async function createField(baseId, token, tableId, spec) {
  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify(spec),
  });
  return { ok: res.ok, status: res.status, json };
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    throw new Error("Set AIRTABLE_BASE_ID and AIRTABLE_API_KEY");
  }

  const { res: listRes, json: listJson } = await metaFetch(baseId, apiKey, "/tables");
  if (!listRes.ok) {
    throw new Error(`List tables failed ${listRes.status}: ${JSON.stringify(listJson)}`);
  }

  const tables = listJson.tables || [];
  const master = tables.find((t) => t.name === MASTER_TABLE);
  if (!master) {
    throw new Error(`Master table not found: "${MASTER_TABLE}"`);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    tableName: LEADERSHIP_PLATFORM_TABLE,
    masterTable: MASTER_TABLE,
    masterTableId: master.id,
    createdTable: false,
    tableId: null,
    fieldsCreated: [],
    fieldsSkipped: [],
    fieldsFailed: [],
  };

  let platformTable = tables.find((t) => t.name === LEADERSHIP_PLATFORM_TABLE);

  if (!platformTable) {
    const createBody = {
      name: LEADERSHIP_PLATFORM_TABLE,
      description:
        "Leadership Explorer platform rows (org structure, team depth, languages, cadence, markets, owner model). Linked to Operator Setup - Master.",
      fields: [...scalarFieldSpecs(), operatorLinkField(master.id)],
    };

    if (!APPLY) {
      console.log(
        "WOULD CREATE TABLE",
        LEADERSHIP_PLATFORM_TABLE,
        "with",
        createBody.fields.length,
        "fields"
      );
      report.wouldCreateTable = true;
    } else {
      const { res, json } = await metaFetch(baseId, apiKey, "/tables", {
        method: "POST",
        body: JSON.stringify(createBody),
      });
      if (!res.ok) {
        throw new Error(`Create table failed ${res.status}: ${JSON.stringify(json)}`);
      }
      platformTable = json;
      report.createdTable = true;
      report.tableId = json.id;
      console.log("CREATED TABLE", json.name, json.id);
      (json.fields || []).forEach((f) => report.fieldsCreated.push(f.name));
    }
  } else {
    report.tableId = platformTable.id;
    console.log("TABLE EXISTS", platformTable.name, platformTable.id);

    const existing = new Set((platformTable.fields || []).map((f) => f.name));
    const needed = [...scalarFieldSpecs(), operatorLinkField(master.id)];

    for (const spec of needed) {
      if (existing.has(spec.name)) {
        report.fieldsSkipped.push(spec.name);
        console.log("SKIP FIELD", spec.name);
        continue;
      }
      if (!APPLY) {
        console.log("WOULD CREATE FIELD", spec.name, `(${spec.type})`);
        report.fieldsCreated.push(spec.name);
        continue;
      }
      const { ok, status, json } = await createField(baseId, apiKey, platformTable.id, spec);
      if (ok) {
        report.fieldsCreated.push(spec.name);
        existing.add(spec.name);
        console.log("CREATED FIELD", spec.name);
      } else {
        report.fieldsFailed.push({ name: spec.name, status, error: json });
        console.error("FAILED FIELD", spec.name, status, JSON.stringify(json));
      }
      await new Promise((r) => setTimeout(r, 220));
    }
  }

  const stamp = new Date().toISOString().slice(0, 10);
  const reportsDir = path.join(ROOT, "reports");
  if (!fs.existsSync(reportsDir)) fs.mkdirSync(reportsDir, { recursive: true });
  const outPath = path.join(reportsDir, `operator-leadership-platform-schema-${stamp}.json`);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("\nWrote", outPath);

  if (!APPLY) {
    console.log("\nDry run. Re-run with --apply to create table/fields in Airtable.");
  } else if (report.tableId) {
    console.log(`\nOptional env override: AIRTABLE_OPERATOR_SETUP_LEADERSHIP_PLATFORM_TABLE=${LEADERSHIP_PLATFORM_TABLE}`);
    console.log("Table id:", report.tableId);
  }

  if (report.fieldsFailed.length) process.exit(1);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
