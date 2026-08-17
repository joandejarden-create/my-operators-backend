/**
 * Create / verify Airtable table "Operator Setup - Brand Relationships".
 *
 *   node scripts/ensure-operator-brand-relationships-table.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BRAND_RELATIONSHIPS_TABLE,
  BRAND_RELATIONSHIPS_SECTIONS,
} from "../api/lib/operator-brand-relationships-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const MASTER_TABLE = process.env.OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";
const APPLY = process.argv.includes("--apply");

const SECTION_CHOICES = Object.values(BRAND_RELATIONSHIPS_SECTIONS).map((name) => ({ name }));

function scalarFieldSpecs() {
  return [
    { name: "title", type: "singleLineText", description: "Brand family, segment, capability title, or KPI label." },
    { name: "section", type: "singleSelect", options: { choices: SECTION_CHOICES } },
    { name: "row_key", type: "singleLineText" },
    { name: "display_order", type: "number", options: { precision: 0 } },
    { name: "subtitle", type: "singleLineText", description: "Mix %, relationship type, etc." },
    { name: "body", type: "multilineText" },
    { name: "extra", type: "singleLineText", description: "Status, depth, signal value." },
  ];
}

function operatorLinkField(linkedTableId) {
  return {
    name: "Operator",
    type: "multipleRecordLinks",
    options: { linkedTableId },
  };
}

async function metaFetch(baseId, token, metaPath, init = {}) {
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

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("Set AIRTABLE_BASE_ID and AIRTABLE_API_KEY");

  const { res: listRes, json: listJson } = await metaFetch(baseId, apiKey, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed: ${JSON.stringify(listJson)}`);

  const tables = listJson.tables || [];
  const master = tables.find((t) => t.name === MASTER_TABLE);
  if (!master) throw new Error(`Master table not found: ${MASTER_TABLE}`);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    tableName: BRAND_RELATIONSHIPS_TABLE,
    tableId: null,
    createdTable: false,
    fieldsCreated: [],
    fieldsSkipped: [],
    fieldsFailed: [],
  };

  let table = tables.find((t) => t.name === BRAND_RELATIONSHIPS_TABLE);
  const needed = [...scalarFieldSpecs(), operatorLinkField(master.id)];

  if (!table) {
    if (!APPLY) {
      console.log("WOULD CREATE TABLE", BRAND_RELATIONSHIPS_TABLE);
      report.wouldCreateTable = true;
    } else {
      const { res, json } = await metaFetch(baseId, apiKey, "/tables", {
        method: "POST",
        body: JSON.stringify({
          name: BRAND_RELATIONSHIPS_TABLE,
          description:
            "Brand & Relationships Explorer rows (portfolio mix, depth, capabilities, narratives, signals). Linked to Operator Setup - Master.",
          fields: needed,
        }),
      });
      if (!res.ok) throw new Error(`Create table failed: ${JSON.stringify(json)}`);
      table = json;
      report.createdTable = true;
      report.tableId = json.id;
      console.log("CREATED TABLE", json.name, json.id);
      (json.fields || []).forEach((f) => report.fieldsCreated.push(f.name));
    }
  } else {
    report.tableId = table.id;
    console.log("TABLE EXISTS", table.name, table.id);
    const existing = new Set((table.fields || []).map((f) => f.name));
    for (const spec of needed) {
      if (existing.has(spec.name)) {
        report.fieldsSkipped.push(spec.name);
        continue;
      }
      if (!APPLY) {
        report.fieldsCreated.push(spec.name);
        continue;
      }
      const { res, json } = await metaFetch(baseId, apiKey, `/tables/${table.id}/fields`, {
        method: "POST",
        body: JSON.stringify(spec),
      });
      if (!res.ok) {
        report.fieldsFailed.push({ name: spec.name, error: json });
        console.error("FIELD FAILED", spec.name, json);
      } else {
        report.fieldsCreated.push(spec.name);
        console.log("CREATED FIELD", spec.name);
      }
    }
  }

  const outPath = path.join(ROOT, "reports", "ensure-operator-brand-relationships-table.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("Wrote", outPath);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
