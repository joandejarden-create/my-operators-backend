/**
 * Create / verify Airtable table "Operator Setup - Operating Platform".
 *
 *   node scripts/ensure-operator-operating-platform-table.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  OPERATING_PLATFORM_TABLE,
  OPERATING_PLATFORM_SECTIONS,
  OPERATING_PLATFORM_ROW_TYPES,
} from "../api/lib/operator-operating-platform-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const MASTER_TABLE = process.env.OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";
const APPLY = process.argv.includes("--apply");

const SECTION_CHOICES = Object.values(OPERATING_PLATFORM_SECTIONS).map((name) => ({ name }));
const ROW_TYPE_CHOICES = Object.values(OPERATING_PLATFORM_ROW_TYPES).map((name) => ({ name }));

function scalarFieldSpecs() {
  return [
    { name: "title", type: "singleLineText", description: "Capability title, KPI label, or card title." },
    { name: "section", type: "singleSelect", options: { choices: SECTION_CHOICES } },
    { name: "row_type", type: "singleSelect", options: { choices: ROW_TYPE_CHOICES } },
    { name: "row_key", type: "singleLineText" },
    { name: "display_order", type: "number", options: { precision: 0 } },
    { name: "subtitle", type: "singleLineText" },
    { name: "body", type: "multilineText" },
    { name: "extra", type: "singleLineText" },
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
    tableName: OPERATING_PLATFORM_TABLE,
    tableId: null,
    createdTable: false,
    fieldsCreated: [],
    fieldsSkipped: [],
    fieldsFailed: [],
  };

  let table = tables.find((t) => t.name === OPERATING_PLATFORM_TABLE);
  const needed = [...scalarFieldSpecs(), operatorLinkField(master.id)];

  if (!table) {
    if (!APPLY) {
      console.log("WOULD CREATE TABLE", OPERATING_PLATFORM_TABLE);
      report.wouldCreateTable = true;
    } else {
      const { res, json } = await metaFetch(baseId, apiKey, "/tables", {
        method: "POST",
        body: JSON.stringify({
          name: OPERATING_PLATFORM_TABLE,
          description:
            "Operating Platform Explorer rows (capability tiles, KPI levels, signals). Linked to Operator Setup - Master.",
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
        console.log("WOULD CREATE FIELD", spec.name);
        report.fieldsCreated.push(spec.name);
        continue;
      }
      const { res, json } = await metaFetch(
        baseId,
        apiKey,
        `/tables/${table.id}/fields`,
        { method: "POST", body: JSON.stringify(spec) }
      );
      if (res.ok) {
        report.fieldsCreated.push(spec.name);
        existing.add(spec.name);
        console.log("CREATED FIELD", spec.name);
      } else {
        report.fieldsFailed.push({ name: spec.name, error: json });
        console.error("FAILED", spec.name, JSON.stringify(json));
      }
      await new Promise((r) => setTimeout(r, 220));
    }
  }

  const outPath = path.join(
    ROOT,
    "reports",
    `operator-operating-platform-schema-${new Date().toISOString().slice(0, 10)}.json`
  );
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("Wrote", outPath);
  if (!APPLY) console.log("\nDry run. Re-run with --apply");
  if (report.fieldsFailed.length) process.exit(1);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
