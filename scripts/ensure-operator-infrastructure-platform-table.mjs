/**
 * Create / verify Airtable table "Operator Setup - Infrastructure Platform" (Metadata API).
 *
 * Child rows for Explorer Infrastructure & Data (signals, stack, services, domains, maturity).
 *
 *   node scripts/ensure-operator-infrastructure-platform-table.mjs
 *   node scripts/ensure-operator-infrastructure-platform-table.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  INFRASTRUCTURE_PLATFORM_TABLE,
  INFRASTRUCTURE_PLATFORM_SECTIONS,
} from "../api/lib/operator-infrastructure-platform-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const MASTER_TABLE =
  process.env.OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";

const APPLY = process.argv.includes("--apply");

const SECTION_CHOICES = Object.values(INFRASTRUCTURE_PLATFORM_SECTIONS).map((name) => ({
  name,
}));

function scalarFieldSpecs() {
  return [
    {
      name: "title",
      type: "singleLineText",
      description: "Primary label (card title, signal name, layer name).",
    },
    {
      name: "section",
      type: "singleSelect",
      description: "Which Infrastructure & Data subsection this row belongs to.",
      options: { choices: SECTION_CHOICES },
    },
    {
      name: "row_key",
      type: "singleLineText",
      description:
        "Stable machine key (e.g. infra_signal_uptime, technology_maturity) for signals and migration.",
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
      description: "Secondary label (optional).",
    },
    {
      name: "body",
      type: "multilineText",
      description: "Description, bullet lists (one item per line), or maturity summary.",
    },
    {
      name: "extra",
      type: "multilineText",
      description: "Signal value, vendor examples, maturity level, or metric value.",
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
    tableName: INFRASTRUCTURE_PLATFORM_TABLE,
    masterTable: MASTER_TABLE,
    masterTableId: master.id,
    createdTable: false,
    tableId: null,
    fieldsCreated: [],
    fieldsSkipped: [],
    fieldsFailed: [],
    sectionOptions: SECTION_CHOICES.map((c) => c.name),
  };

  let platformTable = tables.find((t) => t.name === INFRASTRUCTURE_PLATFORM_TABLE);

  if (!platformTable) {
    const createBody = {
      name: INFRASTRUCTURE_PLATFORM_TABLE,
      description:
        "Infrastructure & Data Explorer rows (decision signals, technology stack, services, data domains, governance, analytics, maturity). Linked to Operator Setup - Master.",
      fields: [...scalarFieldSpecs(), operatorLinkField(master.id)],
    };

    if (!APPLY) {
      console.log(
        "WOULD CREATE TABLE",
        INFRASTRUCTURE_PLATFORM_TABLE,
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
  const outPath = path.join(reportsDir, `operator-infrastructure-platform-schema-${stamp}.json`);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("\nWrote", outPath);

  if (!APPLY) {
    console.log("\nDry run. Re-run with --apply to create table/fields in Airtable.");
  } else if (report.tableId) {
    console.log(
      `\nOptional env override: AIRTABLE_OPERATOR_SETUP_INFRASTRUCTURE_PLATFORM_TABLE=${INFRASTRUCTURE_PLATFORM_TABLE}`
    );
    console.log("Table id:", report.tableId);
  }

  if (report.fieldsFailed.length) process.exit(1);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
