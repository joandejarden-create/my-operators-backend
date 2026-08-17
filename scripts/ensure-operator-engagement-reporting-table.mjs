/**
 * Create / verify Airtable table "Operator Setup - Engagement & Reporting" (Metadata API).
 *
 *   node scripts/ensure-operator-engagement-reporting-table.mjs
 *   node scripts/ensure-operator-engagement-reporting-table.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  ENGAGEMENT_REPORTING_TABLE,
  ENGAGEMENT_REPORTING_SECTIONS,
} from "../api/lib/operator-engagement-reporting-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const MASTER_TABLE =
  process.env.OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";

const APPLY = process.argv.includes("--apply");

const SECTION_CHOICES = Object.values(ENGAGEMENT_REPORTING_SECTIONS).map((name) => ({
  name,
}));

function scalarFieldSpecs() {
  return [
    {
      name: "title",
      type: "singleLineText",
      description: "Card title, report name, engagement type, or lifecycle stage.",
    },
    {
      name: "section",
      type: "singleSelect",
      description: "Which Engagement & Reporting subsection this row belongs to.",
      options: { choices: SECTION_CHOICES },
    },
    {
      name: "row_key",
      type: "singleLineText",
      description: "Stable key (e.g. ov_card_discipline, owner_reporting_level).",
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
      description: "Cadence label (Weekly, Monthly) or secondary label.",
    },
    {
      name: "body",
      type: "multilineText",
      description: "Description, focus text, or owner value card copy.",
    },
    {
      name: "extra",
      type: "singleLineText",
      description: "Signal value (reporting level, governance cadence).",
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
  if (!master) throw new Error(`Master table not found: "${MASTER_TABLE}"`);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    tableName: ENGAGEMENT_REPORTING_TABLE,
    masterTable: MASTER_TABLE,
    masterTableId: master.id,
    createdTable: false,
    tableId: null,
    fieldsCreated: [],
    fieldsSkipped: [],
    fieldsFailed: [],
    sectionOptions: SECTION_CHOICES.map((c) => c.name),
  };

  let platformTable = tables.find((t) => t.name === ENGAGEMENT_REPORTING_TABLE);

  if (!platformTable) {
    const createBody = {
      name: ENGAGEMENT_REPORTING_TABLE,
      description:
        "Owner Engagement & Reporting Explorer rows (value pillars, cadence, reports, tools, lifecycle). Linked to Operator Setup - Master.",
      fields: [...scalarFieldSpecs(), operatorLinkField(master.id)],
    };

    if (!APPLY) {
      console.log("WOULD CREATE TABLE", ENGAGEMENT_REPORTING_TABLE);
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
        console.log("WOULD CREATE FIELD", spec.name);
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
  const outPath = path.join(reportsDir, `operator-engagement-reporting-schema-${stamp}.json`);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("\nWrote", outPath);

  if (!APPLY) {
    console.log("\nDry run. Re-run with --apply to create table/fields in Airtable.");
  }

  if (report.fieldsFailed.length) process.exit(1);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
