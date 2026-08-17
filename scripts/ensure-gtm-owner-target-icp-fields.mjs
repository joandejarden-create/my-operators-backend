/**
 * Add ICP segmentation fields to GTM Owner Targets table.
 *
 *   node scripts/ensure-gtm-owner-target-icp-fields.mjs
 *   node scripts/ensure-gtm-owner-target-icp-fields.mjs --apply
 *
 * Report: reports/ensure-gtm-owner-target-icp-fields.json
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  GTM_OWNER_TARGET_TABLES,
  MAP_GTM_OWNER_TARGET,
  VAL_GTM_ICP_SEGMENT,
  VAL_GTM_DEAL_TRIGGER,
} from "../lib/gtm-owner-target/field-map.js";
import { assertGtmBaseConfigured, assertNotProductBase } from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}

function buildIcpFields() {
  return [
    {
      name: MAP_GTM_OWNER_TARGET.icpSegment,
      type: "singleSelect",
      options: choices(VAL_GTM_ICP_SEGMENT),
      description: "GTM ICP segment — separates asset owners from franchisors and low-fit SPVs.",
    },
    {
      name: MAP_GTM_OWNER_TARGET.strikeList,
      type: "checkbox",
      options: { icon: "check", color: "greenBright" },
      description: "Qualified for curated owner outreach strike list (auto-maintained by classify script).",
    },
    {
      name: MAP_GTM_OWNER_TARGET.dealTrigger,
      type: "singleSelect",
      options: choices(VAL_GTM_DEAL_TRIGGER),
      description: "Known deal motion if identified; default none_known.",
    },
    {
      name: MAP_GTM_OWNER_TARGET.icpClassificationNotes,
      type: "multilineText",
      description: "Machine-readable classification reasons (internal GTM only).",
    },
    {
      name: MAP_GTM_OWNER_TARGET.calaPropertyCount,
      type: "number",
      options: { precision: 0 },
      description: "Count of linked properties in CALA geography scope.",
    },
  ];
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
  const { apiKey, baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);

  const { res: listRes, json: listJson } = await metaFetch(baseId, apiKey, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed: ${JSON.stringify(listJson)}`);

  const table = (listJson.tables || []).find((t) => t.name === GTM_OWNER_TARGET_TABLES.ownerTargets);
  if (!table) {
    throw new Error(
      `Table "${GTM_OWNER_TARGET_TABLES.ownerTargets}" not found. Run ensure-gtm-owner-target-base.mjs first.`
    );
  }

  const existing = new Set((table.fields || []).map((f) => f.name));
  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    tableName: table.name,
    tableId: table.id,
    fieldsWouldCreate: [],
    fieldsCreated: [],
    fieldsSkipped: [],
    fieldsFailed: [],
  };

  for (const fieldSpec of buildIcpFields()) {
    if (existing.has(fieldSpec.name)) {
      report.fieldsSkipped.push(fieldSpec.name);
      console.log("SKIP", fieldSpec.name);
      continue;
    }
    if (!APPLY) {
      report.fieldsWouldCreate.push(fieldSpec.name);
      console.log("WOULD CREATE", fieldSpec.name);
      continue;
    }
    const { res, json } = await metaFetch(baseId, apiKey, `/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify(fieldSpec),
    });
    if (!res.ok) {
      report.fieldsFailed.push({ name: fieldSpec.name, error: json });
      console.error("FIELD FAILED", fieldSpec.name, JSON.stringify(json));
    } else {
      report.fieldsCreated.push(fieldSpec.name);
      console.log("CREATED", fieldSpec.name);
    }
  }

  const outPath = path.join(ROOT, "reports", "ensure-gtm-owner-target-icp-fields.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("Wrote", outPath);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
