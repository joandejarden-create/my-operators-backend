/**
 * Dump Pilot Target List table schema (field types + select options).
 * Report: reports/pilot-target-list-schema-export.json
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  GTM_PILOT_TARGET_LIST_TABLE,
} from "../lib/gtm-owner-target/pilot-target-list-field-map.js";
import { assertGtmBaseConfigured, assertNotProductBase } from "../lib/gtm-owner-target/platform-base.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT = join(__dirname, "..", "reports", "pilot-target-list-schema-export.json");

async function metaFetch(baseId, token, metaPath) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${metaPath}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json();
  return { res, json };
}

async function main() {
  const { apiKey, baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);

  const { res, json } = await metaFetch(baseId, apiKey, "/tables");
  if (!res.ok) throw new Error(`Meta API failed: ${JSON.stringify(json)}`);

  const table = (json.tables || []).find((t) => t.name === GTM_PILOT_TARGET_LIST_TABLE);
  if (!table) {
    throw new Error(`Table "${GTM_PILOT_TARGET_LIST_TABLE}" not found in base ${baseId}`);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    baseId,
    tableName: table.name,
    tableId: table.id,
    fields: (table.fields || []).map((f) => ({
      id: f.id,
      name: f.name,
      type: f.type,
      options: f.options || null,
    })),
  };

  mkdirSync(dirname(REPORT), { recursive: true });
  writeFileSync(REPORT, JSON.stringify(report, null, 2));
  console.log("Wrote", REPORT);
  console.log(table.name, table.id, (table.fields || []).length, "fields");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
