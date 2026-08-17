#!/usr/bin/env node
/**
 * Create Operator Explorer DNA JSON columns on new-base tables (if missing).
 *
 *   node scripts/ensure-operator-dna-explorer-json-schema.mjs
 *   node scripts/ensure-operator-dna-explorer-json-schema.mjs --apply
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { DNA_EXPLORER_JSON_AIRTABLE_SPECS } from "../lib/operator-dna-explorer-json-fields.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");
const APPLY = process.argv.includes("--apply");

async function fetchTables(baseId, apiKey) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const json = await res.json();
  if (!res.ok) throw new Error(json.error?.message || `meta ${res.status}`);
  return json.tables || [];
}

async function createField(baseId, apiKey, tableId, spec) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables/${encodeURIComponent(tableId)}/fields`;
  const res = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ name: spec.name, type: spec.type }),
  });
  const json = await res.json();
  return { ok: res.ok, status: res.status, json };
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const tables = await fetchTables(baseId, apiKey);
  const byName = new Map(tables.map((t) => [t.name, t]));
  const stamp = new Date().toISOString().slice(0, 10);
  if (!fs.existsSync(REPORTS)) fs.mkdirSync(REPORTS, { recursive: true });

  const results = { created: [], skipped: [], failed: [], missingTable: [] };

  for (const spec of DNA_EXPLORER_JSON_AIRTABLE_SPECS) {
    const table = byName.get(spec.table);
    if (!table) {
      results.missingTable.push({ field: spec.name, table: spec.table });
      console.log("NO TABLE", spec.table, "for", spec.name);
      continue;
    }
    const exists = (table.fields || []).some((f) => f.name === spec.name);
    if (exists) {
      results.skipped.push(spec.name);
      console.log("SKIP", spec.name, `@ ${spec.table}`);
      continue;
    }
    if (!APPLY) {
      console.log("WOULD CREATE", spec.name, `@ ${spec.table}`, `(${spec.type})`);
      continue;
    }
    const { ok, status, json } = await createField(baseId, apiKey, table.id, spec);
    if (ok) {
      results.created.push(spec.name);
      table.fields = [...(table.fields || []), { name: spec.name }];
      console.log("CREATED", spec.name, `@ ${spec.table}`);
    } else {
      results.failed.push({ name: spec.name, table: spec.table, status, error: json });
      console.error("FAILED", spec.name, status, JSON.stringify(json));
    }
    await new Promise((r) => setTimeout(r, 220));
  }

  const outPath = path.join(REPORTS, `operator-dna-explorer-json-schema-apply-${stamp}.json`);
  fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
  console.log("\nWrote", outPath);
  if (!APPLY) console.log("Dry run. Re-run with --apply to create fields in Airtable.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
