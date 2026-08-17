#!/usr/bin/env node
/**
 * Create Operator Materials Explorer fields on Governance table (if missing).
 *
 *   node scripts/ensure-operator-materials-explorer-schema.mjs --apply
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { OPERATOR_MATERIALS_EXPLORER_AIRTABLE_FIELD_SPECS } from "../api/lib/operator-materials-explorer-field-map.js";
import { NEW_BASE_GOVERNANCE_TABLE } from "../api/lib/operator-setup-new-base-read.js";

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
  const governance = tables.find((t) => t.name === NEW_BASE_GOVERNANCE_TABLE);
  if (!governance) throw new Error(`Table not found: ${NEW_BASE_GOVERNANCE_TABLE}`);

  const stamp = new Date().toISOString().slice(0, 10);
  if (!fs.existsSync(REPORTS)) fs.mkdirSync(REPORTS, { recursive: true });

  if (!APPLY) {
    for (const spec of OPERATOR_MATERIALS_EXPLORER_AIRTABLE_FIELD_SPECS) {
      const exists = (governance.fields || []).some((f) => f.name === spec.name);
      console.log(exists ? "SKIP" : "WOULD CREATE", spec.name, `(${spec.type})`);
    }
    console.log("\nDry run. Re-run with --apply.");
    return;
  }

  const results = { created: [], skipped: [], failed: [] };
  for (const spec of OPERATOR_MATERIALS_EXPLORER_AIRTABLE_FIELD_SPECS) {
    const exists = (governance.fields || []).some((f) => f.name === spec.name);
    if (exists) {
      results.skipped.push(spec.name);
      console.log("SKIP", spec.name);
      continue;
    }
    const { ok, status, json } = await createField(baseId, apiKey, governance.id, spec);
    if (ok) {
      results.created.push(spec.name);
      governance.fields = [...(governance.fields || []), { name: spec.name }];
      console.log("CREATED", spec.name);
    } else {
      results.failed.push({ name: spec.name, status, error: json });
      console.error("FAILED", spec.name, status, JSON.stringify(json));
    }
    await new Promise((r) => setTimeout(r, 220));
  }

  const outPath = path.join(REPORTS, `operator-materials-explorer-schema-apply-${stamp}.json`);
  fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
  console.log("Wrote", outPath);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
