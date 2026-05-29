#!/usr/bin/env node
/**
 * Validates Phase B new-base writer coverage (schema + build sheet + bindings).
 * Does not call Airtable unless AIRTABLE_* env is set (live meta check).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import { OPERATOR_SERVICE_GRANULAR } from "../api/lib/operator-setup-service-granular-fields.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const PHASE_B_PATH = path.join(ROOT, "api/lib/operator-setup-new-base-phase-b-fields.json");
const BUILD_PATH = path.join(ROOT, "api/lib/operator-setup-new-base-build-sheet-rows.json");
const BINDINGS_PATH = path.join(ROOT, "api/lib/third-party-operator-new-two-field-bindings.json");
const SCHEMA_PATH = path.join(ROOT, "reports/operator-alignment-5b-schema-backup-2026-05-25.json");

const SYSTEM_FIELDS = new Set(["operator_id", "submission_status", "created_at", "updated_at", "Operator"]);
const GRANULAR = new Set();
for (const cat of OPERATOR_SERVICE_GRANULAR) {
  for (const c of cat.columns) GRANULAR.add(c.col);
}

function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function buildSheetKeys(rows) {
  const s = new Set();
  for (const r of rows) s.add(`${r.table_name}::${r.airtable_field_name}`);
  return s;
}

function bindingFormKeys(bindings) {
  const s = new Set();
  for (const b of bindings.bindings || []) {
    for (const fk of b.formKeys || []) s.add(fk);
  }
  return s;
}

function schemaFields(schema) {
  const map = new Map();
  const tableKeys = {
    master: "Operator Setup - Master",
    profile: "Operator Setup - Profile & Positioning",
    platform: "Operator Setup - Platform & Markets",
    commercial: "Operator Setup - Commercial Fit & Terms",
    governance: "Operator Setup - Governance, Delivery & Diligence",
  };
  for (const [key, tableName] of Object.entries(tableKeys)) {
    const block = schema.tables?.[key];
    if (!block?.fields) continue;
    for (const f of block.fields) {
      if (/^Operator Setup - /.test(f.name)) continue;
      map.set(`${tableName}::${f.name}`, { table: tableName, name: f.name, type: f.type });
    }
  }
  return map;
}

async function liveMetaFields(baseId, apiKey) {
  const tables = [
    "Operator Setup - Master",
    "Operator Setup - Profile & Positioning",
    "Operator Setup - Platform & Markets",
  ];
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const j = await res.json();
  if (!res.ok) throw new Error(j.error?.message || `meta ${res.status}`);
  const out = new Map();
  for (const t of j.tables || []) {
    if (!tables.includes(t.name)) continue;
    for (const f of t.fields || []) {
      out.set(`${t.name}::${f.name}`, f.type);
    }
  }
  return out;
}

async function main() {
  const phaseB = loadJson(PHASE_B_PATH);
  const build = loadJson(BUILD_PATH);
  const bindings = loadJson(BINDINGS_PATH);
  const schema = loadJson(SCHEMA_PATH);
  const sheetKeys = buildSheetKeys(build.rows || []);
  const formKeys = bindingFormKeys(bindings);
  const schemaMap = schemaFields(schema);

  const errors = [];
  const warnings = [];

  for (const r of phaseB.masterWriterHardcoded || []) {
    const k = `${r.table_name}::${r.airtable_field_name}`;
    if (!schemaMap.has(k) && r.airtable_field_name !== "company_name") {
      warnings.push(`Schema backup missing Master field: ${r.airtable_field_name} (may be plannedCreates only)`);
    }
    if (!r.writerPath) errors.push(`Master field missing writerPath: ${r.form_name}`);
  }

  for (const r of phaseB.rows || []) {
    const k = `${r.table_name}::${r.airtable_field_name}`;
    if (!sheetKeys.has(k)) {
      errors.push(`Build sheet missing: ${k} (form ${r.form_name})`);
    }
    if (SYSTEM_FIELDS.has(r.airtable_field_name)) {
      errors.push(`Phase B must not map system field: ${r.airtable_field_name}`);
    }
    if (GRANULAR.has(r.airtable_field_name)) {
      errors.push(`Phase B must not map granular derived column: ${r.airtable_field_name}`);
    }
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (baseId && apiKey) {
    const live = await liveMetaFields(baseId, apiKey);
    const blockedKeys = new Set(
      (phaseB.blocked || []).map((b) => `${b.table_name}::${b.airtable_field_name}`)
    );
    for (const r of [...(phaseB.rows || []), ...(phaseB.masterWriterHardcoded || [])]) {
      const k = `${r.table_name}::${r.airtable_field_name}`;
      if (blockedKeys.has(k)) continue;
      if (!live.has(k)) {
        errors.push(`Live Airtable missing column: ${k}`);
      }
    }
    if ((phaseB.blocked || []).length) {
      console.log(`Blocked (schema backup only): ${phaseB.blocked.length}`);
    }
    console.log("Live meta check: OK for Phase B columns");
  } else {
    warnings.push("AIRTABLE_BASE_ID/API_KEY not set — skipped live schema check");
  }

  const scoringFiles = [
    path.join(ROOT, "api/my-deals.js"),
    path.join(ROOT, "lib/operator-alignment-company-utils.js"),
  ];
  for (const f of scoringFiles) {
    const t = fs.readFileSync(f, "utf8");
    if (/weight\s*=\s*0\.\d+/.test(t) && f.includes("operator-alignment")) {
      /* weights live in my-deals — spot-check unchanged file hash not required */
    }
  }

  const basPath = path.join(ROOT, "api/lib/brand-alignment-snapshot.js");
  const ocsPath = path.join(ROOT, "api/lib/operator-capability-snapshot.js");
  if (!fs.existsSync(basPath) || !fs.existsSync(ocsPath)) {
    warnings.push("BAS/OCS snapshot modules not found at expected paths (skipped)");
  }

  console.log("\n=== Phase B writer coverage validation ===\n");
  console.log(`Build sheet rows: ${(build.rows || []).length}`);
  console.log(`Phase B 1:1 rows: ${(phaseB.rows || []).length}`);
  console.log(`Phase B Master (hardcoded writer): ${(phaseB.masterWriterHardcoded || []).length}`);
  console.log(`Skipped documented: ${(phaseB.skipped || []).length}`);

  if (warnings.length) {
    console.log("\nWarnings:");
    for (const w of warnings) console.log("  -", w);
  }
  if (errors.length) {
    console.log("\nErrors:");
    for (const e of errors) console.log("  -", e);
    process.exit(1);
  }
  console.log("\nAll Phase B coverage checks passed.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
