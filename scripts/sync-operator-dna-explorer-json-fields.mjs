#!/usr/bin/env node
/**
 * Adds DNA Explorer JSON fields to new-base build sheet + third-party-operator-new-two-field-bindings.json.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { DNA_EXPLORER_JSON_FIELD_SPECS } from "../lib/operator-dna-explorer-json-fields.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BUILD_SHEET_PATH = path.join(ROOT, "api/lib/operator-setup-new-base-build-sheet-rows.json");
const BINDINGS_PATH = path.join(ROOT, "api/lib/third-party-operator-new-two-field-bindings.json");

function syncBuildSheet() {
  const raw = JSON.parse(fs.readFileSync(BUILD_SHEET_PATH, "utf8"));
  const rows = Array.isArray(raw.rows) ? raw.rows : [];
  const have = new Set(rows.map((r) => r.form_name));
  let added = 0;
  for (const spec of DNA_EXPLORER_JSON_FIELD_SPECS) {
    if (have.has(spec.formKey)) continue;
    rows.push({
      table_name: spec.airtableTable,
      form_name: spec.formKey,
      airtable_field_name: spec.formKey,
      airtable_type: "longText",
    });
    have.add(spec.formKey);
    added += 1;
  }
  raw.rows = rows;
  raw.dnaExplorerJsonSyncedAt = new Date().toISOString();
  fs.writeFileSync(BUILD_SHEET_PATH, JSON.stringify(raw, null, 2) + "\n");
  return added;
}

function syncBindings() {
  const raw = JSON.parse(fs.readFileSync(BINDINGS_PATH, "utf8"));
  const bindings = Array.isArray(raw.bindings) ? raw.bindings : [];
  const have = new Set();
  for (const b of bindings) {
    for (const fk of b.formKeys || []) have.add(fk);
  }
  let added = 0;
  for (const spec of DNA_EXPLORER_JSON_FIELD_SPECS) {
    if (have.has(spec.formKey)) continue;
    bindings.push({
      formKeys: [spec.formKey],
      airtableName: spec.formKey,
      tableKey: spec.tableKey,
      fieldType: "multilineText",
      selectOptions: null,
    });
    have.add(spec.formKey);
    added += 1;
  }
  raw.bindings = bindings;
  fs.writeFileSync(BINDINGS_PATH, JSON.stringify(raw, null, 2) + "\n");
  return added;
}

const buildAdded = syncBuildSheet();
const bindAdded = syncBindings();
console.log("Build sheet rows added:", buildAdded);
console.log("Bindings added:", bindAdded);
console.log("Total DNA JSON fields:", DNA_EXPLORER_JSON_FIELD_SPECS.length);
