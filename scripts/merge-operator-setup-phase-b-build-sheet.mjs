#!/usr/bin/env node
/**
 * Idempotently append Phase B rows to operator-setup-new-base-build-sheet-rows.json
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BUILD_PATH = path.join(ROOT, "api/lib/operator-setup-new-base-build-sheet-rows.json");
const PHASE_B_PATH = path.join(ROOT, "api/lib/operator-setup-new-base-phase-b-fields.json");

function rowKey(r) {
  return `${r.table_name}\0${r.form_name}\0${r.airtable_field_name}`;
}

function main() {
  const phaseB = JSON.parse(fs.readFileSync(PHASE_B_PATH, "utf8"));
  const build = JSON.parse(fs.readFileSync(BUILD_PATH, "utf8"));
  const rows = Array.isArray(build.rows) ? [...build.rows] : [];
  const seen = new Set(rows.map(rowKey));

  let added = 0;
  for (const r of phaseB.rows || []) {
    const entry = {
      table_name: r.table_name,
      form_name: r.form_name,
      airtable_field_name: r.airtable_field_name,
      airtable_type: r.airtable_type,
    };
    const k = rowKey(entry);
    if (seen.has(k)) continue;
    seen.add(k);
    rows.push(entry);
    added += 1;
  }

  build.phaseBMergedAt = new Date().toISOString();
  build.phaseBVersion = phaseB.phaseBVersion;
  build.rows = rows;
  fs.writeFileSync(BUILD_PATH, JSON.stringify(build, null, 2) + "\n", "utf8");
  console.log(`Phase B merge: added ${added} rows (${rows.length} total) → ${BUILD_PATH}`);
}

main();
