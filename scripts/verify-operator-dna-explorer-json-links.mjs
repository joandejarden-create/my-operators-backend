#!/usr/bin/env node
/**
 * Verify all 22 DNA JSON keys reach prefill for each linked Master (read-path / tab link check).
 *
 *   node scripts/verify-operator-dna-explorer-json-links.mjs
 *   node scripts/verify-operator-dna-explorer-json-links.mjs recXXXXXXXX
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { DNA_EXPLORER_JSON_FIELD_SPECS } from "../lib/operator-dna-explorer-json-fields.js";
import { DNA_JSON_FIELD_TO_TAB } from "../lib/operator-dna-explorer-json-seed-data.js";
import {
  NEW_BASE_MASTER_TABLE,
  fetchAllRecordsRest,
  loadNewBaseOperatorBundle,
  buildPrefillObjectFromNewBaseRows,
} from "../api/lib/operator-setup-new-base-read.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

async function main() {
  const filterId = process.argv[2] || null;
  const masters = await fetchAllRecordsRest(NEW_BASE_MASTER_TABLE);
  const rows = [];

  for (const master of masters) {
    if (filterId && master.id !== filterId) continue;
    const bundle = await loadNewBaseOperatorBundle(master.id);
    if (!bundle) continue;
    const prefill = buildPrefillObjectFromNewBaseRows(
      bundle.master,
      bundle.profile,
      bundle.platform,
      bundle.commercial,
      bundle.governance
    );
    const missing = [];
    for (const spec of DNA_EXPLORER_JSON_FIELD_SPECS) {
      if (!nz(prefill[spec.formKey])) {
        missing.push({
          formKey: spec.formKey,
          dnaTab: DNA_JSON_FIELD_TO_TAB[spec.formKey],
          table: spec.airtableTable,
        });
      }
    }
    const name =
      nz(bundle.master?.fields?.company_name) ||
      nz(bundle.master?.fields?.["Company Name"]) ||
      master.id;
    rows.push({ masterId: master.id, companyName: name, ok: missing.length === 0, missing });
    console.log(missing.length ? "FAIL" : "OK", name, missing.length ? missing.map((m) => m.formKey).join(", ") : "all 22");
  }

  const stamp = new Date().toISOString().slice(0, 10);
  const outPath = path.join(REPORTS, `operator-dna-explorer-json-verify-${stamp}.json`);
  fs.mkdirSync(REPORTS, { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify({ rows }, null, 2));
  console.log("Wrote", outPath);
  const fail = rows.filter((r) => !r.ok).length;
  if (fail) process.exitCode = 1;
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
