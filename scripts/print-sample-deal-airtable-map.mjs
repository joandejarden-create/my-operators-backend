#!/usr/bin/env node
/**
 * Print Markdown Airtable field map for a sample-deal fixture.
 * Usage: node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/harborline-airport-amsterdam.example.json
 */
import fs from "node:fs";
import path from "node:path";
import {
  buildAirtableFieldMap,
  formatAirtableFieldMapMarkdown,
  validateSampleDealRecord,
} from "../lib/sample-opportunity-deal-schema.js";

const file = process.argv[2];
if (!file) {
  console.error("Usage: node scripts/print-sample-deal-airtable-map.mjs <fixture.json>");
  process.exit(1);
}

const record = JSON.parse(fs.readFileSync(path.resolve(file), "utf8"));
const validation = validateSampleDealRecord(record);
for (const w of validation.warnings) console.warn("WARN:", w);
if (!validation.ok) {
  for (const e of validation.errors) console.error("ERROR:", e);
  process.exit(1);
}

const rows = buildAirtableFieldMap(record);
console.log("# Sample deal Airtable map\n");
console.log(`**Sample ID:** ${record.meta?.sampleId || "—"}\n`);
console.log(record.disclaimer + "\n");
console.log("## Deals table (core intake)\n");
console.log(formatAirtableFieldMapMarkdown(rows));

if (record.targetListRows?.length) {
  console.log("\n## Target list / brand review candidates\n");
  console.log("| Brand | Parent | Why in review set | Source type |");
  console.log("| --- | --- | --- | --- |");
  for (const t of record.targetListRows) {
    console.log(
      `| ${t.brandName} | ${t.parentCompany || "—"} | ${String(t.whyInReviewSet || "").replace(/\|/g, "\\|")} | ${t.sourceType || "fictional_sample_assumption"} |`
    );
  }
}

if (record.intentionalGaps?.length) {
  console.log("\n## Intentional gaps (readiness demo)\n");
  console.log("| Field | Reason |");
  console.log("| --- | --- |");
  for (const g of record.intentionalGaps) {
    console.log(`| ${g.field} | ${String(g.reason || "").replace(/\|/g, "\\|")} |`);
  }
}
