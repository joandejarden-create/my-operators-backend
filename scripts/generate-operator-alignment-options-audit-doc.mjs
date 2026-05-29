#!/usr/bin/env node
/**
 * Generate docs/operator-alignment-airtable-options-audit.md from live export.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadLiveOptionsFromFile, comparePlannedToLive } from "../lib/operator-alignment-airtable-options-loader.js";
import { OAS_AUDIT_FIELD_SPECS, OAS_AUDIT_TABLES } from "../lib/operator-alignment-airtable-options-registry.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const live = loadLiveOptionsFromFile();
if (!live) throw new Error("Run export-operator-alignment-live-airtable-options.mjs first");

const lines = [
  "# Operator Alignment — Airtable Options Audit",
  "",
  "**Generated:** " + new Date().toISOString(),
  "",
  "**Live export:** `reports/operator-alignment-live-airtable-options.json`",
  "",
  "## Summary",
  "",
];

let exact = 0;
let partial = 0;
let extra = 0;
let legacy = 0;

const tableRows = [];

for (const spec of OAS_AUDIT_FIELD_SPECS) {
  const key = `${spec.tableKey}::${spec.fieldName}`;
  const entry = live.fields[key] || {};
  const planned = spec.plannedOptions || [];
  const liveOpts = entry.liveOptions || [];
  const cmp = comparePlannedToLive(planned, liveOpts);
  let risk = "Low";
  let action = "None";
  if (entry.status === "field_missing") {
    risk = "High";
    action = "Create field or fix registry name";
  } else if (!planned.length && liveOpts.length) {
    legacy += 1;
    risk = "Medium — legacy field; use structured parallel for scoring";
    action = "Do not write new values without mapping; scoring uses structured fields first";
  } else if (cmp.matchStatus === "Partial") {
    partial += 1;
    risk = "High — planned vs live mismatch";
    action = "Sync `operator-alignment-field-options.js` and UI to live labels";
  } else if (cmp.matchStatus === "Exact") {
    exact += 1;
  } else if (cmp.matchStatus === "Extra" && planned.length) {
    extra += 1;
  }

  tableRows.push({
    table: OAS_AUDIT_TABLES[spec.tableKey],
    field: spec.fieldName,
    type: entry.fieldType || entry.status,
    planned: planned.length ? planned.join("; ") : "—",
    live: liveOpts.length ? liveOpts.join("; ") : "—",
    status: planned.length ? cmp.matchStatus : entry.status === "ok" ? "Legacy/Extra" : entry.status,
    risk,
    action,
  });
}

lines.push(`| Metric | Count |`);
lines.push(`|--------|------:|`);
lines.push(`| Phase 5B fields — Exact match | ${exact} |`);
lines.push(`| Phase 5B fields — Partial | ${partial} |`);
lines.push(`| Legacy fields (extra live options) | ${legacy} |`);
lines.push("");
lines.push("## Field audit");
lines.push("");
lines.push("| Table | Field Name | Field Type | Planned Options | Live Airtable Options | Match Status | Risk | Recommended Action |");
lines.push("|-------|------------|------------|-----------------|----------------------|--------------|------|-------------------|");

for (const r of tableRows) {
  const esc = (s) => String(s).replace(/\|/g, "\\|").slice(0, 120);
  lines.push(
    `| ${esc(r.table)} | ${esc(r.field)} | ${esc(r.type)} | ${esc(r.planned)} | ${esc(r.live)} | ${r.status} | ${esc(r.risk)} | ${esc(r.action)} |`
  );
}

lines.push("");
lines.push("## Alias mappings");
lines.push("");
lines.push("See `lib/operator-alignment-airtable-option-aliases.js` — aliases resolve to **live** labels at runtime via `normalizeAirtableSelectValue` / `scoringCanonicalize`.");
lines.push("");
lines.push("## Backfill safety rules");
lines.push("");
lines.push("1. Run `node scripts/export-operator-alignment-live-airtable-options.mjs` before backfill.");
lines.push("2. Backfill validates every value with `validateProposalValue(..., liveIndex)` — writes **exact** live labels only.");
lines.push("3. Dry-run fails on unmapped values; does not create new Airtable options.");
lines.push("4. `--overwrite` still respects live option validation.");
lines.push("");
lines.push("## Remaining risks");
lines.push("");
lines.push("- **Legacy** `Must-Haves From Brand/Operator`, `Services Required From Operator`, `Preferred Deal Structure` have large legacy option sets — scoring reads structured fields first.");
lines.push("- **Operator `Offered Services`** must use same labels as deal service multis for overlap (live options are exact match with deal service fields).");
lines.push("- **chainScalesSupported** live uses `Independent` (not `Independent / Boutique`) — UI/planned list synced 2026-05-25.");
lines.push("");

const out = path.join(ROOT, "docs", "operator-alignment-airtable-options-audit.md");
fs.writeFileSync(out, lines.join("\n"));
console.log("Wrote", out);
