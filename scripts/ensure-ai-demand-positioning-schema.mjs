#!/usr/bin/env node
/**
 * Ensure AI Demand Positioning Airtable schema.
 *
 *   node scripts/ensure-ai-demand-positioning-schema.mjs --dry-run
 *   ADP_SCHEMA_APPLY=true node scripts/ensure-ai-demand-positioning-schema.mjs --apply
 */

import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  ADP_PUBLISHED_REPORTS_TABLE,
  ADP_HOTEL_CENSUS_TABLE,
  getPublishedReportFieldSpecs,
  classifyFieldEnsureAction,
} from "../lib/ai-demand-positioning/airtable-schema-proposal.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;
const REPORT_PATH = path.join(ROOT, "reports", "ensure-ai-demand-positioning-schema.json");

async function metaFetch(baseId, token, pathSuffix, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}${pathSuffix}`;
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

function findTable(tables, name) {
  return (tables || []).find((t) => t.name === name) || null;
}

function fieldByName(table, name) {
  return (table?.fields || []).find((f) => f.name === name) || null;
}

function toCreatePayload(spec, linkedTableId = null) {
  if (spec.type === "multipleRecordLinks") {
    if (!linkedTableId) return null;
    return { name: spec.name, type: "multipleRecordLinks", options: { linkedTableId } };
  }
  const payload = { name: spec.name, type: spec.type };
  if (spec.description) payload.description = spec.description;
  if (spec.options) payload.options = spec.options;
  return payload;
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.ADP_AIRTABLE_BASE_ID || process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const report = {
    dryRun: DRY,
    table: ADP_PUBLISHED_REPORTS_TABLE,
    censusTable: ADP_HOTEL_CENSUS_TABLE,
    fieldsToCreate: [],
    fieldsConflict: [],
    fieldsSkipped: [],
    tableCreated: false,
  };

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    console.error("Meta API error:", json);
    process.exit(1);
  }

  let table = findTable(json.tables, ADP_PUBLISHED_REPORTS_TABLE);
  const censusTable = findTable(json.tables, ADP_HOTEL_CENSUS_TABLE);

  if (!table && APPLY && process.env.ADP_SCHEMA_APPLY === "true") {
    const createRes = await metaFetch(baseId, token, "/tables", {
      method: "POST",
      body: JSON.stringify({
        name: ADP_PUBLISHED_REPORTS_TABLE,
        fields: getPublishedReportFieldSpecs(false)
          .filter((s) => s.type !== "multipleRecordLinks")
          .slice(0, 3)
          .map((s) => toCreatePayload(s)),
      }),
    });
    if (!createRes.res.ok) {
      console.error("Table create failed:", createRes.json);
      process.exit(1);
    }
    report.tableCreated = true;
    table = createRes.json;
  }

  const linkIds = { [ADP_HOTEL_CENSUS_TABLE]: censusTable?.id || null };
  const fieldSpecs = getPublishedReportFieldSpecs(true);

  for (const spec of fieldSpecs) {
    const existing = fieldByName(table, spec.name);
    const classification = classifyFieldEnsureAction(existing, spec);
    if (classification.action === "conflict") {
      report.fieldsConflict.push({ name: spec.name, reason: classification.reason });
      continue;
    }
    if (classification.action === "skip") {
      report.fieldsSkipped.push(spec.name);
      continue;
    }
    report.fieldsToCreate.push(spec.name);
    if (APPLY && process.env.ADP_SCHEMA_APPLY === "true" && table?.id) {
      const payload = toCreatePayload(spec, linkIds[spec.linkedTableName]);
      if (!payload) {
        report.fieldsConflict.push({ name: spec.name, reason: "linked table missing" });
        continue;
      }
      const fieldRes = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
        method: "POST",
        body: JSON.stringify(payload),
      });
      if (!fieldRes.res.ok) {
        report.fieldsConflict.push({ name: spec.name, reason: JSON.stringify(fieldRes.json) });
      }
    }
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));

  console.log(`\n=== Ensure ADP Schema ===`);
  console.log(`Mode: ${DRY ? "DRY RUN" : "APPLY"}`);
  console.log(`Table: ${ADP_PUBLISHED_REPORTS_TABLE} ${table ? "(exists)" : "(missing)"}`);
  console.log(`Census table ${ADP_HOTEL_CENSUS_TABLE}: ${censusTable ? censusTable.id : "not found"}`);
  console.log(`Fields to create: ${report.fieldsToCreate.length}`);
  console.log(`Report: ${REPORT_PATH}`);
  if (DRY) console.log("\nApply requires ADP_SCHEMA_APPLY=true and --apply");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
