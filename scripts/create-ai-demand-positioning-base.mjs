#!/usr/bin/env node
/**
 * Create dedicated Airtable base for AI Demand Positioning.
 *
 *   node scripts/create-ai-demand-positioning-base.mjs --dry-run
 *   ADP_BASE_CREATE_APPLY=true node scripts/create-ai-demand-positioning-base.mjs --apply
 *
 * Requires AIRTABLE_API_KEY (or AIRTABLE_PAT) with schema.bases:write scope.
 * Writes report to reports/create-ai-demand-positioning-base.json
 */

import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { ADP_PUBLISHED_REPORT_CORE_FIELD_SPECS } from "../lib/ai-demand-positioning/airtable-schema-proposal.js";
import { ADP_PUBLISHED_REPORTS_TABLE } from "../lib/ai-demand-positioning/airtable-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT_PATH = path.join(ROOT, "reports", "create-ai-demand-positioning-base.json");
const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;
const BASE_NAME = process.env.ADP_AIRTABLE_BASE_NAME || "AI Demand Positioning";

function getToken() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

async function metaFetch(pathSuffix, init = {}) {
  const token = getToken();
  const res = await fetch(`https://api.airtable.com/v0/meta${pathSuffix}`, {
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

function specToCreateField(spec) {
  const payload = { name: spec.name, type: spec.type };
  if (spec.description) payload.description = spec.description;
  if (spec.options) payload.options = spec.options;
  return payload;
}

function buildInitialTableFields() {
  return ADP_PUBLISHED_REPORT_CORE_FIELD_SPECS.map(specToCreateField);
}

async function resolveWorkspaceId() {
  if (process.env.ADP_AIRTABLE_WORKSPACE_ID) {
    return process.env.ADP_AIRTABLE_WORKSPACE_ID;
  }
  const seedBaseId = process.env.AIRTABLE_BASE_ID;
  if (!seedBaseId) {
    throw new Error("Set AIRTABLE_BASE_ID or ADP_AIRTABLE_WORKSPACE_ID to resolve workspace.");
  }
  const { res, json } = await metaFetch(`/bases/${seedBaseId}`);
  if (!res.ok) {
    throw new Error(`Could not resolve workspaceId from base ${seedBaseId}: ${JSON.stringify(json)}`);
  }
  return json.workspaceId;
}

async function findExistingBaseByName(name) {
  const { res, json } = await metaFetch("/bases");
  if (!res.ok) throw new Error(JSON.stringify(json));
  return (json.bases || []).find((b) => b.name === name) || null;
}

async function main() {
  const token = getToken();
  if (!token) {
    console.error("Set AIRTABLE_API_KEY or AIRTABLE_PAT with schema.bases:write scope.");
    process.exit(1);
  }

  const workspaceId = await resolveWorkspaceId();
  const existing = await findExistingBaseByName(BASE_NAME);
  const report = {
    dryRun: DRY,
    baseName: BASE_NAME,
    workspaceId,
    tableName: ADP_PUBLISHED_REPORTS_TABLE,
    existingBaseId: existing?.id || null,
    censusLinkField: "Census Record ID",
    censusLinkNote:
      "Text rec… from Hotel Property Census. Cross-base link until census consolidation; native linked-record field deferred.",
    fields: ADP_PUBLISHED_REPORT_CORE_FIELD_SPECS.map((f) => f.name),
    createdBaseId: null,
    tableId: null,
  };

  console.log(`\n=== Create ADP Airtable Base ===`);
  console.log(`Base name: ${BASE_NAME}`);
  console.log(`Workspace: ${workspaceId}`);
  console.log(`Mode: ${DRY ? "DRY RUN" : "APPLY"}`);
  console.log(`Census link column: Census Record ID (text rec…)`);

  if (existing) {
    console.log(`\nBase already exists: ${existing.id}`);
    report.createdBaseId = existing.id;
    const { res, json } = await metaFetch(`/bases/${existing.id}`);
    if (res.ok) {
      const table = (json.tables || []).find((t) => t.name === ADP_PUBLISHED_REPORTS_TABLE);
      report.tableId = table?.id || null;
    }
    fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
    fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));
    console.log(`Report: ${REPORT_PATH}`);
    console.log(`\nSet in .env: ADP_AIRTABLE_BASE_ID=${existing.id}`);
    return;
  }

  if (DRY) {
    console.log("\n[DRY RUN] Would create base with table and fields:");
    console.log(JSON.stringify({ name: BASE_NAME, workspaceId, tables: [{ name: ADP_PUBLISHED_REPORTS_TABLE, fieldCount: report.fields.length }] }, null, 2));
    fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
    fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));
    console.log(`\nApply with: ADP_BASE_CREATE_APPLY=true node scripts/create-ai-demand-positioning-base.mjs --apply`);
    return;
  }

  if (process.env.ADP_BASE_CREATE_APPLY !== "true") {
    console.error("\nBlocked: set ADP_BASE_CREATE_APPLY=true to create the base.");
    process.exit(1);
  }

  const createBody = {
    name: BASE_NAME,
    workspaceId,
    tables: [
      {
        name: ADP_PUBLISHED_REPORTS_TABLE,
        description: "Published AI Demand Positioning snapshots for owner UI. Raw monitoring stays off-Airtable.",
        fields: buildInitialTableFields(),
      },
    ],
  };

  const { res, json } = await metaFetch("/bases", {
    method: "POST",
    body: JSON.stringify(createBody),
  });

  if (!res.ok) {
    console.error("Create base failed:", JSON.stringify(json, null, 2));
    process.exit(1);
  }

  report.createdBaseId = json.id;
  const table = (json.tables || []).find((t) => t.name === ADP_PUBLISHED_REPORTS_TABLE);
  report.tableId = table?.id || null;

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));

  console.log(`\nCreated base: ${json.id}`);
  console.log(`Table: ${ADP_PUBLISHED_REPORTS_TABLE} (${report.tableId})`);
  console.log(`Report: ${REPORT_PATH}`);
  console.log(`\nAdd to .env:`);
  console.log(`ADP_AIRTABLE_BASE_ID=${json.id}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
