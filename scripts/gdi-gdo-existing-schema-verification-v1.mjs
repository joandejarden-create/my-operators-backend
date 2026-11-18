#!/usr/bin/env node
/**
 * Verify live GDO Airtable schema already contains CD-3 / Weekly Change columns.
 * Read-only. No schema mutations. No record writes.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_OPPORTUNITIES_TABLE_NAME,
  MAP_GDI_OPPORTUNITY as F,
} from "../lib/group-demand-intelligence/opportunity-field-map.js";
import { getGdiOpportunitiesAirtableBaseId } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-commercial-data-model-cd-implementation-v1",
);

const REQUIRED_EXISTING_FIELDS = Object.freeze([
  F.dgGeneratorId,
  F.dgProgramId,
  F.dgSignalId,
  F.hotelGeneratorFitId,
  F.dgGeneratorLink,
  F.dgProgramLink,
  F.dgSignalLink,
  F.dgSignalsLink,
  F.firstDiscoveredAt,
  F.firstDiscoveredRunId,
  F.lastMaterialChangeAt,
  F.lastMaterialChangeRunId,
  F.opportunityPayloadJson,
  F.opportunityId,
  F.hotelId,
]);

function ensureOut() {
  fs.mkdirSync(OUT, { recursive: true });
}

async function fetchLiveFields(baseId, token) {
  const res = await fetch(
    `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`,
    { headers: { Authorization: `Bearer ${token}` } },
  );
  const body = await res.json();
  if (!res.ok) {
    throw new Error(`Airtable meta failed: ${res.status} ${JSON.stringify(body)}`);
  }
  const table = (body.tables || []).find(
    (t) => t.name === GDI_OPPORTUNITIES_TABLE_NAME,
  );
  if (!table) {
    throw new Error(`Missing table ${GDI_OPPORTUNITIES_TABLE_NAME}`);
  }
  return {
    tableId: table.id,
    fields: (table.fields || []).map((f) => ({
      name: f.name,
      type: f.type,
      id: f.id,
    })),
  };
}

function loadInventoryFallback() {
  const csvPath = path.join(
    ROOT,
    "reports/gdi-airtable-complete-data-model-audit-v1/02-AIRTABLE_FIELD_INVENTORY.csv",
  );
  if (!fs.existsSync(csvPath)) return null;
  const lines = fs.readFileSync(csvPath, "utf8").split(/\r?\n/);
  const fields = [];
  for (const line of lines) {
    if (!line.includes("Group Demand Opportunities")) continue;
    const parts = line.split(",");
    if (parts[1] !== "Group Demand Opportunities") continue;
    fields.push({ name: parts[3], type: parts[4], id: parts[2] });
  }
  return { tableId: "tblRuReslJMwsfRQj", fields, source: "inventory_csv" };
}

async function main() {
  ensureOut();
  const baseId = getGdiOpportunitiesAirtableBaseId();
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
  let live = null;
  let source = "live_meta";
  try {
    if (!token) throw new Error("no_token");
    live = await fetchLiveFields(baseId, token);
  } catch (err) {
    live = loadInventoryFallback();
    source = live?.source || "unavailable";
    if (!live) throw err;
  }

  const byName = new Map(live.fields.map((f) => [f.name, f]));
  const results = REQUIRED_EXISTING_FIELDS.map((name) => {
    const hit = byName.get(name);
    return {
      field: name,
      present: Boolean(hit),
      type: hit?.type || null,
      fieldId: hit?.id || null,
      action: hit ? "REUSE_EXISTING" : "MISSING_UNEXPECTED",
    };
  });
  const missing = results.filter((r) => !r.present);
  const report = {
    generatedAt: new Date().toISOString(),
    baseId,
    tableName: GDI_OPPORTUNITIES_TABLE_NAME,
    tableId: live.tableId,
    source,
    fieldCount: live.fields.length,
    requiredChecks: results,
    missingCount: missing.length,
    ok: missing.length === 0,
    schemaMutationAuthorized: false,
    note:
      "CD-3 amended A: wire existing fields via MAP_GDI_OPPORTUNITY; do not recreate.",
  };

  fs.writeFileSync(
    path.join(OUT, "EXISTING_SCHEMA_VERIFICATION.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  const md = [
    "# Existing GDO schema verification",
    "",
    `**Source:** ${source}`,
    `**Base:** \`${baseId}\``,
    `**Table:** ${GDI_OPPORTUNITIES_TABLE_NAME} (\`${live.tableId}\`)`,
    `**Result:** ${report.ok ? "PASS — all required fields exist" : "FAIL — missing fields"}`,
    "",
    "| Field | Present | Type | Action |",
    "|---|---|---|---|",
    ...results.map(
      (r) =>
        `| \`${r.field}\` | ${r.present ? "yes" : "NO"} | ${r.type || "—"} | ${r.action} |`,
    ),
    "",
    "Schema mutations: **NOT AUTHORIZED**.",
    "",
  ].join("\n");
  fs.writeFileSync(path.join(OUT, "EXISTING_SCHEMA_VERIFICATION.md"), md);
  console.log(JSON.stringify({ ok: report.ok, source, missing: missing.map((m) => m.field) }, null, 2));
  if (!report.ok) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
