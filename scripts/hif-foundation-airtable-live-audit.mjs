#!/usr/bin/env node
/**
 * Read-only live audit of HIF Airtable destinations on HPC base.
 * Does not write. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
  HPC_TABLE_ID,
  HI_ORGANIZATIONS_TABLE_ID,
  HI_GRAPH_EVIDENCE_TABLE_ID,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
if (!token) {
  console.error("NO_AIRTABLE_TOKEN");
  process.exit(2);
}

const idsPath = path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json");
const ids = JSON.parse(fs.readFileSync(idsPath, "utf8"));
const contractsIdsPath = path.join(ROOT, "reports/sec-edgar-contract-timing-v4/SCHEMA_TABLE_IDS.json");
const contractIds = fs.existsSync(contractsIdsPath)
  ? JSON.parse(fs.readFileSync(contractsIdsPath, "utf8"))
  : {};

const tables = [
  { name: "Hotel Property Census", id: HPC_TABLE_ID },
  { name: TABLE_HI_HOTEL_DISCOVERY_INVENTORY, id: ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY] },
  { name: TABLE_HI_DISCOVERY_FINDINGS, id: ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS] },
  { name: "HI Organizations", id: HI_ORGANIZATIONS_TABLE_ID },
  { name: "HI Graph Evidence", id: HI_GRAPH_EVIDENCE_TABLE_ID },
  { name: "HI Hotel–Org Relationships", id: "tblGiwN0foXXDESvZ" },
  { name: "HI Hotel Contracts", id: contractIds.contractsTableId || null },
  { name: "HI Contract Milestones", id: contractIds.milestonesTableId || null },
];

async function metaTables() {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${GRAPH_BASE_ID_CANONICAL}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json.tables || [];
}

async function countRecords(tableId) {
  if (!tableId) return { ok: false, reason: "NO_TABLE_ID" };
  const qs = new URLSearchParams({ pageSize: "1" });
  // Airtable REST has no total count; sample first page + field names via meta
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}?${qs}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) return { ok: false, status: res.status, error: json };
  return {
    ok: true,
    sampleCount: (json.records || []).length,
    sampleIds: (json.records || []).map((r) => r.id),
    offset: json.offset || null,
  };
}

const meta = await metaTables();
const byName = new Map(meta.map((t) => [t.name, t]));
const byId = new Map(meta.map((t) => [t.id, t]));

const results = [];
for (const t of tables) {
  const metaRow = t.id ? byId.get(t.id) : byName.get(t.name);
  const resolvedId = metaRow?.id || t.id;
  const exists = Boolean(metaRow);
  const fields = (metaRow?.fields || []).map((f) => ({ name: f.name, type: f.type }));
  const sample = exists ? await countRecords(resolvedId) : { ok: false, reason: "NOT_IN_BASE" };
  results.push({
    name: t.name,
    expectedId: t.id,
    resolvedId: resolvedId || null,
    existsInBase: exists,
    fieldCount: fields.length,
    fields: fields.slice(0, 80),
    sample,
  });
  await new Promise((r) => setTimeout(r, 150));
}

const out = {
  asOf: new Date().toISOString(),
  baseId: GRAPH_BASE_ID_CANONICAL,
  publicationState: "CUSTOMER_BLOCKED",
  mode: "READ_ONLY_AUDIT",
  tablesInBase: meta.length,
  tableNames: meta.map((t) => t.name).sort(),
  audited: results,
};
const outDir = path.join(ROOT, "reports/sec-edgar-hif-foundation-persistence");
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(path.join(outDir, "AIRTABLE_LIVE_AUDIT.json"), JSON.stringify(out, null, 2));
console.log(JSON.stringify({ ok: true, tablesInBase: meta.length, audited: results.map((r) => ({ name: r.name, exists: r.existsInBase, fields: r.fieldCount, sampleOk: r.sample.ok })) }, null, 2));
