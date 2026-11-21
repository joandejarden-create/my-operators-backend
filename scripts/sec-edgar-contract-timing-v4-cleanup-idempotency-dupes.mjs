#!/usr/bin/env node
/**
 * Cleanup accidental duplicate contracts/milestones created during flawed idempotency verify.
 * Research tables only. Does not touch HPC.
 *
 *   HIF_APPLY_RESEARCH=1 node scripts/sec-edgar-contract-timing-v4-cleanup-idempotency-dupes.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const APPLY = process.argv.includes("--apply");
const OUT_DIR = path.join(ROOT, "reports/sec-edgar-contract-timing-v4");
const IDS = JSON.parse(fs.readFileSync(path.join(OUT_DIR, "SCHEMA_TABLE_IDS.json"), "utf8"));
const BASE = "appCCUsuGsE1ifoLk";

const DUPES = [
  {
    hotel: "Springhill Suites - New Smyrna Beach",
    contractRecordId: "recOETrauJMTByfOI",
    milestoneRecordId: "rec1s9QlqjkvCjUE1",
    inventoryRecordId: "recNZZIX5Sso3PdYB",
    findingRecordId: "recXBAxXq4M8b45eM",
  },
  {
    hotel: "Fairfield Inn & Suites Lebanon",
    contractRecordId: "recaO2FpDFNPMr6W6",
    milestoneRecordId: "recpDO0vASSWU0oXr",
    inventoryRecordId: "recUlvnt3DNQSggiy",
    findingRecordId: "rec5v14yKvLt0rJJi",
  },
  {
    hotel: "SpringHill Suites Newark",
    contractRecordId: "rec9vKsT9FfoX3p1W",
    milestoneRecordId: "rec3HoZiTmHzibPDt",
  },
  {
    hotel: "Home2 Suites - Lithia Springs, GA",
    contractRecordId: "recvXAEjJwGN9bhXU",
    milestoneRecordId: "rec61rzDRvcJoz2er",
    inventoryRecordId: "recVjZ5tm1d7XygSh",
    findingRecordId: "rec1EDjo14wVtv5dS",
  },
  {
    hotel: "Candlewood Suites - Terre Haute IN",
    contractRecordId: "rec1pWvqQ5YQR7ECX",
    milestoneRecordId: "rec8gepaTKy5td97D",
    inventoryRecordId: "recjzLk3HBuqbG9KE",
    findingRecordId: "reczA2q5qeUXBmKJ7",
  },
  {
    hotel: "Candlewood Suites Watertown",
    contractRecordId: "recSc4K7lLRpOhx6k",
    milestoneRecordId: "recgimJBI6Ail3uzP",
  },
  {
    hotel: "Candlewood Suites Sayre",
    contractRecordId: "rec1IuFHaEgOTHGLt",
    milestoneRecordId: "recMnEZs6f4LEVYJ8",
  },
];

function token() {
  return (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "").trim();
}

async function del(tableId, recordId) {
  const res = await fetch(`https://api.airtable.com/v0/${BASE}/${tableId}/${recordId}`, {
    method: "DELETE",
    headers: { Authorization: `Bearer ${token()}` },
  });
  return { ok: res.ok, status: res.status, json: await res.json().catch(() => ({})), recordId, tableId };
}

async function main() {
  if (APPLY && String(process.env.HIF_APPLY_RESEARCH || "") !== "1") {
    console.log(JSON.stringify({ ok: false, error: "HIF_APPLY_RESEARCH_ENV_REQUIRED" }));
    process.exitCode = 2;
    return;
  }
  const ops = [];
  for (const d of DUPES) {
    // Delete milestone → finding → contract → inventory (link order)
    for (const [kind, id, tableId] of [
      ["milestone", d.milestoneRecordId, IDS.milestonesTableId],
      ["finding", d.findingRecordId, IDS.findingsTableId],
      ["contract", d.contractRecordId, IDS.contractsTableId],
      ["inventory", d.inventoryRecordId, IDS.inventoryTableId],
    ]) {
      if (!id) continue;
      if (!APPLY) {
        ops.push({ hotel: d.hotel, kind, recordId: id, action: "WOULD_DELETE" });
        continue;
      }
      const r = await del(tableId, id);
      ops.push({ hotel: d.hotel, kind, recordId: id, ok: r.ok, status: r.status, error: r.ok ? null : r.json });
      await new Promise((x) => setTimeout(x, 120));
    }
  }
  const out = path.join(OUT_DIR, "IDEMPOTENCY_DUPE_CLEANUP.json");
  fs.writeFileSync(
    out,
    JSON.stringify(
      {
        at: new Date().toISOString(),
        apply: APPLY,
        reason: "Accidental duplicates from idempotency verify using audit Contract IDs while pilot rows used different IDs",
        hpcTouched: false,
        ops,
      },
      null,
      2,
    ),
  );
  console.log(JSON.stringify({ out, apply: APPLY, ops: ops.length, failed: ops.filter((o) => o.ok === false).length }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
