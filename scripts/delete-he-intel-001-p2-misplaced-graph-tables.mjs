#!/usr/bin/env node
/**
 * Delete the five HI graph tables mistakenly created on ADP/intelligence base.
 *
 * ONLY after HPC-base tables exist and validation passes.
 *
 *   node scripts/delete-he-intel-001-p2-misplaced-graph-tables.mjs --dry-run
 *   node scripts/delete-he-intel-001-p2-misplaced-graph-tables.mjs --apply --confirm-empty
 *
 * Never deletes Hotel Intelligence Evidence (tblHxS8x0niDEGVsh).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_FORBIDDEN,
  GRAPH_BASE_ID_CANONICAL,
  HI_GRAPH_TABLES,
} from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [
  path.join(ROOT, ".env"),
  "C:/Dev/dealality/my-operators-backend/.env",
]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;
const CONFIRM_EMPTY = process.argv.includes("--confirm-empty");

const MISPLACED = Object.freeze([
  { name: "HI Organizations", id: "tbl4XhanPXiUIsvaB" },
  { name: "HI Hotel–Org Relationships", id: "tblbBbIYtIWEPyRPH" },
  { name: "HI Org Contacts", id: "tblki7hgPmcrkrfSY" },
  { name: "HI Contact Channels", id: "tblMscC8FgXUpmFts" },
  { name: "HI Ownership Research Runs", id: "tbl6cUPY5J84P3QLn" },
]);

const NEVER_DELETE = Object.freeze({
  id: "tblHxS8x0niDEGVsh",
  name: "Hotel Intelligence Evidence",
});

async function metaFetch(baseId, token, pathSuffix, init = {}) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}${pathSuffix}`, {
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

async function recordCount(baseId, token, tableId) {
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${tableId}?pageSize=1&maxRecords=1`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json();
  if (!res.ok) return { ok: false, error: json };
  return {
    ok: true,
    empty: (json.records || []).length === 0 && !json.offset,
    countHint: (json.records || []).length,
  };
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  if (!token) {
    console.error("Missing AIRTABLE_API_KEY");
    process.exit(2);
  }
  if (APPLY && !CONFIRM_EMPTY) {
    console.error("Refusing apply without --confirm-empty");
    process.exit(2);
  }

  // Preconditions: HPC registry must exist with correct-base table IDs
  const registryPath = path.join(
    ROOT,
    "lib/hotel-intelligence/research-engine-v1/airtable-graph-table-ids.generated.json"
  );
  if (!fs.existsSync(registryPath)) {
    console.error("STOP — HPC registry missing; create correct tables first:", registryPath);
    process.exit(3);
  }
  const registry = JSON.parse(fs.readFileSync(registryPath, "utf8"));
  if (registry.baseId !== GRAPH_BASE_ID_CANONICAL) {
    console.error("STOP — registry base is not HPC canonical", registry.baseId);
    process.exit(3);
  }
  for (const name of [
    HI_GRAPH_TABLES.organizations,
    HI_GRAPH_TABLES.hotelOrgRelationships,
    HI_GRAPH_TABLES.orgContacts,
    HI_GRAPH_TABLES.contactChannels,
    HI_GRAPH_TABLES.ownershipResearchRuns,
  ]) {
    if (!registry.tableIds?.[name]) {
      console.error("STOP — missing correct-base table id for", name);
      process.exit(3);
    }
  }

  const wrongBase = GRAPH_BASE_ID_FORBIDDEN;
  const { res, json } = await metaFetch(wrongBase, token, "/tables");
  if (!res.ok) {
    console.error("Failed listing wrong base", res.status, JSON.stringify(json).slice(0, 300));
    process.exit(1);
  }
  const tables = json.tables || [];
  const evidence = tables.find((t) => t.id === NEVER_DELETE.id);
  if (!evidence) {
    console.warn("WARN: Evidence table not found on old base (unexpected but not blocking deletion of the five)");
  }

  const report = {
    at: new Date().toISOString(),
    mode: DRY ? "DRY_RUN" : "APPLY",
    wrongBase,
    neverDelete: NEVER_DELETE,
    evidencePreserved: Boolean(evidence),
    deletions: [],
  };

  for (const target of MISPLACED) {
    const t = tables.find((x) => x.id === target.id);
    const entry = { ...target, present: Boolean(t) };
    if (!t) {
      entry.status = "ALREADY_ABSENT";
      report.deletions.push(entry);
      console.log("ABSENT", target.name, target.id);
      continue;
    }
    if (t.name !== target.name) {
      entry.status = "NAME_MISMATCH_STOP";
      entry.actualName = t.name;
      report.deletions.push(entry);
      console.error("STOP — id/name mismatch", entry);
      process.exit(3);
    }
    const cnt = await recordCount(wrongBase, token, target.id);
    entry.emptiness = cnt;
    if (!cnt.ok || !cnt.empty) {
      entry.status = "NOT_EMPTY_STOP";
      report.deletions.push(entry);
      console.error("STOP — table not empty", entry);
      process.exit(3);
    }
    if (DRY) {
      entry.status = "WOULD_DELETE";
      console.log("[dry-run] WOULD DELETE", target.name, target.id);
    } else {
      const del = await metaFetch(wrongBase, token, `/tables/${target.id}`, { method: "DELETE" });
      if (!del.res.ok) {
        entry.status = "DELETE_FAILED";
        entry.error = del.json;
        report.deletions.push(entry);
        console.error("DELETE FAILED", target.name, JSON.stringify(del.json));
        // Leave remaining intact
        const out = path.join(
          ROOT,
          "reports/hotel-explorer/HE-INTEL-001-P2-misplaced-table-deletion-v1.json"
        );
        fs.writeFileSync(out, `${JSON.stringify(report, null, 2)}\n`);
        process.exit(4);
      }
      entry.status = "DELETED";
      console.log("DELETED", target.name, target.id);
    }
    report.deletions.push(entry);
  }

  // Verify evidence still present
  if (!DRY) {
    const after = await metaFetch(wrongBase, token, "/tables");
    const still = (after.json.tables || []).find((t) => t.id === NEVER_DELETE.id);
    report.evidenceStillPresent = Boolean(still);
    report.remainingMisplaced = MISPLACED.filter((m) =>
      (after.json.tables || []).some((t) => t.id === m.id)
    );
    if (!still) {
      console.error("CRITICAL — Evidence table missing after deletion pass");
      process.exit(5);
    }
    if (report.remainingMisplaced.length) {
      console.error("STOP — some misplaced tables remain", report.remainingMisplaced);
      process.exit(4);
    }
  }

  const out = path.join(
    ROOT,
    "reports/hotel-explorer/HE-INTEL-001-P2-misplaced-table-deletion-v1.json"
  );
  fs.writeFileSync(out, `${JSON.stringify(report, null, 2)}\n`);
  console.log("Wrote", out);
  console.log(DRY ? "DRY-RUN complete" : "APPLY deletion complete");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
