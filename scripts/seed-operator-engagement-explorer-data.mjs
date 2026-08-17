#!/usr/bin/env node
/**
 * Seed Engagement & Reporting Explorer JSON on Commercial rows linked to Master.
 *
 *   node scripts/seed-operator-engagement-explorer-data.mjs --apply
 *   node scripts/seed-operator-engagement-explorer-data.mjs --apply --force
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  ENGAGEMENT_JSON_FIELD_KEYS,
  buildEngagementExplorerSeedFields,
} from "../lib/operator-engagement-explorer-seed-data.js";
import {
  NEW_BASE_COMMERCIAL_TABLE,
  NEW_BASE_MASTER_TABLE,
  fetchAllRecordsRest,
  airtableFetchJson,
} from "../api/lib/operator-setup-new-base-read.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

const APPLY = process.argv.includes("--apply");
const FORCE = process.argv.includes("--force");

function enc(s) {
  return encodeURIComponent(s);
}

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function masterIdFromRow(fields) {
  const op = fields && fields.Operator;
  return Array.isArray(op) && op[0] ? String(op[0]) : "";
}

function hasJsonSeed(fields) {
  return ENGAGEMENT_JSON_FIELD_KEYS.some((k) => nz(fields[k]));
}

async function patchCommercial(recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(NEW_BASE_COMMERCIAL_TABLE)}/${enc(recordId)}`;
  const { ok, status, json } = await airtableFetchJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return { ok, status, json };
}

async function main() {
  const [masters, commercialRows] = await Promise.all([
    fetchAllRecordsRest(NEW_BASE_MASTER_TABLE),
    fetchAllRecordsRest(NEW_BASE_COMMERCIAL_TABLE),
  ]);

  const masterNameById = new Map();
  for (const m of masters) {
    const f = m.fields || {};
    const name = nz(f.company_name) || nz(f["Company Name"]) || m.id;
    masterNameById.set(m.id, name);
  }

  const seedFields = buildEngagementExplorerSeedFields();
  const plan = [];

  for (const row of commercialRows) {
    const fields = row.fields || {};
    const mid = masterIdFromRow(fields);
    if (!mid) continue;

    const companyName = masterNameById.get(mid) || mid;
    if (hasJsonSeed(fields) && !FORCE) {
      plan.push({ recordId: row.id, companyName, action: "skip" });
      continue;
    }

    plan.push({ recordId: row.id, companyName, action: APPLY ? "patch" : "would-patch" });

    if (APPLY) {
      const { ok, status, json } = await patchCommercial(row.id, seedFields);
      if (!ok) {
        console.error("PATCH FAILED", companyName, status, JSON.stringify(json));
        process.exitCode = 1;
      } else {
        console.log("PATCHED", companyName);
      }
      await new Promise((r) => setTimeout(r, 220));
    }
  }

  const stamp = new Date().toISOString().slice(0, 10);
  if (!fs.existsSync(REPORTS)) fs.mkdirSync(REPORTS, { recursive: true });
  const outPath = path.join(REPORTS, `operator-eng-explorer-seed-${stamp}.json`);
  fs.writeFileSync(outPath, JSON.stringify({ apply: APPLY, force: FORCE, plan }, null, 2));
  console.log("\nPlan:", outPath);
  if (!APPLY) console.log("Dry run. Re-run with --apply.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
