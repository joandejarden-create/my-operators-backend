#!/usr/bin/env node
/**
 * Seed Infrastructure & Data Explorer JSON + maturity level on Governance rows (linked to Master).
 *
 *   node scripts/seed-operator-infrastructure-explorer-data.mjs
 *   node scripts/seed-operator-infrastructure-explorer-data.mjs --apply
 *   node scripts/seed-operator-infrastructure-explorer-data.mjs --apply --force
 *   node scripts/seed-operator-infrastructure-explorer-data.mjs --apply --master recXXX
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildInfraExplorerSeedFields } from "../lib/operator-infrastructure-explorer-seed-data.js";
import {
  NEW_BASE_GOVERNANCE_TABLE,
  NEW_BASE_MASTER_TABLE,
  fetchAllRecordsRest,
  airtableFetchJson,
} from "../api/lib/operator-setup-new-base-read.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

const APPLY = process.argv.includes("--apply");
const FORCE = process.argv.includes("--force");
const masterArg = process.argv.find((a, i) => process.argv[i - 1] === "--master");

const JSON_KEYS = [
  "infra_technology_stack_json",
  "infra_services_offered_json",
  "infra_data_domains_json",
  "infra_data_governance_json",
  "infra_analytics_support_json",
];

function enc(s) {
  return encodeURIComponent(s);
}

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function hasJsonSeed(fields) {
  return JSON_KEYS.some((k) => nz(fields[k]));
}

function masterIdFromGovernance(fields) {
  const op = fields && fields.Operator;
  return Array.isArray(op) && op[0] ? String(op[0]) : "";
}

async function patchGovernanceRecord(recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(NEW_BASE_GOVERNANCE_TABLE)}/${enc(recordId)}`;
  const { ok, status, json } = await airtableFetchJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return { ok, status, json };
}

async function main() {
  const [masters, governanceRows] = await Promise.all([
    fetchAllRecordsRest(NEW_BASE_MASTER_TABLE),
    fetchAllRecordsRest(NEW_BASE_GOVERNANCE_TABLE),
  ]);

  const masterNameById = new Map();
  for (const m of masters) {
    const f = m.fields || {};
    const name =
      nz(f.company_name) ||
      nz(f["Company Name"]) ||
      nz(f.companyName) ||
      m.id;
    masterNameById.set(m.id, name);
  }

  let targets = governanceRows.filter((r) => masterIdFromGovernance(r.fields));
  if (masterArg) {
    targets = targets.filter((r) => masterIdFromGovernance(r.fields) === masterArg);
    if (!targets.length) {
      throw new Error(`No Governance row linked to Master ${masterArg}`);
    }
  }

  const plan = [];
  let index = 0;
  for (const row of targets) {
    const fields = row.fields || {};
    const mid = masterIdFromGovernance(fields);
    const companyName = masterNameById.get(mid) || mid;

    if (hasJsonSeed(fields) && !FORCE) {
      plan.push({ recordId: row.id, masterId: mid, companyName, action: "skip", reason: "already seeded (use --force)" });
      index += 1;
      continue;
    }

    const seedFields = buildInfraExplorerSeedFields({
      index,
      existingFields: fields,
      companyName,
    });

    plan.push({
      recordId: row.id,
      masterId: mid,
      companyName,
      action: APPLY ? "patch" : "would-patch",
      maturity: seedFields.infra_technology_maturity_level,
      fieldKeys: Object.keys(seedFields),
    });

    if (APPLY) {
      const { ok, status, json } = await patchGovernanceRecord(row.id, seedFields);
      if (!ok) {
        console.error("PATCH FAILED", companyName, row.id, status, JSON.stringify(json));
        process.exitCode = 1;
      } else {
        console.log("PATCHED", companyName, "→", seedFields.infra_technology_maturity_level);
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    index += 1;
  }

  const stamp = new Date().toISOString().slice(0, 10);
  if (!fs.existsSync(REPORTS)) fs.mkdirSync(REPORTS, { recursive: true });
  const outPath = path.join(REPORTS, `operator-infra-explorer-seed-${stamp}.json`);
  fs.writeFileSync(outPath, JSON.stringify({ apply: APPLY, force: FORCE, plan }, null, 2));
  console.log("\nPlan written:", outPath);
  console.log(
    "Rows:",
    plan.length,
    "| patch:",
    plan.filter((p) => p.action === "patch" || p.action === "would-patch").length,
    "| skip:",
    plan.filter((p) => p.action === "skip").length
  );

  if (!APPLY) {
    console.log("\nDry run. Re-run with --apply to write to Airtable.");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
