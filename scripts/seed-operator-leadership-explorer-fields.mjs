#!/usr/bin/env node
/**
 * Patch Governance 1:1 row with Leadership Explorer JSON fields.
 * Usage: node scripts/seed-operator-leadership-explorer-fields.mjs --operator-id recXXX
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

const GOVERNANCE_TABLE = "Operator Setup - Governance, Delivery & Diligence";

const FIELD_MAP = {
  lead_avg_hospitality_experience: "Leadership Avg Hospitality Experience",
  lead_org_structure_json: "Leadership Org Structure (JSON)",
  lead_team_depth_json: "Leadership Team Depth (JSON)",
  lead_language_capability_json: "Leadership Languages (JSON)",
  lead_governance_cadence_json: "Leadership Governance Cadence (JSON)",
  lead_team_markets_json: "Leadership Team Markets (JSON)",
  lead_owner_relationship_json: "Leadership Owner Relationship (JSON)",
};

function parseArgs() {
  const args = process.argv.slice(2);
  let operatorId = "";
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--operator-id" && args[i + 1]) operatorId = args[++i];
  }
  return { operatorId };
}

async function airtableFetch(url, options = {}) {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const r = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...options.headers,
    },
  });
  const json = await r.json().catch(() => ({}));
  return { ok: r.ok, status: r.status, json };
}

async function findGovernanceRow(baseId, masterId) {
  let offset = null;
  do {
    let url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(GOVERNANCE_TABLE)}?pageSize=100&filterByFormula=${encodeURIComponent(`FIND("${masterId}", ARRAYJOIN({Operator}))`)}`;
    if (offset) url += `&offset=${offset}`;
    const { ok, json } = await airtableFetch(url);
    if (!ok) throw new Error(json.error?.message || "Governance list failed");
    const hit = (json.records || [])[0];
    if (hit) return hit;
    offset = json.offset || null;
  } while (offset);
  return null;
}

async function patchWithFallback(recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(GOVERNANCE_TABLE)}/${recordId}`;
  let attempt = { ...fields };
  for (let tries = 0; tries < 8; tries++) {
    const { ok, json } = await airtableFetch(url, {
      method: "PATCH",
      body: JSON.stringify({ fields: attempt }),
    });
    if (ok) return { ok: true, fields: attempt };
    const msg = json.error?.message || "";
    const m = msg.match(/Unknown field name: "([^"]+)"/);
    if (!m) throw new Error(msg || "PATCH failed");
    delete attempt[m[1]];
    console.warn("[seed] skipped missing column:", m[1]);
    if (!Object.keys(attempt).length) return { ok: false, fields: {} };
  }
  return { ok: false, fields: {} };
}

async function main() {
  const { operatorId } = parseArgs();
  if (!operatorId) {
    console.error("Usage: --operator-id recXXXXXXXX");
    process.exit(1);
  }
  const fixturePath = path.join(ROOT, "fixtures", "operator-leadership-explorer-antillano-norte.json");
  const sample = JSON.parse(fs.readFileSync(fixturePath, "utf8"));

  const fields = {};
  for (const [formKey, airtableName] of Object.entries(FIELD_MAP)) {
    const val = sample[formKey];
    if (val == null) continue;
    fields[airtableName] =
      typeof val === "string" ? val : JSON.stringify(val, null, 0).length > 80000 ? JSON.stringify(val) : JSON.stringify(val);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const row = await findGovernanceRow(baseId, operatorId);
  if (!row) {
    console.error("No Governance row linked to master", operatorId);
    process.exit(1);
  }

  const result = await patchWithFallback(row.id, fields);
  console.log(result.ok ? "Patched governance leadership explorer fields" : "No fields patched", row.id);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
