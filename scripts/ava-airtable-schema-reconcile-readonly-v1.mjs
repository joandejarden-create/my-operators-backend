#!/usr/bin/env node
/**
 * Read-only Airtable schema reconciliation for AVA permanent runner.
 * Never creates tables/fields. Never writes records.
 *
 * Loads credentials from ../my-operators-backend/.env when local .env missing.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { AVA_AIRTABLE_ENTITY_TABLES } from "../lib/ai-visibility-audit-action-plan/ava-audit-airtable-sync-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function loadEnvFile(abs) {
  if (!fs.existsSync(abs)) return {};
  const out = {};
  for (const line of fs.readFileSync(abs, "utf8").split(/\r?\n/)) {
    if (!line || line.trim().startsWith("#")) continue;
    const i = line.indexOf("=");
    if (i < 0) continue;
    out[line.slice(0, i).trim()] = line.slice(i + 1).trim();
  }
  return out;
}

const env = {
  ...loadEnvFile(path.join(ROOT, "..", "my-operators-backend", ".env")),
  ...loadEnvFile(path.join(ROOT, ".env")),
  ...process.env,
};

const apiKey = env.AIRTABLE_API_KEY || env.AIRTABLE_PAT;
const bases = [
  { role: "product", id: env.AIRTABLE_BASE_ID },
  { role: "sandbox", id: env.AIRTABLE_BASE_ID_SANDBOX },
  { role: "alt", id: env.AIRTABLE_BASE_ID_ALT },
].filter((b) => b.id);

async function listTables(baseId) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}/tables`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  const text = await res.text();
  if (!res.ok) {
    return { ok: false, status: res.status, error: text.slice(0, 300) };
  }
  const json = JSON.parse(text);
  return {
    ok: true,
    tables: (json.tables || []).map((t) => ({
      id: t.id,
      name: t.name,
      primaryFieldId: t.primaryFieldId,
      fieldCount: (t.fields || []).length,
      fields: (t.fields || []).map((f) => ({
        id: f.id,
        name: f.name,
        type: f.type,
      })),
    })),
  };
}

if (!apiKey) {
  console.log(
    JSON.stringify(
      {
        ok: false,
        status: "AIRTABLE_CREDENTIALS_MISSING",
        note: "No AIRTABLE_API_KEY/PAT available for read-only schema inspection",
      },
      null,
      2
    )
  );
  process.exit(0);
}

const proposed = Object.values(AVA_AIRTABLE_ENTITY_TABLES);
const report = {
  ok: true,
  mode: "READ_ONLY",
  proposedTables: proposed,
  bases: [],
  verdict: "AIRTABLE_SCHEMA_AUTHORIZATION_REQUIRED",
  notes: [],
};

for (const b of bases) {
  const listed = await listTables(b.id);
  const entry = {
    role: b.role,
    baseIdPrefix: String(b.id).slice(0, 10) + "…",
    ok: listed.ok,
    error: listed.error || null,
    tableCount: listed.tables?.length || 0,
    tableNames: (listed.tables || []).map((t) => t.name),
    avaTableHits: {},
    hotelMasterCandidates: (listed.tables || [])
      .filter((t) =>
        /hotel|census|property|master/i.test(t.name)
      )
      .map((t) => t.name)
      .slice(0, 40),
  };
  for (const name of proposed) {
    const hit = (listed.tables || []).find((t) => t.name === name);
    entry.avaTableHits[name] = hit
      ? { present: true, id: hit.id, fieldCount: hit.fieldCount, fields: hit.fields }
      : { present: false };
  }
  report.bases.push(entry);
  // rate-limit courtesy
  await new Promise((r) => setTimeout(r, 220));
}

const anyPresent = report.bases.some((b) =>
  Object.values(b.avaTableHits || {}).some((h) => h.present)
);
if (anyPresent) {
  report.notes.push("One or more proposed AVA tables already exist — reuse and extend; do not duplicate.");
} else {
  report.notes.push(
    "None of the six proposed AVA historical tables exist yet in inspected bases. Schema create/alter requires separate authorization."
  );
}
report.notes.push(
  "Existing AI Visibility tables (Prompts/Opportunities) are Brand/Operator AI monitoring — not AVA hotel audit history."
);
report.notes.push("No writes performed.");

const outDir = path.join(ROOT, "reports/ava-permanent-audit-runner-v1");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, "AIRTABLE_SCHEMA_RECONCILIATION_READONLY.json");
fs.writeFileSync(outPath, `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify({ ...report, outPath }, null, 2));
