#!/usr/bin/env node
/**
 * Backfill Operator Setup - Master governance so every Production OE shows a trust footnote.
 *
 *   node scripts/operator-explorer-trust-footnote-backfill.mjs --dry-run
 *   node scripts/operator-explorer-trust-footnote-backfill.mjs --apply --approve-operator-explorer-trust-footnote
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { TEST_FIXTURE_MASTER_IDS } from "../lib/operator-explorer/phase-1-universe.js";
import { normalizeProfileGovernance } from "../lib/profile-governance/normalize-profile-governance.js";
import {
  OE_TRUST_FOOTNOTE_VERSION,
  OE_TRUST_FOOTNOTE_POSTURE_BY_MASTER_ID,
  inferOeTrustPosture,
  buildMasterGovernancePatchFromPosture,
  applyOperatorExplorerTrustFootnote,
  evaluateOperatorExplorerTrustFootnoteGate,
} from "../lib/partner-intelligence/operator-explorer-trust-footnote.js";
import { GOVERNANCE_EXTERNAL_DISPLAY as GED } from "../lib/profile-governance/profile-governance-fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/oe-trust-footnote");

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-explorer-trust-footnote") out.approve = true;
  }
  return out;
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function writeJson(p, o) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, JSON.stringify(o, null, 2) + "\n");
}
function writeMd(p, t) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, t.endsWith("\n") ? t : t + "\n");
}

async function listAll(baseId, token, table) {
  const out = [];
  let offset;
  do {
    const u = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`);
    u.searchParams.set("pageSize", "100");
    if (offset) u.searchParams.set("offset", offset);
    const j = await (await fetch(u, { headers: { Authorization: `Bearer ${token}` } })).json();
    if (j.error) throw new Error(JSON.stringify(j.error));
    out.push(...(j.records || []));
    offset = j.offset;
    await sleep(25);
  } while (offset);
  return out;
}

async function patchRecord(baseId, token, table, id, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${id}`, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const j = await res.json();
  if (!res.ok) throw new Error(`PATCH ${id}: ${JSON.stringify(j)}`);
  return j;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Refuse apply without --approve-operator-explorer-trust-footnote");
    process.exit(1);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");

  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const fixture = new Set(TEST_FIXTURE_MASTER_IDS);
  const production = masters
    .filter((m) => m.fields["Record Purpose"] === "Production" && !fixture.has(m.id))
    .sort((a, b) => String(a.fields.company_name || "").localeCompare(String(b.fields.company_name || "")));

  const patches = [];
  const rows = [];

  for (const m of production) {
    const posture =
      OE_TRUST_FOOTNOTE_POSTURE_BY_MASTER_ID[m.id] ||
      inferOeTrustPosture({
        companyName: m.fields.company_name,
        operatingModel: m.fields["Operating Model"],
      });

    // Region fill for exemplars missing Source Region (HE / GHL)
    const fields = buildMasterGovernancePatchFromPosture(posture, { existingFields: m.fields });
    if (
      OE_TRUST_FOOTNOTE_POSTURE_BY_MASTER_ID[m.id] &&
      !m.fields["Source Region"] &&
      posture.sourceRegion
    ) {
      fields["Source Region"] = posture.sourceRegion;
    }
    // Force Show Trust Label when blank
    if (!m.fields["External Display Status"]) {
      fields["External Display Status"] = GED.showTrustLabel;
    }

    const before = normalizeProfileGovernance(m.fields, {
      entityType: "operator",
      sourceTable: "Operator Setup - Master",
      fallbackFields: m.fields,
    });
    const simulatedFields = { ...m.fields, ...fields };
    const afterNorm = normalizeProfileGovernance(simulatedFields, {
      entityType: "operator",
      sourceTable: "Operator Setup - Master",
      fallbackFields: simulatedFields,
    });
    const op = { id: m.id, fields: simulatedFields, governance: afterNorm, prefill: { company_name: m.fields.company_name } };
    applyOperatorExplorerTrustFootnote(op, {
      masterId: m.id,
      masterFields: simulatedFields,
      companyName: m.fields.company_name,
      operatingModel: m.fields["Operating Model"],
    });
    const gate = evaluateOperatorExplorerTrustFootnoteGate(op);

    rows.push({
      masterId: m.id,
      operator: m.fields.company_name,
      beforeLabel: before.displayLabel || null,
      afterLabel: op.governance.displayLabel,
      afterSubtitle: op.governance.displaySubtitle,
      validationStatus: op.governance.validationStatus || posture.validationStatus,
      sourceRegion: fields["Source Region"] || m.fields["Source Region"] || posture.sourceRegion,
      patchFields: fields,
      gatePass: gate.pass,
      gateFailures: gate.failures,
      notes: posture.notes || null,
    });

    if (Object.keys(fields).length) {
      patches.push({ recordId: m.id, operator: m.fields.company_name, fields });
    }
  }

  writeJson(join(OUT, "oe-trust-footnote-plan.json"), { version: OE_TRUST_FOOTNOTE_VERSION, rows, patches });
  writeMd(
    join(ROOT, "reports/operator-explorer-trust-footnote-audit.md"),
    [
      `# Operator Explorer — Trust Footnote Audit`,
      ``,
      `Version: \`${OE_TRUST_FOOTNOTE_VERSION}\`. Production: **${production.length}**.`,
      ``,
      `| Operator | Before | After label | Subtitle | VS | Region | Gate | Patch keys |`,
      `| -------- | ------ | ----------- | -------- | -- | ------ | ---- | ---------- |`,
      ...rows.map(
        (r) =>
          `| ${r.operator} | ${r.beforeLabel || "MISSING"} | ${r.afterLabel} | ${r.afterSubtitle} | ${r.validationStatus} | ${r.sourceRegion} | ${r.gatePass ? "PASS" : r.gateFailures.join(";")} | ${Object.keys(r.patchFields).join(", ") || "—"} |`
      ),
      ``,
      `Patches planned: **${patches.length}**. Gate pass: **${rows.filter((r) => r.gatePass).length}/${rows.length}**.`,
      ``,
    ].join("\n")
  );

  let writes = 0,
    failures = 0,
    backupDir = null;
  if (args.apply) {
    const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
    backupDir = join(ROOT, "backups/operator-setup/oe-trust-footnote", ts);
    mkdirSync(backupDir, { recursive: true });
    writeJson(join(backupDir, "masters-before.json"), masters);
    writeJson(join(backupDir, "patches.json"), patches);
    for (const p of patches) {
      try {
        await patchRecord(baseId, token, "Operator Setup - Master", p.recordId, p.fields);
        writes++;
        await sleep(55);
      } catch (e) {
        failures++;
        console.error(p.operator, e.message || e);
      }
    }
  }

  const stop = {
    version: OE_TRUST_FOOTNOTE_VERSION,
    mode: args.apply ? "apply" : "dry-run",
    productionCount: production.length,
    withLabelBefore: rows.filter((r) => r.beforeLabel).length,
    withLabelAfterProjected: rows.filter((r) => r.afterLabel).length,
    gatePassProjected: rows.filter((r) => r.gatePass).length,
    patches: patches.length,
    airtableWrites: writes,
    failures,
    backupDir,
    report: "reports/operator-explorer-trust-footnote-audit.md",
  };
  writeJson(join(OUT, "oe-trust-footnote-stop.json"), stop);
  console.log(JSON.stringify(stop, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
