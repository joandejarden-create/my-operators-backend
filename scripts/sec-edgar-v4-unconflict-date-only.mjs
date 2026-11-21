#!/usr/bin/env node
/**
 * Phase 2 remediation: do not keep Contract Status=CONFLICTED solely because a
 * date meaning was wrong. Preserve Notes quarantine; restore status to
 * EXPIRED_STATUS_UNCONFIRMED unless agreement evidence itself is contradictory.
 *
 *   node scripts/sec-edgar-v4-unconflict-date-only.mjs --dry-run
 *   HIF_APPLY_RESEARCH=1 node scripts/sec-edgar-v4-unconflict-date-only.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { GRAPH_BASE_ID_CANONICAL } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}
const OUT = path.join(ROOT, "reports/sec-edgar-contract-timing-v4");
const IDS = JSON.parse(fs.readFileSync(path.join(OUT, "SCHEMA_TABLE_IDS.json"), "utf8"));
const APPLY = process.argv.includes("--apply");

function token() {
  return (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "").trim();
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
async function at(tableId, method, body, qs = "") {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}${qs}`, {
    method,
    headers: {
      Authorization: `Bearer ${token()}`,
      ...(body ? { "Content-Type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  return { ok: res.ok, status: res.status, json: await res.json().catch(() => ({})) };
}
async function listAll(tableId, fields = []) {
  const out = [];
  let offset = null;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const r = await at(tableId, "GET", null, `?${params}`);
    if (!r.ok) throw new Error(JSON.stringify(r.json));
    out.push(...(r.json.records || []));
    offset = r.json.offset || null;
    await sleep(120);
  } while (offset);
  return out;
}

async function main() {
  if (APPLY && String(process.env.HIF_APPLY_RESEARCH || "") !== "1") {
    console.log(JSON.stringify({ ok: false, error: "HIF_APPLY_RESEARCH_ENV_REQUIRED" }));
    process.exitCode = 2;
    return;
  }
  const contracts = await listAll(IDS.contractsTableId, [
    "Contract ID",
    "Contract Status",
    "Publication State",
    "Notes / Quarantine",
    "Scheduled Expiration Date",
  ]);
  const milestones = await listAll(IDS.milestonesTableId, [
    "Milestone ID",
    "Contract",
    "Milestone Date",
    "Milestone Type",
  ]);

  const withAudit = contracts.filter((c) =>
    String(c.fields["Notes / Quarantine"] || "").includes("DATE_MEANING_AUDIT_V1"),
  );
  const conflictedDateOnly = withAudit.filter((c) => c.fields["Contract Status"] === "CONFLICTED");

  const restoreTo = "EXPIRED_STATUS_UNCONFIRMED";
  const planned = conflictedDateOnly.map((c) => ({
    id: c.id,
    contractId: c.fields["Contract ID"],
    from: "CONFLICTED",
    to: restoreTo,
    reason: "date_misclassification_only_not_agreement_contradiction",
    keepNotesQuarantine: true,
  }));

  let applied = 0;
  if (APPLY) {
    for (const row of planned) {
      const noteExtra =
        " | IDENTITY_SEPARATION: agreement_status_restored; contractual_date_claim_remains_quarantined_from_decision_windows";
      const prior = contracts.find((c) => c.id === row.id);
      const notes = String(prior.fields["Notes / Quarantine"] || "");
      const fields = {
        "Contract Status": restoreTo,
        "Notes / Quarantine": notes.includes("IDENTITY_SEPARATION:") ? notes : (notes + noteExtra).slice(0, 90000),
      };
      const r = await at(IDS.contractsTableId, "PATCH", { records: [{ id: row.id, fields }] });
      if (!r.ok) throw new Error(JSON.stringify(r.json));
      applied += 1;
      await sleep(150);
    }
  }

  const pub = {};
  for (const c of contracts) {
    const ps = c.fields["Publication State"] || "UNKNOWN";
    pub[ps] = (pub[ps] || 0) + 1;
  }
  const linked = milestones.filter((m) => (m.fields.Contract || []).length > 0).length;

  const report = {
    at: new Date().toISOString(),
    apply: APPLY,
    contracts: contracts.length,
    milestones: milestones.length,
    milestonesLinked: linked,
    publicationStateCounts: pub,
    dateAuditNotesPresent: withAudit.length,
    conflictedDateOnlyBefore: conflictedDateOnly.length,
    plannedRestores: planned.length,
    applied,
    hpcUpdates: 0,
    customerBlocked: (pub.CUSTOMER_BLOCKED || 0) === contracts.length,
  };
  fs.writeFileSync(
    path.join(OUT, "DATE_ONLY_UNCONFLICT_REPORT.json"),
    JSON.stringify({ ...report, sample: planned.slice(0, 5) }, null, 2),
  );
  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
