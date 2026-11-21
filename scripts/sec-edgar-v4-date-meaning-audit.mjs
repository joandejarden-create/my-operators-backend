#!/usr/bin/env node
/**
 * V4 contractual date-meaning integrity audit.
 *
 *   node scripts/sec-edgar-v4-date-meaning-audit.mjs
 *   HIF_APPLY_RESEARCH=1 node scripts/sec-edgar-v4-date-meaning-audit.mjs --apply-corrections
 *
 * Default: dry-run correction manifest only. Preserves evidence. No HPC. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  auditContractDateObservation,
  summarizeDateMeaningAudit,
  buildCorrectionManifest,
  DECISION_WINDOW_ELIGIBILITY,
} from "../lib/hotel-intelligence/sec-edgar/contract-date-meaning-audit-v1.js";
import { GRAPH_BASE_ID_CANONICAL } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-contract-timing-v4");
const APPLY = process.argv.includes("--apply-corrections");
const IDS = JSON.parse(fs.readFileSync(path.join(OUT, "SCHEMA_TABLE_IDS.json"), "utf8"));

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function writeJson(fp, obj) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
}
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
    if (!r.ok) throw new Error(`LIST_FAIL ${tableId} ${JSON.stringify(r.json)}`);
    out.push(...(r.json.records || []));
    offset = r.json.offset || null;
    await sleep(120);
  } while (offset);
  return out;
}

function loadReconcileObservations() {
  const p = path.join(OUT, "VERIFIED_MIGRATION_RECONCILE.json");
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const rows = [...(j.alreadyPersisted || []), ...(j.pending || [])];
  const obs = [];
  for (const a of rows) {
    const ms = (a.milestones || [])[0] || {};
    obs.push({
      contractId: a.contractId,
      liveContractRecordId: a.liveContractRecordId || null,
      liveContractId: a.liveContractId || a.contractId,
      milestoneId: ms.milestoneId || null,
      hotelName: a.hotelName,
      city: a.city,
      stateRegion: a.stateRegion,
      agreementType: a.agreementType,
      primaryDate: ms.primaryDate || null,
      dateMeaning: ms.dateMeaning || null,
      evidenceSnippets: a.evidenceSnippets || [],
      evidenceSnippet: (a.evidenceSnippets || [])[0] || "",
      sourceUrls: a.sourceUrls || ms.sourceUrls || [],
      sourceAccessions: a.sourceAccessions || [],
    });
  }
  return { reconcileMeta: { verifiedAgreements: j.verifiedAgreements, liveContractsListed: j.liveContractsListed }, observations: obs };
}

async function main() {
  const { reconcileMeta, observations } = loadReconcileObservations();
  const audited = observations.map((o) => auditContractDateObservation(o));
  const summary = summarizeDateMeaningAudit(audited);
  const manifest = buildCorrectionManifest(audited);

  let live = { attempted: false };
  if (token()) {
    try {
      const contracts = await listAll(IDS.contractsTableId, [
        "Contract ID",
        "Agreement Type",
        "Scheduled Expiration Date",
        "Commencement Date",
        "Publication State",
        "Notes / Quarantine",
        "Contract Status",
      ]);
      const milestones = await listAll(IDS.milestonesTableId, [
        "Milestone ID",
        "Milestone Type",
        "Milestone Date",
        "Date Meaning Precision",
        "Publication State",
        "Contract",
        "Evidence Snippet",
        "Current Status Class",
      ]);
      const pubStates = {};
      for (const c of contracts) {
        const ps = c.fields["Publication State"] || "UNKNOWN";
        pubStates[ps] = (pubStates[ps] || 0) + 1;
      }
      const linked = milestones.filter((m) => (m.fields.Contract || []).length > 0).length;
      live = {
        attempted: true,
        ok: true,
        contracts: contracts.length,
        milestones: milestones.length,
        milestonesLinkedToContract: linked,
        publicationStateCounts: pubStates,
        customerBlocked:
          Object.keys(pubStates).every((k) => k === "CUSTOMER_BLOCKED") ||
          (pubStates.CUSTOMER_BLOCKED || 0) === contracts.length,
      };

      if (APPLY) {
        if (String(process.env.HIF_APPLY_RESEARCH || "") !== "1") {
          throw new Error("HIF_APPLY_RESEARCH_ENV_REQUIRED");
        }
        const byContractId = new Map(contracts.map((c) => [c.fields["Contract ID"], c]));
        let patched = 0;
        for (const corr of manifest.corrections) {
          const rec = byContractId.get(corr.contractId) || byContractId.get(corr.liveContractId);
          // Prefer liveContractId from observation
          const obs = observations.find((o) => o.contractId === corr.contractId);
          const liveRec =
            byContractId.get(obs?.liveContractId) ||
            byContractId.get(corr.contractId) ||
            null;
          if (!liveRec) continue;
          const prior = String(liveRec.fields["Notes / Quarantine"] || "");
          if (prior.includes("DATE_MEANING_AUDIT_V1") && prior.includes(corr.correctedMeaning)) {
            continue;
          }
          const notes = [prior, corr.notesPatch].filter(Boolean).join("\n").slice(0, 90000);
          const fields = {
            "Notes / Quarantine": notes,
          };
          if (corr.decisionWindowEligibility !== DECISION_WINDOW_ELIGIBILITY.ELIGIBLE) {
            fields["Contract Status"] = "CONFLICTED";
          }
          const r = await at(IDS.contractsTableId, "PATCH", {
            records: [{ id: liveRec.id, fields }],
          });
          if (!r.ok) throw new Error(`PATCH_FAIL ${JSON.stringify(r.json)}`);
          patched += 1;
          await sleep(150);
        }
        live.correctionsApplied = patched;
        live.applyMode = "RESEARCH_NOTES_AND_STATUS_ONLY";
        live.hpcUpdates = 0;
        live.evidenceDeleted = 0;
      }
    } catch (err) {
      live = { attempted: true, ok: false, error: String(err?.message || err) };
    }
  } else {
    live = { attempted: false, error: "AIRTABLE_TOKEN_ABSENT_LOCAL_AUDIT_ONLY" };
  }

  const report = {
    module: "V4_DATE_MEANING_INTEGRITY_AUDIT",
    at: new Date().toISOString(),
    reconcileMeta,
    observationsAudited: audited.length,
    summary,
    liveAirtable: live,
    applyCorrections: APPLY,
    publicationState: "CUSTOMER_BLOCKED",
    hpcUpdates: 0,
    evidencePreserved: true,
  };

  writeJson(path.join(OUT, "DATE_MEANING_AUDIT_REPORT.json"), {
    ...report,
    rows: audited,
  });
  writeJson(path.join(OUT, "DATE_MEANING_AUDIT_SUMMARY.json"), summary);
  writeJson(path.join(OUT, "DATE_MEANING_CORRECTION_MANIFEST.json"), manifest);

  console.log(
    JSON.stringify(
      {
        ok: true,
        audited: audited.length,
        summary,
        correctionsProposed: manifest.correctionCount,
        live,
        apply: APPLY,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
