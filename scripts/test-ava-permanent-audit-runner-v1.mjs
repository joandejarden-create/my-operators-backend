#!/usr/bin/env node
/**
 * Regression tests — AVA Permanent Audit Runner V1
 * No paid research. No production Airtable schema changes. No publish.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  runPermanentAvaAudit,
  runPermanentAvaAuditBatch,
  runPermanentAvaAuditAllEligible,
  compareAvaAuditRuns,
} from "../lib/ai-visibility-audit-action-plan/ava-permanent-audit-runner-v1.js";
import {
  buildAirtableSyncBundle,
  synchronizeAuditRunToAirtable,
  retryAirtableSync,
} from "../lib/ai-visibility-audit-action-plan/ava-audit-airtable-sync-v1.js";
import { loadAuditRun, listAuditRunsForProperty } from "../lib/ai-visibility-audit-action-plan/ava-audit-run-archive-v1.js";
import {
  reconcileAvaPayloadAgainstAdp,
  ZERO_VERDICTS,
} from "../lib/ai-visibility-audit-action-plan/adp-ava-reference-contract-v1.js";
import {
  certifyAvaAgainstCustomerFacingAdp,
  ADP_AVA_CERTIFICATION_STATUS,
} from "../lib/ai-visibility-audit-action-plan/adp-ava-permanent-cross-report-certification-v1.js";
import { AVA_AUDIT_EXECUTION_CONTRACT } from "../lib/ai-visibility-audit-action-plan/ava-audit-execution-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "ava-perm-runner-"));
const env = {
  ...process.env,
  AVA_AUDIT_RUNS_ROOT: path.join(tmpRoot, "runs"),
  AVA_AIRTABLE_MIRROR_ROOT: path.join(tmpRoot, "airtable-mirror"),
  AVA_AUDIT_AIRTABLE_SYNC_ENABLED: "0",
  AVA_AUDIT_PAID_RESEARCH_AUTHORIZED: "0",
};

function loadPayload(file) {
  return JSON.parse(fs.readFileSync(path.join(PAYLOAD_DIR, file), "utf8"));
}

console.log("1) Contract id");
assert.equal(AVA_AUDIT_EXECUTION_CONTRACT, "AVA_AUDIT_EXECUTION_CONTRACT_V1");

console.log("2) Single hotel dry-run");
{
  const r = await runPermanentAvaAudit("adp_cambridge_beaches_bermuda", {
    mode: "dry-run",
    env,
  });
  assert.equal(r.dryRun, true);
  assert.ok(r.identity?.contract === AVA_AUDIT_EXECUTION_CONTRACT);
  assert.ok(r.crossReport?.status);
  assert.ok(!fs.existsSync(path.join(env.AVA_AUDIT_RUNS_ROOT, "adp_cambridge_beaches_bermuda")), "dry-run must not archive");
}

console.log("3) Multiple hotels dry-run (3)");
{
  const batch = await runPermanentAvaAuditBatch(
    [
      "adp_cambridge_beaches_bermuda",
      "adp_ac_hotel_a_coruna",
      "adp_hilton_times_square",
    ],
    { mode: "dry-run", env }
  );
  assert.equal(batch.count, 3);
  assert.equal(batch.results.length, 3);
  const ids = new Set(batch.results.map((r) => r.auditRunId));
  assert.equal(ids.size, 3, "independent run ids");
}

console.log("4) Full portfolio dry-run (38)");
{
  const all = await runPermanentAvaAuditAllEligible({ mode: "dry-run", env });
  assert.equal(all.count, 38);
  assert.ok(all.results.every((r) => r.dryRun === true));
}

console.log("5) Measured zero preserved");
{
  const result = reconcileAvaPayloadAgainstAdp(loadPayload("cambridge-beaches-bermuda-v1.json"));
  const zeros = (result.zeros || []).filter((z) =>
    /restaurant/i.test(z.label || "")
  );
  assert.ok(zeros.length >= 1 || (result.zeros || []).some((z) => z.zeroVerdict === ZERO_VERDICTS.MEASURED_ZERO));
  for (const z of result.zeros || []) {
    if (z.zeroVerdict === ZERO_VERDICTS.MEASURED_ZERO) {
      assert.equal(z.adp, 0);
      assert.ok(Number(z.denominator) > 0);
    }
  }
}

console.log("6) Cross-report mismatch blocks certification");
{
  const payload = loadPayload("cambridge-beaches-bermuda-v1.json");
  const bad = structuredClone(payload);
  bad.kpis = (bad.kpis || []).map((k) =>
    /AI Consideration/i.test(k.label) ? { ...k, value: "1.1%" } : k
  );
  const cross = await certifyAvaAgainstCustomerFacingAdp(bad);
  assert.equal(cross.ok, false);
  assert.ok(
    cross.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED ||
      cross.status === ADP_AVA_CERTIFICATION_STATUS.RECONCILIATION_REQUIRED ||
      (cross.defects || []).length > 0
  );
}

console.log("7) Certify-only archive + historical preservation");
{
  const first = await runPermanentAvaAudit("adp_cambridge_beaches_bermuda", {
    mode: "certify-only",
    env,
  });
  assert.ok(first.auditRunId);
  assert.ok(first.archiveDir);
  assert.ok(fs.existsSync(path.join(first.archiveDir, "OUTPUT_MANIFEST.json")));
  assert.ok(fs.existsSync(path.join(first.archiveDir, "report.pdf")));
  const firstPdf = first.pdfSha256;

  // second version
  const second = await runPermanentAvaAudit("adp_cambridge_beaches_bermuda", {
    mode: "certify-only",
    env,
  });
  assert.notEqual(first.auditRunId, second.auditRunId);
  const runs = listAuditRunsForProperty("adp_cambridge_beaches_bermuda", env);
  assert.ok(runs.length >= 2);
  const loadedFirst = loadAuditRun(first.auditRunId, env);
  assert.equal(loadedFirst.ok, true);
  assert.ok(fs.existsSync(loadedFirst.pdfPath), "first version PDF must remain");
  assert.equal(loadedFirst.identity.pdfSha256, firstPdf);

  console.log("8) Reproduce frozen audit");
  const repro = await runPermanentAvaAudit(null, {
    mode: "reproduce",
    auditRunId: first.auditRunId,
    env,
  });
  assert.equal(repro.semanticContentParity, true);
  assert.equal(repro.contentSha256.match, true);
  if (repro.byteIdenticalPdf !== true) {
    assert.ok(repro.note && /bytes differ|No original PDF|Semantic/i.test(repro.note));
  }

  console.log("9) Historical compare");
  const cmp = compareAvaAuditRuns(first.auditRunId, second.auditRunId, env);
  assert.equal(cmp.ok, true);
  assert.ok(Array.isArray(cmp.persistentFindings) || Array.isArray(cmp.newFindings));
  assert.ok(Array.isArray(cmp.notResearchedOrDropped));

  console.log("10) Airtable mirror idempotency + failure recovery");
  const sync1 = await synchronizeAuditRunToAirtable(first.auditRunId, { env });
  assert.equal(sync1.status, "LOCAL_MIRROR_ONLY");
  assert.equal(sync1.airtableIndexed, false);
  assert.equal(sync1.archiveComplete, false);
  const bundle1 = buildAirtableSyncBundle(first.auditRunId, env);
  assert.equal(bundle1.ok, true);
  assert.equal(bundle1.mappingMode, "EXISTING_ADP_TABLES_V1");
  assert.ok(Array.isArray(bundle1.rows) && bundle1.rows.length >= 2);
  assert.equal(bundle1.rows[0].table, "ADP Report Snapshots");
  assert.equal(bundle1.rows[0].primaryValue, first.auditRunId);
  assert.ok(bundle1.forbiddenTables.includes("AI Demand Positioning - Published Reports"));
  const sync2 = await retryAirtableSync(first.auditRunId, { env });
  assert.equal(sync2.status, "LOCAL_MIRROR_ONLY");
  const bundle2 = buildAirtableSyncBundle(first.auditRunId, env);
  assert.equal(bundle1.rows[0].idempotencyKey, bundle2.rows[0].idempotencyKey);
  assert.equal(bundle1.rows[1].idempotencyKey, bundle2.rows[1].idempotencyKey);
  // local archive still present after mirror-only (no live write) sync
  assert.equal(loadAuditRun(first.auditRunId, env).ok, true);
  assert.ok(first.lifecycle?.CUSTOMER_PUBLISHED === false);
}

console.log("11) Historical ADP package labeling (when applicable)");
{
  // Find any dry-run with HISTORICAL_REFERENCE among portfolio sample
  const sample = await runPermanentAvaAudit("adp_spice_island_beach_resort", {
    mode: "dry-run",
    env,
  });
  if (sample.crossReport?.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_HISTORICAL_REFERENCE) {
    assert.equal(sample.crossReport.jointPublicationAllowed, false);
  } else {
    // Still assert joint delivery only when CERTIFIED_MATCH
    if (sample.crossReport?.status !== ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH) {
      assert.equal(sample.crossReport.jointPublicationAllowed, false);
    }
  }
}

console.log("12) PowerShell script exists and CLI --help");
{
  assert.ok(fs.existsSync(path.join(ROOT, "scripts/Run-AVA-Audit.ps1")));
  const help = spawnSync(process.execPath, ["scripts/run-ava-permanent-audit-v1.mjs", "--help"], {
    cwd: ROOT,
    encoding: "utf8",
  });
  assert.equal(help.status, 0);
  assert.match(help.stdout, /Permanent Audit Runner/i);
}

console.log("13) Wrong hotel ID fails explicitly");
{
  const bad = await runPermanentAvaAudit("recDOESNOTEXIST999", { mode: "dry-run", env });
  assert.equal(bad.ok, false);
  assert.ok(bad.error);
  assert.notEqual(bad.exitCode, 0);
}

console.log("14) Missing audit run reproduce fails explicitly");
{
  const bad = await runPermanentAvaAudit(null, {
    mode: "reproduce",
    auditRunId: "AVA-RUN-DOES-NOT-EXIST",
    env,
  });
  assert.equal(bad.ok, false);
  assert.ok(/NOT_FOUND|AUDIT_RUN/i.test(bad.error || ""));
}

console.log("15) Historical package is not treated as current-period match");
{
  const cam = await runPermanentAvaAudit("adp_cambridge_beaches_bermuda", {
    mode: "dry-run",
    env,
  });
  if (cam.crossReport?.status === "CERTIFIED_HISTORICAL_REFERENCE") {
    assert.equal(cam.crossReport.jointPublicationAllowed, false);
    assert.notEqual(cam.crossReport.status, "CERTIFIED_MATCH");
  }
}

console.log("16) Incomplete historical archive download contract");
{
  const missing = loadAuditRun("AVA-RUN-MISSING-ARCHIVE", env);
  assert.equal(missing.ok, false);
}

console.log("PASS AVA_PERMANENT_AUDIT_RUNNER_V1");
