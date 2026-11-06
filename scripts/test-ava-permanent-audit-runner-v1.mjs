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
  loadAuthoritativeAdpReference,
  ZERO_VERDICTS,
} from "../lib/ai-visibility-audit-action-plan/adp-ava-reference-contract-v1.js";
import {
  certifyAvaAgainstCustomerFacingAdp,
  ADP_AVA_CERTIFICATION_STATUS,
} from "../lib/ai-visibility-audit-action-plan/adp-ava-permanent-cross-report-certification-v1.js";
import { AVA_AUDIT_EXECUTION_CONTRACT } from "../lib/ai-visibility-audit-action-plan/ava-audit-execution-contract-v1.js";

/** Version-bound pin for Cambridge current-period ADP (values loaded live from lean/realityGap). */
const CAMBRIDGE_CURRENT_PERIOD_PIN = Object.freeze({
  propertyId: "adp_cambridge_beaches_bermuda",
  periodId: "adp_period_adp_cambridge_beaches_bermuda_20261105215424_e93d82",
  leanBaselineSha256:
    "bfb1396373e9002ae3164b6f32ecf637a996ebfbc2ccff3d25ec1a69686d6aae",
  historicalPeriodId:
    "adp_period_adp_cambridge_beaches_bermuda_20260903010829_044437",
  attributeId: "on_site_restaurant",
  attributeLabel: "On-Site Restaurant",
});

function cambridgeOnSiteRestaurantFromAdp() {
  const adp = loadAuthoritativeAdpReference(CAMBRIDGE_CURRENT_PERIOD_PIN.propertyId);
  assert.equal(adp.ok, true, adp.error || "Cambridge lean pin missing");
  assert.equal(adp.periodId, CAMBRIDGE_CURRENT_PERIOD_PIN.periodId);
  assert.equal(adp.baselineSha256, CAMBRIDGE_CURRENT_PERIOD_PIN.leanBaselineSha256);
  const rows = [
    ...(adp.realityGap?.gaps || []),
    ...(adp.realityGap?.recognized || []),
    ...(adp.realityGap?.attributes || []),
  ];
  const attr = rows.find(
    (a) => String(a.attribute || "") === CAMBRIDGE_CURRENT_PERIOD_PIN.attributeId
  );
  assert.ok(attr, "current-period ADP realityGap missing on_site_restaurant");
  assert.equal(attr.measurementState, "TESTED_RECOGNIZED");
  assert.ok(Number(attr.total) > 0, "eligible denominator required");
  assert.notEqual(attr.recognitionRate, null);
  // recognitionRate must equal count/total (certified measurement math)
  const derived =
    Math.round((Number(attr.count) / Number(attr.total)) * 1000) / 10;
  assert.equal(Number(attr.recognitionRate), derived);
  return {
    adp,
    attr,
    recognitionRate: Number(attr.recognitionRate),
    numerator: Number(attr.count),
    denominator: Number(attr.total),
    measurementState: String(attr.measurementState),
  };
}

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

console.log("5a) Cambridge current-period On-Site Restaurant matches certified ADP recognitionRate");
{
  const certified = cambridgeOnSiteRestaurantFromAdp();
  const payload = loadPayload("cambridge-beaches-bermuda-v1.json");
  assert.equal(
    payload.reporting?.adpPeriodId,
    CAMBRIDGE_CURRENT_PERIOD_PIN.periodId,
    "AVA payload must bind current ADP period"
  );
  const result = reconcileAvaPayloadAgainstAdp(payload);
  assert.equal(result.ok, true, JSON.stringify(result.defects, null, 2));
  const rest = (result.refs || []).find(
    (r) =>
      r.kind === "ATTRIBUTE_RECOGNITION" &&
      /on.?site.?restaurant/i.test(String(r.label || ""))
  );
  assert.ok(rest, "AVA must cite On-Site Restaurant from current ADP realityGap");
  assert.equal(rest.adp, certified.recognitionRate);
  assert.equal(rest.ava, certified.recognitionRate);
  assert.equal(rest.match, true);
  assert.equal(rest.numerator, certified.numerator);
  assert.equal(rest.denominator, certified.denominator);
  assert.equal(rest.measurementState, certified.measurementState);
  // Stale historical 0% must not appear as the current-period certified value
  assert.notEqual(certified.recognitionRate, 0);
  assert.notEqual(rest.ava, 0);
}

console.log("5b) Historical measured-zero cannot overwrite current-period certified recognition");
{
  const certified = cambridgeOnSiteRestaurantFromAdp();
  assert.ok(certified.recognitionRate > 0);
  assert.notEqual(ZERO_VERDICTS.MEASURED_ZERO, ZERO_VERDICTS.NOT_MEASURED);

  // Historical observation: measured zero (tested, eligible n>0, rate 0) — distinct from NOT_MEASURED
  const historicalMeasuredZero = {
    attribute: CAMBRIDGE_CURRENT_PERIOD_PIN.attributeId,
    label: CAMBRIDGE_CURRENT_PERIOD_PIN.attributeLabel,
    recognitionRate: 0,
    count: 0,
    total: certified.denominator,
    measurementState: "TESTED_NOT_RECOGNIZED",
  };
  assert.equal(historicalMeasuredZero.recognitionRate, 0);
  assert.ok(historicalMeasuredZero.total > 0);
  assert.match(historicalMeasuredZero.measurementState, /TESTED/i);
  assert.notEqual(historicalMeasuredZero.recognitionRate, certified.recognitionRate);

  // If AVA still narrated the historical 0%, lean/current cert must reject it
  const stale = structuredClone(loadPayload("cambridge-beaches-bermuda-v1.json"));
  const staleClaim = `0% recognition for ${CAMBRIDGE_CURRENT_PERIOD_PIN.attributeLabel}`;
  const currentClaim = `${certified.recognitionRate}% recognition for ${CAMBRIDGE_CURRENT_PERIOD_PIN.attributeLabel}`;
  const blob = JSON.stringify(stale);
  assert.ok(blob.includes(currentClaim) || blob.includes(`${certified.recognitionRate}%`), "fixture must contain current rate");
  const poisoned = JSON.parse(
    blob.split(String(certified.recognitionRate) + "%").join("0%")
  );
  poisoned.reporting = {
    ...poisoned.reporting,
    adpPeriodId: CAMBRIDGE_CURRENT_PERIOD_PIN.periodId,
    adpHistoricalPeriodSuperseded: CAMBRIDGE_CURRENT_PERIOD_PIN.historicalPeriodId,
  };
  const staleParity = reconcileAvaPayloadAgainstAdp(poisoned);
  assert.equal(staleParity.ok, false, "historical 0% must not pass against current ADP pin");
  assert.ok(
    (staleParity.defects || []).some(
      (d) =>
        d.type === "METRIC_MISMATCH" &&
        /restaurant/i.test(String(d.metric || d.label || ""))
    ) ||
      (staleParity.zeros || []).some(
        (z) =>
          /restaurant/i.test(String(z.label || "")) &&
          z.ava === 0 &&
          z.adp === certified.recognitionRate
      ),
    JSON.stringify(
      {
        defects: staleParity.defects,
        zeros: staleParity.zeros,
        expectedAdp: certified.recognitionRate,
        staleClaim,
      },
      null,
      2
    )
  );
  const staleCert = await certifyAvaAgainstCustomerFacingAdp(poisoned);
  assert.notEqual(staleCert.status, ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH);
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

console.log("15) Cambridge current-period dry-run is CERTIFIED_MATCH (historical pin superseded)");
{
  const cam = await runPermanentAvaAudit("adp_cambridge_beaches_bermuda", {
    mode: "dry-run",
    env,
  });
  assert.equal(cam.crossReport?.status, ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH);
  assert.equal(cam.crossReport?.jointPublicationAllowed, true);
  assert.equal(
    cam.identity?.adpPeriodId || cam.crossReport?.crossReportIdentity?.adpPeriodId || null,
    CAMBRIDGE_CURRENT_PERIOD_PIN.periodId
  );
}

console.log("16) Incomplete historical archive download contract");
{
  const missing = loadAuditRun("AVA-RUN-MISSING-ARCHIVE", env);
  assert.equal(missing.ok, false);
}

console.log("PASS AVA_PERMANENT_AUDIT_RUNNER_V1");
