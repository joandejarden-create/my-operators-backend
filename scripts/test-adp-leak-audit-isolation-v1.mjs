#!/usr/bin/env node
/**
 * Phase 1 isolation + workflow tests for ADP Demand Leak Audit Runner.
 * Proves free-audit writes stay in leak-audit store and do not touch production ADP paths.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, readFileSync, existsSync, statSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import {
  createLeakAuditStore,
  runLeakAuditPhase1,
  promoteLeakAuditToPilotStub,
  PROMOTE_CONFIRMATION_TEXT,
  buildClientSafeReportPayload,
  assertNoFullPromptInClientPayload,
  assertLeakAuditWritePathAllowed,
  assertNoProductionMutationImports,
  logLeakAuditProviderFailure,
  REQUEST_STATUS,
} from "../lib/ai-demand-positioning/leak-audit/index.js";

const tmpRoot = mkdtempSync(join(tmpdir(), "leak-audit-iso-"));
const store = createLeakAuditStore({ root: tmpRoot });

const productionFingerprints = [];

function fingerprintPath(p) {
  if (!existsSync(p)) return null;
  const st = statSync(p);
  if (st.isDirectory()) {
    const files = readdirSync(p).sort();
    return createHash("sha256").update(files.join("|")).digest("hex");
  }
  return createHash("sha256").update(readFileSync(p)).digest("hex");
}

const watchPaths = [
  join(process.cwd(), "data/ai-demand-positioning/runtime"),
  join(process.cwd(), "data/ai-demand-positioning/published"),
  join(process.cwd(), "lib/ai-demand-positioning/airtable-field-map.js"),
];

for (const p of watchPaths) {
  productionFingerprints.push({ path: p, before: fingerprintPath(p) });
}

// --- 1) Create request without touching hotel/production records
const request = store.createRequest({
  hotelName: "Isolation Test Hotel",
  hotelWebsite: "https://isolation-test-hotel.example",
  city: "Test City",
  country: "Testland",
  contactName: "Test Contact",
  contactEmail: "test@example.com",
  companyName: "Test Co",
  role: "Owner",
  demandSegmentOfInterest: "meetings_groups",
  source: "manual",
  notes: "isolation test",
});
assert.equal(request.status, REQUEST_STATUS.REQUESTED);
assert.ok(request.id.startsWith("leak_req_"));

// --- 2) Approve + run
store.updateRequest(request.id, { status: REQUEST_STATUS.APPROVED });
const result = await runLeakAuditPhase1(store, {
  requestId: request.id,
  liveProviderCalls: false,
});
assert.equal(result.liveProviderCalls, 0);
assert.ok(result.run.id.startsWith("leak_run_"));
assert.equal(result.run.auditRequestId, request.id);
assert.equal(result.run.status, "completed");
assert.ok(result.observations.length > 0);
assert.ok(result.observations.length <= 20);
assert.ok(result.report.id.startsWith("leak_rpt_"));

// Observations only under leak-audit observations collection
for (const obs of result.observations) {
  assert.equal(obs.auditRunId, result.run.id);
  assert.equal(Object.prototype.hasOwnProperty.call(obs, "fullPromptText"), false);
  assert.equal(Object.prototype.hasOwnProperty.call(obs, "promptText"), false);
  assert.ok(obs.promptId);
  assert.ok(obs.promptLabel);
  assert.ok(obs.promptIntentSummary);
}

// --- 3) Client report uses only leak-audit records; no full prompts
const client = buildClientSafeReportPayload({
  report: result.report,
  request: result.request,
  run: result.run,
});
assert.ok(client);
assert.equal(client.reportId, result.report.id);
assert.ok(!("matchedHotelId" in client));
assert.ok(!("fullPromptText" in client));
const promptCheck = assertNoFullPromptInClientPayload(client);
assert.equal(promptCheck.ok, true);

// Report content should not embed INTERNAL prompt templates
const reportBlob = JSON.stringify(client);
assert.equal(/INTERNAL —/i.test(reportBlob), false);
assert.equal(/fullPromptText/i.test(reportBlob), false);

// --- 4) Promote requires explicit confirmation
assert.throws(
  () => promoteLeakAuditToPilotStub(store, { requestId: request.id, confirmed: false }),
  /promotion_requires_explicit_confirmation/
);
assert.throws(
  () => promoteLeakAuditToPilotStub(store, { requestId: request.id }),
  /promotion_requires_explicit_confirmation/
);

const promo = promoteLeakAuditToPilotStub(store, {
  requestId: request.id,
  confirmed: true,
  confirmedBy: "isolation-test",
});
assert.equal(promo.productionAdpRecordCreated, false);
assert.equal(promo.request.status, REQUEST_STATUS.CONVERTED);
assert.ok(PROMOTE_CONFIRMATION_TEXT.includes("No production records will be overwritten"));

// --- 5) Failed provider calls logged on leak run only
const failedRun = store.createRun({
  auditRequestId: request.id,
  status: "running",
  providersUsed: ["openai"],
  demandTerritoriesTested: ["Leisure Travel"],
});
const updatedFail = logLeakAuditProviderFailure(store, failedRun.id, {
  provider: "openai",
  message: "simulated_provider_timeout",
});
assert.ok(updatedFail.errorLog.some((e) => e.message === "simulated_provider_timeout"));

// --- 6) Write path isolation
assert.throws(() => {
  assertLeakAuditWritePathAllowed(
    join(process.cwd(), "data/ai-demand-positioning/runtime/should-not-write.json"),
    { root: tmpRoot }
  );
}, /leak_audit_write_path_forbidden/);

assert.doesNotThrow(() => {
  assertLeakAuditWritePathAllowed(join(tmpRoot, "requests", "x.json"), { root: tmpRoot });
});

// --- 7) Leak-audit modules must not call production mutation entry points
const leakAuditFiles = [
  "run-audit-v1.js",
  "store-v1.js",
  "promote-stub-v1.js",
  "report-generator-v1.js",
  "observation-process-v1.js",
];
for (const file of leakAuditFiles) {
  const src = readFileSync(
    join(process.cwd(), "lib/ai-demand-positioning/leak-audit", file),
    "utf8"
  );
  const check = assertNoProductionMutationImports(src);
  assert.equal(check.ok, true, `${file} imports production mutations: ${check.hits}`);
  assert.equal(src.includes("savePeriod("), false, file);
  assert.equal(src.includes("savePublishedSnapshotBundle("), false, file);
}

// --- 8) Production fingerprints unchanged
for (const item of productionFingerprints) {
  const after = fingerprintPath(item.path);
  assert.equal(
    after,
    item.before,
    `production path changed during leak audit test: ${item.path}`
  );
}

// All store writes stayed under tmpRoot
assert.ok(store.root.startsWith(tmpRoot) || store.root === tmpRoot);

rmSync(tmpRoot, { recursive: true, force: true });

console.log(
  JSON.stringify(
    {
      pass: true,
      gate: "ADP_LEAK_AUDIT_ISOLATION_V1",
      requestId: request.id,
      runId: result.run.id,
      reportId: result.report.id,
      observationCount: result.observations.length,
      productionAdpRecordCreated: false,
      liveProviderCalls: 0,
    },
    null,
    2
  )
);
