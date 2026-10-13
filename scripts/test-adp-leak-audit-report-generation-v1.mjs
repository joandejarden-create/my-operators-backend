#!/usr/bin/env node
/**
 * npm run test:adp-leak-audit-report-generation-v1
 */
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import {
  createLeakAuditStore,
  generateSinglePropertyLeakAuditReport,
  markLeakAuditReportSent,
  promoteLeakAuditToPilotStub,
  buildClientSafeReportPayload,
} from "../lib/ai-demand-positioning/leak-audit/index.js";

const base = join(process.cwd(), "data/ai-demand-positioning/leak-audit/_tmp");
mkdirSync(base, { recursive: true });
const tmp = mkdtempSync(join(base, "gen-test-"));
const store = createLeakAuditStore({ root: tmp });
const request = store.createRequest({
  hotelName: "Cambridge Beaches Resort & Spa",
  city: "Sandys Parish",
  country: "Bermuda",
  source: "manual",
});
store.updateRequest(request.id, { status: "approved" });

const generated = await generateSinglePropertyLeakAuditReport(store, {
  requestId: request.id,
  mode: "sample_seed",
});
assert.equal(generated.ok, true);
assert.ok(generated.hotel?.id);
assert.ok(generated.run?.id);
assert.ok(generated.report?.shareToken);
assert.match(generated.shareUrl, /\/adp-leak-audit\/share\//);

assert.ok(store.listMetrics({ runId: generated.run.id }).length >= 5);
assert.ok(store.listCompetitors({ runId: generated.run.id }).length >= 1);
assert.ok(store.listEvidence({ runId: generated.run.id }).length >= 1);
assert.ok(store.listActions({ runId: generated.run.id }).length >= 3);

const byToken = store.getReportByShareToken(generated.report.shareToken);
assert.equal(byToken.id, generated.report.id);

const payload = buildClientSafeReportPayload({
  report: generated.report,
  request: store.getRequest(request.id),
  run: generated.run,
});
assert.ok(payload.executiveSummary?.paragraphs?.length >= 4);
assert.match(payload.recommendedNextStep || "", /If useful/);

const sent = markLeakAuditReportSent(store, generated.report.id);
assert.equal(sent.reportStatus, "sent");
assert.ok(sent.sentAt);

const promo = promoteLeakAuditToPilotStub(store, {
  requestId: request.id,
  confirmed: true,
  confirmedBy: "test",
});
assert.equal(promo.productionAdpRecordCreated, false);
assert.ok(promo.promotion?.id);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_REPORT_GENERATION_V1",
    shareToken: true,
    markSent: true,
    promoteStubOnly: true,
  })
);
