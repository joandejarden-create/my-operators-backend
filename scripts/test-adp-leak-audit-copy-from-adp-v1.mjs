#!/usr/bin/env node
/**
 * npm run test:adp-leak-audit-copy-from-adp-v1
 */
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import {
  copyFromAdpToLeakAudit,
  buildClientSafeReportPayload,
  createLeakAuditStore,
} from "../lib/ai-demand-positioning/leak-audit/index.js";

const dry = await copyFromAdpToLeakAudit({ dryRun: true });
assert.equal(dry.ok, true);
assert.equal(dry.dryRun, true);
assert.equal(dry.plan.productionAdpWrites, 0);

const base = join(process.cwd(), "data/ai-demand-positioning/leak-audit/_tmp");
mkdirSync(base, { recursive: true });
const tmp = mkdtempSync(join(base, "copy-test-"));
const applied = await copyFromAdpToLeakAudit({ dryRun: false, root: tmp });
assert.equal(applied.ok, true);
assert.equal(applied.productionAdpTouched, false);
assert.match(applied.reportId, /^leak_rpt_/);
assert.match(applied.shareToken, /^share_/);
assert.ok(applied.storageBackend === "filesystem" || applied.storageBackend === "airtable");
assert.ok(applied.storageBackend === "filesystem" || applied.storageBackend === "airtable");

const store = createLeakAuditStore({ root: tmp });
const report = store.getReport(applied.reportId);
const run = store.getRun(applied.runId);
const request = store.getRequest(applied.requestId);
const payload = buildClientSafeReportPayload({ report, request, run });
assert.equal(/fullPromptText|"promptText"/.test(JSON.stringify(payload)), false);
assert.equal(/\brec[A-Za-z0-9]{14}\b/.test(JSON.stringify(payload)), false);
assert.equal(Object.prototype.hasOwnProperty.call(payload, "matchedHotelId"), false);
assert.equal(Object.prototype.hasOwnProperty.call(payload, "propertyId"), false);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_COPY_FROM_ADP_V1",
    dryRunOk: true,
    applyOk: true,
    shareToken: true,
  })
);
