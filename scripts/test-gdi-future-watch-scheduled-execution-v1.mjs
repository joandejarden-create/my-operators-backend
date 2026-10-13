#!/usr/bin/env node
/**
 * Tests: GDI Future Watch Scheduled Execution V1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import {
  WATCH_STATUS,
  DATE_PROVENANCE,
  PROVIDER_ERROR_CLASS,
  queryScheduledDueWatches,
  buildFutureWatchRecord,
  classifyProviderFailure,
  applyProviderRetry,
  claimWatch,
  releaseWatchClaim,
  acquireGlobalSchedulerLock,
  releaseGlobalSchedulerLock,
  resolveSchedulerConfig,
  isForceDueAllowed,
  shouldStopBatch,
  createBatchCostLedger,
  updateBatchCost,
  buildWatchResearchBatch,
} from "../lib/group-demand-intelligence/future-watch/index.js";

let n = 0;
function ok(name) {
  n += 1;
  console.log(`PASS ${name}`);
}

function sampleWatch(overrides = {}) {
  return buildFutureWatchRecord({
    candidateId: overrides.candidateId || "ac_watch_21",
    hotelShort: "AC",
    hpc: "rec2PVBDavppGpenm",
    event: "Congreso END Santiago 2027",
    futureCycle: "2027",
    primaryBlocker: "HOUSING_STATUS",
    stateAfter: "FUTURE_WATCH",
    nextTriggerType: "HOUSING_OPEN",
    ...overrides,
  });
}

{
  const w = sampleWatch();
  w.nextResearchDate = "2030-01-01";
  const due = queryScheduledDueWatches([w], { now: new Date("2026-09-29") });
  assert.equal(due.length, 0);
  ok("0_due");
}

{
  const w = sampleWatch({ candidateId: "due1" });
  w.nextResearchDate = "2020-01-01";
  const due = queryScheduledDueWatches([w], { now: new Date("2026-09-29") });
  assert.equal(due.length, 1);
  assert.equal(due[0].dueReason, "HEURISTIC_DUE");
  ok("1_due_heuristic");
}

{
  const a = sampleWatch({ candidateId: "a" });
  a.nextResearchDate = "2020-01-01";
  a.dateProvenance = DATE_PROVENANCE.EVIDENCE_BASED;
  const b = sampleWatch({ candidateId: "b" });
  b.nextResearchDate = "2020-01-01";
  b.dateProvenance = DATE_PROVENANCE.HEURISTIC;
  const due = queryScheduledDueWatches([b, a], {
    now: new Date("2026-09-29"),
    triggersDetected: { [a.watchId]: true },
  });
  assert.ok(due.length >= 2);
  assert.equal(due[0].duePriority, 1); // explicit trigger first
  ok("multiple_due_priority");
}

{
  const ceiling = sampleWatch({ candidateId: "pdc", stateAfter: "PUBLIC_DATA_CEILING" });
  ceiling.watchStatus = WATCH_STATUS.PUBLIC_DATA_CEILING;
  ceiling.nextResearchDate = "2099-01-01";
  const due = queryScheduledDueWatches([ceiling], { now: new Date("2026-09-29") });
  assert.equal(due.length, 0);
  ok("public_data_ceiling_exclusion");
}

{
  const c429 = classifyProviderFailure({ status: 429, message: "rate limit" });
  assert.equal(c429.class, PROVIDER_ERROR_CLASS.RETRYABLE_PROVIDER_ERROR);
  const c5 = classifyProviderFailure({ status: 503 });
  assert.equal(c5.class, PROVIDER_ERROR_CLASS.RETRYABLE_PROVIDER_ERROR);
  const cto = classifyProviderFailure({ message: "Timeout waiting" });
  assert.equal(cto.class, PROVIDER_ERROR_CLASS.RETRYABLE_PROVIDER_ERROR);
  ok("429_timeout_5xx_classification");
}

{
  const w = sampleWatch();
  const r1 = applyProviderRetry(w, { class: PROVIDER_ERROR_CLASS.RETRYABLE_PROVIDER_ERROR, reason: "RATE_LIMIT_429" }, { maxRetries: 3, retryBackoffDays: [1, 3, 7] });
  assert.equal(r1.retryCount, 1);
  assert.equal(r1.watchStatus, WATCH_STATUS.ACTIVE);
  assert.notEqual(r1.watchStatus, WATCH_STATUS.REJECTED);
  let cur = r1;
  cur = applyProviderRetry(cur, { class: PROVIDER_ERROR_CLASS.RETRYABLE_PROVIDER_ERROR, reason: "x" }, { maxRetries: 3, retryBackoffDays: [1, 3, 7] });
  cur = applyProviderRetry(cur, { class: PROVIDER_ERROR_CLASS.RETRYABLE_PROVIDER_ERROR, reason: "x" }, { maxRetries: 3, retryBackoffDays: [1, 3, 7] });
  cur = applyProviderRetry(cur, { class: PROVIDER_ERROR_CLASS.RETRYABLE_PROVIDER_ERROR, reason: "x" }, { maxRetries: 3, retryBackoffDays: [1, 3, 7] });
  assert.equal(cur.watchStatus, WATCH_STATUS.RESEARCH_BLOCKED_PROVIDER);
  ok("retry_exhaustion");
}

{
  const cfg = resolveSchedulerConfig();
  assert.equal(cfg.maxJevActionsPerWatch, 1);
  assert.ok(cfg.maxWatchesPerBatch >= 1);
  ok("jev_one_action_and_budget_config");
}

{
  delete process.env.GDI_WATCH_ALLOW_FORCE_DUE;
  process.env.NODE_ENV = "test";
  assert.equal(isForceDueAllowed({}), false);
  process.env.GDI_WATCH_ALLOW_FORCE_DUE = "1";
  assert.equal(isForceDueAllowed({}), true);
  process.env.NODE_ENV = "production";
  assert.equal(isForceDueAllowed({ allowForceDue: true }), false);
  delete process.env.NODE_ENV;
  delete process.env.GDI_WATCH_ALLOW_FORCE_DUE;
  ok("forced_due_dev_guard");
}

{
  const ledger = createBatchCostLedger("t");
  updateBatchCost(ledger, { jevCalls: 1, queries: 10, fetches: 20 });
  // force high cost
  ledger.estimatedCostUsd = 30;
  const stop = shouldStopBatch(ledger, { maxEstimatedCostUsd: 25, maxProviderErrors: 5 });
  assert.equal(stop.stop, true);
  assert.equal(stop.reason, "COST_GUARD");
  ok("budget_stop");
}

{
  // Concurrency locks — use real data root; clean up after
  const id = `test_watch_${Date.now()}`;
  const c1 = claimWatch(id, { runId: "runA", staleClaimMs: 60_000 });
  assert.equal(c1.ok, true);
  const c2 = claimWatch(id, { runId: "runB", staleClaimMs: 60_000 });
  assert.equal(c2.ok, false);
  releaseWatchClaim(id, c1.claimToken);
  const c3 = claimWatch(id, { runId: "runC", staleClaimMs: 60_000 });
  assert.equal(c3.ok, true);
  releaseWatchClaim(id, c3.claimToken);

  // Stale recovery
  const id2 = `test_stale_${Date.now()}`;
  const s1 = claimWatch(id2, { runId: "old", staleClaimMs: 1 });
  assert.equal(s1.ok, true);
  // wait tiny bit beyond stale
  const start = Date.now();
  while (Date.now() - start < 5) { /* spin */ }
  const s2 = claimWatch(id2, { runId: "new", staleClaimMs: 1 });
  assert.equal(s2.ok, true);
  assert.equal(s2.staleRecovered, true);
  releaseWatchClaim(id2, s2.claimToken);
  ok("concurrency_lock_and_stale_recovery");
}

{
  const g1 = acquireGlobalSchedulerLock("g1", { staleClaimMs: 60_000 });
  assert.equal(g1.ok, true);
  const g2 = acquireGlobalSchedulerLock("g2", { staleClaimMs: 60_000 });
  assert.equal(g2.ok, false);
  releaseGlobalSchedulerLock("g1");
  const g3 = acquireGlobalSchedulerLock("g3", { staleClaimMs: 60_000 });
  assert.equal(g3.ok, true);
  releaseGlobalSchedulerLock("g3");
  ok("global_scheduler_lock");
}

{
  const w = sampleWatch({ candidateId: "idem" });
  w.nextResearchDate = "2020-01-01";
  const due = queryScheduledDueWatches([w], { now: new Date() });
  const b1 = buildWatchResearchBatch(due, { batchId: "batch1" });
  assert.equal(b1.selected.length, 1);
  const b2 = buildWatchResearchBatch(due, {
    batchId: "batch1",
    processedIds: [b1.selected[0].idempotencyKey],
  });
  assert.equal(b2.selected.length, 0);
  ok("idempotency_batch_keys");
}

console.log(`\n${n} tests passed`);
