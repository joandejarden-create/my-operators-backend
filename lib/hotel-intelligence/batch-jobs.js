/**
 * Controlled batch census enrichment progress (local).
 * States: pending | running | enriched | review_required | failed | completed
 * Quota exhaustion pauses provider work without destroying completed work.
 */

import { randomBytes } from "node:crypto";
import { createLocalStore } from "./local-store.js";

export const BATCH_JOBS_VERSION = "hotel-intelligence-batch-jobs-v1";

export const BATCH_STATUS = Object.freeze({
  PENDING: "pending",
  RUNNING: "running",
  ENRICHED: "enriched",
  REVIEW_REQUIRED: "review_required",
  FAILED: "failed",
  COMPLETED: "completed",
  PAUSED_QUOTA: "paused_quota",
});

function newBatchId() {
  return `batch_${randomBytes(6).toString("hex")}`;
}

/**
 * @param {ReturnType<typeof createLocalStore>} [store]
 */
export function createBatchJobStore(store = createLocalStore()) {
  function list() {
    return store.readBatches().jobs || [];
  }

  function get(batchId) {
    return list().find((j) => j.batch_id === batchId) || null;
  }

  function create(records, opts = {}) {
    const job = {
      batch_id: opts.batch_id || newBatchId(),
      status: BATCH_STATUS.PENDING,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      record_count: (records || []).length,
      records: (records || []).map((r, i) => ({
        index: i,
        input: r,
        status: BATCH_STATUS.PENDING,
        hotel_id: null,
        error: null,
        result: null,
      })),
      provider_pause: null,
      stats: {
        pending: (records || []).length,
        running: 0,
        enriched: 0,
        review_required: 0,
        failed: 0,
        completed: 0,
      },
    };
    const db = store.readBatches();
    db.jobs = [...(db.jobs || []), job];
    store.writeBatches(db);
    return job;
  }

  function save(job) {
    const db = store.readBatches();
    const idx = (db.jobs || []).findIndex((j) => j.batch_id === job.batch_id);
    job.updated_at = new Date().toISOString();
    job.stats = summarize(job);
    if (idx >= 0) db.jobs[idx] = job;
    else db.jobs = [...(db.jobs || []), job];
    store.writeBatches(db);
    return job;
  }

  function summarize(job) {
    const stats = {
      pending: 0,
      running: 0,
      enriched: 0,
      review_required: 0,
      failed: 0,
      completed: 0,
      paused_quota: 0,
    };
    for (const r of job.records || []) {
      const k = r.status;
      if (stats[k] != null) stats[k] += 1;
      else stats.pending += 1;
    }
    return stats;
  }

  function markPausedQuota(job, message) {
    job.status = BATCH_STATUS.PAUSED_QUOTA;
    job.provider_pause = {
      provider: "hotelbeds",
      status: "quota_exhausted",
      retryable: true,
      message: message || "TEST_DAILY_QUOTA_EXHAUSTED",
      at: new Date().toISOString(),
    };
    return save(job);
  }

  return {
    version: BATCH_JOBS_VERSION,
    BATCH_STATUS,
    list,
    get,
    create,
    save,
    summarize,
    markPausedQuota,
  };
}
