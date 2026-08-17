/**
 * Autopilot v2 checkpoint + resume helpers.
 */

import fs from "node:fs";
import path from "node:path";

/**
 * @param {object} partial
 */
export function buildCheckpoint(partial = {}) {
  const runId = partial.run_id || partial.runId;
  return {
    version: "census-autopilot-checkpoint-v2",
    updated_at: new Date().toISOString(),
    run_id: runId,
    parent_company: partial.parent_company || null,
    scope: partial.scope || null,
    region: partial.region || null,
    country: partial.country || null,
    mode: partial.mode || null,
    strategy: partial.strategy || null,
    batch_size: partial.batch_size ?? null,
    current_queue: partial.current_queue || null,
    current_batch_number: partial.current_batch_number ?? 0,
    total_records_in_scope: partial.total_records_in_scope ?? 0,
    records_completed: partial.records_completed ?? 0,
    records_remaining: partial.records_remaining ?? 0,
    records_updated: partial.records_updated ?? 0,
    records_skipped: partial.records_skipped ?? 0,
    records_blocked: partial.records_blocked ?? 0,
    airtable_record_ids_written: partial.airtable_record_ids_written || [],
    fields_written: partial.fields_written || [],
    failed_records: partial.failed_records || [],
    completed_record_ids: partial.completed_record_ids || [],
    provider_decision_needed: partial.provider_decision_needed || [],
    next_cursor: partial.next_cursor || null,
    queues_remaining: partial.queues_remaining || [],
    completion_status: partial.completion_status || "in_progress",
    resume_command: runId ? `npm run census:autopilot -- --resume ${runId}` : null,
  };
}

/**
 * @param {string} runDir
 * @param {object} checkpoint
 */
export function saveCheckpoint(runDir, checkpoint) {
  const fp = path.join(runDir, "checkpoint.json");
  const payload = buildCheckpoint(checkpoint);
  fs.writeFileSync(fp, JSON.stringify(payload, null, 2), "utf8");
  return payload;
}

/**
 * @param {string} runDir
 */
export function loadCheckpoint(runDir) {
  const fp = path.join(runDir, "checkpoint.json");
  if (!fs.existsSync(fp)) return null;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

/**
 * Resolve run directory from run-id under autopilot root.
 * @param {string} autopilotRoot
 * @param {string} runId
 */
export function resolveRunDir(autopilotRoot, runId) {
  const direct = path.join(autopilotRoot, runId);
  if (fs.existsSync(path.join(direct, "checkpoint.json")) || fs.existsSync(direct)) {
    return direct;
  }
  // Allow bare folder match
  if (!fs.existsSync(autopilotRoot)) return direct;
  const entries = fs.readdirSync(autopilotRoot);
  const hit = entries.find((e) => e === runId || e.endsWith(`-${runId}`) || e.includes(runId));
  return hit ? path.join(autopilotRoot, hit) : direct;
}

/**
 * Filter work items, skipping already completed IDs.
 * @param {Array<{ record_id?: string, id?: string }>} items
 * @param {Iterable<string>} completedIds
 */
export function skipCompletedRecords(items = [], completedIds = []) {
  const done = new Set(completedIds);
  const remaining = [];
  const skipped = [];
  for (const item of items) {
    const id = item.record_id || item.id;
    if (id && done.has(id)) skipped.push(item);
    else remaining.push(item);
  }
  return { remaining, skipped_completed: skipped };
}

/**
 * Chunk an array into batches of batchSize (chunk size only — does not cap total scope).
 * @param {Array} items
 * @param {number} batchSize
 */
export function chunkByBatchSize(items = [], batchSize = 100) {
  const size = Math.max(1, Number(batchSize) || 100);
  const chunks = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

/**
 * Apply optional max-records cap for test/sample only (does not change batch-size meaning).
 * @param {Array} items
 * @param {number|null|undefined} maxRecords
 */
export function applyMaxRecordsCap(items = [], maxRecords = null) {
  if (maxRecords == null || !Number.isFinite(Number(maxRecords))) {
    return { items, capped: false, original_count: items.length };
  }
  const n = Math.max(0, Number(maxRecords));
  return {
    items: items.slice(0, n),
    capped: items.length > n,
    original_count: items.length,
    max_records: n,
  };
}
