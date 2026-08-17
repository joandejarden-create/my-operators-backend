/**
 * Autopilot v2 batch engine — run-until-complete, checkpoints, production writes.
 */

import fs from "node:fs";
import path from "node:path";

import {
  applyPreflight,
  checkAutopilotApplyEnv,
  guardApplyBatch,
  guardProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
} from "./census-autopilot-apply-guard.js";
import { routeWebhoundCandidates } from "./census-autopilot-queue-router.js";
import {
  applyMaxRecordsCap,
  chunkByBatchSize,
  saveCheckpoint,
  skipCompletedRecords,
} from "./census-autopilot-checkpoint.js";
import { classifyIdempotentProposals } from "./census-autopilot-idempotent-writer.js";
import {
  AUTOPILOT_TARGET_TABLE,
  AUTOPILOT_TARGET_BASE_LABEL,
  AUTOPILOT_TARGET_TABLE_ID,
} from "./census-autopilot-field-allowlist.js";
import { tallyConfidence } from "./census-autopilot-confidence.js";
import { evaluateProviderReadiness } from "./production-census-description-extraction.js";
import { createRuntimeMetrics } from "./census-autopilot-runtime-guardrails.js";
import { productionHotelPropertyCensus } from "./production-census-source-of-truth.js";

export const BATCH_ENGINE_VERSION = "census-autopilot-batch-engine-v2";

export const COMPLETION_STATUS = Object.freeze({
  COMPLETE: "complete",
  PARTIAL_RESUME: "partial_complete_resume_available",
  BLOCKED_PROVIDER: "blocked_provider_decision",
  BLOCKED_SOURCE: "blocked_source_access",
  BLOCKED_SCHEMA: "blocked_schema_needed",
  BLOCKED_SAFETY: "blocked_safety_failure",
  IN_PROGRESS: "in_progress",
});

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2), "utf8");
}

function writeText(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text, "utf8");
}

function padBatch(n) {
  return String(n).padStart(3, "0");
}

/**
 * Default no-op Airtable adapters (tests inject mocks).
 */
export function createMemoryAirtableAdapter(seedRecords = {}, targetOverride = null) {
  const store = { ...seedRecords };
  const target = targetOverride || {
    base: AUTOPILOT_TARGET_BASE_LABEL,
    baseName: AUTOPILOT_TARGET_BASE_LABEL,
    table: AUTOPILOT_TARGET_TABLE,
    tableName: AUTOPILOT_TARGET_TABLE,
    tableId: AUTOPILOT_TARGET_TABLE_ID,
  };
  return {
    target,
    async readRecords(ids = []) {
      return ids.map((id) => ({
        id,
        fields: { ...(store[id]?.fields || {}) },
      }));
    },
    async patchRecords(updates = []) {
      const gate = guardProductionCensusWriteTarget(target);
      if (!gate.ok) {
        return {
          updated: 0,
          errors: [{ error: BLOCKED_WRONG_CENSUS_TARGET, details: gate }],
          written: [],
          blocked_wrong_census_target: true,
        };
      }
      const written = [];
      for (const u of updates) {
        if (!store[u.id]) store[u.id] = { id: u.id, fields: {} };
        store[u.id].fields = { ...store[u.id].fields, ...u.fields };
        written.push({ id: u.id, fields: u.fields });
      }
      return { updated: written.length, errors: [], written };
    },
    _store: store,
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Live Hotel Property Census read/patch adapter (production cycle / apply).
 * Fail-closed on wrong table ID.
 */
export function createLiveHotelPropertyCensusAdapter(opts = {}) {
  const baseId = opts.baseId;
  const token = opts.token;
  const tableId = opts.tableId || AUTOPILOT_TARGET_TABLE_ID;
  const target = {
    base: AUTOPILOT_TARGET_BASE_LABEL,
    baseName: AUTOPILOT_TARGET_BASE_LABEL,
    baseId,
    table: AUTOPILOT_TARGET_TABLE,
    tableName: AUTOPILOT_TARGET_TABLE,
    tableId,
  };
  return {
    target,
    async readRecords(ids = []) {
      const out = [];
      for (const id of ids) {
        const res = await fetch(
          `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}/${encodeURIComponent(id)}`,
          { headers: { Authorization: `Bearer ${token}` } }
        );
        const json = await res.json().catch(() => ({}));
        if (!res.ok) {
          throw new Error(`census get ${id} ${res.status}: ${JSON.stringify(json.error || json)}`);
        }
        out.push({ id: json.id, fields: json.fields || {} });
        await sleep(80);
      }
      return out;
    },
    async patchRecords(updates = []) {
      const gate = guardProductionCensusWriteTarget(target);
      if (!gate.ok || tableId !== AUTOPILOT_TARGET_TABLE_ID) {
        return {
          updated: 0,
          errors: [{ error: BLOCKED_WRONG_CENSUS_TARGET, details: gate }],
          written: [],
          blocked_wrong_census_target: true,
        };
      }
      const written = [];
      const errors = [];
      for (let i = 0; i < updates.length; i += 10) {
        const chunk = updates.slice(i, i + 10).map((u) => ({
          id: u.id,
          fields: u.fields,
        }));
        const res = await fetch(
          `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
          {
            method: "PATCH",
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({ records: chunk, typecast: true }),
          }
        );
        const json = await res.json().catch(() => ({}));
        if (!res.ok) {
          errors.push({ error: json.error || json, status: res.status, chunk_ids: chunk.map((c) => c.id) });
          await sleep(200);
          continue;
        }
        for (const r of json.records || []) {
          const req = chunk.find((c) => c.id === r.id);
          written.push({ id: r.id, fields: req?.fields || {} });
        }
        await sleep(180);
      }
      return { updated: written.length, errors, written };
    },
  };
}

/**
 * Process one batch of proposals with re-read + idempotent write.
 * @param {object[]} batchProposals
 * @param {object} ctx
 */
export async function processAutopilotBatch(batchProposals, ctx = {}) {
  const {
    airtable,
    allowGeocode = false,
    schemaV114Ready = false,
    threshold = "High",
    mode = "dry-run",
    doWrite = false,
  } = ctx;

  const ids = batchProposals.map((p) => p.record_id || p.id).filter(Boolean);
  let liveById = new Map();
  if (airtable?.readRecords && ids.length) {
    const live = await airtable.readRecords(ids);
    liveById = new Map(live.map((r) => [r.id, r]));
  }

  const withCurrent = batchProposals.map((p) => {
    const id = p.record_id || p.id;
    const live = liveById.get(id);
    return {
      ...p,
      record_id: id,
      current_fields: live?.fields || p.current_fields || {},
    };
  });

  const guarded = guardApplyBatch(withCurrent, {
    allowGeocode,
    schemaV114Ready,
    threshold,
  });

  if (guarded.stop_all) {
    return {
      ok: false,
      stop: true,
      stop_reason: "blocked_safety_failure",
      completion_status: COMPLETION_STATUS.BLOCKED_SAFETY,
      guarded,
      written: [],
      skipped: [],
      blocked: guarded.blocked,
      provider_decision_needed: guarded.provider_decision_needed,
    };
  }

  const classified = classifyIdempotentProposals(
    guarded.writable.map((p) => ({
      ...p,
      patch: p.patch || p.guard?.sanitized_fields || p.fields,
    })),
    { allowGeocode, schemaV114Ready, threshold }
  );

  // Merge soft provider from guard
  const providerNeeded = [
    ...guarded.provider_decision_needed,
    ...classified.provider_decision_needed,
  ];

  const updates = classified.writable
    .map((p) => ({
      id: p.record_id,
      fields: p.idempotent.fields,
      identity_key: p.identity_key,
      confidence: p.confidence,
      queue: p.queue,
    }))
    .filter((u) => u.id && Object.keys(u.fields || {}).length);

  const writeTarget = airtable?.target || {
    base: AUTOPILOT_TARGET_BASE_LABEL,
    table: AUTOPILOT_TARGET_TABLE,
    tableId: AUTOPILOT_TARGET_TABLE_ID,
  };
  const targetGate = guardProductionCensusWriteTarget(writeTarget);
  if (doWrite && mode === "apply" && updates.length && !targetGate.ok) {
    return {
      ok: false,
      stop: true,
      stop_reason: BLOCKED_WRONG_CENSUS_TARGET,
      completion_status: COMPLETION_STATUS.BLOCKED_SAFETY,
      write_target_guard: targetGate,
      written: [],
      skipped: [],
      blocked: guarded.blocked,
      provider_decision_needed: guarded.provider_decision_needed,
      airtable_writes: false,
      target: {
        base: productionHotelPropertyCensus.baseName,
        table: productionHotelPropertyCensus.tableName,
        tableId: productionHotelPropertyCensus.tableId,
      },
    };
  }

  let writeResult = { updated: 0, errors: [], written: [] };
  if (doWrite && (mode === "apply" || mode === "production-cycle") && updates.length) {
    if (!airtable?.patchRecords) {
      return {
        ok: false,
        stop: true,
        stop_reason: "airtable_write_adapter_missing",
        completion_status: COMPLETION_STATUS.BLOCKED_SAFETY,
      };
    }
    writeResult = await airtable.patchRecords(updates);
    if (writeResult.blocked_wrong_census_target || writeResult.errors?.length) {
      const wrongTarget = Boolean(writeResult.blocked_wrong_census_target);
      return {
        ok: false,
        stop: true,
        stop_reason: wrongTarget ? BLOCKED_WRONG_CENSUS_TARGET : "airtable_write_failure",
        completion_status: COMPLETION_STATUS.BLOCKED_SAFETY,
        writeResult,
        write_target_guard: targetGate,
        updates_attempted: updates,
        airtable_writes: false,
      };
    }
  }

  const hardCases = [
    ...classified.steward,
    ...classified.conflicts,
    ...classified.blocked,
  ].filter((p) => p.room_count_ambiguous || p.confidence === "Hold" || p.block_reason);

  return {
    ok: true,
    stop: false,
    mode,
    do_write: doWrite,
    target: {
      base: AUTOPILOT_TARGET_BASE_LABEL,
      table: AUTOPILOT_TARGET_TABLE,
      tableId: AUTOPILOT_TARGET_TABLE_ID,
    },
    write_target_guard: targetGate,
    written: doWrite ? writeResult.written || [] : [],
    would_write: updates,
    skipped: classified.skipped,
    conflicts: classified.conflicts,
    steward: classified.steward,
    blocked: [...guarded.blocked, ...classified.blocked],
    provider_decision_needed: providerNeeded,
    hard_cases: hardCases,
    airtable_writes: Boolean(doWrite && (writeResult.written?.length > 0)),
    writeResult,
  };
}

/**
 * Run-until-complete loop across proposals for a queue (or all queues flattened).
 * @param {object} input
 */
export async function runUntilComplete(input = {}) {
  const {
    runDir,
    runId,
    args,
    proposals = [],
    queues = ["rooms_keys"],
    airtable = null,
    schemaV114Ready = false,
    completedRecordIds = [],
    env = process.env,
    /** When false, apply prepares writes but does not call Airtable (build/test safety). */
    enableProductionWrites = false,
  } = input;

  const started = Date.now();
  const batchesDir = path.join(runDir, "batches");
  fs.mkdirSync(batchesDir, { recursive: true });

  const provider = evaluateProviderReadiness(env);
  const allowGeocode = Boolean(provider.approved_for_geocode_apply);

  const preflight =
    args.mode === "apply" || args.mode === "production-cycle"
      ? applyPreflight(args, checkAutopilotApplyEnv(env))
      : { ok: true, blockers: [], warnings: args.warnings || [] };

  if ((args.mode === "apply" || args.mode === "production-cycle") && !preflight.ok) {
    return {
      ok: false,
      completion_status: COMPLETION_STATUS.BLOCKED_SAFETY,
      blockers: preflight.blockers,
      airtable_writes: false,
    };
  }

  const doWrite =
    (args.mode === "apply" || args.mode === "production-cycle") &&
    enableProductionWrites &&
    preflight.ok &&
    args.allApplyConfirms;

  // Scope: full list unless max-records caps for sample/test
  const scoped = applyMaxRecordsCap(proposals, args.maxRecords);
  const skippedDone = skipCompletedRecords(scoped.items, completedRecordIds);
  const work = skippedDone.remaining;
  const metrics = createRuntimeMetrics();

  // Process queues in priority order (fastest-safe). batch-size chunks only.
  const totalInScope = scoped.original_count;
  const totalWork = work.length;
  const orderedQueues = queues.length ? queues : ["rooms_keys"];

  let recordsUpdated = 0;
  let recordsSkipped = skippedDone.skipped_completed.length;
  let recordsBlocked = 0;
  const idsWritten = [];
  const fieldsWritten = new Set();
  const failed = [];
  const providerQueue = [];
  const stewardAll = [];
  const hardAll = [];
  const blockedAll = [];
  const batchReports = [];
  let stopReason = null;
  let completionStatus = COMPLETION_STATUS.IN_PROGRESS;
  let completedIds = [...completedRecordIds];
  let batchNumber = 0;
  let currentQueue = orderedQueues[0];
  const blockedSourceFamilies = new Set();

  for (let qi = 0; qi < orderedQueues.length; qi += 1) {
    currentQueue = orderedQueues[qi];
    if (
      (currentQueue === "coordinate_resolution" ||
        currentQueue === "coordinate_completion") &&
      !allowGeocode
    ) {
      // Soft-skip geocode queues entirely; continue others
      const geoItems = work.filter(
        (p) =>
          (p.queue || currentQueue) === "coordinate_resolution" ||
          (p.queue || currentQueue) === "coordinate_completion"
      );
      providerQueue.push(...geoItems);
      continue;
    }

    const queueItems = work.filter((p) => {
      if (orderedQueues.length === 1 && !p.queue) return true;
      return (p.queue || orderedQueues[0]) === currentQueue;
    });
    const chunks = chunkByBatchSize(queueItems, args.batchSize);

    for (let ci = 0; ci < chunks.length; ci += 1) {
      batchNumber += 1;
      const chunk = chunks[ci];
      if (!chunk.length) continue;
      const batchStarted = Date.now();

      // Drop records whose source family exceeded block threshold
      const filteredChunk = chunk.filter((p) => {
        const fam = p.family || p.brand_family || p.extractor_family;
        if (fam && blockedSourceFamilies.has(fam)) {
          blockedAll.push({ ...p, block_reason: "blocked_source_access" });
          return false;
        }
        return true;
      });

      const result = await processAutopilotBatch(filteredChunk, {
        airtable,
        allowGeocode,
        schemaV114Ready,
        threshold: args.confidenceThreshold || "High",
        mode: args.mode,
        doWrite,
      });

      const batchMs = Date.now() - batchStarted;
      const writtenList = doWrite ? result.written || [] : result.would_write || [];
      const wouldWriteCount = (result.would_write || []).length;
      const skippedCount = (result.skipped || []).length;
      // Idempotent exhaustion (all skips / no writable fields) is healthy — not a write-rate failure.
      const writeSuccessRatio =
        wouldWriteCount === 0 && skippedCount > 0
          ? null
          : writtenList.length / Math.max(1, wouldWriteCount || filteredChunk.length);
      metrics.recordBatch({
        batch_number: batchNumber,
        queue: currentQueue,
        ms: batchMs,
        records: filteredChunk.length,
        writes: writtenList.length,
        write_attempts: wouldWriteCount || writtenList.length,
        fetches_ok: result.fetches_ok || 0,
        fetches_fail: result.fetches_fail || 0,
        blocked_source_family: result.blocked_source_family || null,
        rate_limit_fail: result.rate_limit_fail || 0,
      });

      if (result.blocked_source_family && metrics.shouldStopRetryingSource(result.blocked_source_family)) {
        blockedSourceFamilies.add(result.blocked_source_family);
      }

      const safety = metrics.evaluateSafetyStop({
        batch_ms: batchMs,
        write_success: writeSuccessRatio,
        high_ratio: filteredChunk.filter((p) => p.confidence === "High").length / Math.max(1, filteredChunk.length),
        records: filteredChunk.length,
        protected_field_in_plan: result.stop && result.stop_reason === "blocked_safety_failure",
      });

      const batchPayload = {
        batch_number: batchNumber,
        queue: currentQueue,
        size: filteredChunk.length,
        runtime_ms: batchMs,
        result: {
          ok: result.ok,
          stop: result.stop,
          airtable_writes: result.airtable_writes,
          written_count: (result.written || []).length,
          would_write_count: (result.would_write || []).length,
          skipped_count: (result.skipped || []).length,
          blocked_count: (result.blocked || []).length,
          provider_decision_needed_count: (result.provider_decision_needed || []).length,
          steward_count: (result.steward || []).length,
        },
        written_ids: writtenList.map((w) => w.id || w.record_id),
        provider_decision_needed: result.provider_decision_needed || [],
        safety,
      };

      writeJson(path.join(batchesDir, `batch-${padBatch(batchNumber)}.json`), batchPayload);
      writeText(
        path.join(batchesDir, `batch-${padBatch(batchNumber)}.md`),
        [
          `# Batch ${batchNumber}`,
          ``,
          `- Queue: ${currentQueue}`,
          `- Size: ${filteredChunk.length}`,
          `- Runtime: ${batchMs} ms`,
          `- Airtable writes: ${Boolean(result.airtable_writes)}`,
          `- Written: ${batchPayload.result.written_count}`,
          `- Would write: ${batchPayload.result.would_write_count}`,
          `- Skipped: ${batchPayload.result.skipped_count}`,
          `- Blocked: ${batchPayload.result.blocked_count}`,
          `- Provider decision needed: ${batchPayload.result.provider_decision_needed_count}`,
          ``,
        ].join("\n")
      );
      batchReports.push(batchPayload);

      if (result.stop || safety.stop) {
        stopReason =
          result.stop_reason ||
          safety.reasons?.[0] ||
          "blocked_safety_failure";
        completionStatus =
          /source/i.test(stopReason)
            ? COMPLETION_STATUS.BLOCKED_SOURCE
            : COMPLETION_STATUS.BLOCKED_SAFETY;
        failed.push(...(result.updates_attempted || []).map((u) => u.id));
        break;
      }

      for (const w of writtenList) {
        const id = w.id || w.record_id;
        if (id) {
          idsWritten.push(id);
          completedIds.push(id);
        }
        for (const f of Object.keys(w.fields || {})) fieldsWritten.add(f);
      }
      recordsUpdated += writtenList.length;
      recordsSkipped += (result.skipped || []).length;
      recordsBlocked += (result.blocked || []).length + (result.conflicts || []).length;
      providerQueue.push(...(result.provider_decision_needed || []));
      stewardAll.push(...(result.steward || []), ...(result.conflicts || []));
      hardAll.push(...(result.hard_cases || []));
      blockedAll.push(...(result.blocked || []));

      const uniqueCompleted = [...new Set(completedIds)];
      saveCheckpoint(runDir, {
        run_id: runId,
        parent_company: args.parentCompany,
        scope: args.scope,
        region: args.region,
        country: args.country,
        mode: args.mode,
        strategy: args.strategy,
        batch_size: args.batchSize,
        current_queue: currentQueue,
        current_batch_number: batchNumber,
        total_records_in_scope: totalInScope,
        records_completed: uniqueCompleted.length,
        records_remaining: Math.max(0, totalWork - uniqueCompleted.length),
        records_updated: recordsUpdated,
        records_skipped: recordsSkipped,
        records_blocked: recordsBlocked,
        airtable_record_ids_written: [...new Set(idsWritten)],
        fields_written: [...fieldsWritten],
        failed_records: failed,
        completed_record_ids: uniqueCompleted,
        provider_decision_needed: providerQueue.map((p) => p.record_id || p.id).filter(Boolean),
        next_cursor: {
          queue_index: qi,
          batch_index: ci + 1,
          next_record_id: chunks[ci + 1]?.[0]?.record_id || chunks[ci + 1]?.[0]?.id || null,
        },
        queues_remaining: orderedQueues.slice(qi + (ci + 1 >= chunks.length ? 1 : 0)),
        completion_status: COMPLETION_STATUS.IN_PROGRESS,
      });
    }
    if (stopReason) break;
  }

  if (!stopReason) {
    if (providerQueue.length && recordsUpdated === 0 && work.length > 0 && providerQueue.length >= work.length) {
      completionStatus = COMPLETION_STATUS.BLOCKED_PROVIDER;
    } else if (args.runUntilComplete || args.maxRecords != null || batchNumber >= 1 || work.length === 0) {
      completionStatus = COMPLETION_STATUS.COMPLETE;
    } else {
      completionStatus = COMPLETION_STATUS.PARTIAL_RESUME;
    }
  }

  const webhound = routeWebhoundCandidates(hardAll, { max: 25 });
  const confidence = tallyConfidence([
    ...proposals.filter((p) => (idsWritten.length ? idsWritten.includes(p.record_id || p.id) : true)),
  ]);

  const runtime = metrics.snapshot({
    records_remaining: Math.max(0, totalWork - [...new Set(completedIds)].length),
    steward_review_count: stewardAll.length,
    webhound_candidate_count: webhound.candidates?.length || 0,
  });
  writeJson(path.join(runDir, "runtime-metrics.json"), runtime);

  const finalCheckpoint = saveCheckpoint(runDir, {
    run_id: runId,
    parent_company: args.parentCompany,
    scope: args.scope,
    region: args.region,
    country: args.country,
    mode: args.mode,
    strategy: args.strategy,
    batch_size: args.batchSize,
    current_queue: currentQueue,
    current_batch_number: batchNumber,
    total_records_in_scope: totalInScope,
    records_completed: [...new Set(completedIds)].length,
    records_remaining: Math.max(0, totalWork - [...new Set(completedIds)].length),
    records_updated: recordsUpdated,
    records_skipped: recordsSkipped,
    records_blocked: recordsBlocked,
    airtable_record_ids_written: [...new Set(idsWritten)],
    fields_written: [...fieldsWritten],
    failed_records: failed,
    completed_record_ids: [...new Set(completedIds)],
    provider_decision_needed: providerQueue.map((p) => p.record_id || p.id).filter(Boolean),
    next_cursor: null,
    queues_remaining: stopReason ? orderedQueues : [],
    completion_status: completionStatus,
  });

  return {
    ok: !stopReason || completionStatus === COMPLETION_STATUS.COMPLETE,
    version: BATCH_ENGINE_VERSION,
    run_id: runId,
    completion_status: completionStatus,
    stop_reason: stopReason,
    batch_size: args.batchSize,
    max_records: args.maxRecords,
    run_until_complete: Boolean(args.runUntilComplete),
    scope: args.scope,
    strategy: args.strategy,
    scope_capped_by_batch_size: false,
    scope_capped_by_max_records: scoped.capped,
    total_records_in_scope: totalInScope,
    total_processed: [...new Set(completedIds)].length,
    total_updated: recordsUpdated,
    total_skipped: recordsSkipped,
    total_blocked: recordsBlocked,
    batches_run: batchNumber,
    batch_reports: batchReports,
    fields_populated: [...fieldsWritten],
    confidence_distribution: confidence,
    provider_decision_needed: providerQueue,
    steward_review_queue: stewardAll,
    webhound_candidates: webhound,
    blocked_records: blockedAll,
    blocked_source_families: [...blockedSourceFamilies],
    checkpoint: finalCheckpoint,
    runtime_metrics: runtime,
    airtable_writes: Boolean(doWrite && idsWritten.length > 0),
    target: {
      base: AUTOPILOT_TARGET_BASE_LABEL,
      table: AUTOPILOT_TARGET_TABLE,
      tableId: AUTOPILOT_TARGET_TABLE_ID,
    },
    runtime_ms: Date.now() - started,
    preflight,
    allow_geocode: allowGeocode,
    resume_command: finalCheckpoint.resume_command,
  };
}
