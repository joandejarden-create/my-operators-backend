/**
 * Autopilot runtime targets + performance guardrails.
 */

export const RUNTIME_TARGETS = Object.freeze({
  plan_mode_parent_region_minutes: 2,
  queue_build_per_5k_records_minutes: 5,
  cached_dry_run_per_1k_minutes: 10,
  live_fetch_dry_run_per_250_minutes: { min: 5, max: 20 },
  apply_per_100_minutes: { min: 2, max: 8 },
  parent_company_estimates: {
    small_100: { min_minutes: 15, max_minutes: 30 },
    medium_500: { min_minutes: 45, max_minutes: 90 },
    large_1000: { min_minutes: 60, max_minutes: 180 },
    very_large_4000: { min_minutes: 240, max_minutes: 600 },
  },
});

export const PERFORMANCE_GUARDRAILS = Object.freeze({
  max_retries_per_source: 2,
  batch_runtime_overage_multiplier: 3,
  repeated_source_block_threshold: 8,
  rate_limit_failure_threshold: 3,
  write_success_floor: 0.15,
  confidence_high_floor_for_extractor_health: 0.05,
});

/**
 * Create a runtime metrics collector for a run.
 */
export function createRuntimeMetrics() {
  const started = Date.now();
  /** @type {Array<object>} */
  const batches = [];
  /** @type {Record<string, { ms: number, records: number, writes: number, fetches_ok: number, fetches_fail: number }>} */
  const queues = {};
  const sourceBlocks = {};
  let writes = 0;
  let writeAttempts = 0;
  let fetchesOk = 0;
  let fetchesFail = 0;
  let extractedOkCount = 0;
  let extractedFailCount = 0;
  let rateLimitFails = 0;

  function ensureQueue(id) {
    if (!queues[id]) queues[id] = { ms: 0, records: 0, writes: 0, fetches_ok: 0, fetches_fail: 0 };
    return queues[id];
  }

  return {
    recordBatch(meta) {
      batches.push({
        batch_number: meta.batch_number,
        queue: meta.queue,
        ms: meta.ms,
        records: meta.records ?? 0,
        writes: meta.writes ?? 0,
        fetches_ok: meta.fetches_ok ?? 0,
        fetches_fail: meta.fetches_fail ?? 0,
      });
      const q = ensureQueue(meta.queue || "unknown");
      q.ms += meta.ms || 0;
      q.records += meta.records || 0;
      q.writes += meta.writes || 0;
      q.fetches_ok += meta.fetches_ok || 0;
      q.fetches_fail += meta.fetches_fail || 0;
      writes += meta.writes || 0;
      writeAttempts += meta.write_attempts ?? meta.writes ?? 0;
      fetchesOk += meta.fetches_ok || 0;
      fetchesFail += meta.fetches_fail || 0;
      extractedOkCount += meta.extracted_ok || 0;
      extractedFailCount += meta.extracted_fail || 0;
      if (meta.rate_limit_fail) rateLimitFails += meta.rate_limit_fail;
      if (meta.blocked_source_family) {
        const f = meta.blocked_source_family;
        sourceBlocks[f] = (sourceBlocks[f] || 0) + 1;
      }
    },

    shouldStopRetryingSource(family) {
      return (sourceBlocks[family] || 0) >= PERFORMANCE_GUARDRAILS.repeated_source_block_threshold;
    },

    maxRetriesPerSource() {
      return PERFORMANCE_GUARDRAILS.max_retries_per_source;
    },

    /**
     * @param {{ batch_ms: number, expected_batch_ms?: number, high_ratio?: number, write_success?: number, records?: number, protected_field_in_plan?: boolean }} obs
     */
    evaluateSafetyStop(obs = {}) {
      const stops = [];
      const expected =
        obs.expected_batch_ms ||
        RUNTIME_TARGETS.apply_per_100_minutes.max * 60 * 1000;
      if (
        obs.batch_ms &&
        obs.batch_ms > expected * PERFORMANCE_GUARDRAILS.batch_runtime_overage_multiplier
      ) {
        stops.push("batch_runtime_exceeds_3x_expected");
      }
      if (rateLimitFails >= PERFORMANCE_GUARDRAILS.rate_limit_failure_threshold) {
        stops.push("repeated_rate_limit_failures");
      }
      if (
        obs.write_success != null &&
        obs.write_success < PERFORMANCE_GUARDRAILS.write_success_floor &&
        writeAttempts >= 20
      ) {
        stops.push("write_success_rate_drop");
      }
      if (
        obs.high_ratio != null &&
        obs.high_ratio < PERFORMANCE_GUARDRAILS.confidence_high_floor_for_extractor_health &&
        (obs.records || 0) >= 50
      ) {
        stops.push("broad_extractor_failure_signal");
      }
      if (obs.protected_field_in_plan) stops.push("protected_field_in_write_plan");
      for (const [fam, n] of Object.entries(sourceBlocks)) {
        if (n >= PERFORMANCE_GUARDRAILS.repeated_source_block_threshold) {
          stops.push(`source_family_blocked:${fam}`);
        }
      }
      return {
        stop: stops.length > 0,
        reasons: stops,
      };
    },

    snapshot(extra = {}) {
      const elapsed = Date.now() - started;
      const records = batches.reduce((s, b) => s + (b.records || 0), 0);
      const avgSecPerRecord = records ? elapsed / 1000 / records : null;
      const avgSecPerWrite = writes ? elapsed / 1000 / writes : null;
      const fetchTotal = fetchesOk + fetchesFail;
      const extractTotal = extractedOkCount + extractedFailCount;
      const remaining = extra.records_remaining ?? null;
      const rate = records && elapsed ? records / (elapsed / 1000) : null;
      const etaSec = remaining != null && rate ? remaining / rate : null;

      return {
        total_runtime_ms: elapsed,
        runtime_per_batch: batches.map((b) => ({
          batch: b.batch_number,
          queue: b.queue,
          ms: b.ms,
        })),
        runtime_per_queue: queues,
        average_seconds_per_record: avgSecPerRecord,
        average_seconds_per_successful_write: avgSecPerWrite,
        fetch_success_rate: fetchTotal ? fetchesOk / fetchTotal : null,
        extraction_success_rate: extractTotal ? extractedOkCount / extractTotal : null,
        write_success_rate: writeAttempts ? writes / writeAttempts : null,
        blocked_source_count: Object.values(sourceBlocks).reduce((a, b) => a + b, 0),
        blocked_sources_by_family: sourceBlocks,
        steward_review_count: extra.steward_review_count ?? null,
        webhound_candidate_count: extra.webhound_candidate_count ?? null,
        estimated_time_remaining_ms: etaSec != null ? Math.round(etaSec * 1000) : null,
        targets: RUNTIME_TARGETS,
        guardrails: PERFORMANCE_GUARDRAILS,
      };
    },
  };
}
