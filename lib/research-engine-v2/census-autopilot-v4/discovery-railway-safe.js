/**
 * Railway-safe Census V4 official-directory discovery runtime.
 *
 * Goals: hard AbortController timeouts, progress checkpoints, resume,
 * skip-on-stall, bounded concurrency. Does not change write policy.
 */
import fs from "node:fs";
import path from "node:path";

export const DISCOVERY_PROGRESS_FILE = "discovery-progress.json";
export const DISCOVERY_STALL_DIAGNOSTICS_FILE = "discovery-stall-diagnostics.json";
export const DISCOVERY_CHECKPOINT_FILE = "discovery-resume-checkpoint.json";

export function loadDiscoveryRailwaySafeConfig(overrides = {}) {
  const envBool = (k, def) => {
    if (overrides[k] != null) return Boolean(overrides[k]);
    const v = process.env[k];
    if (v == null || v === "") return def;
    return v === "1" || /^true$/i.test(v);
  };
  const envNum = (k, def) => {
    if (overrides[k] != null) return Number(overrides[k]);
    const v = process.env[k];
    if (v == null || v === "") return def;
    const n = Number(v);
    return Number.isFinite(n) ? n : def;
  };

  const railwaySafe = envBool("CENSUS_DISCOVERY_RAILWAY_SAFE_MODE", false);
  return {
    railway_safe_mode: railwaySafe,
    fetch_timeout_ms: envNum("CENSUS_DISCOVERY_FETCH_TIMEOUT_MS", railwaySafe ? 20_000 : 60_000),
    source_timeout_ms: envNum("CENSUS_DISCOVERY_SOURCE_TIMEOUT_MS", railwaySafe ? 60_000 : 180_000),
    country_timeout_ms: envNum("CENSUS_DISCOVERY_COUNTRY_TIMEOUT_MS", railwaySafe ? 90_000 : 300_000),
    skip_on_timeout: envBool("CENSUS_DISCOVERY_SKIP_ON_TIMEOUT", railwaySafe ? true : false),
    resume: envBool("CENSUS_DISCOVERY_RESUME", railwaySafe ? true : false),
    force_refresh: envBool("CENSUS_DISCOVERY_FORCE_REFRESH", false),
    concurrency: Math.max(1, envNum("CENSUS_DISCOVERY_CONCURRENCY", railwaySafe ? 2 : 4)),
    max_retries: Math.max(0, envNum("CENSUS_DISCOVERY_MAX_RETRIES", railwaySafe ? 1 : 2)),
    retry_backoff_ms: envNum("CENSUS_DISCOVERY_RETRY_BACKOFF_MS", railwaySafe ? 3_000 : 1_500),
    progress_flush_ms: envNum("CENSUS_DISCOVERY_PROGRESS_FLUSH_MS", 2_000),
  };
}

export function createTimeoutError(label, ms) {
  const err = new Error(`timeout after ${ms}ms: ${label}`);
  err.code = "CENSUS_DISCOVERY_TIMEOUT";
  err.timeout_ms = ms;
  err.label = label;
  return err;
}

export function isDiscoveryTimeoutError(err) {
  return Boolean(err && (err.code === "CENSUS_DISCOVERY_TIMEOUT" || /timeout after \d+ms/i.test(String(err.message || err))));
}

/**
 * Race a promise against a hard timeout. Does not cancel the underlying work
 * unless the promise respects AbortSignal — prefer timedFetch for HTTP.
 */
export function withTimeout(promise, ms, label) {
  if (!ms || ms <= 0) return promise;
  let timer;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => reject(createTimeoutError(label, ms)), ms);
    timer.unref?.();
  });
  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

/**
 * fetch() with AbortController timeout. Always use for outbound discovery HTTP.
 */
export async function timedFetch(url, opts = {}) {
  const timeoutMs = Number(opts.timeoutMs ?? opts.timeout_ms ?? 20_000);
  const label = opts.label || String(url);
  const parentSignal = opts.signal;
  const controller = new AbortController();
  let timer;
  const onParentAbort = () => controller.abort(parentSignal?.reason);
  if (parentSignal) {
    if (parentSignal.aborted) controller.abort(parentSignal.reason);
    else parentSignal.addEventListener("abort", onParentAbort, { once: true });
  }
  if (timeoutMs > 0) {
    timer = setTimeout(() => controller.abort(createTimeoutError(label, timeoutMs)), timeoutMs);
    timer.unref?.();
  }
  const started = Date.now();
  try {
    const { timeoutMs: _t, timeout_ms: _t2, label: _l, signal: _s, ...fetchOpts } = opts;
    const res = await fetch(url, { ...fetchOpts, signal: controller.signal });
    return res;
  } catch (err) {
    if (controller.signal.aborted) {
      const reason = controller.signal.reason;
      if (isDiscoveryTimeoutError(reason)) throw reason;
      throw createTimeoutError(label, timeoutMs || Date.now() - started);
    }
    throw err;
  } finally {
    if (timer) clearTimeout(timer);
    if (parentSignal) parentSignal.removeEventListener("abort", onParentAbort);
  }
}

export function emptyDiscoveryCheckpoint(meta = {}) {
  return {
    version: "census-v4-discovery-resume-v1",
    updated_at: new Date().toISOString(),
    completed: {}, // `${family}|${country}` -> { at, result_count, status }
    failed: {}, // same key -> { at, error, status }
    timed_out: {},
    pending_families: [],
    pending_countries: [],
    meta,
  };
}

export function checkpointKey(family, country) {
  return `${family}|${country}`;
}

export function loadDiscoveryCheckpoint(outDir, { forceRefresh = false } = {}) {
  if (forceRefresh) return emptyDiscoveryCheckpoint({ force_refresh: true });
  const fp = path.join(outDir, DISCOVERY_CHECKPOINT_FILE);
  try {
    if (!fs.existsSync(fp)) return emptyDiscoveryCheckpoint();
    const j = JSON.parse(fs.readFileSync(fp, "utf8"));
    return {
      ...emptyDiscoveryCheckpoint(),
      ...j,
      completed: j.completed || {},
      failed: j.failed || {},
      timed_out: j.timed_out || {},
    };
  } catch {
    return emptyDiscoveryCheckpoint({ load_error: true });
  }
}

export function saveDiscoveryCheckpoint(outDir, checkpoint) {
  fs.mkdirSync(outDir, { recursive: true });
  const row = { ...checkpoint, updated_at: new Date().toISOString() };
  fs.writeFileSync(path.join(outDir, DISCOVERY_CHECKPOINT_FILE), JSON.stringify(row, null, 2));
  return row;
}

export function createDiscoveryProgressTracker(outDir, cfg) {
  fs.mkdirSync(outDir, { recursive: true });
  const progressPath = path.join(outDir, DISCOVERY_PROGRESS_FILE);
  const stallPath = path.join(outDir, DISCOVERY_STALL_DIAGNOSTICS_FILE);
  const state = {
    started_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    config: cfg,
    current: null,
    events: [],
    countries: {},
    sources: {},
    totals: {
      started: 0,
      succeeded: 0,
      failed: 0,
      timed_out: 0,
      skipped_completed: 0,
      result_count: 0,
    },
  };
  const stalls = {
    started_at: state.started_at,
    updated_at: state.updated_at,
    stalls: [],
    last_stall: null,
  };

  let lastFlush = 0;
  const flush = (force = false) => {
    const now = Date.now();
    if (!force && now - lastFlush < (cfg.progress_flush_ms || 2000)) return;
    lastFlush = now;
    state.updated_at = new Date().toISOString();
    stalls.updated_at = state.updated_at;
    fs.writeFileSync(progressPath, JSON.stringify(state, null, 2));
    fs.writeFileSync(stallPath, JSON.stringify(stalls, null, 2));
  };

  const pushEvent = (evt) => {
    state.events.push(evt);
    if (state.events.length > 500) state.events = state.events.slice(-500);
    console.log(
      `[discover] ${evt.status || "event"} family=${evt.family || "-"} country=${evt.country || "-"} source=${evt.source_key || "-"} dur_ms=${evt.duration_ms ?? "-"} count=${evt.result_count ?? "-"} ${evt.skip_reason || evt.error || ""}`.trim()
    );
    flush(false);
  };

  return {
    progressPath,
    stallPath,
    state,
    stalls,
    flush,
    markCurrent(partial) {
      state.current = { ...partial, at: new Date().toISOString() };
      flush(false);
    },
    logStart({ family, country, source_key, url }) {
      state.totals.started += 1;
      const evt = {
        status: "start",
        family,
        country,
        source_key,
        url: url || null,
        fetch_start: new Date().toISOString(),
      };
      state.current = evt;
      pushEvent(evt);
      return evt;
    },
    logEnd(startEvt, { status, result_count = 0, error = null, skip_reason = null }) {
      const end = new Date().toISOString();
      const duration_ms = startEvt?.fetch_start
        ? Date.parse(end) - Date.parse(startEvt.fetch_start)
        : null;
      const evt = {
        status,
        family: startEvt?.family,
        country: startEvt?.country,
        source_key: startEvt?.source_key,
        url: startEvt?.url || null,
        fetch_start: startEvt?.fetch_start || null,
        fetch_end: end,
        duration_ms,
        result_count,
        error,
        skip_reason,
      };
      if (status === "success") {
        state.totals.succeeded += 1;
        state.totals.result_count += Number(result_count) || 0;
      } else if (status === "timed_out") {
        state.totals.timed_out += 1;
        stalls.stalls.push(evt);
        stalls.last_stall = evt;
      } else if (status === "skipped_completed") {
        state.totals.skipped_completed += 1;
      } else {
        state.totals.failed += 1;
      }
      const ck = checkpointKey(evt.family || "?", evt.country || "?");
      state.countries[ck] = evt;
      state.sources[`${ck}|${evt.source_key || "default"}`] = evt;
      pushEvent(evt);
      return evt;
    },
    finalize(extra = {}) {
      state.final = { ...extra, at: new Date().toISOString() };
      flush(true);
      return { progress: state, stalls };
    },
  };
}

/**
 * Run async work over items with bounded concurrency (no unbounded Promise.all).
 */
export async function mapPool(items, concurrency, worker) {
  const list = [...items];
  const results = new Array(list.length);
  let next = 0;
  const runners = Array.from({ length: Math.min(concurrency, list.length) || 1 }, async () => {
    while (next < list.length) {
      const i = next++;
      results[i] = await worker(list[i], i);
    }
  });
  await Promise.all(runners);
  return results;
}

/**
 * Execute one country/source unit with retries + timeouts + progress.
 */
export async function runDiscoveryUnit({
  family,
  country,
  source_key,
  url,
  cfg,
  tracker,
  checkpoint,
  outDir,
  work,
}) {
  const key = checkpointKey(family, country);
  const unitCacheDir = path.join(outDir, "discovery-unit-cache");
  const unitCachePath = path.join(
    unitCacheDir,
    `${String(family).replace(/[^\w.-]+/g, "_")}__${String(country).replace(/[^\w.-]+/g, "_")}.json`
  );

  const loadUnitCache = () => {
    try {
      if (!fs.existsSync(unitCachePath)) return null;
      const j = JSON.parse(fs.readFileSync(unitCachePath, "utf8"));
      return Array.isArray(j.rows) ? j.rows : null;
    } catch {
      return null;
    }
  };
  const saveUnitCache = (rows) => {
    try {
      fs.mkdirSync(unitCacheDir, { recursive: true });
      fs.writeFileSync(
        unitCachePath,
        JSON.stringify(
          {
            family,
            country,
            source_key,
            saved_at: new Date().toISOString(),
            result_count: Array.isArray(rows) ? rows.length : 0,
            rows: Array.isArray(rows) ? rows : [],
          },
          null,
          2
        )
      );
    } catch (err) {
      console.warn("[discover] unit cache write failed", String(err?.message || err));
    }
  };

  if (cfg.resume && !cfg.force_refresh && checkpoint.completed[key]) {
    const cachedRows = loadUnitCache();
    tracker.logEnd(
      { family, country, source_key, url, fetch_start: new Date().toISOString() },
      {
        status: "skipped_completed",
        result_count: cachedRows?.length || checkpoint.completed[key].result_count || 0,
        skip_reason: cachedRows ? "resume_checkpoint_unit_cache" : "resume_checkpoint_completed_no_cache",
      }
    );
    return {
      ok: true,
      skipped: true,
      result_count: cachedRows?.length || checkpoint.completed[key].result_count || 0,
      rows: cachedRows,
    };
  }

  if (cfg.resume && !cfg.force_refresh && cfg.skip_on_timeout && checkpoint.timed_out[key]) {
    tracker.logEnd(
      { family, country, source_key, url, fetch_start: new Date().toISOString() },
      {
        status: "timed_out",
        result_count: 0,
        error: checkpoint.timed_out[key].error || "resume_skip_prior_timeout",
        skip_reason: "resume_checkpoint_timed_out",
      }
    );
    return {
      ok: false,
      skipped: true,
      timed_out: true,
      result_count: 0,
      rows: null,
      error: new Error(checkpoint.timed_out[key].error || "prior_timeout"),
    };
  }

  let attempt = 0;
  let lastErr = null;
  while (attempt <= cfg.max_retries) {
    attempt += 1;
    const start = tracker.logStart({ family, country, source_key, url });
    try {
      const rows = await withTimeout(
        work({
          family,
          country,
          source_key,
          url,
          timeoutMs: cfg.fetch_timeout_ms,
          countryTimeoutMs: cfg.country_timeout_ms,
          sourceTimeoutMs: cfg.source_timeout_ms,
        }),
        cfg.country_timeout_ms,
        `${family}/${country}/${source_key}`
      );
      const list = Array.isArray(rows) ? rows : rows?.size != null ? [...rows.values()].filter((r) => r && typeof r === "object") : [];
      const count = list.length;
      tracker.logEnd(start, { status: "success", result_count: count });
      saveUnitCache(list);
      checkpoint.completed[key] = {
        at: new Date().toISOString(),
        result_count: count,
        status: "success",
        source_key,
      };
      delete checkpoint.failed[key];
      delete checkpoint.timed_out[key];
      if (cfg.resume) saveDiscoveryCheckpoint(outDir, checkpoint);
      return { ok: true, skipped: false, result_count: count, rows: list };
    } catch (err) {
      lastErr = err;
      const timedOut = isDiscoveryTimeoutError(err);
      const status = timedOut ? "timed_out" : "failed";
      tracker.logEnd(start, {
        status,
        result_count: 0,
        error: String(err?.message || err),
      });
      if (timedOut) {
        checkpoint.timed_out[key] = {
          at: new Date().toISOString(),
          error: String(err?.message || err),
          status: "timed_out",
          source_key,
        };
      } else {
        checkpoint.failed[key] = {
          at: new Date().toISOString(),
          error: String(err?.message || err),
          status: "failed",
          source_key,
        };
      }
      if (cfg.resume) saveDiscoveryCheckpoint(outDir, checkpoint);
      if (attempt <= cfg.max_retries) {
        const backoff = cfg.retry_backoff_ms * attempt;
        await new Promise((r) => setTimeout(r, backoff));
        continue;
      }
      if (cfg.skip_on_timeout || timedOut) {
        return { ok: false, skipped: true, timed_out: timedOut, error: err, result_count: 0, rows: null };
      }
      throw err;
    }
  }
  return { ok: false, skipped: true, error: lastErr, result_count: 0, rows: null };
}

export function classifyDiscoveryLaneStatus({ totals, timedOutKeys, failedKeys }) {
  const timed = timedOutKeys?.length || totals?.timed_out || 0;
  const failed = failedKeys?.length || totals?.failed || 0;
  if (!timed && !failed) return "official_directory_discovery_complete";
  if (timed > 0 && failed === 0) return "official_directory_discovery_partial_network_remaining";
  if (failed > 0) return "official_directory_discovery_partial_source_remaining";
  return "official_directory_discovery_partial_source_remaining";
}
