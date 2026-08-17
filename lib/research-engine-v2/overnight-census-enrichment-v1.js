/**
 * Overnight Autonomous Census Enrichment Runner
 *
 * Wraps existing runMasterCensusEnrichment in a timed, checkpointed loop.
 * Automates repetition — never weakens evidence / write policy.
 *
 * Write target: Hotel Property Census only (tbl9aY5ijiuIzzWam).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";

import {
  runMasterCensusEnrichment,
  computeMasterCompleteness,
  MAP_MASTER,
} from "./master-census-enrichment-v1.js";
import {
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const OVERNIGHT_ENRICHMENT_VERSION = "overnight-census-enrichment-v1";

export const OVERNIGHT_STATE_DIR = path.join(
  ROOT,
  "data/research-engine-v2/overnight-census-enrichment"
);
export const OVERNIGHT_STATE_FP = path.join(OVERNIGHT_STATE_DIR, "state.json");
export const OVERNIGHT_REPORT_DIR = path.join(
  ROOT,
  "reports/research-engine-v2/overnight-enrichment"
);
export const OVERNIGHT_STATUS_FP = path.join(
  OVERNIGHT_REPORT_DIR,
  "current-status.json"
);
export const OVERNIGHT_FINAL_FP = path.join(
  OVERNIGHT_REPORT_DIR,
  "overnight-enrichment-final.json"
);
export const OVERNIGHT_FINAL_MD = path.join(
  OVERNIGHT_REPORT_DIR,
  "overnight-enrichment-final.md"
);

/** Source / lane persistent statuses (overnight state machine). */
export const SOURCE_LANE_STATUS = Object.freeze({
  READY: "READY",
  ACTIVE: "ACTIVE",
  EXHAUSTED: "EXHAUSTED",
  PLATEAUED: "PLATEAUED",
  BLOCKED_TEMPORARY: "BLOCKED_TEMPORARY",
  BLOCKED_LONG_TERM: "BLOCKED_LONG_TERM",
  RATE_LIMITED: "RATE_LIMITED",
  USAGE_REVIEW: "USAGE_REVIEW",
  RETRY_LATER: "RETRY_LATER",
});

/** Cycle profiles — rotate when plateau; all reuse master orchestrator. */
export const OVERNIGHT_CYCLE_PROFILES = Object.freeze([
  {
    id: "brand_rooms",
    lane: "Current Brand + Rooms",
    forceBrandRoomsWave: true,
    skipBrandPortfolio: false,
    skipRoomsRegistry: false,
    skipPropertyFundamentals: false,
    skipCoordinates: true,
    maxPfResearch: 80,
    maxBrandValidate: 2500,
  },
  {
    id: "brand_focus",
    lane: "Current Brand",
    forceBrandRoomsWave: true,
    skipBrandPortfolio: false,
    skipRoomsRegistry: true,
    skipPropertyFundamentals: true,
    skipCoordinates: true,
    maxPfResearch: 0,
    maxBrandValidate: 4000,
  },
  {
    id: "rooms_focus",
    lane: "Rooms / Keys",
    forceBrandRoomsWave: true,
    skipBrandPortfolio: true,
    skipRoomsRegistry: false,
    skipPropertyFundamentals: true,
    skipCoordinates: true,
    maxPfResearch: 0,
    maxBrandValidate: 0,
  },
  {
    id: "fundamentals",
    lane: "Property Fundamentals",
    forceBrandRoomsWave: true,
    skipBrandPortfolio: true,
    skipRoomsRegistry: true,
    skipPropertyFundamentals: false,
    skipCoordinates: true,
    maxPfResearch: 200,
    maxBrandValidate: 0,
  },
  {
    id: "opportunistic_coords",
    lane: "Opportunistic Coordinates",
    forceBrandRoomsWave: true,
    skipBrandPortfolio: true,
    skipRoomsRegistry: true,
    skipPropertyFundamentals: false,
    skipCoordinates: false,
    maxPfResearch: 40,
    maxBrandValidate: 0,
    maxOpportunisticCoordinateRequests: 300,
  },
]);

function writeJson(fp, obj) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2));
}

function writeMd(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text);
}

function readJson(fp, fallback) {
  try {
    if (!fs.existsSync(fp)) return fallback;
    const raw = fs.readFileSync(fp, "utf8").replace(/^\uFEFF/, "");
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function num(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

/**
 * @param {NodeJS.ProcessEnv|Record<string,string|undefined>} [env]
 */
export function resolveOvernightConfig(env = process.env) {
  return {
    max_runtime_minutes: Math.max(
      1,
      num(env.OVERNIGHT_MAX_RUNTIME_MINUTES, 360)
    ),
    max_external_cost_usd: Math.max(
      0,
      num(env.OVERNIGHT_MAX_EXTERNAL_COST_USD, 50)
    ),
    plateau_zero_cycles: Math.max(1, num(env.OVERNIGHT_PLATEAU_ZERO_CYCLES, 2)),
    cycle_pause_ms: Math.max(0, num(env.OVERNIGHT_CYCLE_PAUSE_MS, 5000)),
    cutoff_guard_minutes: Math.max(
      1,
      num(env.OVERNIGHT_CUTOFF_GUARD_MINUTES, 12)
    ),
    max_cycles: Math.max(1, num(env.OVERNIGHT_MAX_CYCLES, 200)),
  };
}

/**
 * Priority score from missingness (directional — does not weaken quality gates).
 */
export function scoreLanePriority(completeness = {}) {
  const miss = (k) => {
    const pct = num(completeness[k]?.completeness_pct, 100);
    return Math.max(0, 100 - pct);
  };
  return [
    { lane: "Current Brand", score: miss("current_brand") * 1.4 },
    { lane: "Rooms / Keys", score: miss("rooms") * 1.35 },
    { lane: "Address", score: miss("address") * 1.0 },
    { lane: "Postal Code", score: miss("postal_code") * 0.95 },
    { lane: "Coordinates", score: miss("latitude") * 0.7 },
    { lane: "State / Region", score: miss("state_region") * 0.6 },
    { lane: "Website", score: miss("website") * 0.55 },
    { lane: "Phone", score: miss("phone") * 0.45 },
    { lane: "City", score: miss("city") * 0.2 },
  ].sort((a, b) => b.score - a.score);
}

/**
 * Pick next cycle profile given overnight state + plateau marks.
 */
export function selectNextCycleProfile(state, completeness) {
  const laneStatus = state.lane_status || {};
  const ranked = scoreLanePriority(completeness);
  const top = ranked[0]?.lane || "Current Brand";

  /** @type {string[]} */
  let preferredIds = ["brand_rooms"];
  if (top === "Current Brand") preferredIds = ["brand_focus", "brand_rooms"];
  else if (top === "Rooms / Keys") preferredIds = ["rooms_focus", "brand_rooms"];
  else if (top === "Address" || top === "Postal Code" || top === "Website" || top === "Phone") {
    preferredIds = ["fundamentals", "brand_rooms"];
  } else if (top === "Coordinates") {
    preferredIds = ["opportunistic_coords", "fundamentals"];
  } else if (top === "State / Region" || top === "City") {
    preferredIds = ["fundamentals", "brand_rooms"];
  }

  const startIdx = Number(state.next_profile_index || 0);
  const ordered = [
    ...OVERNIGHT_CYCLE_PROFILES.slice(startIdx),
    ...OVERNIGHT_CYCLE_PROFILES.slice(0, startIdx),
  ];

  for (const prefer of preferredIds) {
    const hit = ordered.find((p) => {
      if (p.id !== prefer) return false;
      const st = laneStatus[p.id];
      return (
        !st ||
        st === SOURCE_LANE_STATUS.READY ||
        st === SOURCE_LANE_STATUS.ACTIVE ||
        st === SOURCE_LANE_STATUS.RETRY_LATER
      );
    });
    if (hit) return hit;
  }

  for (const p of ordered) {
    const st = laneStatus[p.id];
    if (
      st === SOURCE_LANE_STATUS.PLATEAUED ||
      st === SOURCE_LANE_STATUS.EXHAUSTED ||
      st === SOURCE_LANE_STATUS.BLOCKED_LONG_TERM ||
      st === SOURCE_LANE_STATUS.USAGE_REVIEW
    ) {
      continue;
    }
    return p;
  }
  // All plateaued — still rotate brand_rooms lightly rather than stop
  return OVERNIGHT_CYCLE_PROFILES[startIdx % OVERNIGHT_CYCLE_PROFILES.length];
}

/**
 * Detect plateau from a master cycle report.
 */
export function evaluateCycleYield(report = {}) {
  const brand = num(report.CURRENT_BRAND_WRITES || report.BRAND_VALIDATIONS_HIGH);
  const rooms = num(report.ROOMS_WRITTEN);
  const coords = num(
    report.ADDITIONAL_COORDINATES_WRITTEN || report.COORDINATES_WRITTEN
  );
  const fields = num(report.TOTAL_FIELDS_WRITTEN_THIS_RUN);
  const patched = num(report.PROPERTIES_PATCHED_THIS_RUN);
  const useful = brand + rooms + coords + (fields > 0 ? 1 : 0);
  return {
    useful_writes: useful,
    brand_writes: brand,
    rooms_writes: rooms,
    coord_writes: coords,
    fields_written: fields,
    properties_patched: patched,
    zero_yield: useful === 0 && patched === 0,
  };
}

export function emptyOvernightAggregates() {
  return {
    cycles_completed: 0,
    properties_researched: 0,
    properties_patched: 0,
    fields_written: 0,
    current_brand_writes: 0,
    rooms_writes: 0,
    state_patches: 0,
    city_patches: 0,
    address_patches: 0,
    postal_patches: 0,
    coordinates_written: 0,
    website_patches: 0,
    phone_patches: 0,
    brand_conflicts: 0,
    brand_mapping_gaps: 0,
    rooms_candidates_held: 0,
    rooms_conflicts: 0,
    external_requests: 0,
    estimated_cost_usd: 0,
    mapbox_requests: 0,
    mapbox_estimated_cost: 0,
    errors: 0,
    HBX_ROOMS_ARRAY_WRITES: 0,
    HBX_COORDINATE_WRITES: 0,
    CVENT_ONLY_ROOM_VALIDATIONS: 0,
    BENCHMARK_ROOM_WRITES: 0,
    DESTRUCTIVE_OVERWRITES: 0,
    WRONG_TABLE_WRITES: 0,
    sources_exhausted: [],
    sources_plateaued: [],
    sources_blocked: [],
    sources_usage_review: [],
    yield_by_cycle: [],
    top_yield_sources: [],
    low_yield_or_blocked: [],
  };
}

function mergeSafety(agg, report) {
  agg.HBX_ROOMS_ARRAY_WRITES += num(report.HBX_ROOMS_ARRAY_WRITES);
  agg.HBX_COORDINATE_WRITES += num(report.HBX_COORDINATE_WRITES);
  agg.CVENT_ONLY_ROOM_VALIDATIONS += num(report.CVENT_ONLY_ROOM_VALIDATIONS);
  agg.BENCHMARK_ROOM_WRITES += num(
    report.BENCHMARK_ROOM_WRITES || report.BENCHMARK_DATA_WRITES
  );
  agg.DESTRUCTIVE_OVERWRITES += num(report.DESTRUCTIVE_OVERWRITES);
  agg.WRONG_TABLE_WRITES += num(report.WRONG_TABLE_WRITES);
  agg.errors += num(report.ERRORS);
}

function mergeYieldSources(agg, report) {
  const tops = report.TOP_BRAND_SOURCE_YIELDS || [];
  for (const t of tops) {
    const existing = agg.top_yield_sources.find((x) => x.company === t.company);
    if (existing) {
      existing.current_brand_writes += num(t.current_brand_writes);
      existing.attempted += num(t.attempted);
    } else {
      agg.top_yield_sources.push({
        company: t.company,
        current_brand_writes: num(t.current_brand_writes),
        attempted: num(t.attempted),
        yield_pct: t.yield_pct,
      });
    }
  }
  agg.top_yield_sources.sort(
    (a, b) => b.current_brand_writes - a.current_brand_writes
  );
  if (report.ROOMS_BY_SOURCE) {
    for (const [src, n] of Object.entries(report.ROOMS_BY_SOURCE)) {
      const existing = agg.top_yield_sources.find((x) => x.company === src);
      if (existing) existing.rooms_writes = num(existing.rooms_writes) + num(n);
      else {
        agg.top_yield_sources.push({
          company: src,
          current_brand_writes: 0,
          rooms_writes: num(n),
          attempted: 0,
        });
      }
    }
  }
}

/**
 * Write live progress artifact (no secrets).
 */
export function writeOvernightStatus(status) {
  writeJson(OVERNIGHT_STATUS_FP, {
    ...status,
    updated_at: new Date().toISOString(),
  });
  return OVERNIGHT_STATUS_FP;
}

/**
 * Hard production target lock for overnight runner.
 */
export function assertOvernightProductionLock(opts = {}) {
  const base = opts.baseId || resolveTargetBase()?.target_base_id || resolveTargetBase()?.baseId;
  const sot = assertProductionCensusWriteTarget({
    baseId: base,
    tableId:
      opts.tableId ||
      PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID ||
      productionHotelPropertyCensus.tableId,
  });
  if (!sot.ok) {
    return {
      ok: false,
      reason: "wrong_production_target",
      sot,
    };
  }
  return { ok: true, sot, baseId: base };
}

/**
 * Build master opts for one overnight cycle.
 */
export function buildMasterOptsForCycle(profile, overnightOpts, enableWrites) {
  const minutesLeft = overnightOpts.minutes_remaining;
  const guard = overnightOpts.cutoff_guard_minutes;
  const nearCutoff = minutesLeft <= guard;

  return {
    mode: "resume",
    enableProductionWrites: enableWrites,
    forceBrandRoomsWave: profile.forceBrandRoomsWave !== false,
    brandRoomsWave: true,
    skipBrandPortfolio: nearCutoff ? true : profile.skipBrandPortfolio === true,
    skipRoomsRegistry: nearCutoff ? true : profile.skipRoomsRegistry === true,
    skipPropertyFundamentals:
      nearCutoff || profile.skipPropertyFundamentals === true,
    skipCoordinates: profile.skipCoordinates !== false,
    skipCoordinateSample: true,
    maxPfResearch: nearCutoff ? 0 : num(profile.maxPfResearch, 80),
    maxBrandValidate: nearCutoff ? 200 : num(profile.maxBrandValidate, 2000),
    maxRoomsPerSource: nearCutoff ? 500 : 6000,
    maxOpportunisticCoordinateRequests: num(
      profile.maxOpportunisticCoordinateRequests,
      200
    ),
    continueMapboxWave: false,
  };
}

/**
 * Run the overnight enrichment loop.
 *
 * @param {{
 *   mode?: 'dry-run'|'run'|'resume',
 *   enableProductionWrites?: boolean,
 *   maxRuntimeMinutes?: number,
 *   maxExternalCostUsd?: number,
 *   log?: Function,
 *   runMasterFn?: Function,
 *   now?: () => number,
 *   sleepFn?: (ms: number) => Promise<void>,
 * }} opts
 */
export async function runOvernightCensusEnrichment(opts = {}) {
  const log = opts.log || console.log;
  const nowFn = opts.now || Date.now;
  const sleepFn = opts.sleepFn || sleep;
  const runMaster = opts.runMasterFn || runMasterCensusEnrichment;
  const cfg = resolveOvernightConfig(process.env);
  if (opts.maxRuntimeMinutes != null) {
    cfg.max_runtime_minutes = Math.max(1, Number(opts.maxRuntimeMinutes));
  }
  if (opts.maxExternalCostUsd != null) {
    cfg.max_external_cost_usd = Math.max(0, Number(opts.maxExternalCostUsd));
  }

  const mode = String(opts.mode || "dry-run").toLowerCase();
  const enableWrites =
    Boolean(opts.enableProductionWrites) &&
    (mode === "run" || mode === "resume");

  const lock = assertOvernightProductionLock();
  if (!lock.ok) {
    const fail = {
      ok: false,
      OVERNIGHT_ENRICHMENT_STATUS: "overnight_blocked_wrong_table",
      STOP_REASON: "wrong_production_target",
      FOUNDER_DECISION_REQUIRED: "YES",
      FOUNDER_DECISION: "Fix Airtable production target before overnight writes",
      WRONG_TABLE_WRITES: 1,
    };
    writeJson(OVERNIGHT_FINAL_FP, fail);
    return fail;
  }

  fs.mkdirSync(OVERNIGHT_STATE_DIR, { recursive: true });
  fs.mkdirSync(OVERNIGHT_REPORT_DIR, { recursive: true });

  const startedAtMs = nowFn();
  const endAtMs = startedAtMs + cfg.max_runtime_minutes * 60_000;
  const runId =
    mode === "resume"
      ? readJson(OVERNIGHT_STATE_FP, {})?.run_id ||
        `overnight_${crypto.randomBytes(4).toString("hex")}`
      : `overnight_${crypto.randomBytes(4).toString("hex")}`;

  let state =
    mode === "resume"
      ? readJson(OVERNIGHT_STATE_FP, null)
      : null;
  if (!state || mode === "run" || mode === "dry-run") {
    state = {
      run_id: runId,
      version: OVERNIGHT_ENRICHMENT_VERSION,
      started_at: new Date(startedAtMs).toISOString(),
      end_at: new Date(endAtMs).toISOString(),
      mode,
      production_writes: enableWrites,
      config: cfg,
      next_profile_index: 0,
      consecutive_zero_by_profile: {},
      lane_status: Object.fromEntries(
        OVERNIGHT_CYCLE_PROFILES.map((p) => [p.id, SOURCE_LANE_STATUS.READY])
      ),
      aggregates: emptyOvernightAggregates(),
      dashboard_before: null,
      last_cycle_report: null,
      stop_reason: null,
    };
  } else {
    state.end_at = new Date(endAtMs).toISOString();
    state.config = cfg;
    state.aggregates = {
      ...emptyOvernightAggregates(),
      ...(state.aggregates || {}),
    };
  }

  writeJson(OVERNIGHT_STATE_FP, state);
  log(
    `[overnight] start run_id=${state.run_id} max_min=${cfg.max_runtime_minutes} cost_cap_usd=${cfg.max_external_cost_usd} writes=${enableWrites}`
  );

  let stopReason = null;
  let founderDecision = null;
  /** @type {object|null} */
  let lastReport = null;
  let firstDashboard = state.dashboard_before;
  let lastDashboard = null;

  while (true) {
    const now = nowFn();
    const minutesRemaining = Math.max(0, (endAtMs - now) / 60_000);
    if (minutesRemaining <= 0) {
      stopReason = "overnight_runtime_limit_reached";
      break;
    }
    if (state.aggregates.cycles_completed >= cfg.max_cycles) {
      stopReason = "overnight_max_cycles_reached";
      break;
    }
    if (
      state.aggregates.estimated_cost_usd >= cfg.max_external_cost_usd &&
      cfg.max_external_cost_usd > 0
    ) {
      // Pause paid lanes conceptually; if only paid left, stop
      const freeLeft = OVERNIGHT_CYCLE_PROFILES.some((p) => {
        const st = state.lane_status[p.id];
        return (
          p.id !== "opportunistic_coords" &&
          st !== SOURCE_LANE_STATUS.PLATEAUED &&
          st !== SOURCE_LANE_STATUS.EXHAUSTED
        );
      });
      if (!freeLeft) {
        stopReason = "global_external_cost_ceiling";
        founderDecision =
          "Overnight external cost ceiling reached with no free lanes remaining";
        break;
      }
    }

    // Near cutoff — do not start heavy batch
    if (minutesRemaining <= cfg.cutoff_guard_minutes) {
      const light = OVERNIGHT_CYCLE_PROFILES.find((p) => p.id === "brand_focus");
      const profile = light || OVERNIGHT_CYCLE_PROFILES[0];
      log(
        `[overnight] cutoff guard (${minutesRemaining.toFixed(1)}m left) — light cycle only: ${profile.id}`
      );
    }

    const completeness = lastDashboard || firstDashboard || {};
    const profile = selectNextCycleProfile(state, completeness);
    state.lane_status[profile.id] = SOURCE_LANE_STATUS.ACTIVE;
    state.current_lane = profile.lane;
    state.current_source = profile.id;

    writeOvernightStatus({
      RUN_ID: state.run_id,
      START_TIME: state.started_at,
      ELAPSED_TIME_MINUTES: Math.round((now - startedAtMs) / 60_000),
      TIME_REMAINING_MINUTES: Math.round(minutesRemaining),
      CURRENT_LANE: profile.lane,
      CURRENT_SOURCE: profile.id,
      PROPERTIES_ATTEMPTED: state.aggregates.properties_researched,
      PROPERTIES_PATCHED: state.aggregates.properties_patched,
      FIELDS_WRITTEN: state.aggregates.fields_written,
      CURRENT_BRAND_COMPLETENESS:
        completeness.current_brand?.completeness_pct ?? null,
      ROOMS_COMPLETENESS: completeness.rooms?.completeness_pct ?? null,
      ADDRESS_COMPLETENESS: completeness.address?.completeness_pct ?? null,
      POSTAL_COMPLETENESS: completeness.postal_code?.completeness_pct ?? null,
      STATE_COMPLETENESS: completeness.state_region?.completeness_pct ?? null,
      COORDINATE_COMPLETENESS: completeness.latitude?.completeness_pct ?? null,
      WEBSITE_COMPLETENESS: completeness.website?.completeness_pct ?? null,
      PHONE_COMPLETENESS: completeness.phone?.completeness_pct ?? null,
      REQUESTS_USED: state.aggregates.external_requests,
      ESTIMATED_COST: state.aggregates.estimated_cost_usd,
      LAST_CHECKPOINT: new Date().toISOString(),
      STOP_REASON_IF_ANY: null,
      CYCLES_COMPLETED: state.aggregates.cycles_completed,
    });

    log(
      `[overnight] cycle ${state.aggregates.cycles_completed + 1} profile=${profile.id} lane=${profile.lane} rem=${minutesRemaining.toFixed(1)}m`
    );

    const masterOpts = buildMasterOptsForCycle(
      profile,
      {
        minutes_remaining: minutesRemaining,
        cutoff_guard_minutes: cfg.cutoff_guard_minutes,
      },
      enableWrites
    );
    masterOpts.log = (m) => log(`  ${m}`);

    let report;
    try {
      report = await runMaster(masterOpts);
    } catch (err) {
      state.aggregates.errors += 1;
      log(
        `[overnight] cycle error: ${String(err?.message || err).slice(0, 200)}`
      );
      state.lane_status[profile.id] = SOURCE_LANE_STATUS.BLOCKED_TEMPORARY;
      state.next_profile_index =
        (OVERNIGHT_CYCLE_PROFILES.findIndex((p) => p.id === profile.id) + 1) %
        OVERNIGHT_CYCLE_PROFILES.length;
      writeJson(OVERNIGHT_STATE_FP, state);
      if (cfg.cycle_pause_ms) await sleepFn(cfg.cycle_pause_ms);
      continue;
    }

    lastReport = report;
    if (!firstDashboard && report.DASHBOARD_BEFORE) {
      firstDashboard = report.DASHBOARD_BEFORE;
      state.dashboard_before = firstDashboard;
    }
    if (report.DASHBOARD_AFTER) lastDashboard = report.DASHBOARD_AFTER;

    if (num(report.WRONG_TABLE_WRITES) > 0) {
      stopReason = "wrong_production_target";
      founderDecision = "Wrong Airtable production target detected";
      mergeSafety(state.aggregates, report);
      break;
    }
    if (num(report.DESTRUCTIVE_OVERWRITES) > 0) {
      stopReason = "destructive_overwrite_risk";
      founderDecision = "Destructive overwrite risk detected";
      mergeSafety(state.aggregates, report);
      break;
    }

    const yieldInfo = evaluateCycleYield(report);
    state.aggregates.cycles_completed += 1;
    state.aggregates.properties_patched += yieldInfo.properties_patched;
    state.aggregates.fields_written += yieldInfo.fields_written;
    state.aggregates.current_brand_writes += yieldInfo.brand_writes;
    state.aggregates.rooms_writes += yieldInfo.rooms_writes;
    state.aggregates.coordinates_written += yieldInfo.coord_writes;
    state.aggregates.brand_conflicts += num(report.BRAND_CONFLICTS);
    state.aggregates.brand_mapping_gaps += num(report.BRAND_MAPPING_GAPS);
    state.aggregates.rooms_candidates_held += num(report.ROOMS_CANDIDATES_HELD);
    state.aggregates.rooms_conflicts += num(report.ROOMS_CONFLICTS);
    state.aggregates.mapbox_requests += num(report.MAPBOX_REQUESTS);
    state.aggregates.mapbox_estimated_cost += num(report.ESTIMATED_MAPBOX_COST);
    // Approximate external cost: Mapbox this cycle + small crawl allowance
    const cycleCost = num(report.ESTIMATED_MAPBOX_COST);
    state.aggregates.estimated_cost_usd += cycleCost;
    state.aggregates.external_requests += num(report.MAPBOX_REQUESTS);
    mergeSafety(state.aggregates, report);
    mergeYieldSources(state.aggregates, report);
    state.aggregates.yield_by_cycle.push({
      profile: profile.id,
      ...yieldInfo,
      at: new Date().toISOString(),
    });

    // Plateau detection per profile
    const zeroMap = state.consecutive_zero_by_profile || {};
    if (yieldInfo.zero_yield) {
      zeroMap[profile.id] = num(zeroMap[profile.id]) + 1;
    } else {
      zeroMap[profile.id] = 0;
      state.lane_status[profile.id] = SOURCE_LANE_STATUS.READY;
    }
    if (zeroMap[profile.id] >= cfg.plateau_zero_cycles) {
      state.lane_status[profile.id] = SOURCE_LANE_STATUS.PLATEAUED;
      if (!state.aggregates.sources_plateaued.includes(profile.id)) {
        state.aggregates.sources_plateaued.push(profile.id);
      }
      log(`[overnight] plateau detected for ${profile.id} — rotating`);
    }
    state.consecutive_zero_by_profile = zeroMap;

    // Rotate profile index
    const idx = OVERNIGHT_CYCLE_PROFILES.findIndex((p) => p.id === profile.id);
    state.next_profile_index = (idx + 1) % OVERNIGHT_CYCLE_PROFILES.length;
    state.last_cycle_report = {
      profile: profile.id,
      status: report.MASTER_ENRICHMENT_STATUS,
      brand_writes: yieldInfo.brand_writes,
      rooms_writes: yieldInfo.rooms_writes,
    };
    writeJson(OVERNIGHT_STATE_FP, state);

    // If all profiles plateaued, continue until runtime — mark exhausted set
    const allPlateaued = OVERNIGHT_CYCLE_PROFILES.every((p) => {
      const st = state.lane_status[p.id];
      return (
        st === SOURCE_LANE_STATUS.PLATEAUED ||
        st === SOURCE_LANE_STATUS.EXHAUSTED
      );
    });
    if (allPlateaued) {
      // Reset READY for fundamentals once to allow residual harvest, else keep looping lightly
      for (const p of OVERNIGHT_CYCLE_PROFILES) {
        if (p.id === "fundamentals" || p.id === "brand_rooms") {
          state.lane_status[p.id] = SOURCE_LANE_STATUS.RETRY_LATER;
          zeroMap[p.id] = 0;
        }
      }
      state.consecutive_zero_by_profile = zeroMap;
      // If still no useful work after many zero cycles, stop cleanly
      const recent = state.aggregates.yield_by_cycle.slice(-6);
      if (
        recent.length >= 6 &&
        recent.every((y) => y.zero_yield) &&
        state.aggregates.cycles_completed >= 6
      ) {
        stopReason = "all_structured_lanes_plateaued";
        break;
      }
    }

    if (report.FOUNDER_DECISION_REQUIRED === "YES") {
      // Lane-level founder items — log and continue unless catastrophic
      const items = report.FOUNDER_DECISION_ITEMS || [];
      const critical = items.some((i) =>
        /wrong.?table|destructive|HBX_COORDINATE_WRITES_must/i.test(
          String(i?.item || i?.reason || "")
        )
      );
      if (critical) {
        stopReason = "founder_decision_required";
        founderDecision = items.map((i) => i.item || i.reason).join("; ");
        break;
      }
      log(
        `[overnight] non-blocking founder items (${items.length}) — continuing other lanes`
      );
    }

    if (cfg.cycle_pause_ms) await sleepFn(cfg.cycle_pause_ms);
  }

  if (!stopReason) stopReason = "overnight_runtime_limit_reached";

  const after = lastDashboard || lastReport?.DASHBOARD_AFTER || {};
  const before = firstDashboard || lastReport?.DASHBOARD_BEFORE || {};
  const agg = state.aggregates;

  const final = {
    ok: agg.WRONG_TABLE_WRITES === 0 && agg.DESTRUCTIVE_OVERWRITES === 0,
    OVERNIGHT_ENRICHMENT_STATUS:
      stopReason === "wrong_production_target"
        ? "overnight_blocked_wrong_table"
        : stopReason === "destructive_overwrite_risk"
          ? "overnight_blocked_destructive"
          : "overnight_enrichment_complete",
    version: OVERNIGHT_ENRICHMENT_VERSION,
    RUN_ID: state.run_id,
    RUN_DURATION_MINUTES: Math.round((nowFn() - startedAtMs) / 60_000),
    STOP_REASON: stopReason,
    CENSUS_COUNT: after.n || before.n || 15575,
    CURRENT_BRAND_BEFORE: before.current_brand?.completeness_pct ?? null,
    CURRENT_BRAND_AFTER: after.current_brand?.completeness_pct ?? null,
    CURRENT_BRAND_WRITES: agg.current_brand_writes,
    BRAND_FAMILY_AFTER: after.brand_family?.completeness_pct ?? null,
    FAMILY_SOURCE_FAMILY_AFTER:
      after.family_source_family?.completeness_pct ?? null,
    ROOMS_BEFORE: before.rooms?.completeness_pct ?? null,
    ROOMS_AFTER: after.rooms?.completeness_pct ?? null,
    ROOMS_WRITES: agg.rooms_writes,
    STATE_REGION_BEFORE: before.state_region?.completeness_pct ?? null,
    STATE_REGION_AFTER: after.state_region?.completeness_pct ?? null,
    STATE_REGION_PATCHES: agg.state_patches,
    CITY_BEFORE: before.city?.completeness_pct ?? null,
    CITY_AFTER: after.city?.completeness_pct ?? null,
    CITY_PATCHES: agg.city_patches,
    ADDRESS_BEFORE: before.address?.completeness_pct ?? null,
    ADDRESS_AFTER: after.address?.completeness_pct ?? null,
    ADDRESS_PATCHES: agg.address_patches,
    POSTAL_CODE_BEFORE: before.postal_code?.completeness_pct ?? null,
    POSTAL_CODE_AFTER: after.postal_code?.completeness_pct ?? null,
    POSTAL_CODE_PATCHES: agg.postal_patches,
    LATITUDE_LONGITUDE_BEFORE: before.latitude?.completeness_pct ?? null,
    LATITUDE_LONGITUDE_AFTER: after.latitude?.completeness_pct ?? null,
    COORDINATES_WRITTEN: agg.coordinates_written,
    WEBSITE_BEFORE: before.website?.completeness_pct ?? null,
    WEBSITE_AFTER: after.website?.completeness_pct ?? null,
    WEBSITE_PATCHES: agg.website_patches,
    PHONE_BEFORE: before.phone?.completeness_pct ?? null,
    PHONE_AFTER: after.phone?.completeness_pct ?? null,
    PHONE_PATCHES: agg.phone_patches,
    TOTAL_PROPERTIES_RESEARCHED: agg.properties_researched,
    TOTAL_PROPERTIES_PATCHED: agg.properties_patched,
    TOTAL_FIELDS_WRITTEN: agg.fields_written,
    TOP_10_HIGHEST_YIELD_SOURCES: agg.top_yield_sources.slice(0, 10),
    TOP_10_LOWEST_YIELD_OR_BLOCKED_SOURCES: [
      ...agg.sources_plateaued.map((s) => ({ source: s, status: "PLATEAUED" })),
      ...agg.sources_blocked.map((s) => ({ source: s, status: "BLOCKED" })),
    ].slice(0, 10),
    SOURCES_EXHAUSTED: agg.sources_exhausted,
    SOURCES_PLATEAUED: agg.sources_plateaued,
    SOURCES_BLOCKED: agg.sources_blocked,
    SOURCES_USAGE_REVIEW: agg.sources_usage_review,
    TOTAL_EXTERNAL_REQUESTS: agg.external_requests,
    TOTAL_ESTIMATED_COST: Number(agg.estimated_cost_usd.toFixed(4)),
    MAPBOX_CUMULATIVE_REQUESTS: agg.mapbox_requests,
    MAPBOX_CUMULATIVE_ESTIMATED_COST: Number(
      agg.mapbox_estimated_cost.toFixed(4)
    ),
    UNRESOLVED_BY_FIELD: lastReport?.UNRESOLVED_BY_FIELD || {},
    CONFLICTS_BY_FIELD: {
      brand: agg.brand_conflicts,
      rooms: agg.rooms_conflicts,
    },
    HBX_ROOMS_ARRAY_WRITES: agg.HBX_ROOMS_ARRAY_WRITES,
    HBX_COORDINATE_WRITES: agg.HBX_COORDINATE_WRITES,
    CVENT_ONLY_ROOM_VALIDATIONS: agg.CVENT_ONLY_ROOM_VALIDATIONS,
    BENCHMARK_DATA_WRITES: agg.BENCHMARK_ROOM_WRITES,
    DESTRUCTIVE_OVERWRITES: agg.DESTRUCTIVE_OVERWRITES,
    WRONG_TABLE_WRITES: agg.WRONG_TABLE_WRITES,
    ERRORS: agg.errors,
    CYCLES_COMPLETED: agg.cycles_completed,
    FOUNDER_DECISION_REQUIRED: founderDecision ? "YES" : "NO",
    FOUNDER_DECISION: founderDecision,
    NEXT_BEST_AUTONOMOUS_ACTION: founderDecision
      ? "Resolve founder decision, then: npm run census:overnight-enrichment -- --mode resume --enable-production-writes"
      : "Resume overnight or master: npm run census:overnight-enrichment -- --mode resume --enable-production-writes",
    CHECKPOINT_PATH: OVERNIGHT_STATE_FP,
    STATUS_PATH: OVERNIGHT_STATUS_FP,
    generated_at: new Date().toISOString(),
  };

  state.stop_reason = stopReason;
  state.finished_at = final.generated_at;
  writeJson(OVERNIGHT_STATE_FP, state);
  writeJson(OVERNIGHT_FINAL_FP, final);
  writeMd(
    OVERNIGHT_FINAL_MD,
    [
      `# Overnight Census Enrichment`,
      ``,
      `Status: \`${final.OVERNIGHT_ENRICHMENT_STATUS}\``,
      `Stop: \`${final.STOP_REASON}\``,
      `Duration: ${final.RUN_DURATION_MINUTES} minutes`,
      `Cycles: ${final.CYCLES_COMPLETED}`,
      ``,
      `| Field | Before | After | Writes |`,
      `| --- | ---: | ---: | ---: |`,
      `| Current Brand | ${final.CURRENT_BRAND_BEFORE}% | ${final.CURRENT_BRAND_AFTER}% | ${final.CURRENT_BRAND_WRITES} |`,
      `| Rooms | ${final.ROOMS_BEFORE}% | ${final.ROOMS_AFTER}% | ${final.ROOMS_WRITES} |`,
      `| Lat/Long | ${final.LATITUDE_LONGITUDE_BEFORE}% | ${final.LATITUDE_LONGITUDE_AFTER}% | ${final.COORDINATES_WRITTEN} |`,
      ``,
      `FOUNDER_DECISION_REQUIRED: **${final.FOUNDER_DECISION_REQUIRED}**`,
      ``,
    ].join("\n")
  );
  writeOvernightStatus({
    RUN_ID: state.run_id,
    START_TIME: state.started_at,
    ELAPSED_TIME_MINUTES: final.RUN_DURATION_MINUTES,
    TIME_REMAINING_MINUTES: 0,
    CURRENT_LANE: null,
    CURRENT_SOURCE: null,
    PROPERTIES_PATCHED: agg.properties_patched,
    FIELDS_WRITTEN: agg.fields_written,
    CURRENT_BRAND_COMPLETENESS: final.CURRENT_BRAND_AFTER,
    ROOMS_COMPLETENESS: final.ROOMS_AFTER,
    REQUESTS_USED: agg.external_requests,
    ESTIMATED_COST: final.TOTAL_ESTIMATED_COST,
    LAST_CHECKPOINT: final.generated_at,
    STOP_REASON_IF_ANY: stopReason,
    CYCLES_COMPLETED: agg.cycles_completed,
  });

  log(
    `[overnight] done status=${final.OVERNIGHT_ENRICHMENT_STATUS} stop=${stopReason} cycles=${agg.cycles_completed}`
  );
  return final;
}

// Re-export for tests that mock completeness shape
export { computeMasterCompleteness, MAP_MASTER };
