/**
 * V4 Full-Universe Build Controller — outer self-driving loop.
 *
 * Termination ONLY when:
 * - hard circuit breaker
 * - explicit operator stop
 * - no actionable work remains
 * - only temporarily blocked work (schedule resume)
 * - infrastructure runtime boundary (checkpoint + schedule)
 *
 * Checkpoint ≠ stop. Batch ceiling ≠ completion.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import {
  isAllCapsCity,
  isAllLowerCity,
  toProperCasePlace,
  canonicalCalaCity,
} from "../census-city-state-normalizer.js";
import { normName } from "../census-autopilot-v2/identity-dedupe.js";

export const V4_CONTROLLER_VERSION = "v4-full-build-controller-v1";

export const LANES = Object.freeze({
  VERIFIED_READY_INSERT: "VERIFIED_READY_INSERT",
  OFFICIAL_DIRECTORY_DISCOVERY: "OFFICIAL_DIRECTORY_DISCOVERY",
  INDEPENDENT_REDISCOVERY: "INDEPENDENT_REDISCOVERY",
  CITY_PROPER_CASE_REMEDIATION: "CITY_PROPER_CASE_REMEDIATION",
  MISSING_FIELD_SOURCE_STRATEGY: "MISSING_FIELD_SOURCE_STRATEGY",
  EXISTING_RECORD_REMEDIATION: "EXISTING_RECORD_REMEDIATION",
  ADAPTER_NEEDED_ENGINEERING: "ADAPTER_NEEDED_ENGINEERING",
  SERPAPI_IDENTITY_RESEARCH: "SERPAPI_IDENTITY_RESEARCH",
  WAITING_RETRY: "WAITING_RETRY",
});

export const CONTROLLER_STATUS = Object.freeze({
  ACTIVE: "ACTIVE",
  WAITING: "WAITING",
  BLOCKED: "BLOCKED",
  COMPLETE: "COMPLETE",
  INFRASTRUCTURE_RUNTIME_BOUNDARY: "INFRASTRUCTURE_RUNTIME_BOUNDARY",
});

/** Build-mode lane priority (expected value). */
export const LANE_PRIORITY = Object.freeze([
  LANES.VERIFIED_READY_INSERT,
  LANES.CITY_PROPER_CASE_REMEDIATION,
  LANES.OFFICIAL_DIRECTORY_DISCOVERY,
  LANES.MISSING_FIELD_SOURCE_STRATEGY,
  LANES.INDEPENDENT_REDISCOVERY,
  LANES.SERPAPI_IDENTITY_RESEARCH,
  LANES.EXISTING_RECORD_REMEDIATION,
  LANES.ADAPTER_NEEDED_ENGINEERING,
]);

export function defaultControllerConfig(overrides = {}) {
  return {
    wave_batch_size: Number(process.env.V4_WAVE_BATCH_SIZE || 100),
    max_iterations_per_process: Number(process.env.V4_MAX_ITERATIONS || 12),
    max_runtime_ms: Number(process.env.V4_MAX_RUNTIME_MS || 25 * 60 * 1000),
    verified_ready_threshold: 5,
    write_delay_ms: 110,
    serpapi_daily_ceiling: Number(process.env.SERPAPI_V4_DAILY_CEILING || 50),
    min_lane_transitions_for_demo: 3,
    ...overrides,
  };
}

/**
 * Normalize City to Proper Case (never invent; never leave ALL CAPS / all lower).
 */
export function normalizeCityProperCase(city) {
  if (city == null || city === "") return { city: null, changed: false, reason: "blank" };
  const raw = String(city).trim();
  if (!raw) return { city: null, changed: false, reason: "blank" };
  const canon = canonicalCalaCity(raw);
  if (canon && canon !== raw) {
    return { city: canon, changed: true, reason: "cala_canonical" };
  }
  if (isAllCapsCity(raw) || isAllLowerCity(raw)) {
    const proper = toProperCasePlace(raw);
    if (proper && proper !== raw) {
      return { city: proper, changed: true, reason: "proper_case" };
    }
  }
  return { city: raw, changed: false, reason: "already_ok" };
}

/**
 * Authoritative actionable-work probe — across ledger + live + lanes,
 * not merely the in-memory queue from the last wave.
 */
export function hasActionableUniverseWork(snapshot) {
  const s = snapshot || {};
  const counts = s.status_counts || {};
  const verifiedReady =
    Number(counts.VERIFIED_READY_TO_INSERT || 0) + Number(s.verified_ready_queue || 0);
  const researchable =
    Number(counts.RESEARCHABLE_UNVERIFIED || 0) +
    Number(counts.NOT_YET_INDEPENDENTLY_REDISCOVERED || 0);
  const cityCaps = Number(s.all_caps_city_count || 0);
  const directoryNew = Number(s.official_directory_new_remaining || 0);
  const rediscoveryEligible = Number(s.independent_rediscovery_eligible || 0);
  const remediation = Number(s.remediation_eligible || 0);
  const fieldStrategy = Number(s.field_strategy_eligible || 0);
  const freeWork =
    verifiedReady +
    cityCaps +
    directoryNew +
    rediscoveryEligible +
    remediation +
    fieldStrategy +
    (researchable > 0 && s.official_adapters_available && !s.directory_wave_just_exhausted
      ? Math.min(researchable, 1)
      : 0);

  const paidOnly =
    freeWork === 0 &&
    Number(s.serpapi_eligible || 0) > 0 &&
    !s.serpapi_budget_exhausted;

  const engineeringOnly =
    freeWork === 0 &&
    !paidOnly &&
    Number(counts.ENGINEERING_REQUIRED || s.engineering_required || 0) > 0 &&
    researchable === 0;

  const temporarilyBlocked =
    freeWork === 0 &&
    !paidOnly &&
    (s.rate_limited || s.waiting_retry || s.serpapi_budget_exhausted);

  return {
    actionable: freeWork > 0 || paidOnly,
    free_work_units: freeWork,
    paid_only: paidOnly,
    engineering_only: engineeringOnly,
    temporarily_blocked: temporarilyBlocked,
    complete: freeWork === 0 && !paidOnly && !temporarilyBlocked && researchable === 0,
    breakdown: {
      verified_ready: verifiedReady,
      all_caps_city: cityCaps,
      official_directory_new: directoryNew,
      independent_rediscovery_eligible: rediscoveryEligible,
      remediation_eligible: remediation,
      field_strategy_eligible: fieldStrategy,
      researchable_or_cvent_challenge: researchable,
      serpapi_eligible: Number(s.serpapi_eligible || 0),
      engineering_required: Number(counts.ENGINEERING_REQUIRED || s.engineering_required || 0),
    },
  };
}

export function chooseHighestValueLane(snapshot, opts = {}) {
  const probe = hasActionableUniverseWork(snapshot);
  const exhausted = new Set(opts.exhausted_lanes || snapshot.exhausted_lanes || []);

  if (probe.complete) return { lane: null, reason: "no_actionable_work", probe };
  if (snapshot.hard_circuit) return { lane: null, reason: "hard_circuit", probe };

  const b = probe.breakdown;
  for (const lane of LANE_PRIORITY) {
    if (exhausted.has(lane)) continue;
    if (lane === LANES.VERIFIED_READY_INSERT && b.verified_ready > 0) {
      return { lane, reason: "verified_ready_available", probe };
    }
    if (lane === LANES.CITY_PROPER_CASE_REMEDIATION && b.all_caps_city > 0) {
      return { lane, reason: "all_caps_city_remediation", probe };
    }
    if (lane === LANES.OFFICIAL_DIRECTORY_DISCOVERY && b.official_directory_new > 0) {
      return { lane, reason: "directory_new_remaining", probe };
    }
    if (
      lane === LANES.OFFICIAL_DIRECTORY_DISCOVERY &&
      b.researchable_or_cvent_challenge > 0 &&
      snapshot.official_adapters_available &&
      !snapshot.directory_wave_just_exhausted
    ) {
      return { lane, reason: "replenish_via_official_discovery", probe };
    }
    if (
      lane === LANES.MISSING_FIELD_SOURCE_STRATEGY &&
      b.field_strategy_eligible > 0 &&
      snapshot.policy_controller_enabled
    ) {
      return { lane, reason: "missing_field_source_strategy", probe };
    }
    if (lane === LANES.INDEPENDENT_REDISCOVERY && b.independent_rediscovery_eligible > 0) {
      return { lane, reason: "cvent_challenge_rediscovery", probe };
    }
    if (
      lane === LANES.INDEPENDENT_REDISCOVERY &&
      b.researchable_or_cvent_challenge > 0 &&
      snapshot.can_attempt_independent_rediscovery
    ) {
      return { lane, reason: "attempt_independent_rediscovery", probe };
    }
    if (
      lane === LANES.SERPAPI_IDENTITY_RESEARCH &&
      b.serpapi_eligible > 0 &&
      !snapshot.serpapi_budget_exhausted
    ) {
      return { lane, reason: "serpapi_ev_positive", probe };
    }
    if (lane === LANES.EXISTING_RECORD_REMEDIATION && b.remediation_eligible > 0) {
      return { lane, reason: "production_remediation", probe };
    }
  }

  if (probe.temporarily_blocked) {
    return { lane: LANES.WAITING_RETRY, reason: "temporary_block", probe };
  }
  if (probe.engineering_only) {
    return { lane: LANES.ADAPTER_NEEDED_ENGINEERING, reason: "classify_engineering_only", probe };
  }
  return { lane: null, reason: "lanes_exhausted_or_blocked", probe };
}

export function loadLedgerRows(ledgerDir) {
  if (!fs.existsSync(ledgerDir)) return [];
  const files = fs
    .readdirSync(ledgerDir)
    .filter((f) => f.startsWith("ledger-") && f.endsWith(".json"))
    .sort();
  const rows = [];
  for (const f of files) {
    const j = JSON.parse(fs.readFileSync(path.join(ledgerDir, f), "utf8"));
    rows.push(...(j.rows || []));
  }
  return rows;
}

export function summarizeLedgerStatuses(rows) {
  return rows.reduce((a, r) => {
    const s = r.universe_status || "UNKNOWN";
    a[s] = (a[s] || 0) + 1;
    return a;
  }, {});
}

export function estimateUniquePhysical(rows) {
  const pids = new Set();
  for (const r of rows) {
    if (!r.property_identity_id) continue;
    if (["PROBABLE_DUPLICATE", "NON_HOTEL"].includes(r.universe_status)) continue;
    pids.add(r.property_identity_id);
  }
  return pids.size;
}

export function footprintMetrics({ liveCount, uniquePhysical, priorDenom = 12846 }) {
  const denomBest = Math.max(1, uniquePhysical || priorDenom);
  return {
    live_production: liveCount,
    prior_denom_12846: priorDenom,
    reconciled_unique_physical_estimate: uniquePhysical,
    footprint_vs_prior_12846_pct: Math.round((1000 * liveCount) / priorDenom) / 10,
    footprint_vs_reconciled_unique_pct: Math.round((1000 * liveCount) / denomBest) / 10,
    methodology:
      "Footprint = live Hotel Property Census count / reconciled unique property_identity_id (excluding duplicates/non-hotels). Prior 12,846 kept as comparative KPI only.",
  };
}

export function emptyControllerState(seed = {}) {
  return {
    version: V4_CONTROLLER_VERSION,
    controller_status: CONTROLLER_STATUS.ACTIVE,
    standing_authorization: true,
    joan_batch_approval_required: false,
    hard_block_reason: null,
    temporary_block_reason: null,
    current_lane: null,
    current_queue_size: 0,
    actionable_remaining: null,
    last_work_completed_at: null,
    next_work_scheduled_at: null,
    iteration: 0,
    lane_transitions: [],
    exhausted_lanes: [],
    inserts_session: 0,
    updates_session: 0,
    circuit: { tripped: false, reason: null },
    stop_reason: null,
    ...seed,
  };
}

export function persistJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2));
  return fp;
}

export function appendJsonl(fp, row) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.appendFileSync(fp, JSON.stringify({ ...row, at: new Date().toISOString() }) + "\n");
}

export function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function synthesizeChallengeRediscoveryKey(name, country) {
  const cc =
    String(country || "xx")
      .toLowerCase()
      .replace(/[^a-z]/g, "")
      .slice(0, 2) || "xx";
  const h = crypto
    .createHash("sha256")
    .update(`${normName(name)}|${String(country || "").toLowerCase()}`)
    .digest("hex")
    .slice(0, 16);
  return `ind_indep_${cc}_${h}`;
}

/**
 * Match Cvent-challenge ledger rows to official directory discoveries by name|country.
 * Never copies Cvent factual fields.
 */
export function matchChallengesToDirectory(challenges, discovered, liveNameCountrySet) {
  const byNc = new Map();
  for (const d of discovered || []) {
    const name = d.property_name || d.name;
    const country = d.country;
    if (!name || !country) continue;
    const k = `${normName(name)}|${String(country).toLowerCase()}`;
    if (!byNc.has(k)) byNc.set(k, d);
  }
  const hits = [];
  const misses = [];
  for (const c of challenges || []) {
    const k = `${normName(c.candidate_name)}|${String(c.country || "").toLowerCase()}`;
    if (liveNameCountrySet?.has(k)) {
      misses.push({ ...c, rediscovery_status: "ALREADY_IN_PRODUCTION" });
      continue;
    }
    const d = byNc.get(k);
    if (d) {
      hits.push({
        challenge_pid: c.property_identity_id,
        candidate_name: c.candidate_name,
        country: c.country,
        discovery: d,
        rediscovery_status: "INDEPENDENTLY_REDISCOVERED",
        cvent_used_as_production_evidence: false,
      });
    } else {
      misses.push({ ...c, rediscovery_status: "NOT_YET_INDEPENDENTLY_REDISCOVERED" });
    }
  }
  return { hits, misses };
}
