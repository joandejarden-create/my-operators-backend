/**
 * CALA Hotel Census Coverage Dashboard — estimation + status engine.
 * Read-only. Reuses Cvent / HBX / holds / geography registry only (no fresh research).
 */

import fs from "node:fs";
import path from "node:path";
import {
  DEALALITY_CALA_GEOGRAPHIES,
  HBX_WAVE1_SEARCHED_GEOGRAPHIES,
  resolveDealalityCalaGeography,
  listDealalityCalaGeographies,
} from "../../research-engine-v2/dealality-cala-geography-registry-v1.js";
import {
  countUniverseCandidatesByCountry,
  countHbxByCountry,
  countHoldsByCountry,
} from "../universe-expansion/coverage-scorecard.js";
import { scoreCountryPriority } from "../discovery-factory/priority.js";

export const COVERAGE_DASHBOARD_VERSION = "cala-census-coverage-dashboard-v1";

export const COVERAGE_STATUS = Object.freeze({
  EXCELLENT: "EXCELLENT",
  GOOD: "GOOD",
  FAIR: "FAIR",
  POOR: "POOR",
  CRITICAL: "CRITICAL",
  UNKNOWN: "UNKNOWN",
});

export const DISCOVERY_VALUE = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
});

function readJson(fp, fallback = null) {
  try {
    if (!fs.existsSync(fp)) return fallback;
    return JSON.parse(fs.readFileSync(fp, "utf8"));
  } catch {
    return fallback;
  }
}

/**
 * Roll sparse country→count maps onto canonical registry names.
 */
export function rollupToRegistry(byCountryRaw = {}) {
  const out = {};
  for (const [raw, n] of Object.entries(byCountryRaw || {})) {
    const g = resolveDealalityCalaGeography(raw);
    const key = g?.name || raw;
    out[key] = (out[key] || 0) + Number(n || 0);
  }
  return out;
}

export function coverageStatusFromPct(pct, hasEstimate) {
  if (!hasEstimate || pct == null || !Number.isFinite(pct)) {
    return COVERAGE_STATUS.UNKNOWN;
  }
  if (pct >= 95) return COVERAGE_STATUS.EXCELLENT;
  if (pct >= 80) return COVERAGE_STATUS.GOOD;
  if (pct >= 60) return COVERAGE_STATUS.FAIR;
  if (pct >= 30) return COVERAGE_STATUS.POOR;
  return COVERAGE_STATUS.CRITICAL;
}

export function discoveryValueFrom(row) {
  const missing = row.estimated_missing_hotels || 0;
  const candidates = row.discovery_candidates_available || 0;
  const coverage = row.coverage_pct;
  const effort = row.expected_effort || 0.5;
  // High when large missing + available candidates + not already excellent
  if (missing >= 200 && candidates >= 100 && (coverage == null || coverage < 60)) {
    return DISCOVERY_VALUE.HIGH;
  }
  if (missing >= 50 && candidates >= 20 && (coverage == null || coverage < 80)) {
    return DISCOVERY_VALUE.MEDIUM;
  }
  if (missing >= 20 && candidates >= 10) return DISCOVERY_VALUE.MEDIUM;
  if (effort > 0.8 && candidates < 10) return DISCOVERY_VALUE.LOW;
  if (coverage != null && coverage >= 80) return DISCOVERY_VALUE.LOW;
  if (candidates > 0 && missing > 0) return DISCOVERY_VALUE.MEDIUM;
  return DISCOVERY_VALUE.LOW;
}

/**
 * Estimate universe from known project sources only.
 */
export function estimateUniverse(inputs = {}) {
  const census = Number(inputs.census || 0);
  const cvent = Number(inputs.cvent || 0);
  const hbx = Number(inputs.hbx || 0);
  const holds = Number(inputs.holds || 0);
  const censusPlusHolds = census + holds;

  const candidates = [
    { source: "dealality_census", value: census },
    { source: "cvent_candidate_universe", value: cvent },
    { source: "hotelbeds_wave1_or_full_pack", value: hbx },
    { source: "census_plus_weak_holds", value: censusPlusHolds },
  ].filter((c) => c.value > 0);

  if (candidates.length === 0) {
    return {
      estimated_hotel_universe: null,
      estimation_source: "no_project_source_stock",
      estimation_confidence: "low",
      components: { census, cvent, hbx, holds },
    };
  }

  candidates.sort((a, b) => b.value - a.value);
  const top = candidates[0];
  const multi = candidates.filter((c) => c.value === top.value).length > 1;
  let estimation_confidence = "medium";
  if (hbx > 0 && cvent > 0) estimation_confidence = "high";
  else if (top.source === "dealality_census" && cvent === 0 && hbx === 0 && holds === 0) {
    estimation_confidence = "low";
  } else if (holds > 0 && hbx === 0) estimation_confidence = "medium";

  return {
    estimated_hotel_universe: top.value,
    estimation_source: multi
      ? candidates
          .filter((c) => c.value === top.value)
          .map((c) => c.source)
          .join("+")
      : top.source,
    estimation_confidence,
    components: { census, cvent, hbx, holds, census_plus_holds: censusPlusHolds },
  };
}

function loadFactoryQueues(root) {
  const review = readJson(
    path.join(root, "data/hotel-intelligence/discovery-factory/staged-review-required.json"),
    { hotels: [] }
  );
  const ready = readJson(
    path.join(root, "data/hotel-intelligence/discovery-factory/staged-ready-for-import.json"),
    { hotels: [] }
  );
  const byReview = {};
  const byReady = {};
  for (const h of review.hotels || []) {
    const c = h.location?.country || h.discovery?.country || "UNKNOWN";
    const g = resolveDealalityCalaGeography(c);
    const key = g?.name || c;
    byReview[key] = (byReview[key] || 0) + 1;
  }
  for (const h of ready.hotels || []) {
    const c = h.location?.country || h.discovery?.country || "UNKNOWN";
    const g = resolveDealalityCalaGeography(c);
    const key = g?.name || c;
    byReady[key] = (byReady[key] || 0) + 1;
  }
  return { byReview, byReady };
}

/**
 * Build full dashboard from live census counts + local inventories.
 * @param {Record<string, number>} censusByCountry
 * @param {object} [opts]
 */
export function buildCalaCoverageDashboard(censusByCountry = {}, opts = {}) {
  const root = opts.root || process.cwd();
  const census = rollupToRegistry(censusByCountry);
  const cvent = rollupToRegistry(countUniverseCandidatesByCountry(root));
  const hbx = rollupToRegistry(countHbxByCountry(root));
  const holds = rollupToRegistry(countHoldsByCountry(root));
  const { byReview, byReady } = loadFactoryQueues(root);

  const hbxWave1 = new Set(HBX_WAVE1_SEARCHED_GEOGRAPHIES);
  const geos = listDealalityCalaGeographies({ includeScopeReview: true });

  const rows = [];
  for (const g of geos) {
    const name = g.name;
    const current = Number(census[name] || 0);
    const est = estimateUniverse({
      census: current,
      cvent: cvent[name] || 0,
      hbx: hbx[name] || 0,
      holds: holds[name] || 0,
    });

    const universe = est.estimated_hotel_universe;
    const coverage_pct =
      universe && universe > 0
        ? Math.round((1000 * current) / universe) / 10
        : current > 0 && universe == null
          ? null
          : universe === 0
            ? null
            : current === 0 && universe == null
              ? 0
              : null;

    // Prefer explicit 0% when census=0 and we have an estimate
    let cov = coverage_pct;
    if (current === 0 && universe != null && universe > 0) cov = 0;
    if (current === 0 && universe == null) cov = 0;

    const status = coverageStatusFromPct(cov, universe != null && universe > 0);
    const missing =
      universe != null ? Math.max(0, universe - current) : null;
    const discoveryCandidates = Math.max(
      holds[name] || 0,
      Math.max(0, (cvent[name] || 0) - current)
    );
    const expectedNew = Math.min(missing ?? 0, discoveryCandidates);
    const reviewQueue = byReview[name] || 0;
    const readyQueue = byReady[name] || 0;

    const hbxCount = hbx[name] || 0;
    const expected_duplicate_risk =
      cov != null && cov > 60 ? 0.15 : cov != null && cov > 30 ? 0.08 : 0.04;
    const review_burden = hbxCount > 0 ? 0.25 : discoveryCandidates > 0 ? 0.7 : 0.4;
    const expected_effort =
      discoveryCandidates === 0 ? 0.95 : hbxCount > 0 ? 0.35 : 0.65;

    const priorityInput = {
      country: name,
      hotels_in_dealality: current,
      expected_approximate_universe: universe || current,
      discovery_candidates: discoveryCandidates,
      sources: {
        weak_holds: holds[name] || 0,
        cvent_candidates: cvent[name] || 0,
        hbx_candidates: hbxCount,
      },
    };
    const pr = scoreCountryPriority(priorityInput);

    const row = {
      country: name,
      geography_id: g.geography_id,
      iso_code: g.iso_code,
      region: g.region,
      tourism_priority: g.tourism_priority,
      scope: g.scope,
      current_dealality_hotels: current,
      estimated_hotel_universe: universe,
      coverage_pct: cov,
      coverage_status: status,
      estimation_confidence: est.estimation_confidence,
      estimation_source: est.estimation_source,
      estimation_components: est.components,
      current_review_queue: reviewQueue,
      ready_for_import_queue: readyQueue,
      discovery_candidates_available: discoveryCandidates,
      expected_new_hotels: expectedNew,
      estimated_missing_hotels: missing,
      coverage_gap: missing,
      remaining_discovery_potential: discoveryCandidates,
      expected_duplicate_risk,
      review_burden,
      expected_effort,
      priority_score: pr.priority_score,
      confidence: est.estimation_confidence,
      hbx_wave1_searched: hbxWave1.has(name),
      discovery_value: null,
      priority_reason: pr.components
        ? `gap≈${missing ?? 0}; coverage ${cov ?? "n/a"}%; strategic ${pr.components.strategic_importance}; sources ${pr.components.source_availability}`
        : null,
    };
    row.discovery_value = discoveryValueFrom(row);
    rows.push(row);
  }

  // Country table: lowest coverage first (CRITICAL first), then largest missing
  const tableSorted = [...rows].sort((a, b) => {
    const ca = a.coverage_pct == null ? -1 : a.coverage_pct;
    const cb = b.coverage_pct == null ? -1 : b.coverage_pct;
    if (ca !== cb) return ca - cb;
    return (b.estimated_missing_hotels || 0) - (a.estimated_missing_hotels || 0);
  });

  // Priority ranking: highest opportunity / priority score
  const priorityRanking = [...rows]
    .filter((r) => (r.estimated_missing_hotels || 0) > 0 || r.discovery_candidates_available > 0)
    .sort((a, b) => b.priority_score - a.priority_score)
    .map((r, i) => ({
      rank: i + 1,
      country: r.country,
      coverage_pct: r.coverage_pct,
      hotels_missing: r.estimated_missing_hotels,
      expected_gain: r.expected_new_hotels,
      opportunity_score: r.discovery_value,
      priority_score: r.priority_score,
      reason: r.priority_reason,
    }));

  const heatMap = {
    CRITICAL: [],
    POOR: [],
    FAIR: [],
    GOOD: [],
    EXCELLENT: [],
    UNKNOWN: [],
  };
  for (const r of rows) {
    heatMap[r.coverage_status]?.push(r.country);
  }
  for (const k of Object.keys(heatMap)) {
    heatMap[k].sort((a, b) => {
      const ra = rows.find((x) => x.country === a);
      const rb = rows.find((x) => x.country === b);
      return (ra?.coverage_pct ?? 0) - (rb?.coverage_pct ?? 0);
    });
  }

  const totalCensus = rows.reduce((s, r) => s + r.current_dealality_hotels, 0);
  const totalUniverse = rows.reduce(
    (s, r) => s + (r.estimated_hotel_universe || 0),
    0
  );
  const totalMissing = rows.reduce(
    (s, r) => s + (r.estimated_missing_hotels || 0),
    0
  );
  const totalReview = rows.reduce((s, r) => s + r.current_review_queue, 0);
  const totalDiscovery = rows.reduce(
    (s, r) => s + r.discovery_candidates_available,
    0
  );

  const summary = {
    current_census: totalCensus,
    estimated_cala_universe: totalUniverse,
    overall_coverage_pct:
      totalUniverse > 0
        ? Math.round((1000 * totalCensus) / totalUniverse) / 10
        : null,
    hotels_missing: totalMissing,
    countries_total: rows.length,
    countries_excellent: heatMap.EXCELLENT.length,
    countries_good: heatMap.GOOD.length,
    countries_fair: heatMap.FAIR.length,
    countries_poor: heatMap.POOR.length,
    countries_critical: heatMap.CRITICAL.length,
    countries_unknown: heatMap.UNKNOWN.length,
    countries_complete: heatMap.EXCELLENT.length,
    review_queue: totalReview,
    discovery_queue: totalDiscovery,
    ready_for_import_queue: rows.reduce((s, r) => s + r.ready_for_import_queue, 0),
  };

  const zeroCoverage = rows
    .filter((r) => r.current_dealality_hotels === 0)
    .map((r) => ({
      country: r.country,
      estimated_hotels: r.estimated_hotel_universe,
      discovery_source: r.estimation_source,
      discovery_candidates: r.discovery_candidates_available,
      priority_score: r.priority_score,
      discovery_value: r.discovery_value,
      coverage_status: r.coverage_status,
    }))
    .sort((a, b) => b.priority_score - a.priority_score);

  const nearZero = rows
    .filter(
      (r) =>
        r.current_dealality_hotels > 0 &&
        r.current_dealality_hotels <= 5 &&
        (r.coverage_pct == null || r.coverage_pct < 30)
    )
    .map((r) => ({
      country: r.country,
      current: r.current_dealality_hotels,
      estimated_hotels: r.estimated_hotel_universe,
      coverage_pct: r.coverage_pct,
      discovery_source: r.estimation_source,
      priority_score: r.priority_score,
    }));

  return {
    version: COVERAGE_DASHBOARD_VERSION,
    generated_at: new Date().toISOString(),
    production_writes: false,
    registry_version: "dealality-cala-geography-registry-v1",
    estimation_method:
      "max(dealality_census, cvent_candidate_universe, hotelbeds_pack, census+weak_holds); no fresh web research",
    summary,
    rows: tableSorted,
    priority_ranking: priorityRanking,
    heat_map: heatMap,
    zero_coverage_countries: zeroCoverage,
    near_zero_coverage_countries: nearZero,
    brazil_detail: buildBrazilDetail(rows.find((r) => r.country === "Brazil")),
  };
}

function buildBrazilDetail(br) {
  if (!br) return null;
  const current = br.current_dealality_hotels;
  const universe = br.estimated_hotel_universe || 0;
  const after = (n) => {
    const next = Math.min(universe, current + n);
    const pct = universe > 0 ? Math.round((1000 * next) / universe) / 10 : null;
    return {
      hotels: next,
      coverage_pct: pct,
      status: coverageStatusFromPct(pct, universe > 0),
      hotels_added: next - current,
    };
  };
  const remaining = br.discovery_candidates_available || 0;
  return {
    current_hotels: current,
    estimated_universe: universe,
    coverage_pct: br.coverage_pct,
    coverage_status: br.coverage_status,
    discovery_queue: remaining,
    review_queue: br.current_review_queue,
    ready_for_import_queue: br.ready_for_import_queue,
    known_hold_pool: br.estimation_components?.holds || 0,
    expected_gain: br.expected_new_hotels,
    recommended_batch_size: 500,
    estimation_source: br.estimation_source,
    projected: {
      after_plus_500: after(500),
      after_plus_1000: after(1000),
      after_full_brazil_completion: after(remaining),
    },
  };
}

/**
 * Compare to a prior dashboard snapshot (baseline if missing).
 */
export function compareCoverageTrend(current, prior) {
  if (!prior?.summary) {
    return {
      baseline_established: true,
      prior_generated_at: null,
      note: "No prior coverage dashboard found — this run is the baseline.",
      coverage_change_pp: null,
      hotels_added: null,
      priority_movement: [],
      countries_completed: [],
      countries_newly_critical: [],
    };
  }

  const hotelsAdded =
    (current.summary?.current_census || 0) - (prior.summary?.current_census || 0);
  const covChange =
    current.summary?.overall_coverage_pct != null &&
    prior.summary?.overall_coverage_pct != null
      ? Math.round(
          (current.summary.overall_coverage_pct - prior.summary.overall_coverage_pct) *
            10
        ) / 10
      : null;

  const priorRank = new Map(
    (prior.priority_ranking || []).map((p) => [p.country, p.rank])
  );
  const priority_movement = (current.priority_ranking || [])
    .slice(0, 15)
    .map((p) => {
      const was = priorRank.get(p.country);
      return {
        country: p.country,
        rank_now: p.rank,
        rank_was: was ?? null,
        delta: was != null ? was - p.rank : null,
      };
    });

  const priorStatus = new Map(
    (prior.rows || []).map((r) => [r.country, r.coverage_status])
  );
  const countries_completed = (current.rows || []).filter(
    (r) =>
      r.coverage_status === COVERAGE_STATUS.EXCELLENT &&
      priorStatus.get(r.country) !== COVERAGE_STATUS.EXCELLENT
  ).map((r) => r.country);
  const countries_newly_critical = (current.rows || []).filter(
    (r) =>
      r.coverage_status === COVERAGE_STATUS.CRITICAL &&
      priorStatus.get(r.country) &&
      priorStatus.get(r.country) !== COVERAGE_STATUS.CRITICAL
  ).map((r) => r.country);

  return {
    baseline_established: false,
    prior_generated_at: prior.generated_at || null,
    coverage_change_pp: covChange,
    hotels_added: hotelsAdded,
    priority_movement,
    countries_completed,
    countries_newly_critical,
  };
}

export { DEALALITY_CALA_GEOGRAPHIES };
