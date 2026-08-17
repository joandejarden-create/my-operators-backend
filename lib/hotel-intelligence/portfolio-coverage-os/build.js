/**
 * Build Portfolio Coverage OS from coverage dashboard rows.
 */

import {
  PORTFOLIO_OS_VERSION,
  MATURITY_STATUS,
  READINESS,
  maturityStatusFromRow,
  scorePortfolioCoverage,
  scoreGrowth,
  classifyReadiness,
  recommendedAction,
  computeRegionHealth,
} from "./scores.js";
import {
  computeDiscoveryAllocation,
  buildCoverageRoadmap,
  planDiscoverySprint,
} from "./planner.js";

/**
 * @param {object} coverageDashboard — from buildCalaCoverageDashboard
 */
export function buildPortfolioCoverageOs(coverageDashboard, opts = {}) {
  const baseRows = coverageDashboard?.rows || [];
  const regionHealth = computeRegionHealth(baseRows);

  let rows = baseRows.map((r) => {
    const maturity_status = maturityStatusFromRow(r);
    const enriched = { ...r, maturity_status, regionHealth };
    const readinessInfo = classifyReadiness(enriched);
    enriched.readiness = readinessInfo.readiness;
    enriched.readiness_note = readinessInfo.note;
    enriched.soft_blocker = readinessInfo.soft_blocker || null;
    enriched.portfolio_coverage_score = scorePortfolioCoverage(enriched, {
      regionHealth,
    });
    enriched.growth_score = scoreGrowth(enriched);
    enriched.recommended_action = recommendedAction(enriched);
    enriched.difficulty = difficultyFrom(enriched);
    enriched.discovery_confidence = enriched.estimation_confidence || enriched.confidence;
    enriched.estimated_completion_effort = completionEffort(enriched);
    enriched.overall_completion_difficulty = overallDifficulty(enriched);
    enriched.known_discovery_stock = enriched.discovery_candidates_available;
    return enriched;
  });

  // Re-score portfolio once region health known (already done)
  const portfolioHealth = buildPortfolioHealth(rows, coverageDashboard?.summary);

  const allocation = computeDiscoveryAllocation(portfolioHealth, rows);
  const roadmap = buildCoverageRoadmap(rows);
  const sprint = planDiscoverySprint(rows, allocation, opts);

  const portfolioRanking = [...rows]
    .sort((a, b) => b.portfolio_coverage_score - a.portfolio_coverage_score)
    .map((r, i) => ({
      rank: i + 1,
      country: r.country,
      portfolio_coverage_score: r.portfolio_coverage_score,
      coverage_pct: r.coverage_pct,
      maturity_status: r.maturity_status,
      readiness: r.readiness,
      recommended_action: r.recommended_action,
    }));

  const growthRanking = [...rows]
    .sort((a, b) => b.growth_score - a.growth_score)
    .map((r, i) => ({
      rank: i + 1,
      country: r.country,
      growth_score: r.growth_score,
      hotels_missing: r.estimated_missing_hotels,
      discovery_stock: r.discovery_candidates_available,
      maturity_status: r.maturity_status,
      readiness: r.readiness,
    }));

  const portfolioMap = {
    COMPLETE: rows.filter((r) => r.maturity_status === MATURITY_STATUS.COMPLETE).map((r) => r.country),
    NEAR_COMPLETE: rows
      .filter((r) => r.maturity_status === MATURITY_STATUS.NEAR_COMPLETE)
      .map((r) => r.country),
    ADVANCING: rows
      .filter((r) => r.maturity_status === MATURITY_STATUS.ADVANCING)
      .map((r) => r.country),
    PARTIAL: rows.filter((r) => r.maturity_status === MATURITY_STATUS.PARTIAL).map((r) => r.country),
    EARLY: rows.filter((r) => r.maturity_status === MATURITY_STATUS.EARLY).map((r) => r.country),
    CRITICAL: rows.filter((r) => r.maturity_status === MATURITY_STATUS.CRITICAL).map((r) => r.country),
    UNKNOWN: rows.filter((r) => r.maturity_status === MATURITY_STATUS.UNKNOWN).map((r) => r.country),
  };

  const readinessGroups = groupBy(rows, "readiness");

  const kpis = buildKpis(rows, portfolioHealth, growthRanking, portfolioRanking);

  // Matrix sorted by portfolio score (completeness lens) with growth visible
  const matrix = [...rows]
    .sort((a, b) => b.portfolio_coverage_score - a.portfolio_coverage_score)
    .map((r) => ({
      country: r.country,
      current_hotels: r.current_dealality_hotels,
      estimated_universe: r.estimated_hotel_universe,
      coverage_pct: r.coverage_pct,
      coverage_status: r.maturity_status,
      portfolio_coverage_score: r.portfolio_coverage_score,
      growth_score: r.growth_score,
      discovery_opportunity: r.discovery_value,
      known_discovery_stock: r.known_discovery_stock,
      review_queue: r.current_review_queue,
      confidence: r.confidence,
      recommended_action: r.recommended_action,
      readiness: r.readiness,
      hotels_missing: r.estimated_missing_hotels,
      difficulty: r.difficulty,
      estimated_review_burden: r.review_burden,
      estimated_completion_effort: r.estimated_completion_effort,
      overall_completion_difficulty: r.overall_completion_difficulty,
      region: r.region,
    }));

  return {
    version: PORTFOLIO_OS_VERSION,
    generated_at: new Date().toISOString(),
    production_writes: false,
    discovery_ran: false,
    region_health: regionHealth,
    portfolio_health: portfolioHealth,
    matrix,
    portfolio_coverage_ranking: portfolioRanking,
    growth_ranking: growthRanking,
    discovery_allocation: allocation,
    country_readiness: readinessGroups,
    portfolio_map: portfolioMap,
    roadmap,
    recommended_next_sprint: sprint,
    kpis,
    rows,
  };
}

function buildPortfolioHealth(rows, summary = {}) {
  const pct = (pred) => rows.filter(pred).length;
  const withPct = rows.filter((r) => r.coverage_pct != null && Number.isFinite(r.coverage_pct));
  return {
    countries_total: rows.length,
    countries_complete: pct((r) => r.maturity_status === MATURITY_STATUS.COMPLETE),
    countries_gt_95_pct: pct((r) => (r.coverage_pct ?? -1) >= 95),
    countries_80_to_95_pct: pct(
      (r) => (r.coverage_pct ?? -1) >= 80 && (r.coverage_pct ?? 0) < 95
    ),
    countries_50_to_80_pct: pct(
      (r) => (r.coverage_pct ?? -1) >= 50 && (r.coverage_pct ?? 0) < 80
    ),
    countries_20_to_50_pct: pct(
      (r) => (r.coverage_pct ?? -1) >= 20 && (r.coverage_pct ?? 0) < 50
    ),
    countries_lt_20_pct: pct(
      (r) => r.coverage_pct != null && r.coverage_pct < 20 && r.coverage_pct > 0
    ),
    countries_0_pct: pct((r) => r.coverage_pct === 0 || (r.current_dealality_hotels === 0 && (r.estimated_hotel_universe || 0) > 0)),
    countries_unknown: pct((r) => r.maturity_status === MATURITY_STATUS.UNKNOWN),
    overall_cala_country_coverage_pct:
      // % of countries that are represented (census > 0)
      Math.round(
        (1000 * rows.filter((r) => (r.current_dealality_hotels || 0) > 0).length) /
          Math.max(1, rows.length)
      ) / 10,
    overall_hotel_coverage_pct: summary.overall_coverage_pct ?? null,
    current_hotels: summary.current_census ?? rows.reduce((s, r) => s + (r.current_dealality_hotels || 0), 0),
    estimated_universe: summary.estimated_cala_universe ?? null,
    avg_country_coverage_pct:
      withPct.length > 0
        ? Math.round(
            (withPct.reduce((s, r) => s + r.coverage_pct, 0) / withPct.length) * 10
          ) / 10
        : null,
  };
}

function buildKpis(rows, health, growthRanking, portfolioRanking) {
  const neglected = [...rows]
    .filter((r) => (r.current_dealality_hotels || 0) === 0 || (r.coverage_pct != null && r.coverage_pct < 20))
    .sort((a, b) => b.portfolio_coverage_score - a.portfolio_coverage_score)[0];

  const largestOpp = growthRanking[0];
  const highestStrategic = portfolioRanking[0];

  // Highest ROI: growth per unit effort among ready countries
  const roi = [...rows]
    .filter((r) => r.readiness === READINESS.READY && (r.discovery_candidates_available || 0) > 0)
    .map((r) => ({
      country: r.country,
      roi:
        (r.discovery_candidates_available || 0) /
        Math.max(0.2, r.expected_effort || 0.5) /
        (1 + (r.expected_duplicate_risk || 0)),
      growth_score: r.growth_score,
      portfolio_coverage_score: r.portfolio_coverage_score,
    }))
    .sort((a, b) => b.roi - a.roi)[0];

  return {
    current_hotels: health.current_hotels,
    estimated_universe: health.estimated_universe,
    overall_hotel_coverage: health.overall_hotel_coverage_pct,
    overall_cala_country_coverage: health.overall_cala_country_coverage_pct,
    countries_complete: health.countries_complete,
    countries_critical: rows.filter((r) => r.maturity_status === MATURITY_STATUS.CRITICAL).length,
    countries_unknown: health.countries_unknown,
    largest_opportunity: largestOpp
      ? { country: largestOpp.country, growth_score: largestOpp.growth_score }
      : null,
    most_neglected_country: neglected
      ? {
          country: neglected.country,
          portfolio_coverage_score: neglected.portfolio_coverage_score,
          coverage_pct: neglected.coverage_pct,
        }
      : null,
    highest_roi_discovery: roi
      ? { country: roi.country, roi: Math.round(roi.roi * 10) / 10 }
      : null,
    highest_strategic_discovery: highestStrategic
      ? {
          country: highestStrategic.country,
          portfolio_coverage_score: highestStrategic.portfolio_coverage_score,
        }
      : null,
  };
}

function difficultyFrom(r) {
  const effort = r.expected_effort || 0.5;
  const dup = r.expected_duplicate_risk || 0.05;
  const conf = r.estimation_confidence || "medium";
  let d = effort * 50 + dup * 100;
  if (conf === "low") d += 20;
  if ((r.discovery_candidates_available || 0) === 0) d += 30;
  if (d >= 70) return "HARD";
  if (d >= 40) return "MEDIUM";
  return "EASY";
}

function completionEffort(r) {
  const missing = r.estimated_missing_hotels || 0;
  const stock = r.discovery_candidates_available || 0;
  if (missing === 0) return "DONE";
  if (stock === 0) return "NEEDS_SOURCE";
  if (missing <= 50) return "SMALL";
  if (missing <= 300) return "MEDIUM";
  return "LARGE";
}

function overallDifficulty(r) {
  const map = { EASY: 1, MEDIUM: 2, HARD: 3 };
  const effortMap = { DONE: 0, SMALL: 1, MEDIUM: 2, LARGE: 3, NEEDS_SOURCE: 4 };
  const score = (map[r.difficulty] || 2) + (effortMap[r.estimated_completion_effort] || 2);
  if (score <= 2) return "LOW";
  if (score <= 4) return "MODERATE";
  if (score <= 5) return "HIGH";
  return "VERY_HIGH";
}

function groupBy(rows, key) {
  const out = {};
  for (const r of rows) {
    const k = r[key] || "UNKNOWN";
    if (!out[k]) out[k] = [];
    out[k].push({
      country: r.country,
      coverage_pct: r.coverage_pct,
      readiness_note: r.readiness_note,
      discovery_stock: r.discovery_candidates_available,
    });
  }
  return out;
}
