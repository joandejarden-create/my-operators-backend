/**
 * Dynamic discovery allocation + roadmap + sprint planner.
 */

import {
  MATURITY_STATUS,
  READINESS,
  RECOMMENDED_ACTION,
} from "./scores.js";

/**
 * Calculate growth vs portfolio split from portfolio health.
 * More critical/zero countries → more portfolio share.
 */
export function computeDiscoveryAllocation(portfolioHealth = {}, rows = []) {
  const total = Number(portfolioHealth.countries_total || rows.length || 1);
  const critical = Number(portfolioHealth.countries_lt_20_pct || 0);
  const zero = Number(portfolioHealth.countries_0_pct || 0);
  const unknown = Number(portfolioHealth.countries_unknown || 0);
  const complete = Number(portfolioHealth.countries_complete || 0);

  const neglectedShare = (critical + zero * 0.5 + unknown * 0.3) / total;
  // Map neglected share → portfolio % between 25% and 55%
  let portfolioPct = Math.round((25 + neglectedShare * 40) * 10) / 10;
  portfolioPct = Math.max(25, Math.min(55, portfolioPct));
  const growthPct = Math.round((100 - portfolioPct) * 10) / 10;

  const readyGrowth = rows.filter(
    (r) =>
      r.recommended_action === RECOMMENDED_ACTION.GROWTH_BATCH &&
      r.readiness === READINESS.READY
  ).length;
  const readyPortfolio = rows.filter(
    (r) =>
      r.recommended_action === RECOMMENDED_ACTION.PORTFOLIO_BATCH &&
      (r.readiness === READINESS.READY || r.readiness === READINESS.LOW_CONFIDENCE)
  ).length;

  // If almost no ready portfolio countries, shift toward growth
  let adjGrowth = growthPct;
  let adjPortfolio = portfolioPct;
  if (readyPortfolio === 0 && readyGrowth > 0) {
    adjGrowth = 85;
    adjPortfolio = 15;
  } else if (readyGrowth === 0 && readyPortfolio > 0) {
    adjGrowth = 20;
    adjPortfolio = 80;
  }

  return {
    strategic_growth_pct: adjGrowth,
    portfolio_completion_pct: adjPortfolio,
    rationale: [
      `${zero} countries at 0% and ${critical} below 20% → portfolio share ${adjPortfolio}%`,
      `${complete} complete countries reduce urgency for pure completeness`,
      `Ready growth targets: ${readyGrowth}; ready portfolio targets: ${readyPortfolio}`,
      "Allocation is recomputed each dashboard run — not hard-coded.",
    ].join("; "),
    ready_growth_targets: readyGrowth,
    ready_portfolio_targets: readyPortfolio,
  };
}

/**
 * Phase roadmap across the portfolio.
 */
export function buildCoverageRoadmap(rows = []) {
  const phases = [
    {
      phase: 1,
      goal: "Bring every country above 20%",
      threshold: 20,
      countries: [],
    },
    {
      phase: 2,
      goal: "Bring every country above 50%",
      threshold: 50,
      countries: [],
    },
    {
      phase: 3,
      goal: "Bring every country above 80%",
      threshold: 80,
      countries: [],
    },
    {
      phase: 4,
      goal: "Complete remaining gaps (≥95%)",
      threshold: 95,
      countries: [],
    },
  ];

  for (const r of rows) {
    const pct = r.coverage_pct;
    const name = r.country;
    if (pct == null || !Number.isFinite(pct)) {
      phases[0].countries.push({
        country: name,
        current_pct: pct,
        readiness: r.readiness,
        note: "Unknown — needs source before % threshold applies",
      });
      continue;
    }
    if (pct < 20) {
      phases[0].countries.push(phaseItem(r, 20));
    } else if (pct < 50) {
      phases[1].countries.push(phaseItem(r, 50));
    } else if (pct < 80) {
      phases[2].countries.push(phaseItem(r, 80));
    } else if (pct < 95) {
      phases[3].countries.push(phaseItem(r, 95));
    }
  }

  return phases.map((p) => ({
    ...p,
    country_count: p.countries.length,
    estimated_hotels_to_threshold: p.countries.reduce(
      (s, c) => s + (c.hotels_needed_for_threshold || 0),
      0
    ),
  }));
}

function phaseItem(r, threshold) {
  const universe = r.estimated_hotel_universe || 0;
  const current = r.current_dealality_hotels || 0;
  const targetHotels = universe > 0 ? Math.ceil((threshold / 100) * universe) : null;
  const needed =
    targetHotels != null ? Math.max(0, targetHotels - current) : r.estimated_missing_hotels;
  return {
    country: r.country,
    current_pct: r.coverage_pct,
    readiness: r.readiness,
    hotels_needed_for_threshold: needed,
    discovery_stock: r.discovery_candidates_available,
  };
}

/**
 * Next discovery sprint: multi-country balanced plan.
 */
export function planDiscoverySprint(rows = [], allocation = {}, opts = {}) {
  const growthBudget = opts.growthBudgetHotels ?? 700;
  const portfolioBudget = opts.portfolioBudgetHotels ?? 300;
  // Rebalance budgets from allocation %
  const totalBudget = growthBudget + portfolioBudget;
  const gPct = (allocation.strategic_growth_pct ?? 70) / 100;
  const pPct = (allocation.portfolio_completion_pct ?? 30) / 100;
  const gBudget = Math.round(totalBudget * gPct);
  const pBudget = Math.round(totalBudget * pPct);

  const growthPool = [...rows]
    .filter(
      (r) =>
        r.readiness === READINESS.READY &&
        (r.discovery_candidates_available || 0) > 0 &&
        r.maturity_status !== MATURITY_STATUS.COMPLETE
    )
    .sort((a, b) => b.growth_score - a.growth_score);

  const portfolioPool = [...rows]
    .filter(
      (r) =>
        (r.readiness === READINESS.READY || r.readiness === READINESS.LOW_CONFIDENCE) &&
        (r.discovery_candidates_available || 0) > 0 &&
        r.maturity_status !== MATURITY_STATUS.COMPLETE &&
        r.maturity_status !== MATURITY_STATUS.NEAR_COMPLETE &&
        (r.maturity_status === MATURITY_STATUS.CRITICAL ||
          r.maturity_status === MATURITY_STATUS.EARLY ||
          r.maturity_status === MATURITY_STATUS.PARTIAL ||
          (r.coverage_pct != null && r.coverage_pct < 50))
    )
    .sort((a, b) => b.portfolio_coverage_score - a.portfolio_coverage_score);

  const strategic = allocateTrack(growthPool, gBudget, "STRATEGIC_GROWTH", 3);
  const portfolio = allocateTrack(
    portfolioPool.filter((r) => !strategic.countries.some((c) => c.country === r.country)),
    pBudget,
    "PORTFOLIO_COMPLETION",
    5
  );

  const hotelsAdded =
    strategic.planned_hotels + portfolio.planned_hotels;
  const countriesImproved =
    strategic.countries.length + portfolio.countries.length;

  const avgReview =
    [...strategic.countries, ...portfolio.countries].reduce(
      (s, c) => s + (c.review_burden || 0.5),
      0
    ) / Math.max(1, countriesImproved);

  const avgConf =
    [...strategic.countries, ...portfolio.countries].reduce((s, c) => {
      const map = { high: 0.85, medium: 0.65, low: 0.4 };
      return s + (map[c.confidence] || 0.5);
    }, 0) / Math.max(1, countriesImproved);

  // Portfolio health improvement proxy: weight countries starting below 20%
  const criticalTouched = [...strategic.countries, ...portfolio.countries].filter(
    (c) => (c.coverage_pct_before ?? 100) < 20
  ).length;

  return {
    strategic_growth: strategic,
    portfolio_completion: portfolio,
    estimates: {
      hotels_added: hotelsAdded,
      countries_improved: countriesImproved,
      countries_below_20_improved: criticalTouched,
      portfolio_health_improvement:
        criticalTouched > 0
          ? `+${criticalTouched} countries moving off <20% floor`
          : "Incremental completeness gains",
      coverage_increase_note:
        "Hotel coverage rises with growth track; portfolio health rises with Track B",
      review_burden: Math.round(avgReview * 1000) / 1000,
      confidence: Math.round(avgConf * 1000) / 1000,
      growth_budget_hotels: gBudget,
      portfolio_budget_hotels: pBudget,
    },
  };
}

function allocateTrack(pool, budget, track, maxCountries) {
  const countries = [];
  let remaining = budget;
  for (const r of pool) {
    if (countries.length >= maxCountries || remaining <= 0) break;
    const stock = r.discovery_candidates_available || 0;
    const batch = Math.min(
      remaining,
      stock,
      track === "STRATEGIC_GROWTH" ? 500 : 100
    );
    if (batch < 20 && stock < 20 && track === "STRATEGIC_GROWTH") continue;
    if (batch <= 0) continue;
    countries.push({
      country: r.country,
      track,
      planned_batch: batch,
      growth_score: r.growth_score,
      portfolio_coverage_score: r.portfolio_coverage_score,
      coverage_pct_before: r.coverage_pct,
      maturity_status: r.maturity_status,
      readiness: r.readiness,
      review_burden: r.review_burden,
      confidence: r.confidence,
      discovery_stock: stock,
    });
    remaining -= batch;
  }
  return {
    track,
    countries,
    planned_hotels: countries.reduce((s, c) => s + c.planned_batch, 0),
    budget,
    budget_remaining: remaining,
  };
}
