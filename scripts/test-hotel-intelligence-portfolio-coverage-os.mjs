#!/usr/bin/env node
/**
 * Portfolio Coverage OS unit tests — no Airtable.
 */
import assert from "node:assert/strict";
import {
  maturityStatusFromRow,
  scorePortfolioCoverage,
  scoreGrowth,
  classifyReadiness,
  MATURITY_STATUS,
  READINESS,
  computeDiscoveryAllocation,
  buildPortfolioCoverageOs,
} from "../lib/hotel-intelligence/portfolio-coverage-os/index.js";
import { buildCalaCoverageDashboard } from "../lib/hotel-intelligence/coverage-dashboard/index.js";

assert.equal(
  maturityStatusFromRow({ coverage_pct: 100, estimated_hotel_universe: 100 }),
  MATURITY_STATUS.COMPLETE
);
assert.equal(
  maturityStatusFromRow({
    coverage_pct: 0,
    estimated_hotel_universe: 100,
    discovery_candidates_available: 50,
  }),
  MATURITY_STATUS.CRITICAL
);

const neglected = scorePortfolioCoverage({
  coverage_pct: 0,
  current_dealality_hotels: 0,
  tourism_priority: "A",
  region: "Caribbean",
  estimation_confidence: "medium",
  maturity_status: MATURITY_STATUS.CRITICAL,
  discovery_candidates_available: 50,
});
const brazilGrowth = scoreGrowth({
  estimated_missing_hotels: 4842,
  discovery_candidates_available: 4842,
  estimation_components: { holds: 4842 },
  estimated_hotel_universe: 5336,
  expected_duplicate_risk: 0.04,
  expected_effort: 0.7,
});
const brazilPortfolio = scorePortfolioCoverage({
  coverage_pct: 9.3,
  current_dealality_hotels: 494,
  tourism_priority: "S",
  region: "South America",
  estimation_confidence: "medium",
  maturity_status: MATURITY_STATUS.EARLY,
});
assert.ok(neglected > 50, "zero-coverage country should score high on portfolio");
assert.ok(brazilGrowth > 70, "Brazil should dominate growth score");
// Portfolio score for a 0% country should be competitive with Brazil's portfolio score
assert.ok(neglected >= brazilPortfolio - 5);

const ready = classifyReadiness({
  discovery_candidates_available: 100,
  estimation_confidence: "medium",
  hbx_wave1_searched: false,
  maturity_status: MATURITY_STATUS.CRITICAL,
});
assert.equal(ready.readiness, READINESS.READY);

const noSrc = classifyReadiness({
  discovery_candidates_available: 0,
  estimated_hotel_universe: null,
  current_dealality_hotels: 0,
  maturity_status: MATURITY_STATUS.UNKNOWN,
});
assert.equal(noSrc.readiness, READINESS.NO_DISCOVERY_SOURCE);

const alloc = computeDiscoveryAllocation(
  {
    countries_total: 52,
    countries_lt_20_pct: 15,
    countries_0_pct: 20,
    countries_unknown: 4,
    countries_complete: 2,
  },
  []
);
assert.ok(alloc.portfolio_completion_pct >= 25);
assert.ok(alloc.strategic_growth_pct + alloc.portfolio_completion_pct === 100 || Math.abs(alloc.strategic_growth_pct + alloc.portfolio_completion_pct - 100) < 0.2);

const dash = buildCalaCoverageDashboard(
  { Brazil: 494, Mexico: 2181, Belize: 12, Bermuda: 0 },
  { root: process.cwd() }
);
const os = buildPortfolioCoverageOs(dash);
assert.ok(os.matrix.length >= 50);
assert.ok(os.portfolio_coverage_ranking[0]);
assert.ok(os.growth_ranking[0]);
assert.equal(os.growth_ranking[0].country, "Brazil");
// Portfolio #1 should NOT be forced Brazil — a neglected geo should be competitive
assert.ok(os.recommended_next_sprint.strategic_growth);
assert.ok(os.recommended_next_sprint.portfolio_completion);
assert.ok(os.roadmap.length === 4);

console.log("test:hotel-intelligence-portfolio-coverage-os OK");
console.log(
  JSON.stringify(
    {
      growth_1: os.growth_ranking[0].country,
      portfolio_1: os.portfolio_coverage_ranking[0].country,
      allocation: os.discovery_allocation,
      sprint_growth: os.recommended_next_sprint.strategic_growth.countries.map((c) => c.country),
      sprint_portfolio: os.recommended_next_sprint.portfolio_completion.countries.map(
        (c) => c.country
      ),
    },
    null,
    2
  )
);
