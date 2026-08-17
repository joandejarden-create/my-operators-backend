/**
 * Country prioritization engine — no hard-coded Brazil.
 * Brazil ranks #1 only when score warrants it.
 */

export const PRIORITY_ENGINE_VERSION = "discovery-factory-priority-v1";

/** Strategic importance weights for CALA commercial focus (0–1). */
const STRATEGIC = Object.freeze({
  Brazil: 1.0,
  Mexico: 0.95,
  Argentina: 0.85,
  Colombia: 0.9,
  "Costa Rica": 0.8,
  "Dominican Republic": 0.85,
  Peru: 0.75,
  Chile: 0.7,
  Panama: 0.75,
  Jamaica: 0.65,
  "Puerto Rico": 0.7,
  Cuba: 0.55,
  Bahamas: 0.6,
  Barbados: 0.45,
  Aruba: 0.5,
  Belize: 0.45,
  Guatemala: 0.5,
  Honduras: 0.4,
  Nicaragua: 0.4,
  "El Salvador": 0.35,
  Ecuador: 0.55,
  Uruguay: 0.45,
  Paraguay: 0.4,
  Bolivia: 0.35,
  Venezuela: 0.3,
  "Turks and Caicos": 0.55,
  "U.S. Virgin Islands": 0.5,
  "Saint Barthélemy": 0.45,
  "Saint Martin": 0.4,
  "Sint Maarten": 0.4,
});

/**
 * @param {object} row — coverage scorecard-like row + discovery metrics
 */
export function scoreCountryPriority(row = {}) {
  const census = Number(row.hotels_in_dealality || row.current_census || 0);
  const expected = Number(
    row.expected_approximate_universe || row.estimated_universe || 0
  );
  const gap = Math.max(0, expected - census);
  const coveragePct =
    expected > 0 ? (100 * census) / expected : census > 0 ? 100 : 0;
  const candidates = Number(
    row.discovery_candidates || row.sources?.cvent_candidates || row.sources?.weak_holds || 0
  );
  const holds = Number(row.sources?.weak_holds || 0);
  const sourceAvail = Math.min(1, Math.max(candidates, holds) / Math.max(gap, 1));
  const strategic = STRATEGIC[row.country] ?? (census === 0 ? 0.5 : 0.35);

  // Estimated review burden: lower city confidence / missing HBX → higher burden
  const hbx = Number(row.sources?.hbx_candidates || 0);
  const reviewBurden = hbx > 0 ? 0.25 : 0.7; // Cvent-only countries: higher review
  const expectedDupRate = coveragePct > 60 ? 0.15 : coveragePct > 30 ? 0.08 : 0.04;

  // Weighted score (higher = process sooner)
  const missingScore = Math.min(1, gap / 5000) * 40;
  const coverageScore = ((100 - Math.min(coveragePct, 100)) / 100) * 25;
  const strategicScore = strategic * 15;
  const sourceScore = sourceAvail * 12;
  const reviewPenalty = reviewBurden * 5;
  const dupPenalty = expectedDupRate * 8;

  const priority_score = Math.round(
    (missingScore + coverageScore + strategicScore + sourceScore - reviewPenalty - dupPenalty) *
      10
  ) / 10;

  return {
    country: row.country,
    priority_score,
    components: {
      missing_hotels_estimate: gap,
      coverage_pct: Math.round(coveragePct * 10) / 10,
      strategic_importance: strategic,
      source_availability: Math.round(sourceAvail * 1000) / 1000,
      review_burden: reviewBurden,
      expected_duplicate_rate: expectedDupRate,
    },
  };
}

/**
 * @param {object[]} scorecardRows
 */
export function buildPrioritizedQueue(scorecardRows = []) {
  const scored = scorecardRows
    .filter((r) => r.country && (r.gap_estimate > 0 || r.sources?.zero_record_in_geography_audit))
    .map((r) => {
      const p = scoreCountryPriority(r);
      return {
        ...p,
        flag: r.flag,
        current_census: r.hotels_in_dealality,
        estimated_universe: r.expected_approximate_universe,
        discovery_candidates: Math.max(
          r.sources?.weak_holds || 0,
          Math.max(0, (r.sources?.cvent_candidates || 0) - (r.hotels_in_dealality || 0))
        ),
        why: explainPriority(p, r),
      };
    })
    .sort((a, b) => b.priority_score - a.priority_score);

  scored.forEach((q, i) => {
    q.rank = i + 1;
  });

  return {
    version: PRIORITY_ENGINE_VERSION,
    generated_at: new Date().toISOString(),
    item_count: scored.length,
    items: scored,
  };
}

function explainPriority(p, row) {
  const c = p.components;
  const parts = [
    `gap≈${c.missing_hotels_estimate}`,
    `coverage ${c.coverage_pct}%`,
    `strategic ${c.strategic_importance}`,
    `sources ${c.source_availability}`,
  ];
  if (row.sources?.hbx_candidates === 0) parts.push("HBX unavailable");
  return parts.join("; ");
}
