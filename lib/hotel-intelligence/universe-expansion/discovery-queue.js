/**
 * Prioritized discovery queue from previous audits + coverage scorecard.
 */

export const DISCOVERY_QUEUE_VERSION = "universe-expansion-discovery-queue-v1";

/**
 * @param {object} scorecard — from buildCoverageScorecard
 * @param {object} [opts]
 */
export function buildDiscoveryQueue(scorecard, opts = {}) {
  const rows = scorecard?.rows || [];
  const queue = [];

  for (const r of rows) {
    const census = r.hotels_in_dealality || 0;
    const gap = r.gap_estimate || 0;
    const holds = r.sources?.weak_holds || 0;
    const cvent = r.sources?.cvent_candidates || 0;
    const hbx = r.sources?.hbx_candidates || 0;
    const zero = r.sources?.zero_record_in_geography_audit;

    let tier = null;
    let why = [];

    if (zero || census === 0) {
      tier = 1;
      why.push("Country completely missing or zero census records (geography audit)");
      if (cvent > 0) why.push(`${cvent} Cvent candidates available to reopen`);
      if (holds > 0) why.push(`${holds} weak holds staged from prior shell run`);
    } else if (r.flag === "POOR" || (gap >= 200 && (holds > 0 || cvent > census))) {
      tier = 2;
      why.push(
        `Materially underrepresented (coverage ${r.coverage_pct ?? "n/a"}%, gap ~${gap})`
      );
      if (hbx === 0) {
        why.push("HBX geography discovery never completed (403 block on non-Wave1)");
      }
      if (holds > 0) why.push(`${holds} prior holds — reopen for identity discovery`);
    } else if (r.flag === "PARTIAL" && gap >= 50) {
      tier = 3;
      why.push(`Major market incomplete inventory (gap ~${gap})`);
    } else if (gap > 0 && r.flag !== "COMPLETE" && r.flag !== "GOOD") {
      tier = 4;
      why.push(`Secondary / residual gap (~${gap})`);
    } else {
      continue;
    }

    // Expected gain: min(gap, holds||cvent remaining)
    const reopenable = Math.max(holds, Math.max(0, cvent - census));
    const expected_gain = Math.min(gap || reopenable, reopenable || gap || 0);

    queue.push({
      rank: 0,
      tier,
      country: r.country,
      flag: r.flag,
      coverage_pct: r.coverage_pct,
      hotels_in_dealality: census,
      expected_approximate_universe: r.expected_approximate_universe,
      gap_estimate: gap,
      expected_gain,
      why_prioritized: why.join("; "),
      blockers: [
        hbx === 0 && census < 100
          ? "HBX Content API geography expansion blocked (HTTP 403) — use Cvent URL city inference + official directories"
          : null,
        holds > 0 && r.confidence !== "high"
          ? "Prior holds mostly cvent_only_missing_city — require city inference before NEW_HOTEL"
          : null,
      ].filter(Boolean),
      recommended_batch_size: Math.min(250, Math.max(50, expected_gain || 250)),
    });
  }

  queue.sort((a, b) => {
    if (a.tier !== b.tier) return a.tier - b.tier;
    if ((b.expected_gain || 0) !== (a.expected_gain || 0)) {
      return (b.expected_gain || 0) - (a.expected_gain || 0);
    }
    return String(a.country).localeCompare(String(b.country));
  });
  queue.forEach((q, i) => {
    q.rank = i + 1;
  });

  // Hard pin: Brazil was named next unresolved pool by exhausted orchestrator
  if (opts.pinBrazilFirst !== false) {
    const brIdx = queue.findIndex((q) => q.country === "Brazil");
    if (brIdx > 0) {
      const [br] = queue.splice(brIdx, 1);
      br.why_prioritized = `Orchestrator next_unresolved_pool=Brazil; ${br.why_prioritized}`;
      queue.unshift(br);
      queue.forEach((q, i) => {
        q.rank = i + 1;
      });
    }
  }

  return {
    version: DISCOVERY_QUEUE_VERSION,
    generated_at: new Date().toISOString(),
    item_count: queue.length,
    items: queue,
  };
}
