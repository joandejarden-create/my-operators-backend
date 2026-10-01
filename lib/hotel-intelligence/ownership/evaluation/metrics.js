/**
 * Ownership evaluation metrics (coverage / confidence / efficiency / quality skeleton).
 */

export const OWNERSHIP_METRICS_VERSION = "ownership-eval-metrics-v1";

/**
 * @param {object[]} hotelResults — per-hotel research/ownership payloads
 * @param {object} [opts]
 */
export function computeOwnershipEvalMetrics(hotelResults, opts = {}) {
  const rows = Array.isArray(hotelResults) ? hotelResults : [];
  const n = rows.length || 1;

  const coverage = {
    propco_identified_pct: pct(rows, (r) => hasRole(r, "propco")),
    parent_identified_pct: pct(rows, (r) => hasRole(r, "parent")),
    sponsor_identified_pct: pct(rows, (r) => hasRole(r, "ultimate_sponsor")),
    operator_identified_pct: pct(rows, (r) => hasRole(r, "operator")),
    developer_identified_pct: pct(rows, (r) => hasRole(r, "developer")),
  };

  const verificationCounts = {
    verified: 0,
    high: 0,
    probable: 0,
    needs_review: 0,
    conflict: 0,
    unknown: 0,
  };
  for (const r of rows) {
    const v = r.verification?.propco || r.status_summary?.propco || "unknown";
    if (verificationCounts[v] != null) verificationCounts[v] += 1;
    else verificationCounts.unknown += 1;
  }

  const confidence = {};
  for (const [k, v] of Object.entries(verificationCounts)) {
    confidence[`${k}_pct`] = Math.round((1000 * v) / n) / 10;
  }

  let searches = 0;
  let pages = 0;
  let latency = 0;
  let stageAOnly = 0;
  let usedC = 0;
  for (const r of rows) {
    searches += Number(r.metrics?.searches || 0);
    pages += Number(r.metrics?.pages_fetched || 0);
    latency += Number(r.metrics?.latency_ms || 0);
    const stages = r.stages_completed || [];
    if (stages.length === 1 && stages[0] === "A") stageAOnly += 1;
    if (stages.includes("C")) usedC += 1;
  }

  const efficiency = {
    resolved_existing_or_stage_a_only_pct: Math.round((1000 * stageAOnly) / n) / 10,
    required_search_pct: Math.round((1000 * usedC) / n) / 10,
    avg_searches_per_property: Math.round((10 * searches) / n) / 10,
    avg_pages_fetched_per_property: Math.round((10 * pages) / n) / 10,
    avg_latency_ms: Math.round(latency / n),
  };

  const truth = opts.truthSubsetResults || [];
  const quality = {
    truth_subset_size: truth.length,
    truth_propco_exact_match_pct: truth.length
      ? pct(truth, (t) => t.propco_match === true)
      : null,
    false_owner_attribution_count: truth.filter((t) => t.false_owner).length,
    operator_mistaken_for_owner_count: truth.filter(
      (t) => t.operator_as_owner
    ).length,
    brand_mistaken_for_owner_count: truth.filter((t) => t.brand_as_owner)
      .length,
    note: "Quality counters require filled truth subset; null/zero until reviewed",
  };

  return {
    version: OWNERSHIP_METRICS_VERSION,
    hotel_count: rows.length,
    coverage,
    confidence,
    efficiency,
    quality,
  };
}

function hasRole(row, key) {
  const summary = row.summary || {};
  return Boolean(summary[key]?.entity_id || summary[key]?.display_name);
}

function pct(rows, pred) {
  if (!rows.length) return 0;
  const hit = rows.filter(pred).length;
  return Math.round((1000 * hit) / rows.length) / 10;
}
