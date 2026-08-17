/**
 * Operating metrics for Research Engine V2 shadow ops.
 */

export function emptyMetrics() {
  return {
    safety: {
      material_false_positives: null,
      reviewed_false_positives: null,
      bad_reflag_rate: null,
      note: "FP rates filled after steward review cycles",
    },
    discovery: {
      status_changes_found: 0,
      missing_census_records: 0,
      activation_candidates: 0,
      cross_table_inconsistencies: 0,
      image_issues: 0,
    },
    efficiency: {
      hotels_checked: 0,
      brands_checked: 0,
      elapsed_ms: 0,
      hotels_per_minute: 0,
      brands_per_minute: 0,
      external_cost_usd: 0,
      source_success_rate: null,
    },
    steward_workload: {
      queue_items_created: 0,
      p0: 0,
      p1: 0,
      p2: 0,
      p3: 0,
      approval_rate: null,
      rejection_rate: null,
      needs_more_research_rate: null,
      average_queue_age_days: null,
    },
    source_reliability: {
      available: 0,
      blocked: 0,
      failed: 0,
      empty: 0,
      not_applicable: 0,
      fallback_used: 0,
    },
  };
}

/**
 * @param {object} metrics
 * @param {object} run
 */
export function finalizeMetrics(metrics, run = {}) {
  const m = { ...metrics };
  const elapsed = run.elapsedMs || m.efficiency.elapsed_ms || 1;
  const hotels = run.hotelsChecked || m.efficiency.hotels_checked || 0;
  const brands = run.brandsChecked || m.efficiency.brands_checked || 0;
  m.efficiency.elapsed_ms = elapsed;
  m.efficiency.hotels_checked = hotels;
  m.efficiency.brands_checked = brands;
  m.efficiency.hotels_per_minute = elapsed ? Number(((hotels / elapsed) * 60000).toFixed(2)) : 0;
  m.efficiency.brands_per_minute = elapsed ? Number(((brands / elapsed) * 60000).toFixed(2)) : 0;
  m.efficiency.external_cost_usd = run.externalCostUsd ?? 0;

  const src = m.source_reliability;
  const total = src.available + src.blocked + src.failed + src.empty + src.not_applicable;
  m.efficiency.source_success_rate = total ? Number((src.available / total).toFixed(3)) : null;
  return m;
}

/**
 * @param {object} metrics
 * @param {string} state
 */
export function bumpSourceState(metrics, state) {
  const key = String(state || "")
    .toLowerCase()
    .replace(/\s+/g, "_");
  const map = {
    available: "available",
    blocked: "blocked",
    failed: "failed",
    empty: "empty",
    not_applicable: "not_applicable",
  };
  const k = map[key];
  if (k) metrics.source_reliability[k]++;
}
