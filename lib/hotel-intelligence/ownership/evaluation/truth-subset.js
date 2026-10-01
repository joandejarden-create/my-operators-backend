/**
 * Manual truth-subset scaffold (≥20) for ownership evaluation accuracy.
 */

import { createHash } from "node:crypto";
import { OWNERSHIP_COHORT_SEED } from "./cohort.js";

export const TRUTH_SUBSET_VERSION = "ownership-truth-subset-v1";

/**
 * Pick a stratified subset from cohort hotels for human review.
 * @param {object[]} cohortHotels
 * @param {{ size?: number }} [opts]
 */
export function buildTruthSubsetScaffold(cohortHotels, opts = {}) {
  const size = Math.max(20, Number(opts.size || 20));
  const list = Array.isArray(cohortHotels) ? [...cohortHotels] : [];
  list.sort((a, b) =>
    String(a.stable_hash || hash(a.census_record_id)).localeCompare(
      String(b.stable_hash || hash(b.census_record_id))
    )
  );

  const byRegion = new Map();
  for (const h of list) {
    const r = h.eval_region || "Unknown";
    if (!byRegion.has(r)) byRegion.set(r, []);
    byRegion.get(r).push(h);
  }

  const selected = [];
  const selectedIds = new Set();
  const regions = [...byRegion.keys()].sort();
  const per = Math.max(1, Math.floor(size / Math.max(1, regions.length)));

  for (const region of regions) {
    const pool = byRegion.get(region) || [];
    let taken = 0;
    for (const h of pool) {
      if (selectedIds.has(h.census_record_id)) continue;
      selected.push(toTruthRow(h));
      selectedIds.add(h.census_record_id);
      taken += 1;
      if (taken >= per || selected.length >= size) break;
    }
    if (selected.length >= size) break;
  }

  for (const h of list) {
    if (selected.length >= size) break;
    if (selectedIds.has(h.census_record_id)) continue;
    selected.push(toTruthRow(h));
    selectedIds.add(h.census_record_id);
  }

  return {
    version: TRUTH_SUBSET_VERSION,
    seed: OWNERSHIP_COHORT_SEED,
    target_size: size,
    actual_size: selected.length,
    instructions:
      "Fill expected_* fields manually. Do not invent. Leave null when unknown. Compare against system output separately.",
    rows: selected,
  };
}

function toTruthRow(h) {
  return {
    census_record_id: h.census_record_id,
    eval_region: h.eval_region,
    country: h.country,
    name: h.name,
    brand: h.brand,
    expected_propco_legal_name: null,
    expected_parent_legal_name: null,
    expected_sponsor_legal_name: null,
    expected_operator_legal_name: null,
    expected_developer_legal_name: null,
    notes: null,
    reviewer: null,
    reviewed_at: null,
  };
}

function hash(id) {
  return createHash("sha256")
    .update(`${OWNERSHIP_COHORT_SEED}:truth:${id}`)
    .digest("hex");
}
