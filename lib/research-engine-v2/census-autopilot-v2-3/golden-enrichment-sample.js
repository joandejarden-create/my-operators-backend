/**
 * Golden enrichment sample on independently discovered hotels — reuses V2.2 wave runner.
 */

import { runProductionWave } from "../census-autopilot-v2-2/production-wave-runner.js";
import { createHash } from "node:crypto";
import { normName } from "../census-autopilot-v2/identity-dedupe.js";

/**
 * @param {object[]} independentRecords
 * @param {number} sampleSize
 */
export function freezeEnrichmentSample(independentRecords, sampleSize = 250) {
  // Stratify: branded / independent / soft / by country
  const branded = independentRecords.filter((r) => r.strata.branded);
  const independent = independentRecords.filter((r) => r.strata.independent);
  const soft = independentRecords.filter((r) => r.strata.soft_collection);

  const picked = [];
  const used = new Set();
  function take(list, n) {
    let a = 0;
    for (const r of list) {
      if (picked.length >= sampleSize) break;
      if (a >= n) break;
      if (used.has(r.property_identity_id)) continue;
      used.add(r.property_identity_id);
      picked.push(r);
      a += 1;
    }
  }

  take(branded, Math.floor(sampleSize * 0.55));
  take(independent, Math.floor(sampleSize * 0.3));
  take(soft, Math.floor(sampleSize * 0.1));
  take(independentRecords, sampleSize);

  const cohort = picked.slice(0, sampleSize).map((r, i) => ({
    wave_index: i,
    candidate_id: `ind_${r.property_identity_id}`,
    property_identity_id: r.property_identity_id,
    name: r.physical.current_name,
    country: r.physical.country,
    city: r.physical.city,
    brand: r.affiliation.current_brand,
    family: r.affiliation.brand_family,
    website: r.physical.official_url,
    property_ids: r.physical.official_property_id ? [r.physical.official_property_id] : [],
    candidate_origin: "INDEPENDENT_DISCOVERY",
    wave_priority: r.strata.branded ? "P2" : "P4",
    strata: {
      branded: r.strata.branded,
      independent: r.strata.independent,
      cvent_origin: false,
      existing_vic: r.discovery_evidence.source_type === "verified_independent_census_seed",
      soft_collection: r.strata.soft_collection,
      weak_identity: false,
      sibling_risk: false,
      region: r.physical.country,
      country: r.physical.country,
    },
  }));

  return {
    version: "golden-enrichment-sample-freeze-v2.3",
    sample_size: cohort.length,
    not_architecture_benchmark: true,
    purpose: "Does independent discovery feed cleanly into existing enrichment factory?",
    cohort,
  };
}

export async function runGoldenEnrichmentSample(repoRoot, sampleFreeze, opts = {}) {
  const ceiling = opts.ceiling ?? Number(process.env.CAV23_ENRICHMENT_CEILING || 350);
  const wave = await runProductionWave({
    repoRoot,
    cohort: sampleFreeze.cohort,
    ceiling,
    log: opts.log || console.log,
  });

  const confirmed = wave.results.filter(
    (r) =>
      String(r.confirmation || "").includes("INDEPENDENTLY") ||
      String(r.confirmation || "").includes("ENRICHMENT")
  );
  const pcts = confirmed.map((r) => r.final_priority_proxy_pct || 0);
  const pctsExcl = confirmed.map((r) => r.final_priority_proxy_excl_rooms_pct || 0);
  const avg = pcts.length ? Math.round(pcts.reduce((a, b) => a + b, 0) / pcts.length) : 0;
  const ge95 = confirmed.filter((r) => (r.final_priority_proxy_pct || 0) >= 95).length;
  const ge95Excl = confirmed.filter((r) => (r.final_priority_proxy_excl_rooms_pct || 0) >= 95).length;
  const roomsOk = confirmed.filter((r) => r.rooms_result?.ok).length;
  const fp = confirmed.filter(
    (r) => r.rooms_result?.classification === "FIRST-PARTY VALIDATION"
  ).length;

  return {
    version: "golden-enrichment-sample-results-v2.3",
    sample_size: sampleFreeze.cohort.length,
    processed: wave.results.length,
    independently_confirmed: confirmed.length,
    average_priority_completeness: avg,
    ge95,
    pct_ge95: confirmed.length ? Math.round((100 * ge95) / confirmed.length) : 0,
    pct_ge95_excl_rooms: confirmed.length ? Math.round((100 * ge95Excl) / confirmed.length) : 0,
    rooms_coverage_pct: confirmed.length ? Math.round((100 * roomsOk) / confirmed.length) : 0,
    first_party_validation_required: fp,
    serpapi_enrichment_calls: wave.actual_delta ?? wave.credit_ledger?.charged,
    enrichment_call_type: "SERPAPI_ENRICHMENT_CALL",
    runtime_ms: wave.runtime_ms,
    counters: wave.counters,
    feeds_cleanly: confirmed.length / Math.max(1, wave.results.length) >= 0.5,
  };
}
