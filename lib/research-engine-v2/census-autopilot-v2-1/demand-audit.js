/**
 * SerpApi demand audit + call minimization for Autopilot V2.1.
 */

import fs from "node:fs";
import path from "node:path";

const CALL_WHY = Object.freeze({
  A: "New hotel independent confirmation",
  B: "Existing hotel identity confirmation",
  C: "Address gap",
  D: "Coordinate gap",
  E: "Phone gap",
  F: "Website gap",
  G: "Amenities gap",
  H: "Property type/class input",
  I: "Contradiction/freshness validation",
  J: "Other",
});

/**
 * Load classified candidate rows from V2 candidate shards (lightweight fields only).
 * @param {string} v2Dir
 * @param {{ maxRows?: number }} [opts]
 */
export function loadV2CandidateSummaries(v2Dir, opts = {}) {
  const candDir = path.join(v2Dir, "candidates");
  if (!fs.existsSync(candDir)) return [];
  const files = fs.readdirSync(candDir).filter((f) => f.startsWith("candidates-") && f.endsWith(".json")).sort();
  const rows = [];
  for (const f of files) {
    const j = JSON.parse(fs.readFileSync(path.join(candDir, f), "utf8"));
    for (const c of j.candidates || []) {
      rows.push({
        candidate_id: c.candidate_id,
        candidate_origin: c.candidate_origin,
        origin_name: c.origin_name,
        origin_country: c.origin_country,
        origin_city: c.origin_city,
        origin_url: c.origin_url,
        origin_source_record_id: c.origin_source_record_id,
        brand: c.brand || null,
        family: c.family || null,
        website: c.website || null,
      });
      if (opts.maxRows && rows.length >= opts.maxRows) return rows;
    }
  }
  return rows;
}

/**
 * Re-classify SerpApi need with minimization (no API calls).
 * @param {object} dedupeSummary from V2
 * @param {object} brandCov
 * @param {object} v13 completeness map size
 */
export function auditSerpApiDemand(dedupeSummary, brandCov, opts = {}) {
  const oldForecast = opts.oldForecast ?? 14301;
  const newCand = dedupeSummary.new_property_candidates || 0;
  const existing = dedupeSummary.existing_verified || 0;
  const probableDup = dedupeSummary.probable_duplicates || 0;
  const conflicts = dedupeSummary.identity_conflicts || 0;
  const insufficient = dedupeSummary.insufficient_identity || 0;
  const unique = dedupeSummary.estimated_unique_physical_hotels || 0;

  // WHY breakdown of OLD forecast (approximate attribution)
  const whyOld = {
    A_new_confirmation: Math.round(newCand * 1.15),
    B_existing_identity: Math.round(existing * 0.05),
    C_address_gap: Math.round(existing * 0.15),
    D_coordinate_gap: Math.round(existing * 0.15),
    E_phone_gap: Math.round(existing * 0.2),
    F_website_gap: Math.round(existing * 0.05),
    G_amenities_gap: Math.round(existing * 0.1),
    H_property_type_class: Math.round(newCand * 0.05),
    I_freshness: Math.round(unique * 0.02),
    J_other: Math.round(oldForecast * 0.02),
  };

  // Avoidance levers
  const avoid = {
    skip_insufficient_identity: insufficient,
    skip_identity_conflict_until_review: conflicts,
    skip_probable_duplicate_second_call: probableDup, // 1 call per PID already counted in unique
    skip_existing_verified_rooms_only_gaps: Math.round(existing * 0.55), // Rooms not SerpApi
    official_first_native_strong: Math.round(
      (brandCov?.native_strong || []).reduce((s, f) => s + (f.count || 0), 0) * 0.35
    ),
    pid_dedupe_vs_per_candidate: Math.max(0, newCand - Math.max(0, unique - existing)),
    dealality_cache_rerun_savings_pct: 40,
    field_specific_not_blanket: Math.round(newCand * 0.1), // don't call for type/class alone
  };

  const unavoidableNew = Math.max(
    0,
    unique - existing - Math.round(avoid.official_first_native_strong * 0.5)
  );
  // 1.05 searches avg (mostly direct property hit; occasional details)
  const revisedConfirmation = Math.ceil(unavoidableNew * 1.05);
  const revisedExistingGaps = Math.ceil(existing * 0.25 * 1.1); // only SerpApi-allowed gaps
  const revisedForecast = revisedConfirmation + revisedExistingGaps;
  const saved = Math.max(0, oldForecast - revisedForecast);

  return {
    CALL_WHY,
    old_forecast: oldForecast,
    why_old_attribution_approx: whyOld,
    avoidance_levers: avoid,
    revised_forecast: {
      confirmation_searches: revisedConfirmation,
      existing_gap_searches: revisedExistingGaps,
      total: revisedForecast,
      notes: [
        "One SerpApi search per unique physical property needing independent confirmation",
        "Skip Rooms-only gaps (not SerpApi)",
        "Skip conflicts/insufficient until steward/identity repair",
        "Official-first for native-strong before SerpApi",
        "Dealality research cache prevents repeat pays on reconstruction",
      ],
    },
    searches_saved_vs_old: saved,
    savings_pct: Math.round((1000 * saved) / Math.max(1, oldForecast)) / 10,
    goal: "minimize paid calls while maximizing independent Census completion — not maximize quota use",
  };
}
