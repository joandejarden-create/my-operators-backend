/**
 * SerpApi expected-value scoring + one-call optimization.
 */

import { SERPAPI_EV_THRESHOLD } from "./constants.js";

/**
 * @param {object} hotel
 * @param {object} [official]
 * @param {string[]} [officialGaps]
 */
export function scoreSerpApiExpectedValue(hotel, official = null, officialGaps = null) {
  const gaps = officialGaps || [];
  let score = 0;
  const reasons = [];

  const identityUncertainty = hotel.strata?.weak_identity
    ? 30
    : hotel.strata?.existing_vic
      ? 5
      : hotel.strata?.branded
        ? 15
        : 25;
  score += identityUncertainty;
  reasons.push(`identity_uncertainty_${identityUncertainty}`);

  const missing = gaps.length || (hotel.strata?.existing_vic ? 2 : 5);
  const missingScore = Math.min(35, missing * 7);
  score += missingScore;
  reasons.push(`missing_fields_${missing}`);

  if (hotel.priority === "P0" || hotel.wave_priority === "P0") {
    score += 20;
    reasons.push("priority_P0");
  } else if (hotel.priority === "P1" || hotel.wave_priority === "P1") {
    score += 15;
    reasons.push("priority_P1");
  } else if (hotel.priority === "P2" || hotel.wave_priority === "P2") {
    score += 12;
    reasons.push("priority_P2");
  } else if (hotel.priority === "P3" || hotel.wave_priority === "P3") {
    score += 10;
    reasons.push("priority_P3");
  } else if (hotel.priority === "P5" || hotel.wave_priority === "P5") {
    score -= 5;
    reasons.push("priority_P5_penalty");
  }

  if (official?.fields_resolved?.length >= 5) {
    score -= 25;
    reasons.push("official_already_rich");
  } else if (official?.fields_resolved?.length >= 3) {
    score -= 12;
    reasons.push("official_partial");
  }

  // Phone-only gap should not consume multiple paid calls
  if (gaps.length === 1 && gaps[0] === "phone") {
    score -= 40;
    reasons.push("phone_only_skip");
  }

  // Cvent challenge with no official URL — high EV for one confirmation call
  if (hotel.strata?.cvent_origin && !hotel.website && gaps.includes("identity")) {
    score += 15;
    reasons.push("cvent_challenge_confirmation");
  }

  const expectedGain = Math.min(20, (gaps.length || 3) * 4);
  score += expectedGain;
  reasons.push(`expected_gain_${expectedGain}`);

  // Call cost constant
  score -= 5;
  reasons.push("call_cost");

  const clamped = Math.max(0, Math.min(100, score));
  return {
    expected_value_score: clamped,
    threshold: SERPAPI_EV_THRESHOLD,
    eligible: clamped > SERPAPI_EV_THRESHOLD,
    reasons,
    gaps,
  };
}

/**
 * Determine if search response already completes allowed fields (one-call).
 */
export function analyzeOneCallCompleteness(candidate, requiredGaps = []) {
  if (!candidate) {
    return {
      one_call_complete: false,
      second_call_required: false,
      second_call_reason: "no_candidate",
      fields_present: [],
    };
  }
  const present = [];
  if (candidate.name) present.push("identity");
  if (candidate.address) present.push("address");
  if (candidate.latitude != null && candidate.longitude != null) present.push("coordinates");
  if (candidate.phone) present.push("phone");
  if (candidate.website || candidate.google_property_url) present.push("website");
  if (candidate.amenities_raw?.length || Object.keys(candidate.amenities_dealality || {}).length) {
    present.push("amenities");
  }

  const need = requiredGaps.length
    ? requiredGaps
    : ["identity", "address", "coordinates", "phone", "website", "amenities"];
  const missing = need.filter((g) => !present.includes(g === "identity" ? "identity" : g));

  const isDirect = candidate.source_shape === "search_direct_property";
  const one_call_complete = missing.length === 0 || (isDirect && missing.filter((m) => m !== "amenities").length === 0);

  // Second call only if property_token exists, not already direct, and material identity/address/coords still missing
  const materialMissing = missing.filter((m) => ["identity", "address", "coordinates"].includes(m));
  const second_call_required =
    !one_call_complete &&
    Boolean(candidate.property_token) &&
    !isDirect &&
    materialMissing.length > 0;

  return {
    one_call_complete,
    second_call_required,
    second_call_reason: second_call_required
      ? `material_gaps:${materialMissing.join(",")}`
      : one_call_complete
        ? "not_needed"
        : "insufficient_but_no_token_or_already_direct",
    fields_present: present,
    fields_missing: missing,
    source_shape: candidate.source_shape || null,
  };
}

/**
 * Reforecast full-universe SerpApi demand under official-first + one-call + EV gate.
 */
export function reforecastSerpApiDemand(opts) {
  const {
    uniqueHotels = 12846,
    existingVerified = 1194,
    nativeStrongCount = 471 + 356 + 192,
    partialCount = 754,
    priorMinimized = 12400,
    priorFull = 14301,
    officialOnlyRate = 0.12,
    oneCallRate = 0.72,
    evSkipRate = 0.18,
  } = opts;

  const newish = Math.max(0, uniqueHotels - existingVerified);
  // Official-first absorbs native-strong share
  const officialAbsorb = Math.round(nativeStrongCount * 0.55 + partialCount * 0.15);
  const serpapiCandidates = Math.max(0, newish - officialAbsorb);
  // EV gate skips low-value
  const afterEv = Math.round(serpapiCandidates * (1 - evSkipRate));
  // Mostly one call; remainder need ~1.15 avg
  const searches = Math.ceil(afterEv * (oneCallRate * 1.0 + (1 - oneCallRate) * 1.15));
  // Existing gap enrichment (non-rooms)
  const existingGap = Math.round(existingVerified * 0.25 * 0.6);

  const total = searches + existingGap;
  return {
    prior_full_universe_forecast: priorFull,
    prior_minimized_v21: priorMinimized,
    new_forecast: total,
    reduction_vs_prior_full_pct: Math.round((100 * (priorFull - total)) / priorFull),
    reduction_vs_prior_minimized_pct: Math.round((100 * (priorMinimized - total)) / priorMinimized),
    components: {
      new_confirmation_after_official_and_ev: searches,
      existing_non_rooms_gaps: existingGap,
      official_absorb_estimate: officialAbsorb,
      serpapi_candidates_before_ev: serpapiCandidates,
    },
    assumptions: {
      officialOnlyRate,
      oneCallRate,
      evSkipRate,
      nativeStrongCount,
      partialCount,
    },
    note: "Research/staging only until SerpApi production persistence rights clarified.",
  };
}
