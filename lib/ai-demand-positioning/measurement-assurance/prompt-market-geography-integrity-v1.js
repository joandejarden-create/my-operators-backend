/**
 * PROMPT_MARKET_GEOGRAPHY_INTEGRITY + MARKET_TO_PROMPT_PROVENANCE
 */

export const PROMPT_MARKET_GEOGRAPHY_INTEGRITY = "PROMPT_MARKET_GEOGRAPHY_INTEGRITY";
export const MARKET_TO_PROMPT_PROVENANCE = "MARKET_TO_PROMPT_PROVENANCE";

export const GEOGRAPHY_STATUS = Object.freeze({
  PASS: "PASS",
  TOO_BROAD: "TOO_BROAD",
  TOO_NARROW: "TOO_NARROW",
  WRONG_MARKET: "WRONG_MARKET",
  AMBIGUOUS_REVIEW_REQUIRED: "AMBIGUOUS_REVIEW_REQUIRED",
  MARKET_GOVERNANCE_REVIEW_REQUIRED: "MARKET_GOVERNANCE_REVIEW_REQUIRED",
});

/** Broad tokens that are usually too coarse when a city/submarket exists. */
const STATE_OR_REGION_BROAD = Object.freeze([
  "florida",
  "south florida",
  "palm beach county",
  "new york state",
  "missouri",
  "united states",
  "usa",
  "caribbean",
]);

/**
 * Governed geography map from property profile (no invented names).
 */
export function buildGovernedGeographyMap(profile) {
  if (!profile?.propertyId) {
    return { status: GEOGRAPHY_STATUS.MARKET_GOVERNANCE_REVIEW_REQUIRED, defects: ["missing_profile"] };
  }
  const city = String(profile.city || "").trim();
  const market = String(profile.market || "").trim();
  const submarket = String(profile.submarket || "").trim();
  const state = String(profile.state || "").trim();
  const country = String(profile.country || "").trim();

  const defects = [];
  if (!city && !market && !submarket) defects.push("missing_city_market_submarket");

  // Preferred hotel-selection labels derived only from profile fields
  const preferredHotelMarket =
    city ||
    (submarket.includes("/") ? submarket.split("/")[0].trim() : submarket) ||
    market ||
    null;

  return {
    propertyId: profile.propertyId,
    name: profile.name,
    city: city || null,
    market: market || null,
    submarket: submarket || null,
    state: state || null,
    country: country || null,
    preferredHotelMarket,
    broaderDestination: market && market !== city ? market : state || country || null,
    hierarchy: [
      "GOVERNED_SUBMARKET_NEIGHBORHOOD",
      "GOVERNED_HOTEL_MARKET_CITY",
      "BROADER_DESTINATION_ONLY_WHEN_DECISION_IS_BROADER",
    ],
    status: defects.length ? GEOGRAPHY_STATUS.MARKET_GOVERNANCE_REVIEW_REQUIRED : "OK",
    defects,
  };
}

import { tokenPresent } from "./prompt-bias-detection-v1.js";

function includesToken(haystack, needle) {
  return tokenPresent(haystack, needle);
}

/**
 * Scenario-level recommended geography phrase for prompts (from profile only).
 */
export function recommendScenarioGeography(profile, { originalPrompt = "", intent = null } = {}) {
  const map = buildGovernedGeographyMap(profile);
  if (map.status === GEOGRAPHY_STATUS.MARKET_GOVERNANCE_REVIEW_REQUIRED) return map;

  const original = String(originalPrompt || "");
  const sub = map.submarket || "";
  const city = map.city || "";
  const market = map.market || "";

  // If original already uses a specific neighborhood present in submarket, prefer that
  const subParts = sub.split("/").map((s) => s.trim()).filter(Boolean);
  for (const part of subParts) {
    if (includesToken(original, part)) {
      return {
        ...map,
        recommendedPromptGeography: part,
        reason: "preserve_original_submarket_token_present_in_profile",
      };
    }
  }

  // Property-specific known alignments from profile tokens only
  if (profile.propertyId === "adp_renaissance_times_square") {
    if (includesToken(original, "times square")) {
      return { ...map, recommendedPromptGeography: "Times Square", reason: "original_and_submarket" };
    }
    if (includesToken(original, "midtown")) {
      return { ...map, recommendedPromptGeography: "Midtown Manhattan", reason: "original_and_submarket" };
    }
    if (includesToken(original, "new york city") || includesToken(original, "nyc")) {
      // Original used city-scale — preserve unless defective; Times Square is preferred hotel market
      return {
        ...map,
        recommendedPromptGeography: "Times Square",
        reason: "tighten_defective_or_overbroad_nyc_to_governed_submarket_times_square",
        note: "Original said New York City/NYC; governed hotel-selection geography for this property is Times Square / Midtown",
      };
    }
    return { ...map, recommendedPromptGeography: "Times Square", reason: "default_submarket_head" };
  }

  if (profile.propertyId === "adp_now_now_noho") {
    if (includesToken(original, "downtown manhattan") || includesToken(original, "downtown nyc") || includesToken(original, "downtown new york")) {
      return { ...map, recommendedPromptGeography: "downtown Manhattan", reason: "preserve_downtown_manhattan" };
    }
    if (includesToken(original, "noho")) {
      return { ...map, recommendedPromptGeography: "NoHo", reason: "preserve_noho" };
    }
    return { ...map, recommendedPromptGeography: "downtown Manhattan", reason: "default_lower_manhattan_decision_frame" };
  }

  if (profile.propertyId === "adp_waterstone_boca_raton") {
    if (includesToken(original, "florida") && !includesToken(original, "boca")) {
      return {
        ...map,
        recommendedPromptGeography: "Boca Raton",
        reason: "state_too_broad_use_city",
        originalGeographyDefective: true,
      };
    }
    return { ...map, recommendedPromptGeography: "Boca Raton", reason: "governed_city_hotel_market" };
  }

  if (profile.propertyId === "adp_hotel_phillips_kansas_city") {
    if (includesToken(original, "downtown")) {
      return { ...map, recommendedPromptGeography: "downtown Kansas City", reason: "preserve_downtown" };
    }
    // Bare Kansas City when submarket is Downtown — tighten for hotel selection
    if (includesToken(original, "kansas city") && !includesToken(original, "downtown")) {
      return {
        ...map,
        recommendedPromptGeography: "downtown Kansas City",
        reason: "city_without_downtown_tighten_to_submarket",
        originalGeographyDefective: false,
        hotelSelectionPreferSubmarket: true,
      };
    }
    return { ...map, recommendedPromptGeography: "downtown Kansas City", reason: "default_downtown_submarket" };
  }

  if (profile.propertyId === "adp_cambridge_beaches_bermuda") {
    // Market is Bermuda; submarket West End / Somerset — destination-level Bermuda is often correct for resort search
    if (includesToken(original, "bermuda")) {
      return { ...map, recommendedPromptGeography: "Bermuda", reason: "governed_destination_market" };
    }
    return { ...map, recommendedPromptGeography: "Bermuda", reason: "default_destination" };
  }

  return {
    ...map,
    recommendedPromptGeography: map.preferredHotelMarket,
    reason: "fallback_preferred_hotel_market",
  };
}

/**
 * Evaluate whether prompt geography is appropriate for the property.
 * @param {'rewrite'|'reuse'} [args.mode='rewrite'] — reuse preserves legitimate original market frames
 */
export function checkPromptMarketGeographyIntegrity({
  profile,
  exactPrompt,
  originalPrompt = "",
  mode = "rewrite",
}) {
  const map = buildGovernedGeographyMap(profile);
  if (map.status === GEOGRAPHY_STATUS.MARKET_GOVERNANCE_REVIEW_REQUIRED) {
    return {
      gate: PROMPT_MARKET_GEOGRAPHY_INTEGRITY,
      status: GEOGRAPHY_STATUS.MARKET_GOVERNANCE_REVIEW_REQUIRED,
      pass: false,
      map,
      defects: map.defects,
    };
  }

  const prompt = String(exactPrompt || "");
  const original = String(originalPrompt || "");
  const defects = [];
  let status = GEOGRAPHY_STATUS.PASS;

  const rec = recommendScenarioGeography(profile, { originalPrompt: original || prompt });

  // State/region without city when city exists — but allow governed market label (e.g. South Florida)
  for (const broad of STATE_OR_REGION_BROAD) {
    if (!includesToken(prompt, broad)) continue;
    if (map.city && includesToken(prompt, map.city)) continue;
    if (map.market && includesToken(prompt, map.market)) continue;
    if (broad === "florida" && tokenPresent(prompt, "south florida")) continue;
    if (broad === "florida" && map.city) {
      status = GEOGRAPHY_STATUS.TOO_BROAD;
      defects.push(`broad_token:florida_without_city:${map.city}`);
    }
  }

  // Florida alone for Waterstone (not "South Florida" market label)
  if (profile.propertyId === "adp_waterstone_boca_raton") {
    if (
      tokenPresent(prompt, "florida") &&
      !tokenPresent(prompt, "boca raton") &&
      !tokenPresent(prompt, "south florida") &&
      !tokenPresent(prompt, "palm beach")
    ) {
      status = GEOGRAPHY_STATUS.TOO_BROAD;
      defects.push("florida_without_boca_raton");
    }
  }

  // Phillips: for NEW rewrites, prefer downtown submarket; for reuse, city market label is governed
  if (profile.propertyId === "adp_hotel_phillips_kansas_city" && mode === "rewrite") {
    if (
      tokenPresent(prompt, "kansas city") &&
      !tokenPresent(prompt, "downtown") &&
      !tokenPresent(prompt, "power")
    ) {
      status = GEOGRAPHY_STATUS.TOO_BROAD;
      defects.push("kansas_city_without_downtown_submarket");
    }
  }

  // Wrong market cross-contamination
  if (profile.propertyId === "adp_now_now_noho" || profile.propertyId === "adp_renaissance_times_square") {
    if (tokenPresent(prompt, "boca") || tokenPresent(prompt, "kansas city") || tokenPresent(prompt, "bermuda")) {
      status = GEOGRAPHY_STATUS.WRONG_MARKET;
      defects.push("geo_token_from_other_property_market");
    }
  }

  const pass = status === GEOGRAPHY_STATUS.PASS && defects.length === 0;

  return {
    gate: PROMPT_MARKET_GEOGRAPHY_INTEGRITY,
    status: pass ? GEOGRAPHY_STATUS.PASS : status,
    pass,
    defects,
    map,
    recommendedPromptGeography: rec.recommendedPromptGeography,
    recommendationReason: rec.reason,
    mode,
    provenance: {
      gate: MARKET_TO_PROMPT_PROVENANCE,
      city: map.city,
      market: map.market,
      submarket: map.submarket,
      recommendedPromptGeography: rec.recommendedPromptGeography,
      reason: rec.reason,
    },
  };
}
