/**
 * Market archetype assignment — hotel/profile based, not hotel-name hardcoding in router.
 */

import { MARKET_ARCHETYPE } from "./constants.js";
import { JEV_NEXT_ACTION } from "../evidence-gap/evidence-gap-model-v1.js";

/** Soft preferred action sequences by archetype (research direction bias only). */
export const ARCHETYPE_PREFERRED_ACTIONS = Object.freeze({
  [MARKET_ARCHETYPE.URBAN_ASSOCIATION]: [
    JEV_NEXT_ACTION.VERIFY_HOUSING_STATUS,
    JEV_NEXT_ACTION.FIND_OFFICIAL_HOUSING_PAGE,
    JEV_NEXT_ACTION.FIND_REGISTRATION_PAGE,
    JEV_NEXT_ACTION.VERIFY_FUTURE_CYCLE,
    JEV_NEXT_ACTION.VERIFY_MARKET,
    JEV_NEXT_ACTION.FIND_EVENT_MANUAL,
  ],
  [MARKET_ARCHETYPE.URBAN_CORPORATE]: [
    JEV_NEXT_ACTION.FIND_OFFICIAL_EVENT_PAGE,
    JEV_NEXT_ACTION.VERIFY_HOUSING_STATUS,
    JEV_NEXT_ACTION.FIND_REGISTRATION_PAGE,
    JEV_NEXT_ACTION.VERIFY_ORGANIZER_CONTROL,
    JEV_NEXT_ACTION.VERIFY_FUTURE_CYCLE,
  ],
  [MARKET_ARCHETYPE.RESORT_ISLAND]: [
    JEV_NEXT_ACTION.VERIFY_MARKET,
    JEV_NEXT_ACTION.VERIFY_FUTURE_CYCLE,
    JEV_NEXT_ACTION.FIND_OFFICIAL_EVENT_PAGE,
    JEV_NEXT_ACTION.FIND_TRAVEL_ACCOMMODATION_PAGE,
    JEV_NEXT_ACTION.VERIFY_ORGANIZER_CONTROL,
    JEV_NEXT_ACTION.VERIFY_HOTEL_SELECTION_STATUS,
  ],
  [MARKET_ARCHETYPE.LUXURY_URBAN]: [
    JEV_NEXT_ACTION.VERIFY_HOTEL_SELECTION_STATUS,
    JEV_NEXT_ACTION.FIND_OFFICIAL_HOUSING_PAGE,
    JEV_NEXT_ACTION.VERIFY_OVERFLOW,
    JEV_NEXT_ACTION.VERIFY_MARKET,
  ],
  [MARKET_ARCHETYPE.SPORTS_EVENT]: [
    JEV_NEXT_ACTION.FIND_TEAM_MANUAL,
    JEV_NEXT_ACTION.FIND_TRAVEL_ACCOMMODATION_PAGE,
    JEV_NEXT_ACTION.VERIFY_HOUSING_STATUS,
    JEV_NEXT_ACTION.VERIFY_FUTURE_CYCLE,
  ],
  [MARKET_ARCHETYPE.MIXED_DESTINATION]: [
    JEV_NEXT_ACTION.VERIFY_MARKET,
    JEV_NEXT_ACTION.FIND_OFFICIAL_EVENT_PAGE,
    JEV_NEXT_ACTION.VERIFY_HOUSING_STATUS,
  ],
  [MARKET_ARCHETYPE.OTHER]: [
    JEV_NEXT_ACTION.VERIFY_MARKET,
    JEV_NEXT_ACTION.FIND_OFFICIAL_EVENT_PAGE,
    JEV_NEXT_ACTION.VERIFY_HOUSING_STATUS,
  ],
});

/**
 * Assign market archetypes from hotel profile signals (not hard-coded names in router).
 * @returns {{ primary, secondary, weights, confidence, preferredActions }}
 */
export function assignMarketArchetypes(profile = {}) {
  const hotelType = String(profile.hotelType || profile.type || "").toLowerCase();
  const market = String(profile.market || profile.city || profile.destination || "").toLowerCase();
  const region = String(profile.region || profile.country || "").toLowerCase();
  const positioning = String(profile.positioning || profile.segment || "").toLowerCase();
  const tags = [
    hotelType,
    market,
    region,
    positioning,
    String(profile.notes || "").toLowerCase(),
    ...(Array.isArray(profile.tags) ? profile.tags.map((t) => String(t).toLowerCase()) : []),
  ].join(" ");

  const weights = {};
  const bump = (a, w) => {
    weights[a] = (weights[a] || 0) + w;
  };

  if (/resort|island|beach|caribbean|grenada|grand anse|maldives|bali/i.test(tags)) {
    bump(MARKET_ARCHETYPE.RESORT_ISLAND, 0.7);
  }
  if (/association|congress|congreso|universidad|faculty|medical|academic/i.test(tags)) {
    bump(MARKET_ARCHETYPE.URBAN_ASSOCIATION, 0.55);
  }
  if (/corporate|business|urban|city|coru[nñ]a|galicia|bethesda|arlington/i.test(tags)) {
    bump(MARKET_ARCHETYPE.URBAN_CORPORATE, 0.45);
  }
  if (/luxury|five.?star|upscale|collection/i.test(tags)) {
    bump(MARKET_ARCHETYPE.LUXURY_URBAN, 0.35);
  }
  if (/sport|tournament|cup|championship|regatta/i.test(tags)) {
    bump(MARKET_ARCHETYPE.SPORTS_EVENT, 0.4);
  }

  // Known HPC profiles (identity anchors — assignment only; router uses archetype enums)
  if (profile.hpc === "rec2PVBDavppGpenm" || profile.hotelShort === "AC") {
    bump(MARKET_ARCHETYPE.URBAN_ASSOCIATION, 0.5);
    bump(MARKET_ARCHETYPE.URBAN_CORPORATE, 0.35);
  }
  if (profile.hpc === "recKRJjcPnb4tVDDS" || profile.hotelShort === "SPICE") {
    bump(MARKET_ARCHETYPE.RESORT_ISLAND, 0.85);
  }
  if (profile.hpc === "recLuxvwwxID7U2B8" || profile.hotelShort === "BETHESDA") {
    bump(MARKET_ARCHETYPE.URBAN_ASSOCIATION, 0.5);
    bump(MARKET_ARCHETYPE.URBAN_CORPORATE, 0.4);
  }
  if (profile.hpc === "recG66DQJKP2c0UNh" || profile.hotelShort === "RENAISSANCE") {
    bump(MARKET_ARCHETYPE.URBAN_ASSOCIATION, 0.45);
    bump(MARKET_ARCHETYPE.URBAN_CORPORATE, 0.4);
  }
  if (profile.hpc === "recgMYovrrZDJMqzX" || profile.hotelShort === "WATERSTONE") {
    bump(MARKET_ARCHETYPE.URBAN_ASSOCIATION, 0.4);
    bump(MARKET_ARCHETYPE.LUXURY_URBAN, 0.35);
  }

  const ranked = Object.entries(weights).sort((a, b) => b[1] - a[1]);
  if (!ranked.length) {
    return {
      primary: MARKET_ARCHETYPE.OTHER,
      secondary: null,
      weights: { [MARKET_ARCHETYPE.OTHER]: 1 },
      confidence: "LOW",
      preferredActions: ARCHETYPE_PREFERRED_ACTIONS[MARKET_ARCHETYPE.OTHER],
    };
  }
  const primary = ranked[0][0];
  const secondary = ranked[1] && ranked[1][1] >= 0.3 ? ranked[1][0] : null;
  const confidence = ranked[0][1] >= 0.7 ? "HIGH" : ranked[0][1] >= 0.45 ? "MEDIUM" : "LOW";
  const preferred = [
    ...(ARCHETYPE_PREFERRED_ACTIONS[primary] || []),
    ...(secondary ? ARCHETYPE_PREFERRED_ACTIONS[secondary] || [] : []),
  ];
  const preferredActions = [...new Set(preferred)];

  return { primary, secondary, weights, confidence, preferredActions };
}

/**
 * Bias default action using archetype preferred list without inventing free-form actions.
 */
export function archetypeBiasedDefaultAction(primaryBlocker, archetypePrimary, fallbackAction) {
  const prefs = ARCHETYPE_PREFERRED_ACTIONS[archetypePrimary] || [];
  if (!prefs.length) return fallbackAction;
  // If fallback already in prefs, keep it; else pick first pref that maps to blocker family
  if (prefs.includes(fallbackAction)) return fallbackAction;
  const blockerHint = String(primaryBlocker || "");
  if (/MARKET/i.test(blockerHint)) {
    const m = prefs.find((a) => /MARKET|EVENT_PAGE/i.test(a));
    if (m) return m;
  }
  if (/HOUSING|COMMERCIAL|LODGING/i.test(blockerHint)) {
    const m = prefs.find((a) => /HOUSING|TRAVEL_ACCOMMODATION|HOTEL_SELECTION/i.test(a));
    if (m) return m;
  }
  if (/FUTURE|DATE/i.test(blockerHint)) {
    const m = prefs.find((a) => /FUTURE|DATES|EVENT_PAGE/i.test(a));
    if (m) return m;
  }
  return prefs[0] || fallbackAction;
}
