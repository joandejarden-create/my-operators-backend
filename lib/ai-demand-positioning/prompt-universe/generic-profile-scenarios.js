/**
 * Generic ADP scenario synthesis from structured property profile fields.
 * Used when no market-specific standard pack exists — no city/hotel hardcodes.
 *
 * Prompts are INTERNAL ONLY.
 */

import { TRAVELER_INTENTS, DECISION_FRAMES } from "./standard-scenarios.js";

function clean(s) {
  return String(s || "").trim();
}

function slug(s) {
  return clean(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 32);
}

/**
 * Build a portable scenario universe from profile geography + product signals.
 * @param {object} propertyProfile
 * @returns {Array<{scenarioId,intent,frame,query}>}
 */
export function generateGenericProfileScenarios(propertyProfile = {}) {
  const city = clean(propertyProfile.city) || clean(propertyProfile.market) || "the market";
  const market = clean(propertyProfile.market) || city;
  const submarket = clean(propertyProfile.submarket);
  const brand = clean(propertyProfile.brand) || "full-service";
  const affiliation = clean(propertyProfile.affiliation);
  const chainScale = clean(propertyProfile.chainScale) || "upscale";
  const place = submarket ? `${submarket}, ${city}` : city;
  const loyaltyHint = /marriott|bonvoy/i.test(`${affiliation} ${brand}`)
    ? "Marriott Bonvoy"
    : /hilton|honors/i.test(`${affiliation} ${brand}`)
      ? "Hilton Honors"
      : affiliation || brand;
  const pid = slug(propertyProfile.propertyId || "property");
  const scale = chainScale.toLowerCase();

  const rows = [
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best ${scale} hotel in ${place} for a business trip`,
    },
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a ${brand} hotel in ${city} for a corporate executive visit`,
    },
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where should I stay in ${city} for meetings near ${submarket || "the center"}?`,
    },
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best lifestyle ${scale} hotel in ${city} for a city break`,
    },
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a design-forward hotel in ${place} for international travelers`,
    },
    {
      intent: TRAVELER_INTENTS.COUPLES,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best romantic ${scale} hotel in ${city} for a couples weekend`,
    },
    {
      intent: TRAVELER_INTENTS.COUPLES,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where should couples stay in ${place} for dining and nightlife?`,
    },
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best hotel in ${city} for a small executive offsite of 20–30 people`,
    },
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a ${scale} hotel in ${place} with compact meeting studios`,
    },
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where to host a board dinner for 25 in ${city} with lifestyle positioning?`,
    },
    {
      intent: TRAVELER_INTENTS.CELEBRATION,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best hotel in ${city} for a private celebration or milestone stay`,
    },
    {
      intent: TRAVELER_INTENTS.FAMILY,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a full-service hotel in ${city} for a multigenerational city visit`,
    },
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.COMPARE,
      query: `Compare ${scale} lifestyle hotels in ${market} for a weekend trip`,
    },
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best ${loyaltyHint}-affiliated hotel in ${city} for points travelers`,
    },
    {
      intent: TRAVELER_INTENTS.WELLNESS,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a ${scale} hotel in ${place} with spa or wellness amenities`,
    },
    {
      intent: TRAVELER_INTENTS.ADVENTURE,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best hotel in ${city} as a base for exploring the destination`,
    },
    {
      intent: TRAVELER_INTENTS.CELEBRATION,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where to stay in ${city} for a stylish birthday weekend?`,
    },
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.COMPARE,
      query: `Compare boutique meeting hotels in ${market} for a 40-person company event`,
    },
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where should first-time visitors stay in ${place}?`,
    },
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `${brand} hotel in ${city} with strong Wi‑Fi and work-friendly rooms`,
    },
  ];

  return rows.map((r, i) => ({
    scenarioId: `gen_${pid}_${String(i + 1).padStart(2, "0")}`,
    intent: r.intent,
    frame: r.frame,
    query: r.query,
    sourceBasis: "generic_profile_synthesis",
  }));
}
