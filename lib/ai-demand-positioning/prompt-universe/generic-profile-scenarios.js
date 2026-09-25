/**
 * Generic ADP scenario synthesis from structured property profile fields.
 * Used when no market-specific standard pack exists — no city/hotel hardcodes
 * beyond values already present on the property profile.
 *
 * V2 expands the thin 20-row onboarding fallback to methodological parity with
 * certified market packs (~48 destination/archetype + ~15 property-capability).
 *
 * Layer model:
 *   CORE UNIVERSAL + ARCHETYPE + MARKET/DESTINATION  → generateGenericProfileScenarios
 *   PROPERTY CAPABILITY                               → generateGenericPropertyCapabilityScenarios
 *
 * Prompts are INTERNAL ONLY.
 */

import { TRAVELER_INTENTS, DECISION_FRAMES } from "./standard-scenarios.js";

export const GENERIC_PROFILE_SCENARIOS_VERSION = "adp_generic_profile_scenarios_v2";

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

function loyaltyPhrase(propertyProfile) {
  const brand = clean(propertyProfile.brand);
  const affiliation = clean(propertyProfile.affiliation);
  const blob = `${affiliation} ${brand}`;
  if (/marriott|bonvoy/i.test(blob)) return "Marriott Bonvoy";
  if (/hilton|honors/i.test(blob)) return "Hilton Honors";
  if (/ihg|one rewards/i.test(blob)) return "IHG One Rewards";
  if (/hyatt/i.test(blob)) return "World of Hyatt";
  return affiliation || brand || "loyalty program";
}

function placeParts(propertyProfile) {
  const city = clean(propertyProfile.city) || clean(propertyProfile.market) || "the market";
  const market = clean(propertyProfile.market) || city;
  const submarket = clean(propertyProfile.submarket);
  const place = submarket ? `${submarket}, ${city}` : city;
  const brand = clean(propertyProfile.brand) || "full-service";
  const chainScale = clean(propertyProfile.chainScale) || "upscale";
  const scale = chainScale.toLowerCase();
  const lifestyle =
    /lifestyle|design|w hotels|edition|autograph|curio|tribute/i.test(
      `${brand} ${clean(propertyProfile.affiliation)} ${(propertyProfile.attributes || []).join(" ")}`
    ) || /luxury/i.test(scale);
  return { city, market, submarket, place, brand, chainScale, scale, lifestyle };
}

/**
 * Destination + archetype pack (~48) — methodological stand-in for a certified
 * market standard pack when none is registered for the property's market.
 * First 20 rows preserve v1 queries/IDs for continuity with already-run periods.
 *
 * @param {object} propertyProfile
 * @returns {Array<{scenarioId,intent,frame,query,layer,sourceBasis}>}
 */
export function generateGenericProfileScenarios(propertyProfile = {}) {
  const { city, market, submarket, place, brand, scale, lifestyle } = placeParts(propertyProfile);
  const loyaltyHint = loyaltyPhrase(propertyProfile);
  const pid = slug(propertyProfile.propertyId || "property");
  const style = lifestyle ? "lifestyle" : "full-service";

  /** @type {Array<{intent:string,frame:string,query:string,layer:string}>} */
  const rows = [
    // ——— V1 rows 01–20 (LOCKED identity for existing monitoring periods) ———
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best ${scale} hotel in ${place} for a business trip`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a ${brand} hotel in ${city} for a corporate executive visit`,
      layer: "archetype",
    },
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where should I stay in ${city} for meetings near ${submarket || "the center"}?`,
      layer: "market",
    },
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best lifestyle ${scale} hotel in ${city} for a city break`,
      layer: "archetype",
    },
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a design-forward hotel in ${place} for international travelers`,
      layer: "archetype",
    },
    {
      intent: TRAVELER_INTENTS.COUPLES,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best romantic ${scale} hotel in ${city} for a couples weekend`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.COUPLES,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where should couples stay in ${place} for dining and nightlife?`,
      layer: "market",
    },
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best hotel in ${city} for a small executive offsite of 20–30 people`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a ${scale} hotel in ${place} with compact meeting studios`,
      layer: "archetype",
    },
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where to host a board dinner for 25 in ${city} with lifestyle positioning?`,
      layer: "archetype",
    },
    {
      intent: TRAVELER_INTENTS.CELEBRATION,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best hotel in ${city} for a private celebration or milestone stay`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.FAMILY,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a full-service hotel in ${city} for a multigenerational city visit`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.COMPARE,
      query: `Compare ${scale} lifestyle hotels in ${market} for a weekend trip`,
      layer: "market",
    },
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best ${loyaltyHint}-affiliated hotel in ${city} for points travelers`,
      layer: "archetype",
    },
    {
      intent: TRAVELER_INTENTS.WELLNESS,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a ${scale} hotel in ${place} with spa or wellness amenities`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.ADVENTURE,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best hotel in ${city} as a base for exploring the destination`,
      layer: "market",
    },
    {
      intent: TRAVELER_INTENTS.CELEBRATION,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where to stay in ${city} for a stylish birthday weekend?`,
      layer: "archetype",
    },
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.COMPARE,
      query: `Compare boutique meeting hotels in ${market} for a 40-person company event`,
      layer: "market",
    },
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where should first-time visitors stay in ${place}?`,
      layer: "market",
    },
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `${brand} hotel in ${city} with strong Wi‑Fi and work-friendly rooms`,
      layer: "archetype",
    },

    // ——— V2 expansion to certified-pack density (~48 total) ———
    // BUSINESS
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.COMPARE,
      query: `Compare ${scale} hotels in ${market} for a week of client meetings`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best ${style} hotel in ${city} for a senior executive overnight`,
      layer: "archetype",
    },
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where should I stay in ${market} if my meetings span the city center and ${submarket || "main business districts"}?`,
      layer: "market",
    },
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a ${scale} hotel in ${city} with reliable late checkout for business travelers`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best hotel in ${place} for international business travelers who want ${style} positioning`,
      layer: "archetype",
    },
    // LEISURE
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best ${scale} hotel in ${market} for a long weekend city break`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a ${style} hotel in ${city} near dining and cultural attractions`,
      layer: "market",
    },
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where to stay in ${city} for a design-focused leisure trip?`,
      layer: "archetype",
    },
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best ${loyaltyHint} hotel in ${city} for a leisure points redemption`,
      layer: "archetype",
    },
    // COUPLES
    {
      intent: TRAVELER_INTENTS.COUPLES,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a romantic ${scale} hotel in ${place} for an anniversary`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.COUPLES,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best ${style} hotel in ${city} for a couples celebration weekend`,
      layer: "archetype",
    },
    {
      intent: TRAVELER_INTENTS.COUPLES,
      frame: DECISION_FRAMES.COMPARE,
      query: `Compare romantic ${scale} hotels in ${market} for a two-night stay`,
      layer: "market",
    },
    {
      intent: TRAVELER_INTENTS.COUPLES,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where should couples stay in ${city} for nightlife and fine dining?`,
      layer: "market",
    },
    {
      intent: TRAVELER_INTENTS.COUPLES,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `${brand}-style hotel in ${city} for a proposal or anniversary overnight`,
      layer: "archetype",
    },
    // GROUP / MEETING
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best hotel in ${place} for a 50-person corporate meeting`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a ${scale} hotel in ${city} with private dining for a team offsite`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where to host a leadership retreat overnight in ${market}?`,
      layer: "market",
    },
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best ${style} hotel in ${city} for a compact board meeting and dinner`,
      layer: "archetype",
    },
    // FAMILY
    {
      intent: TRAVELER_INTENTS.FAMILY,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best full-service hotel in ${city} for a family city visit`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.FAMILY,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where should a family stay in ${place} with easy access to major attractions?`,
      layer: "market",
    },
    {
      intent: TRAVELER_INTENTS.FAMILY,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a ${scale} hotel in ${city} suitable for teens and parents`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.FAMILY,
      frame: DECISION_FRAMES.COMPARE,
      query: `Compare family-friendly ${scale} hotels in ${market}`,
      layer: "market",
    },
    // CELEBRATION
    {
      intent: TRAVELER_INTENTS.CELEBRATION,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a ${scale} hotel in ${city} for a milestone birthday celebration`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.CELEBRATION,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best ${style} hotel in ${place} for a private celebration dinner and overnight`,
      layer: "archetype",
    },
    {
      intent: TRAVELER_INTENTS.CELEBRATION,
      frame: DECISION_FRAMES.COMPARE,
      query: `Compare celebration hotels in ${market} for a stylish private event`,
      layer: "market",
    },
    // WELLNESS
    {
      intent: TRAVELER_INTENTS.WELLNESS,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best ${scale} hotel in ${city} for a spa-focused overnight`,
      layer: "core",
    },
    {
      intent: TRAVELER_INTENTS.WELLNESS,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where to stay in ${place} for fitness and wellness amenities?`,
      layer: "market",
    },
    // ADVENTURE / EXPERIENCES
    {
      intent: TRAVELER_INTENTS.ADVENTURE,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Recommend a hotel in ${city} as a base for destination experiences and nightlife`,
      layer: "market",
    },
  ];

  return rows.map((r, i) => ({
    scenarioId: `gen_${pid}_${String(i + 1).padStart(2, "0")}`,
    intent: r.intent,
    frame: r.frame,
    query: r.query,
    layer: r.layer,
    sourceBasis: GENERIC_PROFILE_SCENARIOS_VERSION,
  }));
}

/**
 * Property-capability scenarios (~15) synthesized from verified profile facts only.
 * Uses profile name/brand/geo/meeting inventory/attributes — no frozen hotel pack.
 *
 * @param {object} propertyProfile
 * @returns {Array<{scenarioId,intent,frame,query,layer,sourceBasis}>}
 */
export function generateGenericPropertyCapabilityScenarios(propertyProfile = {}) {
  const { city, market, place, brand, scale, lifestyle } = placeParts(propertyProfile);
  const loyaltyHint = loyaltyPhrase(propertyProfile);
  const pid = slug(propertyProfile.propertyId || "property");
  const name = clean(propertyProfile.name) || brand;
  const attrs = new Set((propertyProfile.attributes || []).map((a) => String(a)));
  const meetings = propertyProfile.meetingSpace || {};
  const meetingRooms = Number(meetings.meetingRooms || 0);
  const meetingSqM = Number(meetings.totalSqM || meetings.totalSqFt || 0);
  const hasMeetings = meetingRooms > 0 || meetingSqM > 0;
  const style = lifestyle ? "lifestyle" : "full-service";

  /** @type {Array<{intent:string,frame:string,query:string}>} */
  const rows = [
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `${name} in ${city} for a business overnight`,
    },
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `${brand} hotel in ${city} with ${loyaltyHint} for points travelers`,
    },
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where to stay at ${name} for a ${style} city break in ${place}?`,
    },
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Best ${brand} ${scale} hotel experience in ${market}`,
    },
    {
      intent: TRAVELER_INTENTS.COUPLES,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Romantic stay at ${name} in ${city}`,
    },
    {
      intent: TRAVELER_INTENTS.CELEBRATION,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `${name} for a private celebration or milestone overnight in ${city}`,
    },
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.RECOMMEND,
      query: hasMeetings
        ? `${name} meeting studios in ${city} for a compact executive offsite`
        : `${name} in ${city} for a small team overnight and dinner`,
    },
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.BEST_FOR,
      query: hasMeetings
        ? `Hotel with compact meeting capacity near ${place} for 20–40 guests`
        : `Best ${scale} hotel in ${place} for a small private dinner and overnight`,
    },
    {
      intent: TRAVELER_INTENTS.LEISURE,
      frame: DECISION_FRAMES.RECOMMEND,
      query: attrs.has("design_forward") || lifestyle
        ? `Design-forward ${brand} hotel in ${city}`
        : `Full-service ${brand} hotel in ${city} for leisure travelers`,
    },
    {
      intent: TRAVELER_INTENTS.BUSINESS,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where to stay at a ${loyaltyHint}-affiliated ${brand} hotel in ${city}?`,
    },
    {
      intent: TRAVELER_INTENTS.WELLNESS,
      frame: DECISION_FRAMES.RECOMMEND,
      query: attrs.has("spa") || attrs.has("full_service_spa")
        ? `${name} spa or wellness amenities in ${city}`
        : `${scale} hotel in ${place} with fitness amenities`,
    },
    {
      intent: TRAVELER_INTENTS.CELEBRATION,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `Celebration dinner and overnight at ${name} in ${city}`,
    },
    {
      intent: TRAVELER_INTENTS.COUPLES,
      frame: DECISION_FRAMES.BEST_FOR,
      query: `Couples anniversary at ${name} in ${place}`,
    },
    {
      intent: TRAVELER_INTENTS.GROUP_MEETING,
      frame: DECISION_FRAMES.WHERE_SHOULD,
      query: `Where to host a board dinner near ${place} with ${style} lodging?`,
    },
    {
      intent: TRAVELER_INTENTS.ADVENTURE,
      frame: DECISION_FRAMES.RECOMMEND,
      query: `${name} as a base for exploring ${city} dining and nightlife`,
    },
  ];

  return rows.map((r, i) => ({
    scenarioId: `prop_gen_${pid}_${String(i + 1).padStart(2, "0")}`,
    intent: r.intent,
    frame: r.frame,
    query: r.query,
    layer: "property_capability",
    sourceBasis: GENERIC_PROFILE_SCENARIOS_VERSION,
  }));
}

/**
 * Full generic universe when no certified market pack exists.
 * @param {object} propertyProfile
 */
export function buildGenericParityScenarioUniverse(propertyProfile = {}) {
  const core = generateGenericProfileScenarios(propertyProfile);
  const capability = generateGenericPropertyCapabilityScenarios(propertyProfile);
  return {
    version: GENERIC_PROFILE_SCENARIOS_VERSION,
    scenarios: [...core, ...capability],
    layers: {
      core: core.filter((s) => s.layer === "core").length,
      archetype: core.filter((s) => s.layer === "archetype").length,
      market: core.filter((s) => s.layer === "market").length,
      property_capability: capability.length,
      total: core.length + capability.length,
      destinationArchetypePack: core.length,
    },
  };
}
