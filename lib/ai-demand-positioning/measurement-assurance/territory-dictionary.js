/**
 * Canonical ADP demand-territory dictionary V1.
 */

import { ASSURANCE_TERRITORY_DICT_VERSION } from "./version.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";

export const DEMAND_TERRITORY_DICTIONARY_V1 = Object.freeze({
  version: ASSURANCE_TERRITORY_DICT_VERSION,
  territories: Object.freeze([
    {
      id: TRAVELER_INTENTS.BUSINESS,
      customerLabel: "Business Travel",
      definition: "Work, meetings, corporate travel, executive stays.",
      inclusion: ["business trip", "meetings", "corporate", "executive", "WiFi/work"],
      exclusion: ["pure leisure vacation without work framing"],
      examples: ["Best hotel for a business trip", "corporate executive visiting for meetings"],
      ambiguous: ["bleisure trips — classify by dominant prompt framing"],
    },
    {
      id: TRAVELER_INTENTS.LEISURE,
      customerLabel: "Leisure",
      definition: "Relaxation / vacation / getaway without primary couples/family/celebration framing.",
      inclusion: ["vacation", "weekend getaway", "relaxing"],
      exclusion: ["explicit couples anniversary", "family with kids", "corporate meeting"],
      examples: ["Best resort for a relaxing vacation"],
      ambiguous: ["romantic leisure vs couples — prefer couples when romantic/anniversary explicit"],
    },
    {
      id: TRAVELER_INTENTS.COUPLES,
      customerLabel: "Couples / Romantic Stay",
      definition: "Couples, romantic, anniversary, honeymoon framing.",
      inclusion: ["romantic", "couples", "anniversary", "honeymoon"],
      exclusion: ["family with children", "corporate"],
      examples: ["romantic hotel for a couples weekend"],
      ambiguous: [],
    },
    {
      id: TRAVELER_INTENTS.FAMILY,
      customerLabel: "Family",
      definition: "Travel with children / multigenerational family.",
      inclusion: ["family", "kids", "children", "multigenerational"],
      exclusion: ["adults-only couples"],
      examples: ["family-friendly resort with a pool"],
      ambiguous: [],
    },
    {
      id: TRAVELER_INTENTS.GROUP_MEETING,
      customerLabel: "Group / Meeting",
      definition: "Meetings, retreats, events, offsites, group gatherings.",
      inclusion: ["meeting", "retreat", "offsite", "event", "board meeting"],
      exclusion: ["solo leisure"],
      examples: ["hotel for a small corporate retreat"],
      ambiguous: ["celebration events may be CELEBRATION when social celebration dominates"],
    },
    {
      id: TRAVELER_INTENTS.WELLNESS,
      customerLabel: "Wellness",
      definition: "Spa, wellness, fitness recovery, restful wellness stays.",
      inclusion: ["spa", "wellness", "yoga", "recovery"],
      exclusion: ["generic leisure without wellness framing"],
      examples: ["hotel with spa and wellness amenities"],
      ambiguous: [],
    },
    {
      id: TRAVELER_INTENTS.ADVENTURE,
      customerLabel: "Adventure",
      definition: "Active / adventure travel framing.",
      inclusion: ["adventure", "active travel", "outdoors"],
      exclusion: ["passive spa wellness"],
      examples: [],
      ambiguous: [],
    },
    {
      id: TRAVELER_INTENTS.CELEBRATION,
      customerLabel: "Celebration",
      definition: "Birthdays, weddings, engagement, milestone celebrations.",
      inclusion: ["birthday", "wedding", "engagement", "celebration", "milestone"],
      exclusion: ["ordinary leisure"],
      examples: ["hotel for a birthday celebration dinner"],
      ambiguous: [],
    },
  ]),
});

const VALID_IDS = new Set(DEMAND_TERRITORY_DICTIONARY_V1.territories.map((t) => t.id));

/**
 * Heuristic expected territory from prompt text — used to flag drift, not rewrite production.
 */
export function inferTerritoryFromPrompt(query) {
  const q = String(query || "").toLowerCase();
  if (/wedding|engagement|birthday|celebration|anniversary dinner|rehearsal/.test(q)) {
    return TRAVELER_INTENTS.CELEBRATION;
  }
  if (/spa|wellness|yoga|recovery weekend|fitness and recovery/.test(q)) {
    return TRAVELER_INTENTS.WELLNESS;
  }
  if (/adventure|hiking|outdoors expedition/.test(q)) return TRAVELER_INTENTS.ADVENTURE;
  if (/family|kids|children|multigenerational|school-break/.test(q)) return TRAVELER_INTENTS.FAMILY;
  if (/romantic|couples|honeymoon|anniversary trip|partner and i/.test(q)) {
    return TRAVELER_INTENTS.COUPLES;
  }
  if (/meeting|retreat|offsite|board meeting|corporate event|sales kickoff|incentive trip/.test(q)) {
    return TRAVELER_INTENTS.GROUP_MEETING;
  }
  if (/business|corporate executive|meetings|hilton honors points for business/.test(q)) {
    return TRAVELER_INTENTS.BUSINESS;
  }
  if (/vacation|weekend getaway|leisure|relax/.test(q)) return TRAVELER_INTENTS.LEISURE;
  return null;
}

export function auditScenarioTerritories(scenarios, propertyId) {
  const rows = [];
  let fail = 0;
  let unknown = 0;
  for (const s of scenarios || []) {
    const assigned = s.intent;
    const expected = inferTerritoryFromPrompt(s.query || s.prompt);
    const validAssigned = VALID_IDS.has(assigned);
    let status = "PASS";
    if (!validAssigned) {
      status = "FAIL";
      fail += 1;
    } else if (expected && expected !== assigned) {
      // Property packs may intentionally differ; flag as REVIEW not hard fail unless clear conflict
      status = "REVIEW";
      unknown += 1;
    }
    rows.push({
      scenarioId: s.scenarioId,
      prompt: s.query || s.prompt || null,
      propertyId,
      assignedTerritory: assigned,
      expectedTerritory: expected,
      status,
    });
  }
  return {
    version: ASSURANCE_TERRITORY_DICT_VERSION,
    total: rows.length,
    pass: rows.filter((r) => r.status === "PASS").length,
    review: rows.filter((r) => r.status === "REVIEW").length,
    fail,
    rows,
  };
}
