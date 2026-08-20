/**
 * ADP scenario density expansion catalog V1 — RESEARCH ONLY.
 * Not merged into buildScenarioUniverse. Live measurement stays on the current 65.
 * Internal prompt drafts stay in this module; customer artifacts must not include query text.
 */

export const SCENARIO_EXPANSION_CATALOG_VERSION = "adp_scenario_expansion_catalog_v1";
export const CATALOG_ACTIVATION = "ACTIVATED_IN_BOCA_STANDARD_PACK_V2";

const GATES = Object.freeze([
  "DISTINCT_DECISION_CONTEXT",
  "OWNER_TRAVELER_RELEVANCE",
  "NOT_DUPLICATIVE",
  "ANSWERABLE_BY_ALL_PROVIDERS",
  "PROPERTY_APPLICABILITY",
  "COMPETITIVE_DIFFERENTIATION_VALUE",
  "NO_PROMPT_LEADING_BIAS",
  "NO_BRAND_HOTEL_NAME_FORCING",
]);

function candidate(row) {
  return {
    ...row,
    gates: GATES,
    activation: CATALOG_ACTIVATION,
    measurement: "SCENARIO_DESIGNED_NOT_MEASURED",
  };
}

/** Rejected after similarity / overlap check. */
export const DUPLICATE_CANDIDATES_REJECTED = Object.freeze([
  {
    draftId: "rej_fam_pool_resort",
    intent: "family",
    reason: "Near-duplicate of std_boca_fam_01 (family-friendly resort + pool).",
  },
  {
    draftId: "rej_fam_spring_break_2",
    intent: "family",
    reason: "Near-duplicate of std_boca_fam_03 (spring break / school-break already covered).",
  },
  {
    draftId: "rej_wel_yoga_pool_dining",
    intent: "wellness",
    reason: "Near-duplicate of std_boca_wel_02 (yoga, pool, healthy dining).",
  },
  {
    draftId: "rej_wel_spa_amenities",
    intent: "wellness",
    reason: "Near-duplicate of std_boca_wel_01 (spa and wellness amenities).",
  },
  {
    draftId: "rej_adv_marina_boat",
    intent: "adventure",
    reason: "Near-duplicate of prop_ws_01 (marina and boat access).",
  },
  {
    draftId: "rej_adv_jet_ski_marina",
    intent: "adventure",
    reason: "Near-duplicate of prop_ws_11 (paddle / jet ski / marina).",
  },
  {
    draftId: "rej_cel_sunset_100",
    intent: "celebration",
    reason: "Overlaps prop_ws_09 and std_boca_cel_04 (waterfront reception ~100 guests).",
  },
  {
    draftId: "rej_cel_wedding_under_200",
    intent: "celebration",
    reason: "Overlaps prop_ws_15 and std_boca_cel_02 (wedding / rehearsal venue).",
  },
]);

export const OVERLAPPING_CANDIDATES_MERGED = Object.freeze([
  {
    from: ["kids club + family pool", "kids activities on property"],
    into: "std_boca_fam_06",
    reason: "Same decision: families choosing a hotel for children's activities, not a second pool question.",
  },
  {
    from: ["spa weekend", "day spa overnight"],
    into: "std_boca_wel_03",
    reason: "Same spa-focused stay context.",
  },
  {
    from: ["quiet wellness", "relaxation reset"],
    into: "std_boca_wel_08",
    reason: "Same quiet / restoration stay; do not clone leisure quiet-vacation.",
  },
]);

/**
 * Final new scenarios. Queries are INTERNAL drafts for a future wave only.
 */
export const FINAL_NEW_SCENARIOS = Object.freeze([
  candidate({
    scenarioId: "std_boca_fam_06",
    intent: "family",
    source: "STANDARD",
    frame: "best_for",
    decisionContext: "connecting_rooms_suites",
    personaNeed: "Family of 4–6 needing adjoining rooms or a suite",
    propertyRelevance: "HIGH — 139-key boutique; suites/connecting inventory is a real family filter even if not a mega-resort",
    competitiveValue: "Separates suite hotels (Hilton Boca Suites / Embassy) from waterfront boutiques",
    queryInternal: "Best hotel in Boca Raton with connecting rooms or a suite for a family of five",
  }),
  candidate({
    scenarioId: "std_boca_fam_07",
    intent: "family",
    source: "STANDARD",
    frame: "recommend",
    decisionContext: "kids_activities",
    personaNeed: "Parents choosing a stay for children's activities, not only a pool",
    propertyRelevance: "MEDIUM — Waterstone is pool/waterfront, not a kids-club resort; still a fair market question",
    competitiveValue: "Shows whether AI names mega-resorts vs smaller Boca hotels for kid programming",
    queryInternal: "Recommend a Boca Raton hotel for a family that wants kids activities, not just a swimming pool",
  }),
  candidate({
    scenarioId: "std_boca_fam_08",
    intent: "family",
    source: "STANDARD",
    frame: "where_should",
    decisionContext: "family_dining_convenience",
    personaNeed: "Families who need easy on-site or walkable dining",
    propertyRelevance: "HIGH — waterfront dining + Mizner Park walkability",
    competitiveValue: "Convenience vs destination-resort dining",
    queryInternal: "Where should a family stay in Boca Raton if they want easy hotel dining and nearby restaurants?",
  }),
  candidate({
    scenarioId: "std_boca_fam_09",
    intent: "family",
    source: "STANDARD",
    frame: "best_for",
    decisionContext: "winter_holiday_school_break",
    personaNeed: "Winter / holiday school-break trip (distinct from spring break)",
    propertyRelevance: "HIGH — Palm Beach County winter family demand",
    competitiveValue: "Seasonal family trip, not spring-break party context",
    queryInternal: "Best family hotel in Palm Beach County for a winter holiday school-break trip",
  }),
  candidate({
    scenarioId: "std_boca_wel_03",
    intent: "wellness",
    source: "STANDARD",
    frame: "best_for",
    decisionContext: "spa_focused_stay",
    personaNeed: "Traveler booking a stay primarily for spa treatments",
    propertyRelevance: "MEDIUM — Waterstone has fitness/pool, not a destination spa; market question still valid",
    competitiveValue: "Spa hotels (Boca Raton / Seagate / Opal) vs non-spa waterfront",
    queryInternal: "Best hotel in Boca Raton for a spa-focused weekend stay",
  }),
  candidate({
    scenarioId: "std_boca_wel_04",
    intent: "wellness",
    source: "STANDARD",
    frame: "recommend",
    decisionContext: "fitness_recovery",
    personaNeed: "Fitness / recovery stay (gym, movement, rest)",
    propertyRelevance: "HIGH — 24hr fitness + pool",
    competitiveValue: "Gym/recovery vs spa-only peers",
    queryInternal: "Recommend an upscale Boca Raton hotel for a fitness and recovery weekend",
  }),
  candidate({
    scenarioId: "std_boca_wel_05",
    intent: "wellness",
    source: "STANDARD",
    frame: "where_should",
    decisionContext: "couples_spa",
    personaNeed: "Couple seeking spa time together (wellness, not romantic-stay territory)",
    propertyRelevance: "MEDIUM — spa inventory sits with CORE spa hotels",
    competitiveValue: "Couples spa vs Couples / Romantic Stay overlap is intentional but framed as treatments",
    queryInternal: "Where should a couple stay in Palm Beach County for spa treatments together?",
  }),
  candidate({
    scenarioId: "std_boca_wel_06",
    intent: "wellness",
    source: "STANDARD",
    frame: "best_for",
    decisionContext: "waterfront_wellness",
    personaNeed: "Wellness traveler who wants water + restoration, not nightlife",
    propertyRelevance: "HIGH — Intracoastal setting",
    competitiveValue: "Waterfront restoration vs inland spa",
    queryInternal: "Best waterfront hotel in Boca Raton for a quiet wellness stay",
  }),
  candidate({
    scenarioId: "std_boca_wel_07",
    intent: "wellness",
    source: "STANDARD",
    frame: "recommend",
    decisionContext: "luxury_wellness_escape",
    personaNeed: "Higher-end wellness escape (treatments + privacy)",
    propertyRelevance: "MEDIUM — luxury spa bar is Boca Raton / Eau-class; do not force Waterstone",
    competitiveValue: "Whether AI jumps to luxury-only names",
    queryInternal: "Recommend a luxury wellness escape in the Boca Raton or Palm Beach area",
  }),
  candidate({
    scenarioId: "std_boca_wel_08",
    intent: "wellness",
    source: "STANDARD",
    frame: "best_for",
    decisionContext: "relaxation_quiet_stay",
    personaNeed: "Rest / low-stimulation stay (not leisure sightseeing)",
    propertyRelevance: "HIGH — boutique scale vs mega-resort bustle",
    competitiveValue: "Quiet restoration vs family-pool energy",
    queryInternal: "Best hotel in Boca Raton for a restful, low-key wellness overnight",
  }),
  candidate({
    scenarioId: "std_boca_adv_04",
    intent: "adventure",
    source: "STANDARD",
    frame: "best_for",
    decisionContext: "snorkeling_ocean_day",
    personaNeed: "Ocean / reef day from a Boca base (not marina berth)",
    propertyRelevance: "MEDIUM-HIGH — beach walking distance; not a dive resort",
    competitiveValue: "Ocean-activity hotels vs Intracoastal marina hotels",
    queryInternal: "Best hotel in Boca Raton as a base for snorkeling or an ocean activity day",
  }),
  candidate({
    scenarioId: "std_boca_adv_05",
    intent: "adventure",
    source: "STANDARD",
    frame: "recommend",
    decisionContext: "active_coastal_weekend",
    personaNeed: "Active outdoor weekend (bike, beach, water) without a kids/family frame",
    propertyRelevance: "HIGH — coastal Boca leisure-active mix",
    competitiveValue: "Active adults vs resort-leisure pool stay",
    queryInternal: "Recommend a Boca Raton hotel for an active coastal weekend with beach and outdoor time",
  }),
  candidate({
    scenarioId: "std_boca_cel_06",
    intent: "celebration",
    source: "STANDARD",
    frame: "recommend",
    decisionContext: "hosted_anniversary_gathering",
    personaNeed: "Hosting an anniversary gathering (guests + dinner), not a couples-only anniversary trip",
    propertyRelevance: "HIGH — event space + waterfront dining",
    competitiveValue: "Hosted celebration vs Couples anniversary overnight already in couples pack",
    queryInternal: "Recommend a Boca Raton hotel to host an anniversary dinner for family and friends",
  }),
]);

export const TERRITORY_EXPANSION_PLAN = Object.freeze({
  business: {
    CURRENT_COUNT: 12,
    TARGET_COUNT: 12,
    NEW_SCENARIOS_NEEDED: 0,
    RATIONALE: "Coverage already spans executive, Honors, waterfront-after-work, Mizner, and compare. No semantic gap that a new question would uniquely measure.",
    recommendation: "NO_CHANGE",
  },
  leisure: {
    CURRENT_COUNT: 12,
    TARGET_COUNT: 12,
    NEW_SCENARIOS_NEEDED: 0,
    RATIONALE: "Pool, beach, waterfront dining, pet-friendly, and quiet vacation already present. Do not add for count.",
    recommendation: "NO_CHANGE",
  },
  couples: {
    CURRENT_COUNT: 9,
    TARGET_COUNT: 9,
    NEW_SCENARIOS_NEEDED: 0,
    RATIONALE: "Already at recommended density. Anniversary/honeymoon/balcony live here; do not clone into Celebrations except hosted gatherings.",
    recommendation: "NO_CHANGE",
  },
  family: {
    CURRENT_COUNT: 5,
    TARGET_COUNT: 9,
    NEW_SCENARIOS_NEEDED: 4,
    RATIONALE: "Five questions collapse to pool/beach/spring-break/multigen/compare. Need suites, kids activities, dining convenience, and a non-spring school-break. 9 not 8 because those four contexts are distinct.",
    recommendation: "EXPAND",
  },
  group_meeting: {
    CURRENT_COUNT: 13,
    TARGET_COUNT: 13,
    NEW_SCENARIOS_NEEDED: 0,
    RATIONALE: "Reference territory. Do not copy meeting scenarios into other intents.",
    recommendation: "NO_CHANGE",
  },
  wellness: {
    CURRENT_COUNT: 2,
    TARGET_COUNT: 8,
    NEW_SCENARIOS_NEEDED: 6,
    RATIONALE: "Two questions cannot survive leave-one-out. Keep as a distinct territory: spa/fitness/restoration is a real traveler decision even if Waterstone is not a destination spa.",
    recommendation: "EXPAND",
    KEEP_AS_DISTINCT_TERRITORY: "YES",
  },
  adventure: {
    CURRENT_COUNT: 5,
    TARGET_COUNT: 7,
    NEW_SCENARIOS_NEEDED: 2,
    RATIONALE: "Do not invent a fourth CORE. Expand two non-marina ocean/active contexts so rates are less LOO-fragile. Still not indexable.",
    recommendation: "EXPAND_SCENARIOS_FOR_RATE_ONLY",
    INDEXABLE_FUTURE: "NO",
  },
  celebration: {
    CURRENT_COUNT: 7,
    TARGET_COUNT: 8,
    NEW_SCENARIOS_NEEDED: 1,
    RATIONALE: "Wedding, rehearsal, engagement, birthday, milestone, and waterfront venue already exist; two property-specific questions overlap standard. Add one hosted-anniversary gathering only. Do not pad to 8 with another wedding.",
    recommendation: "PLUS_1",
  },
});

export function publicNewScenario(row) {
  const {
    queryInternal,
    ...rest
  } = row;
  return {
    ...rest,
    queryInternal: undefined,
    promptTextExposed: false,
  };
}

export function newScenariosForIntent(intent) {
  return FINAL_NEW_SCENARIOS.filter((s) => s.intent === intent);
}

export function expansionSummary() {
  const byIntent = {};
  for (const [intent, plan] of Object.entries(TERRITORY_EXPANSION_PLAN)) {
    const neu = newScenariosForIntent(intent);
    byIntent[intent] = {
      ...plan,
      STANDARD_NEW: neu.filter((s) => s.source === "STANDARD").length,
      PROPERTY_SPECIFIC_NEW: neu.filter((s) => s.source === "PROPERTY_SPECIFIC").length,
      FINAL_COUNT: plan.CURRENT_COUNT + neu.length,
      PROPOSED_IDS: neu.map((s) => s.scenarioId),
    };
  }
  const newN = FINAL_NEW_SCENARIOS.length;
  return {
    version: SCENARIO_EXPANSION_CATALOG_VERSION,
    activation: CATALOG_ACTIVATION,
    liveUniverseUnchanged: false,
    NEW_TOTAL_SCENARIOS: 65 + newN,
    newScenarioCount: newN,
    byIntent,
  };
}
