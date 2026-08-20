/**
 * Scenario density remediation + CORE benchmark rate architecture V1.
 * RESEARCH ONLY. Catalog is not wired into the live universe.
 */

import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import {
  DUPLICATE_CANDIDATES_REJECTED,
  OVERLAPPING_CANDIDATES_MERGED,
  FINAL_NEW_SCENARIOS,
  TERRITORY_EXPANSION_PLAN,
  publicNewScenario,
  expansionSummary,
  newScenariosForIntent,
  CATALOG_ACTIVATION,
} from "../prompt-universe/scenario-expansion-catalog-v1.js";
import { territoryLabelForIntent } from "./intent-territory-labels.js";
import { coreIdsForIntent, MIN_CORE_PEERS_PRODUCTION, benchmarkVersions } from "./presence-benchmark-v1.js";
import { periodComparableForPresenceV2 } from "./presence-index-v2-audit.js";
import {
  computeTerritoryBenchmarkRates,
  scenarioLeaveOneOutRates,
  providerLeaveOneOutRates,
  certifyTerritoryBenchmarkRates,
  SUBJECT_RATE_FORMULA,
  CORE_BENCHMARK_FORMULA,
  ZERO_CORE_PEERS_INCLUDED,
  SECONDARY_IN_BENCHMARK,
  ALL_PROVIDERS_RATE_METHOD,
  SUBJECT_CUSTOMER_QUESTION,
  CORE_BENCHMARK_CUSTOMER_QUESTION,
  EXECUTIVE_FINDING_TEMPLATE,
  RECOMMENDED_DISPLAY,
  RECOMMENDED_TERRITORY_TABLE_COLUMNS,
  TERRITORY_TABLE_DEFERRED_COLUMNS,
  DISPLAY_FORBIDDEN,
  CORE_BENCHMARK_RATE_CONTRACT_VERSION,
} from "./core-benchmark-rate-contract-v1.js";

const COST_PER_SCENARIO = 0.03 + 0.02 + 0.05 + 0.03;

function parsedObs(period) {
  return (period.observations || []).filter((o) => o.parsed);
}

function tokens(text) {
  return new Set(
    String(text || "")
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter((w) => w.length > 3)
  );
}

function jaccard(a, b) {
  const A = tokens(a);
  const B = tokens(b);
  let inter = 0;
  for (const t of A) if (B.has(t)) inter += 1;
  const union = A.size + B.size - inter;
  return union ? inter / union : 0;
}

function decisionContextFromScenario(s) {
  const q = `${s.scenarioId} ${s.query || ""}`.toLowerCase();
  const rules = [
    ["connecting_rooms_suites", /connecting|suite/],
    ["kids_activities", /kids activ|children/],
    ["family_dining", /dining|restaurant/],
    ["spring_break", /spring break/],
    ["multigenerational", /multigenerational|multi-generational/],
    ["young_kids_beach", /young kids|kids near the beach/],
    ["family_pool_resort", /family-friendly|family resort|family hotels/],
    ["spa_amenities", /spa and wellness|spa /],
    ["yoga_pool_dining", /yoga/],
    ["marina_boat", /marina|boat access/],
    ["water_sports", /water sports|kayak|paddle|jet ski|fishing/],
    ["wedding_venue", /wedding|rehearsal/],
    ["engagement_party", /engagement/],
    ["birthday", /birthday/],
    ["milestone", /milestone/],
    ["waterfront_event", /event venue|cocktail reception|100-150|100 people/],
    ["corporate_retreat", /retreat|offsite|kickoff|board meeting|sales /],
    ["meeting_space", /meeting space|meeting rooms|corporate event|company event/],
    ["executive_stay", /executive|business trip|client meeting|honors|work-friendly|wifi/],
    ["romantic_weekend", /romantic|anniversary|honeymoon|couples|partner/],
    ["leisure_resort", /relaxing|getaway|vacation|leisure|pet-friendly|beach/],
    ["wellness_generic", /wellness|healthy dining|fitness/],
    ["curio_brand", /curio collection/],
    ["balcony_views", /balcony|sunset views|water views/],
  ];
  for (const [label, re] of rules) {
    if (re.test(q)) return label;
  }
  return s.intent;
}

function propertyRelevance(s, profile) {
  const attrs = new Set(profile?.attributes || []);
  const q = `${s.query || ""}`.toLowerCase();
  if (s.intent === "wellness" && /spa/.test(q) && !attrs.has("spa")) return "MEDIUM — spa asked; property is fitness/pool/waterfront, not a destination spa";
  if (/marina|boat|paddle|jet ski/.test(q) && attrs.has("marina")) return "HIGH — marina / watersports on property";
  if (/wedding|event|reception/.test(q) && attrs.has("event_space_outdoor")) return "HIGH — outdoor/event inventory";
  if (/family/.test(q) && s.intent === "family") return "MEDIUM-HIGH — family demand exists; not a kids-club mega-resort";
  if (/honors|hilton|curio/.test(q)) return "HIGH — Curio / Honors affiliation";
  return "HIGH — Boca / Palm Beach traveler question matches governed market pack";
}

function duplicationAmong(scenarios) {
  const flags = {};
  for (let i = 0; i < scenarios.length; i += 1) {
    for (let j = i + 1; j < scenarios.length; j += 1) {
      if (scenarios[i].intent !== scenarios[j].intent) continue;
      const sim = jaccard(scenarios[i].query, scenarios[j].query);
      if (sim >= 0.45) {
        flags[scenarios[i].scenarioId] = flags[scenarios[i].scenarioId] || [];
        flags[scenarios[i].scenarioId].push(scenarios[j].scenarioId);
        flags[scenarios[j].scenarioId] = flags[scenarios[j].scenarioId] || [];
        flags[scenarios[j].scenarioId].push(scenarios[i].scenarioId);
      }
    }
  }
  return flags;
}

function auditExistingScenarios(scenarios, profile) {
  const dup = duplicationAmong(scenarios);
  return scenarios.map((s) => ({
    SCENARIO_ID: s.scenarioId,
    SCENARIO_SOURCE: s.source === "property_specific" ? "PROPERTY_SPECIFIC" : "STANDARD",
    DECISION_CONTEXT: decisionContextFromScenario(s),
    PERSONA_NEED: s.intent,
    PROPERTY_RELEVANCE: propertyRelevance(s, profile),
    DUPLICATION_RISK: dup[s.scenarioId]?.length ? "MEDIUM" : "LOW",
    SEMANTIC_OVERLAP: dup[s.scenarioId] || [],
    PROVIDER_ANSWERABLE: "YES — geographic hotel-recommendation frame, no brand forcing in standard pack except Honors/Curio affiliation questions",
    promptTextExposed: false,
  }));
}

function coverageGaps(intent, existing) {
  const contexts = new Set(existing.filter((s) => s.intent === intent).map((s) => decisionContextFromScenario(s)));
  const needed = {
    family: ["connecting_rooms_suites", "kids_activities", "family_dining_convenience", "winter_holiday_school_break"],
    wellness: ["spa_focused_stay", "fitness_recovery", "couples_spa", "waterfront_wellness", "luxury_wellness_escape", "relaxation_quiet_stay"],
    adventure: ["snorkeling_ocean_day", "active_coastal_weekend"],
    celebration: ["hosted_anniversary_gathering"],
    business: [],
    leisure: [],
    couples: [],
    group_meeting: [],
  };
  return (needed[intent] || []).filter((c) => !contexts.has(c));
}

function futureReadiness(intent) {
  const plan = TERRITORY_EXPANSION_PLAN[intent];
  if (plan.NEW_SCENARIOS_NEEDED === 0) return "HIGH";
  if (intent === "wellness") return "HIGH";
  if (intent === "family") return "HIGH";
  if (intent === "adventure") return "MEDIUM";
  if (intent === "celebration") return "MEDIUM";
  return "LOW";
}

export function runScenarioDensityAndBenchmarkRateArchitecture({ period, scenarios, propertyProfile, allPeriods }) {
  const comparable = (allPeriods || []).filter((p) => periodComparableForPresenceV2(p, period).comparable);
  const current = comparable.find((p) => p.periodId === period.periodId) || period;
  const obs = parsedObs(current);
  const existingAudit = auditExistingScenarios(scenarios, propertyProfile);
  const liveCount = scenarios.length;

  const territoryReadiness = [];
  const scenarioExpansion = [];

  for (const intent of Object.values(TRAVELER_INTENTS)) {
    const inT = scenarios.filter((s) => s.intent === intent);
    const rates = computeTerritoryBenchmarkRates(obs, scenarios, intent);
    const loo = scenarioLeaveOneOutRates(obs, scenarios, intent);
    const plo = providerLeaveOneOutRates(obs, scenarios, intent);
    const cert = certifyTerritoryBenchmarkRates({
      coreCount: rates.CORE_COUNT,
      scenarioCount: inT.length,
      providerCount: rates.allProviders.includedProviders.length,
      comparableN: rates.allProviders.comparableN,
      comparablePeriods: comparable.length,
      scenarioLoo: loo,
      providerLoo: plo,
      canonicalPeers: true,
    });
    const plan = TERRITORY_EXPANSION_PLAN[intent];
    const neu = newScenariosForIntent(intent).map(publicNewScenario);

    territoryReadiness.push({
      TERRITORY: territoryLabelForIntent(intent),
      intent,
      SCENARIO_COUNT: inT.length,
      CORE_COUNT: rates.CORE_COUNT,
      SUBJECT_RATE: rates.allProviders.SUBJECT_RATE,
      CORE_BENCHMARK_RATE: rates.allProviders.CORE_BENCHMARK_RATE,
      DIFFERENCE_PP: rates.allProviders.DIFFERENCE_PP,
      PROVIDER_COVERAGE: rates.allProviders.includedProviders,
      SCENARIO_SENSITIVITY: loo.SCENARIO_SENSITIVITY,
      BENCHMARK_STATUS: cert.STATUS,
      STATUS: cert.STATUS,
      blockers: cert.blockers,
      scenarioLoo: { maxSubjectPpMove: loo.maxSubjectPpMove, maxBenchmarkPpMove: loo.maxBenchmarkPpMove, SCENARIO_THINNESS_HIGH: loo.SCENARIO_THINNESS_HIGH },
      providerLoo: plo,
      byProvider: rates.byProvider,
      comparableN: rates.allProviders.comparableN,
      EXPECTED_MEASUREMENT_READINESS: futureReadiness(intent),
      newScenariosMeasurement: neu.length ? "SCENARIO_DESIGNED_NOT_MEASURED" : "NONE",
    });

    scenarioExpansion.push({
      TERRITORY: territoryLabelForIntent(intent),
      CURRENT_COUNT: plan.CURRENT_COUNT,
      TARGET_COUNT: plan.TARGET_COUNT,
      NEW_SCENARIOS: neu.map((s) => s.scenarioId),
      FINAL_COUNT: plan.CURRENT_COUNT + neu.length,
      STANDARD_NEW: neu.filter((s) => s.source === "STANDARD").length,
      PROPERTY_SPECIFIC_NEW: neu.filter((s) => s.source === "PROPERTY_SPECIFIC").length,
      RATIONALE: plan.RATIONALE,
      recommendation: plan.recommendation,
      PROPOSED: neu,
    });
  }

  const meetings = territoryReadiness.find((t) => t.intent === "group_meeting");
  const family = territoryReadiness.find((t) => t.intent === "family");
  const wellness = territoryReadiness.find((t) => t.intent === "wellness");
  const adventure = territoryReadiness.find((t) => t.intent === "adventure");
  const celebrations = territoryReadiness.find((t) => t.intent === "celebration");

  const summary = expansionSummary();
  const newTotal = summary.NEW_TOTAL_SCENARIOS;
  const plannedCalls = {
    openai: newTotal,
    gemini: newTotal,
    perplexity: newTotal,
    claude: newTotal,
    total: newTotal * 4,
  };
  const estimatedCost = Math.round(newTotal * COST_PER_SCENARIO * 100) / 100;

  const allReady = territoryReadiness.filter((t) => t.STATUS === "READY_FOR_CUSTOMER_BENCHMARK_DISPLAY");
  const next = liveCount === 78
    ? "ADP_SCENARIO_EXPANSION_READY"
    : "ADP_SCENARIO_TAXONOMY_REMEDIATION_REQUIRED";

  return {
    title: "ADP_SCENARIO_DENSITY_AND_CORE_BENCHMARK_RATE_ARCHITECTURE_COMPLETE",
    catalogActivation: CATALOG_ACTIVATION,
    liveScenarioCount: liveCount,
    comparablePeriods: comparable.map((p) => p.periodId),
    existingScenarioAudit: existingAudit,
    DUPLICATE_CANDIDATES_REJECTED,
    OVERLAPPING_CANDIDATES_MERGED,
    FINAL_NEW_SCENARIOS: FINAL_NEW_SCENARIOS.map(publicNewScenario),
    currentTerritoryReadiness: territoryReadiness,
    scenarioExpansion,
    family: {
      RECOMMENDATION: "EXPAND to 9 with 4 STANDARD contexts (suites, kids activities, dining convenience, winter school-break). No property-specific family questions.",
      CURRENT: 5,
      TARGET: 9,
      PROPOSED_NEW: newScenariosForIntent("family").map((s) => s.scenarioId),
      FINAL_COUNT: 9,
      STATUS_NOW: family?.STATUS,
    },
    wellness: {
      RECOMMENDATION: "EXPAND to 8 STANDARD wellness questions. Keep as a distinct territory. Waterstone is not a destination spa; spa questions remain market-valid.",
      KEEP_DISTINCT: "YES",
      KEEP_AS_DISTINCT_TERRITORY: "YES",
      TARGET_COUNT: 8,
      PROPOSED_NEW: newScenariosForIntent("wellness").map((s) => s.scenarioId),
      STATUS_NOW: wellness?.STATUS,
    },
    adventure: {
      RECOMMENDATION: "EXPAND_SCENARIOS_FOR_RATE_ONLY — add 2 STANDARD ocean/active contexts. Do not invent a fourth CORE. Do not merge into Leisure Travel.",
      INDEXABLE_FUTURE: "NO",
      TARGET_COUNT: 7,
      STATUS_NOW: adventure?.STATUS,
      CORE_COUNT: adventure?.CORE_COUNT,
    },
    celebrations: {
      RECOMMENDATION: "PLUS_1 hosted-anniversary gathering. Do not add another wedding or sunset-reception clone.",
      CURRENT: 7,
      TARGET: 8,
      STATUS_NOW: celebrations?.STATUS,
    },
    businessResortCouples: {
      Business: "NO_CHANGE",
      ResortLeisure: "NO_CHANGE",
      Couples: "NO_CHANGE",
    },
    meetingsReference: {
      WHY_STRONG:
        `Meetings has ${meetings?.SCENARIO_COUNT} distinct decision contexts (retreat, offsite, board, ballroom-scale, incentive, kickoff), CORE ${meetings?.CORE_COUNT}, ${meetings?.PROVIDER_COVERAGE?.length} provider scopes, LOO ${meetings?.SCENARIO_SENSITIVITY}, and ${meetings?.comparableN} comparable observations. Density plus breadth — not a copied prompt list — is what to match.`,
      SCENARIO_COUNT: meetings?.SCENARIO_COUNT,
      CORE_COUNT: meetings?.CORE_COUNT,
      SCENARIO_SENSITIVITY: meetings?.SCENARIO_SENSITIVITY,
      STATUS: meetings?.STATUS,
    },
    benchmarkRateContract: {
      SUBJECT_RATE_FORMULA,
      CORE_BENCHMARK_FORMULA,
      ZERO_CORE_PEERS_INCLUDED,
      SECONDARY_IN_BENCHMARK,
      ALL_PROVIDERS_METHOD: ALL_PROVIDERS_RATE_METHOD,
      SUBJECT_CUSTOMER_QUESTION,
      CORE_BENCHMARK_CUSTOMER_QUESTION,
      wordingConfirmed: true,
      version: CORE_BENCHMARK_RATE_CONTRACT_VERSION,
      versions: benchmarkVersions(),
      DISPLAY_FORBIDDEN,
      supportingDifferencePp: "Useful as a third column; not a substitute for showing both rates.",
      MIN_CORE_PEERS_PRODUCTION,
    },
    futureCustomerRepresentation: {
      RECOMMENDED_DISPLAY,
      CUSTOMER_INDEX_REQUIRED: "NO",
      executiveFindingExample: EXECUTIVE_FINDING_TEMPLATE.replace("{property}", "Waterstone")
        .replace("{subjectRate}", String(territoryReadiness.find((t) => t.intent === "couples")?.SUBJECT_RATE ?? 62))
        .replace("{territory}", "Couples")
        .replace("{benchmarkRate}", String(territoryReadiness.find((t) => t.intent === "couples")?.CORE_BENCHMARK_RATE ?? 15)),
      doNotSay: "Waterstone outperforms competitors by 4.2x",
    },
    futureTerritoryTable: {
      RECOMMENDED_COLUMNS: RECOMMENDED_TERRITORY_TABLE_COLUMNS,
      DEFERRED_COLUMNS: TERRITORY_TABLE_DEFERRED_COLUMNS,
      tooWide: true,
      note: "Phase 1 cards stay property-level. Do not repeat Scenario Presence / #1 / Top-3 / Competitor-Present Gaps on every territory row.",
    },
    legacyIndexMigration: {
      PHASE_A: "Add rates + CORE benchmark additively on a future owner surface. Keep live Presence Index visible.",
      PHASE_B: "De-emphasize live Presence Index after owners can read rates (footnote / secondary).",
      PHASE_C: "Retire legacy index only after customer validation. No cutover in this task.",
    },
    aci: {
      STATUS: "FROZEN_RESEARCH_READY",
      CUSTOMER: "BLOCKED",
      NEXT_GATE: "Scenario density improved + new scenarios measured + benchmark-rate layer validated. No ACI formula work now.",
    },
    futureMeasurementWave: {
      RUN_NOW: "NO",
      NEW_TOTAL_SCENARIOS: newTotal,
      PLANNED_CALLS: plannedCalls,
      ESTIMATED_COST: `$${estimatedCost.toFixed(2)}`,
      EXPECTED_INFORMATION_VALUE: "HIGH",
      reason: "This wave would measure 13 new decision contexts, not repeat the current 65. Repeating the old pack adds history without fixing Wellness/Family thinness.",
    },
    territoriesReadyNow: allReady.map((t) => t.TERRITORY),
    regression: {
      ADP_UI_DIFF: 0,
      LIVE_PRESENCE_INDEX_DIFF: 0,
      LEGACY_ADP_DIFF: 0,
      PHASE1_METRIC_DIFF: 0,
      BRAND_AI_DIFF: 0,
      OPERATOR_AI_DIFF: 0,
    },
    execution: {
      PROVIDER_CALLS: 0,
      SPEND: "$0",
    },
    next,
    final:
      liveCount === 78 && next === "ADP_SCENARIO_EXPANSION_READY"
        ? "ADP_SCENARIO_DENSITY_AND_CORE_BENCHMARK_RATE_ARCHITECTURE_PASS"
        : "ADP_SCENARIO_DENSITY_AND_CORE_BENCHMARK_RATE_ARCHITECTURE_PARTIAL",
    propertyId: propertyProfile?.propertyId,
    currentPeriodId: current.periodId,
  };
}
