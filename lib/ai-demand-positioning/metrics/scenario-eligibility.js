/**
 * Governed scenario eligibility — property profile inputs only (never AI output).
 */

export const ELIGIBILITY_STATES = Object.freeze({
  CORE_RELEVANT: "CORE_RELEVANT",
  CONDITIONALLY_RELEVANT: "CONDITIONALLY_RELEVANT",
  PROPERTY_SPECIFIC: "PROPERTY_SPECIFIC",
  OUT_OF_SCOPE: "OUT_OF_SCOPE",
  INSUFFICIENT_PROPERTY_TRUTH: "INSUFFICIENT_PROPERTY_TRUTH",
});

const GENERIC_UPSCALE_HINTS = ["upscale", "luxury", "resort", "hotel in", "where should", "recommend"];

function queryText(scenario) {
  return String(scenario?.query || "").toLowerCase();
}

function hasAttribute(profile, attr) {
  return (profile?.attributes || []).includes(attr);
}

function isGeographyMatch(query, profile) {
  const city = String(profile?.city || "").toLowerCase();
  const market = String(profile?.market || "").toLowerCase();
  const submarket = String(profile?.submarket || "").toLowerCase();
  if (city && query.includes(city)) return true;
  if (submarket.includes("boca") && query.includes("boca")) return true;
  if (market.includes("south florida") && (query.includes("south florida") || query.includes("palm beach"))) return true;
  if (submarket.includes("bermuda") || market.includes("bermuda")) {
    return query.includes("bermuda");
  }
  if (market.includes("new york") || city.includes("new york")) {
    return query.includes("new york") || query.includes("nyc") || query.includes("manhattan")
      || query.includes("soho") || query.includes("noho") || query.includes("times square");
  }
  return false;
}

function classifyStandardScenario(scenario, profile) {
  const q = queryText(scenario);
  const intent = scenario.intent;

  if (intent === "family" && !hasAttribute(profile, "family_friendly")) {
    return ELIGIBILITY_STATES.CONDITIONALLY_RELEVANT;
  }
  if (intent === "wellness" && !hasAttribute(profile, "full_service_spa") && !hasAttribute(profile, "spa")) {
    return ELIGIBILITY_STATES.CONDITIONALLY_RELEVANT;
  }
  if ((intent === "adventure" || q.includes("snorkel") || q.includes("scuba")) && !hasAttribute(profile, "watersports") && !hasAttribute(profile, "kayaking")) {
    if (q.includes("marina") || q.includes("boat") || q.includes("paddleboard") || q.includes("jet ski")) {
      return hasAttribute(profile, "marina") || hasAttribute(profile, "watersports")
        ? ELIGIBILITY_STATES.CORE_RELEVANT
        : ELIGIBILITY_STATES.OUT_OF_SCOPE;
    }
    return ELIGIBILITY_STATES.CONDITIONALLY_RELEVANT;
  }
  if ((q.includes("marina") || q.includes("boat") || q.includes("intracoastal") || q.includes("waterfront")) && !hasAttribute(profile, "marina") && !hasAttribute(profile, "waterfront")) {
    return ELIGIBILITY_STATES.OUT_OF_SCOPE;
  }
  if ((q.includes("meeting") || q.includes("retreat") || q.includes("event")) && !hasAttribute(profile, "meeting_space")) {
    return ELIGIBILITY_STATES.CONDITIONALLY_RELEVANT;
  }
  if (q.includes("five star") || q.includes("five-star") || q.includes("aaa five-diamond")) {
    return ELIGIBILITY_STATES.CONDITIONALLY_RELEVANT;
  }
  if (isGeographyMatch(q, profile) || GENERIC_UPSCALE_HINTS.some((h) => q.includes(h))) {
    return ELIGIBILITY_STATES.CORE_RELEVANT;
  }
  return ELIGIBILITY_STATES.CONDITIONALLY_RELEVANT;
}

export function classifyScenarioEligibility(scenario, profile) {
  if (scenario.source === "property_specific") {
    return ELIGIBILITY_STATES.PROPERTY_SPECIFIC;
  }
  if (!scenario?.query || !profile?.propertyId) {
    return ELIGIBILITY_STATES.INSUFFICIENT_PROPERTY_TRUTH;
  }
  return classifyStandardScenario(scenario, profile);
}

export function buildScenarioEligibilityMap(scenarios, profile) {
  const map = {};
  const counts = {
    CORE_RELEVANT: 0,
    CONDITIONALLY_RELEVANT: 0,
    PROPERTY_SPECIFIC: 0,
    OUT_OF_SCOPE: 0,
    INSUFFICIENT_PROPERTY_TRUTH: 0,
  };
  for (const scenario of scenarios) {
    const state = classifyScenarioEligibility(scenario, profile);
    map[scenario.scenarioId] = state;
    counts[state] = (counts[state] || 0) + 1;
  }
  return { byScenarioId: map, counts };
}

export function scenariosForEligibility(scenarios, eligibilityMap, allowedStates) {
  const allowed = new Set(allowedStates);
  return scenarios.filter((s) => allowed.has(eligibilityMap.byScenarioId[s.scenarioId]));
}

/** Applicable for consideration metrics: core + conditional + property-specific */
export const CONSIDERATION_ELIGIBLE_STATES = [
  ELIGIBILITY_STATES.CORE_RELEVANT,
  ELIGIBILITY_STATES.CONDITIONALLY_RELEVANT,
  ELIGIBILITY_STATES.PROPERTY_SPECIFIC,
];

export function computeDemandCoverage(eligibilityMap, scenarios) {
  const applicable = scenarios.filter((s) =>
    CONSIDERATION_ELIGIBLE_STATES.includes(eligibilityMap.byScenarioId[s.scenarioId])
  );
  const total = applicable.length;
  const monitored = total;
  return {
    rate: scenarios.length > 0 ? Math.round((total / scenarios.length) * 1000) / 10 : 0,
    applicableTerritories: new Set(applicable.map((s) => s.intent)).size,
    applicableScenarios: total,
    monitoredScenarios: monitored,
    totalScenarios: scenarios.length,
    grain: "SCENARIO_GRAIN",
    note: "Share of full scenario universe classified as CORE, CONDITIONAL, or PROPERTY_SPECIFIC — measures monitoring applicability, not AI performance.",
  };
}
