/**
 * Canonical ADP customer-facing metric semantics registry V1.
 * Single source of truth for grain, numerator/denominator, and owner-facing copy.
 * Shared by SPA/PDF adapters, staged rebuild, and Full E2E certification.
 */

import {
  COMPETITOR_PRESENT_CARD,
  COMPETITOR_PRESENT_COUNTING_GRAIN,
  COMPETITOR_PRESENT_FULLY_ABSENT_GRAIN,
} from "./competitor-present-scenario-copy-v1.js";
import { MIN_RANK_SAMPLE } from "./position-metrics.js";
import { METRIC_GRAINS } from "./grain-governance.js";

export const ADP_METRIC_SEMANTICS_REGISTRY_V1 = "ADP_METRIC_SEMANTICS_REGISTRY_V1";

/** Canonical counting grains used across ADP customer surfaces. */
export const ADP_COUNTING_GRAINS = Object.freeze({
  SCENARIO_LEVEL: "SCENARIO_LEVEL",
  PROVIDER_RESPONSE_LEVEL: "PROVIDER_RESPONSE_LEVEL",
  RANKED_RESPONSE_LEVEL: "RANKED_RESPONSE_LEVEL",
  SCENARIO_PROVIDER_PAIR: "SCENARIO_PROVIDER_PAIR",
  PROPERTY_ATTRIBUTE_LEVEL: "PROPERTY_ATTRIBUTE_LEVEL",
  ATTRIBUTE_OBSERVATION_LEVEL: "ATTRIBUTE_OBSERVATION_LEVEL",
  COMPETITOR_ENTITY_LEVEL: "COMPETITOR_ENTITY_LEVEL",
  CITATION_OCCURRENCE_LEVEL: "CITATION_OCCURRENCE_LEVEL",
  UNIQUE_SOURCE_LEVEL: "UNIQUE_SOURCE_LEVEL",
  BPP_COHORT_LEVEL: "BPP_COHORT_LEVEL",
  MONITORING_PERIOD_LEVEL: "MONITORING_PERIOD_LEVEL",
  RESPONSE_LEVEL_ABSENCE: COMPETITOR_PRESENT_COUNTING_GRAIN,
  SCENARIO_LEVEL_ABSENCE: COMPETITOR_PRESENT_FULLY_ABSENT_GRAIN,
});

/**
 * @typedef {object} AdpMetricDefinition
 * @property {string} id
 * @property {string} displayTitle
 * @property {string} [displaySubtitle]
 * @property {string} businessMeaning
 * @property {string} grain
 * @property {string} numerator
 * @property {string} denominator
 * @property {string} producerModule
 * @property {string} producerFn
 * @property {string} payloadPath
 * @property {string} spaRenderer
 * @property {string[]} surfaces
 * @property {string} ownerExplanation
 * @property {string[]} [invariants]
 * @property {string} [eligibility]
 */

/** @type {readonly AdpMetricDefinition[]} */
export const ADP_METRIC_DEFINITIONS = Object.freeze([
  {
    id: "property_reality_coverage",
    displayTitle: "Property Reality Coverage",
    displaySubtitle:
      "Share of monitored property attributes that AI responses represent for this hotel",
    businessMeaning:
      "How completely AI answers cover the hotel’s governed real-world attributes when the hotel is mentioned",
    grain: ADP_COUNTING_GRAINS.PROPERTY_ATTRIBUTE_LEVEL,
    numerator: "Governed attributes recognized (recognition rate ≥ threshold)",
    denominator: "Governed property attributes in the Reality Gap set",
    producerModule: "lib/ai-demand-positioning/intelligence/reality-gap.js",
    producerFn: "computeRealityGap",
    payloadPath: "realityGap.recognizedCount / realityGap.totalAttributes",
    spaRenderer: "renderExecKpis / propertyRealityCoverageSnapshot",
    surfaces: ["spa", "share", "pdf", "monthly_review"],
    ownerExplanation:
      "Of the property facts we track, how many show up in AI answers about your hotel.",
    eligibility: "MEASURED only with governed attributes AND non-empty observation corpus",
    invariants: [
      "recognizedCount ≤ totalAttributes",
      "empty attributes or empty observations → NOT_MEASURED (never fabricate 0%)",
    ],
  },
  {
    id: "scenarios_monitored",
    displayTitle: "Scenarios Monitored",
    displaySubtitle: "Traveler questions in this monitoring period × AI providers",
    businessMeaning: "Size of the monitored traveler-question × provider hand",
    grain: ADP_COUNTING_GRAINS.SCENARIO_PROVIDER_PAIR,
    numerator: "Eligible monitored scenarios (display) × comparable providers",
    denominator: "N/A (count card, not a rate)",
    producerModule: "lib/ai-demand-positioning/metrics/consideration-rate.js",
    producerFn: "computeConsiderationMetrics (eligibleScenarios) + period providers",
    payloadPath: "executiveMetrics.scenarioPresence.eligibleScenarios | period.scenarioCount",
    spaRenderer: "scenariosMonitoredSnapshot",
    surfaces: ["spa", "share", "pdf"],
    ownerExplanation:
      "How many traveler questions we tested and how many AI models answered them.",
    invariants: ["scenarioCount ≥ 0", "must bind to periodId"],
  },
  {
    id: "traveler_needs_appeared",
    displayTitle: "Traveler Needs Where Hotel Appeared",
    displaySubtitle: "Scenarios with subject present in ≥1 provider answer",
    businessMeaning: "Scenario-level presence (alias of AI Scenario Presence numerator)",
    grain: ADP_COUNTING_GRAINS.SCENARIO_LEVEL,
    numerator: "Eligible scenarios with ≥1 comparable provider mentioning subject",
    denominator: "Eligible monitored scenarios",
    producerModule: "lib/ai-demand-positioning/metrics/consideration-rate.js",
    producerFn: "computeConsiderationMetrics",
    payloadPath: "executiveMetrics.scenarioPresence.capturedScenarios",
    spaRenderer: "travelerNeedsAppearanceSnapshot",
    surfaces: ["spa", "share", "pdf"],
    ownerExplanation:
      "Traveler questions where at least one AI model named your hotel.",
    invariants: [
      "appeared + fullyAbsent = eligibleScenarios when coverage is complete partition",
    ],
  },
  {
    id: "traveler_needs_missing",
    displayTitle: "Traveler Needs Where Hotel Was Missing",
    displaySubtitle: "Scenarios with subject absent from all comparable provider answers",
    businessMeaning: "Full-scenario absence",
    grain: ADP_COUNTING_GRAINS.SCENARIO_LEVEL,
    numerator: "Eligible scenarios with zero comparable subject mentions",
    denominator: "Eligible monitored scenarios",
    producerModule: "lib/ai-demand-positioning/metrics/consideration-rate.js",
    producerFn: "computeConsiderationMetrics (eligible − captured)",
    payloadPath: "derived from scenarioPresence",
    spaRenderer: "travelerNeedsAppearanceSnapshot complement",
    surfaces: ["spa", "share", "pdf"],
    ownerExplanation:
      "Traveler questions where every monitored AI answer left your hotel out.",
    invariants: [
      "fullyAbsent ≤ eligibleScenarios",
      "NOT the same metric as Scenarios With Competitive Displacement",
    ],
  },
  {
    id: "top_observed_ai_alternative",
    displayTitle: "Top Observed AI Alternative",
    displaySubtitle: "Hotel appearing most often across monitored AI responses this period",
    businessMeaning: "Highest-recurrence non-subject hotel entity in comparable observations",
    grain: ADP_COUNTING_GRAINS.COMPETITOR_ENTITY_LEVEL,
    numerator: "Observation mentions of leading alternative entity",
    denominator: "Comparable observations (recurrence ranking)",
    producerModule:
      "lib/ai-demand-positioning/customer/top-observed-ai-alternative-v1.js",
    producerFn: "resolveTopObservedAiAlternative",
    payloadPath: "competitiveSet.topObservedAlternative",
    spaRenderer: "renderExecKpis",
    surfaces: ["spa", "share", "pdf"],
    ownerExplanation:
      "The other hotel AI mentioned most often this period — not automatically your true competitor.",
    invariants: ["subject entity aliases must never win this title"],
  },
  {
    id: "ai_consideration_rate",
    displayTitle: "AI Consideration Rate",
    displaySubtitle: "{present} of {comparable} comparable AI responses",
    businessMeaning: "Share of eligible comparable provider answers that name the subject",
    grain: ADP_COUNTING_GRAINS.PROVIDER_RESPONSE_LEVEL,
    numerator: "Comparable eligible observations with subject mentioned",
    denominator: "Comparable eligible provider responses",
    producerModule: "lib/ai-demand-positioning/metrics/consideration-rate.js",
    producerFn: "computeConsiderationMetrics",
    payloadPath: "executiveMetrics.considerationRate",
    spaRenderer: "renderExecutiveMetricEnhancements",
    surfaces: ["spa", "share", "pdf", "monthly_review"],
    ownerExplanation:
      "How often your hotel is named in the individual AI answers we collected.",
    eligibility: "Eligible consideration scenarios only; exclude dryRun/error",
    invariants: [
      "0 ≤ rate ≤ 100 when denominator > 0",
      "null/insufficient when no comparable observations",
      `legacyGrainAlias=${METRIC_GRAINS.OBSERVATION}`,
    ],
  },
  {
    id: "ai_scenario_presence",
    displayTitle: "AI Scenario Presence",
    displaySubtitle: "Present in {captured} of {eligible} monitored demand scenarios",
    businessMeaning: "Share of scenarios with ≥1 provider mentioning subject",
    grain: ADP_COUNTING_GRAINS.SCENARIO_LEVEL,
    numerator: "Eligible scenarios with any subject presence",
    denominator: "Eligible monitored scenarios",
    producerModule: "lib/ai-demand-positioning/metrics/consideration-rate.js",
    producerFn: "computeConsiderationMetrics",
    payloadPath: "executiveMetrics.scenarioPresence",
    spaRenderer: "renderExecutiveMetricEnhancements",
    surfaces: ["spa", "share", "pdf", "monthly_review"],
    ownerExplanation:
      "Share of traveler questions where at least one AI model named your hotel.",
    invariants: [
      "Not equal to AI Consideration Rate (different grain)",
      `legacyGrainAlias=${METRIC_GRAINS.SCENARIO}`,
    ],
  },
  {
    id: "number_one_appearance_rate",
    displayTitle: "#1 Appearance Rate",
    displaySubtitle:
      "{n1} of {rankEligibleN} AI responses with verifiable hotel rankings",
    businessMeaning: "Share of rank-eligible answers where subject is first",
    grain: ADP_COUNTING_GRAINS.RANKED_RESPONSE_LEVEL,
    numerator: "Rank-eligible observations with position === 1",
    denominator: "Rank-eligible observations (mentioned ∧ rankEligible ∧ position)",
    producerModule: "lib/ai-demand-positioning/metrics/position-metrics.js",
    producerFn: "computePositionMetrics",
    payloadPath: "executiveMetrics.rankMetrics.numberOneAppearanceRate",
    spaRenderer: "renderExecutiveMetricEnhancements",
    surfaces: ["spa", "share", "pdf"],
    ownerExplanation:
      "When AI clearly ranks hotels, how often yours is listed first. Denominator is only responses with a verifiable ranked hotel list — not all Core observations.",
    eligibility: `rankEligibleN ≥ ${MIN_RANK_SAMPLE}`,
    invariants: [
      "numberOneCount ≤ topThreeCount when same denominator",
      "numberOneCount ≤ rankEligibleN",
      "do not use all comparable responses as denominator",
    ],
  },
  {
    id: "top_three_appearance_rate",
    displayTitle: "Top-3 Appearance Rate",
    displaySubtitle:
      "{top3} of {rankEligibleN} AI responses with verifiable hotel rankings",
    businessMeaning: "Share of rank-eligible answers where subject is in top 3",
    grain: ADP_COUNTING_GRAINS.RANKED_RESPONSE_LEVEL,
    numerator: "Rank-eligible observations with position ≤ 3",
    denominator: "Same rank-eligible denominator as #1",
    producerModule: "lib/ai-demand-positioning/metrics/position-metrics.js",
    producerFn: "computePositionMetrics",
    payloadPath: "executiveMetrics.rankMetrics.topThreeAppearanceRate",
    spaRenderer: "renderExecutiveMetricEnhancements",
    surfaces: ["spa", "share", "pdf"],
    ownerExplanation:
      "When AI clearly ranks hotels, how often yours appears in the first three places. Distinct from Portfolio Top-3 Appearance (Loyalty scenario grain).",
    eligibility: `rankEligibleN ≥ ${MIN_RANK_SAMPLE}`,
    invariants: ["topThreeCount ≤ rankEligibleN", "topThreeCount ≥ numberOneCount"],
  },
  {
    id: "scenarios_with_competitive_displacement",
    displayTitle: COMPETITOR_PRESENT_CARD.title,
    displaySubtitle: COMPETITOR_PRESENT_CARD.subtitle,
    businessMeaning:
      "Distinct scenarios with ≥1 response that names a valid competitor without naming the subject",
    grain: ADP_COUNTING_GRAINS.RESPONSE_LEVEL_ABSENCE,
    numerator:
      "Unique scenarios with ≥1 comparable response: subject absent ∧ competitor present",
    denominator: "N/A (scenario count card; not a rate)",
    producerModule: "lib/ai-demand-positioning/metrics/competitor-present-gaps.js",
    producerFn: "computeCompetitorPresentGaps",
    payloadPath: "executiveMetrics.competitorPresentScenarios.scenarioCount",
    spaRenderer: "renderExecutiveMetricEnhancements",
    surfaces: ["spa", "share", "pdf"],
    ownerExplanation: COMPETITOR_PRESENT_CARD.tip.definition,
    eligibility: "Declared comp set when present; else customer-facing competitors; never self",
    invariants: [
      "scenarioCount (RESPONSE_LEVEL) may exceed subject fully-absent scenarios",
      "fullyAbsentScenarioCount ≤ scenarioCount",
      "fullyAbsentScenarioCount ≤ subjectFullyAbsentScenarios",
      "subject aliases never count as competitors",
    ],
    payloadKeys: Object.freeze({
      scenarioCount: "RESPONSE_LEVEL_ABSENCE distinct scenarios",
      fullyAbsentScenarioCount: "SCENARIO_LEVEL_ABSENCE competitor-present",
      observationCount: "PROVIDER_RESPONSE_LEVEL displacement observations",
    }),
  },
  {
    id: "territory_ai_presence",
    displayTitle: "Your AI Presence (by Demand Territory)",
    displaySubtitle: "Presence rate within a demand territory",
    businessMeaning: "Observation presence rate scoped to an intent/territory",
    grain: ADP_COUNTING_GRAINS.PROVIDER_RESPONSE_LEVEL,
    numerator: "Subject-mentioned comparable obs in territory",
    denominator: "Comparable obs in territory",
    producerModule:
      "lib/ai-demand-positioning/metrics/governed-customer-presence-index.js",
    producerFn: "buildGovernedIntentPresenceIndex",
    payloadPath: "intentPresenceIndex[*].subjectRatePct",
    spaRenderer: "AI Presence by Demand Territory table",
    surfaces: ["spa", "share", "pdf"],
    ownerExplanation:
      "How often your hotel appears in AI answers for this type of traveler need.",
  },
  {
    id: "ai_presence_index",
    displayTitle: "AI Presence Index",
    displaySubtitle: "Subject presence ÷ CORE benchmark × 100 (certified scopes only)",
    businessMeaning: "Governed index vs CORE peer mean for a territory",
    grain: ADP_COUNTING_GRAINS.PROVIDER_RESPONSE_LEVEL,
    numerator: "Subject territory presence rate",
    denominator: "CORE benchmark rate (PRODUCTION_VALIDATED only)",
    producerModule:
      "lib/ai-demand-positioning/metrics/governed-customer-presence-index.js",
    producerFn: "buildGovernedIntentPresenceIndex",
    payloadPath: "intentPresenceIndex[*].index",
    spaRenderer: "AI Presence by Demand Territory table",
    surfaces: ["spa", "share", "pdf", "monthly_review"],
    ownerExplanation:
      "How your hotel’s AI presence compares with the certified CORE peer average for that need (100 = parity).",
    eligibility: "PRODUCTION_VALIDATED certification required — never invent",
  },
  {
    id: "provider_presence",
    displayTitle: "Provider Presence",
    displaySubtitle: "Mention rate per AI provider",
    businessMeaning: "Per-provider consideration rate",
    grain: ADP_COUNTING_GRAINS.PROVIDER_RESPONSE_LEVEL,
    numerator: "Mentioned comparable observations for provider",
    denominator: "Comparable observations for provider",
    producerModule: "lib/ai-demand-positioning/metrics/provider-presence-v1.js",
    producerFn: "buildProviderPresenceRows",
    payloadPath: "evidence.providers[*]",
    spaRenderer: "Provider Presence table",
    surfaces: ["spa", "share", "pdf"],
    ownerExplanation: "How often each AI model names your hotel.",
  },
  {
    id: "citation_coverage",
    displayTitle: "Citation Coverage",
    displaySubtitle: "Share of comparable answers that include citations",
    businessMeaning: "Grounded-response rate",
    grain: ADP_COUNTING_GRAINS.PROVIDER_RESPONSE_LEVEL,
    numerator: "Comparable observations with ≥1 citation",
    denominator: "Comparable observations",
    producerModule: "lib/ai-demand-positioning/metrics/source-metrics.js",
    producerFn: "computeSourceMetrics",
    payloadPath: "evidence.citationRate",
    spaRenderer: "Citation Intelligence",
    surfaces: ["spa", "share", "pdf"],
    ownerExplanation: "How often AI answers cite sources when discussing hotels.",
  },
  {
    id: "owned_source_share",
    displayTitle: "Owned Sources",
    displaySubtitle: "Share of citations pointing to owned/brand domains",
    businessMeaning: "Owned vs external citation mix",
    grain: ADP_COUNTING_GRAINS.CITATION_OCCURRENCE_LEVEL,
    numerator: "Owned-domain citation occurrences",
    denominator: "Citation occurrences (or cited responses — see producer)",
    producerModule:
      "lib/ai-demand-positioning/metrics/owned-source-classification-v1.js",
    producerFn: "computeOwnedExternalSourceMix",
    payloadPath: "evidence.ownedSourceShare",
    spaRenderer: "Citation Intelligence",
    surfaces: ["spa", "share", "pdf"],
    ownerExplanation:
      "Among cited sources, how often the citation is your hotel’s or brand’s own site.",
  },
  {
    id: "bpp_portfolio_ai_presence",
    displayTitle: "Portfolio AI Presence",
    displaySubtitle: "Subject presence within certified BPP peer cohort lens",
    businessMeaning: "BPP observation-grain presence",
    grain: ADP_COUNTING_GRAINS.BPP_COHORT_LEVEL,
    numerator: "Subject hits in BPP comparable observations",
    denominator: "BPP comparable observations",
    producerModule:
      "lib/ai-demand-positioning/brand-portfolio/brand-portfolio-first-cycle-metrics-v1.js",
    producerFn: "BPP first-cycle metrics",
    payloadPath: "brandPortfolioPosition.kpis.portfolioAiPresence",
    spaRenderer: "Brand & Portfolio Position",
    surfaces: ["spa", "share", "pdf"],
    ownerExplanation:
      "How often your hotel appears among AI answers in your brand-portfolio comparison set.",
    eligibility: "Certified peer cohort required — never invent ranks",
  },
  {
    id: "current_vs_prior_delta",
    displayTitle: "Δ vs Prior Run",
    displaySubtitle: "Percentage-point change vs last comparable period",
    businessMeaning: "Longitudinal delta for certified comparable periods only",
    grain: ADP_COUNTING_GRAINS.MONITORING_PERIOD_LEVEL,
    numerator: "Current metric − prior comparable metric (pp)",
    denominator: "N/A (delta)",
    producerModule:
      "lib/ai-demand-positioning/metrics/longitudinal-comparability.js",
    producerFn: "buildLongitudinalComparison",
    payloadPath: "executiveMetrics.currentVsPrior.deltas.*",
    spaRenderer: "executive metric cards / territory Δ column",
    surfaces: ["spa", "share", "pdf"],
    ownerExplanation:
      "Change since the last comparable monitoring period — two periods are not a trend.",
    eligibility: "Comparable period rules (scenario count, providers, not dry-run)",
    invariants: ["customerTrendReady remains false until governed trend product"],
  },
]);

export function getMetricDefinition(id) {
  return ADP_METRIC_DEFINITIONS.find((m) => m.id === id) || null;
}

export function listMetricIds() {
  return ADP_METRIC_DEFINITIONS.map((m) => m.id);
}

export function metricsByGrain(grain) {
  return ADP_METRIC_DEFINITIONS.filter((m) => m.grain === grain);
}
