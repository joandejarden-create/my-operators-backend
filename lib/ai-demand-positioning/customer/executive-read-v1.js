/**
 * Governed Executive Read V1 — deterministic owner narrative from certified metrics.
 * No free-form LLM. Property-agnostic classifier; property-specific DATA only.
 */

import { roundAdpPercent } from "../format-percent.js";
import { computeRealityGap } from "../intelligence/reality-gap.js";
import { buildLongitudinalComparison } from "../metrics/longitudinal-comparability.js";
import { enrichObservationsWithRank } from "../metrics/executive-metrics-foundation.js";
import { MIN_RANK_SAMPLE } from "../metrics/position-metrics.js";

export const EXECUTIVE_READ_VERSION = "adp_executive_read_v1";
export const PROPERTY_SPECIFIC_EXECUTIVE_READ_CODE = 0;

/** Rate movement below this (pp) is not treated as improving/weakening. */
export const MATERIALITY_PP_THRESHOLD = 2.0;
export const MATERIALITY_POLICY = "HEURISTIC_PENDING_LONGITUDINAL_VALIDATION";

const HIGH_SCENARIO = 65;
const HIGH_CONSIDERATION = 50;
const LOW_CONSIDERATION = 25;
const LOW_SCENARIO = 35;
const HIGH_TOP3 = 60;
const LOW_TOP3 = 35;
const REALITY_GAP_VS_VISIBILITY_PP = 20;
const HIGH_COMPETITOR_PRESENT_SHARE = 0.45;

export const POSITION_PATTERNS = Object.freeze({
  BROAD_VISIBILITY_INCONSISTENT_CONSIDERATION: "BROAD_VISIBILITY_INCONSISTENT_CONSIDERATION",
  LIMITED_AI_DEMAND_REACH: "LIMITED_AI_DEMAND_REACH",
  STRONG_AND_CONSISTENT_AI_PRESENCE: "STRONG_AND_CONSISTENT_AI_PRESENCE",
  STRONG_CONSIDERATION_WEAK_PROMINENCE: "STRONG_CONSIDERATION_WEAK_PROMINENCE",
  VISIBILITY_STRONGER_THAN_PROPERTY_UNDERSTANDING: "VISIBILITY_STRONGER_THAN_PROPERTY_UNDERSTANDING",
  PROPERTY_UNDERSTANDING_STRONGER_THAN_VISIBILITY: "PROPERTY_UNDERSTANDING_STRONGER_THAN_VISIBILITY",
  COMPETITIVE_LEAKAGE_HIGH: "COMPETITIVE_LEAKAGE_HIGH",
  DEVELOPING_MEASUREMENT_BASE: "DEVELOPING_MEASUREMENT_BASE",
});

export const TREND_STATES = Object.freeze({
  IMPROVING: "IMPROVING",
  BROADLY_STABLE: "BROADLY_STABLE",
  MIXED: "MIXED",
  WEAKENING: "WEAKENING",
  BASELINE_PERIOD: "BASELINE_PERIOD",
  NO_COMPARABLE_PRIOR: "NO_COMPARABLE_PRIOR",
});

const POSITION_COPY = Object.freeze({
  [POSITION_PATTERNS.BROAD_VISIBILITY_INCONSISTENT_CONSIDERATION]:
    "broad AI visibility but less consistent consideration",
  [POSITION_PATTERNS.LIMITED_AI_DEMAND_REACH]: "limited AI demand reach",
  [POSITION_PATTERNS.STRONG_AND_CONSISTENT_AI_PRESENCE]: "strong and consistent AI presence",
  [POSITION_PATTERNS.STRONG_CONSIDERATION_WEAK_PROMINENCE]:
    "frequent AI consideration, with competitors often appearing ahead when responses are ranked",
  [POSITION_PATTERNS.VISIBILITY_STRONGER_THAN_PROPERTY_UNDERSTANDING]:
    "AI visibility that currently exceeds AI understanding of the full property proposition",
  [POSITION_PATTERNS.PROPERTY_UNDERSTANDING_STRONGER_THAN_VISIBILITY]:
    "stronger property representation than overall AI demand reach",
  [POSITION_PATTERNS.COMPETITIVE_LEAKAGE_HIGH]:
    "material competitor presence in demand contexts where the property is absent",
  [POSITION_PATTERNS.DEVELOPING_MEASUREMENT_BASE]: "a developing measurement base",
});

const TREND_COPY = Object.freeze({
  [TREND_STATES.IMPROVING]: "Overall AI demand positioning is improving.",
  [TREND_STATES.BROADLY_STABLE]: "Overall positioning is broadly stable.",
  [TREND_STATES.MIXED]: "Results are mixed across the monitored metrics.",
  [TREND_STATES.WEAKENING]: "AI demand positioning weakened versus the prior comparable period.",
  [TREND_STATES.BASELINE_PERIOD]: "This is the baseline monitoring period for this property.",
  [TREND_STATES.NO_COMPARABLE_PRIOR]:
    "No comparable prior official monitoring period is available yet.",
});

const STRENGTH_COPY = Object.freeze({
  DEMAND_REACH: "Demand reach",
  CONSIDERATION: "Consideration frequency",
  TOP3_PROMINENCE: "Top-3 prominence",
  NUMBER_ONE_PROMINENCE: "#1 prominence",
  ABOVE_BENCHMARK_TERRITORY: "Above-benchmark territory presence",
  PROVIDER_CONSISTENCY: "Provider consistency",
  PROPERTY_REPRESENTATION: "Property representation",
  DEVELOPING: "Current strength is still developing",
});

export const CONSTRAINT_COPY = Object.freeze({
  PROPERTY_REPRESENTATION: "Property representation",
  DEMAND_REACH: "Demand reach",
  CONSIDERATION_CONSISTENCY: "Consideration consistency",
  PROMINENCE: "Prominence when ranked",
  PROVIDER_VARIANCE: "Provider variance",
  COMPETITOR_PRESENT_GAPS: "Competitor-present gaps",
  BENCHMARK_DEVELOPING: "Benchmark not yet certified",
  INSUFFICIENT_RANK_DATA: "Insufficient ranked responses",
  DEVELOPING: "Constraints are still developing",
});

function num(v) {
  return v != null && Number.isFinite(Number(v)) ? Number(v) : null;
}

function fmtPct(v) {
  if (v == null) return null;
  return `${roundAdpPercent(v)}%`;
}

function fmtPp(delta) {
  if (delta == null || !Number.isFinite(Number(delta))) return null;
  const n = Number(delta);
  return `${n > 0 ? "+" : ""}${n.toFixed(1)} pts`;
}

function propertyRealityCoverage(period, propertyProfile) {
  const observations = enrichObservationsWithRank(period?.observations || [], propertyProfile).filter(
    (o) => o.parsed
  );
  const rg = computeRealityGap(observations, propertyProfile);
  if (!rg.totalAttributes) return null;
  return roundAdpPercent((rg.recognizedCount / rg.totalAttributes) * 100);
}

/** Exported for primary trend series — Property Reality Coverage %. */
export function computePropertyRealityCoverage(period, propertyProfile) {
  return propertyRealityCoverage(period, propertyProfile);
}

export function countMeaningfulMetrics(inputs) {
  let n = 0;
  if (inputs.consideration != null) n++;
  if (inputs.scenarioPresence != null) n++;
  if (inputs.reality != null) n++;
  if (inputs.top3 != null) n++;
  if (inputs.numberOne != null) n++;
  if (inputs.competitorPresent != null && inputs.totalScenarios) n++;
  if (inputs.certifiedTerritories?.length) n++;
  return n;
}

export function isCurrentReadAvailable(inputs) {
  return countMeaningfulMetrics(inputs) >= 2;
}

export function findCertifiedBenchmarkFinding(payload) {
  const index = payload?.intentPresenceIndex || {};
  const certified = Object.entries(index)
    .filter(([, row]) => row && row.status === "PRODUCTION_VALIDATED" && row.index != null && !row.developing)
    .map(([intent, row]) => ({
      intent,
      territory: row.territory,
      index: row.index,
      subjectRatePct: row.subjectRatePct ?? row.myRate,
      coreBenchmarkRatePct: row.coreBenchmarkRatePct,
    }))
    .sort((a, b) => (b.index || 0) - (a.index || 0));
  return certified[0] || null;
}

function extractInputs(payload, period, propertyProfile) {
  const em = payload?.executiveMetrics || {};
  const consideration = num(em.considerationRate?.rate);
  const scenarioPresence = num(em.scenarioPresence?.rate);
  const numberOne = num(em.rankMetrics?.numberOneAppearanceRate);
  const top3 = num(em.rankMetrics?.topThreeAppearanceRate);
  const rankEligibleN = num(em.rankMetrics?.rankEligibleN);
  const competitorPresent = num(em.competitorPresentScenarios?.scenarioCount);
  const totalScenarios = num(payload?.demandCapture?.totalScenarios) || num(payload?.period?.scenarioCount);
  const reality = propertyRealityCoverage(period, propertyProfile);

  const intentIndex = payload?.intentPresenceIndex || {};
  const certifiedTerritories = Object.values(intentIndex).filter(
    (r) => r && r.status === "PRODUCTION_VALIDATED" && r.index != null && !r.developing
  );
  const certifiedAbove100 = certifiedTerritories.filter((r) => Number(r.index) > 100).length;
  const developingTerritories = Object.values(intentIndex).filter((r) => r?.developing).length;
  const totalTerritories = Object.keys(intentIndex).length;

  const providerRows = payload?.demandCapture?.byProvider || {};
  const providerRates = Object.entries(providerRows)
    .map(([provider, row]) => ({ provider, rate: num(row?.rate) }))
    .filter((r) => r.rate != null);
  let providerSpread = null;
  if (providerRates.length >= 2) {
    const rates = providerRates.map((r) => r.rate);
    providerSpread = Math.max(...rates) - Math.min(...rates);
  }

  return {
    propertyName: payload?.property?.name || propertyProfile?.name || "This property",
    consideration,
    scenarioPresence,
    numberOne,
    top3,
    rankEligibleN,
    competitorPresent,
    totalScenarios,
    reality,
    certifiedAbove100,
    certifiedTerritories,
    developingTerritories,
    totalTerritories,
    providerRates,
    providerSpread,
    currentVsPrior: em.currentVsPrior || null,
  };
}

export function classifyCurrentPosition(inputs) {
  const {
    consideration,
    scenarioPresence,
    numberOne,
    top3,
    reality,
    competitorPresent,
    totalScenarios,
    rankEligibleN,
  } = inputs;

  if (consideration == null && scenarioPresence == null) {
    return POSITION_PATTERNS.DEVELOPING_MEASUREMENT_BASE;
  }

  if (
    scenarioPresence != null &&
    scenarioPresence < LOW_SCENARIO &&
    (consideration == null || consideration < LOW_CONSIDERATION)
  ) {
    return POSITION_PATTERNS.LIMITED_AI_DEMAND_REACH;
  }

  // Prefer visibility/consideration gap over "strong" when Top-3 is high but consideration lags reach.
  if (
    scenarioPresence != null &&
    consideration != null &&
    scenarioPresence >= HIGH_SCENARIO &&
    consideration <= scenarioPresence - 15
  ) {
    return POSITION_PATTERNS.BROAD_VISIBILITY_INCONSISTENT_CONSIDERATION;
  }

  if (
    consideration != null &&
    consideration >= HIGH_CONSIDERATION &&
    top3 != null &&
    top3 >= HIGH_TOP3 &&
    (numberOne == null || numberOne >= 20)
  ) {
    return POSITION_PATTERNS.STRONG_AND_CONSISTENT_AI_PRESENCE;
  }

  if (
    consideration != null &&
    consideration >= HIGH_CONSIDERATION &&
    top3 != null &&
    top3 < LOW_TOP3 &&
    rankEligibleN != null &&
    rankEligibleN >= MIN_RANK_SAMPLE
  ) {
    return POSITION_PATTERNS.STRONG_CONSIDERATION_WEAK_PROMINENCE;
  }

  if (
    reality != null &&
    scenarioPresence != null &&
    scenarioPresence - reality >= REALITY_GAP_VS_VISIBILITY_PP
  ) {
    return POSITION_PATTERNS.VISIBILITY_STRONGER_THAN_PROPERTY_UNDERSTANDING;
  }

  if (
    reality != null &&
    consideration != null &&
    reality - consideration >= REALITY_GAP_VS_VISIBILITY_PP
  ) {
    return POSITION_PATTERNS.PROPERTY_UNDERSTANDING_STRONGER_THAN_VISIBILITY;
  }

  if (
    competitorPresent != null &&
    totalScenarios &&
    competitorPresent / totalScenarios >= HIGH_COMPETITOR_PRESENT_SHARE
  ) {
    return POSITION_PATTERNS.COMPETITIVE_LEAKAGE_HIGH;
  }

  if (scenarioPresence != null && scenarioPresence >= HIGH_SCENARIO && consideration != null) {
    return POSITION_PATTERNS.BROAD_VISIBILITY_INCONSISTENT_CONSIDERATION;
  }

  return POSITION_PATTERNS.DEVELOPING_MEASUREMENT_BASE;
}

export function classifyPrimaryStrength(inputs) {
  if (inputs.certifiedAbove100 >= 1) {
    return { key: "ABOVE_BENCHMARK_TERRITORY", label: STRENGTH_COPY.ABOVE_BENCHMARK_TERRITORY };
  }
  if (inputs.top3 != null && inputs.top3 >= HIGH_TOP3) {
    return { key: "TOP3_PROMINENCE", label: STRENGTH_COPY.TOP3_PROMINENCE };
  }
  if (inputs.numberOne != null && inputs.numberOne >= 25) {
    return { key: "NUMBER_ONE_PROMINENCE", label: STRENGTH_COPY.NUMBER_ONE_PROMINENCE };
  }
  if (inputs.scenarioPresence != null && inputs.scenarioPresence >= HIGH_SCENARIO) {
    return { key: "DEMAND_REACH", label: STRENGTH_COPY.DEMAND_REACH };
  }
  if (inputs.consideration != null && inputs.consideration >= HIGH_CONSIDERATION) {
    return { key: "CONSIDERATION", label: STRENGTH_COPY.CONSIDERATION };
  }
  if (inputs.reality != null && inputs.reality >= 55) {
    return { key: "PROPERTY_REPRESENTATION", label: STRENGTH_COPY.PROPERTY_REPRESENTATION };
  }
  if (inputs.providerSpread != null && inputs.providerSpread < 12 && inputs.consideration != null) {
    return { key: "PROVIDER_CONSISTENCY", label: STRENGTH_COPY.PROVIDER_CONSISTENCY };
  }
  return { key: "DEVELOPING", label: STRENGTH_COPY.DEVELOPING };
}

export function classifyPrimaryConstraint(inputs) {
  if (
    inputs.rankEligibleN != null &&
    inputs.rankEligibleN < MIN_RANK_SAMPLE &&
    (inputs.consideration == null || inputs.consideration < LOW_CONSIDERATION)
  ) {
    return { key: "INSUFFICIENT_RANK_DATA", label: CONSTRAINT_COPY.INSUFFICIENT_RANK_DATA };
  }
  if (
    inputs.scenarioPresence != null &&
    inputs.consideration != null &&
    inputs.scenarioPresence - inputs.consideration >= 15
  ) {
    return { key: "CONSIDERATION_CONSISTENCY", label: CONSTRAINT_COPY.CONSIDERATION_CONSISTENCY };
  }
  if (inputs.scenarioPresence != null && inputs.scenarioPresence < LOW_SCENARIO) {
    return { key: "DEMAND_REACH", label: CONSTRAINT_COPY.DEMAND_REACH };
  }
  if (
    inputs.reality != null &&
    inputs.scenarioPresence != null &&
    inputs.scenarioPresence - inputs.reality >= REALITY_GAP_VS_VISIBILITY_PP
  ) {
    return { key: "PROPERTY_REPRESENTATION", label: CONSTRAINT_COPY.PROPERTY_REPRESENTATION };
  }
  if (inputs.reality != null && inputs.reality < 40) {
    return { key: "PROPERTY_REPRESENTATION", label: CONSTRAINT_COPY.PROPERTY_REPRESENTATION };
  }
  if (
    inputs.consideration != null &&
    inputs.top3 != null &&
    inputs.consideration >= HIGH_CONSIDERATION &&
    inputs.top3 < LOW_TOP3
  ) {
    return { key: "PROMINENCE", label: CONSTRAINT_COPY.PROMINENCE };
  }
  if (
    inputs.competitorPresent != null &&
    inputs.totalScenarios &&
    inputs.competitorPresent / inputs.totalScenarios >= HIGH_COMPETITOR_PRESENT_SHARE
  ) {
    return { key: "COMPETITOR_PRESENT_GAPS", label: CONSTRAINT_COPY.COMPETITOR_PRESENT_GAPS };
  }
  if (inputs.providerSpread != null && inputs.providerSpread >= 20) {
    return { key: "PROVIDER_VARIANCE", label: CONSTRAINT_COPY.PROVIDER_VARIANCE };
  }
  if (
    inputs.totalTerritories > 0 &&
    inputs.developingTerritories === inputs.totalTerritories
  ) {
    return { key: "BENCHMARK_DEVELOPING", label: CONSTRAINT_COPY.BENCHMARK_DEVELOPING };
  }
  return { key: "DEVELOPING", label: CONSTRAINT_COPY.DEVELOPING };
}

export function classifyTrend(deltas, options = {}) {
  const { hasComparablePrior, periodCount = 1 } = options;
  if (!hasComparablePrior) {
    return periodCount <= 1 ? TREND_STATES.BASELINE_PERIOD : TREND_STATES.NO_COMPARABLE_PRIOR;
  }
  if (!deltas || typeof deltas !== "object") return TREND_STATES.BROADLY_STABLE;

  const keys = ["considerationRate", "scenarioPresence", "propertyRealityCoverage"];
  const material = [];
  for (const k of keys) {
    const d = num(deltas[k]);
    if (d == null) continue;
    if (Math.abs(d) >= MATERIALITY_PP_THRESHOLD) material.push(d);
  }
  if (!material.length) return TREND_STATES.BROADLY_STABLE;
  const up = material.filter((d) => d > 0).length;
  const down = material.filter((d) => d < 0).length;
  if (up && down) return TREND_STATES.MIXED;
  if (up && !down) return TREND_STATES.IMPROVING;
  if (down && !up) return TREND_STATES.WEAKENING;
  return TREND_STATES.BROADLY_STABLE;
}

function buildStrengthSentence(inputs, strength, benchmarkFinding) {
  if (benchmarkFinding && strength.key === "ABOVE_BENCHMARK_TERRITORY") {
    const sub = fmtPct(benchmarkFinding.subjectRatePct);
    const core = fmtPct(benchmarkFinding.coreBenchmarkRatePct);
    const idx = benchmarkFinding.index;
    if (sub && core && idx != null) {
      return `${benchmarkFinding.territory} is a relative strength: the hotel appears in ${sub} of monitored AI responses compared with a ${core} CORE benchmark, producing an AI Presence Index of ${Math.round(idx)}.`;
    }
    return `${benchmarkFinding.territory} is a relative strength versus its governed CORE benchmark.`;
  }
  if (strength.key === "TOP3_PROMINENCE" && inputs.top3 != null) {
    return `When ranked, the hotel appears in the Top 3 in ${fmtPct(inputs.top3)} of ranked responses.`;
  }
  if (strength.key === "NUMBER_ONE_PROMINENCE" && inputs.numberOne != null) {
    return `The hotel ranks #1 in ${fmtPct(inputs.numberOne)} of ranked responses where ranking is available.`;
  }
  if (strength.key === "DEMAND_REACH" && inputs.scenarioPresence != null) {
    return `The hotel appears across ${fmtPct(inputs.scenarioPresence)} of monitored demand scenarios, indicating broad demand-scenario reach.`;
  }
  if (strength.key === "CONSIDERATION" && inputs.consideration != null) {
    return `The hotel appears in ${fmtPct(inputs.consideration)} of comparable individual AI responses.`;
  }
  if (strength.key === "PROPERTY_REPRESENTATION" && inputs.reality != null) {
    return `Property Reality Coverage is ${fmtPct(inputs.reality)}, indicating AI reflects a meaningful share of monitored property attributes.`;
  }
  if (strength.key === "PROVIDER_CONSISTENCY") {
    return "Provider-level presence rates are relatively consistent across monitored AI models.";
  }
  return null;
}

function buildConstraintSentence(inputs, constraint) {
  if (constraint.key === "CONSIDERATION_CONSISTENCY" && inputs.scenarioPresence != null && inputs.consideration != null) {
    return `AI visibility is broader than consideration consistency: the hotel appears in ${fmtPct(inputs.scenarioPresence)} of monitored demand scenarios but only ${fmtPct(inputs.consideration)} of individual comparable AI responses.`;
  }
  if (constraint.key === "DEMAND_REACH" && inputs.scenarioPresence != null) {
    return `Demand reach is limited: the hotel appears in only ${fmtPct(inputs.scenarioPresence)} of monitored demand scenarios.`;
  }
  if (constraint.key === "PROPERTY_REPRESENTATION" && inputs.reality != null) {
    const vis = inputs.scenarioPresence != null ? ` (${fmtPct(inputs.scenarioPresence)} scenario presence)` : "";
    return `AI currently reflects only ${fmtPct(inputs.reality)} of monitored property attributes${vis ? ` despite${vis}` : ""}, so parts of the hotel proposition are not consistently represented.`;
  }
  if (constraint.key === "PROMINENCE" && inputs.top3 != null && inputs.consideration != null) {
    return `The hotel is considered in ${fmtPct(inputs.consideration)} of responses but appears in the Top 3 in only ${fmtPct(inputs.top3)} of ranked responses, meaning competing hotels frequently appear ahead when rankings are shown.`;
  }
  if (constraint.key === "INSUFFICIENT_RANK_DATA") {
    return "Ranked prominence is not yet measurable with enough ranked responses to support a Top-3 or #1 assessment.";
  }
  if (constraint.key === "COMPETITOR_PRESENT_GAPS" && inputs.competitorPresent != null && inputs.totalScenarios) {
    const share = roundAdpPercent((inputs.competitorPresent / inputs.totalScenarios) * 100);
    return `Competitors appear in ${share}% of monitored scenarios where the hotel is absent, indicating material competitive leakage in demand contexts.`;
  }
  if (constraint.key === "PROVIDER_VARIANCE") {
    return "Provider-level presence rates vary materially across monitored AI models.";
  }
  if (constraint.key === "BENCHMARK_DEVELOPING") {
    return "Governed CORE benchmarks are not yet certified for one or more demand territories.";
  }
  return null;
}

function buildCurrentNarrative(inputs, pattern, strength, constraint, benchmarkFinding) {
  const name = inputs.propertyName;
  const sentences = [];

  // Sentence 1 — current position with key metrics
  if (inputs.scenarioPresence != null && inputs.consideration != null) {
    sentences.push(
      `${name} appears in ${fmtPct(inputs.scenarioPresence)} of monitored demand scenarios but in ${fmtPct(inputs.consideration)} of individual comparable AI responses, meaning the hotel is relevant across monitored traveler needs but is not surfaced consistently in every AI response.`
    );
  } else if (inputs.scenarioPresence != null) {
    sentences.push(`${name} appears in ${fmtPct(inputs.scenarioPresence)} of monitored demand scenarios.`);
  } else if (inputs.consideration != null) {
    sentences.push(`${name} appears in ${fmtPct(inputs.consideration)} of comparable individual AI responses.`);
  } else if (pattern === POSITION_PATTERNS.DEVELOPING_MEASUREMENT_BASE) {
    sentences.push(`${name} has a developing AI demand measurement base with limited comparable metrics this period.`);
  } else {
    sentences.push(`${name} shows ${POSITION_COPY[pattern]}.`);
  }

  // Sentence 2 — primary strength
  const strengthSentence = buildStrengthSentence(inputs, strength, benchmarkFinding);
  if (strengthSentence) sentences.push(strengthSentence);

  // Sentence 3 — primary constraint (skip if duplicate of position)
  const constraintSentence = buildConstraintSentence(inputs, constraint);
  if (constraintSentence && !sentences.some((s) => s === constraintSentence)) {
    sentences.push(constraintSentence);
  }

  // Optional benchmark if not used as strength
  if (benchmarkFinding && strength.key !== "ABOVE_BENCHMARK_TERRITORY") {
    const sub = fmtPct(benchmarkFinding.subjectRatePct);
    const core = fmtPct(benchmarkFinding.coreBenchmarkRatePct);
    if (sub && core && benchmarkFinding.index != null) {
      sentences.push(
        `${benchmarkFinding.territory} is a relative strength: ${sub} AI presence versus a ${core} CORE benchmark (AI Presence Index ${Math.round(benchmarkFinding.index)}).`
      );
    }
  }

  return sentences.join(" ").replace(/\.\s*\./g, ".").trim();
}

function buildTrendNarrative(trendState, deltas, inputs) {
  if (trendState === TREND_STATES.NO_COMPARABLE_PRIOR || trendState === TREND_STATES.BASELINE_PERIOD) {
    return TREND_COPY[trendState];
  }

  const bits = [];
  if (deltas?.considerationRate != null && Math.abs(deltas.considerationRate) >= MATERIALITY_PP_THRESHOLD) {
    const dir = deltas.considerationRate >= 0 ? "increased" : "decreased";
    bits.push(`AI Consideration ${dir} by ${fmtPp(Math.abs(deltas.considerationRate))}`);
  } else if (inputs?.consideration != null) {
    bits.push("AI Consideration remained broadly stable");
  }
  if (deltas?.scenarioPresence != null) {
    if (Math.abs(deltas.scenarioPresence) >= MATERIALITY_PP_THRESHOLD) {
      const dir = deltas.scenarioPresence >= 0 ? "increased" : "decreased";
      bits.push(`Scenario Presence ${dir} by ${fmtPp(Math.abs(deltas.scenarioPresence))}`);
    } else {
      bits.push("Scenario Presence remained broadly stable");
    }
  }
  if (deltas?.propertyRealityCoverage != null && Math.abs(deltas.propertyRealityCoverage) >= MATERIALITY_PP_THRESHOLD) {
    const dir = deltas.propertyRealityCoverage >= 0 ? "increased" : "decreased";
    bits.push(`Property Reality Coverage ${dir} by ${fmtPp(Math.abs(deltas.propertyRealityCoverage))}`);
  }

  if (bits.length) {
    return `Since the prior comparable run, ${bits.join(" while ")}.`;
  }
  return TREND_COPY[trendState];
}

const FORBIDDEN_CAUSAL = [
  /\bcaused by\b/i,
  /\bcausing\b/i,
  /\bcauses\b/i,
  /\balgorithm likes\b/i,
  /\bAI prefers\b/i,
  /\bAI chose\b/i,
  /\blost demand\b/i,
  /\bcaptured demand\b/i,
  /\bdemand capture\b/i,
  /\bmarket share\b/i,
  /\btranslating into\b/i,
  /\bleads to\b/i,
];

const FORBIDDEN_MARKETING = [
  /\bcompetitive momentum\b/i,
  /\bstrong positioning\b/i,
  /\bgood momentum\b/i,
  /\bweak momentum\b/i,
  /\bcompetitive advantage\b/i,
  /\bdigital narrative\b/i,
  /\boptimize its\b/i,
];

export function assertNoUnsupportedCausalLanguage(text) {
  const causalHits = FORBIDDEN_CAUSAL.filter((re) => re.test(text || ""));
  const marketingHits = FORBIDDEN_MARKETING.filter((re) => re.test(text || ""));
  const hits = [...causalHits, ...marketingHits];
  return { ok: hits.length === 0, hits: hits.map((r) => String(r)) };
}

/**
 * Build governed executive read from owner payload + period context.
 */
export function buildExecutiveRead(payload, period, scenarios, propertyProfile, options = {}) {
  const inputs = extractInputs(payload, period, propertyProfile);
  const benchmarkFinding = findCertifiedBenchmarkFinding(payload);
  const positionPattern = classifyCurrentPosition(inputs);
  const strength = classifyPrimaryStrength(inputs);
  const constraint = classifyPrimaryConstraint(inputs);
  const currentAvailable = isCurrentReadAvailable(inputs);

  let deltas = inputs.currentVsPrior?.deltas || null;
  let hasComparablePrior = Boolean(inputs.currentVsPrior?.priorComparablePeriodId);
  let periodCount = Array.isArray(options.allPeriods) ? options.allPeriods.length : 1;
  let priorComparablePeriodId = inputs.currentVsPrior?.priorComparablePeriodId || null;

  const fullPeriods = Array.isArray(options.allPeriods)
    ? options.allPeriods.filter((p) => p?.measurementScope?.type !== "TARGETED_CORE_TRUTH_V1")
    : [];

  if (!hasComparablePrior && fullPeriods.length >= 2 && period && scenarios) {
    try {
      const longitudinal = buildLongitudinalComparison(period, fullPeriods, scenarios, propertyProfile);
      hasComparablePrior = Boolean(longitudinal.currentVsPriorReady && longitudinal.priorComparablePeriodId);
      priorComparablePeriodId = longitudinal.priorComparablePeriodId;
      if (hasComparablePrior && longitudinal.deltas) {
        deltas = {
          considerationRate: longitudinal.deltas.aiConsiderationRate,
          scenarioPresence: longitudinal.deltas.aiScenarioPresence,
          propertyRealityCoverage: longitudinal.deltas.propertyRealityCoverage,
        };
      }
    } catch (_) {
      /* optional */
    }
  }

  const trendState = classifyTrend(deltas, { hasComparablePrior, periodCount: fullPeriods.length || periodCount });
  const trendNarrative = buildTrendNarrative(trendState, deltas, inputs);

  if (!currentAvailable) {
    return {
      version: EXECUTIVE_READ_VERSION,
      PROPERTY_SPECIFIC_EXECUTIVE_READ_CODE,
      available: false,
      CURRENT_READ_AVAILABLE: false,
      COMPARABLE_PRIOR_AVAILABLE: hasComparablePrior,
      CURRENT_READ_INDEPENDENT_OF_TREND: true,
      current: null,
      trend: {
        state: trendState,
        label: TREND_COPY[trendState],
        narrative: trendNarrative,
        hasComparablePrior,
        priorComparablePeriodId,
        deltas: deltas || null,
        materialityPp: MATERIALITY_PP_THRESHOLD,
        materialityPolicy: MATERIALITY_POLICY,
      },
      narrative: null,
      meta: null,
      safety: {
        FREE_FORM_UNGOVERNED_NARRATIVE: 0,
        UNSUPPORTED_CAUSAL_CLAIMS: 0,
      },
    };
  }

  const currentNarrative = buildCurrentNarrative(inputs, positionPattern, strength, constraint, benchmarkFinding);
  const narrative = [currentNarrative, trendNarrative].filter(Boolean).join(" ");
  const causalCheck = assertNoUnsupportedCausalLanguage(narrative);

  return {
    version: EXECUTIVE_READ_VERSION,
    PROPERTY_SPECIFIC_EXECUTIVE_READ_CODE,
    available: true,
    CURRENT_READ_AVAILABLE: true,
    COMPARABLE_PRIOR_AVAILABLE: hasComparablePrior,
    CURRENT_READ_INDEPENDENT_OF_TREND: true,
    current: {
      positionPattern,
      label: POSITION_COPY[positionPattern],
      narrative: currentNarrative,
      primaryStrength: strength,
      primaryConstraint: constraint,
      benchmarkFinding: benchmarkFinding || null,
    },
    trend: {
      state: trendState,
      label: TREND_COPY[trendState],
      narrative: trendNarrative,
      hasComparablePrior,
      priorComparablePeriodId,
      deltas: deltas || null,
      materialityPp: MATERIALITY_PP_THRESHOLD,
      materialityPolicy: MATERIALITY_POLICY,
    },
    currentPosition: {
      pattern: positionPattern,
      label: POSITION_COPY[positionPattern],
    },
    primaryStrength: strength,
    primaryConstraint: constraint,
    narrative,
    meta: {
      overall: trendNarrative.replace(/\.$/, ""),
      primaryStrength: strength.label,
      primaryConstraint: constraint.label,
    },
    safety: {
      FREE_FORM_UNGOVERNED_NARRATIVE: 0,
      UNSUPPORTED_CAUSAL_CLAIMS: causalCheck.ok ? 0 : causalCheck.hits.length,
      VAGUE_UNEXPLAINED_PHRASES: 0,
      MARKETING_STYLE_LANGUAGE: 0,
    },
  };
}
