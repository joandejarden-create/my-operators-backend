/**
 * AI Consideration Index research engine — RESEARCH ONLY.
 * Customer ACI remains BLOCKED. Do not import from owner UI.
 *
 * Recommended V1:
 *   Actual share = mean fractional share among subject + CORE hotels
 *     appearing in the same observation (Method B).
 *   Expected share = 1 / (1 + CORE_COUNT)  [Model C: territory-stable CORE]
 *   Observation with neither subject nor CORE present: EXCLUDE_FROM_SHARE_DENOMINATOR
 *   Observation with CORE present and subject absent: actual share = 0
 *   Rank-weighting is NOT used.
 */

import { PROVIDERS } from "../data-model.js";
import { filterComparableObservations } from "./grain-governance.js";
import { roundAdpPercent } from "../format-percent.js";
import { canonicalizeToEntityId } from "./south-florida-entity-registry.js";
import { BENCHMARK_SET_VERSION, MIN_CORE_COMPETITORS } from "./territory-core-contract.js";
import { territoryLabelForIntent } from "./intent-territory-labels.js";
import { ENTITY_RESOLUTION_VERSION } from "./south-florida-entity-registry.js";

export const ACI_RESEARCH_VERSION = "adp_aci_research_engine_v2";
export const ELIGIBILITY_VERSION = "adp_scenario_eligibility_v1";

const SUBJECT_ID = "__subject__";

export function observationFractionalShare(obs, coreIds) {
  const appearing = new Set();
  if (obs.mentioned) appearing.add(SUBJECT_ID);
  for (const name of obs.competitorsMentioned || []) {
    const id = canonicalizeToEntityId(name);
    if (id && coreIds.includes(id)) appearing.add(id);
  }
  if (appearing.size === 0) {
    return { include: false, share: null, reason: "EXCLUDE_FROM_SHARE_DENOMINATOR" };
  }
  if (!appearing.has(SUBJECT_ID)) {
    return { include: true, share: 0, appearing: appearing.size, reason: "SUBJECT_ABSENT_CORE_PRESENT" };
  }
  return {
    include: true,
    share: 1 / appearing.size,
    appearing: appearing.size,
    reason: appearing.size === 1 ? "SUBJECT_ONLY" : "MULTI_HOTEL_FRACTIONAL",
  };
}

export function expectedShareEqualFair(coreCount) {
  const n = Number(coreCount) + 1;
  if (n <= 1) return null;
  return 1 / n;
}

export function computeActualConsiderationShare(observations, coreIds) {
  const comparable = filterComparableObservations(observations);
  const included = [];
  let excluded = 0;
  for (const obs of comparable) {
    const row = observationFractionalShare(obs, coreIds);
    if (!row.include) {
      excluded += 1;
      continue;
    }
    included.push(row.share);
  }
  const actual = included.length ? included.reduce((a, b) => a + b, 0) / included.length : null;
  return {
    method: "METHOD_B_FRACTIONAL_SHARE_CORE_APPEARING",
    actualShare: actual,
    actualSharePct: actual == null ? null : roundAdpPercent(actual * 100),
    includedObservations: included.length,
    excludedNoCoreEvent: excluded,
    comparableObservations: comparable.length,
  };
}

export function computeTerritoryAci(observations, scenarios, intent, coreIds, options = {}) {
  const scenarioIds = new Set((scenarios || []).filter((s) => s.intent === intent).map((s) => s.scenarioId));
  let obs = filterComparableObservations(observations).filter((o) => scenarioIds.has(o.scenarioId));
  if (options.provider && options.provider !== "all") {
    obs = obs.filter((o) => o.provider === options.provider);
  }
  const actual = computeActualConsiderationShare(obs, coreIds);
  const expected = expectedShareEqualFair(coreIds.length);
  const researchAci =
    actual.actualShare != null && expected > 0 ? Math.round((actual.actualShare / expected) * 100) : null;
  return {
    intent,
    territory: territoryLabelForIntent(intent),
    providerScope: options.provider || "all",
    coreCount: coreIds.length,
    coreIds,
    actualShare: actual.actualShare,
    actualSharePct: actual.actualSharePct,
    expectedShare: expected,
    expectedSharePct: expected == null ? null : roundAdpPercent(expected * 100),
    researchAci,
    includedObservations: actual.includedObservations,
    excludedNoCoreEvent: actual.excludedNoCoreEvent,
    extreme: researchAci != null && (researchAci > 300 || researchAci < 25),
  };
}

export function providerScopedAci(observations, scenarios, intent, coreIds) {
  const all = computeTerritoryAci(observations, scenarios, intent, coreIds, { provider: "all" });
  const byProvider = {};
  for (const p of PROVIDERS) {
    const scoped = computeTerritoryAci(observations, scenarios, intent, coreIds, { provider: p });
    byProvider[p] = scoped.includedObservations ? scoped : { ...scoped, researchAci: null, note: "NO_CROSS_FILL" };
  }
  return { allProvidersDerivedFromObservations: all, byProvider, noCrossFill: true };
}

export function aciSensitivity(observations, scenarios, intent, coreIds) {
  const base = computeTerritoryAci(observations, scenarios, intent, coreIds);
  const plus = computeTerritoryAci(observations, scenarios, intent, [...coreIds, "__synthetic_extra__"]);
  const minus = coreIds.length > 1
    ? computeTerritoryAci(observations, scenarios, intent, coreIds.slice(0, -1))
    : base;
  const values = [base.researchAci, plus.researchAci, minus.researchAci].filter((v) => v != null);
  const range = values.length ? Math.max(...values) - Math.min(...values) : null;
  let classLabel = "HIGH";
  if (range == null) classLabel = "HIGH";
  else if (range <= 20) classLabel = "LOW";
  else if (range <= 40) classLabel = "MEDIUM";
  return {
    baseAci: base.researchAci,
    plusOneCoreAci: plus.researchAci,
    minusOneCoreAci: minus.researchAci,
    range,
    sensitivity: classLabel,
    coreToSecondaryWouldMatchMinusOne: true,
  };
}

export function recommendedShareMethodology() {
  return {
    recommendedMethod: "METHOD_B_FRACTIONAL_SHARE_AMONG_SUBJECT_PLUS_CORE_HOTELS_APPEARING",
    multiHotelResponseMethod: "1 / count of (subject if present + CORE hotels present in that observation)",
    noCoreHotelsPresentMethod: "EXCLUDE_FROM_SHARE_DENOMINATOR",
    subjectAbsentCorePresent: "ACTUAL_SHARE = 0",
    rankWeighted: false,
    expectedMethod: "MODEL_C_TERRITORY_STABLE_EQUAL_FAIR_SHARE",
    expectedFormula: "1 / (1 + CORE_COUNT)",
    coreOnly: true,
    secondaryInDenominator: 0,
    modelA: "Equal fair share among subject + CORE (same as Model C when CORE is territory-stable)",
    modelB: "Scenario-specific CORE would re-slice peers per scenario — rejected for V1 (unstable)",
    modelD: "Weighted eligibility — rejected for V1 (opaque)",
    parityInterpretationValid: "PARTIAL",
    parityNote:
      "100 = actual fractional share equals 1/(1+CORE). Valid only when CORE is genuinely substitutable and the observation is a competitive consideration event.",
  };
}

export function versionStamp() {
  return {
    aciResearchVersion: ACI_RESEARCH_VERSION,
    benchmarkSetVersion: BENCHMARK_SET_VERSION,
    entityResolutionVersion: ENTITY_RESOLUTION_VERSION,
    eligibilityVersion: ELIGIBILITY_VERSION,
    minCoreCompetitors: MIN_CORE_COMPETITORS,
  };
}
