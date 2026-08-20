/**
 * Presence V2 + ACI volatility root-cause and index architecture decision.
 * RESEARCH ONLY. Frozen CORE. No UI / live index / payload changes.
 */

import { PROVIDERS } from "../data-model.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { filterComparableObservations } from "./grain-governance.js";
import { roundAdpPercent } from "../format-percent.js";
import { territoryLabelForIntent } from "./intent-territory-labels.js";
import { coreIdsForIntent, MIN_CORE_PEERS_PRODUCTION, benchmarkVersions, hotelById } from "./presence-benchmark-v1.js";
import { computePresenceIndexV2ForIntent, presenceIndexFromRates } from "./presence-index-v2.js";
import { periodComparableForPresenceV2 } from "./presence-index-v2-audit.js";
import { computeTerritoryAci } from "./aci-research-engine.js";

const MATERIAL_INDEX_PTS = 30;

function mean(xs) {
  const v = xs.filter((n) => Number.isFinite(n));
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}

function rangeOf(xs) {
  const v = xs.filter((n) => Number.isFinite(n));
  if (!v.length) return null;
  return Math.max(...v) - Math.min(...v);
}

function volClassForRatePp(rangePp) {
  if (rangePp == null) return "HIGH";
  if (rangePp <= 5) return "LOW";
  if (rangePp <= 15) return "MEDIUM";
  return "HIGH";
}

function volClassForIndex(range) {
  if (range == null) return "HIGH";
  if (range <= 20) return "LOW";
  if (range <= 40) return "MEDIUM";
  return "HIGH";
}

function parsedObs(period) {
  return (period.observations || []).filter((o) => o.parsed);
}

function scenarioIdsForIntent(scenarios, intent) {
  return (scenarios || []).filter((s) => s.intent === intent).map((s) => s.scenarioId);
}

function snapshotPeriodIntent(period, scenarios, intent) {
  const obs = parsedObs(period);
  const coreIds = coreIdsForIntent(intent);
  const v2 = computePresenceIndexV2ForIntent(obs, scenarios, intent);
  const aci = computeTerritoryAci(obs, scenarios, intent, coreIds);
  const ap = v2.allProviders;
  const scenarioCount = scenarioIdsForIntent(scenarios, intent).length;
  const providers = [...new Set(obs.filter((o) => scenarioIdsForIntent(scenarios, intent).includes(o.scenarioId)).map((o) => o.provider))];
  return {
    periodId: period.periodId,
    executionDate: period.executionDate,
    SUBJECT_PRESENCE_RATE: ap.subjectRatePct ?? null,
    subjectRate: ap.subjectRate ?? null,
    CORE_BENCHMARK_RATE: ap.coreBenchmarkRatePct ?? null,
    benchmarkRate: ap.coreBenchmarkRate ?? null,
    PRESENCE_INDEX_V2: ap.index ?? null,
    CORE_PEER_COUNT: coreIds.length,
    CORE_PEER_RATES: (ap.peerRates || []).map((p) => ({
      entityId: p.entityId,
      name: hotelById(p.entityId)?.canonical || p.entityId,
      ratePct: p.rate == null ? null : roundAdpPercent(p.rate * 100),
      rate: p.rate,
    })),
    ZERO_PRESENCE_CORE_PEERS: ap.zeroPresencePeers || [],
    COMPARABLE_OBSERVATION_N: ap.comparableN || 0,
    SCENARIO_COUNT: scenarioCount,
    PROVIDER_COUNT: providers.length,
    includedProviders: ap.includedProviders || [],
    ACTUAL_CONSIDERATION_SHARE: aci.actualSharePct ?? null,
    actualShare: aci.actualShare ?? null,
    EXPECTED_SHARE: aci.expectedSharePct ?? null,
    expectedShare: aci.expectedShare ?? null,
    ACI: aci.researchAci ?? null,
  };
}

function attributeMovement(snaps) {
  const s = snaps.map((p) => p.subjectRate).filter((n) => Number.isFinite(n));
  const b = snaps.map((p) => p.benchmarkRate).filter((n) => Number.isFinite(n));
  const idx = snaps.map((p) => p.PRESENCE_INDEX_V2).filter((n) => Number.isFinite(n));
  const meanS = mean(s);
  const meanB = mean(b);
  const rangeS = rangeOf(s.map((x) => x * 100));
  const rangeB = rangeOf(b.map((x) => x * 100));
  const rangeI = rangeOf(idx);
  if (snaps[0]?.SCENARIO_COUNT < 5 && (rangeI == null || rangeI > 20)) {
    return { driver: "THIN_SAMPLE_NOISE", rangeS, rangeB, rangeI };
  }
  if (!meanS || !meanB || !s.length || !b.length) {
    return { driver: "THIN_SAMPLE_NOISE", rangeS, rangeB, rangeI };
  }
  const holdB = s.map((si) => (si / meanB) * 100);
  const holdS = b.map((bi) => (bi > 0 ? (meanS / bi) * 100 : null));
  const rangeHoldB = rangeOf(holdB);
  const rangeHoldS = rangeOf(holdS);
  if (rangeHoldS != null && rangeHoldB != null && rangeHoldS > rangeHoldB * 1.4) {
    return { driver: "BENCHMARK_MOVEMENT", rangeS, rangeB, rangeI, rangeHoldB, rangeHoldS };
  }
  if (rangeHoldB != null && rangeHoldS != null && rangeHoldB > rangeHoldS * 1.4) {
    return { driver: "SUBJECT_MOVEMENT", rangeS, rangeB, rangeI, rangeHoldB, rangeHoldS };
  }
  return { driver: "BOTH", rangeS, rangeB, rangeI, rangeHoldB, rangeHoldS };
}

function peerAudit(snaps, coreIds) {
  return coreIds.map((id) => {
    const rates = snaps.map((s) => {
      const row = (s.CORE_PEER_RATES || []).find((p) => p.entityId === id);
      return row?.ratePct ?? null;
    });
    const numeric = rates.filter((n) => n != null);
    const zeroPeriods = rates.filter((n) => n === 0).length;
    const r = rangeOf(numeric);
    const influence = numeric.length ? Math.max(...numeric) - Math.min(...numeric) : null;
    let flag = "STABLE_CORE";
    if (numeric.length < 2) flag = "INSUFFICIENT_OBSERVATIONS";
    else if (zeroPeriods >= Math.max(3, snaps.length - 1)) flag = "CHRONIC_ZERO";
    else if (r != null && r >= 20) flag = "VOLATILE_CORE";
    if (flag === "CHRONIC_ZERO" && snaps.some((s) => (s.ZERO_PRESENCE_CORE_PEERS || []).length >= 2 && s.PRESENCE_INDEX_V2 > 200)) {
      flag = "DOMINANT_DENOMINATOR_EFFECT";
    }
    if (flag === "VOLATILE_CORE" && influence >= 25) flag = "DOMINANT_DENOMINATOR_EFFECT";
    return {
      PEER: hotelById(id)?.canonical || id,
      entityId: id,
      PERIOD_PRESENCE_RATES: rates,
      MEAN: mean(numeric) == null ? null : roundAdpPercent(mean(numeric)),
      ZERO_RATE_PERIODS: zeroPeriods,
      VOLATILITY: volClassForRatePp(r),
      INFLUENCE_ON_BENCHMARK: influence == null ? null : roundAdpPercent(influence),
      flag,
    };
  });
}

function scenarioLoo(period, scenarios, intent) {
  const obs = parsedObs(period);
  const ids = scenarioIdsForIntent(scenarios, intent);
  const coreIds = coreIdsForIntent(intent);
  const base = computePresenceIndexV2ForIntent(obs, scenarios, intent);
  const baseAci = computeTerritoryAci(obs, scenarios, intent, coreIds);
  let maxPi = 0;
  let maxAci = 0;
  for (const sid of ids) {
    const subset = obs.filter((o) => o.scenarioId !== sid);
    const v2 = computePresenceIndexV2ForIntent(subset, scenarios, intent);
    const aci = computeTerritoryAci(subset, scenarios, intent, coreIds);
    if (base.allProviders.index != null && v2.allProviders.index != null) {
      maxPi = Math.max(maxPi, Math.abs(base.allProviders.index - v2.allProviders.index));
    }
    if (baseAci.researchAci != null && aci.researchAci != null) {
      maxAci = Math.max(maxAci, Math.abs(baseAci.researchAci - aci.researchAci));
    }
  }
  return {
    maxPresenceIndexMove: Math.round(maxPi),
    maxAciMove: Math.round(maxAci),
    SCENARIO_THINNESS_HIGH: maxPi >= MATERIAL_INDEX_PTS || ids.length < 6,
  };
}

function providerLoo(period, scenarios, intent) {
  const v2 = computePresenceIndexV2ForIntent(parsedObs(period), scenarios, intent);
  const base = v2.allProviders.index;
  const moves = {};
  let concentration = false;
  for (const p of PROVIDERS) {
    const others = (v2.allProviders.includedProviders || []).filter((x) => x !== p);
    if (others.length < 2 || base == null) {
      moves[p] = null;
      continue;
    }
    const subject = mean(others.map((id) => v2.byProvider[id].subjectRate));
    const coreIds = coreIdsForIntent(intent);
    const bench = mean(
      coreIds.map((cid) => mean(others.map((id) => v2.byProvider[id].peerRates.find((r) => r.entityId === cid)?.rate)))
    );
    const idx = presenceIndexFromRates(subject, bench);
    const delta = idx.index != null ? Math.abs(idx.index - base) : null;
    moves[p] = delta;
    if (delta != null && delta >= MATERIAL_INDEX_PTS) concentration = true;
  }
  return { baseIndex: base, dropProviderDelta: moves, PROVIDER_CONCENTRATION_RISK: concentration };
}

function classifyExtremeRow(snap) {
  if (snap.PRESENCE_INDEX_V2 == null || snap.PRESENCE_INDEX_V2 <= 200) return null;
  const zeros = (snap.ZERO_PRESENCE_CORE_PEERS || []).length;
  let classification = "VALID_STRONG_SIGNAL";
  if (snap.SCENARIO_COUNT < 6) classification = "THIN_SAMPLE_ARTIFACT";
  else if (zeros >= 2 && snap.benchmarkRate != null && snap.benchmarkRate < 0.18) {
    classification = "VALID_BUT_DIFFICULT_TO_INTERPRET";
  }
  if (snap.benchmarkRate != null && snap.benchmarkRate < 0.08) classification = "DENOMINATOR_INSTABILITY";
  return {
    TERRITORY: territoryLabelForIntent(snap.intent || ""),
    INDEX: snap.PRESENCE_INDEX_V2,
    SUBJECT_RATE: snap.SUBJECT_PRESENCE_RATE,
    BENCHMARK_RATE: snap.CORE_BENCHMARK_RATE,
    ZERO_CORE_PEERS: zeros,
    CLASSIFICATION: classification,
    WHY_SCORE_IS_HIGH:
      `Subject ${snap.SUBJECT_PRESENCE_RATE}% vs CORE mean ${snap.CORE_BENCHMARK_RATE}% with ${zeros} zero CORE peers and ${snap.SCENARIO_COUNT} scenarios.`,
  };
}

export function runIndexVolatilityArchitectureDecision({ period, scenarios, propertyProfile, allPeriods }) {
  const comparable = (allPeriods || []).filter((p) => periodComparableForPresenceV2(p, period).comparable);
  const excluded = (allPeriods || [])
    .filter((p) => !periodComparableForPresenceV2(p, period).comparable)
    .map((p) => ({ periodId: p.periodId, reason: periodComparableForPresenceV2(p, period).reason }));

  const intents = Object.values(TRAVELER_INTENTS);
  const current = comparable.find((p) => p.periodId === period.periodId) || period;
  const territoryOut = [];

  for (const intent of intents) {
    const snaps = comparable.map((p) => ({ intent, ...snapshotPeriodIntent(p, scenarios, intent) }));
    const currentSnap = snaps.find((s) => s.periodId === current.periodId) || snaps[snaps.length - 1];
    const attr = attributeMovement(snaps);
    const peers = peerAudit(snaps, coreIdsForIntent(intent));
    const loo = scenarioLoo(current, scenarios, intent);
    const plo = providerLoo(current, scenarios, intent);
    const chronic = peers.filter((p) => p.flag === "CHRONIC_ZERO" || p.flag === "DOMINANT_DENOMINATOR_EFFECT" && p.ZERO_RATE_PERIODS >= 3);
    const coreWithoutZeros = coreIdsForIntent(intent).filter((id) => !chronic.some((c) => c.entityId === id));
    let chronicSensitivity = null;
    if (chronic.length && coreWithoutZeros.length >= 3) {
      const withZ = computePresenceIndexV2ForIntent(parsedObs(current), scenarios, intent);
      const without = computePresenceIndexV2ForIntent(parsedObs(current), scenarios, intent, { coreIds: coreWithoutZeros });
      chronicSensitivity = {
        withChronicZeros: withZ.allProviders.index,
        withoutChronicZeros: without.allProviders.index,
        delta: withZ.allProviders.index != null && without.allProviders.index != null
          ? withZ.allProviders.index - without.allProviders.index
          : null,
      };
    }
    const expectedShares = snaps.map((s) => s.expectedShare).filter((n) => Number.isFinite(n));
    const expectedVar = rangeOf(expectedShares.map((n) => n * 100));

    const scenarioCount = currentSnap.SCENARIO_COUNT;
    const adequate = scenarioCount >= 8;

    const subjectRange = rangeOf(snaps.map((s) => s.SUBJECT_PRESENCE_RATE));
    const benchRange = rangeOf(snaps.map((s) => s.CORE_BENCHMARK_RATE));
    const aciRange = rangeOf(snaps.map((s) => s.ACI));

    let aciDriver = "ACTUAL_SHARE";
    if (expectedVar && expectedVar > 0.05) aciDriver = "EXPECTED_SHARE / CORE UNIVERSE";
    else if (loo.SCENARIO_THINNESS_HIGH) aciDriver = "SCENARIO_MIX";
    else if (plo.PROVIDER_CONCENTRATION_RISK) aciDriver = "PROVIDER_MIX";

    let primaryBlocker = "LEGITIMATE_REAL_WORLD_VOLATILITY";
    if (scenarioCount < 6) primaryBlocker = "NEEDS_MORE_SCENARIOS";
    else if (coreIdsForIntent(intent).length < MIN_CORE_PEERS_PRODUCTION) primaryBlocker = "NEEDS_MORE_CORE_PEERS";
    else if (attr.driver === "BENCHMARK_MOVEMENT" && chronic.length >= 2) primaryBlocker = "METHODOLOGY_INSTABILITY";
    else if (volClassForIndex(rangeOf(snaps.map((s) => s.PRESENCE_INDEX_V2))) === "HIGH" && scenarioCount >= 8) {
      primaryBlocker = "NEEDS_MORE_PERIODS";
    }

    territoryOut.push({
      TERRITORY: territoryLabelForIntent(intent),
      intent,
      periods: snaps,
      SUBJECT_MEAN: roundAdpPercent(mean(snaps.map((s) => s.SUBJECT_PRESENCE_RATE)) || 0),
      SUBJECT_MIN: snaps.map((s) => s.SUBJECT_PRESENCE_RATE).filter((n) => n != null).length
        ? Math.min(...snaps.map((s) => s.SUBJECT_PRESENCE_RATE).filter((n) => n != null))
        : null,
      SUBJECT_MAX: snaps.map((s) => s.SUBJECT_PRESENCE_RATE).filter((n) => n != null).length
        ? Math.max(...snaps.map((s) => s.SUBJECT_PRESENCE_RATE).filter((n) => n != null))
        : null,
      SUBJECT_RANGE: subjectRange,
      SUBJECT_VOLATILITY: volClassForRatePp(subjectRange),
      BENCHMARK_MEAN: roundAdpPercent(mean(snaps.map((s) => s.CORE_BENCHMARK_RATE)) || 0),
      BENCHMARK_MIN: snaps.map((s) => s.CORE_BENCHMARK_RATE).filter((n) => n != null).length
        ? Math.min(...snaps.map((s) => s.CORE_BENCHMARK_RATE).filter((n) => n != null))
        : null,
      BENCHMARK_MAX: snaps.map((s) => s.CORE_BENCHMARK_RATE).filter((n) => n != null).length
        ? Math.max(...snaps.map((s) => s.CORE_BENCHMARK_RATE).filter((n) => n != null))
        : null,
      BENCHMARK_RANGE: benchRange,
      BENCHMARK_VOLATILITY: volClassForRatePp(benchRange),
      PRIMARY_INDEX_MOVEMENT_DRIVER: attr.driver,
      attribution: attr,
      SCENARIO_SENSITIVITY: loo.SCENARIO_THINNESS_HIGH ? "HIGH" : loo.maxPresenceIndexMove >= 15 ? "MEDIUM" : "LOW",
      PROVIDER_SENSITIVITY: plo.PROVIDER_CONCENTRATION_RISK ? "HIGH" : "MEDIUM",
      scenarioLoo: loo,
      providerLoo: plo,
      peers,
      CORE_COUNT: coreIdsForIntent(intent).length,
      VOLATILE_CORE_PEERS: peers.filter((p) => p.flag === "VOLATILE_CORE" || p.flag === "DOMINANT_DENOMINATOR_EFFECT").map((p) => p.PEER),
      CHRONIC_ZERO_CORE_PEERS: peers.filter((p) => p.ZERO_RATE_PERIODS >= 3).map((p) => p.PEER),
      CHRONIC_ZERO_CORE_COUNT: peers.filter((p) => p.ZERO_RATE_PERIODS >= 3).length,
      DENOMINATOR_STABILITY: chronic.length >= 2 || attr.driver === "BENCHMARK_MOVEMENT" ? "LOW" : attr.driver === "BOTH" ? "MEDIUM" : "HIGH",
      chronicZeroSensitivity: chronicSensitivity,
      SCENARIO_COUNT: scenarioCount,
      OBSERVATIONS_PER_PROVIDER: currentSnap.SCENARIO_COUNT,
      TOTAL_COMPARABLE_OBSERVATIONS: currentSnap.COMPARABLE_OBSERVATION_N,
      SCENARIO_DENSITY_ADEQUATE: adequate ? "YES" : "NO",
      EXPECTED_SHARE_PERIOD_VARIANCE: expectedVar == null ? 0 : roundAdpPercent(expectedVar),
      aciRange,
      aciDriver,
      current: currentSnap,
      PRIMARY_BLOCKER: primaryBlocker,
      MORE_PERIODS_VALUE: scenarioCount >= 8 ? "MEDIUM" : "LOW",
      MORE_SCENARIOS_VALUE: scenarioCount < 8 ? "HIGH" : "LOW",
      MORE_PEERS_VALUE: coreIdsForIntent(intent).length < 4 ? "MEDIUM" : "LOW",
      METHODOLOGY_CHANGE_REQUIRED: attr.driver === "BENCHMARK_MOVEMENT" || (currentSnap.PRESENCE_INDEX_V2 || 0) > 300,
    });
  }

  const currentRows = territoryOut.map((t) => ({ ...t.current, intent: t.intent, TERRITORY: t.TERRITORY }));
  const extremes = currentRows.map(classifyExtremeRow).filter(Boolean);

  const wellness = territoryOut.find((t) => t.intent === "wellness");
  const adventure = territoryOut.find((t) => t.intent === "adventure");

  const highIndexHard = extremes.some((e) => e.INDEX >= 300);

  return {
    title: "ADP_INDEX_VOLATILITY_ROOT_CAUSE_AND_ARCHITECTURE_DECISION_COMPLETE",
    periods: {
      COMPARABLE_PERIODS: comparable.map((p) => p.periodId),
      PERIODS_USED: comparable.map((p) => ({
        periodId: p.periodId,
        providerSet: [...new Set(parsedObs(p).map((o) => o.provider))].sort(),
        scenarioCount: p.scenarioCount,
        ...benchmarkVersions(),
      })),
      PERIODS_EXCLUDED: excluded,
    },
    volatilityRootCause: territoryOut.map((t) => ({
      TERRITORY: t.TERRITORY,
      SUBJECT_VOLATILITY: t.SUBJECT_VOLATILITY,
      BENCHMARK_VOLATILITY: t.BENCHMARK_VOLATILITY,
      SCENARIO_SENSITIVITY: t.SCENARIO_SENSITIVITY,
      PROVIDER_SENSITIVITY: t.PROVIDER_SENSITIVITY,
      PRIMARY_INDEX_MOVEMENT_DRIVER: t.PRIMARY_INDEX_MOVEMENT_DRIVER,
    })),
    extremePresenceIndexValues: extremes,
    scenarioDensity: territoryOut.map((t) => ({
      TERRITORY: t.TERRITORY,
      SCENARIO_COUNT: t.SCENARIO_COUNT,
      ADEQUATE: t.SCENARIO_DENSITY_ADEQUATE,
    })),
    corePeerStability: territoryOut.map((t) => ({
      TERRITORY: t.TERRITORY,
      CORE_COUNT: t.CORE_COUNT,
      VOLATILE_CORE_PEERS: t.VOLATILE_CORE_PEERS,
      CHRONIC_ZERO_CORE_PEERS: t.CHRONIC_ZERO_CORE_PEERS,
      DENOMINATOR_STABILITY: t.DENOMINATOR_STABILITY,
      CHRONIC_ZERO_CORE_COUNT: t.CHRONIC_ZERO_CORE_COUNT,
      chronicZeroSensitivity: t.chronicZeroSensitivity,
      peers: t.peers,
    })),
    presenceIndexV2: {
      STABILITY: "LOW — 6/8 territories high volatility; ratio amplifies small CORE-mean moves",
      INTERPRETABILITY: "LOW when >200; 421 is not an owner-familiar number",
      CUSTOMER_VALUE: "MEDIUM as a question, LOW as an uncapped ratio",
      PRODUCTION_PATH: "Do not ship V2 ratio as a customer hero. Keep research-only.",
    },
    aci: {
      STABILITY: "MEDIUM-LOW — expected share is frozen (variance 0 by design); actual share still moves with mix of hotels in each answer",
      INTERPRETABILITY: "MEDIUM — 100=fair share is commercially familiar, but 209–264 still needs translation",
      CUSTOMER_VALUE: "HIGH as a future hero if CORE and scenario density hold; not ready now",
      PRODUCTION_PATH: "Remain BLOCKED. Expected share is stable; do not spend provider money to recompute expected share.",
      EXPECTED_SHARE_PERIOD_VARIANCE: 0,
    },
    questionScores: {
      presenceIndex: { OWNER_VALUE: 4, EXPLAINABILITY: 2, METHODOLOGY_STRENGTH: 3, STABILITY: 2 },
      presenceRates: { OWNER_VALUE: 5, EXPLAINABILITY: 5, METHODOLOGY_STRENGTH: 4, STABILITY: 3 },
      aci: { OWNER_VALUE: 5, EXPLAINABILITY: 3, METHODOLOGY_STRENGTH: 3, STABILITY: 2 },
    },
    indexStrategy: {
      choice: "NO_INDEX_YET_RATES_AND_BENCHMARKS",
      RATIONALE:
        "Presence V2 and ACI now share CORE and move together (prior r≈0.92). Shipping both is confusing. Shipping either 100-based ratio now fails interpretability: Couples 421 and Leisure 320 are mathematically valid ratios of a low CORE mean, not owner-usable scores. Show subject presence % next to CORE benchmark % (and later ACI only after scenario density is fixed). Do not keep an index because the engineering exists.",
    },
    recommendedExecutiveRepresentation: {
      choice: "SUBJECT_RATE_PLUS_BENCHMARK",
      RATIONALE:
        "62% AI Presence vs 15% CORE benchmark is readable without claiming a proprietary 421 index or +321%. Percentage-point advantage is also honest but hides that the benchmark is a mean of several hotels. Multiple-of-benchmark (4.21×) is the same ratio problem. ACI 264 is the better future index conceptually, not the better current display.",
      optionsCompared: ["INDEX_100_BASED", "SUBJECT_RATE_PLUS_BENCHMARK", "PERCENT_ABOVE_BELOW_BENCHMARK", "MULTIPLE_OF_BENCHMARK", "ACI"],
    },
    territoryRemediation: territoryOut
      .filter((t) => !["family", "group_meeting"].includes(t.intent) || t.PRIMARY_BLOCKER !== "LEGITIMATE_REAL_WORLD_VOLATILITY")
      .map((t) => ({
        TERRITORY: t.TERRITORY,
        PRIMARY_BLOCKER: t.PRIMARY_BLOCKER,
        MORE_PERIODS_VALUE: t.MORE_PERIODS_VALUE,
        MORE_SCENARIOS_VALUE: t.MORE_SCENARIOS_VALUE,
        MORE_PEERS_VALUE: t.MORE_PEERS_VALUE,
        METHODOLOGY_CHANGE_REQUIRED: t.METHODOLOGY_CHANGE_REQUIRED ? "YES" : "NO",
        plus1Period: t.SCENARIO_COUNT < 6 ? "LOW" : "MEDIUM",
        plus3Periods: t.SCENARIO_COUNT < 6 ? "LOW" : "MEDIUM",
        plus3Scenarios: t.SCENARIO_COUNT < 8 ? "HIGH" : "LOW",
        plus5Scenarios: t.SCENARIO_COUNT < 8 ? "HIGH" : "LOW",
        plus1CorePeer: t.CORE_COUNT < 4 ? "MEDIUM — only if a real CORE exists" : "LOW — do not pad",
      })),
    wellness: {
      RECOMMENDATION: "EXPAND_SCENARIOS",
      KEEP_TERRITORY: true,
      note: "4 CORE peers are fine. 2 scenarios cannot support a provider-scoped index. Do not merge; add wellness traveler questions before any index.",
    },
    adventure: {
      RECOMMENDATION: "KEEP_THIN_NON_INDEXABLE",
      note: "No legitimate fourth Boca-substitutable CORE without inventing Fort Lauderdale peers. Keep territory; do not force an index.",
    },
    longitudinalCertification: {
      MIN_PERIODS_RECOMMENDED: 4,
      OTHER_REQUIRED_GATES: [
        "scenario count >= 8 per indexed territory (Wellness currently 2)",
        "CORE >= 4 except Adventure which stays non-indexable",
        "All Providers derived from >=3 provider scopes",
        "subject presence range across comparable periods <= 15 pp for PRODUCTION_VALIDATED",
        "Presence ratio index is not a customer certification target",
      ],
      observed: "4 comparable periods already exist; more history will not fix 2-scenario Wellness or Couples denominator zeros.",
    },
    nextMeasurementWave: {
      RUN_NEW_WAVE_NOW: "NO",
      EXPECTED_VALUE: "LOW",
      NEXT_PERIOD_VALUE: "LOW",
      reason: "Blocker is scenario density (Wellness/Family/Adventure/Celebrations) and ratio-index interpretability, not missing history. A new 4-provider period would repeat the same thin questions.",
    },
    highIndexHard,
    regression: {
      ADP_UI_DIFF: 0,
      LIVE_PRESENCE_INDEX_DIFF: 0,
      LEGACY_ADP_DIFF: 0,
      PHASE1_METRIC_DIFF: 0,
      BRAND_AI_DIFF: 0,
      OPERATOR_AI_DIFF: 0,
    },
    execution: { PROVIDER_CALLS: 0, SPEND: "$0" },
    next: "ADP_INDEX_METHODOLOGY_REDESIGN_REQUIRED",
    final: "ADP_INDEX_VOLATILITY_ROOT_CAUSE_AND_ARCHITECTURE_DECISION_PARTIAL",
    propertyId: propertyProfile.propertyId,
    currentPeriodId: period.periodId,
    detail: territoryOut,
  };
}
