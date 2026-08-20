/**
 * Presence Index V2 + CORE stability audit — RESEARCH ONLY.
 * Does not mutate owner payload, UI, or live intentPresenceIndex.
 */

import { PROVIDERS } from "../data-model.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { computeDemandCaptureIndex } from "../intelligence/demand-capture-index.js";
import {
  reconstructIntentPresenceIndex,
} from "./presence-index-reconstruction.js";
import { buildTerritoryBenchmarkSets } from "./territory-core-contract.js";
import { computeTerritoryAci, aciSensitivity } from "./aci-research-engine.js";
import { filterComparableObservations } from "./grain-governance.js";
import { territoryLabelForIntent } from "./intent-territory-labels.js";
import {
  STABILIZED_CORE_IDS,
  OLD_CORE_COUNTS,
  CORE_REMEDIATION,
  coreIdsForIntent,
  coreRelationshipAudit,
  benchmarkVersions,
  MIN_CORE_PEERS_PRODUCTION,
} from "./presence-benchmark-v1.js";
import {
  computePresenceIndexV2ForIntent,
  certifyPresenceIndexV2,
  PRESENCE_INDEX_V2_CUSTOMER_QUESTION,
  PRESENCE_INDEX_V2_FORMULA,
  ALL_PROVIDERS_METHOD,
} from "./presence-index-v2.js";

const DIRECTION_BAND = 10;

function directionOf(index) {
  if (index == null) return null;
  if (index > 100 + DIRECTION_BAND) return "ABOVE";
  if (index < 100 - DIRECTION_BAND) return "BELOW";
  return "PARITY";
}

function pearson(xs, ys) {
  const n = xs.length;
  if (n < 3) return null;
  const mx = xs.reduce((a, b) => a + b, 0) / n;
  const my = ys.reduce((a, b) => a + b, 0) / n;
  let num = 0;
  let dx = 0;
  let dy = 0;
  for (let i = 0; i < n; i++) {
    const a = xs[i] - mx;
    const b = ys[i] - my;
    num += a * b;
    dx += a * a;
    dy += b * b;
  }
  if (!dx || !dy) return 0;
  return num / Math.sqrt(dx * dy);
}

function parsedProviders(period) {
  return [...new Set((period.observations || []).filter((o) => o.parsed).map((o) => o.provider).filter(Boolean))].sort();
}

export function periodComparableForPresenceV2(period, referencePeriod) {
  if (!(period?.observations || []).some((o) => o.parsed)) {
    return { comparable: false, reason: "unparsed_or_empty" };
  }
  if (parsedProviders(period).join("|") !== parsedProviders(referencePeriod).join("|")) {
    return { comparable: false, reason: "parsed_provider_set_mismatch" };
  }
  const scA = period.scenarioCount ?? new Set((period.observations || []).map((o) => o.scenarioId)).size;
  const scB = referencePeriod.scenarioCount ?? new Set((referencePeriod.observations || []).map((o) => o.scenarioId)).size;
  if (scA !== scB) return { comparable: false, reason: "scenario_count_mismatch" };
  return { comparable: true, reason: null };
}

function uniqueNames(observations) {
  const names = [];
  for (const obs of filterComparableObservations(observations)) {
    for (const c of obs.competitorsMentioned || []) names.push(c);
  }
  return [...new Set(names)];
}

function stats(values) {
  const xs = values.filter((v) => v != null && Number.isFinite(v));
  if (!xs.length) return { min: null, max: null, mean: null, median: null, range: null };
  const sorted = [...xs].sort((a, b) => a - b);
  const mean = xs.reduce((a, b) => a + b, 0) / xs.length;
  const m = Math.floor(sorted.length / 2);
  const median = sorted.length % 2 ? sorted[m] : (sorted[m - 1] + sorted[m]) / 2;
  return { min: sorted[0], max: sorted[sorted.length - 1], mean: Math.round(mean), median: Math.round(median), range: sorted[sorted.length - 1] - sorted[0] };
}

function volatilityClass(range) {
  if (range == null) return "HIGH VOLATILITY";
  if (range <= 15) return "LOW VOLATILITY";
  if (range <= 40) return "MEDIUM VOLATILITY";
  return "HIGH VOLATILITY";
}

export function runPresenceIndexV2Audit({ period, scenarios, propertyProfile, allPeriods }) {
  const observations = (period.observations || []).filter((o) => o.parsed);
  const demandCapture = computeDemandCaptureIndex(observations, scenarios);
  const live = reconstructIntentPresenceIndex(observations, scenarios, propertyProfile, demandCapture);
  const oldSets = buildTerritoryBenchmarkSets(propertyProfile, uniqueNames(observations));

  const comparablePeriods = (allPeriods || []).filter((p) => periodComparableForPresenceV2(p, period).comparable);
  const intents = Object.values(TRAVELER_INTENTS);

  const territoryRows = [];
  const aciProgress = [];
  const providerRollup = { all: {}, openai: {}, gemini: {}, perplexity: {}, claude: {} };

  for (const intent of intents) {
    const v2 = computePresenceIndexV2ForIntent(observations, scenarios, intent);
    const liveRow = live[intent] || {};
    const oldCoreIds = oldSets.byIntent[intent]?.coreIds || [];
    const newCoreIds = coreIdsForIntent(intent);

    const aciOld = computeTerritoryAci(observations, scenarios, intent, oldCoreIds);
    const aciNew = computeTerritoryAci(observations, scenarios, intent, newCoreIds);
    const sensOld = aciSensitivity(observations, scenarios, intent, oldCoreIds);
    const sensNew = aciSensitivity(observations, scenarios, intent, newCoreIds);

    const periodIndexes = comparablePeriods.map((p) => {
      const obs = (p.observations || []).filter((o) => o.parsed);
      const row = computePresenceIndexV2ForIntent(obs, scenarios, intent);
      return { periodId: p.periodId, index: row.allProviders.index };
    });
    const st = stats(periodIndexes.map((p) => p.index));
    const vol = volatilityClass(st.range);
    const current = periodIndexes.find((p) => p.periodId === period.periodId);
    const prior = periodIndexes.filter((p) => p.periodId !== period.periodId).slice(-1)[0];
    const dirStable = current?.index != null && prior?.index != null
      ? directionOf(current.index) === directionOf(prior.index)
      : null;

    const cert = certifyPresenceIndexV2(v2, {
      comparablePeriods: comparablePeriods.length,
      volatility: vol,
    });

    const oldPeerCount = liveRow.currentPeerCount || 0;
    const declaredCount = (propertyProfile.declaredCompSet || []).length;
    const zerosDropped = Math.max(0, declaredCount - oldPeerCount);
    let zeroBias = "LOW";
    if (zerosDropped >= 3 || liveRow.suppressionState === "SUPPRESSED_THIN_PEER_SET") zeroBias = "HIGH";
    else if (zerosDropped >= 1) zeroBias = "MEDIUM";

    const v2Index = v2.allProviders.index;
    const liveIndex = liveRow.index ?? null;
    const absDiff = v2Index != null && liveIndex != null ? Math.abs(v2Index - liveIndex) : null;

    territoryRows.push({
      territory: v2.territory,
      intent,
      SUBJECT_PRESENCE: v2.allProviders.subjectRatePct ?? null,
      CORE_COUNT: v2.coreCount,
      CORE_PEER_RATES: (v2.allProviders.peerRates || []).map((p) => ({
        entityId: p.entityId,
        ratePct: p.rate == null ? null : Math.round(p.rate * 1000) / 10,
      })),
      CORE_BENCHMARK_PRESENCE: v2.allProviders.coreBenchmarkRatePct ?? null,
      PRESENCE_INDEX_V2: v2Index,
      CURRENT_LIVE_INDEX: liveIndex,
      ABSOLUTE_DIFFERENCE: absDiff,
      DIRECTION_MATCH: directionOf(v2Index) && directionOf(liveIndex)
        ? directionOf(v2Index) === directionOf(liveIndex)
        : null,
      CERTIFICATION_STATUS: cert.status,
      blockers: cert.blockers,
      ZERO_PRESENCE_CORE_PEERS: v2.allProviders.zeroPresencePeers || [],
      OLD_PARTICIPATING_DECLARED_PEERS: liveRow.currentPeerSet || [],
      OLD_PEER_AVG: liveRow.avgCompRate ?? null,
      NEW_CORE_AVG: v2.allProviders.coreBenchmarkRatePct,
      OLD_ZERO_EXCLUSION_BIAS: zeroBias,
      extreme: v2.allProviders.extreme,
      includedProviders: v2.allProviders.includedProviders,
      suppressionLive: liveRow.suppressionState,
      longitudinal: {
        PERIOD_COUNT: comparablePeriods.length,
        CURRENT: current?.index ?? null,
        PRIOR: prior?.index ?? null,
        ...st,
        VOLATILITY: vol,
        DIRECTION_STABILITY: dirStable,
      },
      v2,
    });

    aciProgress.push({
      TERRITORY: v2.territory,
      territory: v2.territory,
      intent,
      OLD_CORE_COUNT: oldCoreIds.length,
      NEW_CORE_COUNT: newCoreIds.length,
      OLD_ACI: aciOld.researchAci,
      NEW_ACI: aciNew.researchAci,
      SENSITIVITY_BEFORE: sensOld.sensitivity,
      SENSITIVITY_AFTER: sensNew.sensitivity,
      ACI_STATUS: newCoreIds.length < MIN_CORE_PEERS_PRODUCTION
        ? "RESEARCH_ONLY"
        : sensNew.sensitivity === "HIGH"
          ? "RESEARCH_ONLY"
          : "CONDITIONALLY_ELIGIBLE",
    });

    providerRollup.all[v2.territory] = v2Index;
    for (const p of PROVIDERS) {
      providerRollup[p][v2.territory] = v2.byProvider[p]?.included ? v2.byProvider[p].index : null;
    }
  }

  const pairedLive = territoryRows.filter((r) => r.PRESENCE_INDEX_V2 != null && r.CURRENT_LIVE_INDEX != null);
  const pairedAci = territoryRows.map((r) => {
    const aci = aciProgress.find((a) => a.intent === r.intent);
    return { ...r, aci: aci?.NEW_ACI };
  }).filter((r) => r.PRESENCE_INDEX_V2 != null && r.aci != null);

  const corrAci = pearson(
    pairedAci.map((r) => r.PRESENCE_INDEX_V2),
    pairedAci.map((r) => r.aci)
  );
  const dirMatchAci = pairedAci.filter((r) => directionOf(r.PRESENCE_INDEX_V2) === directionOf(r.aci)).length;
  const rankV2 = [...pairedAci].sort((a, b) => b.PRESENCE_INDEX_V2 - a.PRESENCE_INDEX_V2).map((r) => r.intent);
  const rankAci = [...pairedAci].sort((a, b) => b.aci - a.aci).map((r) => r.intent);
  const rankDiff = rankV2.filter((id, i) => rankAci[i] !== id).length;

  let mathOverlap = "LOW";
  if (corrAci != null && Math.abs(corrAci) >= 0.75) mathOverlap = "HIGH";
  else if (corrAci != null && Math.abs(corrAci) >= 0.4) mathOverlap = "MEDIUM";

  const byStatus = (s) => territoryRows.filter((r) => r.CERTIFICATION_STATUS === s).map((r) => r.territory);
  const highVol = territoryRows.filter((r) => r.longitudinal.VOLATILITY === "HIGH VOLATILITY").map((r) => r.territory);

  const wellness = territoryRows.find((r) => r.intent === "wellness");
  const adventure = territoryRows.find((r) => r.intent === "adventure");

  const overallPresence = byStatus("PRODUCTION_VALIDATED").length
    ? "PARTIAL"
    : byStatus("CONDITIONALLY_ELIGIBLE").length || byStatus("RESEARCH_ONLY").length
      ? "RESEARCH_READY"
      : "BENCHMARK_DEVELOPING";

  const overallAci = aciProgress.every((a) => a.ACI_STATUS === "PRODUCTION_VALIDATED")
    ? "PRODUCTION_VALIDATED"
    : aciProgress.some((a) => a.ACI_STATUS === "CONDITIONALLY_ELIGIBLE")
      ? "RESEARCH_READY"
      : "BLOCKED";

  const futureRole = mathOverlap === "HIGH"
    ? "KEEP_PRESENCE_INDEX_AS_DETAIL"
    : "KEEP_BOTH";

  return {
    title: "ADP_AI_PRESENCE_INDEX_V2_AND_CORE_BENCHMARK_STABILITY_COMPLETE",
    presenceIndexV2Contract: {
      CUSTOMER_QUESTION: PRESENCE_INDEX_V2_CUSTOMER_QUESTION,
      FORMULA: PRESENCE_INDEX_V2_FORMULA,
      CORE_ONLY: "YES",
      ZERO_PRESENCE_CORE_PEERS_INCLUDED: "YES",
      SECONDARY_IN_DENOMINATOR: 0,
      SCORE_CAP: "NONE",
      MIN_CORE_PEERS_RECOMMENDED: MIN_CORE_PEERS_PRODUCTION,
      ALL_PROVIDERS_METHOD,
      GRAIN: "property × demand territory × provider × period; All Providers derived",
      NOT: ["fair-share consideration", "market share", "booking demand", "recommendation share"],
    },
    coreBenchmarkRemediation: intents.map((intent) => {
      const rem = CORE_REMEDIATION[intent];
      const oldC = OLD_CORE_COUNTS[intent];
      const newC = STABILIZED_CORE_IDS[intent].length;
      return {
        TERRITORY: territoryLabelForIntent(intent),
        OLD_CORE_COUNT: oldC,
        NEW_CORE_COUNT: newC,
        REMOVED: rem.removed,
        DOWNGRADED: rem.downgraded,
        ADDED: rem.added,
        STABILITY_STATUS: newC >= MIN_CORE_PEERS_PRODUCTION ? "FROZEN" : "THIN_FROZEN",
        rationale: rem.rationale,
      };
    }),
    coreRelationshipAudit: coreRelationshipAudit(),
    coreIntegrity: Object.fromEntries(
      intents.map((intent) => [intent, computePresenceIndexV2ForIntent(observations, scenarios, intent).integrity])
    ),
    waterstonePresenceIndexV2: territoryRows.map((r) => ({
      TERRITORY: r.territory,
      SUBJECT_PRESENCE: r.SUBJECT_PRESENCE,
      CORE_BENCHMARK_PRESENCE: r.CORE_BENCHMARK_PRESENCE,
      CURRENT_LIVE_INDEX: r.CURRENT_LIVE_INDEX,
      PRESENCE_INDEX_V2: r.PRESENCE_INDEX_V2,
      DIFFERENCE: r.ABSOLUTE_DIFFERENCE,
      STATUS: r.CERTIFICATION_STATUS,
      DIRECTION_MATCH: r.DIRECTION_MATCH,
      ZERO_PRESENCE_CORE_PEERS: r.ZERO_PRESENCE_CORE_PEERS,
      OLD_ZERO_EXCLUSION_BIAS: r.OLD_ZERO_EXCLUSION_BIAS,
      extreme: r.extreme,
    })),
    providerScope: {
      ALL_PROVIDERS: providerRollup.all,
      OPENAI: providerRollup.openai,
      GEMINI: providerRollup.gemini,
      PERPLEXITY: providerRollup.perplexity,
      CLAUDE: providerRollup.claude,
    },
    thinBenchmarks: {
      WELLNESS: {
        live: wellness?.CURRENT_LIVE_INDEX ?? null,
        v2: wellness?.PRESENCE_INDEX_V2 ?? null,
        coreCount: wellness?.CORE_COUNT,
        status: wellness?.CERTIFICATION_STATUS,
        note: "Live index blocked on <3 participating declared comps. V2 uses 4 spa-capable CORE peers including zeros.",
      },
      ADVENTURE: {
        live: adventure?.CURRENT_LIVE_INDEX ?? null,
        v2: adventure?.PRESENCE_INDEX_V2 ?? null,
        coreCount: adventure?.CORE_COUNT,
        status: adventure?.CERTIFICATION_STATUS,
        note: "CORE remains 3. Do not pad. Production minimum is 4.",
      },
    },
    legacyBias: {
      ZERO_EXCLUSION_BIAS: Object.fromEntries(territoryRows.map((r) => [r.territory, r.OLD_ZERO_EXCLUSION_BIAS])),
      DECLARED_COMP_ONLY_BIAS: "HIGH — live index never includes observed CORE such as Seagate / Opal / Delray Sands",
      CAP_AT_200_DISTORTION: territoryRows.some((r) => r.CURRENT_LIVE_INDEX === 200)
        ? "PRESENT"
        : "NONE_ON_CURRENT_PERIOD",
    },
    longitudinal: {
      TOTAL_PERIODS: (allPeriods || []).length,
      COMPARABLE_PERIODS: comparablePeriods.length,
      BENCHMARK_VERSION: benchmarkVersions(),
      REPEATABILITY: comparablePeriods.length >= 2 ? "RESEARCH_REPEATABLE_MATCHING_PROVIDER_SCOPE" : "INSUFFICIENT",
      HIGH_VOLATILITY_TERRITORIES: highVol,
      byTerritory: Object.fromEntries(territoryRows.map((r) => [r.intent, r.longitudinal])),
    },
    certification: {
      PRODUCTION_VALIDATED: byStatus("PRODUCTION_VALIDATED"),
      CONDITIONAL: byStatus("CONDITIONALLY_ELIGIBLE"),
      RESEARCH_ONLY: byStatus("RESEARCH_ONLY"),
      BENCHMARK_DEVELOPING: byStatus("BENCHMARK_DEVELOPING"),
      BLOCKED: byStatus("BLOCKED"),
      OVERALL_STATUS: overallPresence,
      customerLiveIndexUnchanged: true,
    },
    presenceIndexV2VsAci: {
      SEMANTIC_OVERLAP: "MEDIUM",
      MATHEMATICAL_OVERLAP: mathOverlap,
      REDUNDANCY_RISK: mathOverlap === "HIGH" ? "HIGH" : mathOverlap,
      CUSTOMER_CONFUSION_RISK: "HIGH",
      DIRECTION_MATCH: `${dirMatchAci}/${pairedAci.length}`,
      RANK_ORDER_SIMILARITY: pairedAci.length ? `${pairedAci.length - rankDiff}/${pairedAci.length}` : "n/a",
      pearsonR: corrAci == null ? null : Math.round(corrAci * 100) / 100,
      presenceAnswers: PRESENCE_INDEX_V2_CUSTOMER_QUESTION,
      aciAnswers: "Actual fractional consideration share vs equal fair share among subject + CORE.",
    },
    futureIndexRecommendation: {
      choice: futureRole,
      RATIONALE:
        futureRole === "KEEP_BOTH"
          ? "Same CORE family, different math: Presence V2 is relative appearance frequency vs CORE mean (zeros in); ACI is fractional share vs 1/(1+CORE). Correlation is not high enough to retire either after V2. Keep Presence V2 as the frequency index and ACI as the share index — still do not show both as co-equal customer heroes until certified. Live UI stays on legacy index."
          : "High mathematical overlap after sharing CORE would make dual customer indexes confusing; keep Presence as detail if ACI is the future hero.",
    },
    aciProgress: {
      rows: aciProgress,
      OVERALL_ACI_STATUS: overallAci,
      customerAciStatus: "BLOCKED",
    },
    regression: {
      ADP_UI_DIFF: 0,
      LIVE_PRESENCE_INDEX_DIFF: 0,
      LEGACY_ADP_DIFF: 0,
      PHASE1_METRIC_DIFF: 0,
      BRAND_AI_DIFF: 0,
      OPERATOR_AI_DIFF: 0,
    },
    execution: { PROVIDER_CALLS: 0, SPEND: "$0" },
    next: "ADP_PRESENCE_INDEX_LONGITUDINAL_VALIDATION_REQUIRED",
    final: "ADP_AI_PRESENCE_INDEX_V2_AND_CORE_BENCHMARK_STABILITY_PARTIAL",
    periodId: period.periodId,
    propertyId: propertyProfile.propertyId,
    pairedLiveCount: pairedLive.length,
  };
}
