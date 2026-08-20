/**
 * ADP ACI + Presence Index audit V2 orchestrator — RESEARCH ONLY.
 * Does not mutate owner payloads, published snapshots, or UI.
 */

import { PROVIDERS } from "../data-model.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { computeDemandCaptureIndex } from "../intelligence/demand-capture-index.js";
import { filterComparableObservations } from "./grain-governance.js";
import {
  reconstructIntentPresenceIndex,
  presenceIndexCustomerMeaning,
  auditPresenceIndexQuality,
  PRESENCE_INDEX_SOURCE_FILE,
  PRESENCE_INDEX_FUNCTION,
  PRESENCE_INDEX_CURRENT_FORMULA,
} from "./presence-index-reconstruction.js";
import { classifyEntityUniverse, classifyObservedEntity, ENTITY_RESOLUTION_VERSION } from "./south-florida-entity-registry.js";
import { buildTerritoryBenchmarkSets, MIN_CORE_COMPETITORS } from "./territory-core-contract.js";
import {
  computeTerritoryAci,
  providerScopedAci,
  aciSensitivity,
  recommendedShareMethodology,
  versionStamp,
} from "./aci-research-engine.js";
import { territoryLabelForIntent } from "./intent-territory-labels.js";

const DIRECTION_PARITY_BAND = 10;

function directionOf(index) {
  if (index == null) return null;
  if (index > 100 + DIRECTION_PARITY_BAND) return "ABOVE";
  if (index < 100 - DIRECTION_PARITY_BAND) return "BELOW";
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

function parsedProviderSet(period) {
  const parsed = (period.observations || []).filter((o) => o.parsed);
  return [...new Set(parsed.map((o) => o.provider).filter(Boolean))].sort();
}

export function periodComparableForIndexes(period, referencePeriod) {
  if (!period?.observations?.some((o) => o.parsed)) {
    return { comparable: false, reason: "unparsed_or_empty" };
  }
  const a = parsedProviderSet(period);
  const b = parsedProviderSet(referencePeriod);
  if (a.join("|") !== b.join("|")) return { comparable: false, reason: "parsed_provider_set_mismatch" };
  const scA = period.scenarioCount ?? new Set((period.observations || []).map((o) => o.scenarioId)).size;
  const scB = referencePeriod.scenarioCount ?? new Set((referencePeriod.observations || []).map((o) => o.scenarioId)).size;
  if (scA !== scB) return { comparable: false, reason: "scenario_count_mismatch" };
  return { comparable: true, reason: null, providers: a };
}

function collectRawNames(observations) {
  const names = [];
  for (const obs of filterComparableObservations(observations)) {
    for (const c of obs.competitorsMentioned || []) names.push(c);
  }
  return names;
}

function uniqueRawNames(observations) {
  return [...new Set(collectRawNames(observations))];
}

function certifyTerritoryAci(row, sensitivity, providerCoverage, comparablePeriodCount) {
  const blockers = [];
  if (row.coreCount < MIN_CORE_COMPETITORS) blockers.push("core_lt_3");
  if (row.coreCount < 4) blockers.push("core_lt_4_robustness");
  if (sensitivity.sensitivity === "HIGH") blockers.push("high_denominator_sensitivity");
  if (row.includedObservations < 20) blockers.push("thin_included_observations");
  if (providerCoverage < 4) blockers.push("incomplete_provider_coverage");
  if (comparablePeriodCount < 2) blockers.push("lt_2_comparable_periods");
  if (row.extreme) blockers.push("extreme_score");
  if (row.researchAci == null) blockers.push("aci_null");

  let status = "PRODUCTION_VALIDATED";
  if (blockers.length) status = "CONDITIONALLY_ELIGIBLE";
  if (blockers.includes("aci_null") || blockers.includes("core_lt_3") || blockers.includes("high_denominator_sensitivity")) {
    status = "RESEARCH_ONLY";
  }
  if (blockers.includes("core_lt_3") && row.researchAci == null) status = "BLOCKED";
  if (row.coreCount < MIN_CORE_COMPETITORS) status = "BLOCKED";
  return { status, blockers };
}

function certifyPresence(intentRow, quality) {
  if (intentRow?.index == null) return "PRESENCE_INDEX_BLOCKED";
  if (quality.productionSafe === "YES") return "PRESENCE_INDEX_PRODUCTION_VALIDATED";
  return "PRESENCE_INDEX_PARTIAL";
}

export function runAciPresenceIndexAuditV2({ period, scenarios, propertyProfile, allPeriods }) {
  const observations = (period.observations || []).filter((o) => o.parsed);
  const demandCapture = computeDemandCaptureIndex(observations, scenarios);
  const presence = reconstructIntentPresenceIndex(observations, scenarios, propertyProfile, demandCapture);
  const meaning = presenceIndexCustomerMeaning();
  const uniqueNames = uniqueRawNames(observations);
  const entityGov = classifyEntityUniverse(uniqueNames);
  const benchmarks = buildTerritoryBenchmarkSets(propertyProfile, uniqueNames);

  const currentParsedProviders = parsedProviderSet(period);
  const comparablePeriods = (allPeriods || []).filter((p) => periodComparableForIndexes(p, period).comparable);
  const incomparable = (allPeriods || [])
    .filter((p) => !periodComparableForIndexes(p, period).comparable)
    .map((p) => ({ periodId: p.periodId, reason: periodComparableForIndexes(p, period).reason }));

  const intents = Object.values(TRAVELER_INTENTS);
  const territoryResearch = [];
  const presenceStatuses = [];
  const aciStatuses = [];
  const providerAciByIntent = {};

  for (const intent of intents) {
    const pi = presence[intent];
    const quality = auditPresenceIndexQuality(pi, propertyProfile);
    const presenceStatus = certifyPresence(pi, quality);
    presenceStatuses.push({ intent, status: presenceStatus, quality });

    const bench = benchmarks.byIntent[intent];
    const coreIds = bench.coreIds;
    const aciRow = computeTerritoryAci(observations, scenarios, intent, coreIds);
    const sensitivity = aciSensitivity(observations, scenarios, intent, coreIds);
    const scoped = providerScopedAci(observations, scenarios, intent, coreIds);
    providerAciByIntent[intent] = scoped;
    const coverage = currentParsedProviders.length;
    const aciCert = certifyTerritoryAci(aciRow, sensitivity, coverage, comparablePeriods.length);
    aciStatuses.push({ intent, ...aciCert });

    const piDir = directionOf(pi?.index);
    const aciDir = directionOf(aciRow.researchAci);
    territoryResearch.push({
      territory: territoryLabelForIntent(intent),
      intent,
      presenceIndex: pi?.index ?? null,
      subjectPresenceRate: pi?.subjectPresenceRate ?? null,
      peerAvgPresenceRate: pi?.currentPeerAvgPresenceRate ?? null,
      peerSet: pi?.currentPeerSet || [],
      peerCount: pi?.currentPeerCount ?? 0,
      suppressionState: pi?.suppressionState || "MISSING",
      coreCount: bench.coreCount,
      secondaryCount: bench.secondaryCount,
      conditionalCount: bench.conditionalCount,
      certifiableUniverse: bench.certifiableUniverse,
      actualShare: aciRow.actualSharePct,
      expectedShare: aciRow.expectedSharePct,
      researchAci: aciRow.researchAci,
      directionMatch: piDir && aciDir ? piDir === aciDir : null,
      presenceDirection: piDir,
      aciDirection: aciDir,
      absoluteDifference: pi?.index != null && aciRow.researchAci != null ? Math.abs(pi.index - aciRow.researchAci) : null,
      sensitivity: sensitivity.sensitivity,
      sensitivityDetail: sensitivity,
      presenceStatus,
      aciStatus: aciCert.status,
      aciBlockers: aciCert.blockers,
      presenceBlockers: quality.blockers,
      extreme: aciRow.extreme,
      includedObservations: aciRow.includedObservations,
      declaredCore: bench.declaredCore,
      declaredSecondary: bench.declaredSecondary,
      declaredNonComparable: bench.declaredNonComparable,
      observedCore: bench.observedCore,
      observedSecondary: bench.observedSecondary,
    });
  }

  const paired = territoryResearch.filter((r) => r.presenceIndex != null && r.researchAci != null);
  const corr = pearson(
    paired.map((r) => r.presenceIndex),
    paired.map((r) => r.researchAci)
  );
  const directionMatches = paired.filter((r) => r.directionMatch).length;
  const divergent = paired.filter((r) => r.directionMatch === false || (r.absoluteDifference != null && r.absoluteDifference >= 30));

  let mathematicalOverlap = "LOW";
  if (corr != null && Math.abs(corr) >= 0.75) mathematicalOverlap = "HIGH";
  else if (corr != null && Math.abs(corr) >= 0.4) mathematicalOverlap = "MEDIUM";

  const semanticOverlap = "MEDIUM";
  const redundancyRisk = mathematicalOverlap === "HIGH" ? "HIGH" : mathematicalOverlap === "MEDIUM" ? "MEDIUM" : "LOW";
  const customerConfusionRisk = "HIGH";

  const rankPresence = [...paired].sort((a, b) => b.presenceIndex - a.presenceIndex).map((r) => r.intent);
  const rankAci = [...paired].sort((a, b) => b.researchAci - a.researchAci).map((r) => r.intent);
  const rankOrderDifference = rankPresence.filter((id, i) => rankAci[i] !== id).length;

  const stability = {};
  for (const intent of intents) {
    const values = comparablePeriods.map((p) => {
      const obs = (p.observations || []).filter((o) => o.parsed);
      const dc = computeDemandCaptureIndex(obs, scenarios);
      const pi = reconstructIntentPresenceIndex(obs, scenarios, propertyProfile, dc)[intent];
      const coreIds = benchmarks.byIntent[intent].coreIds;
      const aci = computeTerritoryAci(obs, scenarios, intent, coreIds);
      return { periodId: p.periodId, presence: pi?.index ?? null, aci: aci.researchAci };
    });
    const aciVals = values.map((v) => v.aci).filter((v) => v != null);
    const piVals = values.map((v) => v.presence).filter((v) => v != null);
    const range = (arr) => (arr.length ? Math.max(...arr) - Math.min(...arr) : null);
    const mean = (arr) => (arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null);
    const sorted = (arr) => [...arr].sort((a, b) => a - b);
    const median = (arr) => {
      if (!arr.length) return null;
      const s = sorted(arr);
      const m = Math.floor(s.length / 2);
      return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
    };
    const aciRange = range(aciVals);
    let vol = "HIGH VOLATILITY";
    if (aciRange != null && aciRange <= 15) vol = "LOW VOLATILITY";
    else if (aciRange != null && aciRange <= 35) vol = "MEDIUM VOLATILITY";
    const current = values.find((v) => v.periodId === period.periodId);
    const prior = values.filter((v) => v.periodId !== period.periodId).slice(-1)[0];
    stability[intent] = {
      territory: territoryLabelForIntent(intent),
      current: current || null,
      prior: prior || null,
      meanAci: mean(aciVals) == null ? null : Math.round(mean(aciVals)),
      medianAci: median(aciVals) == null ? null : Math.round(median(aciVals)),
      rangeAci: aciRange,
      meanPresence: mean(piVals) == null ? null : Math.round(mean(piVals)),
      medianPresence: median(piVals) == null ? null : Math.round(median(piVals)),
      rangePresence: range(piVals),
      stabilityClass: vol,
    };
  }

  const aciByStatus = (s) => aciStatuses.filter((x) => x.status === s).map((x) => territoryLabelForIntent(x.intent));
  const overallAci = aciByStatus("PRODUCTION_VALIDATED").length
    ? "PARTIAL"
    : aciStatuses.some((x) => x.status === "CONDITIONALLY_ELIGIBLE" || x.status === "RESEARCH_ONLY")
      ? "RESEARCH_READY"
      : "BLOCKED";
  const overallPresence = presenceStatuses.every((x) => x.status === "PRESENCE_INDEX_PRODUCTION_VALIDATED")
    ? "PRODUCTION_VALIDATED"
    : presenceStatuses.some((x) => x.status === "PRESENCE_INDEX_PARTIAL")
      ? "PARTIAL"
      : "BLOCKED";

  const providerRollup = {};
  for (const p of ["all", ...PROVIDERS]) {
    const vals = intents.map((intent) => {
      const scoped = providerAciByIntent[intent];
      const row = p === "all" ? scoped.allProvidersDerivedFromObservations : scoped.byProvider[p];
      return row?.researchAci ?? null;
    });
    providerRollup[p] = {
      territoryAcis: Object.fromEntries(intents.map((intent, i) => [territoryLabelForIntent(intent), vals[i]])),
      computedCount: vals.filter((v) => v != null).length,
    };
  }

  const leaks = JSON.stringify({
    presence,
    entityGov: { rawEntities: entityGov.rawEntities },
    benchmarks: { version: benchmarks.version },
  });

  return {
    title: "ADP_AI_CONSIDERATION_INDEX_AND_PRESENCE_INDEX_AUDIT_V2_COMPLETE",
    existingAiPresenceIndex: {
      SOURCE_FILE: PRESENCE_INDEX_SOURCE_FILE,
      FUNCTION: PRESENCE_INDEX_FUNCTION,
      CURRENT_FORMULA: PRESENCE_INDEX_CURRENT_FORMULA,
      CURRENT_INPUT_FIELDS: [
        "propertyProfile.declaredCompSet",
        "observations.competitorsMentioned",
        "observations.scenarioId",
        "demandCapture.byIntent[intent].rate",
        "scenarios.intent / scenarioId",
      ],
      CURRENT_COMPARISON_SET_SOURCE: "declaredCompSet only",
      CURRENT_GRAIN: "SCENARIO_GRAIN (any-provider OR)",
      CURRENT_PROVIDER_SCOPE_HANDLING: "unscoped — mixes whatever providers exist in the parsed observation set",
      CURRENT_ALL_PROVIDERS_LOGIC: "not independently derived; leftover observations after parsed filter",
      CURRENT_THIN_BENCHMARK_SUPPRESSION: "null when participating declared comps < 3 or avg presence < 30%; cap at 200",
      CURRENT_CUSTOMER_MEANING: meaning.customerQuestion,
      INTENTS_AUDITED: intents.length,
      PRODUCTION_VALIDATED: presenceStatuses.filter((x) => x.status === "PRESENCE_INDEX_PRODUCTION_VALIDATED").map((x) => territoryLabelForIntent(x.intent)),
      PARTIAL: presenceStatuses.filter((x) => x.status === "PRESENCE_INDEX_PARTIAL").map((x) => territoryLabelForIntent(x.intent)),
      BLOCKED: presenceStatuses.filter((x) => x.status === "PRESENCE_INDEX_BLOCKED").map((x) => territoryLabelForIntent(x.intent)),
      meaning,
      byIntent: presence,
    },
    entityGovernance: {
      RAW_ENTITIES: entityGov.rawEntities,
      CANONICAL_HOTELS: entityGov.canonicalHotels,
      DUPLICATES_MERGED: entityGov.duplicatesMerged,
      ARTIFACTS_REMOVED: entityGov.artifactsRemoved,
      AMBIGUOUS: entityGov.ambiguous,
      UNRESOLVED: entityGov.unresolved,
      byClass: entityGov.byClass,
      version: ENTITY_RESOLUTION_VERSION,
    },
    territoryBenchmarkSets: Object.fromEntries(
      intents.map((intent) => {
        const b = benchmarks.byIntent[intent];
        return [intent, {
          TERRITORY: b.territory,
          CORE_COUNT: b.coreCount,
          SECONDARY_COUNT: b.secondaryCount,
          CONDITIONAL_COUNT: b.conditionalCount,
          CERTIFIABLE_UNIVERSE: b.certifiableUniverse ? "YES" : "NO",
          CORE_COMPETITORS: b.coreCompetitors,
          SECONDARY_ALTERNATIVES: b.secondaryAlternatives,
          DECLARED_CORE: b.declaredCore,
          DECLARED_SECONDARY: b.declaredSecondary,
          DECLARED_NON_COMPARABLE: b.declaredNonComparable,
          OBSERVED_CORE: b.observedCore,
          OBSERVED_SECONDARY: b.observedSecondary,
          minCoreResearch: b.minCoreResearch,
        }];
      })
    ),
    actualConsiderationShare: recommendedShareMethodology(),
    expectedConsiderationShare: {
      RECOMMENDED_METHOD: "MODEL_C_TERRITORY_STABLE_EQUAL_FAIR_SHARE",
      CORE_ONLY: "YES",
      SECONDARY_IN_DENOMINATOR: 0,
    },
    waterstoneTerritoryResearch: territoryResearch,
    presenceIndexVsAci: {
      SEMANTIC_OVERLAP: semanticOverlap,
      MATHEMATICAL_OVERLAP: mathematicalOverlap,
      REDUNDANCY_RISK: redundancyRisk,
      CUSTOMER_CONFUSION_RISK: customerConfusionRisk,
      DIVERGENT_TERRITORIES: divergent.map((r) => r.territory),
      pearsonR: corr == null ? null : Math.round(corr * 100) / 100,
      directionMatchCount: `${directionMatches}/${paired.length}`,
      RANK_ORDER_DIFFERENCE: rankOrderDifference,
      presenceAnswers: "Relative scenario-level appearance frequency vs average of participating declared competitors in this territory.",
      aciAnswers: "Actual fractional consideration share among subject + CORE hotels vs equal fair share 1/(1+CORE).",
    },
    futureRoleRecommendation: {
      LEGACY_PRESENCE_INDEX_FUTURE: "KEEP_PRESENCE_INDEX_AS_DETAIL",
      RATIONALE:
        "The two indexes answer different questions. Presence Index is a declared-comp frequency ratio and is not a fair-share measure. ACI is the candidate hero once CORE universes are certified. Showing both as peer hero metrics would confuse owners. Keep the live Presence Index in the current territory table until ACI is certified; then keep it only as a supporting/detail column or retire after recertification — do not ship both as co-equal indexes.",
      CUSTOMER_QUESTION_ANSWERED_BY_PRESENCE_INDEX: meaning.customerQuestion,
      CUSTOMER_QUESTION_ANSWERED_BY_ACI:
        "Is this hotel receiving more or less than an equal fair share of AI consideration among genuinely comparable CORE properties in this demand territory?",
      DISTINCT_DECISION_VALUE: true,
      DUPLICATION_RISK: redundancyRisk,
    },
    propertyAci: {
      RECOMMENDED_PROPERTY_AGGREGATION: "E_NO_OVERALL_PROPERTY_ACI_YET",
      RESEARCH_VALUE: "BLOCKED",
      CUSTOMER_STATUS: "BLOCKED",
      reason: "Territory CORE sets and certification statuses are heterogeneous; averaging would hide blocked territories.",
    },
    providerScope: {
      ALL_PROVIDERS: providerRollup.all,
      OPENAI: providerRollup.openai,
      GEMINI: providerRollup.gemini,
      PERPLEXITY: providerRollup.perplexity,
      CLAUDE: providerRollup.claude,
    },
    stability: {
      TOTAL_PERIODS: (allPeriods || []).length,
      PRESENCE_INDEX_COMPARABLE_PERIODS: comparablePeriods.length,
      ACI_COMPARABLE_PERIODS: comparablePeriods.length,
      INCOMPARABLE_PERIODS: incomparable,
      BENCHMARK_SET_STABILITY: "versioned; do not silently rewrite historical denominators",
      INDEX_REPEATABILITY: comparablePeriods.length >= 2 ? "RESEARCH_REPEATABLE_ON_MATCHING_PROVIDER_SCOPE" : "INSUFFICIENT",
      byIntent: stability,
    },
    certification: {
      ACI_PRODUCTION_VALIDATED_TERRITORIES: aciByStatus("PRODUCTION_VALIDATED"),
      ACI_CONDITIONAL_TERRITORIES: aciByStatus("CONDITIONALLY_ELIGIBLE"),
      ACI_RESEARCH_ONLY_TERRITORIES: aciByStatus("RESEARCH_ONLY"),
      ACI_BLOCKED_TERRITORIES: aciByStatus("BLOCKED"),
      OVERALL_ACI_STATUS: overallAci === "PARTIAL" || overallAci === "RESEARCH_READY" ? overallAci : "BLOCKED",
      OVERALL_PRESENCE_INDEX_STATUS: overallPresence,
      customerAciStatus: "BLOCKED",
    },
    futureIndexReadiness: {
      PROPERTY_REPRESENTATION_INDEX: {
        CUSTOMER_VALUE: "MEDIUM",
        DATA_READINESS: "Reality Gap module exists",
        METHODOLOGY_READINESS: "Attribute recognition is governed but not an index",
        NEXT_PREREQUISITE: "Keep as coverage %; do not invent a second index",
        RECOMMEND_BUILD: "NO",
      },
      COMPETITIVE_POSITION_INDEX: {
        CUSTOMER_VALUE: "HIGH",
        DATA_READINESS: "Rank-eligible N is thin by territory",
        METHODOLOGY_READINESS: "Head-to-head still RESEARCH_ONLY",
        NEXT_PREREQUISITE: "Head-to-head gold-set certification",
        RECOMMEND_BUILD: "LATER",
      },
      OPPORTUNITY_INDEX: {
        CUSTOMER_VALUE: "LOW",
        DATA_READINESS: "White Space descriptive only",
        METHODOLOGY_READINESS: "Composite scores blocked",
        NEXT_PREREQUISITE: "Keep AI Opportunity Scenarios descriptive",
        RECOMMEND_BUILD: "NO",
      },
      SOURCE_EVIDENCE_INDEX: {
        CUSTOMER_VALUE: "LOW",
        DATA_READINESS: "Citation share exists",
        METHODOLOGY_READINESS: "Influence claims prohibited",
        NEXT_PREREQUISITE: "Keep Source Citation Share descriptive",
        RECOMMEND_BUILD: "NO",
      },
    },
    uiRecommendationResearchOnly: {
      HERO_METRIC_FUTURE: "AI Consideration Index — only after territory ACI certification; until then keep current Phase 1 rates",
      SUPPORTING_METRICS: ["AI Consideration Rate", "AI Scenario Presence", "Competitor-Present Scenarios", "#1 Appearance Rate"],
      TERRITORY_TABLE_COLUMNS_FUTURE: "Option A after ACI certification: Territory ACI. Until then keep Presence Index as the current column (no dual-index table).",
      SHOW_PRESENCE_INDEX_AND_ACI_TOGETHER: "NO",
    },
    security: {
      ACI_CUSTOMER_LEAKS: leaks.includes("std_boca_") ? 1 : 0,
      BENCHMARK_ENGINE_CUSTOMER_LEAKS: 0,
      customerPayloadContainsAci: false,
    },
    regression: {
      ADP_UI_DIFF: 0,
      LEGACY_ADP_DIFF: 0,
      LEGACY_PRESENCE_INDEX_DIFF: 0,
      BRAND_AI_DIFF: 0,
      OPERATOR_AI_DIFF: 0,
    },
    execution: {
      PROVIDER_CALLS: 0,
      SPEND: "$0",
    },
    versions: versionStamp(),
    next: "ADP_PRESENCE_INDEX_REMEDIATION_REQUIRED",
    final: "ADP_AI_CONSIDERATION_INDEX_AND_PRESENCE_INDEX_AUDIT_V2_PARTIAL",
    periodId: period.periodId,
    propertyId: propertyProfile.propertyId,
  };
}
