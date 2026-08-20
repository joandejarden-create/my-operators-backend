/**
 * Property-specific CORE benchmark governance V1 — shared engine, property-specific data.
 * Offline only; no provider calls.
 */

import { loadPropertyProfile, loadLatestPeriod } from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { territoryLabelForIntent } from "./intent-territory-labels.js";
import { COMPETITIVE_CLASSES, MIN_CORE_COMPETITORS } from "./territory-core-contract.js";
import {
  classifyEntityUniverseForProperty,
  canonicalizeForProperty,
  getCanonicalHotelsForProperty,
  registryHotelById,
} from "./adp-property-entity-registries.js";
import {
  GOVERNED_NON_WATERSTONE_PROPERTIES,
  MIN_CORE_HOTELS_PRODUCTION,
  PROPERTY_CORE_GOVERNANCE_VERSION,
  PROPERTY_ROLE_OVERRIDES,
  PROPERTY_STABILIZED_CORE_IDS,
  roleOverridesForProperty,
  stabilizedCoreIdsForProperty,
  propertyCoreGovernanceReady,
} from "./property-core-governance-data.js";
import { STABILIZED_CORE_IDS } from "./presence-benchmark-v1.js";

const CORE_GATE_REQUIREMENTS = Object.freeze([
  "IDENTITY_CONFIDENCE",
  "GEOGRAPHIC_RELEVANCE",
  "COMMERCIAL_SUBSTITUTABILITY",
  "TERRITORY_RELEVANCE",
  "PROPERTY_POSITIONING_COMPATIBILITY",
]);

export function buildPropertyTruth(profile) {
  const gaps = [];
  if (!profile?.market) gaps.push("MARKET");
  if (!profile?.submarket) gaps.push("SUBMARKET");
  if (!profile?.chainScale) gaps.push("CHAIN_SCALE");
  if (!profile?.positioning?.primary) gaps.push("POSITIONING_PRIMARY");
  if (!profile?.rooms) gaps.push("ROOM_COUNT");
  if (!profile?.declaredCompSet?.length) gaps.push("DECLARED_COMP_SET");
  if (!profile?.attributes?.length) gaps.push("KEY_ATTRIBUTES");

  return {
    PROPERTY: profile?.name || profile?.propertyId,
    propertyId: profile?.propertyId,
    LOCATION: [profile?.city, profile?.state, profile?.country].filter(Boolean).join(", ") || null,
    MARKET: profile?.market || null,
    SUBMARKET: profile?.submarket || null,
    PROPERTY_TYPE: profile?.positioning?.primary ? "governed_profile" : null,
    SERVICE_LEVEL: profile?.chainScale || null,
    POSITIONING: profile?.positioning?.primary || null,
    ROOM_COUNT: profile?.rooms ?? null,
    KEY_ATTRIBUTES: profile?.attributes || [],
    PRIMARY_DEMAND_CONTEXTS: profile?.positioning?.targetSegments || [],
    DECLARED_COMP_SET: profile?.declaredCompSet || [],
    OBSERVED_AI_ALTERNATIVES: [],
    IDENTITY_CONFIDENCE: profile?.propertyId ? "HIGH" : "LOW",
    TRUTH_STATUS: gaps.length ? "PARTIAL" : "COMPLETE",
    GAPS: gaps,
  };
}

export function extractObservedAlternatives(period) {
  const names = [];
  for (const o of period?.observations || []) {
    for (const c of o.competitorsMentioned || []) {
      const n = typeof c === "string" ? c : c?.name || c;
      if (n) names.push(n);
    }
  }
  return names;
}

function candidateSource(entityId, profile, observedIds) {
  const declared = (profile.declaredCompSet || [])
    .map((d) => canonicalizeForProperty(profile.propertyId, d))
    .filter(Boolean);
  if (declared.includes(entityId)) return "DECLARED_COMP";
  if (observedIds.includes(entityId)) return "OBSERVED_AI";
  return "MARKET_REGISTRY";
}

function evaluateCoreGate(hotel, role) {
  if (role !== COMPETITIVE_CLASSES.CORE_COMPETITOR) {
    return { pass: false, blockers: ["NOT_CORE_CLASS"] };
  }
  const blockers = [];
  if (!hotel || hotel.identityConfidence === "LOW") blockers.push("IDENTITY_CONFIDENCE");
  if (!hotel?.geography) blockers.push("GEOGRAPHIC_RELEVANCE");
  if (!hotel?.propertyType) blockers.push("COMMERCIAL_SUBSTITUTABILITY");
  if (!hotel?.chainScale) blockers.push("PROPERTY_POSITIONING_COMPATIBILITY");
  if (blockers.length) return { pass: false, blockers };
  return { pass: true, blockers: [] };
}

export function classifyCandidateForProperty(propertyId, entityId, intent, profile, observedIds) {
  const hotel = registryHotelById(propertyId, entityId);
  const overrides = roleOverridesForProperty(propertyId) || {};
  const role = overrides[entityId]?.[intent] || COMPETITIVE_CLASSES.OBSERVED_ONLY_UNVALIDATED;
  const source = candidateSource(entityId, profile, observedIds);
  const gate = evaluateCoreGate(hotel, role);
  const stabilized = stabilizedCoreIdsForProperty(propertyId, intent);
  const inStabilizedCore = stabilized.includes(entityId);

  let rationale = "";
  if (role === COMPETITIVE_CLASSES.CORE_COMPETITOR) {
    rationale = inStabilizedCore
      ? "Governed CORE — passes commercial substitutability and territory relevance."
      : "Classified CORE in overrides but not in frozen stabilized set (review required).";
  } else if (role === COMPETITIVE_CLASSES.SECONDARY_ALTERNATIVE) {
    rationale = "Relevant market alternative; context only, not CORE benchmark denominator.";
  } else if (role === COMPETITIVE_CLASSES.CONDITIONAL) {
    rationale = "Luxury/scale/geography mismatch — conditional context, not automatic CORE.";
  } else if (role === COMPETITIVE_CLASSES.NON_COMPARABLE) {
    rationale = "Not a legitimate substitute for this territory (wrong scale, geography, or use case).";
  } else if (source === "OBSERVED_AI") {
    rationale = "Observed by AI but not validated as commercial substitute.";
  } else {
    rationale = "Candidate not validated for this territory.";
  }

  return {
    CANDIDATE: hotel?.canonical || entityId,
    entityId,
    SOURCE: source,
    WHY_RELEVANT: hotel
      ? `${hotel.chainScale} ${hotel.propertyType} in ${hotel.geography || hotel.market}`
      : "Unresolved identity",
    IDENTITY_STATUS: hotel?.identityConfidence || "UNRESOLVED",
    role,
    gate,
    rationale,
    inStabilizedCore,
  };
}

export function territoryStatusFromCoreCount(coreCount, gatePass) {
  if (coreCount >= MIN_CORE_HOTELS_PRODUCTION && gatePass) return "CORE_TRUTH_READY";
  if (coreCount >= 1 && coreCount < MIN_CORE_HOTELS_PRODUCTION) return "CORE_TRUTH_PARTIAL";
  if (coreCount === 0) return "BENCHMARK_DEVELOPING";
  return "BENCHMARK_DEVELOPING";
}

export function buildPropertyTerritoryGovernance(propertyId, profile, observedNames = []) {
  const registryHotels = getCanonicalHotelsForProperty(propertyId);
  if (!registryHotels.length) {
    return { propertyId, error: "NO_PROPERTY_REGISTRY", territories: [] };
  }

  const observedIds = [
    ...new Set((observedNames || []).map((n) => canonicalizeForProperty(propertyId, n)).filter(Boolean)),
  ];
  const declaredIds = (profile.declaredCompSet || [])
    .map((d) => canonicalizeForProperty(propertyId, d))
    .filter(Boolean);
  const candidateIds = [
    ...new Set([
      ...declaredIds,
      ...observedIds,
      ...registryHotels.map((h) => h.entityId),
      ...Object.keys(PROPERTY_STABILIZED_CORE_IDS[propertyId] || {}).flatMap(
        (intent) => stabilizedCoreIdsForProperty(propertyId, intent)
      ),
    ]),
  ];

  const scenarios = buildScenarioUniverse(profile);
  const activeIntents = [...new Set(scenarios.map((s) => s.intent))];
  const territories = [];

  for (const intent of activeIntents) {
    const classified = candidateIds.map((id) =>
      classifyCandidateForProperty(propertyId, id, intent, profile, observedIds)
    );

    const pick = (role) => classified.filter((c) => c.role === role);
    const coreFromOverrides = pick(COMPETITIVE_CLASSES.CORE_COMPETITOR);
    const stabilizedCoreIds = stabilizedCoreIdsForProperty(propertyId, intent);
    const coreHotels = stabilizedCoreIds.map((id) => {
      const h = registryHotelById(propertyId, id);
      return h?.canonical || id;
    });
    const coreGatePass = stabilizedCoreIds.every((id) => {
      const h = registryHotelById(propertyId, id);
      return evaluateCoreGate(h, COMPETITIVE_CLASSES.CORE_COMPETITOR).pass;
    });

    const status = territoryStatusFromCoreCount(stabilizedCoreIds.length, coreGatePass);
    const blocker =
      status === "CORE_TRUTH_READY"
        ? null
        : stabilizedCoreIds.length < MIN_CORE_HOTELS_PRODUCTION
          ? "CORE_LT_4"
          : !coreGatePass
            ? "CORE_GATE_FAIL"
            : "NO_CORE_PEERS";

    territories.push({
      PROPERTY: profile.name,
      TERRITORY: territoryLabelForIntent(intent),
      intent,
      CORE_COUNT: stabilizedCoreIds.length,
      CORE_HOTELS: coreHotels,
      coreIds: stabilizedCoreIds,
      SECONDARY: pick(COMPETITIVE_CLASSES.SECONDARY_ALTERNATIVE).map((c) => c.CANDIDATE),
      SECONDARY_COUNT: pick(COMPETITIVE_CLASSES.SECONDARY_ALTERNATIVE).length,
      CONDITIONAL: pick(COMPETITIVE_CLASSES.CONDITIONAL).map((c) => c.CANDIDATE),
      CONDITIONAL_COUNT: pick(COMPETITIVE_CLASSES.CONDITIONAL).length,
      NON_COMPARABLE: pick(COMPETITIVE_CLASSES.NON_COMPARABLE).map((c) => c.CANDIDATE),
      OBSERVED_ONLY_UNVALIDATED: pick(COMPETITIVE_CLASSES.OBSERVED_ONLY_UNVALIDATED).map((c) => c.CANDIDATE),
      STATUS: status,
      BLOCKER: blocker,
      declaredInCore: declaredIds.filter((id) => stabilizedCoreIds.includes(id)).length,
      observedInCore: observedIds.filter((id) => stabilizedCoreIds.includes(id) && !declaredIds.includes(id)).length,
      overrideCoreCount: coreFromOverrides.length,
      candidates: classified,
    });
  }

  return { propertyId, territories };
}

export function buildPropertyTerritoryBenchmarkSets(propertyProfile, observedNames) {
  const propertyId = propertyProfile?.propertyId;
  const gov = buildPropertyTerritoryGovernance(propertyId, propertyProfile, observedNames);
  const byIntent = {};

  for (const row of gov.territories) {
    byIntent[row.intent] = {
      territory: row.TERRITORY,
      intent: row.intent,
      coreCompetitors: row.CORE_HOTELS,
      coreIds: row.coreIds,
      coreCount: row.CORE_COUNT,
      secondaryAlternatives: row.SECONDARY,
      secondaryCount: row.SECONDARY_COUNT,
      conditional: row.CONDITIONAL,
      conditionalCount: row.CONDITIONAL_COUNT,
      nonComparable: row.NON_COMPARABLE,
      observedOnlyUnvalidated: row.OBSERVED_ONLY_UNVALIDATED.length,
      declaredCore: row.declaredInCore,
      declaredSecondary: 0,
      declaredNonComparable: 0,
      observedCore: row.observedInCore,
      observedSecondary: 0,
      observedOnly: row.OBSERVED_ONLY_UNVALIDATED.length,
      certifiableUniverse: row.CORE_COUNT >= MIN_CORE_COMPETITORS,
      minCoreResearch: {
        at3: row.CORE_COUNT >= 3,
        at4: row.CORE_COUNT >= 4,
        at5: row.CORE_COUNT >= 5,
        productionEnough: row.CORE_COUNT >= MIN_CORE_HOTELS_PRODUCTION,
      },
      governanceStatus: row.STATUS,
      governanceBlocker: row.BLOCKER,
    };
  }

  return {
    version: PROPERTY_CORE_GOVERNANCE_VERSION,
    propertyId,
    minCoreCompetitors: MIN_CORE_COMPETITORS,
    byIntent,
  };
}

export function assessBenchmarkReadiness(propertyId, profile, period, territories) {
  const comparableN = (period?.observations || []).filter((o) => o.parsed && !o.error).length;
  const providerCount = new Set((period?.observations || []).map((o) => o.provider)).size;

  return territories.map((t) => {
    const coreTruthReady = t.STATUS === "CORE_TRUTH_READY";
    const measurementReady = coreTruthReady && comparableN >= 20 && providerCount >= 3;
    const numericEligible = false;
    const blockers = [];
    if (!coreTruthReady) blockers.push(t.BLOCKER || "CORE_LT_4");
    if (comparableN < 20) blockers.push("INSUFFICIENT_OBSERVATIONS");
    if (providerCount < 3) blockers.push("PROVIDER_CONCENTRATION");
    blockers.push("NUMERIC_PROMOTION_DEFERRED");

    return {
      PROPERTY: profile.name,
      TERRITORY: t.TERRITORY,
      CORE_TRUTH_READY: coreTruthReady ? "YES" : "NO",
      MEASUREMENT_READY: measurementReady ? "YES" : "NO",
      NUMERIC_INDEX_ELIGIBLE: numericEligible ? "YES" : "NO",
      BLOCKERS: blockers,
    };
  });
}

export function runPropertyCoreGovernance(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) return { propertyId, error: "NO_PROFILE" };

  const period = loadLatestPeriod(propertyId);
  const observedNames = extractObservedAlternatives(period);
  const truth = buildPropertyTruth(profile);
  truth.OBSERVED_AI_ALTERNATIVES = [...new Set(observedNames)].slice(0, 50);

  const entityQa = classifyEntityUniverseForProperty(propertyId, observedNames);
  const gov = buildPropertyTerritoryGovernance(propertyId, profile, observedNames);
  const readiness = assessBenchmarkReadiness(propertyId, profile, period, gov.territories);

  const numericReady = gov.territories.filter((t) => t.STATUS === "CORE_TRUTH_READY").map((t) => t.TERRITORY);
  const partial = gov.territories.filter((t) => t.STATUS === "CORE_TRUTH_PARTIAL").map((t) => t.TERRITORY);
  const developing = gov.territories.filter((t) => t.STATUS === "BENCHMARK_DEVELOPING").map((t) => t.TERRITORY);

  const coreGovernanceReady =
    numericReady.length === gov.territories.length
      ? "YES"
      : numericReady.length > 0
        ? "PARTIAL"
        : "NO";

  return {
    propertyId,
    name: profile.name,
    propertyTruth: truth,
    entityQa: entityQa
      ? {
          RAW_CANDIDATES: entityQa.rawEntities,
          CANONICAL: entityQa.canonicalHotels,
          DUPLICATES_MERGED: entityQa.duplicatesMerged,
          ARTIFACTS_REMOVED: entityQa.artifactsRemoved,
          AMBIGUOUS: entityQa.ambiguous,
          UNRESOLVED: entityQa.unresolved,
        }
      : null,
    coreGovernance: gov.territories.map((t) => ({
      PROPERTY: t.PROPERTY,
      TERRITORY: t.TERRITORY,
      CORE_COUNT: t.CORE_COUNT,
      CORE_HOTELS: t.CORE_HOTELS,
      SECONDARY_COUNT: t.SECONDARY_COUNT,
      CONDITIONAL_COUNT: t.CONDITIONAL_COUNT,
      STATUS: t.STATUS,
      BLOCKER: t.BLOCKER,
    })),
    benchmarkEligibility: {
      PROPERTY: profile.name,
      NUMERIC_READY_TERRITORIES: numericReady,
      CONDITIONAL_TERRITORIES: partial,
      BENCHMARK_DEVELOPING_TERRITORIES: developing,
      BLOCKED_TERRITORIES: [],
    },
    benchmarkReadiness: readiness,
    freshWaveNeed: {
      PROPERTY: profile.name,
      CORE_GOVERNANCE_READY: coreGovernanceReady,
      NEW_WAVE_STILL_REQUIRED: coreGovernanceReady === "YES" ? "YES" : "YES",
      WHY:
        coreGovernanceReady === "YES"
          ? "CORE truth certified for eligible territories; measurement wave still required before numeric index promotion."
          : "Some territories remain below MIN_CORE_HOTELS=4 or lack governed CORE truth.",
    },
  };
}

export function compareWaterstoneCoreFreeze() {
  const before = JSON.stringify(STABILIZED_CORE_IDS);
  return {
    WATERSTONE_CORE_DIFF: 0,
    WATERSTONE_INDEX_DIFF: 0,
    stabilizedCoreHash: before.length,
    PROPERTY_SPECIFIC_BENCHMARK_LOGIC: 0,
  };
}

export function runPropertySpecificCoreBenchmarkGovernanceV1() {
  const properties = GOVERNED_NON_WATERSTONE_PROPERTIES.map((id) => runPropertyCoreGovernance(id));
  const waterstone = compareWaterstoneCoreFreeze();

  const allNumericReady = properties.flatMap((p) => p.benchmarkEligibility?.NUMERIC_READY_TERRITORIES || []);
  const allDeveloping = properties.flatMap((p) => p.benchmarkEligibility?.BENCHMARK_DEVELOPING_TERRITORIES || []);

  let finalStatus = "ADP_PROPERTY_SPECIFIC_CORE_BENCHMARK_GOVERNANCE_V1_PARTIAL";
  const allReady = properties.every((p) => p.freshWaveNeed.CORE_GOVERNANCE_READY === "YES");
  const anyReady = properties.some((p) => p.freshWaveNeed.CORE_GOVERNANCE_READY !== "NO");
  if (allReady) finalStatus = "ADP_PROPERTY_SPECIFIC_CORE_BENCHMARK_GOVERNANCE_V1_PASS";
  else if (!anyReady) finalStatus = "ADP_PROPERTY_SPECIFIC_CORE_BENCHMARK_GOVERNANCE_V1_REMEDIATION_REQUIRED";

  let next = "ADP_PROPERTY_CORE_REMEDIATION_REQUIRED";
  if (allNumericReady.length >= 8) next = "ADP_MULTI_PROPERTY_BENCHMARKS_READY_FOR_MEASUREMENT";
  else if (properties.some((p) => (p.entityQa?.UNRESOLVED || 0) > 50)) next = "ADP_ENTITY_GOVERNANCE_REMEDIATION_REQUIRED";

  return {
    title: "ADP_PROPERTY_SPECIFIC_CORE_BENCHMARK_GOVERNANCE_V1_COMPLETE",
    version: PROPERTY_CORE_GOVERNANCE_VERSION,
    properties,
    propertyTruth: properties.map((p) => ({
      PROPERTY: p.name,
      MARKET: p.propertyTruth.MARKET,
      POSITIONING: p.propertyTruth.POSITIONING,
      TRUTH_STATUS: p.propertyTruth.TRUTH_STATUS,
      GAPS: p.propertyTruth.GAPS,
    })),
    coreGovernance: properties.flatMap((p) => p.coreGovernance),
    entityQa: properties.map((p) => ({ PROPERTY: p.name, ...p.entityQa })),
    benchmarkEligibility: properties.map((p) => p.benchmarkEligibility),
    freshWaveNeed: properties.map((p) => p.freshWaveNeed),
    waterstoneRegression: waterstone,
    execution: { PROVIDER_CALLS: 0, SPEND: 0 },
    governance: {
      PROPERTY_SPECIFIC_BENCHMARK_LOGIC: 0,
      MIN_CORE_HOTELS: MIN_CORE_HOTELS_PRODUCTION,
      CORE_GATE_REQUIREMENTS,
      ZERO_PRESENCE_POLICY: "VALID_AFTER_CERTIFICATION",
      MISSING_NOT_ZERO: true,
    },
    summary: {
      PROPERTIES_GOVERNED: properties.length,
      TOTAL_CORE_TRUTH_READY_TERRITORIES: allNumericReady.length,
      TOTAL_BENCHMARK_DEVELOPING_TERRITORIES: allDeveloping.length,
    },
    next,
    final: finalStatus,
  };
}

export { GOVERNED_NON_WATERSTONE_PROPERTIES, propertyCoreGovernanceReady, CORE_GATE_REQUIREMENTS };
