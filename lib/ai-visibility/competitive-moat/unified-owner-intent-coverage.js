/**
 * Unified Owner Intent coverage for Coverage Diagnostics — taxonomy + aggregation only.
 * Combines governed scenario presence, peer-present gaps, and certified benchmark rows.
 * PROVIDER_CALLS = 0 · No benchmark certification rule changes.
 */

import { AVAILABILITY } from "../availability-states.js";
import { CUSTOMER_DECISION_CONTEXT } from "../customer-prompt-disclosure.js";
import { computeAiPresenceRate } from "../metrics.js";
import { providersMatch } from "../provider-dimension.js";
import { buildScenarioRegistryIndex, loadScenarioRegistry } from "../scenario-registry.js";
import { buildPromptMetadataById } from "../associations/prompt-metadata-lookup.js";
import { SCENARIO_IDS as S } from "./benchmark-brand-ids.js";
import { loadBenchmarkEligibleUniverse } from "./benchmark-eligible-universe.js";
import {
  auditObservationScenarioMapping,
  resolveObservationScenario,
} from "./prompt-scenario-bridge.js";
import { resolveScenarioCommercialPeers } from "./scenario-peer-eligibility.js";
import {
  buildOwnerIntentBenchmarksForBrand,
  CUSTOMER_SCENARIO_DISPLAY_ORDER,
  getCustomerScenarioDisplayLabel,
} from "./scenario-benchmark-customer-service.js";
import { attachChgVsPriorToCoverageRows } from "./owner-intent-chg-vs-prior.js";

export const UNIFIED_OWNER_INTENT_COVERAGE_VERSION =
  "unified_owner_intent_coverage_v1";

/** Extended display order — benchmark rows plus monitored CORE scenarios with presence data. */
export const COVERAGE_OWNER_INTENT_DISPLAY_ORDER = Object.freeze([
  S.SOFT_BRAND,
  S.CONVERSION_SUITABILITY,
  S.OWNER_FLEXIBILITY,
  S.LIFESTYLE,
  S.INDEPENDENT_UU_CONVERSION,
  S.NEWBUILD_UU,
  S.BRANDED_RESIDENCES,
  S.CHAIN_SCALE,
  S.MARKET_ENTRY,
  S.OWNER_ECONOMICS,
  S.DISTRIBUTION_LOYALTY,
  S.HMA_VS_FRANCHISE,
]);

const RESEARCH_ONLY_SCENARIO_IDS = new Set([S.DISTRIBUTION_LOYALTY]);

const PEER_GAP_QUALIFYING_POLICY =
  "CORE_PEERS_PRESENT_ONLY — governed CORE commercial peers from resolveScenarioCommercialPeers; " +
  "SECONDARY and observed competitors do not create peer-present gaps.";

function presentIds(obs) {
  const fromField = obs.presentEntityIds || [];
  if (fromField.length) return new Set(fromField.filter(Boolean));
  const mentions = obs.mentions || obs.payload?.mentions || [];
  return new Set(
    mentions
      .map((m) => m.entityId || m.resolvedEntityId || m.canonicalEntityId)
      .filter(Boolean)
  );
}

function filterObservationsForScope(observations = [], opts = {}) {
  const { allProvidersMode = true, provider = null } = opts;
  return (observations || []).filter((o) => {
    if (!o || o.success === false) return false;
    if (allProvidersMode) return true;
    return providersMatch(o.provider, provider);
  });
}

function corePeerIdsForScenario(subjectId, scenarioId, universe) {
  const peers = resolveScenarioCommercialPeers(subjectId, scenarioId, { universe });
  return new Set(
    peers.calculationPeers
      .filter((p) => p.commercialRelation === "CORE")
      .map((p) => p.peerBrandId)
      .filter(Boolean)
  );
}

function selectCorePeerNames(subjectId, scenarioId, observations, brandNamesById = {}) {
  const universe = loadBenchmarkEligibleUniverse();
  const coreIds = corePeerIdsForScenario(subjectId, scenarioId, universe);
  const presentNames = new Set();
  for (const obs of observations || []) {
    if (!obs || obs.success === false) continue;
    const resolved = resolveObservationScenario(obs, {});
    if (resolved.scenarioId !== scenarioId) continue;
    for (const id of presentIds(obs)) {
      if (coreIds.has(id)) {
        const name = brandNamesById[id];
        if (name) presentNames.add(name);
      }
    }
  }
  return [...presentNames].slice(0, 3);
}

function selectObservedCompetitors(subjectId, scenarioId, observations, coreNames, brandNamesById = {}) {
  const coreSet = new Set(coreNames);
  const seen = new Set();
  for (const obs of observations || []) {
    if (!obs || obs.success === false) continue;
    const resolved = resolveObservationScenario(obs, {});
    if (resolved.scenarioId !== scenarioId) continue;
    for (const id of presentIds(obs)) {
      if (id === subjectId) continue;
      const name = brandNamesById[id];
      if (name && !coreSet.has(name) && !seen.has(name)) {
        seen.add(name);
        if (seen.size >= 3) break;
      }
    }
    if (seen.size >= 3) break;
  }
  return [...seen];
}

/**
 * Aggregate comparable observations for one governed scenario.
 */
export function aggregateScenarioCoverage(subjectBrandId, scenarioId, observations = [], opts = {}) {
  const scenarioIndex = opts.scenarioIndex || buildScenarioRegistryIndex(loadScenarioRegistry());
  const promptMap = opts.promptMap || buildPromptMetadataById();
  const scoped = filterObservationsForScope(observations, opts);

  const comparable = [];
  for (const obs of scoped) {
    const resolved = resolveObservationScenario(obs, { scenarioIndex, promptMap });
    if (resolved.scenarioId === scenarioId) comparable.push(obs);
  }

  if (!comparable.length) {
    return {
      scenarioId,
      comparableObservationCount: 0,
      withPresenceCount: 0,
      missingCount: 0,
      subjectPresence: null,
      peerPresentGapCount: null,
      measurable: false,
    };
  }

  const rate = computeAiPresenceRate(comparable, subjectBrandId);
  const withPresenceCount =
    typeof rate.numerator === "number" ? rate.numerator : 0;
  const comparableObservationCount =
    typeof rate.denominator === "number" ? rate.denominator : comparable.length;
  const missingCount = Math.max(0, comparableObservationCount - withPresenceCount);
  const subjectPresence =
    typeof rate.value === "number" ? Math.round(rate.value * 10000) / 10000 : null;

  const universe = loadBenchmarkEligibleUniverse();
  const corePeerIds = corePeerIdsForScenario(subjectBrandId, scenarioId, universe);
  let peerPresentGapCount = 0;
  let missingWithoutQualifyingPeer = 0;
  let missingWithQualifyingPeer = 0;

  for (const obs of comparable) {
    const present = presentIds(obs);
    if (present.has(subjectBrandId)) continue;
    const qualifyingPeerPresent = [...present].some((id) => corePeerIds.has(id));
    if (qualifyingPeerPresent) {
      peerPresentGapCount += 1;
      missingWithQualifyingPeer += 1;
    } else {
      missingWithoutQualifyingPeer += 1;
    }
  }

  return {
    scenarioId,
    comparableObservationCount,
    withPresenceCount,
    missingCount,
    subjectPresence,
    peerPresentGapCount,
    measurable: true,
    missingWithoutQualifyingPeer,
    missingWithQualifyingPeer,
  };
}

function positionFromBenchmark(relativeGapPct, indexValue) {
  if (indexValue == null || relativeGapPct == null) return null;
  const gap = Math.round(Math.abs(relativeGapPct));
  if (indexValue > 100) return `${gap}% above benchmark`;
  if (indexValue < 100) return `${gap}% below benchmark`;
  return "At parity";
}

function presenceDisplay(subjectPresence, comparableObservationCount, withPresenceCount) {
  if (comparableObservationCount === 0) {
    return { display: "Not enough comparable observations", secondary: null };
  }
  if (subjectPresence == null) {
    return { display: "Not enough comparable observations", secondary: null };
  }
  const pct = `${Math.round(subjectPresence * 100)}%`;
  const secondary =
    comparableObservationCount != null && withPresenceCount != null
      ? `${withPresenceCount} of ${comparableObservationCount} observations`
      : null;
  return { display: pct, secondary };
}

function rowEligibility(scenarioId, registry, hasObservations) {
  const row = (registry.scenarios || []).find((s) => s.scenarioId === scenarioId);
  if (RESEARCH_ONLY_SCENARIO_IDS.has(scenarioId) || row?.status === "PLANNED_NO_PROMPTS") {
    return "RESEARCH_ONLY";
  }
  if (!hasObservations) return "MEASUREMENT_NOT_READY";
  return "CUSTOMER_VISIBLE_MONITORED";
}

/**
 * Build unified Owner Intent coverage payload for Coverage Diagnostics.
 */
export function buildUnifiedOwnerIntentCoverage(brandId, opts = {}) {
  const observations = opts.observations || [];
  const allProvidersMode = opts.allProvidersMode !== false;
  const provider = opts.provider || null;
  const brandNamesById = opts.brandNamesById || {};
  const registry = loadScenarioRegistry();
  const scenarioIndex = buildScenarioRegistryIndex(registry);
  const promptMap = buildPromptMetadataById();
  const mappingAudit = auditObservationScenarioMapping(
    filterObservationsForScope(observations, { allProvidersMode, provider }),
    { scenarioIndex, promptMap }
  );

  const benchmarkBlock = buildOwnerIntentBenchmarksForBrand(brandId, {
    allProvidersMode,
    provider,
    observations,
  });
  const benchmarkByScenario = new Map(
    (benchmarkBlock.ownerIntentBenchmarks || []).map((r) => [r.scenarioId, r])
  );

  const aggregatedByScenario = new Map();
  for (const scenarioId of COVERAGE_OWNER_INTENT_DISPLAY_ORDER) {
    aggregatedByScenario.set(
      scenarioId,
      aggregateScenarioCoverage(brandId, scenarioId, observations, {
        allProvidersMode,
        provider,
        scenarioIndex,
        promptMap,
      })
    );
  }

  const customerVisible = [];
  const researchOnly = [];
  const notApplicable = [];
  const omittedOther = [];

  const peerGapAudit = {
    NO_MISSING_OBSERVATIONS: 0,
    MISSING_BUT_NO_QUALIFYING_PEER: 0,
    PEER_GAP_MAPPING_BROKEN: 0,
    PEER_CONTEXT_NOT_JOINED: 0,
    PROVIDER_SCOPE_MISMATCH: 0,
    DISPLAY_BUG: 0,
  };

  const rows = [];

  for (const scenarioId of COVERAGE_OWNER_INTENT_DISPLAY_ORDER) {
    const agg = aggregatedByScenario.get(scenarioId);
    const benchmark = benchmarkByScenario.get(scenarioId);
    const hasObservations = agg.measurable && agg.comparableObservationCount > 0;
    const eligibility = rowEligibility(scenarioId, registry, hasObservations);

    if (eligibility === "RESEARCH_ONLY") {
      researchOnly.push({ scenarioId, intentLabel: getCustomerScenarioDisplayLabel(scenarioId) });
      continue;
    }

    const inBenchmarkOrder = CUSTOMER_SCENARIO_DISPLAY_ORDER.includes(scenarioId);
    const showRow =
      hasObservations || (inBenchmarkOrder && benchmark) || benchmark?.subjectPresence != null;

    if (!showRow) {
      if (eligibility === "MEASUREMENT_NOT_READY") {
        omittedOther.push({
          scenarioId,
          intentLabel: getCustomerScenarioDisplayLabel(scenarioId),
          reason: "MEASUREMENT_NOT_READY",
        });
      }
      continue;
    }

    if (hasObservations) {
      customerVisible.push({ scenarioId, intentLabel: getCustomerScenarioDisplayLabel(scenarioId) });
    }

    const presenceFmt = presenceDisplay(
      agg.measurable ? agg.subjectPresence : benchmark?.subjectPresence ?? null,
      agg.measurable ? agg.comparableObservationCount : null,
      agg.measurable ? agg.withPresenceCount : null
    );

    let peerPresentGapCount = agg.measurable ? agg.peerPresentGapCount : null;
    if (agg.measurable) {
      if (agg.missingCount === 0) {
        peerGapAudit.NO_MISSING_OBSERVATIONS += 1;
        peerPresentGapCount = 0;
      } else if (agg.missingWithoutQualifyingPeer > 0 && agg.missingWithQualifyingPeer === 0) {
        peerGapAudit.MISSING_BUT_NO_QUALIFYING_PEER += 1;
        peerPresentGapCount = 0;
      } else if (agg.missingWithQualifyingPeer > 0) {
        peerPresentGapCount = agg.peerPresentGapCount;
      }
    }

    const indexValue =
      typeof benchmark?.indexValue === "number" ? benchmark.indexValue : null;
    const relativeGapPct =
      typeof benchmark?.relativeGapPct === "number" ? benchmark.relativeGapPct : null;
    const benchmarkStatus =
      indexValue != null
        ? benchmark?.benchmarkStatus || "CERTIFIED"
        : "Benchmark still developing";

    const corePeers = selectCorePeerNames(brandId, scenarioId, observations, brandNamesById);
    const observedCompetitors = selectObservedCompetitors(
      brandId,
      scenarioId,
      observations,
      corePeers,
      brandNamesById
    );

    rows.push({
      scenarioId,
      intentLabel: getCustomerScenarioDisplayLabel(scenarioId),
      decisionContext: CUSTOMER_DECISION_CONTEXT[scenarioId] || null,
      subjectPresence: agg.measurable ? agg.subjectPresence : benchmark?.subjectPresence ?? null,
      subjectPresenceDisplay: presenceFmt.display,
      presenceDenominatorDisplay: presenceFmt.secondary,
      comparableObservationCount: agg.measurable ? agg.comparableObservationCount : null,
      withPresenceCount: agg.measurable ? agg.withPresenceCount : null,
      missingCount: agg.measurable ? agg.missingCount : null,
      peerPresentGapCount: agg.measurable ? peerPresentGapCount : null,
      benchmarkStatus,
      indexValue,
      relativeGapPct,
      position: positionFromBenchmark(relativeGapPct, indexValue),
      selectedCorePeers: benchmark?.selectedCorePeers?.length
        ? benchmark.selectedCorePeers.slice(0, 3)
        : corePeers,
      selectedObservedCompetitors: benchmark?.selectedObservedCompetitors?.length
        ? benchmark.selectedObservedCompetitors.slice(0, 3)
        : observedCompetitors,
      eligibility,
      watchlistLinkEligible:
        (agg.measurable && (agg.missingCount > 0 || (peerPresentGapCount || 0) > 0)) || false,
    });
  }

  if (mappingAudit.unmapped > 0) {
    peerGapAudit.PEER_GAP_MAPPING_BROKEN += mappingAudit.unmapped;
  }

  const historyRows = attachChgVsPriorToCoverageRows(rows, {
    brandId,
    allProvidersMode,
    provider,
    currentObservations: filterObservationsForScope(observations, { allProvidersMode, provider }),
    priorObservations: opts.priorObservations || null,
    currentPeriodId: opts.currentPeriodId || null,
    geography: opts.geography || "CALA",
    language: opts.language || "en",
    periods: opts.periods,
    storeRoot: opts.storeRoot,
  });

  const availability =
    historyRows.length > 0 ? AVAILABILITY.OBSERVED : AVAILABILITY.NOT_MONITORED;

  return {
    version: UNIFIED_OWNER_INTENT_COVERAGE_VERSION,
    availability,
    title: "AI Presence by Owner Intent",
    subtext:
      "How consistently your brand appears across monitored owner decisions, where gaps exist, " +
      "and how performance compares with relevant peers when a certified benchmark is available.",
    rows: historyRows,
    SINGLE_CUSTOMER_TAXONOMY: "OWNER_INTENT",
    LEGACY_PROMPT_FAMILY_PRIMARY_LABELS: false,
    unmappedMonitoredObservations: mappingAudit.unmapped,
    unmappedSamples: mappingAudit.unmappedSamples,
    customerVisibleIntents: customerVisible,
    researchOnlyIntents: researchOnly,
    notApplicableIntents: notApplicable,
    omittedForOtherReason: omittedOther,
    peerPresentGapDefinition:
      "Subject absent on a comparable monitored observation AND at least one governed CORE peer present on the same observation.",
    qualifyingPeerPolicy: PEER_GAP_QUALIFYING_POLICY,
    peerPresentGapAudit: peerGapAudit,
    TRUE_ZERO_RENDERED_AS_ZERO: true,
    BENCHMARK_SCOPE: benchmarkBlock.BENCHMARK_SCOPE || null,
    PROVIDER_CALLS: 0,
  };
}

/**
 * Deterministic peer-present gap fixtures for contract tests (spec §40).
 */
export function computePeerPresentGapFromFixture(observations, subjectBrandId, corePeerIds = []) {
  const coreSet = new Set(corePeerIds);
  let missing = 0;
  let peerGaps = 0;
  for (const obs of observations || []) {
    if (!obs || obs.success === false) continue;
    const present = presentIds(obs);
    if (present.has(subjectBrandId)) continue;
    missing += 1;
    const qualifying = [...present].some((id) => coreSet.has(id));
    if (qualifying) peerGaps += 1;
  }
  return { missing, peerPresentGapCount: peerGaps };
}
