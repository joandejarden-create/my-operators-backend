/**
 * ADP Scenario Universe Certification V1 — fail-closed gate before paid measurement.
 *
 * Canonical measurement density: ADP_SCENARIO_DENSITY_TIER_STANDARD_V1 ≈ 63
 * (mature market+property packs 60–65, or generic parity v2 ≈63).
 * Oversized market flagship packs (e.g. Boca) require explicit tier disclosure.
 */

import crypto from "crypto";
import {
  buildScenarioUniverse,
  resolveStandardScenarioMarket,
} from "../prompt-universe/scenario-registry.js";
import { getStandardScenarios } from "../prompt-universe/standard-scenarios.js";
import { generatePropertyScenarios } from "../prompt-universe/property-scenarios.js";
import { buildScenarioUniverseManifest } from "./adp-scenario-universe-contract-v1.js";
import { PROVIDERS } from "../data-model.js";

export const ADP_SCENARIO_UNIVERSE_CERTIFICATION_V1 =
  "ADP_SCENARIO_UNIVERSE_CERTIFICATION_V1";

/** Commercial standard density for comparable ADP property reports */
export const ADP_SCENARIO_DENSITY_TIER_STANDARD_V1 = Object.freeze({
  id: "ADP_SCENARIO_DENSITY_TIER_STANDARD_V1",
  targetScenarioCount: 63,
  /** Certified market packs vary slightly (45–50 std + 15 prop) */
  allowedMin: 60,
  allowedMax: 65,
  providerCount: 4,
  providers: Object.freeze([...PROVIDERS]),
  requiredIntentCoverage: Object.freeze([
    "business",
    "leisure",
    "group_meeting",
    "couples",
    "family",
    "celebration",
    "wellness",
  ]),
  layers: Object.freeze([
    "CORE_UNIVERSAL",
    "HOTEL_ARCHETYPE",
    "MARKET_DESTINATION",
    "PROPERTY_CAPABILITY",
  ]),
  methodologyVersion: "adp_scenario_methodology_v1",
});

/** Explicit oversized pack (stewarded market depth) — not silently comparable to STANDARD */
export const ADP_SCENARIO_DENSITY_TIER_EXPANDED_MARKET_V1 = Object.freeze({
  id: "ADP_SCENARIO_DENSITY_TIER_EXPANDED_MARKET_V1",
  allowedMin: 66,
  allowedMax: 80,
  note: "Stewarded expanded market packs (e.g. Boca waterfront). Disclose when comparing to STANDARD tier.",
});

export const SCENARIO_DIFFERENCE_CLASSES = Object.freeze({
  LEGITIMATE_MARKET_CUSTOMIZATION: "LEGITIMATE_MARKET_CUSTOMIZATION",
  LEGITIMATE_PROPERTY_CUSTOMIZATION: "LEGITIMATE_PROPERTY_CUSTOMIZATION",
  HISTORICAL_METHODOLOGY_VERSION: "HISTORICAL_METHODOLOGY_VERSION",
  MISSING_SCENARIO_GENERATION: "MISSING_SCENARIO_GENERATION",
  INCOMPLETE_PROVIDER_EXECUTION: "INCOMPLETE_PROVIDER_EXECUTION",
  ARCHIVE_EVIDENCE_MISSING: "ARCHIVE_EVIDENCE_MISSING",
  DUPLICATE_OR_INVALID_SCENARIOS: "DUPLICATE_OR_INVALID_SCENARIOS",
  SCENARIO_FILTERING_DEFECT: "SCENARIO_FILTERING_DEFECT",
  COUNTING_OR_DENOMINATOR_DEFECT: "COUNTING_OR_DENOMINATOR_DEFECT",
  UNEXPLAINED_VARIANCE: "UNEXPLAINED_VARIANCE",
});

function sha16(value) {
  return crypto.createHash("sha256").update(String(value || "")).digest("hex").slice(0, 16);
}

export function intentDistribution(scenarios) {
  const by = {};
  for (const s of scenarios || []) {
    const intent = s.intent || s.travelerIntent || "UNKNOWN";
    by[intent] = (by[intent] || 0) + 1;
  }
  return by;
}

export function sourceDistribution(scenarios) {
  const by = {};
  for (const s of scenarios || []) {
    const src = s.source || s.layer || "unknown";
    by[src] = (by[src] || 0) + 1;
  }
  return by;
}

export function detectDuplicateScenarioIds(scenarios) {
  const seen = new Map();
  const dupes = [];
  for (const s of scenarios || []) {
    const id = s.scenarioId || s.id;
    if (!id) continue;
    if (seen.has(id)) dupes.push(id);
    else seen.set(id, true);
  }
  return [...new Set(dupes)];
}

/**
 * Classify live universe composition vs canonical STANDARD tier.
 */
export function classifyScenarioUniverseComposition({
  standardCount,
  propertyCount,
  capabilityMergeCount,
  universeCount,
  resolvedMarketKey,
} = {}) {
  const classes = [];
  const tier = ADP_SCENARIO_DENSITY_TIER_STANDARD_V1;

  if (capabilityMergeCount > 0 && propertyCount > 0) {
    classes.push(SCENARIO_DIFFERENCE_CLASSES.SCENARIO_FILTERING_DEFECT);
  }
  if (standardCount === 0 && propertyCount === 0 && universeCount >= tier.allowedMin) {
    classes.push(SCENARIO_DIFFERENCE_CLASSES.LEGITIMATE_MARKET_CUSTOMIZATION);
    // generic parity — acceptable for new markets
  }
  if (standardCount > 0 && propertyCount > 0) {
    classes.push(SCENARIO_DIFFERENCE_CLASSES.LEGITIMATE_PROPERTY_CUSTOMIZATION);
    if (standardCount >= 45) {
      classes.push(SCENARIO_DIFFERENCE_CLASSES.LEGITIMATE_MARKET_CUSTOMIZATION);
    }
  }
  if (standardCount > 0 && propertyCount === 0 && capabilityMergeCount > 0) {
    classes.push(SCENARIO_DIFFERENCE_CLASSES.LEGITIMATE_PROPERTY_CUSTOMIZATION);
  }
  if (universeCount > tier.allowedMax && universeCount <= ADP_SCENARIO_DENSITY_TIER_EXPANDED_MARKET_V1.allowedMax) {
    classes.push(SCENARIO_DIFFERENCE_CLASSES.LEGITIMATE_MARKET_CUSTOMIZATION);
  }
  if (universeCount > ADP_SCENARIO_DENSITY_TIER_EXPANDED_MARKET_V1.allowedMax) {
    classes.push(SCENARIO_DIFFERENCE_CLASSES.UNEXPLAINED_VARIANCE);
  }
  if (universeCount > 0 && universeCount < tier.allowedMin) {
    classes.push(SCENARIO_DIFFERENCE_CLASSES.MISSING_SCENARIO_GENERATION);
  }
  if (!resolvedMarketKey && standardCount === 0) {
    classes.push(SCENARIO_DIFFERENCE_CLASSES.HISTORICAL_METHODOLOGY_VERSION);
  }

  return [...new Set(classes)];
}

/**
 * Fail-closed certification of planned scenario universe before paid providers.
 */
export function certifyScenarioUniverseForMeasurement(propertyProfile, options = {}) {
  const failures = [];
  const warnings = [];
  const tier = options.tier || ADP_SCENARIO_DENSITY_TIER_STANDARD_V1;
  const marketKey = resolveStandardScenarioMarket(propertyProfile);
  const chainScale = String(propertyProfile?.chainScale || "")
    .toLowerCase()
    .replace(/\s+/g, "_");
  const standard = getStandardScenarios(marketKey, chainScale);
  const specific = generatePropertyScenarios(propertyProfile);
  const scenarios = options.scenarios || buildScenarioUniverse(propertyProfile);
  const manifest = buildScenarioUniverseManifest(propertyProfile, {
    scenarios,
    scenarioUniverseVersion: options.scenarioUniverseVersion,
  });

  const duplicates = detectDuplicateScenarioIds(scenarios);
  if (duplicates.length) {
    failures.push({
      code: "DUPLICATE_SCENARIO_IDS",
      class: SCENARIO_DIFFERENCE_CLASSES.DUPLICATE_OR_INVALID_SCENARIOS,
      detail: duplicates.slice(0, 20),
    });
  }

  const byIntent = intentDistribution(scenarios);
  const bySource = sourceDistribution(scenarios);
  const capabilityMergeCount = bySource.generic_property_capability || 0;
  const missingIntents = tier.requiredIntentCoverage.filter((i) => !byIntent[i]);
  if (missingIntents.length) {
    warnings.push({
      code: "MISSING_INTENT_COVERAGE",
      detail: missingIntents,
    });
  }

  if (capabilityMergeCount > 0 && specific.length > 0) {
    failures.push({
      code: "CAPABILITY_STACKED_ON_PROPERTY_PACK",
      class: SCENARIO_DIFFERENCE_CLASSES.SCENARIO_FILTERING_DEFECT,
      detail: {
        propertyPack: specific.length,
        capabilityMerge: capabilityMergeCount,
      },
    });
  }

  const count = manifest.scenarioCount;
  const expanded =
    count > tier.allowedMax &&
    count <= ADP_SCENARIO_DENSITY_TIER_EXPANDED_MARKET_V1.allowedMax;
  const assignedTiers = [];
  if (count >= tier.allowedMin && count <= tier.allowedMax) {
    assignedTiers.push(tier.id);
  } else if (expanded) {
    assignedTiers.push(ADP_SCENARIO_DENSITY_TIER_EXPANDED_MARKET_V1.id);
    warnings.push({
      code: "EXPANDED_MARKET_TIER",
      detail: "Disclose when comparing to STANDARD density hotels",
    });
  } else if (count < tier.allowedMin) {
    failures.push({
      code: "BELOW_STANDARD_DENSITY",
      class: SCENARIO_DIFFERENCE_CLASSES.MISSING_SCENARIO_GENERATION,
      detail: { count, allowedMin: tier.allowedMin },
    });
  } else {
    failures.push({
      code: "ABOVE_EXPANDED_DENSITY_WITHOUT_AUTHORIZATION",
      class: SCENARIO_DIFFERENCE_CLASSES.UNEXPLAINED_VARIANCE,
      detail: { count, expandedMax: ADP_SCENARIO_DENSITY_TIER_EXPANDED_MARKET_V1.allowedMax },
    });
  }

  if (!manifest.scenarioIdsHash || !manifest.scenarioUniverseId) {
    failures.push({
      code: "MISSING_MANIFEST_HASH",
      class: SCENARIO_DIFFERENCE_CLASSES.COUNTING_OR_DENOMINATOR_DEFECT,
    });
  }

  const expectedObservations = count * (tier.providerCount || PROVIDERS.length);
  const compositionClasses = classifyScenarioUniverseComposition({
    standardCount: standard.length,
    propertyCount: specific.length,
    capabilityMergeCount,
    universeCount: count,
    resolvedMarketKey: marketKey,
  });

  const ok = failures.length === 0;
  return {
    ok,
    certificationVersion: ADP_SCENARIO_UNIVERSE_CERTIFICATION_V1,
    methodologyVersion: tier.methodologyVersion,
    propertyId: propertyProfile?.propertyId || null,
    resolvedMarketKey: marketKey,
    plannedScenarioCount: count,
    standardPackCount: standard.length,
    propertySpecificCount: specific.length,
    capabilityMergeCount,
    byIntent,
    bySource,
    assignedTiers,
    compositionClasses,
    manifest,
    expectedProviderObservations: expectedObservations,
    providers: [...(tier.providers || PROVIDERS)],
    failures,
    warnings,
    certifiedAt: new Date().toISOString(),
  };
}

/**
 * After measurement: reconcile planned vs executed vs archived.
 */
export function reconcileScenarioExecutionCoverage({
  certification,
  period,
  archiveReadback,
} = {}) {
  const failures = [];
  const plannedIds = new Set(certification?.manifest?.scenarioIds || []);
  const plannedCount = plannedIds.size || certification?.plannedScenarioCount || 0;

  const obs = period?.observations || [];
  const executedScenarioIds = [...new Set(obs.map((o) => o.scenarioId).filter(Boolean))];
  const executedProviders = [...new Set(obs.map((o) => o.provider).filter(Boolean))];
  const executedPairs = new Set(
    obs.map((o) => `${o.scenarioId}||${o.provider}`).filter((k) => !k.startsWith("||") && !k.endsWith("||"))
  );

  const expectedProviders = certification?.providers || [...PROVIDERS];
  const expectedObs = plannedCount * expectedProviders.length;
  const actualObs = obs.length;

  if (executedScenarioIds.length < plannedCount) {
    failures.push({
      code: "EXECUTED_SCENARIOS_BELOW_PLANNED",
      class: SCENARIO_DIFFERENCE_CLASSES.INCOMPLETE_PROVIDER_EXECUTION,
      detail: {
        planned: plannedCount,
        executedUnique: executedScenarioIds.length,
      },
    });
  }

  if (actualObs < Math.floor(expectedObs * 0.85)) {
    failures.push({
      code: "PROVIDER_OBSERVATION_COVERAGE_INCOMPLETE",
      class: SCENARIO_DIFFERENCE_CLASSES.INCOMPLETE_PROVIDER_EXECUTION,
      detail: { expectedObs, actualObs },
    });
  }

  const archivedCount =
    archiveReadback?.archivedObservationCount ??
    archiveReadback?.observationCount ??
    null;
  if (archiveReadback && archivedCount != null && archivedCount < actualObs) {
    failures.push({
      code: "ARCHIVE_BELOW_EXECUTED",
      class: SCENARIO_DIFFERENCE_CLASSES.ARCHIVE_EVIDENCE_MISSING,
      detail: { archivedCount, actualObs },
    });
  }
  if (period && !archiveReadback?.ok && archiveReadback != null) {
    failures.push({
      code: "ARCHIVE_READBACK_FAILED",
      class: SCENARIO_DIFFERENCE_CLASSES.ARCHIVE_EVIDENCE_MISSING,
    });
  }

  const periodManifestCount = period?.scenarioUniverse?.scenarioCount ?? period?.scenarioIds?.length;
  if (
    periodManifestCount != null &&
    plannedCount > 0 &&
    Number(periodManifestCount) !== plannedCount
  ) {
    failures.push({
      code: "PERIOD_MANIFEST_COUNT_DRIFT",
      class: SCENARIO_DIFFERENCE_CLASSES.HISTORICAL_METHODOLOGY_VERSION,
      detail: { periodManifestCount, plannedCount },
    });
  }

  return {
    ok: failures.length === 0,
    plannedScenarioCount: plannedCount,
    executedUniqueScenarioCount: executedScenarioIds.length,
    executedObservationCount: actualObs,
    expectedObservationCount: expectedObs,
    executedProviders,
    executedPairCount: executedPairs.size,
    archivedObservationCount: archivedCount,
    failures,
    fullyMeasured: failures.length === 0,
  };
}

export function estimateRemeasurementCostUsd(scenarioCount, options = {}) {
  const providers = options.providers || PROVIDERS;
  const costs = { openai: 0.03, gemini: 0.02, perplexity: 0.05, claude: 0.03 };
  let core = 0;
  for (const p of providers) core += scenarioCount * (costs[p] || 0.03);
  const retryReserve = options.retryReservePct != null ? options.retryReservePct : 0.1;
  const withRetry = core * (1 + retryReserve);
  return {
    scenarioCount,
    providerCount: providers.length,
    expectedObservations: scenarioCount * providers.length,
    coreUsd: Math.round(core * 100) / 100,
    withRetryUsd: Math.round(withRetry * 100) / 100,
    authorizationRequired: true,
  };
}

export function methodologyDisclosureForPeriod(period, liveCertification) {
  const storedCount =
    period?.scenarioUniverse?.scenarioCount ??
    period?.scenarioIds?.length ??
    [...new Set((period?.observations || []).map((o) => o.scenarioId))].length;
  const liveCount = liveCertification?.plannedScenarioCount ?? null;
  const drift = liveCount != null && storedCount != null && storedCount !== liveCount;
  return {
    storedScenarioCount: storedCount,
    livePlannedScenarioCount: liveCount,
    methodologyVersion:
      period?.scenarioUniverseVersion ||
      period?.scenarioUniverse?.scenarioUniverseVersion ||
      liveCertification?.methodologyVersion ||
      null,
    densityTier: liveCertification?.assignedTiers || [],
    comparableToStandardTier: !drift && (liveCertification?.assignedTiers || []).includes(
      ADP_SCENARIO_DENSITY_TIER_STANDARD_V1.id
    ),
    disclosureRequired: drift || !(liveCertification?.assignedTiers || []).includes(
      ADP_SCENARIO_DENSITY_TIER_STANDARD_V1.id
    ),
    note: drift
      ? "Historical period scenario count differs from live certified universe — do not silently merge; disclose methodology version."
      : null,
  };
}

export { sha16 };
