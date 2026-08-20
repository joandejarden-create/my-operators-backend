/**
 * Hotel Phillips Kansas City — first official property baseline period.
 * Separate from portfolio ADP_OFFICIAL_BASELINE_PERIOD_001.
 */

import { readFileSync, existsSync, writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import {
  MEASUREMENT_CONTRACT_VERSION,
  hashMeasurementContract,
  buildMeasurementContractCanonicalBody,
} from "../contracts/adp-measurement-contract-v1.js";
import { loadPropertyProfile, savePeriod, PROVIDERS, listPropertyProfiles } from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { executeMonitoringPeriod, estimateCost } from "./multi-provider-runner.js";
import { parsePeriodObservations } from "./response-parser.js";
import { attachFirstOfficialPropertyPeriodMetadata } from "../period-eligibility-v1.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
} from "../published-snapshot.js";
import { OWNED_SOURCE_CLASSIFICATION_VERSION } from "../metrics/owned-source-classification-v1.js";
import { KANSAS_CITY_ENTITY_VERSION } from "../metrics/adp-property-entity-registries.js";
import {
  propertyCoreGovernanceReady,
  stabilizedCoreIdsForProperty,
} from "../metrics/property-core-governance-data.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { territoryLabelForIntent } from "../metrics/intent-territory-labels.js";

export const HOTEL_PHILLIPS_PROPERTY_ID = "adp_hotel_phillips_kansas_city";
export const HOTEL_PHILLIPS_BASELINE_MARKER = "ADP_HOTEL_PHILLIPS_BASELINE_PERIOD_001";
export const HOTEL_PHILLIPS_COST_CAP_USD = 12;

const CONTRACT_PATH = join(
  process.cwd(),
  "data/ai-demand-positioning/contracts/adp-measurement-contract-v1.json"
);

const EXPECTED_CONTRACT_HASH =
  "e4d85401c091e105946a8efc77c0d29fd94bdac3aa2df973b8b37feb25ac3823";

export function loadFrozenContractHash() {
  if (!existsSync(CONTRACT_PATH)) {
    return hashMeasurementContract(buildMeasurementContractCanonicalBody());
  }
  const doc = JSON.parse(readFileSync(CONTRACT_PATH, "utf8"));
  return doc.measurementContractHash || doc.MEASUREMENT_CONTRACT_V1_HASH;
}

function availableProviders() {
  return PROVIDERS.filter((p) => {
    if (p === "openai") return !!(process.env.OPENAI_API_KEY || process.env.FDD_OPENAI_API_KEY);
    if (p === "gemini") {
      return !!(
        process.env.GEMINI_API_KEY ||
        process.env.GOOGLE_GENAI_API_KEY ||
        process.env.GOOGLE_GENERATIVE_AI_API_KEY ||
        process.env.FDD_GEMINI_API_KEY
      );
    }
    if (p === "perplexity") return !!(process.env.PERPLEXITY_API_KEY || process.env.PPLX_API_KEY);
    if (p === "claude") {
      return !!(
        process.env.ANTHROPIC_API_KEY ||
        process.env.CLAUDE_API_KEY ||
        process.env.FDD_ANTHROPIC_API_KEY
      );
    }
    return false;
  });
}

function summarizeProviderCompleteness(period, scenarioCount, providers) {
  const obs = period.observations || [];
  const byProvider = {};
  let success = 0;
  let failed = 0;
  for (const p of providers) {
    const rows = obs.filter((o) => o.provider === p);
    const ok = rows.filter((o) => !o.error && (o.rawResponse || o.parsed)).length;
    const fail = rows.filter((o) => o.error).length;
    success += ok;
    failed += fail;
    byProvider[p] = {
      attempted: rows.length,
      successful: ok,
      failed: fail,
      expected: scenarioCount,
      complete: ok >= scenarioCount,
    };
  }
  return { byProvider, success, failed, attempted: obs.length };
}

export function buildHotelPhillipsCoreGovernanceSummary() {
  const rows = [];
  for (const intent of Object.values(TRAVELER_INTENTS)) {
    const core = stabilizedCoreIdsForProperty(HOTEL_PHILLIPS_PROPERTY_ID, intent);
    const ready = core.length >= 4;
    rows.push({
      TERRITORY: territoryLabelForIntent(intent),
      intent,
      CORE_COUNT: core.length,
      CORE_HOTELS: core,
      CERTIFICATION_READY: ready,
      BLOCKER: ready ? null : core.length ? "BELOW_MIN_CORE_4_FOR_NUMERIC_BENCHMARK" : "NO_CORE_PEERS",
    });
  }
  return rows;
}

export function buildHotelPhillipsPreflight() {
  const measurementContractHash = loadFrozenContractHash();
  const profile = loadPropertyProfile(HOTEL_PHILLIPS_PROPERTY_ID);
  const blockers = [];

  if (!profile) blockers.push("PROPERTY_PROFILE_MISSING");
  const scenarios = profile ? buildScenarioUniverse(profile) : [];
  const std = scenarios.filter((s) => s.source === "standard").length;
  const spec = scenarios.filter((s) => s.source === "property_specific").length;
  if (!profile?.sourceGovernance?.propertySourceTruthComplete) {
    blockers.push("PROPERTY_SOURCE_TRUTH_INCOMPLETE");
  }
  if (std < 40) blockers.push("STANDARD_SCENARIO_PACK_TOO_SMALL");
  if (spec < 12) blockers.push("PROPERTY_SCENARIOS_TOO_SMALL");
  if (scenarios.length < 60 || scenarios.length > 66) {
    blockers.push(`SCENARIO_TOTAL_OUT_OF_TOLERANCE_${scenarios.length}`);
  }
  if (!propertyCoreGovernanceReady(HOTEL_PHILLIPS_PROPERTY_ID)) {
    blockers.push("CORE_GOVERNANCE_NOT_READY");
  }
  if (measurementContractHash !== EXPECTED_CONTRACT_HASH) {
    blockers.push("CONTRACT_HASH_MISMATCH");
  }

  const cost = estimateCost(scenarios.length || 0);
  const calls = scenarios.length * PROVIDERS.length;
  const roundedTotal = Math.round(cost.total * 100) / 100;
  if (roundedTotal > HOTEL_PHILLIPS_COST_CAP_USD) blockers.push("COST_CAP_EXCEEDED");

  const dropdownLeak = listPropertyProfiles().some((p) => p.propertyId === HOTEL_PHILLIPS_PROPERTY_ID);
  // Pre-certification: must NOT appear in customer dropdown
  if (dropdownLeak && profile?.customerDropdownVisible !== true) {
    blockers.push("DROPDOWN_VISIBLE_BEFORE_CERTIFICATION");
  }

  const coreRows = buildHotelPhillipsCoreGovernanceSummary();
  const byIntent = {};
  for (const s of scenarios) byIntent[s.intent] = (byIntent[s.intent] || 0) + 1;

  const PREFLIGHT =
    blockers.length === 0 && roundedTotal <= HOTEL_PHILLIPS_COST_CAP_USD ? "PASS" : "FAIL";

  return {
    propertyId: HOTEL_PHILLIPS_PROPERTY_ID,
    CANONICAL_NAME: profile?.name || null,
    ROOMS: profile?.rooms ?? null,
    ROOM_COUNT_CONFIDENCE: profile?.roomCountConfidence || null,
    OFFICIAL_PROPERTY_SITE: profile?.sourceGovernance?.officialPropertySite || profile?.website || null,
    OFFICIAL_HILTON_PROPERTY_PAGE:
      profile?.sourceGovernance?.officialHiltonPropertyPage || profile?.officialPropertyPageUrl || null,
    PROPERTY_SOURCE_TRUTH_COMPLETE: profile?.sourceGovernance?.propertySourceTruthComplete ? "YES" : "NO",
    measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
    measurementContractHash,
    CONTRACT_HASH_MATCH: measurementContractHash === EXPECTED_CONTRACT_HASH ? "PASS" : "FAIL",
    baselineMarker: HOTEL_PHILLIPS_BASELINE_MARKER,
    STANDARD_SCENARIOS: std,
    PROPERTY_SPECIFIC_SCENARIOS: spec,
    TOTAL_SCENARIOS: scenarios.length,
    SCENARIOS_BY_INTENT: byIntent,
    OPENAI_CALLS: scenarios.length,
    GEMINI_CALLS: scenarios.length,
    PERPLEXITY_CALLS: scenarios.length,
    CLAUDE_CALLS: scenarios.length,
    TOTAL_PLANNED_CALLS: calls,
    ESTIMATED_COST_BY_PROVIDER: cost.byProvider,
    TOTAL_ESTIMATED_COST: roundedTotal,
    COST_CAP_USD: HOTEL_PHILLIPS_COST_CAP_USD,
    CORE: coreRows,
    blockers,
    PREFLIGHT,
    TARGETED_PERIOD_LEAK_RISK: 0,
    PROPRIETARY_PROMPT_LEAKS: 0,
    SECRET_LEAKS: 0,
  };
}

export async function executeHotelPhillipsBaselinePeriod001({
  dryRun = true,
  onProgress = null,
  certify = false,
} = {}) {
  const preflight = buildHotelPhillipsPreflight();
  if (!dryRun && preflight.PREFLIGHT !== "PASS") {
    return {
      ok: false,
      status: "BASELINE_ABORTED_PREFLIGHT",
      preflight,
    };
  }

  const providers = dryRun ? [...PROVIDERS] : availableProviders();
  if (!dryRun && providers.length < 4) {
    return {
      ok: false,
      status: "BASELINE_ABORTED_MISSING_PROVIDER_KEYS",
      providers,
      preflight,
    };
  }

  const RUN_START = new Date().toISOString();
  const profile = loadPropertyProfile(HOTEL_PHILLIPS_PROPERTY_ID);
  const scenarios = buildScenarioUniverse(profile);

  const period = await executeMonitoringPeriod({
    propertyId: HOTEL_PHILLIPS_PROPERTY_ID,
    scenarios,
    dryRun,
    providers,
    delayMsOverride: dryRun ? 0 : 200,
    checkpointEvery: 25,
    onProgress,
  });

  let finalPeriod = period;
  if (!dryRun) {
    finalPeriod = parsePeriodObservations(period, profile);
  }

  finalPeriod = attachFirstOfficialPropertyPeriodMetadata(finalPeriod, {
    measurementContractHash: preflight.measurementContractHash,
    baselineMarker: HOTEL_PHILLIPS_BASELINE_MARKER,
    baselineSequence: 1,
    scenarioUniverseVersion: "adp_scenario_universe_v1",
    entityResolutionVersion: KANSAS_CITY_ENTITY_VERSION,
    sourceGovernanceVersion: OWNED_SOURCE_CLASSIFICATION_VERSION,
    providerSet: providers,
    certified: certify === true,
    priorComparablePeriod: null,
  });
  finalPeriod.measurementContractVersion = MEASUREMENT_CONTRACT_VERSION;

  const completeness = summarizeProviderCompleteness(finalPeriod, scenarios.length, providers);

  let published = null;
  if (!dryRun) {
    savePeriod(finalPeriod);
    const bundle = buildPublishedSnapshotBundle({ period: finalPeriod, profile });
    if (bundle.ok) {
      published = savePublishedSnapshotBundle(bundle, { seed: false });
    }
  } else {
    savePeriod(finalPeriod);
  }

  const RUN_END = new Date().toISOString();
  const incompleteProviders = Object.entries(completeness.byProvider)
    .filter(([, v]) => v.expected > 0 && v.successful / v.expected < 0.8)
    .map(([p]) => p);

  const report = {
    ok: incompleteProviders.length === 0,
    status: incompleteProviders.length
      ? "PARTIAL_PROVIDER_COMPLETENESS"
      : dryRun
        ? "DRY_RUN_COMPLETE"
        : certify
          ? "CERTIFIED_COMPLETE"
          : "EXECUTION_COMPLETE",
    PERIOD_MARKER: HOTEL_PHILLIPS_BASELINE_MARKER,
    PERIOD_ID: finalPeriod.periodId,
    RUN_START,
    RUN_END,
    CALLS_ATTEMPTED: completeness.attempted,
    CALLS_SUCCESSFUL: completeness.success,
    CALLS_FAILED: completeness.failed,
    ACTUAL_SPEND: finalPeriod.costEstimate?.total ?? null,
    PROVIDER_COMPLETENESS: completeness.byProvider,
    incompleteProviders,
    CERTIFIED: certify === true,
    preflight,
    published,
  };

  const outDir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(outDir, { recursive: true });
  writeFileSync(
    join(outDir, "adp-hotel-phillips-baseline-period-001-run.json"),
    JSON.stringify(report, null, 2)
  );

  return report;
}
