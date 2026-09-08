/**
 * Monterrey Valle — first official ADP property baselines (JW + Westin).
 * Pattern: Hotel Phillips first-official-property baseline.
 * RESEARCH / MEASUREMENT — customer dropdown stays hidden until certify.
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
import { MONTERREY_VALLE_ENTITY_VERSION } from "../metrics/adp-property-entity-registries.js";
import {
  propertyCoreGovernanceReady,
  stabilizedCoreIdsForProperty,
} from "../metrics/property-core-governance-data.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { territoryLabelForIntent } from "../metrics/intent-territory-labels.js";
import { getCensusLinkEntry } from "../census-link-registry.js";

export const MONTERREY_VALLE_PROPERTY_IDS = Object.freeze([
  "adp_jw_marriott_monterrey_valle",
  "adp_westin_monterrey_valle",
]);

export const MONTERREY_VALLE_BASELINE_MARKERS = Object.freeze({
  adp_jw_marriott_monterrey_valle: "ADP_JW_MARRIOTT_MONTERREY_VALLE_BASELINE_PERIOD_001",
  adp_westin_monterrey_valle: "ADP_WESTIN_MONTERREY_VALLE_BASELINE_PERIOD_001",
});

export const MONTERREY_VALLE_COST_CAP_USD = 12;

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
    const ok = rows.filter((o) => !o.error && (o.rawResponse || o.parsed || o.dryRun)).length;
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

export function buildMonterreyVallePropertyPreflight(propertyId) {
  const measurementContractHash = loadFrozenContractHash();
  const profile = loadPropertyProfile(propertyId);
  const blockers = [];
  const baselineMarker = MONTERREY_VALLE_BASELINE_MARKERS[propertyId];

  if (!MONTERREY_VALLE_PROPERTY_IDS.includes(propertyId)) {
    blockers.push("UNKNOWN_MONTERREY_VALLE_PROPERTY");
  }
  if (!profile) blockers.push("PROPERTY_PROFILE_MISSING");
  if (!baselineMarker) blockers.push("BASELINE_MARKER_MISSING");

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
  if (!propertyCoreGovernanceReady(propertyId)) {
    blockers.push("CORE_GOVERNANCE_NOT_READY");
  }
  if (measurementContractHash !== EXPECTED_CONTRACT_HASH) {
    blockers.push("CONTRACT_HASH_MISMATCH");
  }
  if (!getCensusLinkEntry(propertyId)?.censusRecordId) {
    blockers.push("CENSUS_LINK_MISSING");
  }

  const cost = estimateCost(scenarios.length || 0);
  const calls = scenarios.length * PROVIDERS.length;
  const roundedTotal = Math.round(cost.total * 100) / 100;
  if (roundedTotal > MONTERREY_VALLE_COST_CAP_USD) blockers.push("COST_CAP_EXCEEDED");

  const dropdownLeak = listPropertyProfiles().some((p) => p.propertyId === propertyId);
  if (dropdownLeak && profile?.customerDropdownVisible !== true) {
    blockers.push("DROPDOWN_VISIBLE_BEFORE_CERTIFICATION");
  }

  const coreRows = [];
  for (const intent of Object.values(TRAVELER_INTENTS)) {
    const core = stabilizedCoreIdsForProperty(propertyId, intent);
    coreRows.push({
      TERRITORY: territoryLabelForIntent(intent),
      intent,
      CORE_COUNT: core.length,
      CORE_HOTELS: core,
      CERTIFICATION_READY: core.length >= 4,
      BLOCKER: core.length >= 4 ? null : core.length ? "BELOW_MIN_CORE_4_FOR_NUMERIC_BENCHMARK" : "NO_CORE_PEERS",
    });
  }

  const byIntent = {};
  for (const s of scenarios) byIntent[s.intent] = (byIntent[s.intent] || 0) + 1;

  const PREFLIGHT =
    blockers.length === 0 && roundedTotal <= MONTERREY_VALLE_COST_CAP_USD ? "PASS" : "FAIL";

  return {
    propertyId,
    CANONICAL_NAME: profile?.name || null,
    ROOMS: profile?.rooms ?? null,
    MANAGEMENT_COMPANY: profile?.managementCompany || null,
    CENSUS_RECORD_ID: getCensusLinkEntry(propertyId)?.censusRecordId || null,
    PROPERTY_SOURCE_TRUTH_COMPLETE: profile?.sourceGovernance?.propertySourceTruthComplete ? "YES" : "NO",
    measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
    measurementContractHash,
    CONTRACT_HASH_MATCH: measurementContractHash === EXPECTED_CONTRACT_HASH ? "PASS" : "FAIL",
    baselineMarker,
    STANDARD_SCENARIOS: std,
    PROPERTY_SPECIFIC_SCENARIOS: spec,
    TOTAL_SCENARIOS: scenarios.length,
    SCENARIOS_BY_INTENT: byIntent,
    TOTAL_PLANNED_CALLS: calls,
    ESTIMATED_COST_BY_PROVIDER: cost.byProvider,
    TOTAL_ESTIMATED_COST: roundedTotal,
    COST_CAP_USD: MONTERREY_VALLE_COST_CAP_USD,
    CORE: coreRows,
    blockers,
    PREFLIGHT,
  };
}

export async function executeMonterreyValleBaselinePeriod001({
  propertyId,
  dryRun = true,
  onProgress = null,
  certify = false,
} = {}) {
  if (!propertyId) throw new Error("propertyId required");
  const preflight = buildMonterreyVallePropertyPreflight(propertyId);
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
  const profile = loadPropertyProfile(propertyId);
  const scenarios = buildScenarioUniverse(profile);

  const period = await executeMonitoringPeriod({
    propertyId,
    scenarios,
    dryRun,
    providers,
    delayMsOverride: dryRun ? 0 : 200,
    checkpointEvery: 25,
    onProgress: onProgress
      ? (completed, total) => onProgress({ propertyId, completed, total })
      : null,
  });

  let finalPeriod = period;
  if (!dryRun) {
    finalPeriod = parsePeriodObservations(period, profile);
  }

  finalPeriod = attachFirstOfficialPropertyPeriodMetadata(finalPeriod, {
    measurementContractHash: preflight.measurementContractHash,
    baselineMarker: preflight.baselineMarker,
    baselineSequence: 1,
    scenarioUniverseVersion: "adp_scenario_universe_v1",
    entityResolutionVersion: MONTERREY_VALLE_ENTITY_VERSION,
    sourceGovernanceVersion: OWNED_SOURCE_CLASSIFICATION_VERSION,
    providerSet: providers,
    certified: certify === true,
    priorComparablePeriod: null,
  });
  finalPeriod.measurementContractVersion = MEASUREMENT_CONTRACT_VERSION;

  const completeness = summarizeProviderCompleteness(finalPeriod, scenarios.length, providers);

  let published = null;
  savePeriod(finalPeriod);
  if (!dryRun) {
    const bundle = buildPublishedSnapshotBundle({ period: finalPeriod, profile });
    if (bundle.ok) {
      published = savePublishedSnapshotBundle(bundle, { seed: false });
    }
  }

  const RUN_END = new Date().toISOString();
  const incompleteProviders = Object.entries(completeness.byProvider)
    .filter(([, v]) => v.expected > 0 && v.successful / v.expected < 0.8)
    .map(([p]) => p);

  const slug = propertyId.replace(/^adp_/, "").replace(/_/g, "-");
  const report = {
    ok: incompleteProviders.length === 0,
    status: incompleteProviders.length
      ? "PARTIAL_PROVIDER_COMPLETENESS"
      : dryRun
        ? "DRY_RUN_COMPLETE"
        : certify
          ? "CERTIFIED_COMPLETE"
          : "EXECUTION_COMPLETE",
    propertyId,
    PERIOD_MARKER: preflight.baselineMarker,
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
    join(outDir, `adp-${slug}-baseline-period-001-run.json`),
    JSON.stringify(report, null, 2) + "\n"
  );

  return report;
}
