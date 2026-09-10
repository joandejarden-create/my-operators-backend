/**
 * Bethesda Marriott — first property baseline period (LIVE authorized).
 * RESEARCH / MEASUREMENT — DO NOT publish; customer dropdown stays hidden.
 *
 * Doctrine: METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 */

import { readFileSync, existsSync, writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import {
  MEASUREMENT_CONTRACT_VERSION,
  hashMeasurementContract,
  buildMeasurementContractCanonicalBody,
} from "../contracts/adp-measurement-contract-v1.js";
import { loadPropertyProfile, savePeriod, PROVIDERS, listPropertyProfiles } from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { executeMonitoringPeriod, estimateCost } from "./multi-provider-runner.js";
import { parsePeriodObservations } from "./response-parser.js";
import { applyGovernedInterpretation } from "../subject-presence/canonical-subject-presence-v1.js";
import { attachFirstOfficialPropertyPeriodMetadata } from "../period-eligibility-v1.js";
import { OWNED_SOURCE_CLASSIFICATION_VERSION } from "../metrics/owned-source-classification-v1.js";
import {
  propertyCoreGovernanceReady,
  stabilizedCoreIdsForProperty,
} from "../metrics/property-core-governance-data.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { territoryLabelForIntent } from "../metrics/intent-territory-labels.js";
import { BETHESDA_MONTGOMERY_ENTITY_VERSION } from "../metrics/bethesda-montgomery-entity-registry.js";
import { assertBethesdaMarriottVsNorthDistinctEntity } from "../metrics/bethesda-montgomery-entity-registry.js";
import { getCensusLinkEntry } from "../census-link-registry.js";
import { getEntityRegistryForProperty } from "../metrics/adp-property-entity-registries.js";
import {
  BETHESDA_ADP_PROPERTY_ID,
  BETHESDA_MARRIOTT_COST_CAP_USD,
  BETHESDA_IDENTITY_KEY,
} from "./bethesda-marriott-foundation-v1.js";

export const BETHESDA_PROPERTY_ID = BETHESDA_ADP_PROPERTY_ID;
export const BETHESDA_BASELINE_MARKER = "ADP_BETHESDA_MARRIOTT_BASELINE_PERIOD_001";
export const BETHESDA_COST_CAP_USD = BETHESDA_MARRIOTT_COST_CAP_USD;

const CONTRACT_PATH = join(
  process.cwd(),
  "data/ai-demand-positioning/contracts/adp-measurement-contract-v1.json"
);

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
  return { byProvider, success, failed, attempted: obs.length, retryCalls: 0 };
}

export function buildBethesdaCoreGovernanceSummary() {
  const rows = [];
  for (const intent of Object.values(TRAVELER_INTENTS)) {
    const core = stabilizedCoreIdsForProperty(BETHESDA_PROPERTY_ID, intent);
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

export function buildBethesdaMarriottBaselinePreflight() {
  const measurementContractHash = loadFrozenContractHash();
  const profile = loadPropertyProfile(BETHESDA_PROPERTY_ID);
  const blockers = [];
  const censusLink = getCensusLinkEntry(BETHESDA_PROPERTY_ID);
  const entityRegistry = getEntityRegistryForProperty(BETHESDA_PROPERTY_ID);
  const distinct = assertBethesdaMarriottVsNorthDistinctEntity();

  if (!profile) blockers.push("PROPERTY_PROFILE_MISSING");
  if (!censusLink?.censusRecordId) blockers.push("CENSUS_LINK_MISSING");
  if (!entityRegistry) blockers.push("ENTITY_REGISTRY_MISSING");
  if (!distinct.pass) blockers.push("BETHESDA_NORTH_DISTINCT_ENTITY_REGRESSION_FAIL");
  if (!propertyCoreGovernanceReady(BETHESDA_PROPERTY_ID)) blockers.push("CORE_GOVERNANCE_NOT_READY");

  const scenarios = profile ? buildScenarioUniverse(profile) : [];
  const std = scenarios.filter((s) => s.source === "standard").length;
  const spec = scenarios.filter((s) => s.source === "property_specific").length;
  if (std < 40) blockers.push("STANDARD_SCENARIO_PACK_TOO_SMALL");
  if (spec < 12) blockers.push("PROPERTY_SCENARIOS_TOO_SMALL");
  if (scenarios.length < 60 || scenarios.length > 66) {
    blockers.push(`SCENARIO_TOTAL_OUT_OF_TOLERANCE_${scenarios.length}`);
  }

  const cost = estimateCost(scenarios.length || 0);
  const calls = scenarios.length * PROVIDERS.length;
  const roundedTotal = Math.round(cost.total * 100) / 100;
  if (roundedTotal > BETHESDA_COST_CAP_USD) blockers.push("COST_CAP_EXCEEDED");

  const dropdownLeak = listPropertyProfiles().some((p) => p.propertyId === BETHESDA_PROPERTY_ID);
  if (dropdownLeak) blockers.push("DROPDOWN_LIST_LEAK_BEFORE_CERTIFICATION");

  const coreRows = buildBethesdaCoreGovernanceSummary();
  const byIntent = {};
  for (const s of scenarios) byIntent[s.intent] = (byIntent[s.intent] || 0) + 1;

  const PREFLIGHT =
    blockers.length === 0 && roundedTotal <= BETHESDA_COST_CAP_USD ? "PASS" : "FAIL";

  return {
    propertyId: BETHESDA_PROPERTY_ID,
    CANONICAL_NAME: profile?.name || null,
    CANONICAL_HOTEL_ID: BETHESDA_IDENTITY_KEY,
    CENSUS_ID: censusLink?.censusRecordId || null,
    MARKET: profile?.market || null,
    ROOMS: profile?.rooms ?? null,
    ROOM_COUNT_CONFIDENCE: profile?.roomCountConfidence || null,
    measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
    measurementContractHash,
    baselineMarker: BETHESDA_BASELINE_MARKER,
    STANDARD_SCENARIOS: std,
    PROPERTY_SPECIFIC_SCENARIOS: spec,
    TOTAL_SCENARIOS: scenarios.length,
    SCENARIOS_BY_INTENT: byIntent,
    TOTAL_PLANNED_CALLS: calls,
    ESTIMATED_COST_BY_PROVIDER: cost.byProvider,
    TOTAL_ESTIMATED_COST: roundedTotal,
    COST_CAP_USD: BETHESDA_COST_CAP_USD,
    CORE: coreRows,
    DISTINCT_ENTITY: distinct,
    customerDropdownVisible: profile?.customerDropdownVisible === true,
    publishAuthorized: false,
    blockers,
    PREFLIGHT,
  };
}

/**
 * Execute Bethesda baseline. Never writes to published/ universe in this task.
 */
export async function executeBethesdaMarriottBaselinePeriod001({
  dryRun = true,
  onProgress = null,
  certify = false,
} = {}) {
  const preflight = buildBethesdaMarriottBaselinePreflight();
  if (!dryRun && preflight.PREFLIGHT !== "PASS") {
    return { ok: false, status: "BASELINE_ABORTED_PREFLIGHT", preflight };
  }
  if (preflight.TOTAL_ESTIMATED_COST > BETHESDA_COST_CAP_USD) {
    return { ok: false, status: "BASELINE_ABORTED_COST_CAP", preflight };
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
  const profile = loadPropertyProfile(BETHESDA_PROPERTY_ID);
  const scenarios = buildScenarioUniverse(profile);

  const period = await executeMonitoringPeriod({
    propertyId: BETHESDA_PROPERTY_ID,
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
    // Persist Path A governedInterpretation (required for MA / SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH).
    const ts = new Date().toISOString();
    finalPeriod = {
      ...finalPeriod,
      observations: (finalPeriod.observations || []).map((obs) =>
        applyGovernedInterpretation(obs, profile, {
          timestamp: ts,
          correctionReason: "CANONICAL_OPTION_A_BETHESDA_BASELINE_PARSE",
        })
      ),
      governedSubjectPresenceAppliedAt: ts,
      governedSubjectPresenceVersion: "adp_bethesda_marriott_baseline_period_001_path_a",
    };
  }

  finalPeriod = attachFirstOfficialPropertyPeriodMetadata(finalPeriod, {
    measurementContractHash: preflight.measurementContractHash,
    baselineMarker: BETHESDA_BASELINE_MARKER,
    baselineSequence: 1,
    scenarioUniverseVersion: "adp_scenario_universe_v1",
    entityResolutionVersion: BETHESDA_MONTGOMERY_ENTITY_VERSION,
    sourceGovernanceVersion: OWNED_SOURCE_CLASSIFICATION_VERSION,
    providerSet: providers,
    certified: false,
    priorComparablePeriod: null,
  });

  // HARD: unpublished / undistributed until separate founder publication auth
  finalPeriod.customerVisible = false;
  finalPeriod.customerTrendEligible = false;
  finalPeriod.externalShareDistributed = false;
  finalPeriod.publicationBlocked = true;
  finalPeriod.publicationBlockReason = "FOUNDER_REVIEW_REQUIRED_BEFORE_PUBLISH";
  finalPeriod.measurementContractVersion = MEASUREMENT_CONTRACT_VERSION;
  if (certify === true) {
    finalPeriod.measurementCertified = true;
    finalPeriod.measurementCertifiedAt = new Date().toISOString();
  }

  const completeness = summarizeProviderCompleteness(finalPeriod, scenarios.length, providers);
  const actualCost = Math.round((finalPeriod.costEstimate?.total || 0) * 100) / 100;
  if (actualCost > BETHESDA_COST_CAP_USD) {
    finalPeriod.status = "COST_CAP_BREACH";
  }

  savePeriod(finalPeriod);

  const RUN_END = new Date().toISOString();
  const incompleteProviders = Object.entries(completeness.byProvider)
    .filter(([, v]) => v.expected > 0 && v.successful / v.expected < 0.8)
    .map(([p]) => p);

  const report = {
    ok: incompleteProviders.length === 0 && actualCost <= BETHESDA_COST_CAP_USD,
    status: incompleteProviders.length
      ? "PARTIAL_PROVIDER_COMPLETENESS"
      : dryRun
        ? "DRY_RUN_COMPLETE"
        : "EXECUTION_COMPLETE_UNPUBLISHED",
    PERIOD_MARKER: BETHESDA_BASELINE_MARKER,
    PERIOD_ID: finalPeriod.periodId,
    RUN_START,
    RUN_END,
    CALLS_ATTEMPTED: completeness.attempted,
    CALLS_SUCCESSFUL: completeness.success,
    CALLS_FAILED: completeness.failed,
    RETRY_CALLS: completeness.retryCalls,
    EXPECTED_CALLS: scenarios.length * providers.length,
    EXPECTED_COST: preflight.TOTAL_ESTIMATED_COST,
    ACTUAL_SPEND: actualCost,
    HARD_COST_CAP: BETHESDA_COST_CAP_USD,
    CAP_RESPECTED: actualCost <= BETHESDA_COST_CAP_USD,
    PROVIDER_COMPLETENESS: completeness.byProvider,
    incompleteProviders,
    PUBLISHED: false,
    PUBLISHED_UNIVERSE_COUNT_EXPECTED: 13,
    customerDropdownVisible: false,
    CERTIFIED: false,
    preflight,
  };

  const outDir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(outDir, { recursive: true });
  writeFileSync(
    join(outDir, "adp-bethesda-marriott-baseline-period-001-run.json"),
    JSON.stringify(report, null, 2)
  );

  return report;
}
