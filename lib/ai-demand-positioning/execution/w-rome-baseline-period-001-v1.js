/**
 * W Rome — first official ADP property baseline period.
 * Hotel-instance execution; uses certified peer pack + scenario universe from
 * buildScenarioUniverse (generic parity pack when no Rome market pack exists).
 *
 * Historical period 001 used thin generic v1 (20 × 4 = 80). Corrected methodology
 * targets ~63 scenarios (generic parity v2); cost cap raised accordingly.
 */

import { readFileSync, existsSync, writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import {
  MEASUREMENT_CONTRACT_VERSION,
  hashMeasurementContract,
  buildMeasurementContractCanonicalBody,
} from "../contracts/adp-measurement-contract-v1.js";
import { loadPropertyProfile, savePeriod, PROVIDERS } from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { executeMonitoringPeriod, estimateCost } from "./multi-provider-runner.js";
import { parsePeriodObservations } from "./response-parser.js";
import { attachFirstOfficialPropertyPeriodMetadata } from "../period-eligibility-v1.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
} from "../published-snapshot.js";
import { OWNED_SOURCE_CLASSIFICATION_VERSION } from "../metrics/owned-source-classification-v1.js";
import {
  propertyCoreGovernanceReady,
  stabilizedCoreIdsForProperty,
} from "../metrics/property-core-governance-data.js";
import { getBrandPortfolioPeerSet } from "../brand-portfolio/brand-portfolio-peer-set-v1.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { territoryLabelForIntent } from "../metrics/intent-territory-labels.js";

export const W_ROME_PROPERTY_ID = "adp_w_rome";
export const W_ROME_BASELINE_MARKER = "ADP_W_ROME_BASELINE_PERIOD_001";
export const W_ROME_COST_CAP_USD = 12;
export const W_ROME_ENTITY_VERSION = "adp_w_rome_entity_v1";

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

function bleedScan(scenarios) {
  const bleed = [];
  const re = /\b(bethesda|nih|dmv|renaissance|times square|manhattan|new york)\b/i;
  for (const s of scenarios) {
    if (re.test(s.query || "")) bleed.push(s.scenarioId);
  }
  return bleed;
}

export function buildWRomePreflight() {
  const measurementContractHash = loadFrozenContractHash();
  const profile = loadPropertyProfile(W_ROME_PROPERTY_ID);
  const peers = getBrandPortfolioPeerSet(W_ROME_PROPERTY_ID);
  const blockers = [];

  if (!profile) blockers.push("PROPERTY_PROFILE_MISSING");
  if (profile?.censusRecordId !== "rece0or38cxo3Fymb") blockers.push("CENSUS_LINK_MISSING");
  const scenarios = profile ? buildScenarioUniverse(profile) : [];
  if (scenarios.length < 15) blockers.push(`SCENARIO_TOO_SMALL_${scenarios.length}`);
  const bleed = bleedScan(scenarios);
  if (bleed.length) blockers.push(`SCENARIO_BLEED_${bleed.join(",")}`);
  if (!peers || peers.adequacy?.status !== "ADEQUATE") {
    blockers.push("PEER_PACK_NOT_ADEQUATE");
  }
  if (!propertyCoreGovernanceReady(W_ROME_PROPERTY_ID)) {
    blockers.push("CORE_GOVERNANCE_NOT_READY");
  }
  if (profile?.stewardship?.peerPackStatus !== "CERTIFIED") {
    blockers.push("PEER_PACK_NOT_CERTIFIED");
  }

  const providers = availableProviders();
  const cost = estimateCost(scenarios.length || 0, providers.length ? providers : PROVIDERS);
  const calls = scenarios.length * (providers.length || PROVIDERS.length);
  const roundedTotal = Math.round(cost.total * 100) / 100;
  if (roundedTotal > W_ROME_COST_CAP_USD) blockers.push("COST_CAP_EXCEEDED");

  const coreRows = Object.values(TRAVELER_INTENTS).map((intent) => {
    const core = stabilizedCoreIdsForProperty(W_ROME_PROPERTY_ID, intent);
    return {
      TERRITORY: territoryLabelForIntent(intent),
      intent,
      CORE_COUNT: core.length,
      CORE_HOTELS: core,
      CERTIFICATION_READY: core.length >= 4,
    };
  });

  const PREFLIGHT = blockers.length === 0 ? "PASS" : "FAIL";

  return {
    propertyId: W_ROME_PROPERTY_ID,
    CANONICAL_NAME: profile?.name || null,
    CENSUS: profile?.censusRecordId || null,
    ROOMS: profile?.rooms ?? null,
    measurementContractHash,
    baselineMarker: W_ROME_BASELINE_MARKER,
    TOTAL_SCENARIOS: scenarios.length,
    SCENARIO_SOURCES: scenarios.reduce((a, s) => {
      a[s.source || "?"] = (a[s.source || "?"] || 0) + 1;
      return a;
    }, {}),
    PEER_COUNT: peers?.included?.length || 0,
    PEER_ADEQUACY: peers?.adequacy?.status || null,
    AVAILABLE_PROVIDERS: providers,
    TOTAL_PLANNED_CALLS: calls,
    TOTAL_ESTIMATED_COST: roundedTotal,
    COST_CAP_USD: W_ROME_COST_CAP_USD,
    CORE: coreRows,
    bleedScenarioIds: bleed,
    blockers,
    PREFLIGHT,
  };
}

export async function executeWRomeBaselinePeriod001({
  dryRun = true,
  onProgress = null,
  certify = false,
} = {}) {
  const preflight = buildWRomePreflight();
  if (!dryRun && preflight.PREFLIGHT !== "PASS") {
    return { ok: false, status: "BASELINE_ABORTED_PREFLIGHT", preflight };
  }

  const providers = dryRun ? [...PROVIDERS] : availableProviders();
  if (!dryRun && providers.length < 2) {
    return {
      ok: false,
      status: "BASELINE_ABORTED_MISSING_PROVIDER_KEYS",
      providers,
      preflight,
    };
  }

  const RUN_START = new Date().toISOString();
  const profile = loadPropertyProfile(W_ROME_PROPERTY_ID);
  const scenarios = buildScenarioUniverse(profile);

  const period = await executeMonitoringPeriod({
    propertyId: W_ROME_PROPERTY_ID,
    scenarios,
    dryRun,
    providers,
    delayMsOverride: dryRun ? 0 : 200,
    checkpointEvery: 20,
    onProgress: onProgress
      ? (completed, total) => onProgress({ completed, total })
      : null,
  });

  let finalPeriod = period;
  if (!dryRun) {
    finalPeriod = parsePeriodObservations(period, profile);
  }

  finalPeriod = attachFirstOfficialPropertyPeriodMetadata(finalPeriod, {
    measurementContractHash: preflight.measurementContractHash,
    baselineMarker: W_ROME_BASELINE_MARKER,
    baselineSequence: 1,
    scenarioUniverseVersion: "adp_generic_profile_scenarios_v1",
    entityResolutionVersion: W_ROME_ENTITY_VERSION,
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
    } else {
      return {
        ok: false,
        status: "PUBLISH_BUNDLE_FAILED",
        bundleError: bundle.message || bundle,
        PERIOD_ID: finalPeriod.periodId,
        preflight,
        PROVIDER_COMPLETENESS: completeness.byProvider,
      };
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
    PERIOD_MARKER: W_ROME_BASELINE_MARKER,
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
    join(outDir, "adp-w-rome-baseline-period-001-run.json"),
    JSON.stringify(report, null, 2)
  );

  return report;
}
