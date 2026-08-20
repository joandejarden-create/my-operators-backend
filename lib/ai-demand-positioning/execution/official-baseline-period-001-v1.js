/**
 * Official ADP Baseline Period 001 — synchronized full-property measurement.
 */

import { readFileSync, existsSync, writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import {
  ADP_PROPERTY_IDS_V1,
  MEASUREMENT_CONTRACT_VERSION,
  OFFICIAL_BASELINE_PERIOD_MARKER,
  hashMeasurementContract,
  buildMeasurementContractCanonicalBody,
} from "../contracts/adp-measurement-contract-v1.js";
import { loadPropertyProfile, savePeriod, PROVIDERS } from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import {
  executeMonitoringPeriod,
  estimateCost,
  PROVIDER_CONFIGS,
} from "./multi-provider-runner.js";
import { parsePeriodObservations } from "./response-parser.js";
import { attachOfficialBaselinePeriodMetadata } from "../period-eligibility-v1.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
} from "../published-snapshot.js";
import { OWNED_SOURCE_CLASSIFICATION_VERSION } from "../metrics/owned-source-classification-v1.js";
import { ENTITY_RESOLUTION_VERSION } from "../metrics/south-florida-entity-registry.js";

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

export function buildOfficialBaselinePreflight() {
  const measurementContractHash = loadFrozenContractHash();
  const rows = [];
  let totalCalls = 0;
  let totalCost = 0;
  const byProvider = { openai: 0, gemini: 0, perplexity: 0, claude: 0 };

  for (const propertyId of ADP_PROPERTY_IDS_V1) {
    const profile = loadPropertyProfile(propertyId);
    if (!profile) {
      rows.push({ propertyId, error: "NO_PROFILE" });
      continue;
    }
    const scenarios = buildScenarioUniverse(profile);
    const std = scenarios.filter((s) => s.source === "standard").length;
    const spec = scenarios.filter((s) => s.source === "property_specific").length;
    const cost = estimateCost(scenarios.length);
    const calls = scenarios.length * PROVIDERS.length;
    totalCalls += calls;
    totalCost += cost.total;
    for (const p of PROVIDERS) byProvider[p] += cost.byProvider[p] || 0;
    rows.push({
      PROPERTY: profile.name,
      propertyId,
      STANDARD_SCENARIOS: std,
      PROPERTY_SPECIFIC_SCENARIOS: spec,
      TOTAL_APPLICABLE_SCENARIOS: scenarios.length,
      PROVIDER_COUNT: PROVIDERS.length,
      PLANNED_PROVIDER_CALLS: calls,
      ESTIMATED_COST: cost.total,
      ESTIMATED_COST_BY_PROVIDER: cost.byProvider,
    });
  }

  const roundedTotal = Math.round(totalCost * 100) / 100;
  return {
    measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
    measurementContractHash,
    baselineMarker: OFFICIAL_BASELINE_PERIOD_MARKER,
    PROPERTIES: rows.length,
    rows,
    TOTAL_SCENARIOS: rows.reduce((n, r) => n + (r.TOTAL_APPLICABLE_SCENARIOS || 0), 0),
    TOTAL_PLANNED_PROVIDER_CALLS: totalCalls,
    ESTIMATED_COST_BY_PROVIDER: Object.fromEntries(
      Object.entries(byProvider).map(([k, v]) => [k, Math.round(v * 100) / 100])
    ),
    ESTIMATED_COST_BY_PROPERTY: Object.fromEntries(
      rows.filter((r) => !r.error).map((r) => [r.propertyId, r.ESTIMATED_COST])
    ),
    TOTAL_ESTIMATED_COST: roundedTotal,
    COST_CAP_USD: 40,
    PREFLIGHT: roundedTotal <= 40 ? "PASS" : "FAIL_COST_APPROVAL_REQUIRED",
  };
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

export async function executeOfficialBaselinePeriod001({
  dryRun = true,
  onProgress = null,
  certify = false,
} = {}) {
  const preflight = buildOfficialBaselinePreflight();
  if (!dryRun && preflight.PREFLIGHT !== "PASS") {
    return {
      ok: false,
      status: "BASELINE_ABORTED_COST_GATE",
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
  const propertyResults = [];
  let callsAttempted = 0;
  let callsSuccessful = 0;
  let callsFailed = 0;
  let actualSpend = 0;

  async function runOneProperty(propertyId) {
    const profile = loadPropertyProfile(propertyId);
    const scenarios = buildScenarioUniverse(profile);
    const PROPERTY_START = new Date().toISOString();

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

    finalPeriod = attachOfficialBaselinePeriodMetadata(finalPeriod, {
      measurementContractHash: preflight.measurementContractHash,
      scenarioUniverseVersion: "adp_scenario_universe_v1",
      entityResolutionVersion: ENTITY_RESOLUTION_VERSION,
      sourceGovernanceVersion: OWNED_SOURCE_CLASSIFICATION_VERSION,
      providerSet: providers,
      certified: false,
    });
    finalPeriod.baselineMarker = OFFICIAL_BASELINE_PERIOD_MARKER;
    finalPeriod.measurementContractVersion = MEASUREMENT_CONTRACT_VERSION;

    const completeness = summarizeProviderCompleteness(
      finalPeriod,
      scenarios.length,
      providers
    );

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

    const PROPERTY_END = new Date().toISOString();
    const incompleteProviders = Object.entries(completeness.byProvider)
      .filter(([, v]) => v.expected > 0 && v.successful / v.expected < 0.8)
      .map(([p]) => p);

    return {
      PROPERTY: profile.name,
      propertyId,
      PERIOD_ID: finalPeriod.periodId,
      SCENARIOS: scenarios.length,
      COMPARABLE_OBSERVATIONS: completeness.success,
      PROVIDER_COMPLETENESS: completeness.byProvider,
      incompleteProviders,
      CERTIFIED: false,
      STATUS: incompleteProviders.length
        ? "PARTIAL_PROVIDER_COMPLETENESS"
        : dryRun
          ? "DRY_RUN_COMPLETE"
          : "EXECUTION_COMPLETE",
      PROPERTY_START,
      PROPERTY_END,
      published,
      cost: finalPeriod.costEstimate?.total || null,
      completeness,
    };
  }

  // Synchronized portfolio window: run two properties at a time (tight wall-clock, shared epoch).
  const queue = [...ADP_PROPERTY_IDS_V1];
  const concurrency = dryRun ? 4 : 2;
  while (queue.length) {
    const batch = queue.splice(0, concurrency);
    const batchResults = await Promise.all(batch.map((id) => runOneProperty(id)));
    for (const row of batchResults) {
      callsAttempted += row.completeness.attempted;
      callsSuccessful += row.completeness.success;
      callsFailed += row.completeness.failed;
      actualSpend += row.cost || 0;
      delete row.completeness;
      propertyResults.push(row);
    }
  }

  const RUN_END = new Date().toISOString();
  const materialIncomplete = propertyResults.some((r) => r.incompleteProviders?.length);

  const report = {
    ok: !materialIncomplete || dryRun,
    status: materialIncomplete && !dryRun
      ? "BASELINE_RUN_PARTIAL_REMEDIATION_REQUIRED"
      : dryRun
        ? "DRY_RUN_COMPLETE"
        : "EXECUTION_COMPLETE",
    baselineMarker: OFFICIAL_BASELINE_PERIOD_MARKER,
    measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
    measurementContractHash: preflight.measurementContractHash,
    preflight,
    RUN_START,
    RUN_END,
    PROVIDER_CALLS_ATTEMPTED: callsAttempted,
    PROVIDER_CALLS_SUCCESSFUL: callsSuccessful,
    PROVIDER_CALLS_FAILED: callsFailed,
    ACTUAL_SPEND: Math.round(actualSpend * 100) / 100,
    propertyResults,
    certifyRequested: certify,
  };

  const outPath = join(
    process.cwd(),
    "reports/ai-demand-positioning/adp-official-baseline-period-001-run.json"
  );
  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n");
  report.reportFile = outPath;
  return report;
}

export async function certifyOfficialBaselinePeriods(periodIdsByProperty, {
  measurementContractHash,
} = {}) {
  const { loadPeriod, savePeriod } = await import("../data-model.js");
  const certified = [];
  for (const [propertyId, periodId] of Object.entries(periodIdsByProperty || {})) {
    const period = loadPeriod(periodId);
    if (!period) {
      certified.push({ propertyId, periodId, ok: false, error: "PERIOD_NOT_FOUND" });
      continue;
    }
    period.certified = true;
    period.certifiedAt = new Date().toISOString();
    period.measurementContractHash = measurementContractHash || period.measurementContractHash;
    period.customerTrendEligible = true;
    period.customerVisible = true;
    period.officialPeriod = true;
    period.measurementPhase = "OFFICIAL_PRODUCTION";
    savePeriod(period);

    const profile = loadPropertyProfile(propertyId);
    const bundle = buildPublishedSnapshotBundle({ period, profile });
    let published = null;
    if (bundle.ok) {
      // Stamp contract metadata onto published manifest
      bundle.manifest.officialPeriod = true;
      bundle.manifest.baselineMarker = OFFICIAL_BASELINE_PERIOD_MARKER;
      bundle.manifest.measurementContractVersion = MEASUREMENT_CONTRACT_VERSION;
      bundle.manifest.measurementContractHash = period.measurementContractHash;
      bundle.manifest.certified = true;
      bundle.report.payload.period = {
        ...(bundle.report.payload.period || {}),
        officialPeriod: true,
        baselineMarker: OFFICIAL_BASELINE_PERIOD_MARKER,
        measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
        measurementContractHash: period.measurementContractHash,
        certified: true,
      };
      published = savePublishedSnapshotBundle(bundle, { seed: false });
    }
    certified.push({ propertyId, periodId, ok: true, published });
  }
  return certified;
}
