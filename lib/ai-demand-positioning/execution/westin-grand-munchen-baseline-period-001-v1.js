/**
 * The Westin Grand München — first official ADP property baseline period.
 * Uses shared providers / parsers / classifiers / certifyAdpPeriod publish path.
 * Peer-pack competitive layer may be FIRST_BASELINE_DEFERRED.
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
  publishExistingHotelAdpSnapshot,
} from "../published-snapshot.js";
import { OWNED_SOURCE_CLASSIFICATION_VERSION } from "../metrics/owned-source-classification-v1.js";
import { buildScenarioUniverseManifest } from "../certification/adp-scenario-universe-contract-v1.js";
import { runAdpIdentityPreflight, certifyAdpPeriod } from "../certification/certify-adp-period-v1.js";

export const WESTIN_MUC_PROPERTY_ID = "adp_westin_grand_munchen";
export const WESTIN_MUC_CENSUS_RECORD_ID = "recFaxTEFF9ILHWC9";
export const WESTIN_MUC_BASELINE_MARKER = "ADP_WESTIN_GRAND_MUNCHEN_BASELINE_PERIOD_001";
export const WESTIN_MUC_COST_CAP_USD = 12;
export const WESTIN_MUC_ENTITY_VERSION = "adp_westin_grand_munchen_entity_v1";

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
      mentioned: rows.filter((o) => o.mentioned).length,
    };
  }
  return { byProvider, success, failed, attempted: obs.length };
}

function bleedScan(scenarios) {
  const bleed = [];
  const re =
    /\b(bethesda|nih|dmv|renaissance|times square|manhattan|new york|via liguria|w rome|a coruña|a coruna|yotel|geneva lake|founex|grand cayman|monterrey valle)\b/i;
  for (const s of scenarios) {
    if (re.test(s.query || "")) bleed.push(s.scenarioId);
  }
  return bleed;
}

export function buildWestinGrandMunchenPreflight() {
  const measurementContractHash = loadFrozenContractHash();
  const profile = loadPropertyProfile(WESTIN_MUC_PROPERTY_ID);
  const blockers = [];
  const warnings = [];

  if (!profile) blockers.push("PROPERTY_PROFILE_MISSING");
  if (profile?.censusRecordId !== WESTIN_MUC_CENSUS_RECORD_ID && profile?.hpcHotelId !== WESTIN_MUC_CENSUS_RECORD_ID) {
    blockers.push("CENSUS_LINK_MISSING");
  }

  const identity = profile
    ? runAdpIdentityPreflight(WESTIN_MUC_PROPERTY_ID, { propertyProfile: profile })
    : { outcome: "IDENTITY_FAIL", hardFailures: [{ code: "NO_PROFILE" }] };
  if (identity.outcome === "IDENTITY_FAIL") {
    blockers.push("IDENTITY_PREFLIGHT_FAILED");
  } else if (identity.outcome === "IDENTITY_REVIEW") {
    warnings.push("IDENTITY_PREFLIGHT_REVIEW");
  }

  const scenarios = profile ? buildScenarioUniverse(profile) : [];
  if (scenarios.length < 15) blockers.push(`SCENARIO_TOO_SMALL_${scenarios.length}`);
  const bleed = bleedScan(scenarios);
  if (bleed.length) blockers.push(`SCENARIO_BLEED_${bleed.join(",")}`);

  const universe = profile
    ? buildScenarioUniverseManifest(profile, { scenarios })
    : null;

  if (profile?.stewardship?.peerPackStatus === "FIRST_BASELINE_DEFERRED") {
    warnings.push("PEER_PACK_FIRST_BASELINE_DEFERRED");
  }

  const providers = availableProviders();
  const cost = estimateCost(scenarios.length || 0, providers.length ? providers : PROVIDERS);
  const calls = scenarios.length * (providers.length || PROVIDERS.length);
  const roundedTotal = Math.round(cost.total * 100) / 100;
  if (roundedTotal > WESTIN_MUC_COST_CAP_USD) blockers.push("COST_CAP_EXCEEDED");
  if (providers.length < 4) warnings.push(`PROVIDER_KEYS_${providers.length}_OF_4`);

  const PREFLIGHT = blockers.length === 0 ? "PASS" : "FAIL";

  return {
    propertyId: WESTIN_MUC_PROPERTY_ID,
    CANONICAL_NAME: profile?.name || null,
    CENSUS: profile?.censusRecordId || profile?.hpcHotelId || null,
    ROOMS: profile?.rooms ?? null,
    measurementContractHash,
    baselineMarker: WESTIN_MUC_BASELINE_MARKER,
    TOTAL_SCENARIOS: scenarios.length,
    scenarioUniverse: universe,
    SCENARIO_SOURCES: scenarios.reduce((a, s) => {
      a[s.source || "?"] = (a[s.source || "?"] || 0) + 1;
      return a;
    }, {}),
    identityPreflight: {
      outcome: identity.outcome,
      hardFailures: identity.hardFailures || [],
      reviewFlags: identity.reviewFlags || [],
      canaryPass: identity.canary?.pass ?? null,
    },
    AVAILABLE_PROVIDERS: providers,
    TOTAL_PLANNED_CALLS: calls,
    TOTAL_ESTIMATED_COST: roundedTotal,
    COST_CAP_USD: WESTIN_MUC_COST_CAP_USD,
    bleedScenarioIds: bleed,
    blockers,
    warnings,
    PREFLIGHT,
  };
}

export async function executeWestinGrandMunchenBaselinePeriod001({
  dryRun = true,
  onProgress = null,
  certify = false,
} = {}) {
  const preflight = buildWestinGrandMunchenPreflight();
  if (!dryRun && preflight.PREFLIGHT !== "PASS") {
    return { ok: false, status: "BASELINE_ABORTED_PREFLIGHT", preflight };
  }
  if (!dryRun && preflight.identityPreflight?.outcome === "IDENTITY_FAIL") {
    return { ok: false, status: "BASELINE_ABORTED_IDENTITY", preflight };
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
  const profile = loadPropertyProfile(WESTIN_MUC_PROPERTY_ID);
  const scenarios = buildScenarioUniverse(profile);
  const universe = buildScenarioUniverseManifest(profile, { scenarios });

  const period = await executeMonitoringPeriod({
    propertyId: WESTIN_MUC_PROPERTY_ID,
    scenarios,
    dryRun,
    providers,
    delayMsOverride: dryRun ? 0 : 200,
    checkpointEvery: 20,
    propertyProfile: profile,
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
    baselineMarker: WESTIN_MUC_BASELINE_MARKER,
    baselineSequence: 1,
    scenarioUniverseVersion: universe.scenarioUniverseVersion,
    entityResolutionVersion: WESTIN_MUC_ENTITY_VERSION,
    sourceGovernanceVersion: OWNED_SOURCE_CLASSIFICATION_VERSION,
    providerSet: providers,
    certified: false,
    priorComparablePeriod: null,
  });
  finalPeriod.measurementContractVersion = MEASUREMENT_CONTRACT_VERSION;
  finalPeriod.globalCertificationEngineVersion = "adp_global_certification_engine_v1";
  finalPeriod.certificationStatus = "DRAFT";
  finalPeriod.scenarioUniverse = universe;
  finalPeriod.scenarioUniverseId = universe.scenarioUniverseId;
  finalPeriod.scenarioIds = universe.scenarioIds;
  finalPeriod.identityPreflight = preflight.identityPreflight;

  const completeness = summarizeProviderCompleteness(finalPeriod, scenarios.length, providers);
  savePeriod(finalPeriod);

  let certification = null;
  let published = null;
  if (!dryRun && certify) {
    certification = await certifyAdpPeriod(
      { propertyId: WESTIN_MUC_PROPERTY_ID, period: finalPeriod, propertyProfile: profile },
      {
        writeManifest: true,
        writePeriod: true,
        writeAuditTrail: true,
        forceOfficialCertification: true,
        trigger: "westin_grand_munchen_baseline_period_001",
      }
    );

    if (
      certification.engineStatus === "CERTIFIED" ||
      certification.publishCertificationStatus === "CERTIFIED"
    ) {
      finalPeriod = {
        ...finalPeriod,
        certified: true,
        certificationStatus: "CERTIFIED",
        officialPeriod: true,
        measurementPhase: "OFFICIAL_PRODUCTION",
        customerVisible: true,
        globalCertificationEngineVersion: "adp_global_certification_engine_v1",
        certificationTimestamp: certification.certificationTimestamp,
      };
      savePeriod(finalPeriod);

      const bundle = buildPublishedSnapshotBundle({ period: finalPeriod, profile });
      if (bundle.ok) {
        published = await publishExistingHotelAdpSnapshot(bundle, {
          certificationStatus: "CERTIFIED",
          status: "CERTIFIED",
          assuranceVersion: certification.engineVersion,
          certificationTimestamp: certification.certificationTimestamp,
          engineVersion: certification.engineVersion,
          hardFailures: [],
          reviewFlags: certification.reviewFlags || [],
          manifest: certification.manifest,
        }, {
          officialCustomerPublish: true,
          skipAutoCertify: true,
        });
      } else {
        return {
          ok: false,
          status: "PUBLISH_BUNDLE_FAILED",
          bundleError: bundle.message || bundle,
          PERIOD_ID: finalPeriod.periodId,
          preflight,
          certification,
          PROVIDER_COMPLETENESS: completeness.byProvider,
        };
      }
    } else {
      return {
        ok: false,
        status: "CERTIFICATION_NOT_PASSED",
        PERIOD_ID: finalPeriod.periodId,
        preflight,
        certification,
        PROVIDER_COMPLETENESS: completeness.byProvider,
      };
    }
  }

  const RUN_END = new Date().toISOString();
  const incompleteProviders = Object.entries(completeness.byProvider)
    .filter(([, v]) => v.expected > 0 && v.successful / v.expected < 0.8)
    .map(([p]) => p);

  const report = {
    ok: incompleteProviders.length === 0 && (!certify || published?.ok !== false),
    status: incompleteProviders.length
      ? "PARTIAL_PROVIDER_COMPLETENESS"
      : dryRun
        ? "DRY_RUN_COMPLETE"
        : certify
          ? "CERTIFIED_COMPLETE"
          : "EXECUTION_COMPLETE",
    PERIOD_MARKER: WESTIN_MUC_BASELINE_MARKER,
    PERIOD_ID: finalPeriod.periodId,
    RUN_START,
    RUN_END,
    CALLS_ATTEMPTED: completeness.attempted,
    CALLS_SUCCESSFUL: completeness.success,
    CALLS_FAILED: completeness.failed,
    ACTUAL_SPEND: finalPeriod.costEstimate?.total ?? null,
    PROVIDER_COMPLETENESS: completeness.byProvider,
    incompleteProviders,
    CERTIFIED: Boolean(finalPeriod.certified),
    certificationStatus: finalPeriod.certificationStatus,
    preflight,
    certification,
    published,
  };

  const outDir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(outDir, { recursive: true });
  writeFileSync(
    join(outDir, "adp-yotel-geneva-lake-baseline-period-001-run.json"),
    JSON.stringify(report, null, 2)
  );

  return report;
}
