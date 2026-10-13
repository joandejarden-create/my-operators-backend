/**
 * Shared first official ADP property baseline period executor.
 * Hotel-instance configs supply propertyId / census / marker / entity version —
 * measurement, providers, parsers, and certification stay on the shared path.
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
    const ok = rows.filter((o) => o.status === "ok" || o.success === true || o.responseText).length;
    const fail = rows.length - ok;
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

function bleedScan(scenarios, extraBleedRe) {
  const bleed = [];
  const re =
    extraBleedRe ||
    /\b(bethesda|nih|dmv|renaissance|times square|manhattan|new york|via liguria|w rome|a coruña|a coruna|yotel|geneva lake|founex|grand cayman|monterrey valle|westin grand münchen|westin grand munich|arabellapark munich)\b/i;
  for (const s of scenarios) {
    if (re.test(s.query || "")) bleed.push(s.scenarioId);
  }
  return bleed;
}

/**
 * @param {{
 *   propertyId: string,
 *   censusRecordId: string,
 *   baselineMarker: string,
 *   entityVersion: string,
 *   costCapUsd?: number,
 *   certifyTrigger?: string,
 *   reportFileName?: string,
 * }} cfg
 */
export function buildSharedFirstPropertyPreflight(cfg) {
  const {
    propertyId,
    censusRecordId,
    baselineMarker,
    costCapUsd = 12,
  } = cfg;
  const measurementContractHash = loadFrozenContractHash();
  const profile = loadPropertyProfile(propertyId);
  const blockers = [];
  const warnings = [];

  if (!profile) blockers.push("PROPERTY_PROFILE_MISSING");
  if (
    profile?.censusRecordId !== censusRecordId &&
    profile?.hpcHotelId !== censusRecordId
  ) {
    blockers.push("CENSUS_LINK_MISSING");
  }

  const identity = profile
    ? runAdpIdentityPreflight(propertyId, { propertyProfile: profile })
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
  if (roundedTotal > costCapUsd) blockers.push("COST_CAP_EXCEEDED");
  if (providers.length < 4) warnings.push(`PROVIDER_KEYS_${providers.length}_OF_4`);

  const PREFLIGHT = blockers.length === 0 ? "PASS" : "FAIL";

  return {
    propertyId,
    CANONICAL_NAME: profile?.name || null,
    CENSUS: profile?.censusRecordId || profile?.hpcHotelId || null,
    ROOMS: profile?.rooms ?? null,
    measurementContractHash,
    baselineMarker,
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
    COST_CAP_USD: costCapUsd,
    bleedScenarioIds: bleed,
    blockers,
    warnings,
    PREFLIGHT,
  };
}

/**
 * @param {Parameters<typeof buildSharedFirstPropertyPreflight>[0]} cfg
 * @param {{ dryRun?: boolean, onProgress?: Function, certify?: boolean }} opts
 */
export async function executeSharedFirstPropertyBaselinePeriod001(cfg, opts = {}) {
  const {
    propertyId,
    baselineMarker,
    entityVersion,
    certifyTrigger = `${propertyId}_baseline_period_001`,
    reportFileName = `adp-${propertyId}-baseline-period-001-run.json`,
  } = cfg;
  const { dryRun = true, onProgress = null, certify = false } = opts;

  const preflight = buildSharedFirstPropertyPreflight(cfg);
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
  const profile = loadPropertyProfile(propertyId);
  const scenarios = buildScenarioUniverse(profile);
  const universe = buildScenarioUniverseManifest(profile, { scenarios });

  const period = await executeMonitoringPeriod({
    propertyId,
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
    baselineMarker,
    baselineSequence: 1,
    scenarioUniverseVersion: universe.scenarioUniverseVersion,
    entityResolutionVersion: entityVersion,
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
      { propertyId, period: finalPeriod, propertyProfile: profile },
      {
        writeManifest: true,
        writePeriod: true,
        writeAuditTrail: true,
        forceOfficialCertification: true,
        trigger: certifyTrigger,
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
        published = await publishExistingHotelAdpSnapshot(
          bundle,
          {
            certificationStatus: "CERTIFIED",
            status: "CERTIFIED",
            assuranceVersion: certification.engineVersion,
            certificationTimestamp: certification.certificationTimestamp,
            engineVersion: certification.engineVersion,
            hardFailures: [],
            reviewFlags: certification.reviewFlags || [],
            manifest: certification.manifest,
          },
          {
            officialCustomerPublish: true,
            skipAutoCertify: true,
          }
        );
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
    PERIOD_MARKER: baselineMarker,
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
  writeFileSync(join(outDir, reportFileName), JSON.stringify(report, null, 2));

  return report;
}
