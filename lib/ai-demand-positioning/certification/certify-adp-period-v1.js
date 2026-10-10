/**
 * Global ADP certification engine — certifyAdpPeriod (Parts 1, 3, 13–16, 30–35).
 *
 * Shared path for EVERY hotel / EVERY period.
 * No hotel-specific certification bypasses.
 * Does not change ADP thresholds or scoring methodology.
 */

import crypto from "crypto";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";
import { join, dirname } from "path";
import {
  loadPeriod,
  loadPropertyProfile,
  loadAllPeriods,
  PROVIDERS,
  savePeriod,
} from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { computeConsiderationMetrics } from "../metrics/consideration-rate.js";
import { filterComparableObservations, METRIC_GRAINS } from "../metrics/grain-governance.js";
import { computeOwnedExternalSourceMix } from "../metrics/owned-source-classification-v1.js";
import { loadPublishedManifest, loadPublishedReport } from "../published-snapshot.js";
import {
  ADP_PERIOD_PIPELINE_STATES,
  mapLegacyCertificationToPipelineState,
} from "./adp-period-pipeline-states-v1.js";
import {
  buildAdpHotelIdentityContract,
  validateAdpHotelIdentityContract,
  runIdentityCanaries,
  listAllAdpPropertyProfiles,
  IDENTITY_PREFLIGHT_OUTCOMES,
} from "./adp-hotel-identity-contract-v1.js";
import {
  buildScenarioUniverseManifest,
  scenarioIdsFromPeriod,
} from "./adp-scenario-universe-contract-v1.js";
import {
  evaluateAdpComparability,
  ADP_COMPARABILITY_OUTCOMES,
} from "./adp-comparability-engine-v1.js";
import { auditSourceAttributionLabels } from "./adp-source-attribution-contract-v1.js";
import {
  detectAdpAnomalies,
  auditCrossMetricConsistency,
  explainAdpAnomaly,
} from "./adp-anomaly-rules-v1.js";
import { CERTIFICATION_STATUSES } from "./certification-status.js";
import { evaluateProviderCompletenessGate } from "./adp-provider-completeness-policy-v1.js";

export const ADP_GLOBAL_CERTIFICATION_ENGINE_VERSION = "adp_global_certification_engine_v1";
export const ADP_CERTIFICATION_MANIFEST_VERSION = "adp_certification_manifest_v1";

const ALLOWED_DENOMINATOR_GRAINS = Object.freeze([
  "SCENARIO",
  "PROVIDER_RESPONSE",
  "RANK_ELIGIBLE_RESPONSE",
  "CITATION_ELIGIBLE_RESPONSE",
  "ATTRIBUTE",
  METRIC_GRAINS.SCENARIO,
  METRIC_GRAINS.OBSERVATION,
]);

function sha16(value) {
  return crypto.createHash("sha256").update(String(value || "")).digest("hex").slice(0, 16);
}

function nearlyEqual(a, b, tol = 0.15) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return Math.abs(Number(a) - Number(b)) <= tol;
}

function providerCompletenessGate(period, liveScenarioCount = null) {
  return evaluateProviderCompletenessGate(period, liveScenarioCount);
}

function recomputeCoreMetrics(period, scenarios, propertyProfile) {
  const consideration = computeConsiderationMetrics(
    period?.observations || [],
    scenarios,
    propertyProfile
  );
  const comparable = filterComparableObservations(period?.observations || []);
  const ownedMix = computeOwnedExternalSourceMix(comparable, propertyProfile);

  const providerPresence = {};
  for (const p of PROVIDERS) {
    const rows = comparable.filter((o) => o.provider === p);
    const mentioned = rows.filter((o) => o.mentioned).length;
    providerPresence[p] = {
      numerator: mentioned,
      denominator: rows.length,
      grain: "PROVIDER_RESPONSE",
      rate: rows.length ? (mentioned / rows.length) * 100 : null,
    };
  }

  return {
    aiConsideration: consideration.observationConsiderationRate,
    scenarioPresence: consideration.scenarioConsiderationCoverage,
    considerationNumerator: consideration.presentObservations,
    considerationDenominator: consideration.comparableObservations,
    considerationGrain: "PROVIDER_RESPONSE",
    scenarioPresenceNumerator: consideration.capturedScenarios,
    scenarioPresenceDenominator: consideration.eligibleScenarios,
    scenarioPresenceGrain: "SCENARIO",
    ownedSourceShare: ownedMix.ownedShare,
    citationCoverage:
      ownedMix.withCitations != null
        ? ownedMix.withCitations
        : comparable.filter((o) => (o.sourcesCited || o.providerCitations || []).length).length,
    providerPresence,
    denominators: [
      {
        metric: "AI Consideration",
        numerator: consideration.presentObservations,
        denominator: consideration.comparableObservations,
        grain: "PROVIDER_RESPONSE",
      },
      {
        metric: "Scenario Presence",
        numerator: consideration.capturedScenarios,
        denominator: consideration.eligibleScenarios,
        grain: "SCENARIO",
      },
      {
        metric: "Owned Source Share",
        numerator: ownedMix.ownedResponses,
        denominator: ownedMix.withCitations,
        grain: "CITATION_ELIGIBLE_RESPONSE",
      },
    ],
  };
}

function auditDenominatorGrains(denominators = []) {
  const hardFailures = [];
  for (const row of denominators) {
    if (!ALLOWED_DENOMINATOR_GRAINS.includes(row.grain)) {
      hardFailures.push({ code: "INVALID_DENOMINATOR_GRAIN", metric: row.metric, grain: row.grain });
    }
    if (row.metric === "AI Consideration" && row.grain === "SCENARIO") {
      hardFailures.push({
        code: "PROVIDER_RESPONSE_LABELED_AS_SCENARIO",
        metric: row.metric,
      });
    }
    if (row.metric === "Scenario Presence" && String(row.grain).includes("OBSERVATION")) {
      hardFailures.push({
        code: "SCENARIO_LABELED_AS_PROVIDER_RESPONSE",
        metric: row.metric,
      });
    }
  }
  return { hardFailures };
}

function pickStoredSummary(period, published) {
  const exec = published?.executiveMetrics || published?.payload?.executiveMetrics || {};
  return {
    aiConsideration:
      period?.summaryMetrics?.aiConsideration ??
      exec?.considerationRate?.value ??
      exec?.aiConsiderationRate ??
      null,
    scenarioPresence:
      period?.summaryMetrics?.scenarioPresence ??
      exec?.scenarioPresence?.value ??
      exec?.aiScenarioPresence ??
      null,
    ownedSourceShare:
      period?.summaryMetrics?.ownedSourceShare ??
      published?.ownedSourceMix?.ownedShare ??
      null,
  };
}

function resolvePeriodAndProfile({ periodId, period, propertyId, propertyProfile }) {
  let resolvedPeriod = period || null;
  if (!resolvedPeriod && periodId) resolvedPeriod = loadPeriod(periodId);
  const pid = propertyId || resolvedPeriod?.propertyId;
  const profile = propertyProfile || (pid ? loadPropertyProfile(pid) : null);
  return { period: resolvedPeriod, profile, propertyId: pid };
}

function certificationManifestPath(propertyId, periodId) {
  return join(
    process.cwd(),
    "data/ai-demand-positioning/runtime/certification-manifests",
    propertyId,
    `${periodId}.json`
  );
}

export function persistCertificationManifest(manifest, { write = true } = {}) {
  if (!write) return { ok: true, path: null, manifest };
  const path = certificationManifestPath(manifest.hotelId || manifest.propertyId, manifest.periodId);
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(manifest, null, 2));
  return { ok: true, path, manifest };
}

export function loadCertificationManifest(propertyId, periodId) {
  const path = certificationManifestPath(propertyId, periodId);
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, "utf8"));
}

function appendAuditTrail(entry) {
  const path = join(
    process.cwd(),
    "data/ai-demand-positioning/runtime/certification-audit-trail.jsonl"
  );
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(entry)}\n`, { flag: "a" });
}

/**
 * Shared certification engine.
 * @param {string|{periodId?:string, period?:object, propertyId?:string, propertyProfile?:object}} input
 */
export async function certifyAdpPeriod(input = {}, options = {}) {
  const args = typeof input === "string" ? { periodId: input } : input || {};
  const { period, profile, propertyId } = resolvePeriodAndProfile(args);

  const hardFailures = [];
  const reviewFlags = [];
  const warnings = [];

  if (!profile) {
    return {
      status: ADP_PERIOD_PIPELINE_STATES.QA_FAILED,
      hardFailures: [{ code: "PROPERTY_PROFILE_MISSING", propertyId }],
      reviewFlags: [],
      warnings: [],
      comparability: null,
      manifest: null,
      engineVersion: ADP_GLOBAL_CERTIFICATION_ENGINE_VERSION,
    };
  }

  if (!period) {
    // Legacy / inventory mode: no runtime period → LEGACY_UNCERTIFIED (do not auto-certify)
    const identity = validateAdpHotelIdentityContract(profile, {
      allProfiles: listAllAdpPropertyProfiles(),
    });
    const canary = runIdentityCanaries(profile);
    const universe = buildScenarioUniverseManifest(profile);
    if (identity.outcome === IDENTITY_PREFLIGHT_OUTCOMES.IDENTITY_FAIL) {
      hardFailures.push(...identity.hardFailures);
    } else if (identity.outcome === IDENTITY_PREFLIGHT_OUTCOMES.IDENTITY_REVIEW) {
      reviewFlags.push(...identity.reviewFlags);
    }
    if (!canary.pass) hardFailures.push({ code: "IDENTITY_CANARY_FAIL", failed: canary.failed });

    const status = hardFailures.length
      ? ADP_PERIOD_PIPELINE_STATES.QA_FAILED
      : reviewFlags.length
        ? ADP_PERIOD_PIPELINE_STATES.QA_REVIEW_REQUIRED
        : ADP_PERIOD_PIPELINE_STATES.LEGACY_UNCERTIFIED;

    const manifest = {
      manifestVersion: ADP_CERTIFICATION_MANIFEST_VERSION,
      periodId: null,
      hotelId: propertyId,
      propertyId,
      subjectId: identity.contract.subjectId,
      measurementDate: null,
      ...universe,
      providerSet: [...PROVIDERS],
      expectedProviderResponses: null,
      successfulProviderResponses: null,
      failedResponses: null,
      timeoutResponses: null,
      parseFailures: null,
      identityContractVersion: identity.contract.identityContractVersion,
      aliasSetHash: identity.contract.aliasSetHash,
      ownedDomainSetHash: identity.contract.ownedDomainSetHash,
      certificationStatus: status,
      certificationTimestamp: new Date().toISOString(),
      qaFindings: [...hardFailures, ...reviewFlags],
      comparisonEligibility: ADP_COMPARABILITY_OUTCOMES.NOT_COMPARABLE,
      legacyUncertified: true,
      engineVersion: ADP_GLOBAL_CERTIFICATION_ENGINE_VERSION,
    };

    return {
      status,
      hardFailures,
      reviewFlags,
      warnings,
      comparability: null,
      manifest,
      identity,
      canary,
      engineVersion: ADP_GLOBAL_CERTIFICATION_ENGINE_VERSION,
    };
  }

  // --- Identity ---
  const identity = validateAdpHotelIdentityContract(profile, {
    allProfiles: listAllAdpPropertyProfiles(),
  });
  const canary = runIdentityCanaries(profile);
  if (identity.outcome === IDENTITY_PREFLIGHT_OUTCOMES.IDENTITY_FAIL) {
    hardFailures.push(...identity.hardFailures.map((f) => ({ ...f, gate: "IDENTITY_PREFLIGHT" })));
  } else if (identity.outcome === IDENTITY_PREFLIGHT_OUTCOMES.IDENTITY_REVIEW) {
    reviewFlags.push(...identity.reviewFlags.map((f) => ({ ...f, gate: "IDENTITY_PREFLIGHT" })));
  }
  if (!canary.pass) {
    hardFailures.push({ code: "IDENTITY_CANARY_FAIL", failed: canary.failed, gate: "IDENTITY_CANARY" });
  }

  // --- Scenario universe ---
  const scenarios = options.scenarios || buildScenarioUniverse(profile);
  const universe = buildScenarioUniverseManifest(profile, {
    scenarios,
    scenarioUniverseId: period.scenarioUniverseId || period.scenarioUniverse?.scenarioUniverseId,
    scenarioUniverseVersion:
      period.scenarioUniverseVersion || period.scenarioUniverse?.scenarioUniverseVersion,
    capabilityExclusions: period.capabilityExclusions || [],
  });
  const periodScenarioIds = scenarioIdsFromPeriod(period);
  if (!periodScenarioIds.length && !(period.observations || []).length) {
    hardFailures.push({ code: "MISSING_SCENARIO_MANIFEST", gate: "SCENARIO_UNIVERSE" });
  }

  // --- Provider completeness ---
  const providerGate = providerCompletenessGate(period, universe.scenarioCount);
  if (providerGate.silentDenominatorReduction) {
    hardFailures.push({
      code: "SILENT_DENOMINATOR_REDUCTION",
      gate: "PROVIDER_COMPLETENESS",
      details: {
        expected: providerGate.expected,
        attempted: providerGate.attempted,
        successful: providerGate.successful,
        failed: providerGate.failed,
        timedOut: providerGate.timedOut,
        periodScenarioCount: providerGate.periodScenarioCount,
      },
    });
  } else if (providerGate.materialFailure) {
    reviewFlags.push({
      code: "PROVIDER_COMPLETENESS_MATERIAL_FAILURE",
      gate: "PROVIDER_COMPLETENESS",
      policyName: providerGate.policyName,
      details: {
        expected: providerGate.expected,
        successful: providerGate.successful,
        failed: providerGate.failed,
        timedOut: providerGate.timedOut,
        overallFailureCap: providerGate.overallFailureCap,
        providerFailureCap: providerGate.providerFailureCap,
        providerImbalance: providerGate.providerImbalance || [],
        zeroSuccessProvider: providerGate.zeroSuccessProvider,
      },
    });
  }
  if (providerGate.liveUniverseDrift) {
    const legacyOrFrozenControl =
      !period.globalCertificationEngineVersion ||
      Boolean(period.matchedControlSetId || period.controlSetId || period.frozenScenarioUniverse);
    const driftRow = {
      code: "LIVE_SCENARIO_UNIVERSE_DRIFT_VS_PERIOD",
      gate: "SCENARIO_UNIVERSE",
      periodScenarioCount: providerGate.periodScenarioCount,
      liveScenarioCount: providerGate.liveScenarioCount,
      classification: legacyOrFrozenControl ? "LEGACY_METADATA_MISSING" : "SCENARIO_UNIVERSE_MISMATCH",
    };
    // Historical / matched-control universes are expected to differ from live builder — do not block.
    if (legacyOrFrozenControl) warnings.push(driftRow);
    else reviewFlags.push(driftRow);
  }

  // --- Raw recompute ---
  const recomputed = recomputeCoreMetrics(period, scenarios, profile);
  const published = loadPublishedReport(propertyId);
  const publishedManifest = loadPublishedManifest(propertyId);
  const stored = pickStoredSummary(period, published);

  if (stored.aiConsideration != null && recomputed.aiConsideration != null) {
    if (!nearlyEqual(stored.aiConsideration, recomputed.aiConsideration, 0.2)) {
      hardFailures.push({
        code: "RAW_STORED_METRIC_MISMATCH",
        metric: "aiConsideration",
        stored: stored.aiConsideration,
        recomputed: recomputed.aiConsideration,
        gate: "RAW_METRIC_RECOMPUTE",
      });
    }
  }
  if (stored.scenarioPresence != null && recomputed.scenarioPresence != null) {
    if (!nearlyEqual(stored.scenarioPresence, recomputed.scenarioPresence, 0.2)) {
      hardFailures.push({
        code: "RAW_STORED_METRIC_MISMATCH",
        metric: "scenarioPresence",
        stored: stored.scenarioPresence,
        recomputed: recomputed.scenarioPresence,
        gate: "RAW_METRIC_RECOMPUTE",
      });
    }
  }

  // --- Denominator grains ---
  const grainAudit = auditDenominatorGrains(recomputed.denominators);
  hardFailures.push(...grainAudit.hardFailures.map((f) => ({ ...f, gate: "DENOMINATOR_GRAIN" })));

  // --- Cross-metric ---
  const cross = auditCrossMetricConsistency({
    scenarioPresence: recomputed.scenarioPresence,
    considerationNumerator: recomputed.considerationNumerator,
    considerationDenominator: recomputed.considerationDenominator,
  });
  hardFailures.push(...cross.hardFailures.map((f) => ({ ...f, gate: "CROSS_METRIC" })));

  // --- Source attribution ---
  const sourceAudit = auditSourceAttributionLabels(period, profile, {
    storedOwnedSourceShare: stored.ownedSourceShare ?? recomputed.ownedSourceShare,
    claimedTopSourceDomain: options.claimedTopSourceDomain || null,
  });
  hardFailures.push(...sourceAudit.hardFailures.map((f) => ({ ...f, gate: "SOURCE_ATTRIBUTION" })));
  reviewFlags.push(...sourceAudit.reviewFlags.map((f) => ({ ...f, gate: "SOURCE_ATTRIBUTION" })));

  // --- Cross-section period ID mixing ---
  if (
    publishedManifest?.latestPeriodId &&
    period.periodId &&
    publishedManifest.latestPeriodId === period.periodId
  ) {
    const reportPeriodId =
      published?.period?.periodId || published?.payload?.period?.periodId || null;
    if (reportPeriodId && reportPeriodId !== period.periodId) {
      hardFailures.push({
        code: "PERIOD_ID_MIXING",
        reportPeriodId,
        runtimePeriodId: period.periodId,
        gate: "CROSS_SECTION",
      });
    }
  }

  // --- Prior comparable / anomalies ---
  const allPeriods = loadAllPeriods(propertyId);
  const priorSorted = allPeriods
    .filter((p) => p.periodId !== period.periodId)
    .sort((a, b) => String(a.executionDate || "").localeCompare(String(b.executionDate || "")));
  const prior = options.priorPeriod || priorSorted[priorSorted.length - 1] || null;

  const comparability = prior
    ? evaluateAdpComparability(
        { ...period, scenarioUniverse: universe, certificationStatus: period.certificationStatus },
        prior
      )
    : null;

  const isFirstComparableRun = !prior;
  const anomalies = detectAdpAnomalies({
    period,
    priorPeriod: prior,
    propertyProfile: profile,
    recomputed,
    stored,
    identityCanary: canary,
    isFirstComparableRun,
  });
  hardFailures.push(...anomalies.hardFailures);
  reviewFlags.push(...anomalies.reviewFlags);
  warnings.push(...(anomalies.warnings || []));

  // --- Status aggregation (Part 30) ---
  let engineStatus = ADP_PERIOD_PIPELINE_STATES.QA_PASSED;
  if (hardFailures.length) engineStatus = ADP_PERIOD_PIPELINE_STATES.QA_FAILED;
  else if (reviewFlags.length) engineStatus = ADP_PERIOD_PIPELINE_STATES.QA_REVIEW_REQUIRED;
  else engineStatus = ADP_PERIOD_PIPELINE_STATES.CERTIFIED;

  // Map CERTIFIED_WITH_DISCLOSURES path: review-only soft → still CERTIFIED if no hard fails
  // and only non-blocking reviews that options allow
  if (
    engineStatus === ADP_PERIOD_PIPELINE_STATES.QA_REVIEW_REQUIRED &&
    options.promoteReviewToCertifiedWithDisclosure === true
  ) {
    engineStatus = CERTIFICATION_STATUSES.CERTIFIED_WITH_DISCLOSURES;
  }

  // Legacy periods: evaluate only — never silently stamp CERTIFIED as official.
  // Matched-control / already-certified runtime periods are framework-eligible when stamped.
  const createdUnderFramework = Boolean(
    period.globalCertificationEngineVersion ||
      options.forceOfficialCertification === true ||
      (period.certified === true &&
        (period.matchedControlSetId ||
          period.controlSetId ||
          period.certificationStatus === ADP_PERIOD_PIPELINE_STATES.CERTIFIED))
  );
  let status = engineStatus;
  if (options.auditOnly === true || (!createdUnderFramework && options.allowLegacyAutoCertify !== true)) {
    if (engineStatus === ADP_PERIOD_PIPELINE_STATES.CERTIFIED) {
      status = ADP_PERIOD_PIPELINE_STATES.LEGACY_UNCERTIFIED;
    }
  }

  const certificationTimestamp = new Date().toISOString();
  const manifest = {
    manifestVersion: ADP_CERTIFICATION_MANIFEST_VERSION,
    periodId: period.periodId,
    hotelId: propertyId,
    propertyId,
    subjectId: identity.contract.subjectId,
    measurementDate: period.executionDate || period.measurementDate || null,
    scenarioUniverseId: universe.scenarioUniverseId,
    scenarioIds: universe.scenarioIds,
    scenarioCount: universe.scenarioCount,
    providerSet: [...PROVIDERS],
    providerVersions: period.providerVersions || period.providerConfig || null,
    expectedProviderResponses: providerGate.expected,
    successfulProviderResponses: providerGate.successful,
    failedResponses: providerGate.failed,
    timeoutResponses: providerGate.timedOut,
    parseFailures: (period.observations || []).filter((o) => o.parseError).length,
    rankParserVersion: period.rankParserVersion || null,
    citationParserVersion: period.citationParserVersion || null,
    identityContractVersion: identity.contract.identityContractVersion,
    aliasSetHash: identity.contract.aliasSetHash,
    ownedDomainSetHash: identity.contract.ownedDomainSetHash,
    scenarioBuilderVersion: universe.scenarioBuilderVersion,
    metricComputationVersion: "consideration-rate/computeConsiderationMetrics",
    certificationStatus: status,
    engineWouldCertify: engineStatus,
    legacyUncertified: status === ADP_PERIOD_PIPELINE_STATES.LEGACY_UNCERTIFIED,
    certificationTimestamp,
    qaFindings: [...hardFailures, ...reviewFlags, ...warnings],
    comparisonEligibility: comparability?.outcome || ADP_COMPARABILITY_OUTCOMES.NOT_COMPARABLE,
    denominators: recomputed.denominators,
    sourceAttribution: {
      topSourceSupportingThisProperty: sourceAudit.metrics.topSourceSupportingThisProperty,
      topOwnedBrandSource: sourceAudit.metrics.topOwnedBrandSource,
      topExternalPropertySource: sourceAudit.metrics.topExternalPropertySource,
      topCompetitiveUniverseSource: sourceAudit.metrics.topCompetitiveUniverseSource,
    },
    anomalyExplanations: anomalies.explanations,
    engineVersion: ADP_GLOBAL_CERTIFICATION_ENGINE_VERSION,
    scenarioUniverse: universe,
    identityContract: identity.contract,
    supersedesPeriodId: period.supersedesPeriodId || null,
    supersededByPeriodId: period.supersededByPeriodId || null,
    correctionReason: period.correctionReason || null,
    codeVersion: options.codeVersion || ADP_GLOBAL_CERTIFICATION_ENGINE_VERSION,
  };

  if (options.writeManifest !== false) {
    persistCertificationManifest(manifest, { write: true });
  }

  // Optionally stamp period metadata (never mutate certified history metrics)
  if (options.stampPeriod === true && status === ADP_PERIOD_PIPELINE_STATES.CERTIFIED) {
    if (period.certified === true && period.certificationStatus === ADP_PERIOD_PIPELINE_STATES.CERTIFIED) {
      // immutability: do not overwrite certified metrics; only attach manifest pointer if missing
      if (!period.certificationManifestId) {
        const stamped = {
          ...period,
          certificationManifestPath: certificationManifestPath(propertyId, period.periodId),
        };
        if (options.writePeriod === true) savePeriod(stamped);
      }
    } else if (period.certified !== true) {
      const stamped = {
        ...period,
        certificationStatus: status,
        certified: status === ADP_PERIOD_PIPELINE_STATES.CERTIFIED,
        certificationTimestamp,
        scenarioUniverse: period.scenarioUniverse || universe,
        identityContractVersion: identity.contract.identityContractVersion,
        aliasSetHash: identity.contract.aliasSetHash,
      };
      if (options.writePeriod === true) savePeriod(stamped);
    }
  }

  if (options.writeAuditTrail !== false) {
    appendAuditTrail({
      hotelId: propertyId,
      periodId: period.periodId,
      trigger: options.trigger || "certifyAdpPeriod",
      bugType: hardFailures[0]?.code || reviewFlags[0]?.code || null,
      oldPeriod: prior?.periodId || null,
      newPeriod: period.periodId,
      affectedMetrics: hardFailures.filter((f) => f.metric).map((f) => f.metric),
      codeVersion: ADP_GLOBAL_CERTIFICATION_ENGINE_VERSION,
      scenarioUniverseVersion: universe.scenarioUniverseVersion,
      identityVersion: identity.contract.identityContractVersion,
      certificationResult: status,
      timestamp: certificationTimestamp,
    });
  }

  return {
    status,
    certificationStatus: status,
    engineStatus,
    // Compatibility with publish guard (CERTIFIED / CERTIFIED_WITH_DISCLOSURES / NOT_CERTIFIED)
    // Official publish requires explicit forceOfficialCertification / created-under-framework path.
    publishCertificationStatus:
      engineStatus === ADP_PERIOD_PIPELINE_STATES.CERTIFIED &&
      (createdUnderFramework || options.forceOfficialCertification === true)
        ? CERTIFICATION_STATUSES.CERTIFIED
        : engineStatus === CERTIFICATION_STATUSES.CERTIFIED_WITH_DISCLOSURES &&
            (createdUnderFramework || options.forceOfficialCertification === true)
          ? CERTIFICATION_STATUSES.CERTIFIED_WITH_DISCLOSURES
          : CERTIFICATION_STATUSES.NOT_CERTIFIED,
    hardFailures,
    reviewFlags,
    warnings,
    comparability,
    manifest,
    identity,
    canary,
    providerGate,
    recomputed,
    stored,
    sourceAudit,
    explanations: anomalies.explanations,
    engineVersion: ADP_GLOBAL_CERTIFICATION_ENGINE_VERSION,
    assuranceVersion: ADP_GLOBAL_CERTIFICATION_ENGINE_VERSION,
    certificationTimestamp,
  };
}

/**
 * Identity pre-flight gate before provider execution (shared).
 */
export function runAdpIdentityPreflight(propertyId, options = {}) {
  const profile = options.propertyProfile || loadPropertyProfile(propertyId);
  if (!profile) {
    return {
      outcome: IDENTITY_PREFLIGHT_OUTCOMES.IDENTITY_FAIL,
      hardFailures: [{ code: "PROPERTY_PROFILE_MISSING" }],
    };
  }
  const identity = validateAdpHotelIdentityContract(profile, {
    allProfiles: options.allProfiles || listAllAdpPropertyProfiles(),
  });
  const canary = runIdentityCanaries(profile);
  if (!canary.pass) {
    return {
      ...identity,
      outcome: IDENTITY_PREFLIGHT_OUTCOMES.IDENTITY_FAIL,
      canary,
      hardFailures: [
        ...(identity.hardFailures || []),
        { code: "IDENTITY_CANARY_FAIL", failed: canary.failed },
      ],
    };
  }
  return { ...identity, canary };
}

export {
  explainAdpAnomaly,
  mapLegacyCertificationToPipelineState,
  sha16,
  buildAdpHotelIdentityContract,
};
