/**
 * Run Measurement Assurance V1 for one property (read-only — does not mutate production metrics).
 */

import { createHash } from "crypto";
import {
  ADP_MEASUREMENT_ASSURANCE_VERSION,
  ASSURANCE_LAYERS,
  CERTIFICATION_OUTCOMES,
  LIVE_ASSURANCE_COHORT,
  PIPELINE_STATES,
  REQUIRED_CERTIFICATION_LAYERS,
} from "./version.js";
import { buildObservationLedger } from "./observation-ledger.js";
import {
  buildReferenceMetricPack,
  reconcileProductionVsReference,
} from "./reference-metrics.js";
import { runEntityAssurance } from "./entity-assurance.js";
import { auditScenarioTerritories, DEMAND_TERRITORY_DICTIONARY_V1 } from "./territory-dictionary.js";
import { runAttributeGoldSet, ATTRIBUTE_DICTIONARY_V1 } from "./attribute-dictionary.js";
import {
  auditCustomerClaims,
  detectAnomalies,
} from "./claims-anomalies-publish-guard.js";
import { evaluateSingleCanonicalSubjectPresencePath } from "../certification/single-canonical-subject-presence-path.js";
import { loadPropertyProfile, loadAllPeriods } from "../data-model.js";
import { loadPublishedManifest, loadPublishedReport } from "../published-snapshot.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { getAdpPublishedReadSourceStatus } from "../published-read-service.js";
import { buildOwnerPayload } from "../customer/owner-payload.js";
import {
  MEASUREMENT_CONTRACT_VERSION,
  buildMeasurementContractCanonicalBody,
} from "../contracts/adp-measurement-contract-v1.js";
import {
  buildAnalyticalDiscrepancyRegister,
  CROSS_SURFACE_SEMANTIC_RECONCILIATION,
  ANALYTICAL_COHERENCE,
  DISCREPANCY_CLASSES,
} from "./cross-surface-semantic-reconciliation-v1.js";
import {
  buildProviderCoverageGapLedger,
  evaluateProviderCoverageRecoveryGate,
  PROVIDER_COVERAGE_RECOVERY_GATE,
  DEFECT_UNRECOVERED_PROVIDER_COVERAGE_GAP,
} from "./provider-coverage-recovery-v1.js";
import {
  runPositiveEvidenceContextIntegrity,
  POSITIVE_EVIDENCE_CONTEXT_INTEGRITY,
} from "./positive-evidence-assurance-v1.js";
import {
  runCompetitiveRankHistoryIntegrity,
  COMPETITIVE_RANK_HISTORY_INTEGRITY,
} from "./competitive-rank-history-assurance-v1.js";
import {
  runDisplacementCrossSurfaceAssurance,
  DISPLACEMENT_CROSS_SURFACE_MUST_MATCH,
  DEFECT_DUPLICATED_CONCEPT_CROSS_SURFACE_MISMATCH,
} from "./displacement-cross-surface-assurance-v1.js";

function pickPeriod(propertyId) {
  const man = loadPublishedManifest(propertyId);
  const periods = loadAllPeriods(propertyId);
  if (man?.latestPeriodId) {
    const hit = periods.find((p) => p.periodId === man.latestPeriodId);
    if (hit) return hit;
  }
  return periods.sort((a, b) =>
    String(b.executionDate || "").localeCompare(String(a.executionDate || ""))
  )[0];
}

function extractProductionMetrics(published) {
  const p = published?.payload || published;
  return {
    considerationRate: p?.executiveMetrics?.considerationRate?.rate ?? null,
    scenarioPresence: p?.executiveMetrics?.scenarioPresence?.rate ?? null,
    demandCapture: p?.demandCapture?.overallRate ?? null,
    numberOneAppearance: p?.executiveMetrics?.numberOneAppearanceRate?.rate ?? null,
    top3Appearance: p?.executiveMetrics?.top3AppearanceRate?.rate ?? null,
    propertyRealityCoverage:
      p?.executiveMetrics?.propertyRealityCoverage ??
      p?.trends?.[0]?.propertyRealityCoverage ??
      null,
  };
}

function layerStatus(materialFails, disclosureOnly) {
  if (materialFails.length) return "FAIL";
  if (disclosureOnly.length) return "PASS_WITH_DISCLOSURE";
  return "PASS";
}

export function runPropertyMeasurementAssurance(propertyId, options = {}) {
  const profile = loadPropertyProfile(propertyId);
  const period = options.periodOverride || pickPeriod(propertyId);
  const scenarios = buildScenarioUniverse(profile);
  const published = options.periodOverride ? null : loadPublishedReport(propertyId);
  const readSource = getAdpPublishedReadSourceStatus() || {
    requested: "filesystem",
    active: "filesystem",
  };

  // --- Layer 1: Observation Integrity ---
  const ledger = buildObservationLedger({
    propertyId,
    period,
    scenarios,
    propertyProfile: profile,
  });
  const presencePath = evaluateSingleCanonicalSubjectPresencePath(
    period?.observations || [],
    profile
  );

  // --- Layer 2: Measurement Integrity (independent reference) ---
  const reference = buildReferenceMetricPack(period, scenarios, profile);
  const production = published
    ? extractProductionMetrics(published)
    : {
        considerationRate: null,
        scenarioPresence: null,
        demandCapture: null,
        numberOneAppearance: null,
        top3Appearance: null,
        propertyRealityCoverage: null,
      };
  let metricReconcile = published
    ? reconcileProductionVsReference(production, reference)
    : [];

  // Runtime corrected (reprocessed period via owner payload) — not published
  let runtimeCorrected = null;
  let runtimePayload = null;
  try {
    const payload = buildOwnerPayload(period, scenarios, profile);
    if (payload?.ok !== false) {
      runtimePayload = payload;
      runtimeCorrected = {
        considerationRate: payload.executiveMetrics?.considerationRate?.rate ?? null,
        scenarioPresence: payload.executiveMetrics?.scenarioPresence?.rate ?? null,
        demandCapture: payload.demandCapture?.overallRate ?? null,
      };
    }
  } catch {
    runtimeCorrected = null;
    runtimePayload = null;
  }
  if (!published?.payload && !published && runtimeCorrected) {
    metricReconcile = reconcileProductionVsReference(runtimeCorrected, reference);
  }
  const metricFails = Array.isArray(metricReconcile)
    ? metricReconcile.filter((r) => r.status === "FAIL")
    : [];

  // Cross-surface semantic reconciliation (ANALYTICAL_COHERENCE)
  const analyticalDiscrepancies = buildAnalyticalDiscrepancyRegister({
    propertyId,
    period,
    scenarios,
    propertyProfile: profile,
    payload: runtimePayload || published?.payload || published || {},
    referenceMetrics: reference,
  });

  // Provider coverage recovery (MISSING != ZERO + recovery attempt required)
  const providerCoverageLedger = buildProviderCoverageGapLedger({
    propertyId,
    period,
    scenarios,
    propertyProfile: profile,
  });
  const providerCoverageGate = evaluateProviderCoverageRecoveryGate(providerCoverageLedger);

  const positiveEvidenceIntegrity = runPositiveEvidenceContextIntegrity({
    period,
    scenarios,
    propertyProfile: profile,
    intent: "leisure",
  });
  const competitiveRankHistoryIntegrity = runCompetitiveRankHistoryIntegrity({
    period,
    scenarios,
    propertyProfile: profile,
    allPeriods: loadAllPeriods(propertyId),
    historySnapshots: [],
    certificationStatus: "CERTIFIED_WITH_DISCLOSURES",
  });
  const displacementCrossSurface = runDisplacementCrossSurfaceAssurance({
    observations: period?.observations || [],
    scenarios,
    propertyProfile: profile,
  });

  // Published customer payload may still carry pre-fix Context counts — fail until republish.
  let publishedDisplacementCrossSurface = null;
  const publishedPayload = published?.payload || published || null;
  if (publishedPayload?.lostDemand?.displacement && publishedPayload?.competitiveRankingByTerritory) {
    const pubMismatches = [];
    const overallRows =
      publishedPayload.competitiveRankingByTerritory?.byTerritory?.overall?.displayRows || [];
    const ctx = publishedPayload.lostDemand.displacement || [];
    const ctxById = Object.create(null);
    for (const d of ctx) {
      if (d.entityId) ctxById[d.entityId] = Number(d.displacementCount) || 0;
    }
    for (const row of overallRows) {
      if (row.isSubject || !row.entityId || !row.displacement) continue;
      if (!(row.entityId in ctxById)) continue;
      const ov = Number(row.displacement.count) || 0;
      const cx = ctxById[row.entityId];
      if (ov !== cx) {
        pubMismatches.push({
          entityId: row.entityId,
          name: row.name,
          overviewDisplacement: ov,
          contextDisplacement: cx,
          defect: DEFECT_DUPLICATED_CONCEPT_CROSS_SURFACE_MISMATCH,
          source: "published_payload",
        });
      }
    }
    publishedDisplacementCrossSurface = {
      gate: DISPLACEMENT_CROSS_SURFACE_MUST_MATCH,
      status: pubMismatches.length ? "FAIL" : "PASS",
      mismatches: pubMismatches,
      materialBlocker: pubMismatches.length > 0,
    };
    if (publishedDisplacementCrossSurface.materialBlocker) {
      // Elevate runtime gate to reflect live customer page contradiction
      displacementCrossSurface.publishedCustomerPageMismatch = true;
      displacementCrossSurface.publishedMismatches = pubMismatches;
      if (displacementCrossSurface.status === "PASS") {
        displacementCrossSurface.status = "FAIL";
        displacementCrossSurface.materialBlocker = true;
        displacementCrossSurface.overallMismatches = pubMismatches;
        displacementCrossSurface.failReason = "PUBLISHED_PAYLOAD_STALE_CONTEXT_DISPLACEMENT";
      }
    }
  }

  // Provider completeness from reference
  const providerAssurance = {
    providers: reference.providers,
    missingEqualsZeroViolations: reference.providers.filter(
      (p) => p.presence === 0 && p.comparable === 0 && p.scheduled > 0
    ),
    rule: "MISSING != ZERO; denominator = comparable_observations",
  };

  // Entity
  const customerNames = [
    ...new Set(
      (published?.payload?.competitiveSet?.observed || published?.competitiveSet?.observed || [])
        .map((o) => o.name || o)
        .filter(Boolean)
    ),
  ];
  const entity = runEntityAssurance(profile, customerNames);

  // --- Layer 3: Comparability ---
  const territories = auditScenarioTerritories(scenarios, propertyId);
  const attributes = runAttributeGoldSet();
  const contract = buildMeasurementContractCanonicalBody();

  // Claims + anomalies
  const claims = auditCustomerClaims(published?.payload || published || {});
  const anomalies = detectAnomalies({
    ledger,
    reference,
    production,
    providers: reference.providers,
  });

  // --- Layer 4: Publication ---
  const publication = {
    readSource: readSource.active || readSource.requested || "unknown",
    readSourceOk: (readSource.active || "filesystem") === "filesystem",
    periodId: period?.periodId || null,
    publishedPeriodId: published?.periodId || published?.payload?.period?.periodId || null,
    periodMatch:
      period?.periodId &&
      (published?.periodId || published?.payload?.period?.periodId) === period.periodId,
    contractVersion: MEASUREMENT_CONTRACT_VERSION,
    snapshotPath: published
      ? `data/ai-demand-positioning/published/${propertyId}/report-${period?.periodId}.json`
      : null,
  };

  // Evidence completeness (structural)
  const evidence = {
    hasCompetitiveSet: Boolean(
      published?.payload?.competitiveSet || published?.competitiveSet
    ),
    hasActions: Boolean((published?.payload?.actions || published?.actions || []).length),
    hasTrends: Boolean((published?.payload?.trends || published?.trends || []).length),
    hasDemandCapture: published?.payload?.demandCapture != null || published?.demandCapture != null,
    status: "STRUCTURAL_ONLY",
  };

  // Manual review queue items
  const manualItems = [];
  for (const row of ledger.rows) {
    if (row.reviewerStatus === "MANUAL_REVIEW") {
      manualItems.push({
        type: "OBSERVATION",
        observationId: row.observationId,
        class: row.disagreementClass,
        material: true,
      });
    }
  }
  for (const g of entity.goldResults || []) {
    if (g.status === "AMBIGUOUS_ALIAS_GAP") {
      manualItems.push({
        type: "ENTITY_ALIAS",
        id: g.id,
        material: true,
        note: g.note,
      });
    }
  }
  for (const a of anomalies) {
    if (a.requiresReview) {
      manualItems.push({
        type: "ANOMALY",
        code: a.code,
        material: a.severity === "MATERIAL",
        detail: a.detail,
      });
    }
  }

  const materialBlockers = [];
  if (presencePath.status === "FAIL") {
    materialBlockers.push({
      layer: ASSURANCE_LAYERS.MEASUREMENT_INTEGRITY,
      gate: "SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH",
      summary: presencePath.summary,
    });
  }
  if (ledger.enrichPathFlips > 0) {
    materialBlockers.push({
      layer: ASSURANCE_LAYERS.OBSERVATION_INTEGRITY,
      gate: "SUBJECT_PRESENCE_VALIDATED",
      summary: `Metric projection diverges from governedInterpretation on ${ledger.enrichPathFlips} observations`,
    });
  }
  if (presencePath.status === "FAIL" && presencePath.details?.runtime?.missingGoverned > 0) {
    materialBlockers.push({
      layer: ASSURANCE_LAYERS.OBSERVATION_INTEGRITY,
      gate: "SUBJECT_PRESENCE_VALIDATED",
      summary: `${presencePath.details.runtime.missingGoverned} observations missing governedInterpretation`,
    });
  }
  for (const m of metricFails) {
    if (Math.abs(m.delta || 0) >= 5) {
      materialBlockers.push({
        layer: ASSURANCE_LAYERS.PUBLICATION_INTEGRITY,
        gate: "REFERENCE_METRIC_RECONCILE",
        summary: `Published ${m.metric}: prod=${m.production} ref=${m.reference} delta=${m.delta} (republish required; runtime corrected not published)`,
      });
    }
  }
  if (claims.status === "FAIL") {
    materialBlockers.push({
      layer: ASSURANCE_LAYERS.PUBLICATION_INTEGRITY,
      gate: "CUSTOMER_CLAIMS",
      summary: `${claims.unsupportedCount} unsupported claims`,
    });
  }
  if (!publication.readSourceOk) {
    materialBlockers.push({
      layer: ASSURANCE_LAYERS.PUBLICATION_INTEGRITY,
      gate: "PRODUCTION_SOURCE",
      summary: `active read source=${publication.readSource}`,
    });
  }
  if (entity.counts.goldFail > 0) {
    materialBlockers.push({
      layer: ASSURANCE_LAYERS.OBSERVATION_INTEGRITY,
      gate: "ENTITY_GOLD",
      summary: `${entity.counts.goldFail} entity gold failures`,
    });
  }
  if (analyticalDiscrepancies.status === "FAIL") {
    materialBlockers.push({
      layer: ASSURANCE_LAYERS.ANALYTICAL_COHERENCE,
      gate: CROSS_SURFACE_SEMANTIC_RECONCILIATION,
      principle: ANALYTICAL_COHERENCE,
      summary: `${analyticalDiscrepancies.blockerCount} cross-surface analytical contradiction(s)`,
      discrepancies: analyticalDiscrepancies.discrepancies.filter(
        (d) => d.classification === DISCREPANCY_CLASSES.MATERIAL_CERTIFICATION_BLOCKER && d.match === false
      ),
    });
  }
  if (providerCoverageGate.blocksFullCertification) {
    materialBlockers.push({
      layer: ASSURANCE_LAYERS.MEASUREMENT_INTEGRITY,
      gate: PROVIDER_COVERAGE_RECOVERY_GATE,
      defectClass: DEFECT_UNRECOVERED_PROVIDER_COVERAGE_GAP,
      summary: providerCoverageGate.summary,
      certificationImpact: providerCoverageGate.certificationImpact,
      recoverableUnattempted: providerCoverageLedger.recoverableUnattempted.length,
      materialProviders: providerCoverageLedger.materialProviders,
      reviewRequired: true,
    });
  }
  if (positiveEvidenceIntegrity.status === "FAIL") {
    materialBlockers.push({
      layer: ASSURANCE_LAYERS.ANALYTICAL_COHERENCE,
      gate: POSITIVE_EVIDENCE_CONTEXT_INTEGRITY,
      summary: `${positiveEvidenceIntegrity.defects.length} positive evidence integrity defect(s)`,
      defects: positiveEvidenceIntegrity.defects.slice(0, 8),
    });
  }
  if (competitiveRankHistoryIntegrity.status === "FAIL") {
    materialBlockers.push({
      layer: ASSURANCE_LAYERS.ANALYTICAL_COHERENCE,
      gate: COMPETITIVE_RANK_HISTORY_INTEGRITY,
      summary: `${competitiveRankHistoryIntegrity.defects.length} competitive rank history defect(s)`,
      defects: competitiveRankHistoryIntegrity.defects.slice(0, 8),
    });
  }
  if (displacementCrossSurface.materialBlocker || displacementCrossSurface.status === "FAIL") {
    materialBlockers.push({
      layer: ASSURANCE_LAYERS.ANALYTICAL_COHERENCE,
      gate: DISPLACEMENT_CROSS_SURFACE_MUST_MATCH,
      defectClass: DEFECT_DUPLICATED_CONCEPT_CROSS_SURFACE_MISMATCH,
      principle: "SAME_CONCEPT_SAME_CANONICAL_SOURCE",
      summary: `${(displacementCrossSurface.overallMismatches || []).length} Overview↔Context displacement mismatch(es)`,
      mismatches: (displacementCrossSurface.overallMismatches || []).slice(0, 10),
      reviewRequired: true,
    });
  }

  const disclosureOnly = [];
  if (ledger.subjectDisagreement > 0 && ledger.falsePositives < 10) {
    disclosureOnly.push("Non-zero subject disagreement under disclosure threshold");
  }
  if (territories.review > 0) {
    disclosureOnly.push(`${territories.review} territory heuristic REVIEW rows`);
  }
  if (entity.counts.goldAmbiguous > 0) {
    disclosureOnly.push(`${entity.counts.goldAmbiguous} alias-gap ambiguous entity cases`);
  }
  if (analyticalDiscrepancies.clarificationCount > 0) {
    disclosureOnly.push(
      `${analyticalDiscrepancies.clarificationCount} cross-surface presentation clarification(s)`
    );
  }
  if (
    !providerCoverageGate.blocksFullCertification &&
    providerCoverageLedger.missingObservations?.length
  ) {
    disclosureOnly.push(
      `Residual provider coverage gaps: ${providerCoverageLedger.missingObservations.length} missing observation(s) (MISSING≠ZERO; recovery complete or non-material)`
    );
  }

  const layers = {
    [ASSURANCE_LAYERS.OBSERVATION_INTEGRITY]: {
      status: layerStatus(
        materialBlockers.filter((b) => b.layer === ASSURANCE_LAYERS.OBSERVATION_INTEGRITY),
        disclosureOnly.filter((d) => /subject|entity|alias/i.test(d))
      ),
      ledgerSummary: {
        total: ledger.totalObservations,
        agree: ledger.subjectAgreement,
        disagree: ledger.subjectDisagreement,
        fp: ledger.falsePositives,
        fn: ledger.falseNegatives,
        enrichFlips: ledger.enrichPathFlips,
      },
      entity: entity.counts,
    },
    [ASSURANCE_LAYERS.MEASUREMENT_INTEGRITY]: {
      status: layerStatus(
        materialBlockers.filter((b) => b.layer === ASSURANCE_LAYERS.MEASUREMENT_INTEGRITY),
        []
      ),
      singleCanonicalPath: presencePath,
      metricReconcile,
      providerAssurance,
      providerCoverageRecovery: {
        gate: providerCoverageGate,
        ledgerSummary: {
          missing: providerCoverageLedger.missingObservations?.length || 0,
          recoverableUnattempted: providerCoverageLedger.recoverableUnattempted?.length || 0,
          materialProviders: providerCoverageLedger.materialProviders,
          estimatedRecoveryCostUsd: providerCoverageLedger.recoveryPreview?.estimatedCostUsd,
          recoveryExecuted: providerCoverageLedger.recoveryPreview?.executed || false,
          residualCoverage: providerCoverageLedger.residualCoverage,
        },
      },
      reference,
      production,
      runtimeCorrected,
      publishedStaleVsRuntime:
        runtimeCorrected &&
        production?.demandCapture != null &&
        runtimeCorrected.demandCapture != null &&
        Math.abs(production.demandCapture - runtimeCorrected.demandCapture) >= 5,
    },
    [ASSURANCE_LAYERS.COMPARABILITY_INTEGRITY]: {
      status: territories.fail ? "FAIL" : "PASS_WITH_DISCLOSURE",
      contractVersion: MEASUREMENT_CONTRACT_VERSION,
      territoryDictionaryVersion: DEMAND_TERRITORY_DICTIONARY_V1.version,
      attributeDictionaryVersion: ATTRIBUTE_DICTIONARY_V1.version,
      territories,
      attributes,
      subjectPresenceRule: contract.SUBJECT_PRESENCE_RULE,
    },
    [ASSURANCE_LAYERS.ANALYTICAL_COHERENCE]: {
      status: layerStatus(
        materialBlockers.filter((b) => b.layer === ASSURANCE_LAYERS.ANALYTICAL_COHERENCE),
        disclosureOnly.filter((d) => /cross-surface|analytical/i.test(d))
      ),
      gate: CROSS_SURFACE_SEMANTIC_RECONCILIATION,
      principle: ANALYTICAL_COHERENCE,
      analyticalDiscrepancies,
      positiveEvidenceIntegrity,
      competitiveRankHistoryIntegrity,
      displacementCrossSurface,
      title: analyticalDiscrepancies.title || "ANALYTICAL DISCREPANCIES / QUESTIONS",
      relationshipCoverage: analyticalDiscrepancies.relationshipCoverage,
      hardInvariants: analyticalDiscrepancies.hardInvariants,
    },
    [ASSURANCE_LAYERS.PUBLICATION_INTEGRITY]: {
      status: layerStatus(
        materialBlockers.filter((b) => b.layer === ASSURANCE_LAYERS.PUBLICATION_INTEGRITY),
        []
      ),
      publication,
      claims,
      evidence,
    },
  };

  let certificationStatus = CERTIFICATION_OUTCOMES.CERTIFIED;
  if (materialBlockers.length) {
    const reviewOnlyGates = new Set([
      PROVIDER_COVERAGE_RECOVERY_GATE,
      DISPLACEMENT_CROSS_SURFACE_MUST_MATCH,
    ]);
    const onlyReviewRequired =
      materialBlockers.every((b) => reviewOnlyGates.has(b.gate) && b.reviewRequired) &&
      materialBlockers.some((b) => b.reviewRequired);
    const hasHardBlockers = materialBlockers.some(
      (b) => !(reviewOnlyGates.has(b.gate) && b.reviewRequired)
    );
    certificationStatus = hasHardBlockers
      ? CERTIFICATION_OUTCOMES.NOT_CERTIFIED
      : onlyReviewRequired
        ? CERTIFICATION_OUTCOMES.ASSURANCE_REVIEW_REQUIRED
        : CERTIFICATION_OUTCOMES.NOT_CERTIFIED;
  } else if (disclosureOnly.length || anomalies.some((a) => a.severity === "REVIEW")) {
    certificationStatus = CERTIFICATION_OUTCOMES.CERTIFIED_WITH_DISCLOSURES;
  }

  // Material manual items block CERTIFIED
  const materialManual = manualItems.filter((m) => m.material);
  if (materialManual.length && certificationStatus === CERTIFICATION_OUTCOMES.CERTIFIED) {
    certificationStatus = CERTIFICATION_OUTCOMES.ASSURANCE_REVIEW_REQUIRED;
  }
  // Dual path structural block removed after Option A; missing governed still blocks.
  if (ledger.dualPathActive) {
    certificationStatus = CERTIFICATION_OUTCOMES.NOT_CERTIFIED;
  }
  if (presencePath.status === "FAIL") {
    certificationStatus = CERTIFICATION_OUTCOMES.NOT_CERTIFIED;
  }

  const certificationRecord = {
    propertyId,
    periodId: period?.periodId,
    certificationStatus,
    certificationTimestamp: new Date().toISOString(),
    assuranceVersion: ADP_MEASUREMENT_ASSURANCE_VERSION,
    measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
    parserVersion: "detectPropertyMention+extractPropertyRank",
    entityResolverVersion: "customer-entity-resolution-v1",
    attributeDictionaryVersion: ATTRIBUTE_DICTIONARY_V1.version,
    scenarioPackVersion: "adp_scenario_universe_v1",
    observationCounts: {
      total: ledger.totalObservations,
      comparable: reference.considerationRate != null ? "see reference" : null,
    },
    providerCompleteness: reference.providers,
    activeProductionSource: publication.readSource,
    pipelineState:
      certificationStatus === CERTIFICATION_OUTCOMES.CERTIFIED
        ? PIPELINE_STATES.CERTIFIED
        : certificationStatus === CERTIFICATION_OUTCOMES.CERTIFIED_WITH_DISCLOSURES
          ? PIPELINE_STATES.CERTIFIED_WITH_DISCLOSURES
          : certificationStatus === CERTIFICATION_OUTCOMES.ASSURANCE_REVIEW_REQUIRED
            ? PIPELINE_STATES.ASSURANCE_REVIEW_REQUIRED
            : PIPELINE_STATES.NOT_CERTIFIED,
  };

  const report = {
    title: "ADP_MEASUREMENT_ASSURANCE_V1",
    propertyId,
    propertyName: profile.name,
    periodId: period?.periodId,
    finished: new Date().toISOString(),
    certificationStatus,
    requiredCertificationLayers: REQUIRED_CERTIFICATION_LAYERS,
    materialBlockers,
    disclosures: disclosureOnly,
    analyticalDiscrepancies,
    providerCoverageRecovery: {
      gate: providerCoverageGate,
      preview: providerCoverageLedger.recoveryPreview,
      materialProviders: providerCoverageLedger.materialProviders,
      residualCoverage: providerCoverageLedger.residualCoverage,
    },
    layers,
    anomalies,
    manualReviewQueue: manualItems,
    certificationRecord,
    methodologyChanged: false,
    productionMetricsMutated: false,
  };

  const hash = createHash("sha256").update(JSON.stringify(report)).digest("hex").slice(0, 16);
  report.certificationRecord.reportHash = hash;

  // Gold-set eligible rows (auto agree or auto-classified with gold truth)
  const goldCandidates = ledger.rows
    .filter(
      (r) =>
        r.reviewerStatus === "AUTO_AGREE" ||
        (r.reviewerStatus === "AUTO_CLASSIFIED" && r.goldSubjectMentioned != null)
    )
    .map((r) => ({
      observationId: r.observationId,
      propertyId: r.propertyId,
      periodId: r.periodId,
      scenarioId: r.scenarioId,
      provider: r.provider,
      demandTerritory: r.demandTerritory,
      canonicalSubject: r.canonicalSubjectEntity,
      subjectMentionedTruth: r.goldSubjectMentioned,
      subjectRankTruth: r.goldRank,
      reviewSource: r.reviewerStatus,
      reviewDate: report.finished,
      assuranceVersion: ADP_MEASUREMENT_ASSURANCE_VERSION,
      ambiguityFlag: false,
    }));

  return {
    report,
    ledger,
    goldCandidates,
    options,
  };
}

export function runPortfolioMeasurementAssurance(propertyIds = LIVE_ASSURANCE_COHORT) {
  const results = {};
  for (const id of propertyIds) {
    results[id] = runPropertyMeasurementAssurance(id);
  }
  return results;
}
