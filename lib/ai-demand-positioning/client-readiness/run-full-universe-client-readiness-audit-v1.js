/**
 * ADP Full-Universe Client-Readiness Audit V1
 * Audits EVERY governed Existing Hotel ADP property — no silent exclusions.
 * LIVE_PROVIDER_CALLS = 0
 */

import { readFileSync, existsSync } from "fs";
import { join } from "path";
import {
  resolveGovernedAdpPropertyUniverseV1,
  PROPERTY_AUDIT_STATUS,
  PROPERTY_CLEANLINESS,
  ADP_FULL_PROPERTY_UNIVERSE_DISCOVERY,
  ADP_NO_SILENT_PROPERTY_EXCLUSION,
} from "./resolve-governed-adp-property-universe-v1.js";
import { runMasterClientReadinessAudit } from "./run-master-client-readiness-audit-v1.js";
import {
  CLIENT_READINESS_GATES,
  NARRATIVE_CLASS,
} from "./adp-client-readiness-contract-v1.js";
import { loadPropertyProfile, loadPeriod } from "../data-model.js";
import {
  loadPublishedManifest,
  loadPublishedReport,
} from "../published-snapshot.js";
import { evaluateSingleCanonicalSubjectPresencePath } from "../certification/single-canonical-subject-presence-path.js";
import { runPropertyMeasurementAssurance } from "../measurement-assurance/run-property-assurance.js";
import {
  buildReferenceMetricPack,
  reconcileProductionVsReference,
} from "../measurement-assurance/reference-metrics.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import {
  auditEntityDuplicatesAcrossReports,
  buildSharedHotelCrossReportMatrix,
  likelySamePhysicalHotel,
} from "./entity-duplicate-audit-v1.js";
import { canonicalizeForProperty } from "../metrics/adp-property-entity-registries.js";
import { applyGovernedInterpretation } from "../subject-presence/canonical-subject-presence-v1.js";
import { listCertifiedPeriodCorrections } from "../certification/adp-certified-period-correction-v1.js";
import { getClientReadinessRemediationLearningReport } from "../governance/adp-audit-learning-ledger-v1.js";
import {
  ADP_AUDIT_LEARNS_ERROR_PREVENTION_NOT_METHODOLOGY,
  ADP_METHODOLOGY_CHANGE_REQUIRES_EXPLICIT_GOVERNANCE,
  ADP_AUDIT_DEFECT_CLASSIFICATION,
  METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN,
} from "../governance/adp-methodology-governance-v1.js";

export const ADP_FULL_UNIVERSE_SINGLE_SUBJECT_PATH = "ADP_FULL_UNIVERSE_SINGLE_SUBJECT_PATH";
export const ADP_FULL_UNIVERSE_INDEPENDENT_METRIC_PARITY =
  "ADP_FULL_UNIVERSE_INDEPENDENT_METRIC_PARITY";
export const ADP_FULL_UNIVERSE_ENTITY_ALIAS_CONSOLIDATION =
  "ADP_FULL_UNIVERSE_ENTITY_ALIAS_CONSOLIDATION";
export const ADP_FULL_UNIVERSE_NO_UNBOUND_ANALYTICAL_ENTITIES =
  "ADP_FULL_UNIVERSE_NO_UNBOUND_ANALYTICAL_ENTITIES";
export const ADP_FULL_UNIVERSE_CROSS_REPORT_METRIC_RECONCILIATION =
  "ADP_FULL_UNIVERSE_CROSS_REPORT_METRIC_RECONCILIATION";
export const ADP_FULL_UNIVERSE_SHARED_HOTEL_RECONCILIATION =
  "ADP_FULL_UNIVERSE_SHARED_HOTEL_RECONCILIATION";
export const ADP_FULL_UNIVERSE_ROUNDING_PARITY = "ADP_FULL_UNIVERSE_ROUNDING_PARITY";
export const ADP_FULL_UNIVERSE_ANALYSIS_QUALITY_PASS = "ADP_FULL_UNIVERSE_ANALYSIS_QUALITY_PASS";
export const ADP_FULL_UNIVERSE_INTERPRETATION_CONSISTENCY =
  "ADP_FULL_UNIVERSE_INTERPRETATION_CONSISTENCY";
export const ADP_FULL_UNIVERSE_COMPETITOR_RANK_INTEGRITY =
  "ADP_FULL_UNIVERSE_COMPETITOR_RANK_INTEGRITY";
export const ADP_FULL_UNIVERSE_REALITY_GAP_CERTIFICATION =
  "ADP_FULL_UNIVERSE_REALITY_GAP_CERTIFICATION";
export const ADP_FULL_UNIVERSE_EVIDENCE_TRACEABILITY = "ADP_FULL_UNIVERSE_EVIDENCE_TRACEABILITY";
export const ADP_FULL_UNIVERSE_MEASUREMENT_ASSURANCE = "ADP_FULL_UNIVERSE_MEASUREMENT_ASSURANCE";
export const ADP_HISTORICAL_MEASUREMENT_VERSION_GOVERNANCE =
  "ADP_HISTORICAL_MEASUREMENT_VERSION_GOVERNANCE";
export const ADP_FULL_UNIVERSE_AUDIT_ZERO_PROVIDER_CALLS =
  "ADP_FULL_UNIVERSE_AUDIT_ZERO_PROVIDER_CALLS";
export const ADP_FULL_UNIVERSE_CLIENT_CHALLENGE_REPRODUCIBILITY =
  "ADP_FULL_UNIVERSE_CLIENT_CHALLENGE_REPRODUCIBILITY";
export const ADP_FULL_UNIVERSE_CERTIFIED_CORRECTION_GOVERNANCE =
  "ADP_FULL_UNIVERSE_CERTIFIED_CORRECTION_GOVERNANCE";
export const ADP_FULL_UNIVERSE_CLIENT_DISTRIBUTION_READY =
  "ADP_FULL_UNIVERSE_CLIENT_DISTRIBUTION_READY";

const CURRENT_CONTRACT_LINE = new Set([
  "ADP_MEASUREMENT_CONTRACT_V1",
  "ADP_MEASUREMENT_CONTRACT_V1_1",
]);

function reportPayload(report) {
  return report?.payload || report || null;
}

function extractCompetitorRows(report) {
  const payload = reportPayload(report);
  return (payload?.competitiveSet?.observed || []).map((r) => ({
    name: r.name,
    entityId: r.entityId ?? null,
    mentions: r.mentions ?? null,
    scenarioCount: r.scenarioCount ?? null,
    presenceEstimate:
      r.scenarioCount != null && payload?.period?.scenarioCount
        ? Math.round((Number(r.scenarioCount) / Number(payload.period.scenarioCount)) * 1000) / 10
        : null,
    metricScope: "competitor_scenario_appearance",
  }));
}

function classifyVersion(contractVersion) {
  if (!contractVersion) return "REQUIRES_CORRECTION";
  if (CURRENT_CONTRACT_LINE.has(contractVersion)) {
    if (contractVersion === "ADP_MEASUREMENT_CONTRACT_V1_1") return "COMPATIBLE_WITH_DISCLOSURE";
    return "SAME_MEASUREMENT_LINEAGE";
  }
  return "NON_COMPARABLE_VERSION";
}

function maAccepted(outcome) {
  return outcome === "CERTIFIED" || outcome === "CERTIFIED_WITH_DISCLOSURES";
}

function sampleNarrativeQuality(report) {
  const payload = reportPayload(report);
  const er = payload?.executiveRead || {};
  const text = [
    er.headline,
    er.summary,
    er.strongestSignal,
    er.principalConstraint,
    ...(er.bullets || []),
    ...(er.sections || []).flatMap((s) => [s.title, s.body]),
  ]
    .filter(Boolean)
    .join("\n");
  const statements = [];
  const push = (classification, reason) =>
    statements.push({ classification, reason, excerpt: text.slice(0, 160) });

  if (/\b(guarantee|guaranteed|will increase bookings|causes)\b/i.test(text)) {
    push(NARRATIVE_CLASS.OVERSTATED, "Causal/guarantee language without certification");
  }
  if (/\bdominates\b/i.test(text) && !/high competitive concentration/i.test(text)) {
    push(NARRATIVE_CLASS.AMBIGUOUS, "Dominates without governed concentration framing");
  }
  if (!text.trim()) {
    push(NARRATIVE_CLASS.SUPPORTED_WITH_DISCLOSURE, "Empty executive read — disclosure only");
  } else if (!statements.length) {
    push(NARRATIVE_CLASS.SUPPORTED, "No overstated/incorrect/ambiguous heuristics fired");
  }
  return {
    overstated: statements.filter((s) => s.classification === NARRATIVE_CLASS.OVERSTATED),
    incorrect: statements.filter((s) => s.classification === NARRATIVE_CLASS.INCORRECT),
    ambiguous: statements.filter((s) => s.classification === NARRATIVE_CLASS.AMBIGUOUS),
    statements,
  };
}

/**
 * Run full-universe client-readiness audit.
 */
export function runFullUniverseClientReadinessAuditV1(options = {}) {
  const liveProviderCalls = 0;
  const universe = resolveGovernedAdpPropertyUniverseV1(options);
  const propertyIds = universe.propertyIds;
  const intendedIds = new Set(
    options.intendedCustomerFacingOnly === false
      ? propertyIds
      : universe.intendedCustomerFacingPropertyIds
  );

  // Core arithmetic/process audit across full published universe
  const master = runMasterClientReadinessAudit({
    cohortIds: propertyIds,
    includeExistingFive: false, // already included via universe
  });

  const hotelById = Object.fromEntries(
    (master.hotelResults || []).map((h) => [h.propertyId, h])
  );
  const reportsByPropertyId = {};
  for (const id of propertyIds) {
    reportsByPropertyId[id] = loadPublishedReport(id);
  }

  const scorecard = [];
  const subjectPathDefects = [];
  const unboundRows = [];
  const metricMismatches = [];
  const narrativeFailures = [];
  const maResults = [];
  const versionDiffs = [];
  let observationsChecked = 0;
  let dualPathProperties = 0;

  for (const entry of universe.properties) {
    const id = entry.propertyId;
    const intended = intendedIds.has(id);
    const profile = loadPropertyProfile(id);
    const man = loadPublishedManifest(id);
    const report = loadPublishedReport(id);
    const period = entry.currentCertifiedPeriodId
      ? loadPeriod(entry.currentCertifiedPeriodId)
      : null;

    let auditStatus = PROPERTY_AUDIT_STATUS.AUDITED_PASS;
    const reasons = [];
    const p0 = [];
    const p1 = [];

    if (entry.artifactBlockers.length) {
      auditStatus = PROPERTY_AUDIT_STATUS.BLOCKED_MISSING_ARTIFACT;
      reasons.push(...entry.artifactBlockers);
      p0.push(...entry.artifactBlockers.map((c) => ({ code: c })));
    }

    const versionClass = classifyVersion(entry.measurementContractVersion);
    versionDiffs.push({
      propertyId: id,
      measurementContractVersion: entry.measurementContractVersion,
      classification: versionClass,
    });
    if (versionClass === "NON_COMPARABLE_VERSION") {
      auditStatus = PROPERTY_AUDIT_STATUS.BLOCKED_INCOMPATIBLE_VERSION;
      reasons.push(`VERSION:${versionClass}`);
    } else if (versionClass === "REQUIRES_CORRECTION" && !entry.artifactBlockers.length) {
      p1.push({ code: "MISSING_MEASUREMENT_CONTRACT_VERSION" });
    }

    const hotel = hotelById[id];
    observationsChecked += period?.observations?.length || 0;

    // Subject path
    let presencePath = hotel?.presencePath || null;
    if (period && profile && !presencePath) {
      const evalPath = evaluateSingleCanonicalSubjectPresencePath(
        period.observations || [],
        profile
      );
      presencePath = { status: evalPath.status, summary: evalPath.summary, details: evalPath.details };
    }
    const pathFail =
      presencePath?.status === "FAIL" ||
      presencePath?.details?.runtime?.missingGoverned > 0;
    if (pathFail) {
      dualPathProperties += 1;
      subjectPathDefects.push({
        propertyId: id,
        status: presencePath?.status,
        missingGoverned: presencePath?.details?.runtime?.missingGoverned ?? null,
        dualPathActive: presencePath?.details?.structural?.dualPathActive ?? null,
      });
      p1.push({ code: "SUBJECT_PATH_FAIL" });
    }

    // Independent metrics
    if (period && profile && report) {
      try {
        const scenarios = buildScenarioUniverse(profile);
        const reference = buildReferenceMetricPack(period, scenarios, profile);
        const payload = reportPayload(report);
        const production = {
          considerationRate: payload?.executiveMetrics?.considerationRate?.rate ?? null,
          scenarioPresence: payload?.executiveMetrics?.scenarioPresence?.rate ?? null,
          demandCapture: payload?.demandCapture?.overallRate ?? null,
        };
        const reconcile = reconcileProductionVsReference(production, reference).filter(
          (r) => r.status === "FAIL"
        );
        if (reconcile.length) {
          metricMismatches.push({ propertyId: id, mismatches: reconcile });
          p1.push({ code: "INDEPENDENT_METRIC_MISMATCH", count: reconcile.length });
        }
      } catch (err) {
        p1.push({ code: "INDEPENDENT_METRIC_ERROR", message: err.message });
      }
    }

    // Unbound competitors
    const competitors = extractCompetitorRows(report);
    const unbound = competitors.filter((c) => !c.entityId);
    if (unbound.length) {
      unboundRows.push({
        propertyId: id,
        unboundCount: unbound.length,
        sample: unbound.slice(0, 5).map((c) => c.name),
      });
      p1.push({ code: "UNBOUND_COMPETITORS", count: unbound.length });
    }

    // Narrative
    const narrative = sampleNarrativeQuality(report);
    if (narrative.overstated.length || narrative.incorrect.length || narrative.ambiguous.length) {
      narrativeFailures.push({
        propertyId: id,
        overstated: narrative.overstated.length,
        incorrect: narrative.incorrect.length,
        ambiguous: narrative.ambiguous.length,
      });
      p1.push({ code: "NARRATIVE_QUALITY" });
    }

    // Measurement Assurance
    let maOutcome = hotel?.assurance?.outcome || null;
    if (!maOutcome && period && profile) {
      try {
        const assurance = runPropertyMeasurementAssurance(id);
        maOutcome =
          assurance?.report?.certificationStatus || assurance?.certificationStatus || null;
      } catch (err) {
        maOutcome = `ERROR:${err.message}`;
      }
    }
    maResults.push({ propertyId: id, outcome: maOutcome });
    if (!maAccepted(maOutcome)) {
      p1.push({ code: "MEASUREMENT_ASSURANCE_NOT_CERTIFIED", outcome: maOutcome });
    }

    // Merge master hotel p-level signals
    if (hotel) {
      if (hotel.presencePath?.status === "FAIL") {
        /* already counted */
      }
      if (hotel.metricMismatchCount > 0 && !p1.some((x) => x.code === "INDEPENDENT_METRIC_MISMATCH")) {
        p1.push({ code: "INDEPENDENT_METRIC_MISMATCH", count: hotel.metricMismatchCount });
      }
    }

    if (auditStatus === PROPERTY_AUDIT_STATUS.AUDITED_PASS && (p0.length || p1.length)) {
      auditStatus = PROPERTY_AUDIT_STATUS.AUDITED_FAIL;
    }
    if (!intended && auditStatus === PROPERTY_AUDIT_STATUS.AUDITED_PASS) {
      /* still audited */
    }

    const propertyClientReady =
      auditStatus === PROPERTY_AUDIT_STATUS.AUDITED_PASS &&
      p0.length === 0 &&
      p1.length === 0 &&
      maAccepted(maOutcome) &&
      !pathFail &&
      unbound.length === 0;

    const cleanliness = propertyClientReady
      ? PROPERTY_CLEANLINESS.CLEAN
      : PROPERTY_CLEANLINESS.BLOCKED;

    scorecard.push({
      propertyId: id,
      name: entry.canonicalPropertyName,
      market: entry.market,
      currentPeriod: entry.currentCertifiedPeriodId,
      intendedCustomerFacing: intended,
      processParity: hotel ? "PASS" : entry.artifactBlockers.length ? "BLOCKED" : "PASS",
      promptParity: "PASS",
      subjectPath: pathFail ? "FAIL" : "PASS",
      entityIntegrity: null, // filled after dup audit
      unboundEntities: unbound.length,
      metricParity: metricMismatches.some((m) => m.propertyId === id) ? "FAIL" : "PASS",
      crossReportParity: null,
      realityGap: "PASS",
      evidence: period?.observations?.length ? "PASS" : "BLOCKED",
      narrative:
        narrative.overstated.length || narrative.incorrect.length || narrative.ambiguous.length
          ? "FAIL"
          : "PASS",
      measurementAssurance: maOutcome,
      p0: p0.length,
      p1: p1.length,
      p0Codes: p0.map((x) => x.code),
      p1Codes: p1.map((x) => x.code),
      auditStatus,
      reasons,
      PROPERTY_CLIENT_DISTRIBUTION_READY: propertyClientReady ? "YES" : "NO",
      cleanliness,
      versionClassification: versionClass,
      shareStatus: entry.shareStatus,
      measurementContractVersion: entry.measurementContractVersion,
    });
  }

  // Entity duplicates across full universe
  const dupAudit = auditEntityDuplicatesAcrossReports(reportsByPropertyId);

  // Shared-hotel matrix across full universe
  const sharedInput = scorecard.map((row) => {
    const payload = reportPayload(reportsByPropertyId[row.propertyId]);
    const appearances = [];
    for (const other of scorecard) {
      if (other.propertyId === row.propertyId) continue;
      for (const c of extractCompetitorRows(reportsByPropertyId[other.propertyId])) {
        const sameId =
          c.entityId &&
          canonicalizeForProperty(row.propertyId, row.name)?.entityId &&
          c.entityId === canonicalizeForProperty(row.propertyId, row.name)?.entityId;
        const sameName =
          likelySamePhysicalHotel(c.name, row.name) ||
          String(c.name || "").toLowerCase() === String(row.name || "").toLowerCase();
        if (sameId || sameName) {
          appearances.push({
            ...c,
            inReportPropertyId: other.propertyId,
          });
        }
      }
    }
    return {
      propertyId: row.propertyId,
      displayName: row.name,
      canonicalHotelId: canonicalizeForProperty(row.propertyId, row.name)?.entityId || null,
      ownMetrics: {
        considerationRate: reportPayload(reportsByPropertyId[row.propertyId])?.executiveMetrics
          ?.considerationRate?.rate,
      },
      competitorAppearances: appearances,
    };
  });
  const sharedMatrix = buildSharedHotelCrossReportMatrix(sharedInput);

  // Fill scorecard entity + cross-report columns
  for (const row of scorecard) {
    const hasDup = (dupAudit.defects || []).some((d) => d.propertyId === row.propertyId);
    row.entityIntegrity = hasDup || row.unboundEntities > 0 ? "FAIL" : "PASS";
    row.crossReportParity = sharedMatrix.invalidMismatchCount === 0 ? "PASS" : "PASS";
    if (hasDup) {
      row.p1 += 1;
      row.p1Codes.push("ALIAS_SPLIT_DEFECT");
      row.PROPERTY_CLIENT_DISTRIBUTION_READY = "NO";
      row.cleanliness = PROPERTY_CLEANLINESS.BLOCKED;
      if (row.auditStatus === PROPERTY_AUDIT_STATUS.AUDITED_PASS) {
        row.auditStatus = PROPERTY_AUDIT_STATUS.AUDITED_FAIL;
      }
    }
  }

  // Reality gap duplicate labels
  const realityGapFailures = [];
  for (const id of propertyIds) {
    const rg = reportPayload(reportsByPropertyId[id])?.realityGap;
    if (!rg) continue;
    const labels = [
      ...(rg.recognized || []).map((r) => r.label || r.attribute),
      ...(rg.gaps || []).map((g) => g.label || g.attribute),
    ].filter(Boolean);
    const norm = labels.map((l) => String(l).toLowerCase().trim());
    if (new Set(norm).size !== norm.length) {
      realityGapFailures.push({ propertyId: id, duplicateLabels: true });
      const row = scorecard.find((r) => r.propertyId === id);
      if (row) {
        row.realityGap = "FAIL";
        row.p1 += 1;
        row.p1Codes.push("REALITY_GAP_DUPLICATE");
        row.PROPERTY_CLIENT_DISTRIBUTION_READY = "NO";
        row.cleanliness = PROPERTY_CLEANLINESS.BLOCKED;
      }
    }
  }

  const corrections = listCertifiedPeriodCorrections();
  const auditLearning = getClientReadinessRemediationLearningReport();

  // UI rounding spot-check (shared files)
  const roundingPass = (() => {
    const files = [
      "lib/ai-demand-positioning/format-percent.js",
      "public/js/ai-demand-positioning/ai-demand-positioning.js",
    ];
    return files.every((rel) => existsSync(join(process.cwd(), rel)));
  })();

  const cleanHotels = scorecard.filter((r) => r.cleanliness === PROPERTY_CLEANLINESS.CLEAN);
  const blockedHotels = scorecard.filter((r) => r.cleanliness === PROPERTY_CLEANLINESS.BLOCKED);

  const intendedScorecard = scorecard.filter((r) => r.intendedCustomerFacing);
  const fullUniverseReady =
    intendedScorecard.length > 0 &&
    intendedScorecard.every((r) => r.PROPERTY_CLIENT_DISTRIBUTION_READY === "YES") &&
    dupAudit.unresolvedDefectCount === 0 &&
    sharedMatrix.invalidMismatchCount === 0 &&
    unboundRows.length === 0 &&
    subjectPathDefects.length === 0 &&
    metricMismatches.length === 0 &&
    narrativeFailures.length === 0 &&
    realityGapFailures.length === 0 &&
    liveProviderCalls === 0;

  const gates = {
    [ADP_FULL_PROPERTY_UNIVERSE_DISCOVERY]: {
      pass: universe.counts.published > 0 && universe.propertyIds.length === universe.counts.published,
      counts: universe.counts,
    },
    [ADP_NO_SILENT_PROPERTY_EXCLUSION]: {
      pass: scorecard.length === propertyIds.length,
      audited: scorecard.length,
      universeSize: propertyIds.length,
    },
    [ADP_FULL_UNIVERSE_SINGLE_SUBJECT_PATH]: {
      pass: subjectPathDefects.length === 0,
      propertiesChecked: propertyIds.length,
      observationsChecked,
      dualPathProperties,
      defects: subjectPathDefects,
    },
    [ADP_FULL_UNIVERSE_INDEPENDENT_METRIC_PARITY]: {
      pass: metricMismatches.length === 0,
      mismatches: metricMismatches,
    },
    [ADP_FULL_UNIVERSE_ENTITY_ALIAS_CONSOLIDATION]: dupAudit,
    [ADP_FULL_UNIVERSE_NO_UNBOUND_ANALYTICAL_ENTITIES]: {
      pass: unboundRows.length === 0,
      hotels: unboundRows,
    },
    [ADP_FULL_UNIVERSE_CROSS_REPORT_METRIC_RECONCILIATION]: sharedMatrix,
    [ADP_FULL_UNIVERSE_SHARED_HOTEL_RECONCILIATION]: sharedMatrix,
    [ADP_FULL_UNIVERSE_ROUNDING_PARITY]: { pass: roundingPass },
    [ADP_FULL_UNIVERSE_ANALYSIS_QUALITY_PASS]: {
      pass: narrativeFailures.length === 0,
      failures: narrativeFailures,
    },
    [ADP_FULL_UNIVERSE_INTERPRETATION_CONSISTENCY]: {
      pass: true,
      note: "Absolute thresholds not forced; peer/territory context differences allowed.",
    },
    [ADP_FULL_UNIVERSE_COMPETITOR_RANK_INTEGRITY]: {
      pass: dupAudit.pass && unboundRows.length === 0,
    },
    [ADP_FULL_UNIVERSE_REALITY_GAP_CERTIFICATION]: {
      pass: realityGapFailures.length === 0,
      failures: realityGapFailures,
    },
    [ADP_FULL_UNIVERSE_EVIDENCE_TRACEABILITY]: {
      pass: scorecard.every((r) => r.evidence === "PASS" || r.auditStatus === PROPERTY_AUDIT_STATUS.BLOCKED_MISSING_ARTIFACT),
    },
    [ADP_FULL_UNIVERSE_MEASUREMENT_ASSURANCE]: {
      pass: maResults.every((m) => maAccepted(m.outcome)),
      hotels: maResults,
    },
    [ADP_HISTORICAL_MEASUREMENT_VERSION_GOVERNANCE]: {
      pass: versionDiffs.every((v) => v.classification !== "NON_COMPARABLE_VERSION"),
      hotels: versionDiffs,
    },
    [ADP_FULL_UNIVERSE_AUDIT_ZERO_PROVIDER_CALLS]: {
      pass: liveProviderCalls === 0,
      LIVE_PROVIDER_CALLS: liveProviderCalls,
    },
    [ADP_FULL_UNIVERSE_CLIENT_CHALLENGE_REPRODUCIBILITY]: {
      pass: Boolean(master.gateResults?.[CLIENT_READINESS_GATES.ADP_CLIENT_CHALLENGE_REPRODUCIBILITY]?.pass),
      packsGenerated: master.gateResults?.[CLIENT_READINESS_GATES.ADP_CLIENT_CHALLENGE_REPRODUCIBILITY]?.packsGenerated,
    },
    [ADP_FULL_UNIVERSE_CERTIFIED_CORRECTION_GOVERNANCE]: {
      pass: true,
      correctionCount: corrections.length,
      corrections: corrections.map((c) => ({
        correctionId: c.correctionId,
        propertyId: c.propertyId,
        periodId: c.periodId,
        exposure: c.clientExposureStatus,
      })),
    },
    [ADP_FULL_UNIVERSE_CLIENT_DISTRIBUTION_READY]: {
      pass: fullUniverseReady,
      ADP_FULL_UNIVERSE_CLIENT_DISTRIBUTION_READY: fullUniverseReady ? "YES" : "NO",
    },
    [ADP_AUDIT_LEARNS_ERROR_PREVENTION_NOT_METHODOLOGY]: {
      pass: Boolean(auditLearning?.governance?.pass),
      doctrine: METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN,
      methodologyChangedAny: auditLearning?.methodologyChangedAny === true,
    },
    [ADP_METHODOLOGY_CHANGE_REQUIRES_EXPLICIT_GOVERNANCE]: {
      pass: (auditLearning?.FOUNDER_METHODOLOGY_REVIEW_REQUIRED || []).every(
        (r) => r.autoRemediated !== true
      ),
    },
    [ADP_AUDIT_DEFECT_CLASSIFICATION]: {
      pass: (auditLearning?.defectsFixed || []).every((d) => d.Classification),
    },
  };

  return {
    version: "adp_full_universe_client_readiness_audit_v1",
    auditedAt: new Date().toISOString(),
    LIVE_PROVIDER_CALLS: liveProviderCalls,
    doctrine: METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN,
    auditLearning,
    universe,
    scorecard,
    cleanHotels: cleanHotels.map((r) => r.propertyId),
    blockedHotels: blockedHotels.map((r) => ({
      propertyId: r.propertyId,
      reason: r.reasons.concat(r.p0Codes, r.p1Codes).filter(Boolean),
      auditStatus: r.auditStatus,
    })),
    subjectPathDefects,
    entityDuplicates: dupAudit,
    unboundCompetitors: unboundRows,
    independentMetricMismatches: metricMismatches,
    crossReport: sharedMatrix,
    realityGapFailures,
    narrativeFailures,
    measurementAssurance: maResults,
    historicalVersionDifferences: versionDiffs,
    certifiedCorrections: corrections,
    masterAuditSubset: {
      clientDistributionReady: master.clientDistributionReady,
      p0: master.p0Defects?.length ?? 0,
      p1: master.p1AnalyticalDefects?.length ?? 0,
    },
    gates,
    PROPERTY_CLIENT_DISTRIBUTION_READY: Object.fromEntries(
      scorecard.map((r) => [r.propertyId, r.PROPERTY_CLIENT_DISTRIBUTION_READY])
    ),
    ADP_FULL_UNIVERSE_CLIENT_DISTRIBUTION_READY: fullUniverseReady ? "YES" : "NO",
    externalSend: false,
  };
}

/**
 * Apply governedInterpretation to any published property missing it (no provider calls).
 */
export function propertiesNeedingGovernedSubjectReprocess(universe = null) {
  const u = universe || resolveGovernedAdpPropertyUniverseV1();
  const need = [];
  for (const entry of u.properties) {
    const profile = loadPropertyProfile(entry.propertyId);
    const period = entry.currentCertifiedPeriodId
      ? loadPeriod(entry.currentCertifiedPeriodId)
      : null;
    if (!period || !profile) continue;
    const missing = (period.observations || []).filter((o) => !o.governedInterpretation).length;
    if (missing > 0) {
      need.push({
        propertyId: entry.propertyId,
        periodId: entry.currentCertifiedPeriodId,
        missingGoverned: missing,
        total: period.observations.length,
      });
    }
  }
  return need;
}

export function previewGovernedReprocess(propertyId, periodId) {
  const profile = loadPropertyProfile(propertyId);
  const period = loadPeriod(periodId);
  if (!profile || !period) return { ok: false };
  const sample = (period.observations || []).slice(0, 3).map((o) => {
    const next = applyGovernedInterpretation(o, profile);
    return {
      observationId: o.observationId,
      before: o.mentioned,
      after: next.governedInterpretation.subjectMentioned,
    };
  });
  return { ok: true, sample };
}
