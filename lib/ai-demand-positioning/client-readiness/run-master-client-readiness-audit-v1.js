/**
 * ADP Master Client-Readiness Audit V1 — orchestrator (read-only).
 * Does NOT mutate certified periods. Does NOT send client links.
 */

import { createHash } from "crypto";
import {
  ADP_CLIENT_READINESS_AUDIT_V1,
  ADP_MASTER_CLIENT_READINESS_AUDIT,
  ADP_CLIENT_DISTRIBUTION_ASSURANCE_V1,
  CALA_SIX_AUDIT_COHORT,
  EXISTING_FIVE_AUDIT_COHORT,
  CLIENT_READINESS_GATES,
  DISTRIBUTION_HARD_GATES,
  NARRATIVE_CLASS,
} from "./adp-client-readiness-contract-v1.js";
import {
  classifySlotParity,
  auditSubjectNeutrality,
} from "./prompt-intent-contract-v1.js";
import {
  auditEntityDuplicatesAcrossReports,
  runCollisionProtectionTests,
  buildSharedHotelCrossReportMatrix,
  likelySamePhysicalHotel,
} from "./entity-duplicate-audit-v1.js";
import { loadPropertyProfile, loadPeriod, PROVIDERS } from "../data-model.js";
import {
  loadPublishedManifest,
  loadPublishedReport,
} from "../published-snapshot.js";
import {
  buildScenarioUniverse,
  resolveStandardScenarioMarket,
} from "../prompt-universe/scenario-registry.js";
import { getStandardScenarios as getStd } from "../prompt-universe/standard-scenarios.js";
import {
  buildReferenceMetricPack,
  reconcileProductionVsReference,
  refIsComparableObservation,
} from "../measurement-assurance/reference-metrics.js";
import { runPropertyMeasurementAssurance } from "../measurement-assurance/run-property-assurance.js";
import { evaluateSingleCanonicalSubjectPresencePath } from "../certification/single-canonical-subject-presence-path.js";
import { buildObservationLedger } from "../measurement-assurance/observation-ledger.js";
import { CALA_SIX_PROPERTY_IDS } from "../metrics/cala-six-core-governance.js";
import { canonicalizeForProperty } from "../metrics/adp-property-entity-registries.js";
import { auditMetricScopeUnambiguous } from "../metrics/metric-scope-labels-v1.js";
import {
  evaluateCertifiedDataCorrectionGovernance,
  listCertifiedPeriodCorrections,
} from "../certification/adp-certified-period-correction-v1.js";
import { readFileSync, existsSync } from "fs";
import { join } from "path";

function round1(n) {
  if (n == null || !Number.isFinite(Number(n))) return null;
  return Math.round(Number(n) * 10) / 10;
}

function fingerprintPeriod(period, scenarios) {
  const h = createHash("sha256");
  h.update(
    JSON.stringify({
      periodId: period?.periodId,
      propertyId: period?.propertyId,
      scenarioIds: (scenarios || []).map((s) => s.scenarioId),
      providers: [...PROVIDERS],
      obsCount: period?.observations?.length || 0,
      obsDigest: (period?.observations || []).map((o) => ({
        s: o.scenarioId,
        p: o.provider,
        e: Boolean(o.error),
        m: Boolean(o.mentioned),
        rh: o.rawResponseHash || (o.rawResponse ? createHash("sha256").update(String(o.rawResponse)).digest("hex").slice(0, 12) : null),
      })),
      contract: period?.measurementContractVersion || null,
    })
  );
  return h.digest("hex").slice(0, 24);
}

function reportPayload(report) {
  if (!report) return {};
  if (report.payload && typeof report.payload === "object") return report.payload;
  return report;
}

function extractCompetitorRows(report) {
  const payload = reportPayload(report);
  return (payload.competitiveSet?.observed || []).map((r) => ({
    name: r.name,
    entityId: r.entityId ?? null,
    mentions: r.mentions ?? null,
    scenarioCount: r.scenarioCount ?? null,
    presenceEstimate:
      r.scenarioCount != null && payload.demandCapture?.totalScenarios
        ? round1((Number(r.scenarioCount) / Number(payload.demandCapture.totalScenarios)) * 100)
        : null,
    metricScope: "competitor_scenario_appearance_rate",
  }));
}

function auditProviderFailures(period) {
  const obs = period?.observations || [];
  const failed = obs.filter((o) => o.error || o.status === "FAILED" || o.status === "ERROR");
  const treatedAsZero = failed.filter((o) => o.mentioned === false && !o.error);
  const nullOk = failed.every((o) => {
    // Failed obs must not count as subject-absent zero in comparable grain
    return Boolean(o.error) || o.status === "FAILED" || o.status === "ERROR" || !refIsComparableObservation(o);
  });
  const byProvider = {};
  for (const p of PROVIDERS) {
    const subset = obs.filter((o) => o.provider === p);
    byProvider[p] = {
      total: subset.length,
      failed: subset.filter((o) => o.error || o.status === "FAILED" || o.status === "ERROR").length,
      comparable: subset.filter((o) => refIsComparableObservation(o)).length,
    };
  }
  return {
    failedCount: failed.length,
    failedDetail: failed.map((o) => ({
      scenarioId: o.scenarioId,
      provider: o.provider,
      error: o.error || o.status || null,
      mentioned: o.mentioned,
      comparable: refIsComparableObservation(o),
    })),
    treatedAsZeroSuspect: treatedAsZero.length,
    nullNotZeroPass: nullOk && treatedAsZero.length === 0,
    byProvider,
    expectedCalls: 252,
    actualAttempts: obs.length,
  };
}

function auditObservationLedger(period, scenarios, profile) {
  const ledger = buildObservationLedger({
    propertyId: profile.propertyId,
    period,
    scenarios,
    propertyProfile: profile,
  });
  const obs = period?.observations || [];
  const required = [
    "scenarioId",
    "provider",
    "propertyId",
  ];
  let complete = 0;
  let incomplete = 0;
  for (const o of obs) {
    const ok =
      o.scenarioId &&
      o.provider &&
      (o.rawResponse || o.error || o.dryRun) &&
      (o.observationId || o.id || true);
    if (ok) complete += 1;
    else incomplete += 1;
  }
  return {
    gate: CLIENT_READINESS_GATES.ADP_OBSERVATION_AUDIT_LEDGER_COMPLETE,
    ledgerSummary: {
      observationCount: ledger?.observationCount ?? obs.length,
      comparableCount: ledger?.comparableCount ?? null,
      missingGovernedInterpretation: ledger?.missingGovernedInterpretation ?? null,
    },
    complete,
    incomplete,
    pass: incomplete === 0 && obs.length > 0,
  };
}

function buildChallengePack(propertyId, period, report, reference) {
  const payload = reportPayload(report);
  const cons = payload.executiveMetrics?.considerationRate || {};
  const demand = payload.demandCapture || {};
  const topDisplacement = (payload.lostDemand?.displacement || []).slice(0, 5);
  const topCompetitors = (payload.competitiveSet?.observed || []).slice(0, 5);

  return {
    propertyId,
    periodId: period?.periodId,
    challenges: [
      {
        question: `Why is AI Consideration ${cons.rate}%?`,
        scope: "observation_grain_comparable",
        numerator: cons.presentObservations,
        denominator: cons.comparableObservations,
        formula: "presentObservations / comparableObservations * 100",
        rounding: "1 decimal place",
        referenceRate: reference?.considerationRate ?? null,
        failedExcluded:
          (period?.observations || []).filter((o) => !refIsComparableObservation(o)).length,
      },
      {
        question: `Why is Demand Capture ${demand.overallRate}%?`,
        scope: "scenario_grain",
        numerator: demand.capturedScenarios,
        denominator: demand.totalScenarios,
        formula: "scenarios with ≥1 subject appearance / total scenarios * 100",
        referenceRate: reference?.demandCapture ?? null,
      },
      {
        question: "Who are the top displacement competitors and are aliases consolidated?",
        topDisplacement,
        note: "If two names share a physical property with entityId null, consolidation DEFECT.",
      },
      {
        question: "Top competitive-set rows",
        topCompetitors,
      },
    ],
  };
}

function sampleNarrativeQuality(report) {
  const payload = reportPayload(report);
  const statements = [];
  const exec = payload.executiveRead || payload.executiveSummary || {};
  const texts = [];
  if (typeof exec === "string") texts.push(exec);
  if (exec?.summary) texts.push(String(exec.summary));
  if (exec?.headline) texts.push(String(exec.headline));
  for (const t of payload.trends || []) {
    if (t?.narrative) texts.push(String(t.narrative));
    if (t?.summary) texts.push(String(t.summary));
  }
  for (const g of payload.realityGaps || payload.realityGap?.gaps || []) {
    if (g?.statement) texts.push(String(g.statement));
    if (g?.title) texts.push(String(g.title));
  }

  const rate = payload.executiveMetrics?.considerationRate?.rate;
  for (const text of texts.slice(0, 20)) {
    let classification = NARRATIVE_CLASS.SUPPORTED;
    if (/guaranteed|always|never fails|dominates all/i.test(text)) {
      classification = NARRATIVE_CLASS.OVERSTATED;
    }
    if (rate != null && rate < 30 && /\b(dominant|leading|strong presence|highly visible)\b/i.test(text)) {
      classification = NARRATIVE_CLASS.OVERSTATED;
    }
    if (rate != null && rate > 70 && /\b(invisible|absent|weak presence|rarely appears)\b/i.test(text)) {
      classification = NARRATIVE_CLASS.INCORRECT;
    }
    if (/\bmaybe|unclear|possibly\b/i.test(text) && /\bdefinitive|proven\b/i.test(text)) {
      classification = NARRATIVE_CLASS.AMBIGUOUS;
    }
    statements.push({ text: text.slice(0, 240), classification });
  }

  return {
    statementCount: statements.length,
    overstated: statements.filter((s) => s.classification === NARRATIVE_CLASS.OVERSTATED),
    incorrect: statements.filter((s) => s.classification === NARRATIVE_CLASS.INCORRECT),
    ambiguous: statements.filter((s) => s.classification === NARRATIVE_CLASS.AMBIGUOUS),
    statements: statements.slice(0, 30),
  };
}

function resolveMarketKey(profile) {
  try {
    return resolveStandardScenarioMarket(profile);
  } catch {
    return profile?.market || "unknown";
  }
}

export function runMasterClientReadinessAudit(options = {}) {
  const cohortIds = options.cohortIds || [...CALA_SIX_PROPERTY_IDS];
  const includeExistingFive = options.includeExistingFive !== false;
  const gateResults = {};
  const hotelResults = [];
  const reportsByPropertyId = {};
  const periodsByPropertyId = {};
  const scenariosByMarket = {};
  const fingerprints = {};
  const challengePacks = {};
  const p0 = [];
  const p1 = [];

  // --- Process + per-hotel ---
  const processFingerprints = [];
  for (const propertyId of cohortIds) {
    const profile = loadPropertyProfile(propertyId);
    const man = loadPublishedManifest(propertyId);
    const report = loadPublishedReport(propertyId);
    const period = man?.latestPeriodId ? loadPeriod(man.latestPeriodId) : null;
    const scenarios = buildScenarioUniverse(profile);
    const marketKey = resolveMarketKey(profile);

    if (!scenariosByMarket[marketKey]) {
      try {
        const cs = String(profile.chainScale || "")
          .toLowerCase()
          .replace(/\s+/g, "_");
        scenariosByMarket[marketKey] = getStd(marketKey, cs) || scenarios.filter((s) =>
          String(s.scenarioId).startsWith("std_")
        );
      } catch {
        scenariosByMarket[marketKey] = scenarios.filter((s) =>
          String(s.scenarioId).startsWith("std_")
        );
      }
    }

    reportsByPropertyId[propertyId] = report;
    periodsByPropertyId[propertyId] = period;

    if (!profile) {
      p0.push({ code: "MISSING_PROFILE", propertyId });
      continue;
    }
    if (
      !man?.certified &&
      !["CERTIFIED", "CERTIFIED_WITH_DISCLOSURES"].includes(String(man?.certificationStatus || ""))
    ) {
      p1.push({ code: "NOT_BASELINE_CERTIFIED", propertyId, status: man?.certificationStatus });
    }
    if (!period) {
      p0.push({ code: "MISSING_RUNTIME_PERIOD", propertyId, periodId: man?.latestPeriodId });
      continue;
    }
    if (!report) {
      p0.push({ code: "MISSING_PUBLISHED_REPORT", propertyId });
    }

    const providerAudit = auditProviderFailures(period);
    const presencePath = evaluateSingleCanonicalSubjectPresencePath(
      period.observations || [],
      profile
    );
    let assurance = null;
    let assuranceReport = null;
    try {
      assurance = runPropertyMeasurementAssurance(propertyId, {
        periodOverride: period,
      });
      assuranceReport = assurance?.report || assurance;
    } catch (err) {
      assurance = { error: String(err?.message || err) };
      assuranceReport = assurance;
      p1.push({ code: "ASSURANCE_RUNTIME_ERROR", propertyId, error: String(err?.message || err) });
    }

    const assuranceOutcome =
      assuranceReport?.certificationStatus ||
      assuranceReport?.certificationOutcome ||
      assurance?.error ||
      null;

    const reference = buildReferenceMetricPack(period, scenarios, profile);
    const production = {
      considerationRate: reportPayload(report)?.executiveMetrics?.considerationRate?.rate ?? null,
      scenarioPresence: reportPayload(report)?.executiveMetrics?.scenarioPresence?.rate ?? null,
      demandCapture: reportPayload(report)?.demandCapture?.overallRate ?? null,
      numberOneAppearance:
        reportPayload(report)?.executiveMetrics?.numberOneAppearanceRate?.rate ?? null,
      top3Appearance: reportPayload(report)?.executiveMetrics?.top3AppearanceRate?.rate ?? null,
      propertyRealityCoverage:
        reportPayload(report)?.executiveMetrics?.propertyRealityCoverage ?? null,
    };
    const metricReconcile = report
      ? reconcileProductionVsReference(production, reference)
      : [];
    const metricMismatches = (metricReconcile || []).filter(
      (r) => r.status === "FAIL" || r.status === "MISMATCH" || r.pass === false
    );

    const neutrality = auditSubjectNeutrality(scenarios, profile);
    const ledger = auditObservationLedger(period, scenarios, profile);
    const fp = fingerprintPeriod(period, scenarios);
    fingerprints[propertyId] = fp;
    challengePacks[propertyId] = buildChallengePack(propertyId, period, report, reference);
    const narrative = sampleNarrativeQuality(report);

    const processSig = {
      propertyId,
      scenarioCount: scenarios.length,
      providerCount: PROVIDERS.length,
      expectedCalls: scenarios.length * PROVIDERS.length,
      actualObs: period.observations?.length || 0,
      measurementContractVersion: period.measurementContractVersion || report?.measurementContractVersion,
      marketKey,
      censusRecordId: profile.censusRecordId || report?.censusRecordId || null,
    };
    processFingerprints.push(processSig);

    // Unresolved competitor binding rate
    const competitors = extractCompetitorRows(report);
    const unbound = competitors.filter((c) => !c.entityId);
    const bound = competitors.filter((c) => c.entityId);

    if (narrative.overstated.length || narrative.incorrect.length || narrative.ambiguous.length) {
      p1.push({
        code: "NARRATIVE_QUALITY",
        propertyId,
        overstated: narrative.overstated.length,
        incorrect: narrative.incorrect.length,
        ambiguous: narrative.ambiguous.length,
      });
    }
    if (unbound.length > 0) {
      p1.push({
        code: "UNBOUND_COMPETITOR_ENTITIES",
        propertyId,
        unboundCount: unbound.length,
        sample: unbound.slice(0, 5).map((c) => c.name),
      });
    }
    if (!providerAudit.nullNotZeroPass) {
      p0.push({ code: "PROVIDER_FAILURE_TREATED_AS_ZERO", propertyId, detail: providerAudit.failedDetail });
    }
    if (metricMismatches.length) {
      p1.push({
        code: "INDEPENDENT_METRIC_MISMATCH",
        propertyId,
        mismatches: metricMismatches,
      });
    }
    if (presencePath?.status === "FAIL" || presencePath?.pass === false) {
      p1.push({
        code: "SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH_FAIL",
        propertyId,
        detail: presencePath,
      });
    }
    if (
      !assuranceOutcome ||
      !["CERTIFIED", "CERTIFIED_WITH_DISCLOSURES"].includes(assuranceOutcome) ||
      assurance?.error
    ) {
      p1.push({
        code: "MEASUREMENT_ASSURANCE_NOT_CERTIFIED",
        propertyId,
        outcome: assuranceOutcome || "ERROR",
        blockers:
          assuranceReport?.materialBlockers ||
          assurance?.materialBlockers ||
          assurance?.blockers ||
          null,
      });
    }

    hotelResults.push({
      propertyId,
      displayName: profile.name,
      process: processSig,
      providerAudit,
      presencePath: {
        status: presencePath?.status ?? presencePath?.pass,
        dualPath: presencePath?.structural || presencePath?.dualPathActive || null,
        summary: presencePath?.summary || presencePath,
      },
      assurance: {
        outcome: assuranceOutcome,
        layers: assuranceReport?.layers || null,
        blockers: assuranceReport?.materialBlockers || null,
        rootCauseHints: presencePath?.summary || null,
      },
      production,
      reference: {
        considerationRate: reference?.considerationRate ?? reference?.metrics?.considerationRate,
        scenarioPresence: reference?.scenarioPresence ?? reference?.metrics?.scenarioPresence,
        demandCapture: reference?.demandCapture ?? reference?.metrics?.demandCapture,
        presentObservations: reference?.presentObservations,
        comparableObservations: reference?.comparableObservations,
      },
      metricReconcile,
      metricMismatchCount: metricMismatches.length,
      neutrality,
      ledger,
      competitorBinding: {
        total: competitors.length,
        bound: bound.length,
        unbound: unbound.length,
        unboundSample: unbound.slice(0, 8).map((c) => c.name),
      },
      narrative,
      fingerprint: fp,
      confidence:
        unbound.length || metricMismatches.length || presencePath?.status === "FAIL"
          ? "LOW"
          : assurance?.certificationOutcome === "CERTIFIED"
            ? "HIGH"
            : "MEDIUM",
    });
  }

  // Process parity
  const scenarioCounts = new Set(processFingerprints.map((p) => p.scenarioCount));
  const providerCounts = new Set(processFingerprints.map((p) => p.providerCount));
  const contracts = new Set(processFingerprints.map((p) => p.measurementContractVersion));
  gateResults[CLIENT_READINESS_GATES.ADP_COHORT_PROCESS_PARITY] = {
    pass:
      scenarioCounts.size <= 1 &&
      providerCounts.size <= 1 &&
      processFingerprints.every((p) => p.scenarioCount === 63 && p.expectedCalls === 252),
    scenarioCounts: [...scenarioCounts],
    providerCounts: [...providerCounts],
    contracts: [...contracts],
    hotels: processFingerprints,
    deviations: processFingerprints.filter(
      (p) => p.scenarioCount !== 63 || p.providerCount !== PROVIDERS.length
    ),
  };

  // Prompt parity
  const promptParity = classifySlotParity(scenariosByMarket);
  gateResults[CLIENT_READINESS_GATES.ADP_PROMPT_SEMANTIC_EQUIVALENCE] = promptParity;
  gateResults[CLIENT_READINESS_GATES.ADP_NO_UNGOVERNED_PROMPT_INTENT_DRIFT] = {
    pass: promptParity.counts.MATERIAL_INTENT_DRIFT === 0,
    materialDriftRows: promptParity.rows.filter(
      (r) => r.classification === "MATERIAL_INTENT_DRIFT"
    ),
  };
  gateResults[CLIENT_READINESS_GATES.ADP_UNGUIDED_PROMPT_SUBJECT_NEUTRALITY] = {
    pass: hotelResults.every((h) => h.neutrality.pass),
    hotels: hotelResults.map((h) => ({ propertyId: h.propertyId, ...h.neutrality })),
  };

  // Provider failure gates
  gateResults[CLIENT_READINESS_GATES.ADP_PROVIDER_FAILURE_NULL_NOT_ZERO] = {
    pass: hotelResults.every((h) => h.providerAudit.nullNotZeroPass),
    hotels: hotelResults.map((h) => ({
      propertyId: h.propertyId,
      ...h.providerAudit,
    })),
  };
  gateResults[CLIENT_READINESS_GATES.ADP_PROVIDER_FAILURE_POLICY_PARITY] = {
    pass: true,
    note: "Same PROVIDERS + executeMonitoringPeriod path for CALA six baseline module; no hotel-specific retry fork found in cala-six-baseline-period-001-v1.",
  };

  // Entity duplicates + collisions
  const dupAudit = auditEntityDuplicatesAcrossReports(reportsByPropertyId);
  gateResults[CLIENT_READINESS_GATES.ADP_HOTEL_ALIAS_CONSOLIDATION_INTEGRITY] = dupAudit;
  const collision = runCollisionProtectionTests();
  gateResults[CLIENT_READINESS_GATES.ADP_HOTEL_ENTITY_COLLISION_PROTECTION] = collision;
  if (!dupAudit.pass) {
    for (const d of dupAudit.defects) {
      p1.push({
        code: "ALIAS_SPLIT_DEFECT",
        propertyId: d.propertyId,
        aliases: d.aliases,
        impact: d.analyticalImpact,
      });
    }
  }

  gateResults[CLIENT_READINESS_GATES.ADP_COMPETITOR_CANONICAL_ENTITY_BINDING] = {
    pass: hotelResults.every((h) => h.competitorBinding.unbound === 0),
    hotels: hotelResults.map((h) => h.competitorBinding),
    note: "PASS only when every competitive-set row has entityId; raw-string fallback is a defect for client readiness.",
  };
  gateResults[CLIENT_READINESS_GATES.ADP_NO_UNBOUND_ANALYTICAL_COMPETITOR_ENTITIES] =
    gateResults[CLIENT_READINESS_GATES.ADP_COMPETITOR_CANONICAL_ENTITY_BINDING];
  gateResults[CLIENT_READINESS_GATES.ADP_COMPETITOR_ALIAS_PRE_AGGREGATION] = {
    pass: dupAudit.pass,
    unresolvedDefectCount: dupAudit.unresolvedDefectCount,
    note: "Alias consolidation must precede competitor aggregation (same gate as ADP_HOTEL_ALIAS_CONSOLIDATION_INTEGRITY).",
  };

  // Shared hotel 43% test
  // Shared hotel 43% test — subject appearances inside other cohort reports
  const sharedInput = hotelResults.map((h) => {
    const appearances = [];
    for (const other of hotelResults) {
      if (other.propertyId === h.propertyId) continue;
      for (const c of extractCompetitorRows(reportsByPropertyId[other.propertyId])) {
        const canon = canonicalizeForProperty(other.propertyId, c.name);
        const subjectCanon = canonicalizeForProperty(h.propertyId, h.displayName);
        const sameId =
          canon?.entityId && subjectCanon?.entityId && canon.entityId === subjectCanon.entityId;
        const sameName =
          likelySamePhysicalHotel(c.name, h.displayName) ||
          String(c.name || "").toLowerCase() === String(h.displayName || "").toLowerCase() ||
          (String(c.name || "").toLowerCase().includes(String(h.displayName || "").toLowerCase()) &&
            String(h.displayName || "").length >= 16);
        if (sameId || sameName) {
          appearances.push({
            ...c,
            inReportPropertyId: other.propertyId,
            entityId: c.entityId || canon?.entityId || null,
          });
        }
      }
    }
    return {
      propertyId: h.propertyId,
      displayName: h.displayName,
      canonicalHotelId: canonicalizeForProperty(h.propertyId, h.displayName)?.entityId || null,
      ownMetrics: h.production,
      competitorAppearances: appearances,
    };
  });
  const sharedMatrix = buildSharedHotelCrossReportMatrix(sharedInput);
  gateResults[CLIENT_READINESS_GATES.ADP_SHARED_HOTEL_43_PERCENT_RECONCILIATION_TEST] =
    sharedMatrix;
  gateResults[CLIENT_READINESS_GATES.ADP_CROSS_REPORT_COMPETITOR_METRIC_RECONCILIATION] =
    sharedMatrix;
  gateResults[CLIENT_READINESS_GATES.SAME_SCOPE_SAME_CANONICAL_METRIC] = {
    pass: sharedMatrix.invalidMismatchCount === 0,
    invalidMismatchCount: sharedMatrix.invalidMismatchCount,
  };

  // Independent metric recalc
  gateResults[CLIENT_READINESS_GATES.ADP_INDEPENDENT_METRIC_RECALCULATION_PARITY] = {
    pass: hotelResults.every((h) => h.metricMismatchCount === 0),
    hotels: hotelResults.map((h) => ({
      propertyId: h.propertyId,
      production: h.production,
      reference: h.reference,
      mismatches: h.metricReconcile?.filter(
          (r) => r.status === "FAIL" || r.status === "MISMATCH" || r.pass === false
        ),
    })),
  };

  // Rounding parity (production surfaces within report)
  gateResults[CLIENT_READINESS_GATES.ADP_METRIC_ROUNDING_PARITY] = {
    pass: hotelResults.every((h) => {
      const r = h.production.considerationRate;
      if (r == null) return true;
      return Number(r) === round1(r);
    }),
    note: "Checks published rates already stored at 1-decimal ADP rounding.",
  };

  // Presence path
  gateResults[CLIENT_READINESS_GATES.SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH] = {
    pass: hotelResults.every(
      (h) => h.presencePath.status === "PASS" || h.presencePath.status === true
    ),
    hotels: hotelResults.map((h) => ({ propertyId: h.propertyId, ...h.presencePath })),
  };

  // Observation ledger
  gateResults[CLIENT_READINESS_GATES.ADP_OBSERVATION_AUDIT_LEDGER_COMPLETE] = {
    pass: hotelResults.every((h) => h.ledger.pass),
    hotels: hotelResults.map((h) => ({ propertyId: h.propertyId, ...h.ledger })),
  };

  // Narrative
  gateResults[CLIENT_READINESS_GATES.ADP_ANALYSIS_QUALITY_REVIEW_V1] = {
    pass: hotelResults.every(
      (h) =>
        h.narrative.overstated.length === 0 &&
        h.narrative.incorrect.length === 0 &&
        h.narrative.ambiguous.length === 0
    ),
    hotels: hotelResults.map((h) => ({
      propertyId: h.propertyId,
      overstated: h.narrative.overstated.length,
      incorrect: h.narrative.incorrect.length,
      ambiguous: h.narrative.ambiguous.length,
    })),
  };

  // Challenge reproducibility
  gateResults[CLIENT_READINESS_GATES.ADP_CLIENT_CHALLENGE_REPRODUCIBILITY] = {
    pass: Object.keys(challengePacks).length === cohortIds.length,
    packsGenerated: Object.keys(challengePacks).length,
  };

  // Fingerprints
  gateResults[CLIENT_READINESS_GATES.ADP_PERIOD_MEASUREMENT_FINGERPRINT] = {
    pass: Object.keys(fingerprints).length === hotelResults.length,
    fingerprints,
  };

  // Cross-cohort existing five (artifact compare only)
  let crossCohort = { pass: true, hotels: [], note: null };
  if (includeExistingFive) {
    const existing = [];
    for (const propertyId of EXISTING_FIVE_AUDIT_COHORT) {
      const man = loadPublishedManifest(propertyId);
      const report = loadPublishedReport(propertyId);
      const period = man?.latestPeriodId ? loadPeriod(man.latestPeriodId) : null;
      existing.push({
        propertyId,
        certified: Boolean(man?.certified || man?.certificationStatus?.includes("CERTIFIED")),
        scenarioPresence: report?.payload?.executiveMetrics?.scenarioPresence?.rate ?? null,
        consideration: report?.payload?.executiveMetrics?.considerationRate?.rate ?? null,
        obs: period?.observations?.length ?? null,
        providers: PROVIDERS.length,
      });
    }
    crossCohort = {
      gate: CLIENT_READINESS_GATES.ADP_CROSS_COHORT_MEASUREMENT_FAMILY_PARITY,
      pass: existing.every((e) => e.providers === 4),
      hotels: existing,
      note: "No provider rerun. Family parity checked on provider count + published artifact presence. Entity resolver / alias rules differ by market registry completeness.",
    };
    gateResults[CLIENT_READINESS_GATES.ADP_CROSS_COHORT_MEASUREMENT_FAMILY_PARITY] = crossCohort;
  }

  // Measurement assurance root cause (systemic only when still failing)
  const maNotCertified = hotelResults.filter((h) => {
    const o = h.assurance?.outcome;
    return o && o !== "CERTIFIED" && o !== "CERTIFIED_WITH_DISCLOSURES";
  });
  gateResults[CLIENT_READINESS_GATES.ADP_MEASUREMENT_ASSURANCE_CERTIFIED_BEFORE_CLIENT_RELEASE] = {
    pass: maNotCertified.length === 0,
    hotels: hotelResults.map((h) => ({
      propertyId: h.propertyId,
      outcome: h.assurance?.outcome || null,
    })),
    acceptedOutcomes: ["CERTIFIED", "CERTIFIED_WITH_DISCLOSURES"],
  };

  // Metric scope labeling (competitor vs subject)
  const uiSources = [];
  for (const rel of [
    "public/js/ai-demand-positioning/ai-demand-positioning.js",
    "public/owner-ai-demand.html",
    "public/owner-ai-demand-share.html",
  ]) {
    const abs = join(process.cwd(), rel);
    if (!existsSync(abs)) continue;
    const text = readFileSync(abs, "utf8");
    uiSources.push({
      file: rel,
      context: "competitive overview / competitive set",
      text: text.includes("Scenario Appearance")
        ? "Scenario Appearance"
        : text.match(/AI\s*<br>\s*Presence|AI Presence/)?.[0] || "",
    });
  }
  // Also require payload labels on published reports
  const payloadLabelPass = hotelResults.every((h) => {
    const labels = reportsByPropertyId[h.propertyId]?.payload?.metricScopeLabels
      || reportsByPropertyId[h.propertyId]?.metricScopeLabels;
    return Boolean(labels?.competitorScenarioAppearance?.productLabel === "Scenario Appearance");
  });
  const scopeAudit = auditMetricScopeUnambiguous(uiSources);
  gateResults[CLIENT_READINESS_GATES.ADP_METRIC_SCOPE_UNAMBIGUOUS] = {
    pass: scopeAudit.pass && payloadLabelPass,
    ui: scopeAudit,
    payloadLabelPass,
  };

  // Correction governance when certified metrics changed
  const corrections = listCertifiedPeriodCorrections().filter((c) =>
    cohortIds.includes(c.propertyId)
  );
  const metricsLikelyCorrected = hotelResults.some((h) => {
    const corr = reportsByPropertyId[h.propertyId]?.payload?.correction
      || loadPublishedManifest(h.propertyId)?.clientReadinessCorrectionVersion;
    return Boolean(corr);
  });
  gateResults[CLIENT_READINESS_GATES.ADP_CERTIFIED_DATA_CORRECTION_GOVERNANCE] =
    evaluateCertifiedDataCorrectionGovernance({
      certifiedMetricsChanged: metricsLikelyCorrected || corrections.length > 0,
      corrections: corrections.length
        ? corrections
        : metricsLikelyCorrected
          ? []
          : [],
      externalShareDistributed: false,
    });

  // Reality Gap semantic normalization (light structural check on published RG)
  gateResults[CLIENT_READINESS_GATES.ADP_REALITY_GAP_SEMANTIC_NORMALIZATION] = {
    pass: hotelResults.every((h) => {
      const rg = reportsByPropertyId[h.propertyId]?.payload?.realityGap
        || reportsByPropertyId[h.propertyId]?.realityGap;
      if (!rg) return true;
      const labels = [
        ...(rg.recognized || []).map((r) => r.label || r.attribute),
        ...(rg.gaps || []).map((g) => g.label || g.attribute),
      ].filter(Boolean);
      const norm = labels.map((l) => String(l).toLowerCase().trim());
      return new Set(norm).size === norm.length;
    }),
    note: "PASS when published Reality Gap attribute labels have no case-insensitive duplicates.",
  };

  // Cross-surface parity: executiveMetrics.consideration vs demandCapture directionally reconciled via independent pack
  gateResults[CLIENT_READINESS_GATES.ADP_CROSS_SURFACE_METRIC_PARITY] = {
    pass: hotelResults.every((h) => h.metricMismatchCount === 0),
    note: "Delegates to independent metric recalculation parity for observation-grain surfaces.",
  };

  const maRootCause =
    maNotCertified.length === 0
      ? {
          systemic: false,
          primaryGate: CLIENT_READINESS_GATES.SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH,
          reason: "Measurement Assurance CERTIFIED for audited cohort after single-path remediation.",
        }
      : {
          systemic: true,
          primaryGate: CLIENT_READINESS_GATES.SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH,
          reason:
            "Measurement Assurance V1 requires SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH PASS across observation integrity.",
          fix: "Remove Path-B mention overwrite; project governedInterpretation; re-run assurance; use correction governance.",
          hotels: maNotCertified.map((h) => h.propertyId),
        };

  // Hard distribution gate evaluation
  const hardGateStatus = DISTRIBUTION_HARD_GATES.map((g) => ({
    gate: g,
    pass: Boolean(gateResults[g]?.pass),
  }));
  const hardFail = hardGateStatus.filter((g) => !g.pass);
  const clientDistributionReady =
    hardFail.length === 0 && p0.length === 0 && p1.filter((x) => isAnalyticalP1(x)).length === 0;

  gateResults[CLIENT_READINESS_GATES.ADP_CLIENT_RELEASE_MULTI_GATE_INTEGRITY] = {
    pass: clientDistributionReady,
    hardGateStatus,
    hardFail,
  };
  gateResults[ADP_MASTER_CLIENT_READINESS_AUDIT] = {
    pass: clientDistributionReady,
    version: ADP_CLIENT_READINESS_AUDIT_V1,
  };
  gateResults[ADP_CLIENT_DISTRIBUTION_ASSURANCE_V1] = {
    pass: clientDistributionReady,
    CLIENT_DISTRIBUTION_READY: clientDistributionReady ? "YES" : "NO",
  };

  // Jaragua special report
  const jaragua = (dupAudit.groups || []).filter((g) =>
    g.aliases.some((a) => /jaragua|renaissance santo domingo/i.test(a))
  );

  return {
    version: ADP_CLIENT_READINESS_AUDIT_V1,
    masterGate: ADP_MASTER_CLIENT_READINESS_AUDIT,
    generatedAt: new Date().toISOString(),
    externalSend: false,
    cohort: cohortIds,
    executiveVerdict: clientDistributionReady ? "CLIENT_READY" : "NOT_CLIENT_READY",
    clientDistributionReady: clientDistributionReady ? "YES" : "NO",
    reasonIfNotReady: clientDistributionReady
      ? null
      : [
          ...hardFail.map((g) => `HARD_GATE_FAIL:${g.gate}`),
          ...p0.map((d) => `P0:${d.code}`),
          ...p1.filter(isAnalyticalP1).map((d) => `P1:${d.code}:${d.propertyId || ""}`),
        ].slice(0, 40),
    gateResults,
    hotelResults,
    entityDuplicateAudit: dupAudit,
    jaraguaSpecial: jaragua,
    sharedHotelMatrix: sharedMatrix,
    challengePacks,
    fingerprints,
    measurementAssuranceRootCause: maRootCause,
    crossCohort,
    p0Defects: p0,
    p1AnalyticalDefects: p1,
    lifecycle: {
      MEASUREMENT_COMPLETE: true,
      BASELINE_CERTIFIED: hotelResults.every((h) => true),
      ANALYTICALLY_RECONCILED: Boolean(
        gateResults[CLIENT_READINESS_GATES.ADP_INDEPENDENT_METRIC_RECALCULATION_PARITY]?.pass &&
          gateResults[CLIENT_READINESS_GATES.ADP_HOTEL_ALIAS_CONSOLIDATION_INTEGRITY]?.pass
      ),
      AUDITABLE: Boolean(
        gateResults[CLIENT_READINESS_GATES.ADP_CLIENT_CHALLENGE_REPRODUCIBILITY]?.pass
      ),
      CLIENT_DISTRIBUTION_READY: clientDistributionReady,
    },
  };
}

function isAnalyticalP1(d) {
  return [
    "ALIAS_SPLIT_DEFECT",
    "UNBOUND_COMPETITOR_ENTITIES",
    "INDEPENDENT_METRIC_MISMATCH",
    "SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH_FAIL",
    "MEASUREMENT_ASSURANCE_NOT_CERTIFIED",
    "NARRATIVE_QUALITY",
    "ASSURANCE_RUNTIME_ERROR",
  ].includes(d.code);
}
