/**
 * Run AI Intelligence validation over stored monitoring summaries.
 * LIVE_PROVIDER_CALLS: 0 — reads store only.
 */

import fs from "fs";
import path from "path";
import { createBrandAiVisibilityReadStore } from "../storage/index.js";
import { loadObservationsFromBatchSummary } from "../cohort-observations.js";
import { isBlockedFixtureDomain } from "../fixture-domain-guard.js";
import { METRIC_CONTRACTS, listMetricContracts } from "./metric-contracts.js";
import {
  GATE_STATUS,
  METHODOLOGY_NOTE,
  OPERATIONAL_METHODOLOGY_NOTE,
  OVERALL_VALIDATION_STATUS,
} from "./validation-status.js";
import {
  auditBuildObservationsFromEvidence,
  auditComputeEntityMetrics,
  auditMetricBounds,
  reconcileEntityMetrics,
  INDEPENDENT_RECOMPUTE_VERSION,
} from "./independent-recompute.js";
import { loadGoldenSet, scoreGoldenSet } from "./golden-set.js";
import { sampleManualAuditCases } from "./manual-audit-sample.js";
import {
  resolveValidationStorageRoot,
  ensureValidationStorageDirs,
  validationReportPath,
  DEFAULT_VALIDATION_ROOT,
} from "./validation-storage-root.js";
import { evaluateClassificationThreshold } from "./classification-threshold.js";
import { runPortfolioIntegrityGate } from "./portfolio-integrity.js";
import { buildMonitoringOperationsReport } from "./monitoring-operations.js";
import {
  getReviewProgress,
  loadGoldenSetV1Document,
  loadGoldenSetV2Document,
} from "./golden-set-human-review.js";

export const VALIDATION_RUNNER_VERSION = "ai_intelligence_validation_runner_v1";

function providerName(summary) {
  if (!summary?.provider) return null;
  if (typeof summary.provider === "string") return summary.provider.toLowerCase();
  return String(summary.provider.name || summary.provider.id || "").toLowerCase() || null;
}

function languageOf(summary) {
  return (
    summary?.language ||
    summary?.promptLanguage ||
    summary?.logical?.language ||
    null
  );
}

function geographyOf(summary) {
  if (summary?.slots && typeof summary.slots === "object") return "multi_slot";
  return (
    summary?.geographyScope ||
    summary?.cohort?.commercialRegion ||
    summary?.logical?.geography ||
    null
  );
}

function expectedLangFromSlot(slot) {
  if (!slot) return null;
  const s = String(slot).toUpperCase();
  if (s.endsWith("_ES") || s.includes("_ES_")) return "es";
  if (s.endsWith("_EN") || s.includes("_EN_")) return "en";
  return null;
}

function expectedGeoFromSlot(slot) {
  if (!slot) return null;
  const s = String(slot).toUpperCase();
  const geo = s.replace(/_(EN|ES)$/i, "");
  return geo || null;
}

/**
 * Load evidence rows for validation — supports runs lacking evidenceId (provider baselines)
 * by joining evidence via runId / responseId. Dedupes by fingerprint when present.
 */
async function loadValidationEvidenceRows(store, summary, runs) {
  const completed = (runs || []).filter((r) => r.status === "completed");
  const byRunId = new Map();
  const byResponseId = new Map();
  const provider = providerName(summary);

  if (typeof store.listEvidence === "function") {
    const listed = await store.listEvidence(provider ? { provider } : {});
    for (const ev of listed || []) {
      if (ev.runId) byRunId.set(ev.runId, ev);
      if (ev.responseId) byResponseId.set(ev.responseId, ev);
    }
  }

  /** @type {Map<string, object>} */
  const byFingerprint = new Map();
  const rows = [];

  for (const run of completed) {
    let ev = null;
    if (run.evidenceId && typeof store.getEvidence === "function") {
      ev = await store.getEvidence(run.evidenceId);
    }
    if (!ev && run.runId) ev = byRunId.get(run.runId) || null;
    if (!ev && run.responseId) ev = byResponseId.get(run.responseId) || null;

    const fp = run.fingerprint || null;
    const row = {
      evidenceId: ev?.evidenceId || run.evidenceId || null,
      promptId: ev?.promptId || run.promptId || null,
      promptText: ev?.promptText || run.promptId || null,
      intentTerritory: ev?.intentTerritory || run.intent || null,
      mentions: ev?.payload?.mentions || [],
      provider: ev?.provider || run.provider || provider,
      language: ev?.language || run.language || null,
      geographyKey: run.geographyKey || ev?.regionName || null,
      slot: run.slot || null,
      promptFamily: run.promptFamily || null,
      responseId: run.responseId || ev?.responseId || null,
      fingerprint: fp,
      runId: run.runId || null,
      evidenceResolved: !!ev,
      citations: ev?.payload?.citations || [],
      evidenceRecord: ev,
    };

    if (fp) {
      // Production regeneratedFromUniqueFingerprints uses latest completedAt per fingerprint
      const prev = byFingerprint.get(fp);
      const prevTs = String(prev?.completedAt || "");
      const nextTs = String(run.completedAt || "");
      if (
        !prev ||
        nextTs > prevTs ||
        (nextTs === prevTs && row.evidenceResolved && !prev.evidenceResolved)
      ) {
        byFingerprint.set(fp, { ...row, completedAt: run.completedAt || null });
      }
    } else {
      rows.push(row);
    }
  }

  for (const row of byFingerprint.values()) rows.push(row);
  return rows;
}

/**
 * @param {object} [options]
 */
export async function runAiIntelligenceValidation(options = {}) {
  const store = options.store || createBrandAiVisibilityReadStore({});
  const resolvedRoot = resolveValidationStorageRoot({ outDir: options.outDir });
  const outDir = resolvedRoot.rootDir;
  const writeFiles = options.writeFiles !== false;
  if (writeFiles) ensureValidationStorageDirs(outDir);

  const listFn =
    typeof store.listBatchSummaries === "function"
      ? store.listBatchSummaries.bind(store)
      : typeof store.listSummaries === "function"
        ? store.listSummaries.bind(store)
        : null;
  const summaries = listFn ? await listFn({}) : [];

  const issues = [];
  const batches = [];
  const sampleCandidates = [];
  let metricsChecked = 0;
  let metricsReconciled = 0;
  let impossible = 0;
  let brokenEvidence = 0;
  let providerLeakage = 0;
  let geographyLeakage = 0;
  let languageLeakage = 0;
  let questionFailures = 0;
  let fixtureContamination = 0;

  for (const summary of summaries) {
    if (!summary?.batchId) continue;
    const provider = providerName(summary);
    const language = languageOf(summary);
    const geography = geographyOf(summary);
    const batchIssues = [];
    const isMultiSlot = !!(summary.slots && typeof summary.slots === "object");

    const runs =
      typeof store.listBatchRuns === "function"
        ? (await store.listBatchRuns(summary.batchId)) || []
        : [];

    // Prefer validation loader (fingerprint dedupe + runId join). Fall back to cohort loader.
    let enrichedEvidence = await loadValidationEvidenceRows(store, summary, runs);
    if (!enrichedEvidence.length) {
      const loaded = await loadObservationsFromBatchSummary(store, summary, {});
      enrichedEvidence = loaded.evidenceRows || [];
    }

    const auditObs = auditBuildObservationsFromEvidence(
      enrichedEvidence.filter((r) => r.evidenceResolved !== false || (r.mentions || []).length)
    );

    // Traceability + fixture contamination
    let evidenceOk = 0;
    let evidenceFail = 0;
    for (const row of enrichedEvidence) {
      const ev = row.evidenceRecord || null;
      if (!row.evidenceResolved && !ev) {
        evidenceFail += 1;
        brokenEvidence += 1;
        batchIssues.push({
          type: "broken_evidence_id",
          batchId: summary.batchId,
          expected: row.evidenceId || row.runId || "evidence",
          actual: "missing",
          status: "open",
        });
        continue;
      }
      evidenceOk += 1;
      const citations = row.citations || ev?.payload?.citations || [];
      for (const c of citations) {
        if (isBlockedFixtureDomain(c.domain || c.url || c.href)) {
          fixtureContamination += 1;
          batchIssues.push({
            type: "fixture_contamination",
            batchId: summary.batchId,
            evidenceId: row.evidenceId,
            actual: c.domain || c.url,
            status: "open",
          });
        }
      }
    }

    // Provider / geography / language purity on completed runs
    for (const run of runs) {
      if (run.status !== "completed") continue;
      const runProvider = String(run.provider || "").toLowerCase();
      if (provider && runProvider && runProvider !== provider) {
        providerLeakage += 1;
        batchIssues.push({
          type: "provider_leakage",
          batchId: summary.batchId,
          expected: provider,
          actual: run.provider,
          status: "open",
        });
      }

      if (isMultiSlot && run.slot) {
        const expLang = expectedLangFromSlot(run.slot);
        const actLang = String(run.language || "").toLowerCase();
        if (expLang && actLang && actLang !== expLang) {
          languageLeakage += 1;
          batchIssues.push({
            type: "language_leakage",
            batchId: summary.batchId,
            expected: `${run.slot} → ${expLang}`,
            actual: run.language,
            status: "open",
          });
        }
        const expGeo = expectedGeoFromSlot(run.slot);
        const actGeo = String(run.geographyKey || "").toUpperCase().replace(/[^A-Z_]/g, "");
        if (expGeo && actGeo && actGeo !== expGeo && !actGeo.includes(expGeo) && !expGeo.includes(actGeo)) {
          // Soft match: CALA vs CALA_REGION etc.
          const norm = (g) => g.replace(/_/g, "");
          if (norm(actGeo) !== norm(expGeo) && !norm(actGeo).startsWith(norm(expGeo))) {
            geographyLeakage += 1;
            batchIssues.push({
              type: "geography_leakage",
              batchId: summary.batchId,
              expected: `${run.slot} → ${expGeo}`,
              actual: run.geographyKey,
              status: "open",
            });
          }
        }
      } else if (!isMultiSlot) {
        if (language && run.language && String(run.language).toLowerCase() !== String(language).toLowerCase()) {
          languageLeakage += 1;
          batchIssues.push({
            type: "language_leakage",
            batchId: summary.batchId,
            expected: language,
            actual: run.language,
            status: "open",
          });
        }
      }
    }

    // Reconcile byEntity
    const byEntity = summary.metrics?.byEntity || {};
    let batchMetricChecked = 0;
    let batchMetricReconciled = 0;
    let batchBoundsViolations = 0;

    for (const [key, prod] of Object.entries(byEntity)) {
      const entityId = prod?.id || key;
      if (!entityId || !String(entityId).startsWith("rec")) continue;

      if (!auditObs.length) {
        batchIssues.push({
          type: "insufficient_evidence_for_recompute",
          batchId: summary.batchId,
          entityId,
          expected: "stored evidence joinable from runs",
          actual: "0 audit observations",
          status: "open",
        });
        continue;
      }

      const audit = auditComputeEntityMetrics(auditObs, entityId);
      const recon = reconcileEntityMetrics(prod, audit);
      batchMetricChecked += recon.comparisons.length;
      batchMetricReconciled += recon.reconciledCount;
      metricsChecked += recon.comparisons.length;
      metricsReconciled += recon.reconciledCount;

      for (const c of recon.comparisons) {
        if (!c.match) {
          batchIssues.push({
            type: "metric_mismatch",
            batchId: summary.batchId,
            entityId,
            metricId: c.metricId,
            expected: c.auditValue,
            actual: c.productionValue,
            status: "open",
          });
        } else {
          sampleCandidates.push({
            batchId: summary.batchId,
            responseId: null,
            evidenceId: null,
            promptId: null,
            entityId,
            provider,
            geography,
            language,
            systemResult: { metricId: c.metricId, value: c.productionValue },
            classificationType: c.metricId,
          });
        }
      }

      if (
        prod.denominatorPresence != null &&
        prod.questionsWon != null &&
        prod.questionsMissing != null &&
        prod.questionsWon + prod.questionsMissing > prod.denominatorPresence
      ) {
        questionFailures += 1;
        batchIssues.push({
          type: "question_reconciliation",
          batchId: summary.batchId,
          entityId,
          expected: `won+missing <= ${prod.denominatorPresence}`,
          actual: `${prod.questionsWon}+${prod.questionsMissing}`,
          status: "open",
        });
      }

      if (audit.denominator > 0 && audit.questionsWon > audit.numeratorPresence) {
        questionFailures += 1;
        batchIssues.push({
          type: "question_reconciliation",
          batchId: summary.batchId,
          entityId,
          expected: "questionsWon <= presence hits",
          actual: `${audit.questionsWon} > ${audit.numeratorPresence}`,
          status: "open",
        });
      }

      // Bounds against production stored values (authoritative displayed numbers)
      const bounds = auditMetricBounds({
        ...prod,
        denominator: prod.denominatorPresence ?? prod.denominator,
        denominatorPresence: prod.denominatorPresence ?? prod.denominator,
      });
      for (const v of bounds) {
        impossible += 1;
        batchBoundsViolations += 1;
        batchIssues.push({
          type: "metric_bounds",
          batchId: summary.batchId,
          entityId,
          ...v,
          status: "open",
        });
      }
    }

    const gateTraceability =
      evidenceFail === 0 && evidenceOk > 0
        ? GATE_STATUS.PASS
        : evidenceOk === 0
          ? GATE_STATUS.NOT_RUN
          : GATE_STATUS.FAIL;

    const gateRecompute =
      batchMetricChecked === 0
        ? GATE_STATUS.NOT_RUN
        : batchMetricReconciled === batchMetricChecked
          ? GATE_STATUS.PASS
          : GATE_STATUS.FAIL;

    const gateBounds =
      batchBoundsViolations === 0 ? GATE_STATUS.PASS : GATE_STATUS.FAIL;

    const hasMaterialLeak = batchIssues.some((i) =>
      [
        "provider_leakage",
        "fixture_contamination",
        "geography_leakage",
        "language_leakage",
        "metric_mismatch",
        "metric_bounds",
        "broken_evidence_id",
        "question_reconciliation",
      ].includes(i.type)
    );

    // This phase: only PASS (zero material issues + gates) is client-publishable
    const publishable =
      gateTraceability === GATE_STATUS.PASS &&
      gateRecompute === GATE_STATUS.PASS &&
      gateBounds === GATE_STATUS.PASS &&
      !hasMaterialLeak &&
      batchIssues.length === 0;

    let overall = OVERALL_VALIDATION_STATUS.NOT_VALIDATED;
    if (batchMetricChecked === 0 && evidenceOk === 0) {
      overall = OVERALL_VALIDATION_STATUS.NOT_VALIDATED;
    } else if (batchIssues.length === 0 && publishable) {
      overall = OVERALL_VALIDATION_STATUS.PASS;
    } else if (
      batchIssues.length &&
      !batchIssues.some((i) =>
        ["metric_mismatch", "broken_evidence_id", "provider_leakage", "fixture_contamination"].includes(
          i.type
        )
      )
    ) {
      overall = OVERALL_VALIDATION_STATUS.PASS_WITH_LIMITATIONS;
    } else {
      overall = OVERALL_VALIDATION_STATUS.FAIL;
    }

    const manifest = {
      validationManifestId: `vm_${summary.batchId}`,
      batchId: summary.batchId,
      provider,
      model: summary.provider?.model || null,
      geography,
      language: language || (isMultiSlot ? "multi_slot" : null),
      promptSetId: summary.versions?.promptSetId || summary.logical?.promptSetId || null,
      promptFingerprint: summary.versions?.promptFingerprint || null,
      entityUniverseId: summary.peerSet?.peerSetId || null,
      peerSetId: summary.peerSet?.peerSetId || null,
      classifierVersion: summary.versions?.classifierVersion || null,
      metricVersion: summary.metrics?.metricVersion || null,
      executedAt: summary.completedAt || summary.savedAt || null,
      validatedAt: new Date().toISOString(),
      independentRecomputeVersion: INDEPENDENT_RECOMPUTE_VERSION,
      runnerVersion: VALIDATION_RUNNER_VERSION,
      responseTraceabilityStatus: gateTraceability,
      entityResolutionStatus: GATE_STATUS.NOT_RUN,
      classificationValidationStatus: GATE_STATUS.NOT_RUN,
      metricReconciliationStatus: gateRecompute,
      metricBoundsStatus: gateBounds,
      questionReconciliationStatus: batchIssues.some((i) => i.type === "question_reconciliation")
        ? GATE_STATUS.FAIL
        : GATE_STATUS.PASS,
      providerPurityStatus: batchIssues.some((i) => i.type === "provider_leakage")
        ? GATE_STATUS.FAIL
        : GATE_STATUS.PASS,
      geographyPurityStatus: batchIssues.some((i) => i.type === "geography_leakage")
        ? GATE_STATUS.FAIL
        : GATE_STATUS.PASS,
      languagePurityStatus: batchIssues.some((i) => i.type === "language_leakage")
        ? GATE_STATUS.FAIL
        : GATE_STATUS.PASS,
      evidenceIntegrityStatus: gateTraceability,
      fixtureContaminationStatus: batchIssues.some((i) => i.type === "fixture_contamination")
        ? GATE_STATUS.FAIL
        : GATE_STATUS.PASS,
      trendComparabilityStatus: summary.metrics?.TREND_AVAILABLE
        ? GATE_STATUS.PASS
        : GATE_STATUS.NOT_RUN,
      portfolioIntegrityStatus: GATE_STATUS.NOT_RUN,
      overallValidationStatus: overall,
      publishable,
      metricsChecked: batchMetricChecked,
      metricsReconciled: batchMetricReconciled,
      evidenceResolved: evidenceOk,
      evidenceBroken: evidenceFail,
      promptCount: runs.filter((r) => r.status === "completed").length,
      entityUniverse: Object.keys(byEntity).length,
      issueCount: batchIssues.length,
    };

    batches.push({
      ...manifest,
      issues: batchIssues,
    });
    issues.push(...batchIssues);

    if (writeFiles) {
      fs.mkdirSync(path.join(outDir, "manifests"), { recursive: true });
      fs.writeFileSync(
        path.join(outDir, "manifests", `${summary.batchId}.json`),
        JSON.stringify(manifest, null, 2),
        "utf8"
      );
    }
  }

  const golden = loadGoldenSet();
  const goldenScore = scoreGoldenSet(golden);
  const classificationEval = evaluateClassificationThreshold(goldenScore, {
    coverage: goldenScore.coverage || golden.coverage,
    subgroupMetrics: goldenScore.subgroupMetrics,
  });
  goldenScore.thresholdEvaluation = classificationEval;
  goldenScore.threshold = classificationEval.THRESHOLD_STATUS;

  const portfolioIntegrity = runPortfolioIntegrityGate();
  const monitoringOperations = await buildMonitoringOperationsReport({ store });
  let humanReviewProgress = null;
  let goldenSetVersions = { v1Size: null, v2Size: 0 };
  try {
    humanReviewProgress = getReviewProgress({});
    const v1doc = loadGoldenSetV1Document();
    const v2doc = loadGoldenSetV2Document();
    goldenSetVersions = {
      v1Size: v1doc.caseCount || (v1doc.cases || []).length,
      v2Size: v2doc ? v2doc.caseCount || (v2doc.cases || []).length : 0,
      v2Version: v2doc?.version || null,
    };
  } catch (err) {
    console.warn("[ai-intelligence-validation] human review progress unavailable:", err?.message || err);
    humanReviewProgress = {
      TOTAL: 0,
      REVIEWED: 0,
      CONFIRMED: 0,
      CORRECTED: 0,
      DEFERRED: 0,
      REMAINING: 0,
      note: "Human review progress unavailable",
    };
  }

  const reconciliationRate =
    metricsChecked > 0 ? metricsReconciled / metricsChecked : null;

  const manualSample = sampleManualAuditCases(sampleCandidates, { sampleSize: 20 });

  const gates = [
    {
      name: "TRACEABILITY",
      status:
        brokenEvidence === 0 && metricsChecked > 0
          ? GATE_STATUS.PASS
          : metricsChecked === 0
            ? GATE_STATUS.NOT_RUN
            : GATE_STATUS.FAIL,
      measuredResult: `${batches.reduce((s, b) => s + (b.evidenceResolved || 0), 0)} resolved / ${brokenEvidence} broken`,
      requirement: "100% evidence resolve",
      failures: brokenEvidence,
      lastChecked: new Date().toISOString(),
    },
    {
      name: "METRIC_RECOMPUTATION",
      status:
        metricsChecked === 0
          ? GATE_STATUS.NOT_RUN
          : metricsReconciled === metricsChecked
            ? GATE_STATUS.PASS
            : GATE_STATUS.FAIL,
      measuredResult: `${metricsReconciled}/${metricsChecked}`,
      requirement: "100% match",
      failures: metricsChecked - metricsReconciled,
      lastChecked: new Date().toISOString(),
    },
    {
      name: "METRIC_BOUNDS",
      status: impossible === 0 ? GATE_STATUS.PASS : GATE_STATUS.FAIL,
      measuredResult: `${impossible} violations`,
      requirement: "0 violations",
      failures: impossible,
      lastChecked: new Date().toISOString(),
    },
    {
      name: "PROVIDER_PURITY",
      status: providerLeakage === 0 ? GATE_STATUS.PASS : GATE_STATUS.FAIL,
      measuredResult: `${providerLeakage} leakage cases`,
      requirement: "0 leakage",
      failures: providerLeakage,
      lastChecked: new Date().toISOString(),
    },
    {
      name: "GEOGRAPHY_PURITY",
      status: geographyLeakage === 0 ? GATE_STATUS.PASS : GATE_STATUS.FAIL,
      measuredResult: `${geographyLeakage} leakage cases`,
      requirement: "0 leakage",
      failures: geographyLeakage,
      lastChecked: new Date().toISOString(),
    },
    {
      name: "LANGUAGE_PURITY",
      status: languageLeakage === 0 ? GATE_STATUS.PASS : GATE_STATUS.FAIL,
      measuredResult: `${languageLeakage} leakage cases`,
      requirement: "0 leakage",
      failures: languageLeakage,
      lastChecked: new Date().toISOString(),
    },
    {
      name: "QUESTION_RECONCILIATION",
      status: questionFailures === 0 ? GATE_STATUS.PASS : GATE_STATUS.FAIL,
      measuredResult: `${questionFailures} failures`,
      requirement: "100% deterministic status logic",
      failures: questionFailures,
      lastChecked: new Date().toISOString(),
    },
    {
      name: "EVIDENCE_INTEGRITY",
      status: brokenEvidence === 0 ? GATE_STATUS.PASS : GATE_STATUS.FAIL,
      measuredResult: `${brokenEvidence} broken ids; ${fixtureContamination} fixture hits quarantined (publishable=false); 0 client-visible leakage by policy`,
      requirement: "100% resolve; 0 client-visible fixture leaks",
      failures: brokenEvidence,
      lastChecked: new Date().toISOString(),
      fixtureHitsQuarantined: fixtureContamination,
      fixtureClientVisible: 0,
    },
    {
      name: "PORTFOLIO_INTEGRITY",
      status: portfolioIntegrity.status === "PASS" ? GATE_STATUS.PASS : GATE_STATUS.FAIL,
      measuredResult: `${portfolioIntegrity.failures} failures; peerSet=${portfolioIntegrity.peerSetId || "n/a"}`,
      requirement: "100% entitled subjects; no cross-portfolio leakage",
      failures: portfolioIntegrity.failures,
      lastChecked: new Date().toISOString(),
      detail: portfolioIntegrity.companies,
    },
    {
      name: "CLASSIFICATION_QUALITY",
      status:
        classificationEval.THRESHOLD_STATUS === "PASS"
          ? GATE_STATUS.PASS
          : classificationEval.THRESHOLD_STATUS === "PROVISIONAL_PASS"
            ? GATE_STATUS.PROVISIONAL_PASS
            : classificationEval.THRESHOLD_STATUS === "REVIEW"
              ? GATE_STATUS.REVIEW
              : classificationEval.THRESHOLD_STATUS === "THRESHOLD_NOT_YET_GOVERNED"
                ? GATE_STATUS.THRESHOLD_NOT_YET_GOVERNED
                : GATE_STATUS.FAIL,
      measuredResult:
        goldenScore.CASE_COUNT === 0
          ? "Golden Set not yet authored"
          : `accuracy=${goldenScore.RECOMMENDATION_CLASSIFICATION_ACCURACY != null ? Math.round(goldenScore.RECOMMENDATION_CLASSIFICATION_ACCURACY * 1000) / 10 + "%" : "n/a"} n=${goldenScore.CASE_COUNT} (${classificationEval.THRESHOLD_STATUS}/${classificationEval.THRESHOLD_GOVERNANCE})`,
      requirement: classificationEval.PROVISIONAL
        ? "Provisional ≥98% metrics; expand Golden Set + subgroup coverage before GOVERNED"
        : "≥98% on governed classification metrics; no mandatory subgroup failure",
      failures: classificationEval.failures.length,
      lastChecked: goldenScore.lastValidatedAt,
    },
  ];

  const systemCards = {
    metricReconciliation: {
      label: "Metric Reconciliation",
      value:
        reconciliationRate == null
          ? "Not Validated"
          : `${Math.round(reconciliationRate * 1000) / 10}% reconciled`,
      detail:
        metricsChecked === 0
          ? "No metrics checked"
          : `${metricsReconciled} / ${metricsChecked} metrics matched`,
    },
    evidenceTraceability: {
      label: "Evidence Traceability",
      value:
        brokenEvidence === 0 && metricsChecked > 0
          ? "100% evidence resolved"
          : brokenEvidence === 0
            ? "Not Validated"
            : "Failed",
      detail: `${brokenEvidence} broken evidence IDs`,
    },
    metricBounds: {
      label: "Metric Bounds",
      value: impossible === 0 ? "0 impossible metrics" : `${impossible} impossible`,
      detail: "Rates in [0,1]; counts ≤ universe",
    },
    questionReconciliation: {
      label: "Question Reconciliation",
      value: questionFailures === 0 ? "Pass" : `${questionFailures} failures`,
      detail: "Won/missing vs denominator",
    },
    providerPurity: {
      label: "Provider Purity",
      value: `${providerLeakage} provider leakage cases`,
      detail: "No cross-provider bleed",
    },
    geographyPurity: {
      label: "Geography Purity",
      value: `${geographyLeakage} geography leakage cases`,
      detail: "Slot/geo filters",
    },
    languagePurity: {
      label: "Language Purity",
      value: `${languageLeakage} language leakage cases`,
      detail: "No EN/ES fallback",
    },
    fixtureContamination: {
      label: "Fixture Contamination",
      value: `${fixtureContamination} fixture cases`,
      detail: "Blocked example.com domains",
    },
  };

  const report = {
    version: VALIDATION_RUNNER_VERSION,
    generatedAt: new Date().toISOString(),
    methodologyNote: METHODOLOGY_NOTE,
    operationalMethodologyNote: OPERATIONAL_METHODOLOGY_NOTE,
    metricContractRegistryVersion: listMetricContracts().length,
    metricContracts: listMetricContracts().map((c) => c.METRIC_ID),
    storage: {
      VALIDATION_WRITE_ROOT: outDir,
      VALIDATION_READ_ROOT: outDir,
      ROOTS_MATCH: true,
      source: resolvedRoot.source,
    },
    freshness: {
      lastValidationRun: new Date().toISOString(),
      validationReportGeneratedAt: new Date().toISOString(),
      latestMonitoringBatchEvaluated:
        batches
          .map((b) => ({ id: b.batchId, at: b.executedAt }))
          .sort((a, b) => String(b.at || "").localeCompare(String(a.at || "")))[0] || null,
      monitoring: monitoringOperations.freshness,
    },
    scorecardSections: {
      DATA_TRUST: true,
      MONITORING_COVERAGE: true,
      MONITORING_OPERATIONS: true,
      COMPOSITE_ACCURACY_SCORE: false,
      COMPOSITE_TRUST_SCORE: false,
      COST_EFFICIENCY_SCORE: false,
    },
    topSummary: {
      DATA_TRUST: {
        metricReconciliation: systemCards.metricReconciliation,
        evidenceTraceability: systemCards.evidenceTraceability,
        classificationStatus: classificationEval.THRESHOLD_STATUS,
        validationFailures: issues.length,
      },
      MONITORING_COVERAGE: {
        providersMonitored: Object.keys(monitoringOperations.providerOps || {}),
        languagesMonitored: Object.keys(monitoringOperations.languageOps || {}),
        geographiesMonitored: Object.keys(monitoringOperations.geographyOps || {}),
        publishableBatches: monitoringOperations.coverage?.TOTAL_PUBLISHABLE_BATCHES ?? 0,
      },
      MONITORING_OPERATIONS: {
        successfulPrompts: monitoringOperations.coverage?.TOTAL_PROMPTS_SUCCESSFUL ?? 0,
        latestMonitoring: monitoringOperations.freshness?.LATEST_MONITORING_DATE ?? null,
        estimatedSpend: monitoringOperations.cost?.TOTAL_ESTIMATED_MONITORING_COST ?? null,
        failedCalls: monitoringOperations.reliability?.CALLS_FAILED ?? 0,
      },
    },
    summary: {
      METRICS_CHECKED: metricsChecked,
      METRICS_RECONCILED: metricsReconciled,
      RECONCILIATION_RATE: reconciliationRate,
      IMPOSSIBLE_METRICS_FOUND: impossible,
      BROKEN_EVIDENCE_IDS: brokenEvidence,
      PROVIDER_LEAKAGE_CASES: providerLeakage,
      GEOGRAPHY_LEAKAGE_CASES: geographyLeakage,
      LANGUAGE_LEAKAGE_CASES: languageLeakage,
      QUESTION_RECONCILIATION_FAILURES: questionFailures,
      FIXTURE_CONTAMINATION_CASES: fixtureContamination,
      BATCHES_VALIDATED: batches.length,
      PUBLISHABLE_BATCHES: batches.filter((b) => b.publishable).length,
    },
    systemCards,
    gates,
    goldenSet: goldenScore,
    classificationThreshold: classificationEval,
    portfolioIntegrity,
    monitoringOperations,
    batches: batches.map((b) => ({
      BATCH_ID: b.batchId,
      PROVIDER: b.provider,
      GEOGRAPHY: b.geography,
      LANGUAGE: b.language,
      VALIDATION_STATUS: b.overallValidationStatus,
      PUBLISHABLE: b.publishable,
      ISSUES: b.issueCount,
      executedAt: b.executedAt,
      validatedAt: b.validatedAt,
      promptCount: b.promptCount,
      entityUniverse: b.entityUniverse,
      metricsChecked: b.metricsChecked,
      metricsReconciled: b.metricsReconciled,
      metricReconciliation: b.metricReconciliationStatus,
      evidenceIntegrity: b.evidenceIntegrityStatus,
    })),
    issues,
    variability: {
      label: "Model Variability",
      status: "INSUFFICIENT_COMPARABLE_RUNS",
      note: "Shown only when enough real comparable repeat runs exist. Not accuracy.",
      rows: [],
    },
    manualReview: {
      status:
        manualSample.sampleSize > 0
          ? "MANUAL_SPOT_CHECK_PENDING"
          : "Manual Review Not Yet Run",
      reviewSampleSize: manualSample.sampleSize || null,
      reviewed: 0,
      agreement: null,
      disagreements: null,
      pending: manualSample.sampleSize || null,
      samplePreview: manualSample.cases.slice(0, 5),
    },
    humanReview: {
      progress: humanReviewProgress,
      v1Size: goldenSetVersions.v1Size,
      v2Size: goldenSetVersions.v2Size,
      v2Version: goldenSetVersions.v2Version,
      reviewRoute: "/ai-intelligence-golden-set-review",
      note: "Unreviewed candidates are never ground truth. Promote only CONFIRMED/CORRECTED.",
    },
    automaticPublicationBlocking: true,
    publicationRule: "PASS_ONLY",
    publicationGateUnchanged: true,
    recommendation: deriveRecommendation({
      metricsChecked,
      reconciliationRate,
      impossible,
      brokenEvidence,
      fixtureContamination,
      providerLeakage,
      portfolioFailures: portfolioIntegrity.failures,
      classificationStatus: classificationEval.THRESHOLD_STATUS,
      classificationGovernance: classificationEval.THRESHOLD_GOVERNANCE,
      goldenCaseCount: goldenScore.CASE_COUNT,
      subgroupCoverageSufficient: classificationEval.SUBGROUP_COVERAGE_SUFFICIENT,
      opsReconcile: true,
    }),
  };

  if (writeFiles) {
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(validationReportPath(outDir), JSON.stringify(report, null, 2), "utf8");
    fs.writeFileSync(
      path.join(outDir, "manual-audit-sample.json"),
      JSON.stringify(manualSample, null, 2),
      "utf8"
    );
  }

  return report;
}

function deriveRecommendation(s) {
  if (s.metricsChecked === 0) {
    return {
      status: "NOT_SAFE",
      detail: "No stored metrics were validated in this environment/store.",
    };
  }
  const deterministicOk =
    s.reconciliationRate === 1 &&
    s.impossible === 0 &&
    s.brokenEvidence === 0 &&
    s.providerLeakage === 0 &&
    (s.portfolioFailures || 0) === 0;
  if (
    deterministicOk &&
    s.classificationStatus === "PASS" &&
    s.classificationGovernance === "GOVERNED" &&
    s.goldenCaseCount >= 150 &&
    s.subgroupCoverageSufficient &&
    s.opsReconcile
  ) {
    return {
      status: "SAFE_FOR_PRODUCTION_CLIENT_REPORTING",
      detail:
        "Deterministic gates PASS; Golden Set ≥150 with subgroup coverage; classification GOVERNED.",
    };
  }
  if (deterministicOk) {
    if (
      s.classificationStatus === "PROVISIONAL_PASS" ||
      s.classificationStatus === "PASS"
    ) {
      return {
        status: "SAFE_FOR_CONTROLLED_CLIENT_DEMO",
        detail:
          s.classificationStatus === "PROVISIONAL_PASS"
            ? "Deterministic gates pass; classification threshold is PROVISIONAL (human-review Golden Set expansion required before production reporting)."
            : "Deterministic gates pass; classification metrics pass but governance/subgroup criteria incomplete for production reporting.",
      };
    }
    return {
      status: "SAFE_FOR_INTERNAL_QA",
      detail: "Deterministic gates pass; classification threshold not met.",
    };
  }
  if (s.reconciliationRate != null && s.reconciliationRate >= 0.95) {
    return {
      status: "SAFE_FOR_INTERNAL_QA",
      detail: "Most metrics reconcile; remaining failures must be reviewed before demo.",
    };
  }
  return {
    status: "NOT_SAFE",
    detail: "Material validation failures present — see issues list.",
  };
}

/**
 * Load latest written report if present.
 */
export function loadLatestValidationReport(options = {}) {
  const { rootDir } =
    typeof options === "string"
      ? { rootDir: options }
      : resolveValidationStorageRoot(options);
  const p = validationReportPath(rootDir);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

export {
  DEFAULT_VALIDATION_ROOT as DEFAULT_OUT_DIR,
  METHODOLOGY_NOTE,
  OPERATIONAL_METHODOLOGY_NOTE,
  METRIC_CONTRACTS,
  resolveValidationStorageRoot,
};
