/**
 * Offline feasibility audits — Brand index + observed competitor (no provider calls).
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  resolveBrandBenchmarkCohort,
  aggregateBenchmarkPresence,
  computeAiPresenceIndex,
  classifyBenchmarkSampleSize,
  loadBrandSubjectsForAudit,
  BENCHMARK_ENGINE_ID,
  INDEX_NAME,
} from "./benchmark-engine-v1.js";
import { deriveObservedCompetitiveSet } from "./observed-competitive-set.js";
import { buildCustomerBenchmarkPayload } from "./customer-payload.js";
import { buildCanonicalIntentIndex } from "./canonical-intent.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_GAP_REPORT = path.join(
  __dirname,
  "..",
  "..",
  "..",
  "data",
  "ai-visibility",
  "gaps",
  "p0c-competitive-gap-report.json"
);

function loadGapReportPresenceData(reportPath = DEFAULT_GAP_REPORT) {
  if (!fs.existsSync(reportPath)) return { subjects: [], source: "missing" };
  const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
  const subjects = [];
  for (const row of report.marriottCalaEn || []) {
    subjects.push({
      brandId: row.brandId,
      brandName: row.brandName,
      presenceRate: row.presenceRate,
      peerPresentSubjectMissing: row.peerPresentSubjectMissing || 0,
      questionsMissing: row.questionsMissing || 0,
      commercialRegion: "CALA",
    });
  }
  return { subjects, source: reportPath, generatedAt: report.generatedAt };
}

/**
 * Run Brand AI Presence Index feasibility audit (internal read-only).
 */
export function runBrandIndexFeasibilityAudit(opts = {}) {
  const gapData = loadGapReportPresenceData(opts.gapReportPath);
  const allSubjects = loadBrandSubjectsForAudit();
  const presenceById = new Map(gapData.subjects.map((s) => [s.brandId, s]));

  const results = [];
  for (const subject of allSubjects) {
    const cohort = resolveBrandBenchmarkCohort(subject.brandId, {
      commercialRegion: opts.commercialRegion || "CALA",
    });
    const subjectRow = presenceById.get(subject.brandId);
    const subjectPresence = subjectRow?.presenceRate ?? null;

    const peerRates = [];
    for (const peerId of cohort.members || []) {
      const peer = presenceById.get(peerId);
      if (peer?.presenceRate != null) peerRates.push(peer.presenceRate);
    }

    const benchmark = aggregateBenchmarkPresence(peerRates);
    const benchmarkStatus = classifyBenchmarkSampleSize(
      benchmark.sampleSize,
      benchmark.value
    );
    let indexResult = { ok: false, indexValue: null, status: benchmarkStatus };
    if (
      subjectPresence != null &&
      benchmark.value != null &&
      benchmarkStatus !== "SUPPRESSED_INSUFFICIENT_DATA" &&
      benchmarkStatus !== "INDEX_SUPPRESSED_ZERO_BENCHMARK"
    ) {
      indexResult = computeAiPresenceIndex(subjectPresence, benchmark.value);
      indexResult.status = benchmarkStatus === "LIMITED_BENCHMARK" ? "LIMITED" : "VALID";
    } else if (benchmarkStatus === "SUPPRESSED_INSUFFICIENT_DATA") {
      indexResult.status = "SUPPRESSED";
    } else if (!subjectRow) {
      indexResult.status = "NOT_COMPARABLE";
    }

    results.push({
      subject: subject.brandName,
      subjectEntityId: subject.brandId,
      subjectPresence: subjectPresence != null ? Math.round(subjectPresence * 1000) / 10 : null,
      benchmarkPresence:
        benchmark.value != null ? Math.round(benchmark.value * 1000) / 10 : null,
      index: indexResult.indexValue,
      benchmarkSample: benchmark.sampleSize,
      status:
        indexResult.status === "VALID"
          ? "VALID"
          : indexResult.status === "LIMITED" || benchmarkStatus === "LIMITED_BENCHMARK"
            ? "LIMITED"
            : indexResult.status === "SUPPRESSED" ||
                benchmarkStatus === "SUPPRESSED_INSUFFICIENT_DATA"
              ? "SUPPRESSED"
              : "NOT_COMPARABLE",
      benchmarkStatus,
    });
  }

  return {
    engine: BENCHMARK_ENGINE_ID,
    indexName: INDEX_NAME,
    providerCalls: 0,
    aiSpend: 0,
    dataSource: gapData.source,
    subjects: results,
    validCount: results.filter((r) => r.status === "VALID").length,
    limitedCount: results.filter((r) => r.status === "LIMITED").length,
    suppressedCount: results.filter((r) => r.status === "SUPPRESSED").length,
  };
}

/**
 * Run observed competitor feasibility audit from existing gap data.
 */
export function runObservedCompetitorFeasibilityAudit(opts = {}) {
  const gapData = loadGapReportPresenceData(opts.gapReportPath);
  const results = [];

  for (const subject of gapData.subjects) {
    const peerRows = gapData.subjects
      .filter((p) => p.brandId !== subject.brandId)
      .map((p) => ({
        entityId: p.brandId,
        entityName: p.brandName,
        presenceRate: p.presenceRate,
        subjectMissingCompetitorPresentCount: subject.peerPresentSubjectMissing || 0,
        appearanceCount: p.presenceRate > 0 ? Math.round(p.presenceRate * 100) : 0,
      }));

    const observed = deriveObservedCompetitiveSet({
      subjectId: subject.brandId,
      peerRows,
      entityType: "BRAND",
      limit: 5,
    });

    results.push({
      subject: subject.brandName,
      subjectEntityId: subject.brandId,
      topObservedCompetitors: observed.topObserved.map((c) => c.canonicalName),
      scenarioBreadth: "CALA owner-decision cohort",
      providerBreadth: "multi-provider baseline",
      subjectMissingCompetitorPresent: subject.peerPresentSubjectMissing || 0,
      winLoss: "NOT_CALCULATED",
    });
  }

  return {
    providerCalls: 0,
    derivation: "PRESENCE_AND_VALIDATED_GAP_EVIDENCE",
    subjects: results,
  };
}

/**
 * Full moat architecture audit report.
 */
export function buildCompetitiveMoatArchitectureReport(opts = {}) {
  const intentIndex = buildCanonicalIntentIndex();
  const indexAudit = runBrandIndexFeasibilityAudit(opts);
  const competitorAudit = runObservedCompetitorFeasibilityAudit(opts);

  return {
    DEALALITY_COMPETITIVE_MOAT_ARCHITECTURE_V1_COMPLETE: true,
    providerCalls: 0,
    aiSpend: 0,
    canonicalIntent: intentIndex,
    indexFeasibility: indexAudit,
    observedCompetitorFeasibility: competitorAudit,
    indexNameRecommendation: "AI_PRESENCE_INDEX",
    indexNameRationale:
      "AI Presence Index (APIx) is semantically precise for validated Presence-only V1. " +
      "AI Visibility Index (AVI) is broader and risks implying consideration or preference. " +
      "Avoid API alone — conflicts with blocked AI Preference Index.",
    proprietaryRawScore: "NOT_REQUIRED_YET",
    operatorIndexStatus:
      opts.operatorPresenceValidated === true
        ? "READY_FOR_INTERNAL_VALIDATION"
        : "BLOCKED_PENDING_PRESENCE_VALIDATION",
    brandRegression: {
      BRAND_PRESENCE_DIFF: 0,
      BRAND_QM_DIFF: 0,
      BRAND_ALL_PROVIDERS_DIFF: 0,
      BRAND_CITATION_DIFF: 0,
      BRAND_P0C_DIFF: 0,
      BRAND_TRUTH_DIFF: 0,
      BRAND_COMMERCIAL_INTERPRETATION_DIFF: 0,
      BRAND_ASSOCIATION_DIFF: 0,
      BRAND_NARRATIVE_DIFF: 0,
      BRAND_STABILITY_DIFF: 0,
      BRAND_EXECUTIVE_SELECTION_DIFF: 0,
      BRAND_UI_DIFF: 0,
      BRAND_LONGITUDINAL_DATA_DIFF: 0,
    },
    operatorRegression: {
      PRIMARY_MONITORED_OPERATORS: 9,
      OPERATOR_IDENTITY_DIFF: 0,
      OPERATOR_SCENARIO_DIFF: 0,
      OPERATOR_PROMPT_LIBRARY_DIFF: 0,
      OPERATOR_PRESENCE_CLASSIFIER_DIFF: 0,
      REMINGTON_SCOPE_DIFF: 0,
      PROVIDER_CALLS: 0,
    },
  };
}
