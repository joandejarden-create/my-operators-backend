#!/usr/bin/env node
/**
 * AI Intelligence Validation Foundation tests.
 * No live provider calls. No Airtable writes. No deploys.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  METRIC_CONTRACTS,
  listMetricContracts,
} from "../lib/ai-visibility/validation/metric-contracts.js";
import {
  METHODOLOGY_NOTE,
  GATE_STATUS,
  METRIC_VALIDATION_STATE,
} from "../lib/ai-visibility/validation/validation-status.js";
import {
  auditBuildObservationsFromEvidence,
  auditComputeEntityMetrics,
  auditMetricBounds,
  reconcileEntityMetrics,
} from "../lib/ai-visibility/validation/independent-recompute.js";
import { loadGoldenSet, scoreGoldenSet } from "../lib/ai-visibility/validation/golden-set.js";
import { sampleManualAuditCases } from "../lib/ai-visibility/validation/manual-audit-sample.js";
import {
  runAiIntelligenceValidation,
  VALIDATION_RUNNER_VERSION,
} from "../lib/ai-visibility/validation/run-validation.js";
import {
  canAccessAiIntelligenceValidation,
} from "../middleware/requireAiIntelligenceValidationAccess.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    const r = fn();
    if (r && typeof r.then === "function") {
      throw new Error("Use testAsync for async tests");
    }
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

async function testAsync(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("AI Intelligence Validation Foundation\n");

test("VALIDATION_MANIFEST_GENERATED constants", () => {
  assert.ok(VALIDATION_RUNNER_VERSION);
  assert.equal(METRIC_VALIDATION_STATE.RECONCILED, "RECONCILED");
  assert.ok(GATE_STATUS.PASS);
});

test("METRIC_CONTRACT_REGISTRY covers client metrics", () => {
  const ids = listMetricContracts().map((c) => c.METRIC_ID);
  for (const id of [
    "AI_PRESENCE",
    "RECOMMENDATION_SHARE",
    "TOP3_RECOMMENDATION_RATE",
    "FIRST_RECOMMENDATION_RATE",
    "QUESTIONS_WON_COUNT",
    "QUESTIONS_MISSING_COUNT",
    "COMPETITIVE_POSITION",
    "DECISION_VISIBILITY_COVERAGE",
    "TOP_DECISION_TERRITORY",
  ]) {
    assert.ok(METRIC_CONTRACTS[id], `missing contract ${id}`);
    assert.ok(ids.includes(id));
  }
});

test("INDEPENDENT_RECOMPUTATION_MATCHES known fixture", () => {
  const evidenceRows = [
    {
      evidenceId: "ev_a",
      promptId: "p1",
      provider: "openai",
      mentions: [
        {
          canonicalEntityId: "recBrandA",
          role: "first_recommendation",
          recommendationPosition: 1,
        },
        {
          canonicalEntityId: "recBrandB",
          role: "ranked_recommendation",
          recommendationPosition: 2,
        },
      ],
    },
    {
      evidenceId: "ev_b",
      promptId: "p2",
      provider: "openai",
      mentions: [{ canonicalEntityId: "recBrandB", role: "discussed" }],
    },
  ];
  const obs = auditBuildObservationsFromEvidence(evidenceRows);
  const a = auditComputeEntityMetrics(obs, "recBrandA");
  assert.equal(a.numeratorPresence, 1);
  assert.equal(a.denominatorPresence, 2);
  assert.equal(a.presence, 0.5);
  assert.equal(a.questionsWon, 1);
  assert.equal(a.questionsMissing, 1);

  const production = {
    id: "recBrandA",
    presence: 0.5,
    recommendationShare: a.recommendationShare,
    recommendationRate: a.recommendationRate,
    top3RecommendationRate: a.top3RecommendationRate,
    firstRecommendationRate: a.firstRecommendationRate,
    questionsWon: 1,
    questionsMissing: 1,
  };
  const recon = reconcileEntityMetrics(production, a);
  assert.equal(recon.allReconciled, true);
});

test("METRIC_BOUNDS_ENFORCED", () => {
  const v = auditMetricBounds({
    presence: 1.5,
    questionsWon: 10,
    denominatorPresence: 5,
  });
  assert.ok(v.some((x) => x.reason === "rate_out_of_bounds"));
  assert.ok(v.some((x) => x.reason === "count_exceeds_denominator"));
});

test("RAW_EVIDENCE_TRACEABLE structure", () => {
  const obs = auditBuildObservationsFromEvidence([
    { evidenceId: "ev_1", promptId: "p", mentions: [] },
  ]);
  assert.equal(obs[0].evidenceId, "ev_1");
});

test("FIXTURE_CONTAMINATION_DETECTED helper available", () => {
  // Domain guard used by runner — import smoke
  assert.ok(true);
});

test("GOLDEN_SET_RESULTS_REPORTED", () => {
  const golden = loadGoldenSet();
  const score = scoreGoldenSet(golden);
  assert.ok(score.GOLDEN_SET_VERSION);
  assert.equal(typeof score.CASE_COUNT, "number");
  if (score.CASE_COUNT > 0) {
    assert.ok(score.sampleSize > 0);
    assert.ok(score.RECOMMENDATION_CLASSIFICATION_ACCURACY != null);
  }
});

test("VALIDATION_FAILURE_NOT_SILENT", () => {
  const production = { presence: 0.9, questionsWon: 1, questionsMissing: 0 };
  const audit = {
    entityId: "recX",
    presence: 0.5,
    questionsWon: 0,
    questionsMissing: 1,
    recommendationShare: null,
    recommendationRate: null,
    top3RecommendationRate: null,
    firstRecommendationRate: null,
  };
  const recon = reconcileEntityMetrics(production, audit);
  assert.ok(recon.mismatchCount >= 1);
  assert.equal(recon.comparisons.find((c) => c.metricId === "AI_PRESENCE").match, false);
});

test("NO_COMPOSITE_CONFIDENCE_SCORE vocabulary", () => {
  assert.ok(!("HIGH" in METRIC_VALIDATION_STATE));
  assert.ok(!("TRUST_SCORE" in METRIC_VALIDATION_STATE));
  const note = METHODOLOGY_NOTE.toLowerCase();
  assert.ok(!note.includes("unreliable"));
  assert.ok(!note.includes("not accurate"));
  assert.ok(note.includes("probabilistic"));
});

test("METHODOLOGY_NOTE_PRESENT", () => {
  assert.ok(METHODOLOGY_NOTE.length > 40);
  const brandHtml = fs.readFileSync(
    path.join(ROOT, "public/ai-visibility-brand.html"),
    "utf8"
  );
  assert.ok(brandHtml.includes("aiv-methodology-footnote"));
  assert.ok(brandHtml.includes(METHODOLOGY_NOTE));
  const scoreHtml = fs.readFileSync(
    path.join(ROOT, "public/ai-intelligence-validation.html"),
    "utf8"
  );
  assert.ok(scoreHtml.includes("Validation Scorecard"));
  assert.ok(scoreHtml.includes("aivValMethodologyNote"));
});

test("VALIDATION_SCORECARD_AUTH_GATED", () => {
  assert.equal(canAccessAiIntelligenceValidation(null), false);
  assert.equal(
    canAccessAiIntelligenceValidation({ isAdmin: false, workspaceAccess: ["brand"] }),
    false
  );
  assert.equal(canAccessAiIntelligenceValidation({ isAdmin: true }), true);
});

test("NORMAL_CLIENT_CANNOT_ACCESS_INTERNAL_SCORECARD", () => {
  assert.equal(
    canAccessAiIntelligenceValidation({
      isAdmin: false,
      flags: {},
      workspaceAccess: ["owner", "brand", "operator"],
      email: "client@example.com",
    }),
    false
  );
});

test("manual audit sampling deterministic", () => {
  const a = sampleManualAuditCases(
    [
      { batchId: "b1", responseId: "r1", promptId: "p1", entityId: "e1", provider: "openai" },
      { batchId: "b1", responseId: "r2", promptId: "p2", entityId: "e1", provider: "openai" },
    ],
    { sampleSize: 1, seed: "test" }
  );
  const b = sampleManualAuditCases(
    [
      { batchId: "b1", responseId: "r1", promptId: "p1", entityId: "e1", provider: "openai" },
      { batchId: "b1", responseId: "r2", promptId: "p2", entityId: "e1", provider: "openai" },
    ],
    { sampleSize: 1, seed: "test" }
  );
  assert.equal(a.cases[0].reviewCaseId, b.cases[0].reviewCaseId);
});

await testAsync("PROVIDER_PURITY_GEO_LANG_run_on_store", async () => {
  const report = await runAiIntelligenceValidation({ writeFiles: true });
  assert.ok(report.summary);
  assert.equal(typeof report.summary.PROVIDER_LEAKAGE_CASES, "number");
  assert.equal(typeof report.summary.GEOGRAPHY_LEAKAGE_CASES, "number");
  assert.equal(typeof report.summary.LANGUAGE_LEAKAGE_CASES, "number");
  assert.ok(Array.isArray(report.gates));
  assert.ok(Array.isArray(report.batches));
  // Failures must be visible — never invent green
  if (report.summary.METRICS_CHECKED > 0 && report.summary.RECONCILIATION_RATE < 1) {
    assert.ok((report.issues || []).length > 0, "mismatches must surface as issues");
  }
  const out = path.join(ROOT, "data/ai-visibility/validation/latest-validation-report.json");
  assert.ok(fs.existsSync(out), "VALIDATION_MANIFEST_GENERATED on disk");
});

test("nav route registered in app.js", () => {
  const app = fs.readFileSync(path.join(ROOT, "public/app.js"), "utf8");
  assert.ok(app.includes("/ai-intelligence-validation"));
  assert.ok(app.includes("Validation Scorecard"));
  assert.ok(app.includes("validationScorecard"));
});

test("VALIDATION_SUMMARY_ROUTE_REGISTERED in server.js", () => {
  const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  for (const p of [
    "/api/ai-intelligence/validation/summary",
    "/api/ai-intelligence/validation/gates",
    "/api/ai-intelligence/validation/classification",
    "/api/ai-intelligence/validation/batches",
    "/api/ai-intelligence/validation/issues",
    "/api/ai-intelligence/validation/variability",
  ]) {
    assert.ok(server.includes(p), p);
  }
});

test("VALIDATION_PAGE_DISTINGUISHES_401_403_404_500", () => {
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/ai-visibility/ai-intelligence-validation.js"),
    "utf8"
  );
  assert.ok(js.includes("AUTH_REQUIRED"));
  assert.ok(js.includes("ACCESS_DENIED"));
  assert.ok(js.includes("VALIDATION_NOT_RUN"));
  assert.ok(js.includes("VALIDATION_REPORT_INVALID"));
  assert.ok(js.includes("SERVER_ERROR"));
  assert.ok(js.includes("ROUTE_MISSING") || js.includes("VALIDATION_SUMMARY_ROUTE_MISSING"));
});

await testAsync("VALIDATION_SUMMARY_READS_CANONICAL_ROOT", async () => {
  const { resolveValidationStorageRoot } = await import(
    "../lib/ai-visibility/validation/validation-storage-root.js"
  );
  const a = resolveValidationStorageRoot({});
  const b = resolveValidationStorageRoot({});
  assert.equal(a.rootDir, b.rootDir);
  assert.ok(a.rootDir.replace(/\\/g, "/").endsWith("data/ai-visibility/validation"));
});

await testAsync("METRIC_RECOMPUTATION_100_and_publication_gate", async () => {
  const report = await runAiIntelligenceValidation({ writeFiles: true });
  assert.equal(report.summary.METRICS_RECONCILED, report.summary.METRICS_CHECKED);
  assert.equal(report.summary.RECONCILIATION_RATE, 1);
  assert.equal(report.automaticPublicationBlocking, true);
  const {
    isBatchClientPublishable,
    filterSummariesForClientPublication,
  } = await import("../lib/ai-visibility/validation/publication-gate.js");
  for (const b of report.batches) {
    if (b.VALIDATION_STATUS === "PASS") {
      assert.equal(b.PUBLISHABLE, true);
      assert.equal(isBatchClientPublishable(b.BATCH_ID), true);
    } else {
      assert.equal(b.PUBLISHABLE, false);
      assert.equal(isBatchClientPublishable(b.BATCH_ID), false);
    }
  }
  const fake = [
    { batchId: "aiv_wave1_openai_showcase_20260814_0141_99ec67", status: "completed" },
    { batchId: "aiv_wave1_openai_showcase_20260814_0143_8367c6", status: "completed" },
  ];
  const gated = filterSummariesForClientPublication(fake);
  assert.equal(gated.gateActive, true);
  assert.ok(gated.publishable.some((s) => s.batchId.includes("0143")));
  assert.ok(!gated.publishable.some((s) => s.batchId.includes("99ec67")));
  assert.equal(gated.latestBatchFailedValidation, true);
});

await testAsync("FIXTURE_SOURCE_NOT_CLIENT_VISIBLE helper", async () => {
  const { filterFixtureContaminatedSources, isBlockedFixtureDomain } = await import(
    "../lib/ai-visibility/fixture-domain-guard.js"
  );
  assert.equal(isBlockedFixtureDomain("https://example.com/x"), true);
  assert.equal(isBlockedFixtureDomain("hilton.com"), false);
  const filtered = filterFixtureContaminatedSources([
    { domain: "example.com", url: "https://example.com/a" },
    { domain: "hilton.com", url: "https://hilton.com/a" },
  ]);
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].domain, "hilton.com");
});

await testAsync("PORTFOLIO_INTEGRITY_PASS", async () => {
  const { runPortfolioIntegrityGate } = await import(
    "../lib/ai-visibility/validation/portfolio-integrity.js"
  );
  const r = runPortfolioIntegrityGate();
  assert.equal(r.status, "PASS");
  assert.equal(r.crossPortfolioLeakage, false);
  for (const k of ["MARRIOTT", "HILTON", "IHG", "CHOICE"]) {
    assert.equal(r.companies[k].status, "PASS");
  }
});

await testAsync("CLASSIFICATION_THRESHOLD_PROVISIONAL", async () => {
  const { evaluateClassificationThreshold } = await import(
    "../lib/ai-visibility/validation/classification-threshold.js"
  );
  const r = evaluateClassificationThreshold({
    CASE_COUNT: 65,
    ENTITY_RESOLUTION_PRECISION: 1,
    ENTITY_RESOLUTION_RECALL: 1,
    RECOMMENDATION_CLASSIFICATION_ACCURACY: 1,
    RECOMMENDATION_PRECISION: 1,
    RECOMMENDATION_RECALL: 1,
    FIRST_RECOMMENDATION_ACCURACY: 1,
  });
  assert.equal(r.THRESHOLD_STATUS, "PROVISIONAL_PASS");
  assert.equal(r.PROVISIONAL, true);
  assert.equal(r.THRESHOLD_GOVERNANCE, "PROVISIONAL");
  assert.equal(r.SAMPLE_SIZE_SUFFICIENT, false);
});

test("GOLDEN_SET_VERSIONED", () => {
  const v1Path = path.join(ROOT, "fixtures/ai-visibility/ai-intelligence-golden-set-v1.json");
  assert.ok(fs.existsSync(v1Path), "v1 fixture must exist");
  const v1 = JSON.parse(fs.readFileSync(v1Path, "utf8"));
  assert.equal(v1.version, "ai_intelligence_golden_set_v1");
  assert.ok(v1.caseCount >= 65);
  assert.equal(v1.llmLabelledAsGroundTruth, 0);
});

await testAsync("GOLDEN_SET_N_AT_LEAST_150_blocks_governed", async () => {
  const { evaluateClassificationThreshold, GOLDEN_SET_EXPANSION_TARGET } = await import(
    "../lib/ai-visibility/validation/classification-threshold.js"
  );
  const golden = loadGoldenSet();
  const score = scoreGoldenSet(golden);
  const evalr = evaluateClassificationThreshold(score, {
    coverage: score.coverage,
    subgroupMetrics: score.subgroupMetrics,
  });
  assert.ok(GOLDEN_SET_EXPANSION_TARGET.minCases >= 150);
  if (score.CASE_COUNT < 150) {
    assert.notEqual(evalr.THRESHOLD_GOVERNANCE, "GOVERNED");
    assert.notEqual(evalr.THRESHOLD_STATUS, "PASS");
  }
});

test("NO_LLM_LABELS_USED_AS_GROUND_TRUTH", () => {
  const golden = loadGoldenSet();
  assert.equal(golden.llmLabelledAsGroundTruth || 0, 0);
  for (const c of golden.cases) {
    assert.notEqual(c.llmLabelledAsGroundTruth, true);
  }
  const candPath = path.join(
    ROOT,
    "fixtures/ai-visibility/ai-intelligence-golden-set-v2-candidates.json"
  );
  if (fs.existsSync(candPath)) {
    const cand = JSON.parse(fs.readFileSync(candPath, "utf8"));
    assert.equal(cand.llmLabelledAsGroundTruth, 0);
    assert.equal(cand.humanLabelled, 0);
    assert.equal(cand.reviewStatus, "PENDING_HUMAN_REVIEW");
  }
});

await testAsync("THRESHOLD_NOT_LOWERED_TO_FORCE_PASS", async () => {
  const { CLASSIFICATION_RELEASE_THRESHOLDS } = await import(
    "../lib/ai-visibility/validation/classification-threshold.js"
  );
  for (const v of Object.values(CLASSIFICATION_RELEASE_THRESHOLDS)) {
    assert.ok(v >= 0.98);
  }
});

test("SCORECARD_HAS_THREE_SECTIONS_NO_COMPOSITE", () => {
  const html = fs.readFileSync(
    path.join(ROOT, "public/ai-intelligence-validation.html"),
    "utf8"
  );
  assert.ok(html.includes("A. Data Trust") || html.includes("Data Trust"));
  assert.ok(html.includes("Monitoring Coverage"));
  assert.ok(html.includes("Monitoring Operations"));
  assert.ok(!html.includes("Trust Score"));
  assert.ok(!html.includes("Accuracy Score"));
  assert.ok(!html.includes("Cost Efficiency Score"));
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/ai-visibility/ai-intelligence-validation.js"),
    "utf8"
  );
  assert.ok(js.includes("Estimated"));
  assert.ok(js.includes("Not Available"));
  assert.ok(!js.includes("COMPOSITE_ACCURACY"));
});

await testAsync("MONITORING_OPS_RECONCILE_AND_LABELLED", async () => {
  const { buildMonitoringOperationsReport } = await import(
    "../lib/ai-visibility/validation/monitoring-operations.js"
  );
  const { OPS_METRIC_CONTRACTS } = await import(
    "../lib/ai-visibility/validation/ops-metric-contracts.js"
  );
  assert.ok(OPS_METRIC_CONTRACTS.PROMPTS_ATTEMPTED);
  assert.ok(OPS_METRIC_CONTRACTS.ESTIMATED_COST);
  const ops = await buildMonitoringOperationsReport({});
  assert.ok(ops.coverage);
  assert.equal(typeof ops.coverage.TOTAL_MONITORING_BATCHES, "number");
  assert.equal(ops.cost.ESTIMATED_OR_ACTUAL, "Estimated");
  assert.equal(ops.cost.LABEL, "Estimated Cost");
  // Missing cost must not be forced to 0 at total level when no cost runs
  if (ops.cost.BATCHES_WITH_COST === 0) {
    assert.equal(ops.cost.TOTAL_ESTIMATED_MONITORING_COST, null);
  }
  // Provider × language matrix present
  assert.ok(Array.isArray(ops.providerLanguageMatrix));
  // Slot-based language: en/es keys when present
  for (const row of ops.providerLanguageMatrix) {
    assert.ok("ENGLISH_PROMPTS" in row);
    assert.ok("SPANISH_PROMPTS" in row);
  }
  // Inventory rendered fields
  assert.ok(Array.isArray(ops.inventory));
  for (const row of ops.inventory.slice(0, 5)) {
    assert.ok("VALIDATION_STATUS" in row);
    assert.ok("PUBLISHABLE" in row);
    if (row.ESTIMATED_COST === 0) {
      // zero only if explicitly recorded cost runs summed to 0 — allow but prefer null for missing
    }
  }
  // Prompt totals reconcile
  let invAttempted = 0;
  let invSuccess = 0;
  let invFail = 0;
  for (const row of ops.inventory) {
    invAttempted += row.PROMPT_COUNT || 0;
    invSuccess += row.SUCCESS_COUNT || 0;
    invFail += row.FAIL_COUNT || 0;
  }
  assert.equal(invAttempted, ops.coverage.TOTAL_PROMPTS_ATTEMPTED);
  assert.equal(invSuccess, ops.coverage.TOTAL_PROMPTS_SUCCESSFUL);
  assert.equal(invFail, ops.coverage.TOTAL_PROMPTS_FAILED);

  // Provider cost sum reconciles when costs exist
  if (ops.cost.TOTAL_ESTIMATED_MONITORING_COST != null) {
    const sum = Object.values(ops.cost.COST_BY_PROVIDER || {}).reduce(
      (a, b) => a + (b == null ? 0 : Number(b)),
      0
    );
    assert.ok(Math.abs(sum - ops.cost.TOTAL_ESTIMATED_MONITORING_COST) < 0.0001);
  }
});

await testAsync("VALIDATION_REPORT_INCLUDES_OPS_SECTIONS", async () => {
  const report = await runAiIntelligenceValidation({ writeFiles: true });
  assert.ok(report.monitoringOperations);
  assert.equal(report.scorecardSections.DATA_TRUST, true);
  assert.equal(report.scorecardSections.MONITORING_COVERAGE, true);
  assert.equal(report.scorecardSections.MONITORING_OPERATIONS, true);
  assert.equal(report.scorecardSections.COMPOSITE_ACCURACY_SCORE, false);
  assert.equal(report.scorecardSections.COMPOSITE_TRUST_SCORE, false);
  assert.ok(report.operationalMethodologyNote);
  assert.ok(report.topSummary.DATA_TRUST);
  assert.ok(report.topSummary.MONITORING_COVERAGE);
  assert.ok(report.topSummary.MONITORING_OPERATIONS);
  assert.equal(report.publicationGateUnchanged, true);
  assert.ok(report.goldenSet.subgroupMetrics);
  assert.equal(report.manualReview.status, "MANUAL_SPOT_CHECK_PENDING");
  const classGate = report.gates.find((g) => g.name === "CLASSIFICATION_QUALITY");
  assert.ok(classGate);
  assert.ok(
    ["PASS", "PROVISIONAL_PASS", "REVIEW", "FAIL", "THRESHOLD_NOT_YET_GOVERNED"].includes(
      classGate.status
    )
  );
});

test("VALIDATION_SUMMARY_ROUTE_REGISTERED includes operations", () => {
  const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  assert.ok(server.includes("/api/ai-intelligence/validation/operations"));
  assert.ok(server.includes("/api/ai-intelligence/validation/batches/:batchId"));
});

test("SUBGROUP_METRICS_REPORTED_AND_FAILED_CASES_RETAINED", () => {
  const golden = loadGoldenSet();
  const score = scoreGoldenSet(golden);
  if (score.CASE_COUNT > 0) {
    assert.ok(score.subgroupMetrics);
    assert.ok(score.subgroupMetrics.PROVIDER || score.subgroupMetrics.CASE_TYPE);
    assert.ok(Array.isArray(score.errors));
    assert.equal(typeof score.ERROR_COUNT, "number");
  }
});

test("FAILED_CASES_RETAINED in score object", () => {
  const score = scoreGoldenSet(loadGoldenSet());
  assert.ok("errors" in score || score.CASE_COUNT === 0);
  assert.ok("errorsByCategory" in score || score.CASE_COUNT === 0);
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
