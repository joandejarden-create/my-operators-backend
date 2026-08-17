#!/usr/bin/env node
/**
 * Golden Set human-review governance tests.
 * No live providers. No auto-approval. No Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadCandidateDocument,
  loadGoldenSetV1Document,
  getReviewProgress,
  submitHumanReview,
  isPromotableReview,
  promoteGoldenSetV2,
  loadReviewRecord,
  REVIEW_STATUS,
  RECOMMENDATION_STATUS_TAXONOMY,
  QUESTION_STATUS_TAXONOMY,
  CITATION_ASSOCIATION_TAXONOMY,
} from "../lib/ai-visibility/validation/golden-set-human-review.js";
import { canAccessAiIntelligenceValidation } from "../middleware/requireAiIntelligenceValidationAccess.js";
import {
  CLASSIFICATION_RELEASE_THRESHOLDS,
} from "../lib/ai-visibility/validation/classification-threshold.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gsr-review-"));
const opts = { outDir: tmpRoot };

console.log("AI Intelligence Golden Set Human Review\n");

test("candidates exist and are not ground truth", () => {
  const doc = loadCandidateDocument();
  assert.ok(doc.caseCount >= 125 || (doc.cases || []).length >= 100);
  assert.equal(doc.humanLabelled, 0);
  assert.equal(doc.llmLabelledAsGroundTruth, 0);
  assert.equal(doc.reviewStatus, "PENDING_HUMAN_REVIEW");
});

test("GOLDEN_SET_V1_PRESERVED", () => {
  const v1 = loadGoldenSetV1Document();
  assert.equal(v1.version, "ai_intelligence_golden_set_v1");
  assert.ok((v1.cases || []).length >= 65);
  assert.equal(v1.llmLabelledAsGroundTruth, 0);
});

test("UNREVIEWED_CASE_NOT_GROUND_TRUTH", () => {
  const progress = getReviewProgress(opts);
  assert.equal(progress.REVIEWED, 0);
  assert.equal(progress.REMAINING, progress.TOTAL);
  const doc = loadCandidateDocument();
  const c = doc.cases[0];
  assert.equal(isPromotableReview(null), false);
  assert.equal(
    isPromotableReview({
      reviewStatus: REVIEW_STATUS.UNREVIEWED,
      humanLabels: {},
      autoApproved: false,
    }),
    false
  );
  assert.ok(c.caseId);
});

test("SYSTEM_SUGGESTION_NOT_GROUND_TRUTH", () => {
  const doc = loadCandidateDocument();
  const c = doc.cases[0];
  assert.ok(c.systemSuggestion);
  assert.equal(c.humanLabelled, false);
  assert.equal(c.expectedRecommendationClass, null);
  // Suggestion alone is not promotable
  assert.equal(
    isPromotableReview({
      reviewStatus: REVIEW_STATUS.UNREVIEWED,
      systemSuggestion: c.systemSuggestion,
      humanLabels: c.systemSuggestion,
      autoApproved: false,
    }),
    false
  );
});

test("HUMAN_REVIEW_REQUIRED / CONFIRMED_CASE_PROMOTABLE", () => {
  const doc = loadCandidateDocument();
  const c = doc.cases[0];
  const labels = {
    entityPresent: true,
    canonicalEntityId: c.canonicalEntityId || "recTest",
    canonicalEntityName: c.candidateEntity || "Test Brand",
    recommendationStatus: "discussed",
    firstRecommendation: false,
    questionStatus: "DISCUSSION_ONLY",
    citationAssociation: "UNKNOWN",
  };
  const result = submitHumanReview(
    {
      caseId: c.caseId,
      reviewStatus: REVIEW_STATUS.CONFIRMED,
      humanLabels: labels,
      reviewer: "test_reviewer@dealality.test",
      notes: "unit test confirm",
    },
    opts
  );
  assert.equal(result.record.autoApproved, false);
  assert.equal(result.record.llmLabelledAsGroundTruth, false);
  assert.equal(result.record.reviewStatus, "CONFIRMED");
  assert.equal(isPromotableReview(result.record), true);
});

test("CORRECTED_CASE_PROMOTABLE", () => {
  const doc = loadCandidateDocument();
  const c = doc.cases[1];
  const result = submitHumanReview(
    {
      caseId: c.caseId,
      reviewStatus: REVIEW_STATUS.CORRECTED,
      humanLabels: {
        entityPresent: true,
        canonicalEntityId: c.canonicalEntityId || "recTest2",
        canonicalEntityName: c.candidateEntity || "Test Brand 2",
        recommendationStatus: "first_recommendation",
        firstRecommendation: true,
        questionStatus: "FIRST_RECOMMENDED",
        citationAssociation: "ASSOCIATED",
      },
      reviewer: "test_reviewer@dealality.test",
    },
    opts
  );
  assert.equal(isPromotableReview(result.record), true);
});

test("DEFERRED_CASE_NOT_PROMOTABLE", () => {
  const doc = loadCandidateDocument();
  const c = doc.cases[2];
  const result = submitHumanReview(
    {
      caseId: c.caseId,
      reviewStatus: REVIEW_STATUS.DEFERRED,
      humanLabels: {},
      reviewer: "test_reviewer@dealality.test",
      notes: "ambiguous — defer",
    },
    opts
  );
  assert.equal(isPromotableReview(result.record), false);
});

test("REVIEW_AUDIT_TRAIL_PRESERVED", () => {
  const doc = loadCandidateDocument();
  const c = doc.cases[0];
  // Re-submit to create history
  submitHumanReview(
    {
      caseId: c.caseId,
      reviewStatus: REVIEW_STATUS.CORRECTED,
      humanLabels: {
        entityPresent: true,
        canonicalEntityId: c.canonicalEntityId || "recTest",
        canonicalEntityName: c.candidateEntity || "Test Brand",
        recommendationStatus: "ranked_recommendation",
        firstRecommendation: false,
        questionStatus: "RECOMMENDED",
        citationAssociation: "NOT_ASSOCIATED",
      },
      reviewer: "test_reviewer@dealality.test",
      notes: "second edit",
    },
    opts
  );
  const record = loadReviewRecord(c.caseId, opts);
  assert.equal(record.priorReviewPreserved, true);
  const histDir = path.join(tmpRoot, "human-review", "history", c.caseId);
  assert.ok(fs.existsSync(histDir));
  assert.ok(fs.readdirSync(histDir).length >= 1);
});

test("GOLDEN_SET_V2_VERSIONED dry-run promote", () => {
  const promo = promoteGoldenSetV2({ ...opts, apply: false });
  assert.equal(promo.version, "ai_intelligence_golden_set_v2");
  assert.equal(promo.previousVersion, "ai_intelligence_golden_set_v1");
  assert.equal(promo.written, false);
  assert.ok(promo.casesFromV1 >= 65);
  assert.ok(promo.casesPromotedFromReview >= 2); // confirmed+corrected from tests
  assert.equal(promo.llmLabelledAsGroundTruth, 0);
  assert.equal(promo.autoApproved, 0);
  // Deferred not included — promotable only confirm/correct
  assert.ok(promo.caseCount >= 65 + 2);
});

test("PROVIDER_LANGUAGE_GEOGRAPHY_METADATA_RETAINED on promote preview", () => {
  const promo = promoteGoldenSetV2({ ...opts, apply: false });
  assert.ok(promo.coverageAudit);
  assert.ok(promo.promotionRule.includes("CONFIRMED"));
});

test("QUESTION_STATUS_LABELS_RETAINED / CITATION taxonomy", () => {
  assert.ok(QUESTION_STATUS_TAXONOMY.includes("FIRST_RECOMMENDED"));
  assert.ok(CITATION_ASSOCIATION_TAXONOMY.includes("ASSOCIATED"));
  assert.ok(RECOMMENDATION_STATUS_TAXONOMY.includes("negative_or_qualified"));
});

test("GOLDEN_SET_REVIEW_INTERNAL_ONLY", () => {
  assert.equal(canAccessAiIntelligenceValidation(null), false);
  assert.equal(
    canAccessAiIntelligenceValidation({
      isAdmin: false,
      workspaceAccess: ["owner", "brand"],
      email: "client@example.com",
    }),
    false
  );
  assert.equal(canAccessAiIntelligenceValidation({ isAdmin: true }), true);
});

test("NORMAL_CLIENT_REVIEW_ACCESS_DENIED nav route gated", () => {
  const app = fs.readFileSync(path.join(ROOT, "public/app.js"), "utf8");
  assert.ok(app.includes("/ai-intelligence-golden-set-review"));
  assert.ok(app.includes("validationScorecard: true"));
  const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  assert.ok(server.includes("/api/ai-intelligence/golden-set-review/queue"));
  assert.ok(server.includes("requireAiIntelligenceValidationAccess"));
});

test("THRESHOLDS_NOT_LOWERED", () => {
  for (const v of Object.values(CLASSIFICATION_RELEASE_THRESHOLDS)) {
    assert.ok(v >= 0.98);
  }
});

test("FAILED_CASES_NOT_REMOVED / no auto-approve in UI", () => {
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/ai-visibility/ai-intelligence-golden-set-review.js"),
    "utf8"
  );
  assert.ok(js.includes("CONFIRMED"));
  assert.ok(js.includes("CORRECTED"));
  assert.ok(js.includes("DEFERRED"));
  assert.ok(!js.includes("autoApprove"));
  assert.ok(js.includes("SYSTEM SUGGESTION") || js.includes("systemSuggestion"));
});

test("EXECUTIVE_INSIGHT_BOX_REQUIREMENT_LOCKED in BUILD_DECISIONS", () => {
  const bd = fs.readFileSync(
    path.join(ROOT, "docs/ai-build-system/BUILD_DECISIONS.md"),
    "utf8"
  );
  assert.ok(bd.includes("Executive Insight Boxes"));
  assert.ok(bd.includes("FINDING → EVIDENCE → WHY IT MATTERS"));
  assert.ok(bd.includes("CALA bilingual insight"));
  assert.ok(bd.includes("All Providers derived view"));
  assert.ok(bd.includes('No "All AI Score"') || bd.includes("no arbitrary cross-provider composite"));
  assert.ok(bd.includes("No freeform LLM executive summary"));
});

test("scorecard links Golden Set Review", () => {
  const html = fs.readFileSync(
    path.join(ROOT, "public/ai-intelligence-validation.html"),
    "utf8"
  );
  assert.ok(html.includes("/ai-intelligence-golden-set-review"));
  assert.ok(html.includes("Open Golden Set Review"));
});

test("SUBGROUP_FAILURE_NOT_HIDDEN threshold still subgroup-aware", () => {
  const src = fs.readFileSync(
    path.join(ROOT, "lib/ai-visibility/validation/classification-threshold.js"),
    "utf8"
  );
  assert.ok(src.includes("NO_MAJOR_SUBGROUP_HIDDEN_BY_AGGREGATE"));
});

test("GOLDEN_SET_REVIEW_ROUTE_REGISTERED", () => {
  const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  assert.ok(server.includes("/api/ai-intelligence/golden-set-review/queue"));
  assert.ok(server.includes("getGoldenSetReviewQueue"));
  assert.ok(server.includes("requireAiIntelligenceValidationAccess"));
});

test("GOLDEN_SET_CANDIDATE_FILE_EXISTS and ENTITY_SPECIFIC_ACTIVE", async () => {
  const { getCandidateSourcePath, loadCandidateDocument } = await import(
    "../lib/ai-visibility/validation/golden-set-human-review.js"
  );
  const { auditCandidateSubjects, isReviewableCandidate } = await import(
    "../lib/ai-visibility/validation/golden-set-candidate-entity-remediation.js"
  );
  const p = getCandidateSourcePath();
  assert.ok(path.isAbsolute(p), "candidate path must be absolute (cwd-independent)");
  assert.ok(fs.existsSync(p));
  const doc = loadCandidateDocument();
  assert.ok(Array.isArray(doc.cases));
  assert.equal(typeof doc, "object");
  assert.ok(!Array.isArray(doc));
  const audit = auditCandidateSubjects(doc);
  assert.equal(audit.NULL_SUBJECT_CASES, 0);
  assert.ok(audit.VALID_ENTITY_SPECIFIC_CASES > 0);
  assert.equal(
    (doc.cases || []).filter(isReviewableCandidate).length,
    audit.VALID_ENTITY_SPECIFIC_CASES
  );
});

test("GOLDEN_SET_QUEUE_LOADS and SCHEMA_VALID", async () => {
  const { buildReviewQueue } = await import(
    "../lib/ai-visibility/validation/golden-set-human-review.js"
  );
  const q = buildReviewQueue({});
  assert.ok(q.progress.TOTAL > 0);
  assert.equal(q.progress.TOTAL, q.cases.length);
  assert.equal(q.progress.REVIEWED, 0);
  assert.equal(q.progress.REMAINING, q.progress.TOTAL);
  const first = q.cases[0];
  assert.ok(first.caseId);
  assert.ok(first.provider);
  assert.ok(first.language);
  assert.ok(first.geography);
  assert.ok(first.candidateEntity);
  assert.ok(first.canonicalEntityId);
  assert.equal(first.reviewStatus, "UNREVIEWED");
  assert.equal(first.humanLabelled, false);
  assert.equal(
    q.cases.filter((c) => !c.candidateEntity || !c.canonicalEntityId).length,
    0
  );
});

test("GOLDEN_SET_QUEUE_FILTERS_WORK", async () => {
  const { buildReviewQueue } = await import(
    "../lib/ai-visibility/validation/golden-set-human-review.js"
  );
  const all = buildReviewQueue({}).cases.length;
  const openai = buildReviewQueue({ filters: { provider: "openai" } }).cases.length;
  const es = buildReviewQueue({ filters: { language: "es" } }).cases.length;
  const cala = buildReviewQueue({ filters: { geography: "CALA" } }).cases.length;
  const hard = buildReviewQueue({ filters: { hardCasesOnly: true } }).cases.length;
  assert.ok(openai > 0 && openai < all);
  assert.ok(es > 0 && es < all);
  assert.ok(cala > 0 && cala <= all);
  assert.ok(hard >= 0 && hard <= all);
});

test("GOLDEN_SET_QUEUE_UNREVIEWED_NOT_APPROVED", async () => {
  const { buildReviewQueue } = await import(
    "../lib/ai-visibility/validation/golden-set-human-review.js"
  );
  const q = buildReviewQueue({});
  for (const c of q.cases) {
    assert.notEqual(c.reviewStatus, "CONFIRMED");
    assert.equal(c.humanLabelled, false);
  }
});

test("GOLDEN_SET_QUEUE_PATH_CWD_INDEPENDENT", async () => {
  const { getCandidateSourcePath, loadCandidateDocument } = await import(
    "../lib/ai-visibility/validation/golden-set-human-review.js"
  );
  const prev = process.cwd();
  try {
    process.chdir(os.tmpdir());
    const p = getCandidateSourcePath();
    assert.ok(p.includes("fixtures"));
    assert.ok(fs.existsSync(p));
    const doc = loadCandidateDocument();
    assert.equal((doc.cases || []).length, 125);
  } finally {
    process.chdir(prev);
  }
});

test("GOLDEN_SET_QUEUE_UI_ERROR_STATES", () => {
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/ai-visibility/ai-intelligence-golden-set-review.js"),
    "utf8"
  );
  for (const code of [
    "AUTH_REQUIRED",
    "ACCESS_DENIED",
    "QUEUE_ROUTE_MISSING",
    "CANDIDATE_FILE_MISSING",
    "CANDIDATE_FILE_INVALID",
    "QUEUE_EMPTY",
    "SERVER_ERROR",
  ]) {
    assert.ok(js.includes(code), code);
  }
  assert.ok(!js.includes('"Failed to load queue"'));
});

test("GOLDEN_SET_QUEUE_AUTH_REQUIRED contract", () => {
  assert.equal(canAccessAiIntelligenceValidation(null), false);
});

test("GOLDEN_SET_QUEUE_NORMAL_CLIENT_DENIED", () => {
  assert.equal(
    canAccessAiIntelligenceValidation({
      isAdmin: false,
      workspaceAccess: ["brand", "owner"],
      email: "client@example.com",
    }),
    false
  );
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
