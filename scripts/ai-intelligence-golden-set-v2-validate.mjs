#!/usr/bin/env node
/**
 * Golden Set v2 post-promotion validation report.
 * LIVE_PROVIDER_CALLS: 0. CLASSIFIER_CHANGES: 0. No deploys / Airtable.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet, scoreGoldenSet } from "../lib/ai-visibility/validation/golden-set.js";
import { evaluateClassificationThreshold } from "../lib/ai-visibility/validation/classification-threshold.js";
import { buildLearningReport } from "../lib/ai-visibility/validation/golden-set-review-learning.js";
import { getReviewProgress } from "../lib/ai-visibility/validation/golden-set-human-review.js";
import { runAiIntelligenceValidation } from "../lib/ai-visibility/validation/run-validation.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "data/ai-visibility/validation");
const OUT_REPORT = path.join(OUT_DIR, "golden-set-v2-post-promotion-validation.json");

function pct(n) {
  if (n == null || Number.isNaN(n)) return null;
  return Math.round(n * 10000) / 10000;
}

function mapErrorPattern(err) {
  const exp = String(err.EXPECTED || err.HUMAN_EXPECTED || "");
  const act = String(err.ACTUAL || err.CLASSIFIER_ACTUAL || "");
  const et = err.ERROR_TYPE || "";
  if (et === "alias_resolution") return "ALIAS_RESOLUTION_ERROR";
  if (et === "question_status") return "QUESTION_STATUS_MISMATCH";
  if (et === "ranking_first_recommendation" || exp === "first_recommendation" || act === "first_recommendation") {
    return "FIRST_RECOMMENDATION_MISSED";
  }
  if (exp === "ranked_recommendation" || act === "ranked_recommendation") return "RANKED_RECOMMENDATION_ERROR";
  if (exp === "explicit_recommendation" || act === "explicit_recommendation") {
    return "EXPLICIT_RECOMMENDATION_ERROR";
  }
  if (exp === "associated_option" || act === "associated_option") return "ASSOCIATED_OPTION_ERROR";
  if (exp === "comparator" || act === "comparator") return "COMPARATOR_ERROR";
  if (exp === "discussed" || act === "discussed") return "DISCUSSION_ERROR";
  if (exp === "passing_mention" || act === "passing_mention") return "PASSING_MENTION_ERROR";
  if (exp === "negative_or_qualified" || act === "negative_or_qualified") {
    return "NEGATIVE_OR_QUALIFIED_ERROR";
  }
  if (exp === "source_only" || act === "source_only") return "SOURCE_ONLY_ERROR";
  const positive = new Set([
    "first_recommendation",
    "ranked_recommendation",
    "explicit_recommendation",
  ]);
  if (positive.has(exp) && (act === "discussed" || act === "passing_mention" || !act)) {
    return "RECOMMENDATION_UNDERCLASSIFICATION";
  }
  if (positive.has(act) && (exp === "discussed" || exp === "passing_mention" || exp === "associated_option")) {
    return "RECOMMENDATION_OVERCLASSIFICATION";
  }
  return "OTHER";
}

function humanCoverage(cases) {
  const bump = (obj, k) => {
    const key = k == null || k === "" ? "unspecified" : String(k);
    obj[key] = (obj[key] || 0) + 1;
  };
  const out = {
    PROVIDER: {},
    LANGUAGE: {},
    GEOGRAPHY: {},
    CASE_TYPE: {},
    QUESTION_STATUS: {},
    CITATION_ASSOCIATION: {},
  };
  for (const c of cases) {
    bump(out.PROVIDER, c.provider);
    bump(out.LANGUAGE, c.language);
    bump(out.GEOGRAPHY, c.geography);
    bump(out.CASE_TYPE, c.caseType || c.expectedRecommendationRole);
    bump(out.QUESTION_STATUS, c.expectedQuestionStatus);
    bump(out.CITATION_ASSOCIATION, c.expectedCitationAssociation);
  }
  return out;
}

function improvementFromPatterns(byPattern) {
  return Object.values(byPattern).map((p) => ({
    STATUS: "REVIEW_REQUIRED",
    DO_NOT_APPLY: true,
    OBSERVED_PATTERN: p.PATTERN,
    HUMAN_GROUND_TRUTH_COUNT: p.CASE_COUNT,
    CURRENT_CLASSIFIER_BEHAVIOR: p.COMMON_PATTERN || p.PATTERN,
    PROPOSED_GENERAL_RULE: p.POTENTIAL_RULE_GAP || "Review deterministic classifier rules for this pattern; do not auto-apply.",
    AFFECTED_CASES: (p.CASE_IDS || []).slice(0, 40),
    AFFECTED_CASE_COUNT: p.CASE_COUNT,
    PROVIDERS: p.PROVIDERS,
    LANGUAGES: p.LANGUAGES,
    GEOGRAPHIES: p.GEOGRAPHIES,
    EXPECTED_REGRESSION_SURFACE:
      "Any change must re-score full Golden Set v2 + holdout; protect Confirmed matches and v1 cases.",
  }));
}

const golden = loadGoldenSet();
const score = scoreGoldenSet(golden);
const threshold = evaluateClassificationThreshold(score, {
  coverage: score.coverage || golden.coverage,
  subgroupMetrics: score.subgroupMetrics,
});
score.thresholdEvaluation = threshold;
score.threshold = threshold.THRESHOLD_STATUS;

const coverageHuman = humanCoverage(golden.cases || []);
const learning = buildLearningReport({ write: true });

const enrichedErrors = (score.errors || []).map((e) => {
  const pattern = mapErrorPattern(e);
  return {
    CASE_ID: e.CASE_ID,
    PROVIDER: e.PROVIDER,
    LANGUAGE: e.LANGUAGE,
    GEOGRAPHY: e.GEOGRAPHY,
    PROMPT_FAMILY: e.PROMPT_FAMILY || null,
    ENTITY: e.ENTITY || null,
    HUMAN_EXPECTED: e.EXPECTED,
    CLASSIFIER_ACTUAL: e.ACTUAL,
    ERROR_FIELD:
      e.ERROR_TYPE === "question_status"
        ? "questionStatus"
        : e.ERROR_TYPE === "alias_resolution"
          ? "entityPresent"
          : "recommendationStatus",
    ERROR_TYPE: e.ERROR_TYPE,
    PATTERN: pattern,
    ROOT_CAUSE: e.ROOT_CAUSE,
  };
});

const byPattern = {};
for (const e of enrichedErrors) {
  const p = e.PATTERN || "OTHER";
  if (!byPattern[p]) {
    byPattern[p] = {
      PATTERN: p,
      COUNT: 0,
      CASE_IDS: [],
      PROVIDERS: new Set(),
      LANGUAGES: new Set(),
      GEOGRAPHIES: new Set(),
      COMMON_PATTERN: p,
      CURRENT_RULE: "production recommendation-classifier-v3 + entity resolver",
      POTENTIAL_RULE_GAP: `Human labels disagree with classifier for ${p}. Propose deterministic rule refinement only after review.`,
    };
  }
  byPattern[p].COUNT += 1;
  byPattern[p].CASE_IDS.push(e.CASE_ID);
  if (e.PROVIDER) byPattern[p].PROVIDERS.add(e.PROVIDER);
  if (e.LANGUAGE) byPattern[p].LANGUAGES.add(e.LANGUAGE);
  if (e.GEOGRAPHY) byPattern[p].GEOGRAPHIES.add(e.GEOGRAPHY);
}
for (const p of Object.values(byPattern)) {
  p.PROVIDERS = [...p.PROVIDERS];
  p.LANGUAGES = [...p.LANGUAGES];
  p.GEOGRAPHIES = [...p.GEOGRAPHIES];
}

const byField = {};
for (const e of enrichedErrors) {
  byField[e.ERROR_FIELD] = (byField[e.ERROR_FIELD] || 0) + 1;
}

const progress = getReviewProgress();
const improvements = improvementFromPatterns(byPattern);

const sampleOk = (score.CASE_COUNT || 0) >= 150;
const subgroupOk = !!threshold.SUBGROUP_COVERAGE_SUFFICIENT;
const metricsPass = threshold.THRESHOLD_STATUS === "PASS" || threshold.THRESHOLD_STATUS === "PASS_PROVISIONAL";
// Read actual statuses from threshold eval
const threshStatus = threshold.THRESHOLD_STATUS;
const aggregateFail = Object.values(threshold.checks || {}).some((c) => c && c.pass === false);

let release = "NOT_SAFE";
if (score.CASE_COUNT >= 150 && !aggregateFail && (score.ERROR_COUNT || 0) === 0 && subgroupOk) {
  release = "SAFE_FOR_PRODUCTION_CLIENT_REPORTING";
} else if (score.CASE_COUNT >= 150 && (score.RECOMMENDATION_CLASSIFICATION_ACCURACY || 0) >= 0.9) {
  release = "SAFE_FOR_CONTROLLED_CLIENT_DEMO";
} else if (score.CASE_COUNT >= 65) {
  release = "SAFE_FOR_INTERNAL_QA";
}
// With CORRECTED-heavy set, classification will miss 98% — force honest gate
if (aggregateFail || threshStatus === "FAIL" || threshStatus === "REVIEW") {
  if ((score.RECOMMENDATION_CLASSIFICATION_ACCURACY || 0) >= 0.85 && score.CASE_COUNT >= 150) {
    release = "SAFE_FOR_INTERNAL_QA";
  } else if (score.CASE_COUNT >= 150) {
    release = "SAFE_FOR_INTERNAL_QA";
  } else {
    release = "NOT_SAFE";
  }
}
// Never claim production if thresholds fail
if (threshStatus !== "PASS" && release === "SAFE_FOR_PRODUCTION_CLIENT_REPORTING") {
  release = "SAFE_FOR_CONTROLLED_CLIENT_DEMO";
}
if (aggregateFail) {
  release =
    score.CASE_COUNT >= 150 ? "SAFE_FOR_INTERNAL_QA" : "NOT_SAFE";
}

const report = {
  version: "ai_intelligence_golden_set_v2_post_promotion_validation_v1",
  generatedAt: new Date().toISOString(),
  promotion: {
    REVIEWED: progress.REVIEWED,
    PROMOTABLE: progress.PROMOTABLE,
    PROMOTED: golden.casesPromotedFromReview ?? progress.PROMOTABLE,
    DEFERRED: progress.DEFERRED,
    REJECTED: 0,
    V1_SIZE: golden.casesFromV1 ?? 65,
    V2_NEW_CASES: golden.casesPromotedFromReview ?? null,
    V2_TOTAL_SIZE: score.CASE_COUNT,
    HUMAN_LABELLED: score.HUMAN_LABELLED,
    LLM_GROUND_TRUTH: score.LLM_LABELLED_AS_GROUND_TRUTH,
    CONFIRMED: progress.CONFIRMED,
    CORRECTED: progress.CORRECTED,
  },
  coverage: coverageHuman,
  holdout: score.HOLDOUT,
  classification: {
    ENTITY_PRECISION: pct(score.ENTITY_RESOLUTION_PRECISION),
    ENTITY_RECALL: pct(score.ENTITY_RESOLUTION_RECALL),
    ENTITY_F1: pct(score.ENTITY_RESOLUTION_F1),
    RECOMMENDATION_ACCURACY: pct(score.RECOMMENDATION_CLASSIFICATION_ACCURACY),
    RECOMMENDATION_PRECISION: pct(score.RECOMMENDATION_PRECISION),
    RECOMMENDATION_RECALL: pct(score.RECOMMENDATION_RECALL),
    RECOMMENDATION_F1: pct(score.RECOMMENDATION_F1),
    FIRST_RECOMMENDATION_ACCURACY: pct(score.FIRST_RECOMMENDATION_ACCURACY),
    QUESTION_STATUS_ACCURACY: pct(score.QUESTION_STATUS_ACCURACY),
    CITATION_ASSOCIATION_PRECISION: pct(score.CITATION_ASSOCIATION_PRECISION),
    CITATION_ASSOCIATION_RECALL: pct(score.CITATION_ASSOCIATION_RECALL),
    CITATION_ASSOCIATION_F1: pct(score.CITATION_ASSOCIATION_F1),
    CITATION_STATUS:
      (score.CITATION_ASSOCIATION_LABEL_COUNT || 0) >= 10
        ? "PARTIAL_HUMAN_LABELS_CLASSIFIER_NOT_SCORED"
        : "NOT_YET_GOVERNED",
  },
  subgroups: score.subgroupMetrics,
  errors: {
    TOTAL: score.ERROR_COUNT,
    BY_FIELD: byField,
    BY_PATTERN: Object.fromEntries(
      Object.entries(byPattern).map(([k, v]) => [k, { ...v, CASE_IDS: v.CASE_IDS.slice(0, 25) }])
    ),
    CRITICAL_ERRORS: enrichedErrors.filter(
      (e) =>
        e.PATTERN === "ALIAS_RESOLUTION_ERROR" ||
        e.PATTERN === "FIRST_RECOMMENDATION_MISSED" ||
        e.PATTERN === "RECOMMENDATION_UNDERCLASSIFICATION"
    ).length,
    inventory: enrichedErrors,
  },
  improvementCandidates: improvements,
  governance: {
    SAMPLE_SIZE_SUFFICIENT: sampleOk,
    SUBGROUP_COVERAGE_SUFFICIENT: subgroupOk,
    THRESHOLD_STATUS: threshStatus,
    THRESHOLD_GOVERNANCE: threshold.THRESHOLD_GOVERNANCE,
    CLASSIFICATION_QUALITY: threshStatus,
    HOLDOUT_STATUS: score.HOLDOUT?.HOLDOUT_CREATED ? "CREATED" : "DEFERRED",
    CHECKS: threshold.checks,
    FAILURES: threshold.failures,
  },
  releaseRecommendation: release,
  activity: {
    LIVE_PROVIDER_CALLS: 0,
    NEW_MONITORING: 0,
    PUBLIC_CRAWL_CALLS: 0,
    AUTO_HUMAN_APPROVALS: 0,
    UNREVIEWED_PROMOTIONS: 0,
    CLASSIFIER_CHANGES: 0,
    AIRTABLE_WRITES: 0,
    SCHEMA_CHANGES: 0,
    DEPLOYS: 0,
  },
  learningSummary: learning.learning || learning,
  goldenSource: golden.source,
  goldenVersion: golden.version,
};

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(OUT_REPORT, JSON.stringify(report, null, 2), "utf8");

console.log("\n=== Golden Set v2 Post-Promotion Validation ===\n");
console.log(
  JSON.stringify(
    {
      promotion: report.promotion,
      holdout: report.holdout,
      classification: report.classification,
      governance: {
        SAMPLE_SIZE_SUFFICIENT: report.governance.SAMPLE_SIZE_SUFFICIENT,
        SUBGROUP_COVERAGE_SUFFICIENT: report.governance.SUBGROUP_COVERAGE_SUFFICIENT,
        THRESHOLD_STATUS: report.governance.THRESHOLD_STATUS,
        HOLDOUT_STATUS: report.governance.HOLDOUT_STATUS,
        FAILURES: report.governance.FAILURES,
      },
      errors: {
        TOTAL: report.errors.TOTAL,
        BY_FIELD: report.errors.BY_FIELD,
        BY_PATTERN_COUNTS: Object.fromEntries(
          Object.entries(report.errors.BY_PATTERN).map(([k, v]) => [k, v.COUNT])
        ),
        CRITICAL_ERRORS: report.errors.CRITICAL_ERRORS,
      },
      improvementCandidateCount: report.improvementCandidates.length,
      releaseRecommendation: report.releaseRecommendation,
      wrote: OUT_REPORT,
    },
    null,
    2
  )
);

// Refresh main validation scorecard (deterministic only)
const full = await runAiIntelligenceValidation({ writeFiles: true });
console.log("\nScorecard recommendation:", full.recommendation?.status, full.recommendation?.detail);
console.log("Golden n in scorecard:", full.goldenSet?.CASE_COUNT);

const finalStatus =
  report.releaseRecommendation === "SAFE_FOR_PRODUCTION_CLIENT_REPORTING"
    ? "AI_INTELLIGENCE_GOLDEN_SET_V2_VALIDATION_PASS"
    : report.releaseRecommendation === "NOT_SAFE"
      ? "AI_INTELLIGENCE_GOLDEN_SET_V2_VALIDATION_BLOCKED"
      : "AI_INTELLIGENCE_GOLDEN_SET_V2_VALIDATION_REVIEW_REQUIRED";

console.log("\nFINAL_STATUS:", finalStatus);
process.exit(0);
