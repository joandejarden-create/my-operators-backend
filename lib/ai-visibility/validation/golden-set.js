/**
 * AI Intelligence Golden Set — human-labelled reference only.
 * Do not fabricate labels. Prefer versioned v1 fixture; fall back to Phase 2C.
 * LLM outputs are NEVER accepted as ground truth.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildAiVisibilityEntityIndex,
  extractMentions,
} from "../index.js";
import { auditGoldenCoverage } from "./golden-set-expansion.js";
import { buildGoldenSetScoringEntityIndex } from "./golden-set-entity-index.js";
import { hydrateGoldenSetCasesForScoring } from "./hydrate-golden-set-texts.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURES = path.resolve(__dirname, "../../../fixtures/ai-visibility");
const DEFAULT_GOLDEN_PATH = path.join(FIXTURES, "validation-golden-set.json");
const V2_PATH = path.join(FIXTURES, "ai-intelligence-golden-set-v2.json");
const V1_PATH = path.join(FIXTURES, "ai-intelligence-golden-set-v1.json");
const PHASE2C_GOLDEN_PATH = path.join(FIXTURES, "phase2c-classification-golden.json");

export const GOLDEN_SET_DEFAULT_VERSION = "ai_intelligence_golden_set_v0_empty";

/**
 * Normalize a case into scoring shape.
 */
function normalizeCase(c, sourceHint) {
  const text = c.rawResponseExcerpt || c.text || null;
  const entityName = c.candidateEntity || c.entityName || null;
  const expectedRole =
    c.expectedRecommendationClass || c.expectedRecommendationRole || c.expectedRole || null;
  return {
    id: c.caseId || c.id,
    caseId: c.caseId || c.id,
    text,
    entityName,
    canonicalEntityId: c.canonicalEntityId || null,
    responseId: c.responseId || c.sourceResponseId || null,
    sourceResponseId: c.sourceResponseId || c.responseId || null,
    batchId: c.batchId || null,
    promptIntentTerritory: c.promptIntentTerritory || null,
    promptFamily: c.promptFamily || c.promptIntentTerritory || null,
    expectedRecommendationRole: expectedRole,
    expectedFirstRecommendation:
      c.expectedFirstRecommendation != null
        ? c.expectedFirstRecommendation
        : expectedRole === "first_recommendation",
    expectedQuestionStatus: c.expectedQuestionStatus || null,
    expectedCitationAssociation: c.expectedCitationAssociation ?? null,
    expectedEntityPresent: c.expectedEntityPresent ?? (entityName ? true : null),
    geography: c.geography || null,
    language: c.language || null,
    provider: c.provider || null,
    model: c.model || null,
    caseType: c.caseType || null,
    hardCase: c.hardCase || false,
    humanLabelled: c.humanLabelled !== false,
    llmLabelledAsGroundTruth: c.llmLabelledAsGroundTruth === true,
    source: c.source || sourceHint || null,
    reviewStatus: c.reviewStatus || null,
    holdoutSplit: c.holdoutSplit || null,
    reviewer: c.reviewer || null,
    reviewedAt: c.reviewedAt || null,
    groundTruthInvalidated: c.groundTruthInvalidated === true,
    excludeFromClassificationDenominator: c.excludeFromClassificationDenominator === true,
  };
}

/**
 * @returns {{
 *   version: string,
 *   caseCount: number,
 *   cases: object[],
 *   source: string,
 *   lastValidatedAt: string|null,
 *   note?: string|null,
 *   coverage?: object,
 *   previousVersion?: string|null,
 *   humanLabelled?: number,
 *   llmLabelledAsGroundTruth?: number
 * }}
 */
export function loadGoldenSet(options = {}) {
  const preferPath = options.filePath || null;
  // Prefer human-labelled v2 when present; never load pending candidates as ground truth.
  const candidates = preferPath
    ? [preferPath]
    : [DEFAULT_GOLDEN_PATH, V2_PATH, V1_PATH, PHASE2C_GOLDEN_PATH];

  for (const filePath of candidates) {
    if (!fs.existsSync(filePath)) continue;
    const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
    let cases = Array.isArray(raw.cases) ? raw.cases : [];

    // Only human-labelled cases are ground truth — never candidates / pending review
    if (
      filePath.includes("ai-intelligence-golden-set-v2") ||
      filePath.includes("ai-intelligence-golden-set-v1") ||
      raw.version === "ai_intelligence_golden_set_v2" ||
      raw.version === "ai_intelligence_golden_set_v1"
    ) {
      cases = cases.filter(
        (c) =>
          c.humanLabelled !== false &&
          c.llmLabelledAsGroundTruth !== true &&
          c.reviewStatus !== "PENDING_HUMAN_REVIEW" &&
          c.reviewStatus !== "UNREVIEWED" &&
          c.reviewStatus !== "DEFERRED" &&
          // Invalidated candidate subjects remain in fixture for audit but are
          // excluded from classification denominators / active evaluation.
          c.reviewStatus !== "INVALIDATED_CANDIDATE_SUBJECT" &&
          c.groundTruthInvalidated !== true &&
          c.excludeFromClassificationDenominator !== true
      );
    } else if (filePath.includes("phase2c-classification-golden")) {
      cases = cases.map((c) =>
        normalizeCase(
          {
            ...c,
            humanLabelled: true,
            llmLabelledAsGroundTruth: false,
            expectedRecommendationClass: c.expectedRole,
          },
          "phase2c_human_labelled"
        )
      );
      return {
        version: `ai_intelligence_golden_set_from_phase2c_v${raw.version || "1"}`,
        previousVersion: null,
        caseCount: cases.length,
        cases,
        source: filePath,
        lastValidatedAt: null,
        humanLabelled: cases.length,
        llmLabelledAsGroundTruth: 0,
        coverage: auditGoldenCoverage(cases),
        note:
          "Human-labelled Phase 2C classification golden (not LLM-generated).",
      };
    } else if (filePath.includes("validation-golden-set") && cases.length === 0) {
      continue;
    }

    if (!cases.length) continue;

    const normalized = cases.map((c) => normalizeCase(c, path.basename(filePath)));
    const llmGt = normalized.filter((c) => c.llmLabelledAsGroundTruth).length;
    return {
      version: raw.version || GOLDEN_SET_DEFAULT_VERSION,
      previousVersion: raw.previousVersion || null,
      caseCount: normalized.length,
      cases: normalized,
      source: filePath,
      lastValidatedAt: raw.lastValidatedAt || raw.reviewDate || null,
      humanLabelled: raw.humanLabelled ?? normalized.filter((c) => c.humanLabelled).length,
      llmLabelledAsGroundTruth: llmGt,
      coverage: raw.coverageAudit || auditGoldenCoverage(normalized),
      holdout: raw.holdout || null,
      casesFromV1: raw.casesFromV1 ?? null,
      casesPromotedFromReview: raw.casesPromotedFromReview ?? null,
      note: raw.note || null,
    };
  }

  return {
    version: GOLDEN_SET_DEFAULT_VERSION,
    previousVersion: null,
    caseCount: 0,
    cases: [],
    source: "missing_fixture",
    lastValidatedAt: null,
    humanLabelled: 0,
    llmLabelledAsGroundTruth: 0,
    coverage: auditGoldenCoverage([]),
    note: "Human-labelled Golden Set not yet authored. Classification quality NOT_YET_RUN.",
  };
}

function f1(p, r) {
  if (p == null || r == null) return null;
  if (p + r === 0) return null;
  return (2 * p * r) / (p + r);
}

function classifyErrorType(expected, actual) {
  if (expected === "negative_or_qualified" || actual === "negative_or_qualified") {
    return "negative_language";
  }
  if (
    expected === "first_recommendation" ||
    actual === "first_recommendation"
  ) {
    return "ranking_first_recommendation";
  }
  if (expected === "comparator" || actual === "comparator") {
    return "recommendation_ambiguity";
  }
  if (
    expected === "discussed" ||
    expected === "passing_mention" ||
    actual === "discussed"
  ) {
    return "recommendation_ambiguity";
  }
  return "other";
}

/**
 * Score golden set with current classifier (system vs human labels).
 */
export function scoreGoldenSet(golden, systemResolver) {
  const cases = (golden?.cases || []).filter((c) => {
    if (c.llmLabelledAsGroundTruth) return false;
    if (c.reviewStatus === "PENDING_HUMAN_REVIEW") return false;
    if (c.humanLabelled === false) return false;
    return true;
  });

  if (!cases.length) {
    return {
      GOLDEN_SET_VERSION: golden?.version || GOLDEN_SET_DEFAULT_VERSION,
      CASE_COUNT: 0,
      ENTITY_RESOLUTION_PRECISION: null,
      ENTITY_RESOLUTION_RECALL: null,
      ENTITY_RESOLUTION_F1: null,
      RECOMMENDATION_CLASSIFICATION_ACCURACY: null,
      RECOMMENDATION_PRECISION: null,
      RECOMMENDATION_RECALL: null,
      RECOMMENDATION_F1: null,
      FIRST_RECOMMENDATION_ACCURACY: null,
      QUESTION_STATUS_ACCURACY: null,
      CITATION_ASSOCIATION_PRECISION: null,
      CITATION_ASSOCIATION_RECALL: null,
      ERROR_COUNT: 0,
      FALSE_POSITIVES: 0,
      FALSE_NEGATIVES: 0,
      errors: [],
      errorsByCategory: {},
      subgroupMetrics: null,
      coverage: golden?.coverage || auditGoldenCoverage([]),
      status: "NOT_YET_RUN",
      sampleSize: 0,
      lastValidatedAt: golden?.lastValidatedAt || null,
      note: golden?.note || "Manual Golden Set not yet run.",
      HUMAN_LABELLED: 0,
      LLM_LABELLED_AS_GROUND_TRUTH: golden?.llmLabelledAsGroundTruth || 0,
      HOLDUT: { HOLDOUT_CREATED: false, reason: "HOLDOUT_DEFERRED" },
    };
  }

  const resolver =
    typeof systemResolver === "function" ? systemResolver : buildDefaultClassifierResolver();

  let entTp = 0,
    entFp = 0,
    entFn = 0;
  let recTp = 0,
    recFp = 0,
    recFn = 0;
  let recOk = 0,
    recN = 0;
  let firstOk = 0,
    firstN = 0;
  let qOk = 0,
    qN = 0;
  let citTp = 0,
    citFp = 0,
    citFn = 0;
  let errorCount = 0;
  const errors = [];
  const errorsByCategory = {};

  /** @type {Record<string, Record<string, object>>} */
  const subgroupAcc = {
    PROVIDER: {},
    LANGUAGE: {},
    GEOGRAPHY: {},
    CASE_TYPE: {},
  };

  function ensureSub(dim, key) {
    const k = key || "unspecified";
    if (!subgroupAcc[dim][k]) {
      subgroupAcc[dim][k] = {
        CASE_COUNT: 0,
        recOk: 0,
        recN: 0,
        firstOk: 0,
        firstN: 0,
        entTp: 0,
        entFp: 0,
        entFn: 0,
      };
    }
    return subgroupAcc[dim][k];
  }

  for (const c of cases) {
    const sys = resolver(c) || {};
    const subRows = [
      ensureSub("PROVIDER", c.provider),
      ensureSub("LANGUAGE", c.language),
      ensureSub("GEOGRAPHY", c.geography),
      ensureSub("CASE_TYPE", c.caseType || c.expectedRecommendationRole),
    ];
    for (const row of subRows) row.CASE_COUNT += 1;

    if (c.expectedEntityPresent != null) {
      const got = !!sys.entityPresent;
      const exp = !!c.expectedEntityPresent;
      if (got && exp) {
        entTp += 1;
        for (const row of subRows) row.entTp += 1;
      } else if (got && !exp) {
        entFp += 1;
        for (const row of subRows) row.entFp += 1;
      } else if (!got && exp) {
        entFn += 1;
        for (const row of subRows) row.entFn += 1;
        errorCount += 1;
        errors.push({
          CASE_ID: c.caseId || c.id,
          PROVIDER: c.provider,
          MODEL: c.model,
          LANGUAGE: c.language,
          GEOGRAPHY: c.geography,
          PROMPT_FAMILY: c.promptFamily || c.promptIntentTerritory || null,
          ENTITY: c.entityName || null,
          CASE_TYPE: c.caseType,
          EXPECTED: "entity_present",
          ACTUAL: "entity_absent",
          ERROR_TYPE: "alias_resolution",
          ROOT_CAUSE:
            "Classifier did not resolve expected entity mention (check excerpt truncation / alias coverage)",
          SAFE_FIX_AVAILABLE: false,
        });
        errorsByCategory.alias_resolution = (errorsByCategory.alias_resolution || 0) + 1;
      }
    } else if (c.entityName) {
      if (sys.entityPresent) {
        entTp += 1;
        for (const row of subRows) row.entTp += 1;
      } else {
        entFn += 1;
        for (const row of subRows) row.entFn += 1;
        errorCount += 1;
        const err = {
          CASE_ID: c.caseId || c.id,
          PROVIDER: c.provider,
          MODEL: c.model,
          LANGUAGE: c.language,
          GEOGRAPHY: c.geography,
          PROMPT_FAMILY: c.promptFamily || c.promptIntentTerritory || null,
          ENTITY: c.entityName || null,
          CASE_TYPE: c.caseType,
          EXPECTED: "entity_present",
          ACTUAL: "entity_absent",
          ERROR_TYPE: "alias_resolution",
          ROOT_CAUSE: "Classifier did not resolve expected entity mention",
          SAFE_FIX_AVAILABLE: false,
        };
        errors.push(err);
        errorsByCategory.alias_resolution = (errorsByCategory.alias_resolution || 0) + 1;
      }
    }

    if (c.expectedRecommendationRole != null) {
      recN += 1;
      const match = sys.role === c.expectedRecommendationRole;
      for (const row of subRows) {
        row.recN += 1;
        if (match) row.recOk += 1;
      }
      if (match) {
        recOk += 1;
        recTp += 1;
      } else {
        recFn += 1;
        if (sys.role) recFp += 1;
        errorCount += 1;
        const et = classifyErrorType(c.expectedRecommendationRole, sys.role);
        errors.push({
          CASE_ID: c.caseId || c.id,
          PROVIDER: c.provider,
          MODEL: c.model,
          LANGUAGE: c.language,
          GEOGRAPHY: c.geography,
          PROMPT_FAMILY: c.promptFamily || c.promptIntentTerritory || null,
          ENTITY: c.entityName || null,
          CASE_TYPE: c.caseType,
          EXPECTED: c.expectedRecommendationRole,
          ACTUAL: sys.role,
          ERROR_TYPE: et,
          ROOT_CAUSE: `Recommendation class mismatch: expected ${c.expectedRecommendationRole}, got ${sys.role}`,
          SAFE_FIX_AVAILABLE: false,
        });
        errorsByCategory[et] = (errorsByCategory[et] || 0) + 1;
      }
    }

    if (c.expectedFirstRecommendation != null) {
      firstN += 1;
      const gotFirst = sys.role === "first_recommendation";
      const ok = gotFirst === !!c.expectedFirstRecommendation;
      for (const row of subRows) {
        row.firstN += 1;
        if (ok) row.firstOk += 1;
      }
      if (ok) firstOk += 1;
    }

    if (c.expectedQuestionStatus != null) {
      qN += 1;
      if (sys.questionStatus === c.expectedQuestionStatus) qOk += 1;
      else {
        errorCount += 1;
        errors.push({
          CASE_ID: c.caseId || c.id,
          PROVIDER: c.provider,
          MODEL: c.model,
          LANGUAGE: c.language,
          GEOGRAPHY: c.geography,
          PROMPT_FAMILY: c.promptFamily || c.promptIntentTerritory || null,
          ENTITY: c.entityName || null,
          CASE_TYPE: c.caseType,
          EXPECTED: c.expectedQuestionStatus,
          ACTUAL: sys.questionStatus,
          ERROR_TYPE: "question_status",
          ROOT_CAUSE: "Question status mismatch",
          SAFE_FIX_AVAILABLE: false,
        });
        errorsByCategory.question_status = (errorsByCategory.question_status || 0) + 1;
      }
    }

    if (c.expectedCitationAssociation != null && c.expectedCitationAssociation !== "citations_present_unreviewed") {
      // Human-labelled citation association only
      const got = sys.citationAssociation;
      if (got === c.expectedCitationAssociation) citTp += 1;
      else if (got && !c.expectedCitationAssociation) citFp += 1;
      else if (!got && c.expectedCitationAssociation) citFn += 1;
    }
  }

  const prec = (tp, fp) => (tp + fp ? tp / (tp + fp) : null);
  const rec = (tp, fn) => (tp + fn ? tp / (tp + fn) : null);

  const erP = prec(entTp, entFp);
  const erR = rec(entTp, entFn);
  const recP = prec(recTp, recFp);
  const recR = rec(recTp, recFn);
  const citP = citTp + citFp ? prec(citTp, citFp) : null;
  const citR = citTp + citFn ? rec(citTp, citFn) : null;

  const subgroupMetrics = {};
  for (const [dim, rows] of Object.entries(subgroupAcc)) {
    subgroupMetrics[dim] = {};
    for (const [key, row] of Object.entries(rows)) {
      const p = prec(row.entTp, row.entFp);
      const r = rec(row.entTp, row.entFn);
      subgroupMetrics[dim][key] = {
        CASE_COUNT: row.CASE_COUNT,
        ENTITY_RESOLUTION_PRECISION: p,
        ENTITY_RESOLUTION_RECALL: r,
        RECOMMENDATION_CLASSIFICATION_ACCURACY: row.recN ? row.recOk / row.recN : null,
        RECOMMENDATION_PRECISION: row.recN ? row.recOk / row.recN : null,
        RECOMMENDATION_RECALL: row.recN ? row.recOk / row.recN : null,
        FIRST_RECOMMENDATION_ACCURACY: row.firstN ? row.firstOk / row.firstN : null,
      };
    }
  }

  const coverage = golden?.coverage || auditGoldenCoverage(cases);
  const holdoutMeta = golden?.holdout || null;
  const holdoutCreated =
    holdoutMeta?.HOLDOUT_CREATED === true ||
    cases.some((c) => c.holdoutSplit === "holdout");

  return {
    GOLDEN_SET_VERSION: golden.version,
    PREVIOUS_VERSION: golden.previousVersion || null,
    CASE_COUNT: cases.length,
    HUMAN_LABELLED: cases.length,
    LLM_LABELLED_AS_GROUND_TRUTH: 0,
    ENTITY_RESOLUTION_PRECISION: erP,
    ENTITY_RESOLUTION_RECALL: erR,
    ENTITY_RESOLUTION_F1: f1(erP, erR),
    RECOMMENDATION_CLASSIFICATION_ACCURACY: recN ? recOk / recN : null,
    RECOMMENDATION_PRECISION: recP,
    RECOMMENDATION_RECALL: recR,
    RECOMMENDATION_F1: f1(recP, recR),
    FIRST_RECOMMENDATION_ACCURACY: firstN ? firstOk / firstN : null,
    QUESTION_STATUS_ACCURACY: qN ? qOk / qN : null,
    QUESTION_STATUS_LABEL_COUNT: qN,
    CITATION_ASSOCIATION_PRECISION: citP,
    CITATION_ASSOCIATION_RECALL: citR,
    CITATION_ASSOCIATION_F1: f1(citP, citR),
    CITATION_ASSOCIATION_LABEL_COUNT: citTp + citFp + citFn,
    ERROR_COUNT: errorCount,
    FALSE_POSITIVES: recFp + entFp,
    FALSE_NEGATIVES: recFn + entFn,
    errors,
    errorsByCategory,
    subgroupMetrics,
    coverage,
    status: "SCORED",
    sampleSize: cases.length,
    lastValidatedAt: new Date().toISOString(),
    note: golden.note || null,
    threshold: "Threshold Not Yet Governed",
    HOLDOUT: holdoutCreated
      ? {
          HOLDOUT_CREATED: true,
          DEVELOPMENT_N:
            holdoutMeta?.DEVELOPMENT_N ??
            cases.filter((c) => c.holdoutSplit !== "holdout").length,
          HOLDOUT_N:
            holdoutMeta?.HOLDOUT_N ??
            cases.filter((c) => c.holdoutSplit === "holdout").length,
          HOLDOUT_RESULTS: null,
          STRATIFICATION_STATUS:
            holdoutMeta?.STRATIFICATION_STATUS || "STRATIFIED_BY_PROVIDER_LANGUAGE_CASE_TYPE",
          VERSION: holdoutMeta?.VERSION || "ai_intelligence_holdout_v1",
          reason: "Holdout reserved for future tuning — not used to adjust classifier in this phase.",
        }
      : {
          HOLDOUT_CREATED: false,
          DEVELOPMENT_N: null,
          HOLDOUT_N: null,
          HOLDOUT_RESULTS: null,
          reason: "HOLDOUT_DEFERRED — expand and human-review to ≥150 before splitting holdout",
        },
  };
}

function questionStatusFromRole(role, entityPresent) {
  if (!entityPresent) return "MISSING";
  if (role === "first_recommendation") return "FIRST_RECOMMENDED";
  if (
    role === "ranked_recommendation" ||
    role === "explicit_recommendation"
  ) {
    return "RECOMMENDED";
  }
  if (role === "negative_or_qualified") return "NEGATIVE_OR_NOT_RECOMMENDED";
  if (role === "associated_option" || role === "passing_mention") {
    return "PRESENT";
  }
  if (role === "discussed" || role === "comparator" || role === "source_only") {
    return "DISCUSSION_ONLY";
  }
  if (entityPresent) return "PRESENT";
  return "NOT_APPLICABLE";
}

function buildDefaultClassifierResolver() {
  const indexBundle = buildGoldenSetScoringEntityIndex({});
  const aliasIndex = indexBundle.aliasIndex;
  const roleRank = new Map(
    [
      "negative_or_qualified",
      "first_recommendation",
      "ranked_recommendation",
      "explicit_recommendation",
      "associated_option",
      "comparator",
      "discussed",
      "passing_mention",
      "source_only",
      "no_mention",
    ].map((r, i) => [r, i])
  );

  return (c) => {
    if (!c?.text || !c?.entityName) return {};
    const mentions = extractMentions({
      responseId: `resp_${c.id || c.caseId || "case"}`,
      text: c.text,
      entityIndex: aliasIndex,
      promptIntentTerritory: c.promptIntentTerritory || c.promptFamily,
    });
    const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
    const entityPresent = hits.length > 0;
    let role = null;
    if (hits.length) {
      role = hits.slice().sort((a, b) => {
        const ar = roleRank.get(a.role) ?? 99;
        const br = roleRank.get(b.role) ?? 99;
        if (ar !== br) return ar - br;
        return (a.mentionPosition ?? 0) - (b.mentionPosition ?? 0);
      })[0].role;
    }
    return {
      entityPresent,
      role,
      questionStatus: questionStatusFromRole(role, entityPresent),
      citationAssociation: null,
    };
  };
}

/**
 * Score Golden Set after hydrating full monitoring-store response text.
 * Does not change human labels. Prefer for Classifier Hardening.
 */
export async function scoreGoldenSetHydrated(golden, options = {}) {
  const holdoutPolicy = options.holdoutPolicy || "include"; // include | exclude | holdout_only
  let cases = [...(golden?.cases || [])];
  if (holdoutPolicy === "exclude") {
    cases = cases.filter((c) => c.holdoutSplit !== "holdout");
  } else if (holdoutPolicy === "holdout_only") {
    cases = cases.filter((c) => c.holdoutSplit === "holdout");
  }

  const { cases: hydrated, stats } = await hydrateGoldenSetCasesForScoring(cases, {
    store: options.store,
  });
  const score = scoreGoldenSet(
    { ...golden, cases: hydrated, caseCount: hydrated.length },
    options.systemResolver
  );
  return {
    ...score,
    hydrationStats: stats,
    holdoutPolicy,
    HOLDOUT_ACCESSED: holdoutPolicy === "holdout_only",
    HOLDOUT_CASES_INSPECTED: holdoutPolicy === "holdout_only" ? hydrated.length : 0,
    HOLDOUT_METRICS_RUN: holdoutPolicy === "holdout_only",
  };
}
