/**
 * Golden Set human-review workflow.
 * SYSTEM_SUGGESTION never becomes ground truth. Auto-approval forbidden.
 * LIVE_PROVIDER_CALLS: 0. AIRTABLE_WRITES: 0.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createBrandAiVisibilityReadStore } from "../storage/index.js";
import {
  GOLDEN_SET_V1_VERSION,
  GOLDEN_SET_V2_CANDIDATES_VERSION,
  QUESTION_STATUS_TAXONOMY,
  auditGoldenCoverage,
  OUT_CANDIDATES,
  OUT_V1,
} from "./golden-set-expansion.js";
import { resolveValidationStorageRoot } from "./validation-storage-root.js";
import {
  getActiveGoldenSetReviewCandidates,
  isActiveReviewCandidate,
  assertCaseIsActiveForReview,
  summarizeCandidatePopulation,
  SUPERSEDED_INVALID_SUBJECT,
} from "./golden-set-active-candidates.js";
import {
  getGoldenSetReviewState,
  resolveReviewStateFilter,
  summarizeReviewStateBuckets,
  loadInvalidatedCandidateIdSet,
  REVIEW_STATE_BUCKET,
  INVALIDATED_CANDIDATE_SUBJECT,
} from "./golden-set-review-state.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURES = path.resolve(__dirname, "../../../fixtures/ai-visibility");
const V2_PATH = path.join(FIXTURES, "ai-intelligence-golden-set-v2.json");

export const GOLDEN_SET_V2_VERSION = "ai_intelligence_golden_set_v2";
export const HUMAN_REVIEW_STORE_VERSION = "ai_intelligence_golden_set_human_review_v1";

export const RECOMMENDATION_STATUS_TAXONOMY = Object.freeze([
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
  "associated_option",
  "comparator",
  "discussed",
  "passing_mention",
  "negative_or_qualified",
  "source_only",
  "no_mention",
]);

export const CITATION_ASSOCIATION_TAXONOMY = Object.freeze([
  "ASSOCIATED",
  "NOT_ASSOCIATED",
  "UNKNOWN",
  "NOT_APPLICABLE",
]);

export const REVIEW_STATUS = Object.freeze({
  UNREVIEWED: "UNREVIEWED",
  CONFIRMED: "CONFIRMED",
  CORRECTED: "CORRECTED",
  DEFERRED: "DEFERRED",
  SECOND_REVIEW_REQUIRED: "SECOND_REVIEW_REQUIRED",
  SUPERSEDED_INVALID_SUBJECT: "SUPERSEDED_INVALID_SUBJECT",
});

export { QUESTION_STATUS_TAXONOMY };

function humanReviewRoot(options = {}) {
  const { rootDir } = resolveValidationStorageRoot(options);
  const dir = path.join(rootDir, "human-review");
  return {
    root: dir,
    reviewsDir: path.join(dir, "reviews"),
    historyDir: path.join(dir, "history"),
    indexPath: path.join(dir, "index.json"),
  };
}

function ensureDirs(paths) {
  fs.mkdirSync(paths.reviewsDir, { recursive: true });
  fs.mkdirSync(paths.historyDir, { recursive: true });
}

export function getCandidateSourcePath() {
  return OUT_CANDIDATES;
}

/**
 * Load Golden Set v2 candidates. Path is module-relative (not process.cwd()).
 * Expected shape: { version, cases: [...] } — not a bare array.
 */
export function loadCandidateDocument() {
  const absPath = OUT_CANDIDATES;
  if (!fs.existsSync(absPath)) {
    const err = new Error(
      `GOLDEN_SET_CANDIDATES_MISSING: ${absPath} (resolved from module dir, not cwd)`
    );
    err.code = "GOLDEN_SET_CANDIDATES_MISSING";
    err.path = absPath;
    throw err;
  }
  let raw;
  try {
    raw = JSON.parse(fs.readFileSync(absPath, "utf8"));
  } catch (parseErr) {
    const err = new Error(`GOLDEN_SET_CANDIDATES_INVALID: JSON parse failed at ${absPath}`);
    err.code = "GOLDEN_SET_CANDIDATES_INVALID";
    err.path = absPath;
    throw err;
  }
  if (Array.isArray(raw)) {
    const err = new Error(
      "GOLDEN_SET_CANDIDATES_INVALID: expected object with cases[] — got top-level array"
    );
    err.code = "GOLDEN_SET_CANDIDATES_INVALID";
    err.path = absPath;
    throw err;
  }
  if (!raw || typeof raw !== "object" || !Array.isArray(raw.cases)) {
    const err = new Error(
      "GOLDEN_SET_CANDIDATES_INVALID: expected { version, cases: [] } schema"
    );
    err.code = "GOLDEN_SET_CANDIDATES_INVALID";
    err.path = absPath;
    throw err;
  }
  return raw;
}

export function loadGoldenSetV1Document() {
  if (!fs.existsSync(OUT_V1)) {
    const err = new Error("GOLDEN_SET_V1_MISSING");
    err.code = "GOLDEN_SET_V1_MISSING";
    throw err;
  }
  return JSON.parse(fs.readFileSync(OUT_V1, "utf8"));
}

export function loadGoldenSetV2Document() {
  if (!fs.existsSync(V2_PATH)) return null;
  return JSON.parse(fs.readFileSync(V2_PATH, "utf8"));
}

function reviewFilePath(paths, caseId) {
  return path.join(paths.reviewsDir, `${caseId}.json`);
}

export function loadReviewRecord(caseId, options = {}) {
  const paths = humanReviewRoot(options);
  const p = reviewFilePath(paths, caseId);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function listAllReviewRecords(options = {}) {
  const paths = humanReviewRoot(options);
  if (!fs.existsSync(paths.reviewsDir)) return [];
  return fs
    .readdirSync(paths.reviewsDir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => JSON.parse(fs.readFileSync(path.join(paths.reviewsDir, f), "utf8")));
}

/**
 * Coverage gaps for governance prioritization (does not invent quotas).
 */
export function computeCoverageNeeded(options = {}) {
  const v1 = loadGoldenSetV1Document();
  const v2 = loadGoldenSetV2Document();
  const labelled = [
    ...(v1.cases || []),
    ...((v2 && v2.cases) || []).filter((c) => c.humanLabelled),
  ];
  const reviews = listAllReviewRecords(options).filter((r) =>
    [REVIEW_STATUS.CONFIRMED, REVIEW_STATUS.CORRECTED].includes(r.reviewStatus)
  );
  const candidates = loadCandidateDocument().cases || [];
  const byId = Object.fromEntries(candidates.map((c) => [c.caseId, c]));

  const promotedLike = [
    ...labelled,
    ...reviews.map((r) => ({
      provider: byId[r.caseId]?.provider || r.provider,
      language: byId[r.caseId]?.language || r.language,
      geography: byId[r.caseId]?.geography || r.geography,
      hardCase: byId[r.caseId]?.hardCase,
      expectedQuestionStatus: r.humanLabels?.questionStatus,
      expectedCitationAssociation: r.humanLabels?.citationAssociation,
    })),
  ];

  const counts = {
    PROVIDER: { openai: 0, gemini: 0, perplexity: 0, claude: 0 },
    LANGUAGE: { en: 0, es: 0 },
    GEOGRAPHY: { GLOBAL: 0, CALA: 0, MEXICO: 0, EUROPE: 0, NORTH_AMERICA: 0 },
    QUESTION_STATUS: 0,
    CITATION_ASSOCIATION: 0,
    HARD: 0,
  };
  for (const c of promotedLike) {
    const p = String(c.provider || "").toLowerCase();
    if (counts.PROVIDER[p] != null) counts.PROVIDER[p] += 1;
    const lang = String(c.language || "").toLowerCase();
    if (lang === "en" || lang === "es") counts.LANGUAGE[lang] += 1;
    const geo = String(c.geography || "").toUpperCase();
    if (counts.GEOGRAPHY[geo] != null) counts.GEOGRAPHY[geo] += 1;
    if (c.expectedQuestionStatus || c.humanLabels?.questionStatus) counts.QUESTION_STATUS += 1;
    if (c.expectedCitationAssociation || c.humanLabels?.citationAssociation) {
      counts.CITATION_ASSOCIATION += 1;
    }
    if (c.hardCase) counts.HARD += 1;
  }

  const MIN = 10;
  const needed = [];
  for (const [k, n] of Object.entries(counts.PROVIDER)) {
    if (n < MIN) needed.push({ dimension: "PROVIDER", key: k, have: n, targetHint: MIN });
  }
  for (const [k, n] of Object.entries(counts.LANGUAGE)) {
    if (n < MIN) needed.push({ dimension: "LANGUAGE", key: k, have: n, targetHint: MIN });
  }
  for (const [k, n] of Object.entries(counts.GEOGRAPHY)) {
    if (n < MIN) needed.push({ dimension: "GEOGRAPHY", key: k, have: n, targetHint: MIN });
  }
  if (counts.QUESTION_STATUS < MIN) {
    needed.push({
      dimension: "QUESTION_STATUS",
      key: "labelled",
      have: counts.QUESTION_STATUS,
      targetHint: MIN,
    });
  }
  if (counts.CITATION_ASSOCIATION < MIN) {
    needed.push({
      dimension: "CITATION_ASSOCIATION",
      key: "labelled",
      have: counts.CITATION_ASSOCIATION,
      targetHint: MIN,
    });
  }
  if (counts.HARD < MIN) {
    needed.push({ dimension: "HARD_CASE", key: "hard", have: counts.HARD, targetHint: MIN });
  }

  return {
    labelledIncludingInProgressReviews: promotedLike.length,
    v1Size: (v1.cases || []).length,
    v2Size: (v2?.cases || []).length,
    counts,
    needed,
    note: "Hints only — not equal-distribution quotas. GOVERNED requires meaningful representation.",
  };
}

function priorityScore(candidate, coverageNeeded) {
  let score = 0;
  const needed = coverageNeeded?.needed || [];
  for (const n of needed) {
    if (n.dimension === "PROVIDER" && candidate.provider === n.key) score += 100 - n.have;
    if (n.dimension === "LANGUAGE" && candidate.language === n.key) score += 80 - n.have;
    if (
      n.dimension === "GEOGRAPHY" &&
      String(candidate.geography || "").toUpperCase() === n.key
    ) {
      score += 70 - n.have;
    }
    if (n.dimension === "QUESTION_STATUS") score += 40;
    if (n.dimension === "CITATION_ASSOCIATION" && (candidate.citationCount || 0) > 0) {
      score += 35;
    }
    if (n.dimension === "HARD_CASE") {
      const sug = candidate.systemSuggestion || {};
      if (
        sug.expectedRecommendationClass === "negative_or_qualified" ||
        sug.expectedRecommendationClass === "comparator" ||
        (candidate.mentionCount || 0) >= 3
      ) {
        score += 50;
      }
    }
  }
  if (candidate.provider && candidate.provider !== "openai") score += 5;
  if (candidate.language === "es") score += 5;
  return score;
}

export function getReviewProgress(options = {}) {
  const doc = loadCandidateDocument();
  const population = summarizeCandidatePopulation(doc);
  const reviews = listAllReviewRecords(options);
  const byId = Object.fromEntries(reviews.map((r) => [r.caseId, r]));
  const invalidatedIds = loadInvalidatedCandidateIdSet(options);
  const buckets = summarizeReviewStateBuckets({
    doc,
    reviews,
    invalidatedIds,
  });

  let confirmed = 0;
  let corrected = 0;
  let deferred = 0;
  let second = 0;
  let reviewed = 0;
  const activeCases = getActiveGoldenSetReviewCandidates(doc);
  for (const c of activeCases) {
    const r = byId[c.caseId];
    if (!r || r.reviewStatus === REVIEW_STATUS.UNREVIEWED) continue;
    if (r.reviewStatus === REVIEW_STATUS.SUPERSEDED_INVALID_SUBJECT) continue;
    const bucket = getGoldenSetReviewState(
      { caseId: c.caseId, reviewStatus: r.reviewStatus },
      { invalidatedIds }
    );
    if (bucket === REVIEW_STATE_BUCKET.INVALIDATED) continue;
    reviewed += 1;
    if (r.reviewStatus === REVIEW_STATUS.CONFIRMED) confirmed += 1;
    if (r.reviewStatus === REVIEW_STATUS.CORRECTED) corrected += 1;
    if (r.reviewStatus === REVIEW_STATUS.DEFERRED) deferred += 1;
    if (r.reviewStatus === REVIEW_STATUS.SECOND_REVIEW_REQUIRED) second += 1;
  }

  return {
    candidateVersion: doc.version || GOLDEN_SET_V2_CANDIDATES_VERSION,
    // Legacy fields (kept for older clients)
    TOTAL: activeCases.length,
    REVIEWED: reviewed,
    CONFIRMED: confirmed,
    CORRECTED: corrected,
    DEFERRED: deferred,
    SECOND_REVIEW_REQUIRED: second,
    REMAINING: buckets.ACTIVE_REVIEW,
    PROMOTABLE: confirmed + corrected,
    // Current-state progress cards (preferred)
    OUTSTANDING_REVIEW: buckets.ACTIVE_REVIEW,
    COMPLETED_REVIEW: buckets.COMPLETED,
    INVALIDATED: buckets.INVALIDATED,
    SUPERSEDED: buckets.SUPERSEDED,
    TOTAL_HISTORICAL: buckets.TOTAL_HISTORICAL,
    ACTIVE_REVIEW: buckets.ACTIVE_REVIEW,
    COMPLETED: buckets.COMPLETED,
    stateBuckets: buckets,
    ZERO_ACTIVE_COMPLETE_STATE: buckets.ACTIVE_REVIEW === 0,
    storedCandidateCount: population.storedCandidateCount,
    activeReviewCandidateCount: population.activeReviewCandidateCount,
    supersededCandidateCount: population.supersededCandidateCount,
    note:
      "Outstanding Review = ACTIVE only. Completed and Invalidated are audit buckets (one primary current state each).",
  };
}

/**
 * Build review queue with filters + coverage priority.
 * Default state bucket = ACTIVE_REVIEW (completed/invalidated excluded).
 */
export function buildReviewQueue(options = {}) {
  const doc = loadCandidateDocument();
  const reviews = listAllReviewRecords(options);
  const byId = Object.fromEntries(reviews.map((r) => [r.caseId, r]));
  const coverage = computeCoverageNeeded(options);
  const filters = options.filters || {};
  const population = summarizeCandidatePopulation(doc);
  const invalidatedIds = loadInvalidatedCandidateIdSet(options);
  const stateBucket = resolveReviewStateFilter(filters);

  // Include superseded only when explicitly requested
  let sourceCases = getActiveGoldenSetReviewCandidates(doc);
  if (stateBucket === REVIEW_STATE_BUCKET.SUPERSEDED || stateBucket === "ALL") {
    const superseded = (doc.cases || []).filter(
      (c) => c.reviewStatus === SUPERSEDED_INVALID_SUBJECT || !isActiveReviewCandidate(c)
    );
    if (stateBucket === REVIEW_STATE_BUCKET.SUPERSEDED) {
      sourceCases = superseded;
    } else {
      // ALL = active subjects + superseded audit rows (dedupe by caseId)
      const seen = new Set(sourceCases.map((c) => c.caseId));
      for (const c of superseded) {
        if (!seen.has(c.caseId)) {
          seen.add(c.caseId);
          sourceCases.push(c);
        }
      }
    }
  }

  let rows = sourceCases.map((c) => {
    const review = byId[c.caseId] || null;
    const status =
      review?.reviewStatus ||
      (c.reviewStatus === "PENDING_HUMAN_REVIEW"
        ? REVIEW_STATUS.UNREVIEWED
        : c.reviewStatus || REVIEW_STATUS.UNREVIEWED);
    const hardCase =
      c.hardCase === true ||
      c.systemSuggestion?.expectedRecommendationClass === "negative_or_qualified" ||
      c.systemSuggestion?.expectedRecommendationClass === "comparator" ||
      (c.mentionCount || 0) >= 3 ||
      (c.positiveRecCount || 0) === 0;
    const reviewStateBucket = getGoldenSetReviewState(
      {
        caseId: c.caseId,
        reviewStatus: status === SUPERSEDED_INVALID_SUBJECT ? SUPERSEDED_INVALID_SUBJECT : status,
        groundTruthInvalidated: invalidatedIds.has(c.caseId),
      },
      { invalidatedIds }
    );
    return {
      ...c,
      reviewStatus:
        reviewStateBucket === REVIEW_STATE_BUCKET.INVALIDATED
          ? INVALIDATED_CANDIDATE_SUBJECT
          : status === SUPERSEDED_INVALID_SUBJECT
            ? REVIEW_STATUS.SUPERSEDED_INVALID_SUBJECT
            : status,
      reviewStateBucket,
      humanLabels: review?.humanLabels || null,
      reviewer: review?.reviewer || null,
      reviewedAt: review?.reviewedAt || null,
      secondReviewRequired: review?.secondReviewRequired === true,
      hardCase,
      priorityScore: priorityScore({ ...c, hardCase }, coverage),
      hasHumanReview:
        !!review &&
        status !== REVIEW_STATUS.UNREVIEWED &&
        status !== SUPERSEDED_INVALID_SUBJECT,
      missingSubject: false,
      promoted:
        reviewStateBucket === REVIEW_STATE_BUCKET.COMPLETED ||
        reviewStateBucket === REVIEW_STATE_BUCKET.INVALIDATED
          ? true
          : false,
    };
  });

  if (stateBucket === REVIEW_STATE_BUCKET.SUPERSEDED) {
    // already sourced
  } else if (stateBucket === "ALL") {
    // Full historical audit view — do not collapse to ACTIVE
  } else if (stateBucket) {
    rows = rows.filter((r) => r.reviewStateBucket === stateBucket);
  } else if (!filters.reviewStatus) {
    // Default ACTIVE when no legacy reviewStatus and no explicit all
    rows = rows.filter((r) => r.reviewStateBucket === REVIEW_STATE_BUCKET.ACTIVE_REVIEW);
  }

  // Defense in depth for non-superseded / non-all views
  if (stateBucket !== REVIEW_STATE_BUCKET.SUPERSEDED && stateBucket !== "ALL") {
    rows = rows.filter(
      (r) =>
        r.reviewStateBucket === REVIEW_STATE_BUCKET.INVALIDATED ||
        isActiveReviewCandidate(r) ||
        r.reviewStatus === INVALIDATED_CANDIDATE_SUBJECT
    );
  }

  if (filters.provider) rows = rows.filter((r) => r.provider === filters.provider);
  if (filters.language) rows = rows.filter((r) => r.language === filters.language);
  if (filters.geography) {
    rows = rows.filter(
      (r) => String(r.geography || "").toUpperCase() === String(filters.geography).toUpperCase()
    );
  }
  if (filters.reviewStatus) rows = rows.filter((r) => r.reviewStatus === filters.reviewStatus);
  if (filters.caseType) {
    rows = rows.filter(
      (r) => (r.caseType || r.promptIntentTerritory || r.promptFamily) === filters.caseType
    );
  }
  if (filters.hardCasesOnly) rows = rows.filter((r) => r.hardCase);

  rows.sort((a, b) => {
    const aDone = a.reviewStateBucket === REVIEW_STATE_BUCKET.ACTIVE_REVIEW ? 0 : 1;
    const bDone = b.reviewStateBucket === REVIEW_STATE_BUCKET.ACTIVE_REVIEW ? 0 : 1;
    if (aDone !== bDone) return aDone - bDone;
    return b.priorityScore - a.priorityScore || String(a.caseId).localeCompare(String(b.caseId));
  });

  const progress = getReviewProgress(options);

  return {
    candidateVersion: doc.version,
    progress,
    coverageNeeded: coverage,
    population,
    storedCandidateCount: population.storedCandidateCount,
    activeReviewCandidateCount: population.activeReviewCandidateCount,
    supersededCandidateCount: population.supersededCandidateCount,
    reviewStateFilter: stateBucket || (filters.reviewStatus ? "LEGACY_STATUS" : REVIEW_STATE_BUCKET.ACTIVE_REVIEW),
    stateBuckets: progress.stateBuckets,
    taxonomies: {
      recommendationStatus: RECOMMENDATION_STATUS_TAXONOMY,
      questionStatus: QUESTION_STATUS_TAXONOMY,
      citationAssociation: CITATION_ASSOCIATION_TAXONOMY,
      reviewStatus: Object.values(REVIEW_STATUS),
      reviewStateBuckets: Object.values(REVIEW_STATE_BUCKET),
    },
    cases: rows,
  };
}

/**
 * Expand stored evidence for a case (no provider calls).
 */
export async function loadCaseEvidence(caseId, options = {}) {
  const doc = loadCandidateDocument();
  const candidate = (doc.cases || []).find((c) => c.caseId === caseId);
  if (!candidate) {
    const err = new Error("CASE_NOT_FOUND");
    err.code = "CASE_NOT_FOUND";
    throw err;
  }
  const review = loadReviewRecord(caseId, options);
  const store = options.store || createBrandAiVisibilityReadStore({});
  let evidence = null;
  let expandedText = candidate.rawResponseExcerpt || "";
  let fullAvailable = false;

  if (candidate.batchId && typeof store.listBatchRuns === "function") {
    const runs = (await store.listBatchRuns(candidate.batchId)) || [];
    const run =
      runs.find((r) => r.responseId && r.responseId === candidate.responseId) ||
      runs.find((r) => r.promptId && r.promptId === candidate.promptId) ||
      null;
    if (run?.evidenceId && store.getEvidence) {
      evidence = await store.getEvidence(run.evidenceId);
    }
    if (!evidence && run?.runId && store.listEvidence) {
      const listed = await store.listEvidence({ provider: candidate.provider });
      evidence = (listed || []).find((e) => e.runId === run.runId) || null;
    }
    if (run?.rawText) {
      expandedText = String(run.rawText);
      fullAvailable = true;
    }
  }
  if (evidence?.payload) {
    const t =
      evidence.payload.rawText ||
      evidence.payload.text ||
      evidence.payload.responseText ||
      null;
    if (t) {
      expandedText = String(t);
      fullAvailable = true;
    }
  }

  const MAX = 12000;
  const truncated = expandedText.length > MAX;
  const displayText = truncated ? expandedText.slice(0, MAX) + "\n…[truncated for UI]" : expandedText;

  return {
    candidate,
    review,
    evidenceRef: {
      batchId: candidate.batchId,
      responseId: candidate.responseId,
      promptId: candidate.promptId,
      evidenceId: evidence?.evidenceId || null,
      fullAvailable,
      truncated,
    },
    promptText: candidate.promptText || evidence?.promptText || null,
    rawResponseText: displayText,
    systemSuggestion: candidate.systemSuggestion || null,
    taxonomies: {
      recommendationStatus: RECOMMENDATION_STATUS_TAXONOMY,
      questionStatus: QUESTION_STATUS_TAXONOMY,
      citationAssociation: CITATION_ASSOCIATION_TAXONOMY,
    },
  };
}

function validateHumanLabels(labels, reviewStatus) {
  const failures = [];
  if (
    reviewStatus === REVIEW_STATUS.CONFIRMED ||
    reviewStatus === REVIEW_STATUS.CORRECTED
  ) {
    if (labels.entityPresent !== true && labels.entityPresent !== false) {
      failures.push("entityPresent required (YES/NO)");
    }
    if (labels.entityPresent === true && !labels.canonicalEntityId && !labels.canonicalEntityName) {
      failures.push("canonicalEntity required when entity present");
    }
    if (!labels.recommendationStatus) {
      failures.push("recommendationStatus required");
    } else if (!RECOMMENDATION_STATUS_TAXONOMY.includes(labels.recommendationStatus)) {
      failures.push("recommendationStatus not in governed taxonomy");
    }
    if (
      labels.firstRecommendation !== true &&
      labels.firstRecommendation !== false &&
      labels.firstRecommendation !== "NOT_APPLICABLE"
    ) {
      failures.push("firstRecommendation required (YES/NO/NOT_APPLICABLE)");
    }
    if (!labels.questionStatus) {
      failures.push("questionStatus required");
    } else if (!QUESTION_STATUS_TAXONOMY.includes(labels.questionStatus)) {
      failures.push("questionStatus not in governed taxonomy");
    }
    if (!labels.citationAssociation) {
      failures.push("citationAssociation required");
    } else if (!CITATION_ASSOCIATION_TAXONOMY.includes(labels.citationAssociation)) {
      failures.push("citationAssociation not in governed taxonomy");
    }
  }
  return failures;
}

/**
 * Persist a human review. Never auto-approves.
 */
export function submitHumanReview(payload, options = {}) {
  const caseId = payload?.caseId;
  if (!caseId) {
    const err = new Error("CASE_ID_REQUIRED");
    err.code = "CASE_ID_REQUIRED";
    throw err;
  }
  const doc = loadCandidateDocument();
  const candidate = (doc.cases || []).find((c) => c.caseId === caseId);
  if (!candidate) {
    const err = new Error("CASE_NOT_FOUND");
    err.code = "CASE_NOT_FOUND";
    throw err;
  }
  assertCaseIsActiveForReview(candidate, caseId);

  const reviewStatus = payload.reviewStatus;
  if (
    !Object.values(REVIEW_STATUS).includes(reviewStatus) ||
    reviewStatus === REVIEW_STATUS.UNREVIEWED ||
    reviewStatus === REVIEW_STATUS.SUPERSEDED_INVALID_SUBJECT
  ) {
    const err = new Error("INVALID_REVIEW_STATUS");
    err.code = "INVALID_REVIEW_STATUS";
    err.message =
      "reviewStatus must be CONFIRMED, CORRECTED, DEFERRED, or SECOND_REVIEW_REQUIRED";
    throw err;
  }

  const humanLabels = payload.humanLabels || {};
  const failures = validateHumanLabels(humanLabels, reviewStatus);
  if (failures.length) {
    const err = new Error("INCOMPLETE_HUMAN_LABELS");
    err.code = "INCOMPLETE_HUMAN_LABELS";
    err.failures = failures;
    throw err;
  }

  if (!payload.reviewer || !String(payload.reviewer).trim()) {
    const err = new Error("REVIEWER_REQUIRED");
    err.code = "REVIEWER_REQUIRED";
    throw err;
  }

  const paths = humanReviewRoot(options);
  ensureDirs(paths);
  const prior = loadReviewRecord(caseId, options);
  const now = new Date().toISOString();

  // Diff vs system suggestion (preserve suggestion; never overwrite)
  const sug = candidate.systemSuggestion || {};
  const systemComparable = {
    entityPresent: candidate.candidateEntity ? true : null,
    canonicalEntityId: candidate.canonicalEntityId || null,
    canonicalEntityName: candidate.candidateEntity || null,
    recommendationStatus: sug.expectedRecommendationClass || null,
    firstRecommendation:
      sug.expectedFirstRecommendation === true
        ? true
        : sug.expectedFirstRecommendation === false
          ? false
          : null,
    questionStatus: sug.expectedQuestionStatus || null,
    citationAssociation:
      sug.expectedCitationAssociation === "citations_present_unreviewed"
        ? "UNKNOWN"
        : sug.expectedCitationAssociation || null,
  };
  const humanComparable = {
    entityPresent: humanLabels.entityPresent,
    canonicalEntityId: humanLabels.canonicalEntityId || candidate.canonicalEntityId || null,
    canonicalEntityName: humanLabels.canonicalEntityName || candidate.candidateEntity || null,
    recommendationStatus: humanLabels.recommendationStatus,
    firstRecommendation: humanLabels.firstRecommendation,
    questionStatus: humanLabels.questionStatus,
    citationAssociation: humanLabels.citationAssociation,
  };
  const differences = [];
  for (const f of Object.keys(systemComparable)) {
    if (String(systemComparable[f]) !== String(humanComparable[f])) {
      differences.push({ field: f, system: systemComparable[f], human: humanComparable[f] });
    }
  }

  const record = {
    version: HUMAN_REVIEW_STORE_VERSION,
    caseId,
    candidateVersion: doc.version,
    reviewStatus,
    systemSuggestion: candidate.systemSuggestion || null,
    humanLabels: {
      entityPresent: humanLabels.entityPresent,
      canonicalEntityId: humanLabels.canonicalEntityId || candidate.canonicalEntityId || null,
      canonicalEntityName: humanLabels.canonicalEntityName || candidate.candidateEntity || null,
      recommendationStatus: humanLabels.recommendationStatus,
      firstRecommendation: humanLabels.firstRecommendation,
      questionStatus: humanLabels.questionStatus,
      citationAssociation: humanLabels.citationAssociation,
      parentVsBrandNote: humanLabels.parentVsBrandNote || null,
    },
    differences,
    fieldsChanged: differences.map((d) => d.field),
    suggestedActionFromDiff: differences.length === 0 ? "CONFIRM" : "CORRECT",
    confirmedOrCorrected:
      reviewStatus === REVIEW_STATUS.CONFIRMED || reviewStatus === REVIEW_STATUS.CORRECTED,
    secondReviewRequired: payload.secondReviewRequired === true,
    reviewer: String(payload.reviewer).trim(),
    reviewedAt: now,
    notes: payload.notes || null,
    reviewReason: payload.reviewReason || payload.notes || null,
    importBatchId: payload.importBatchId || null,
    externalAssistanceUsed: payload.externalAssistanceUsed === true,
    provider: candidate.provider,
    model: candidate.model,
    geography: candidate.geography,
    language: candidate.language,
    promptFamily: candidate.promptFamily || candidate.promptIntentTerritory || null,
    batchId: candidate.batchId,
    responseId: candidate.responseId,
    promptId: candidate.promptId,
    llmLabelledAsGroundTruth: false,
    autoApproved: false,
    externalAssistanceGroundTruth: false,
  };

  if (prior) {
    const histDir = path.join(paths.historyDir, caseId);
    fs.mkdirSync(histDir, { recursive: true });
    const stamp = (prior.reviewedAt || "prior").replace(/[:.]/g, "-");
    fs.writeFileSync(path.join(histDir, `${stamp}.json`), JSON.stringify(prior, null, 2), "utf8");
    record.priorReviewPreserved = true;
  }

  fs.writeFileSync(reviewFilePath(paths, caseId), JSON.stringify(record, null, 2), "utf8");

  // Learning ledger for CORRECTED cases (analysis only — never auto-applies rules)
  if (reviewStatus === REVIEW_STATUS.CORRECTED) {
    try {
      const learnDir = path.join(paths.root, "learning");
      fs.mkdirSync(learnDir, { recursive: true });
      const ledgerPath = path.join(learnDir, "corrections.jsonl");
      const pattern = (() => {
        const fields = new Set(record.fieldsChanged || []);
        if (fields.has("recommendationStatus")) {
          const s = systemComparable.recommendationStatus;
          const h = humanComparable.recommendationStatus;
          const positive = new Set([
            "first_recommendation",
            "ranked_recommendation",
            "explicit_recommendation",
          ]);
          if ((s === "discussed" || s === "passing_mention") && positive.has(h)) {
            return "RECOMMENDATION_UNDERCLASSIFICATION";
          }
          if (h === "negative_or_qualified" || s === "negative_or_qualified") {
            return "NEGATIVE_LANGUAGE_MISCLASSIFIED";
          }
          return "RECOMMENDATION_STATUS_MISMATCH";
        }
        if (fields.has("firstRecommendation")) return "FIRST_RECOMMENDATION_MISSED";
        if (fields.has("questionStatus")) return "QUESTION_STATUS_MISMATCH";
        if (fields.has("citationAssociation")) return "CITATION_ASSOCIATION_MISMATCH";
        if (fields.has("canonicalEntityId") || fields.has("canonicalEntityName")) {
          return "PARENT_BRAND_CONFUSION";
        }
        if (fields.has("entityPresent")) return "AMBIGUOUS_ALIAS";
        return "OTHER_DISAGREEMENT";
      })();
      fs.appendFileSync(
        ledgerPath,
        JSON.stringify({
          caseId: record.caseId,
          provider: record.provider,
          model: record.model,
          language: record.language,
          geography: record.geography,
          promptFamily: record.promptFamily,
          entity: record.humanLabels.canonicalEntityName,
          systemSuggestion: record.systemSuggestion,
          humanGroundTruth: record.humanLabels,
          fieldsChanged: record.fieldsChanged,
          differences: record.differences,
          pattern,
          reviewer: record.reviewer,
          reviewedAt: record.reviewedAt,
          notes: record.notes,
          autoApplied: false,
          status: "NOT_APPLIED",
        }) + "\n",
        "utf8"
      );
    } catch (err) {
      console.warn("[golden-set-review] correction ledger write failed:", err?.message || err);
    }
  }

  const progress = getReviewProgress(options);
  fs.writeFileSync(
    paths.indexPath,
    JSON.stringify({ version: HUMAN_REVIEW_STORE_VERSION, updatedAt: now, progress }, null, 2),
    "utf8"
  );

  return { ok: true, record, progress };
}

export function isPromotableReview(record) {
  if (!record) return false;
  if (record.autoApproved) return false;
  if (record.llmLabelledAsGroundTruth) return false;
  if (record.assistedProposalOnly === true) return false;
  if (record.reviewStatus === REVIEW_STATUS.SUPERSEDED_INVALID_SUBJECT) return false;
  if (
    record.reviewStatus !== REVIEW_STATUS.CONFIRMED &&
    record.reviewStatus !== REVIEW_STATUS.CORRECTED
  ) {
    return false;
  }
  if (!record.humanLabels?.canonicalEntityId && !record.humanLabels?.canonicalEntityName) {
    return false;
  }
  if (!record.reviewer || !String(record.reviewer).trim()) return false;
  if (!record.reviewedAt) return false;
  const failures = validateHumanLabels(record.humanLabels || {}, record.reviewStatus);
  return failures.length === 0;
}

/**
 * Deterministic stratified holdout (development ~78% / holdout ~22%).
 * Stratify by provider|language|caseType where available. Never used for tuning in this phase.
 * Mutates cases with holdoutSplit.
 */
export function assignDeterministicHoldout(cases, options = {}) {
  const holdoutRate = options.holdoutRate != null ? Number(options.holdoutRate) : 0.22;
  const minN = options.minN != null ? Number(options.minN) : 150;
  const list = Array.isArray(cases) ? cases : [];
  if (list.length < minN) {
    for (const c of list) c.holdoutSplit = null;
    return {
      HOLDOUT_CREATED: false,
      DEVELOPMENT_N: list.length,
      HOLDOUT_N: 0,
      STRATIFICATION_STATUS: "HOLDOUT_DEFERRED",
      reason: `HOLDOUT_DEFERRED — need ≥${minN} human-labelled cases (have ${list.length})`,
      VERSION: null,
    };
  }

  const strata = new Map();
  for (const c of list) {
    const key = [
      c.provider || "unspecified",
      c.language || "unspecified",
      c.caseType || c.expectedRecommendationClass || c.expectedRecommendationRole || "unspecified",
    ].join("|");
    if (!strata.has(key)) strata.set(key, []);
    strata.get(key).push(c);
  }

  let holdoutN = 0;
  for (const [, rows] of strata) {
    rows.sort((a, b) => String(a.caseId || a.id).localeCompare(String(b.caseId || b.id)));
    const nHold = Math.max(1, Math.round(rows.length * holdoutRate));
    // Last nHold after stable sort → holdout (deterministic)
    rows.forEach((c, i) => {
      const isHold = i >= rows.length - nHold;
      c.holdoutSplit = isHold ? "holdout" : "development";
      if (isHold) holdoutN += 1;
    });
  }

  return {
    HOLDOUT_CREATED: true,
    DEVELOPMENT_N: list.length - holdoutN,
    HOLDOUT_N: holdoutN,
    TARGET_HOLDOUT_RATE: holdoutRate,
    STRATIFICATION_STATUS: "STRATIFIED_BY_PROVIDER_LANGUAGE_CASE_TYPE",
    VERSION: "ai_intelligence_holdout_v1",
    reason: null,
  };
}

/**
 * Promote CONFIRMED/CORRECTED reviews + v1 into Golden Set v2.
 * Dry-run by default unless apply=true. Never overwrites v1.
 */
export function promoteGoldenSetV2(options = {}) {
  const apply = options.apply === true;
  const v1 = loadGoldenSetV1Document();
  const doc = loadCandidateDocument();
  const activeById = Object.fromEntries(
    getActiveGoldenSetReviewCandidates(doc).map((c) => [c.caseId, c])
  );
  const reviews = listAllReviewRecords(options).filter((r) => {
    if (!isPromotableReview(r)) return false;
    const cand = activeById[r.caseId];
    if (!cand) return false; // SUPERSEDED / INVALID_SUBJECT / INACTIVE
    if (!isActiveReviewCandidate(cand)) return false;
    if (!cand.canonicalEntityId || !(cand.candidateEntity || cand.canonicalEntityName)) {
      return false;
    }
    return true;
  });

  const v1Cases = (v1.cases || []).map((c) => ({
    ...c,
    humanLabelled: true,
    llmLabelledAsGroundTruth: false,
    goldenSetVersion: GOLDEN_SET_V2_VERSION,
    sourceGoldenSet: GOLDEN_SET_V1_VERSION,
  }));

  const promoted = reviews.map((r) => {
    const cand = activeById[r.caseId] || {};
    const labels = r.humanLabels || {};
    const first =
      labels.firstRecommendation === "NOT_APPLICABLE" ? null : !!labels.firstRecommendation;
    return {
      caseId: `v2_${r.caseId}`,
      sourceCaseId: r.caseId,
      source: "human_review_promoted",
      humanLabelled: true,
      llmLabelledAsGroundTruth: false,
      autoApproved: false,
      reviewStatus: r.reviewStatus,
      provider: cand.provider || r.provider,
      model: cand.model || r.model,
      batchId: cand.batchId || r.batchId,
      responseId: cand.responseId || r.responseId,
      promptId: cand.promptId || r.promptId,
      promptFamily: cand.promptFamily || null,
      promptIntentTerritory: cand.promptIntentTerritory || null,
      geography: cand.geography || r.geography,
      language: cand.language || r.language,
      promptText: cand.promptText || null,
      rawResponseExcerpt: cand.rawResponseExcerpt || null,
      candidateEntity: labels.canonicalEntityName || cand.candidateEntity,
      canonicalEntityId: labels.canonicalEntityId || cand.canonicalEntityId,
      expectedEntityPresent: labels.entityPresent,
      expectedRecommendationClass: labels.recommendationStatus,
      expectedRecommendationRole: labels.recommendationStatus,
      expectedFirstRecommendation: first,
      expectedQuestionStatus: labels.questionStatus,
      expectedCitationAssociation: labels.citationAssociation,
      caseType: labels.recommendationStatus || null,
      hardCase:
        labels.recommendationStatus === "negative_or_qualified" ||
        labels.recommendationStatus === "comparator" ||
        labels.questionStatus === "NEGATIVE_OR_NOT_RECOMMENDED",
      systemSuggestionPreserved: r.systemSuggestion || null,
      reviewer: r.reviewer,
      reviewedAt: r.reviewedAt,
      notes: r.notes || null,
      goldenSetVersion: GOLDEN_SET_V2_VERSION,
      parentVsBrandNote: labels.parentVsBrandNote || null,
    };
  });

  const allCases = [...v1Cases, ...promoted];
  const holdout = assignDeterministicHoldout(allCases);
  const coverage = auditGoldenCoverage(allCases);
  const result = {
    version: GOLDEN_SET_V2_VERSION,
    previousVersion: GOLDEN_SET_V1_VERSION,
    candidateVersion: doc.version,
    caseCount: allCases.length,
    casesFromV1: v1Cases.length,
    casesPromotedFromReview: promoted.length,
    humanLabelled: allCases.length,
    llmLabelledAsGroundTruth: 0,
    autoApproved: 0,
    reviewDate: new Date().toISOString().slice(0, 10),
    promotionRule:
      "Only CONFIRMED or CORRECTED reviews with complete human labels, reviewer, and reviewedAt. UNREVIEWED/DEFERRED/INCOMPLETE/proposal-only excluded.",
    coverageAudit: coverage,
    holdout,
    note:
      allCases.length >= 150
        ? "Size floor met — subgroup coverage still required for GOVERNED status."
        : `n=${allCases.length}; need ≥150 human-labelled (need ≥${Math.max(0, 150 - allCases.length)} more promotions).`,
    cases: allCases,
  };

  if (apply) {
    if (!fs.existsSync(OUT_V1)) {
      const err = new Error("GOLDEN_SET_V1_MISSING");
      err.code = "GOLDEN_SET_V1_MISSING";
      throw err;
    }
    fs.writeFileSync(V2_PATH, JSON.stringify(result, null, 2), "utf8");
  }

  return {
    apply,
    path: V2_PATH,
    written: apply,
    version: result.version,
    previousVersion: result.previousVersion,
    caseCount: result.caseCount,
    casesFromV1: result.casesFromV1,
    casesPromotedFromReview: result.casesPromotedFromReview,
    humanLabelled: result.humanLabelled,
    llmLabelledAsGroundTruth: result.llmLabelledAsGroundTruth,
    autoApproved: result.autoApproved,
    promotionRule: result.promotionRule,
    coverageAudit: result.coverageAudit,
    holdout: result.holdout,
    note: result.note,
    previewCaseIds: allCases.map((c) => c.caseId).slice(0, 20),
    progress: getReviewProgress(options),
  };
}

export { V2_PATH };
