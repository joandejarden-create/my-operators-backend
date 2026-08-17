/**
 * Classifier learning loop from human Golden Set corrections.
 * Aggregates patterns and proposes improvements with STATUS=REVIEW_REQUIRED.
 * Does NOT auto-apply classifier changes. LIVE_PROVIDER_CALLS: 0.
 */

import fs from "fs";
import path from "path";
import {
  listAllReviewRecords,
  REVIEW_STATUS,
  loadCandidateDocument,
} from "./golden-set-human-review.js";
import {
  diffHumanVsSystem,
  systemSuggestionAsLabels,
} from "./golden-set-review-packet.js";
import { resolveValidationStorageRoot } from "./validation-storage-root.js";

export const LEARNING_LOOP_VERSION = "ai_intelligence_golden_set_learning_loop_v1";
export const IMPROVEMENT_STATUS = Object.freeze({
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  NOT_APPLIED: "NOT_APPLIED",
});

function learningRoot(options = {}) {
  const { rootDir } = resolveValidationStorageRoot(options);
  const dir = path.join(rootDir, "human-review", "learning");
  return {
    dir,
    correctionsPath: path.join(dir, "corrections.jsonl"),
    reportPath: path.join(dir, "latest-learning-report.json"),
  };
}

function classifyDisagreementPattern(diff, human, system) {
  const fields = new Set(diff.fieldsChanged || []);
  if (fields.has("recommendationStatus")) {
    const s = system?.recommendationStatus;
    const h = human?.recommendationStatus;
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
    if (h === "associated_option" || s === "associated_option") {
      return "CONDITIONAL_RECOMMENDATION_ERROR";
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
}

/**
 * Append correction ledger entry when review is CORRECTED.
 */
export function captureCorrectionLedger(record, candidate, options = {}) {
  if (!record || record.reviewStatus !== REVIEW_STATUS.CORRECTED) return null;
  const systemLabels = systemSuggestionAsLabels(candidate || record);
  const diff = diffHumanVsSystem(record.humanLabels || {}, systemLabels);
  const pattern = classifyDisagreementPattern(diff, record.humanLabels, systemLabels);
  const entry = {
    version: LEARNING_LOOP_VERSION,
    caseId: record.caseId,
    provider: record.provider || candidate?.provider || null,
    model: record.model || candidate?.model || null,
    language: record.language || candidate?.language || null,
    geography: record.geography || candidate?.geography || null,
    promptFamily: candidate?.promptFamily || candidate?.promptIntentTerritory || null,
    entity: record.humanLabels?.canonicalEntityName || candidate?.candidateEntity || null,
    systemSuggestion: record.systemSuggestion || null,
    humanGroundTruth: record.humanLabels || null,
    fieldsChanged: diff.fieldsChanged,
    differences: diff.differences,
    pattern,
    reviewer: record.reviewer,
    reviewedAt: record.reviewedAt,
    notes: record.notes || null,
    autoApplied: false,
    status: IMPROVEMENT_STATUS.NOT_APPLIED,
  };

  const root = learningRoot(options);
  fs.mkdirSync(root.dir, { recursive: true });
  fs.appendFileSync(root.correctionsPath, JSON.stringify(entry) + "\n", "utf8");
  return entry;
}

function readCorrectionLedger(options = {}) {
  const root = learningRoot(options);
  if (!fs.existsSync(root.correctionsPath)) {
    // Derive from review records if ledger empty
    return deriveCorrectionsFromReviews(options);
  }
  const lines = fs.readFileSync(root.correctionsPath, "utf8").split(/\r?\n/).filter(Boolean);
  const byCase = new Map();
  for (const line of lines) {
    try {
      const row = JSON.parse(line);
      if (row?.caseId) byCase.set(row.caseId, row);
    } catch {
      // skip bad lines
    }
  }
  return [...byCase.values()];
}

function deriveCorrectionsFromReviews(options = {}) {
  const doc = loadCandidateDocument();
  const byCand = Object.fromEntries((doc.cases || []).map((c) => [c.caseId, c]));
  const out = [];
  for (const r of listAllReviewRecords(options)) {
    if (r.reviewStatus !== REVIEW_STATUS.CORRECTED) continue;
    const cand = byCand[r.caseId] || {};
    const systemLabels = systemSuggestionAsLabels({ ...cand, systemSuggestion: r.systemSuggestion });
    const diff = r.fieldsChanged
      ? {
          fieldsChanged: r.fieldsChanged,
          differences: r.differences || [],
        }
      : diffHumanVsSystem(r.humanLabels || {}, systemLabels);
    out.push({
      caseId: r.caseId,
      provider: r.provider,
      model: r.model,
      language: r.language,
      geography: r.geography,
      promptFamily: cand.promptFamily || cand.promptIntentTerritory || null,
      entity: r.humanLabels?.canonicalEntityName || cand.candidateEntity,
      systemSuggestion: r.systemSuggestion,
      humanGroundTruth: r.humanLabels,
      fieldsChanged: diff.fieldsChanged,
      differences: diff.differences,
      pattern: classifyDisagreementPattern(diff, r.humanLabels, systemLabels),
      reviewer: r.reviewer,
      reviewedAt: r.reviewedAt,
      notes: r.notes,
      autoApplied: false,
      status: IMPROVEMENT_STATUS.NOT_APPLIED,
    });
  }
  return out;
}

/**
 * Aggregate correction patterns + improvement candidates (never auto-applied).
 */
export function buildLearningReport(options = {}) {
  const reviews = listAllReviewRecords(options);
  const corrections = readCorrectionLedger(options);
  const confirmed = reviews.filter((r) => r.reviewStatus === REVIEW_STATUS.CONFIRMED).length;
  const corrected = reviews.filter((r) => r.reviewStatus === REVIEW_STATUS.CORRECTED).length;
  const deferred = reviews.filter((r) => r.reviewStatus === REVIEW_STATUS.DEFERRED).length;
  const reviewed = reviews.filter((r) => r.reviewStatus !== REVIEW_STATUS.UNREVIEWED).length;

  const byPattern = {};
  for (const c of corrections) {
    const p = c.pattern || "OTHER_DISAGREEMENT";
    if (!byPattern[p]) {
      byPattern[p] = {
        PATTERN: p,
        CASE_COUNT: 0,
        CASE_IDS: [],
        PROVIDERS: new Set(),
        LANGUAGES: new Set(),
        GEOGRAPHIES: new Set(),
        COMMON_CONTEXT: [],
        CURRENT_RULE: "Production classifier (v3) vs human ground truth",
        POTENTIAL_RULE_GAP: null,
      };
    }
    const row = byPattern[p];
    row.CASE_COUNT += 1;
    row.CASE_IDS.push(c.caseId);
    if (c.provider) row.PROVIDERS.add(c.provider);
    if (c.language) row.LANGUAGES.add(c.language);
    if (c.geography) row.GEOGRAPHIES.add(c.geography);
    if (c.differences?.length) {
      row.COMMON_CONTEXT.push(
        c.differences.map((d) => `${d.field}: ${d.system}→${d.human}`).join("; ")
      );
    }
  }

  const patterns = Object.values(byPattern).map((p) => ({
    PATTERN: p.PATTERN,
    CASE_COUNT: p.CASE_COUNT,
    CASE_IDS: p.CASE_IDS,
    PROVIDERS: [...p.PROVIDERS],
    LANGUAGES: [...p.LANGUAGES],
    GEOGRAPHIES: [...p.GEOGRAPHIES],
    COMMON_CONTEXT: p.COMMON_CONTEXT.slice(0, 8),
    CURRENT_RULE: p.CURRENT_RULE,
    POTENTIAL_RULE_GAP: describeGap(p.PATTERN),
  }));

  const improvementCandidates = patterns
    .filter((p) => p.CASE_COUNT >= 3)
    .map((p) => ({
      STATUS: IMPROVEMENT_STATUS.REVIEW_REQUIRED,
      PATTERN: p.PATTERN,
      CASE_COUNT: p.CASE_COUNT,
      CASE_IDS: p.CASE_IDS,
      OBSERVED: p.COMMON_CONTEXT.slice(0, 5),
      POSSIBLE_DETERMINISTIC_IMPROVEMENT: describeGap(p.PATTERN),
      DO_NOT_APPLY: true,
      note: "Separate approved hardening phase required. Must regress Golden Set v1+v2.",
    }));

  const fieldCounts = {
    recommendation: 0,
    firstRecommendation: 0,
    questionStatus: 0,
    citation: 0,
    parentBrand: 0,
  };
  for (const c of corrections) {
    for (const f of c.fieldsChanged || []) {
      if (f === "recommendationStatus") fieldCounts.recommendation += 1;
      if (f === "firstRecommendation") fieldCounts.firstRecommendation += 1;
      if (f === "questionStatus") fieldCounts.questionStatus += 1;
      if (f === "citationAssociation") fieldCounts.citation += 1;
      if (f === "canonicalEntityId" || f === "canonicalEntityName") fieldCounts.parentBrand += 1;
    }
  }

  const mostCommon =
    patterns.sort((a, b) => b.CASE_COUNT - a.CASE_COUNT)[0]?.PATTERN || null;

  const report = {
    version: LEARNING_LOOP_VERSION,
    generatedAt: new Date().toISOString(),
    AUTO_RULE_CHANGES: false,
    progress: {
      REVIEWED: reviewed,
      CONFIRMED: confirmed,
      CORRECTED: corrected,
      DEFERRED: deferred,
    },
    learning: {
      CORRECTIONS: corrections.length,
      MOST_COMMON_DISAGREEMENT: mostCommon,
      RECOMMENDATION_CLASSIFICATION_CORRECTIONS: fieldCounts.recommendation,
      FIRST_RECOMMENDATION_CORRECTIONS: fieldCounts.firstRecommendation,
      QUESTION_STATUS_CORRECTIONS: fieldCounts.questionStatus,
      CITATION_CORRECTIONS: fieldCounts.citation,
      PARENT_BRAND_CORRECTIONS: fieldCounts.parentBrand,
      CLASSIFIER_IMPROVEMENT_CANDIDATES: improvementCandidates.length,
    },
    patterns,
    improvementCandidates,
    corrections,
    note: "Learning means pattern analysis from human ground truth. No automatic classifier rewrites.",
  };

  if (options.write !== false) {
    const root = learningRoot(options);
    fs.mkdirSync(root.dir, { recursive: true });
    fs.writeFileSync(root.reportPath, JSON.stringify(report, null, 2), "utf8");
  }
  return report;
}

function describeGap(pattern) {
  switch (pattern) {
    case "RECOMMENDATION_UNDERCLASSIFICATION":
      return "Positive ranked/explicit recommendation language may be falling through to discussed; review structure-first recommendation cues.";
    case "FIRST_RECOMMENDATION_MISSED":
      return "Leading/first-list cues may not be promoting first_recommendation consistently.";
    case "NEGATIVE_LANGUAGE_MISCLASSIFIED":
      return "Negative/qualified language cues may need broader coverage without overfitting.";
    case "QUESTION_STATUS_MISMATCH":
      return "Question-status mapping from roles may diverge from human interpretation.";
    case "CITATION_ASSOCIATION_MISMATCH":
      return "Citation association rules may not match human association judgments.";
    case "PARENT_BRAND_CONFUSION":
      return "Parent vs brand canonical identity may need clearer resolution rules.";
    case "AMBIGUOUS_ALIAS":
      return "Alias/presence detection may miss or over-match difficult aliases.";
    case "MULTI_BRAND_RANKING_ERROR":
      return "Multi-brand ranked lists may need stronger ordered-list handling.";
    case "CONDITIONAL_RECOMMENDATION_ERROR":
      return "Associated/conditional option language may be mis-tiered vs recommendation.";
    default:
      return "Review corrected examples for a deterministic, regression-tested rule change.";
  }
}

/**
 * Export reviewed cases as JSON (and optional CSV rows).
 */
export function exportReviewedCases(options = {}) {
  const doc = loadCandidateDocument();
  const byCand = Object.fromEntries((doc.cases || []).map((c) => [c.caseId, c]));
  const rows = [];
  for (const r of listAllReviewRecords(options)) {
    if (r.reviewStatus === REVIEW_STATUS.UNREVIEWED) continue;
    const cand = byCand[r.caseId] || {};
    const systemLabels = systemSuggestionAsLabels({
      ...cand,
      systemSuggestion: r.systemSuggestion,
    });
    const diff = diffHumanVsSystem(r.humanLabels || {}, systemLabels);
    rows.push({
      caseId: r.caseId,
      reviewStatus: r.reviewStatus,
      provider: r.provider,
      language: r.language,
      geography: r.geography,
      promptFamily: cand.promptFamily || cand.promptIntentTerritory || null,
      model: r.model,
      systemSuggestion: r.systemSuggestion,
      humanLabel: r.humanLabels,
      differences: diff.differences,
      fieldsChanged: diff.fieldsChanged,
      reviewer: r.reviewer,
      reviewedAt: r.reviewedAt,
      notes: r.notes,
    });
  }
  const csvHeader = [
    "caseId",
    "reviewStatus",
    "provider",
    "language",
    "geography",
    "promptFamily",
    "reviewedAt",
    "fieldsChanged",
  ].join(",");
  const csvLines = rows.map((r) =>
    [
      r.caseId,
      r.reviewStatus,
      r.provider,
      r.language,
      r.geography,
      JSON.stringify(r.promptFamily || ""),
      r.reviewedAt,
      JSON.stringify((r.fieldsChanged || []).join("|")),
    ].join(",")
  );
  return {
    count: rows.length,
    json: rows,
    csv: [csvHeader, ...csvLines].join("\n"),
  };
}
