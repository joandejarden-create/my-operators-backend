/**
 * Golden Set Review — export ALL candidates + bulk human/assisted import.
 * Assisted proposals are NOT ground truth. No provider calls. No auto-promote.
 */

import fs from "fs";
import path from "path";
import crypto from "crypto";
import {
  loadCandidateDocument,
  listAllReviewRecords,
  loadReviewRecord,
  submitHumanReview,
  getReviewProgress,
  buildReviewQueue,
  REVIEW_STATUS,
  RECOMMENDATION_STATUS_TAXONOMY,
  QUESTION_STATUS_TAXONOMY,
  CITATION_ASSOCIATION_TAXONOMY,
  HUMAN_REVIEW_STORE_VERSION,
} from "./golden-set-human-review.js";
import {
  systemSuggestionAsLabels,
  diffHumanVsSystem,
  TAXONOMY_HELP,
  normalizeCitationSuggestion,
} from "./golden-set-review-packet.js";
import {
  getActiveGoldenSetReviewCandidates,
  isActiveReviewCandidate,
  assertCaseIsActiveForReview,
  summarizeCandidatePopulation,
  SUPERSEDED_INVALID_SUBJECT,
} from "./golden-set-active-candidates.js";
import { validateActiveCandidatesForExport } from "./golden-set-candidate-entity-remediation.js";
import { resolveValidationStorageRoot } from "./validation-storage-root.js";

export const BULK_EXPORT_VERSION = "ai_intelligence_golden_set_bulk_export_v1";
export const ASSISTANCE_TEMPLATE_VERSION = "ai_intelligence_golden_set_assistance_return_v1";
/** External assisted-review proposal file (NOT human ground truth). */
export const ASSISTED_REVIEW_IMPORT_VERSION = "ai_intelligence_golden_set_assisted_review_v1";
export const HUMAN_IMPORT_VERSION = "ai_intelligence_golden_set_human_import_v1";

export const IMPORT_ERROR_CODES = Object.freeze({
  IMPORT_FILE_INVALID: "IMPORT_FILE_INVALID",
  IMPORT_SCHEMA_UNSUPPORTED: "IMPORT_SCHEMA_UNSUPPORTED",
  CANDIDATE_VERSION_MISMATCH: "CANDIDATE_VERSION_MISMATCH",
  UNKNOWN_CASE_IDS: "UNKNOWN_CASE_IDS",
  INVALID_ENUM: "INVALID_ENUM",
  CANONICAL_ENTITY_MISMATCH: "CANONICAL_ENTITY_MISMATCH",
  CANDIDATE_NOT_ACTIVE: "CANDIDATE_NOT_ACTIVE",
  IMPORT_PAYLOAD_TOO_LARGE: "IMPORT_PAYLOAD_TOO_LARGE",
  SERVER_ERROR: "SERVER_ERROR",
});

export const EXPORT_MODE = Object.freeze({
  ALL: "ALL",
  NEXT_10: "NEXT_10",
  NEXT_25: "NEXT_25",
  FILTERED_CURRENT_VIEW: "FILTERED_CURRENT_VIEW",
});

export const REVIEW_INSTRUCTIONS = Object.freeze([
  "Judge only what the stored AI response says about the evaluated entity.",
  "Do not judge whether the AI recommendation is objectively correct for the market.",
  "System suggestion is NOT ground truth.",
  "External ChatGPT assistance is NOT ground truth.",
  "Choose only from the provided allowedHumanLabels options.",
  "Human reviewer must explicitly confirm or correct each case in Dealality (or via authorized import Apply).",
  "UNKNOWN is allowed when evidence is insufficient.",
  "Do not infer citation association without evidence in the stored response.",
]);

const ALLOWED_ENTITY_PRESENT = Object.freeze(["YES", "NO"]);
const ALLOWED_FIRST = Object.freeze(["YES", "NO", "NOT_APPLICABLE"]);

function bulkRoot(options = {}) {
  const { rootDir } = resolveValidationStorageRoot(options);
  const dir = path.join(rootDir, "human-review");
  return {
    root: dir,
    assistedDir: path.join(dir, "assisted-proposals"),
    importsDir: path.join(dir, "imports"),
    exportsDir: path.join(dir, "exports"),
  };
}

function ensureBulkDirs(paths) {
  fs.mkdirSync(paths.assistedDir, { recursive: true });
  fs.mkdirSync(paths.importsDir, { recursive: true });
  fs.mkdirSync(paths.exportsDir, { recursive: true });
}

function stamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function csvEscape(v) {
  if (v == null) return "";
  const s = String(v);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function firstToExport(v) {
  if (v === true) return "YES";
  if (v === false) return "NO";
  if (v === "NOT_APPLICABLE") return "NOT_APPLICABLE";
  return null;
}

function parseEntityPresent(v) {
  if (v === true || v === "YES" || v === "yes" || v === "true") return true;
  if (v === false || v === "NO" || v === "no" || v === "false") return false;
  return null;
}

function parseFirst(v) {
  if (v === true || v === "YES" || v === "yes" || v === "true") return true;
  if (v === false || v === "NO" || v === "no" || v === "false") return false;
  if (v === "NOT_APPLICABLE") return "NOT_APPLICABLE";
  return null;
}

function normalizeHumanLabels(raw = {}) {
  return {
    entityPresent: parseEntityPresent(raw.entityPresent),
    canonicalEntityId: raw.canonicalEntityId || null,
    canonicalEntityName: raw.canonicalEntityName || null,
    recommendationStatus: raw.recommendationStatus || null,
    firstRecommendation: parseFirst(raw.firstRecommendation),
    questionStatus: raw.questionStatus || null,
    citationAssociation: raw.citationAssociation
      ? normalizeCitationSuggestion(raw.citationAssociation) || raw.citationAssociation
      : null,
    parentVsBrandNote: raw.parentBrandNote || raw.parentVsBrandNote || null,
  };
}

function allowedHumanLabels() {
  return {
    entityPresent: [...ALLOWED_ENTITY_PRESENT],
    recommendationStatus: [...RECOMMENDATION_STATUS_TAXONOMY],
    firstRecommendation: [...ALLOWED_FIRST],
    questionStatus: [...QUESTION_STATUS_TAXONOMY],
    citationAssociation: [...CITATION_ASSOCIATION_TAXONOMY],
  };
}

function systemSuggestionExport(candidate) {
  const labels = systemSuggestionAsLabels(candidate);
  const sug = candidate.systemSuggestion || {};
  return {
    entityPresent: firstToExport(labels.entityPresent),
    recommendationStatus: labels.recommendationStatus,
    firstRecommendation: firstToExport(labels.firstRecommendation),
    questionStatus: labels.questionStatus,
    citationAssociation: labels.citationAssociation,
    parentBrandClassification: sug.parentBrandClassification || null,
    note: sug.note || "SYSTEM_SUGGESTION only — not ground truth",
  };
}

function humanReviewExport(review) {
  if (!review || review.reviewStatus === REVIEW_STATUS.UNREVIEWED) {
    return {
      entityPresent: null,
      canonicalEntityId: null,
      canonicalEntityName: null,
      recommendationStatus: null,
      firstRecommendation: null,
      questionStatus: null,
      citationAssociation: null,
      parentBrandNote: null,
      notes: null,
      reviewer: null,
      reviewedAt: null,
    };
  }
  const h = review.humanLabels || {};
  return {
    entityPresent: firstToExport(h.entityPresent),
    canonicalEntityId: h.canonicalEntityId || null,
    canonicalEntityName: h.canonicalEntityName || null,
    recommendationStatus: h.recommendationStatus || null,
    firstRecommendation: firstToExport(h.firstRecommendation),
    questionStatus: h.questionStatus || null,
    citationAssociation: h.citationAssociation || null,
    parentBrandNote: h.parentVsBrandNote || null,
    notes: review.notes || null,
    reviewer: review.reviewer || null,
    reviewedAt: review.reviewedAt || null,
  };
}

export function loadAssistedProposal(caseId, options = {}) {
  const paths = bulkRoot(options);
  const p = path.join(paths.assistedDir, `${caseId}.json`);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function listAssistedProposals(options = {}) {
  const paths = bulkRoot(options);
  if (!fs.existsSync(paths.assistedDir)) return [];
  return fs
    .readdirSync(paths.assistedDir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => JSON.parse(fs.readFileSync(path.join(paths.assistedDir, f), "utf8")));
}

function buildExportCaseRow(candidate, review, assisted) {
  const status = review?.reviewStatus || REVIEW_STATUS.UNREVIEWED;
  const sys = systemSuggestionExport(candidate);
  const sysLabels = systemSuggestionAsLabels(candidate);
  let assistedDiffers = false;
  if (assisted?.recommendedHumanReview) {
    const proposed = normalizeHumanLabels(assisted.recommendedHumanReview);
    assistedDiffers = !diffHumanVsSystem(proposed, sysLabels).matches;
  }
  return {
    caseId: candidate.caseId,
    candidateVersion: candidate.goldenSetVersion || null,
    provider: candidate.provider || null,
    model: candidate.model || null,
    batchId: candidate.batchId || null,
    responseId: candidate.responseId || null,
    promptId: candidate.promptId || null,
    geography: candidate.geography || null,
    language: candidate.language || null,
    promptFamily: candidate.promptFamily || candidate.promptIntentTerritory || null,
    promptText: candidate.promptText || null,
    storedResponse: candidate.rawResponseExcerpt || null,
    subjectEntityName: candidate.candidateEntity || null,
    canonicalEntityId: candidate.canonicalEntityId || null,
    canonicalEntityName: candidate.candidateEntity || null,
    systemSuggestion: sys,
    allowedHumanLabels: allowedHumanLabels(),
    reviewStatus: status,
    humanReview: humanReviewExport(review),
    reviewAssistance: {
      reason: review?.reviewReason || assisted?.recommendedHumanReview?.reason || null,
      externalAssistanceUsed: !!(assisted || review?.externalAssistanceUsed),
      assistedProposalAvailable: !!assisted,
      assistedProposalDiffersFromSystem: assistedDiffers,
      assistedProposal: assisted
        ? {
            recommendedHumanReview: assisted.recommendedHumanReview,
            importedAt: assisted.importedAt || null,
            note: "ASSISTED PROPOSAL — not ground truth until human Apply/Accept",
          }
        : null,
    },
  };
}

function selectCasesForMode(doc, reviewsById, options = {}) {
  const mode = String(options.mode || EXPORT_MODE.ALL).toUpperCase();
  const filters = options.filters || {};
  // ALL review/export modes start from ACTIVE candidates only (never raw stored set)
  let cases =
    options.includeSuperseded === true
      ? doc.cases || []
      : getActiveGoldenSetReviewCandidates(doc);

  if (mode === EXPORT_MODE.FILTERED_CURRENT_VIEW) {
    const queue = buildReviewQueue({ ...options, filters });
    cases = queue.cases.map((c) => {
      const {
        reviewStatus,
        humanLabels,
        reviewer,
        reviewedAt,
        hardCase,
        priorityScore,
        hasHumanReview,
        missingSubject,
        assistedProposalAvailable,
        assistedProposalDiffersFromSystem,
        assistedProposal,
        needsHumanDecision,
        ...rest
      } = c;
      return rest;
    });
  } else if (mode === EXPORT_MODE.NEXT_10 || mode === EXPORT_MODE.NEXT_25) {
    const limit = mode === EXPORT_MODE.NEXT_10 ? 10 : 25;
    const queue = buildReviewQueue({
      ...options,
      filters: { ...filters, reviewStatus: REVIEW_STATUS.UNREVIEWED },
    });
    const ids = new Set(queue.cases.slice(0, limit).map((c) => c.caseId));
    cases = getActiveGoldenSetReviewCandidates(doc).filter((c) => ids.has(c.caseId));
  } else {
    if (filters.provider) cases = cases.filter((c) => c.provider === filters.provider);
    if (filters.language) cases = cases.filter((c) => c.language === filters.language);
    if (filters.geography) {
      cases = cases.filter(
        (c) => String(c.geography || "").toUpperCase() === String(filters.geography).toUpperCase()
      );
    }
    if (filters.reviewStatus) {
      cases = cases.filter(
        (c) =>
          (reviewsById[c.caseId]?.reviewStatus || REVIEW_STATUS.UNREVIEWED) === filters.reviewStatus
      );
    }
  }
  if (options.includeSuperseded !== true) {
    cases = cases.filter(isActiveReviewCandidate);
  }
  return { mode, cases };
}

export function computeExportCoverage(cases) {
  const coverage = {
    OPENAI: 0,
    GEMINI: 0,
    PERPLEXITY: 0,
    CLAUDE: 0,
    ENGLISH: 0,
    SPANISH: 0,
    GLOBAL: 0,
    CALA: 0,
    MEXICO: 0,
    EUROPE: 0,
    NORTH_AMERICA: 0,
  };
  for (const c of cases || []) {
    const p = String(c.provider || "").toLowerCase();
    if (p === "openai") coverage.OPENAI += 1;
    if (p === "gemini") coverage.GEMINI += 1;
    if (p === "perplexity") coverage.PERPLEXITY += 1;
    if (p === "claude") coverage.CLAUDE += 1;
    const lang = String(c.language || "").toLowerCase();
    if (lang === "en") coverage.ENGLISH += 1;
    if (lang === "es") coverage.SPANISH += 1;
    const geo = String(c.geography || "").toUpperCase();
    if (coverage[geo] != null) coverage[geo] += 1;
  }
  return coverage;
}

/**
 * Export candidates (JSON object). Does not write unless write=true.
 * "ALL" means ALL ACTIVE review candidates — not all historical stored records.
 */
export function exportAllReviewCandidates(options = {}) {
  const doc = loadCandidateDocument();
  const population = summarizeCandidatePopulation(doc);
  const reviews = listAllReviewRecords(options);
  const reviewsById = Object.fromEntries(reviews.map((r) => [r.caseId, r]));
  const assistedById = Object.fromEntries(
    listAssistedProposals(options).map((a) => [a.caseId, a])
  );

  const gate = validateActiveCandidatesForExport(doc, {});
  if (!gate.ok && options.requireExportGate !== false) {
    const err = new Error("EXPORT_VALIDATION_FAILED");
    err.code = "EXPORT_VALIDATION_FAILED";
    err.gate = gate;
    throw err;
  }

  const { mode, cases } = selectCasesForMode(doc, reviewsById, options);
  const reviewable = cases.filter(isActiveReviewCandidate);
  const rows = reviewable.map((c) =>
    buildExportCaseRow(c, reviewsById[c.caseId] || null, assistedById[c.caseId] || null)
  );
  const reviewedCount = rows.filter(
    (r) =>
      r.reviewStatus !== REVIEW_STATUS.UNREVIEWED && r.reviewStatus !== "PENDING_HUMAN_REVIEW"
  ).length;
  const nullSubjects = rows.filter(
    (r) => !r.subjectEntityName || !r.canonicalEntityId || !r.canonicalEntityName
  ).length;
  const supersededInExport = rows.filter(
    (r) => r.reviewStatus === SUPERSEDED_INVALID_SUBJECT
  ).length;

  const payload = {
    exportVersion: BULK_EXPORT_VERSION,
    candidateVersion: doc.version,
    generatedAt: new Date().toISOString(),
    exportMode: mode,
    storedCandidateCount: population.storedCandidateCount,
    activeReviewCandidateCount: population.activeReviewCandidateCount,
    supersededCandidateCount: population.supersededCandidateCount,
    exportedCandidateCount: rows.length,
    totalCandidates: rows.length,
    queueTotal: population.activeReviewCandidateCount,
    reviewedCount,
    unreviewedCount: rows.length - reviewedCount,
    entitySpecificCaseCount: rows.length - nullSubjects,
    invalidSubjectCaseCount: nullSubjects,
    coverage: computeExportCoverage(rows),
    reviewInstructions: [...REVIEW_INSTRUCTIONS],
    humanReviewReturnTemplate: buildHumanReviewReturnTemplate({ cases: [] }),
    cases: rows,
    AUTO_LABEL: false,
    AUTO_APPROVAL: false,
    AUTO_PROMOTION: false,
    note:
      "Export contains ACTIVE review candidates only. Superseded invalid subjects are retained in the candidate fixture for audit but are not exported.",
  };

  if ((nullSubjects > 0 || supersededInExport > 0) && options.requireExportGate !== false) {
    const err = new Error(nullSubjects > 0 ? "EXPORT_HAS_NULL_SUBJECTS" : "EXPORT_HAS_SUPERSEDED");
    err.code = nullSubjects > 0 ? "EXPORT_HAS_NULL_SUBJECTS" : "EXPORT_HAS_SUPERSEDED";
    err.invalidSubjectCaseCount = nullSubjects;
    err.supersededInExport = supersededInExport;
    throw err;
  }

  if (options.write === true) {
    const paths = bulkRoot(options);
    ensureBulkDirs(paths);
    const base = `golden-set-review-candidates-${String(mode).toLowerCase()}-${stamp()}`;
    const jsonPath = path.join(paths.exportsDir, `${base}.json`);
    const csvPath = path.join(paths.exportsDir, `${base}.csv`);
    fs.writeFileSync(jsonPath, JSON.stringify(payload, null, 2), "utf8");
    fs.writeFileSync(csvPath, exportCandidatesToCsv(payload), "utf8");
    const convenience = path.join(paths.exportsDir, "golden-set-review-candidates-all.json");
    if (mode === EXPORT_MODE.ALL) {
      fs.writeFileSync(convenience, JSON.stringify(payload, null, 2), "utf8");
      payload.writtenFiles = { jsonPath, csvPath, conveniencePath: convenience };
    } else {
      payload.writtenFiles = { jsonPath, csvPath };
    }
  }
  return payload;
}

export function exportCandidatesToCsv(exportPayload) {
  const headers = [
    "caseId",
    "provider",
    "model",
    "geography",
    "language",
    "promptFamily",
    "entityName",
    "canonicalEntityId",
    "promptText",
    "storedResponse",
    "systemEntityPresent",
    "systemRecommendationStatus",
    "systemFirstRecommendation",
    "systemQuestionStatus",
    "systemCitationAssociation",
    "reviewStatus",
    "humanEntityPresent",
    "humanRecommendationStatus",
    "humanFirstRecommendation",
    "humanQuestionStatus",
    "humanCitationAssociation",
    "humanReason",
  ];
  const lines = [headers.join(",")];
  for (const c of exportPayload.cases || []) {
    const sys = c.systemSuggestion || {};
    const hum = c.humanReview || {};
    lines.push(
      [
        c.caseId,
        c.provider,
        c.model,
        c.geography,
        c.language,
        c.promptFamily,
        c.subjectEntityName,
        c.canonicalEntityId,
        c.promptText,
        c.storedResponse,
        sys.entityPresent,
        sys.recommendationStatus,
        sys.firstRecommendation,
        sys.questionStatus,
        sys.citationAssociation,
        c.reviewStatus,
        hum.entityPresent,
        hum.recommendationStatus,
        hum.firstRecommendation,
        hum.questionStatus,
        hum.citationAssociation,
        c.reviewAssistance?.reason || hum.notes,
      ]
        .map(csvEscape)
        .join(",")
    );
  }
  return lines.join("\n") + "\n";
}

/**
 * ChatGPT-friendly review packets JSON (no classifier internals).
 */
export function exportReviewPacketsJson(options = {}) {
  const bulk = exportAllReviewCandidates({ ...options, write: false });
  const cases = (bulk.cases || []).map((c) => ({
    caseId: c.caseId,
    subjectEntity: c.subjectEntityName,
    canonicalEntityId: c.canonicalEntityId,
    provider: c.provider,
    model: c.model,
    geography: c.geography,
    language: c.language,
    promptFamily: c.promptFamily,
    prompt: c.promptText,
    storedResponse: c.storedResponse,
    systemSuggestion: c.systemSuggestion,
    allowedHumanLabels: c.allowedHumanLabels,
    taxonomyHelp: TAXONOMY_HELP,
    reviewStatus: c.reviewStatus,
  }));
  const payload = {
    exportVersion: `${BULK_EXPORT_VERSION}_packets`,
    candidateVersion: bulk.candidateVersion,
    generatedAt: bulk.generatedAt,
    exportMode: bulk.exportMode,
    totalCandidates: cases.length,
    reviewInstructions: bulk.reviewInstructions,
    disclosure:
      "External review assistance may help interpret a case, but the final label must be confirmed by the human reviewer. Assisted responses are not automatically treated as ground truth.",
    suggestedReturnSchema: "recommendedHumanReview (NOT groundTruth)",
    cases,
  };
  if (options.write === true) {
    const paths = bulkRoot(options);
    ensureBulkDirs(paths);
    const p = path.join(
      paths.exportsDir,
      `golden-set-review-packets-${String(bulk.exportMode).toLowerCase()}-${stamp()}.json`
    );
    fs.writeFileSync(p, JSON.stringify(payload, null, 2), "utf8");
    payload.writtenPath = p;
  }
  return payload;
}

export function buildHumanReviewReturnTemplate(options = {}) {
  const exampleCases = options.cases || [
    {
      caseId: "cand_EXAMPLE",
      reviewStatus: "CORRECTED",
      humanReview: {
        entityPresent: "YES",
        canonicalEntityId: "recEXAMPLE",
        canonicalEntityName: "Example Brand",
        recommendationStatus: "first_recommendation",
        firstRecommendation: "YES",
        questionStatus: "FIRST_RECOMMENDED",
        citationAssociation: "UNKNOWN",
        parentBrandNote: "",
        notes: "",
      },
      reviewReason: "Entity is explicitly ranked #1 as a strong option.",
    },
  ];
  return {
    reviewVersion: HUMAN_IMPORT_VERSION,
    candidateVersion: options.candidateVersion || null,
    note: "Fill reviewStatus + humanReview. Import requires authorized human Apply. Not auto-approved.",
    cases: exampleCases,
  };
}

export function buildAssistanceReturnTemplate(options = {}) {
  const doc = loadCandidateDocument();
  const modeOpts = { ...options, mode: options.mode || EXPORT_MODE.ALL };
  const { cases } = selectCasesForMode(doc, {}, modeOpts);
  const limit = options.templateCaseLimit != null ? options.templateCaseLimit : cases.length;
  const selected = cases.slice(0, limit);
  const payload = {
    reviewVersion: ASSISTANCE_TEMPLATE_VERSION,
    candidateVersion: doc.version,
    generatedAt: new Date().toISOString(),
    note: "Field is recommendedHumanReview — NOT groundTruth. Human must Apply in Dealality.",
    cases: selected.map((c) => ({
      caseId: c.caseId,
      recommendedHumanReview: {
        entityPresent: null,
        canonicalEntityId: c.canonicalEntityId || null,
        canonicalEntityName: c.candidateEntity || null,
        recommendationStatus: null,
        firstRecommendation: null,
        questionStatus: null,
        citationAssociation: null,
        parentBrandNote: "",
        reason: "",
      },
    })),
  };
  if (options.write === true) {
    const paths = bulkRoot(options);
    ensureBulkDirs(paths);
    const p = path.join(paths.exportsDir, `golden-set-review-assistance-template-${stamp()}.json`);
    fs.writeFileSync(p, JSON.stringify(payload, null, 2), "utf8");
    payload.writtenPath = p;
  }
  return payload;
}

function validateLabelEnums(labels, failures, caseId, cand = null) {
  if (labels.entityPresent !== true && labels.entityPresent !== false) {
    failures.push({ caseId, code: "INVALID_ENUM", field: "entityPresent" });
  }
  if (labels.entityPresent === true && !labels.canonicalEntityId && !labels.canonicalEntityName) {
    failures.push({ caseId, code: "MISSING_REQUIRED_FIELDS", field: "canonicalEntity" });
  }
  if (!labels.recommendationStatus) {
    failures.push({ caseId, code: "MISSING_REQUIRED_FIELDS", field: "recommendationStatus" });
  } else if (!RECOMMENDATION_STATUS_TAXONOMY.includes(labels.recommendationStatus)) {
    failures.push({
      caseId,
      code: "INVALID_ENUM",
      field: "recommendationStatus",
      value: labels.recommendationStatus,
    });
  }
  if (
    labels.firstRecommendation !== true &&
    labels.firstRecommendation !== false &&
    labels.firstRecommendation !== "NOT_APPLICABLE"
  ) {
    failures.push({ caseId, code: "INVALID_ENUM", field: "firstRecommendation" });
  }
  if (!labels.questionStatus) {
    failures.push({ caseId, code: "MISSING_REQUIRED_FIELDS", field: "questionStatus" });
  } else if (!QUESTION_STATUS_TAXONOMY.includes(labels.questionStatus)) {
    failures.push({
      caseId,
      code: "INVALID_ENUM",
      field: "questionStatus",
      value: labels.questionStatus,
    });
  }
  if (!labels.citationAssociation) {
    failures.push({ caseId, code: "MISSING_REQUIRED_FIELDS", field: "citationAssociation" });
  } else if (!CITATION_ASSOCIATION_TAXONOMY.includes(labels.citationAssociation)) {
    failures.push({
      caseId,
      code: "INVALID_ENUM",
      field: "citationAssociation",
      value: labels.citationAssociation,
    });
  }

  if (cand && labels.entityPresent === true) {
    if (
      labels.canonicalEntityId &&
      cand.canonicalEntityId &&
      String(labels.canonicalEntityId) !== String(cand.canonicalEntityId)
    ) {
      failures.push({
        caseId,
        code: "CANONICAL_ENTITY_MISMATCH",
        field: "canonicalEntityId",
        expected: cand.canonicalEntityId,
        value: labels.canonicalEntityId,
      });
    }
    if (
      labels.canonicalEntityName &&
      cand.candidateEntity &&
      String(labels.canonicalEntityName).trim() !== String(cand.candidateEntity).trim()
    ) {
      failures.push({
        caseId,
        code: "CANONICAL_ENTITY_MISMATCH",
        field: "canonicalEntityName",
        expected: cand.candidateEntity,
        value: labels.canonicalEntityName,
      });
    }
  }
}

/**
 * Discriminate ASSISTED proposal import vs HUMAN FINAL import.
 * Assisted must never be treated as ground truth.
 */
export function detectImportKind(doc) {
  const rv = doc?.reviewVersion != null ? String(doc.reviewVersion) : "";
  if (rv === ASSISTED_REVIEW_IMPORT_VERSION || rv === ASSISTANCE_TEMPLATE_VERSION) {
    return "ASSISTED";
  }
  if (rv === HUMAN_IMPORT_VERSION) return "HUMAN";
  const cases = doc?.cases || [];
  if (!cases.length) return "EMPTY";
  const sample = cases[0] || {};
  if (sample.recommendedHumanReview && !sample.humanReview) return "ASSISTED";
  if (sample.humanReview || sample.reviewStatus) return "HUMAN";
  if (sample.recommendedHumanReview) return "ASSISTED";
  return "UNKNOWN";
}

function bumpDist(map, key) {
  const k = key == null || key === "" ? "UNKNOWN" : String(key);
  map[k] = (map[k] || 0) + 1;
}

function countFieldDiffs(changes, field) {
  return (changes || []).filter((ch) =>
    (ch.differences || []).some((d) => d.field === field)
  ).length;
}

/**
 * Dry-run import preview. Never writes.
 */
export function previewHumanReviewImport(fileDoc, options = {}) {
  const doc = loadCandidateDocument();
  const byCand = Object.fromEntries(
    getActiveGoldenSetReviewCandidates(doc).map((c) => [c.caseId, c])
  );
  const allByCand = Object.fromEntries((doc.cases || []).map((c) => [c.caseId, c]));
  const kind = detectImportKind(fileDoc);
  const rows = Array.isArray(fileDoc?.cases) ? fileDoc.cases : [];
  const seen = new Set();
  const invalidCaseIds = [];
  const inactiveCaseIds = [];
  const invalidEnums = [];
  const missingRequired = [];
  const canonicalMismatches = [];
  const versionMismatches = [];
  const duplicates = [];
  const statusMismatches = [];
  const changes = [];
  let confirmed = 0;
  let corrected = 0;
  let deferred = 0;
  let matched = 0;

  const distribution = {
    provider: {},
    language: {},
    geography: {},
  };

  if (kind === "UNKNOWN" || kind === "EMPTY") {
    return {
      importVersion:
        kind === "ASSISTED" ? ASSISTED_REVIEW_IMPORT_VERSION : HUMAN_IMPORT_VERSION,
      reviewVersion: fileDoc?.reviewVersion || null,
      preview: true,
      wrote: false,
      kind,
      errorCode: IMPORT_ERROR_CODES.IMPORT_SCHEMA_UNSUPPORTED,
      PROPOSALS_TOTAL: rows.length,
      MATCHED_ACTIVE_CASES: 0,
      INVALID_CASES: rows.length,
      MISSING_CASES: 0,
      UNKNOWN_CASE_IDS: [],
      SUPERSEDED: 0,
      SYSTEM_MATCHES: 0,
      SYSTEM_DIFFERENCES: 0,
      ENTITY_PRESENT_DIFFS: 0,
      RECOMMENDATION_DIFFS: 0,
      FIRST_RECOMMENDATION_DIFFS: 0,
      QUESTION_STATUS_DIFFS: 0,
      CITATION_DIFFS: 0,
      CANONICAL_ENTITY_DIFFS: 0,
      TOTAL_ROWS: rows.length,
      MATCHED_CASES: 0,
      CONFIRMED: 0,
      CORRECTED: 0,
      DEFERRED: 0,
      INVALID_CASE_IDS: [],
      CANDIDATE_NOT_ACTIVE: [],
      INVALID_ENUMS: [],
      MISSING_REQUIRED_FIELDS: [],
      CANONICAL_ENTITY_MISMATCHES: [],
      VERSION_MISMATCHES: [],
      DUPLICATES: [],
      STATUS_NOTES: [],
      HARD_STATUS_ERRORS: [],
      CHANGES_TO_APPLY: [],
      HIGHLIGHTS: {},
      DISTRIBUTION: distribution,
      canApply: false,
      allOrNothing: true,
      ASSISTED_PROPOSAL_GROUND_TRUTH: false,
      CASES_MARKED_REVIEWED: 0,
      HUMAN_FINAL_CREATED: 0,
      AUTO_APPLY: false,
      AUTO_APPROVALS: 0,
      note: "Unsupported import schema. Use assisted_review_v1 or human_import_v1.",
    };
  }

  const fileVersion = fileDoc?.candidateVersion || null;
  if (fileVersion && fileVersion !== doc.version) {
    versionMismatches.push({
      fileCandidateVersion: fileVersion,
      expected: doc.version,
      code: IMPORT_ERROR_CODES.CANDIDATE_VERSION_MISMATCH,
    });
  }

  for (const row of rows) {
    const caseId = row?.caseId;
    if (!caseId) {
      missingRequired.push({ caseId: null, code: "MISSING_REQUIRED_FIELDS", field: "caseId" });
      continue;
    }
    if (seen.has(caseId)) {
      duplicates.push(caseId);
      continue;
    }
    seen.add(caseId);
    const stored = allByCand[caseId];
    if (!stored) {
      invalidCaseIds.push(caseId);
      continue;
    }
    const cand = byCand[caseId];
    if (!cand || !isActiveReviewCandidate(stored)) {
      inactiveCaseIds.push({
        caseId,
        code: "CANDIDATE_NOT_ACTIVE",
        reviewStatus: stored.reviewStatus,
      });
      continue;
    }
    matched += 1;
    bumpDist(distribution.provider, cand.provider);
    bumpDist(distribution.language, cand.language);
    bumpDist(
      distribution.geography,
      cand.geography || cand.commercialRegion || cand.region || null
    );

    if (kind === "ASSISTED") {
      const labels = normalizeHumanLabels(row.recommendedHumanReview || {});
      const failures = [];
      validateLabelEnums(labels, failures, caseId, cand);
      for (const f of failures) {
        if (f.code === "INVALID_ENUM" || f.code === "INVALID_ENUMS") invalidEnums.push(f);
        else if (f.code === "CANONICAL_ENTITY_MISMATCH") canonicalMismatches.push(f);
        else missingRequired.push(f);
      }
      const sys = systemSuggestionAsLabels(cand);
      const diff = diffHumanVsSystem(labels, sys);
      changes.push({
        caseId,
        kind: "ASSISTED_PROPOSAL",
        groundTruth: false,
        reviewStatusUnchanged: REVIEW_STATUS.UNREVIEWED,
        differsFromSystem: !diff.matches,
        differences: diff.differences,
        fieldsChanged: diff.fieldsChanged || [],
        proposedLabels: labels,
        systemLabels: sys,
        provider: cand.provider || null,
        language: cand.language || null,
        geography: cand.geography || cand.commercialRegion || cand.region || null,
        reason: row.recommendedHumanReview?.reason || row.reviewReason || null,
      });
      continue;
    }

    // HUMAN import
    let requestedStatus = row.reviewStatus;
    if (
      requestedStatus &&
      ![REVIEW_STATUS.CONFIRMED, REVIEW_STATUS.CORRECTED, REVIEW_STATUS.DEFERRED].includes(
        requestedStatus
      )
    ) {
      statusMismatches.push({ caseId, reviewStatus: requestedStatus, code: "INVALID_REVIEW_STATUS" });
      continue;
    }

    if (requestedStatus === REVIEW_STATUS.DEFERRED) {
      deferred += 1;
      changes.push({
        caseId,
        kind: "HUMAN",
        finalComputedReviewStatus: REVIEW_STATUS.DEFERRED,
        requestedStatus,
        deferred: true,
      });
      continue;
    }

    const labels = normalizeHumanLabels(row.humanReview || row.humanLabels || {});
    const failures = [];
    validateLabelEnums(labels, failures, caseId, cand);
    for (const f of failures) {
      if (f.code === "INVALID_ENUM" || f.code === "INVALID_ENUMS") invalidEnums.push(f);
      else if (f.code === "CANONICAL_ENTITY_MISMATCH") canonicalMismatches.push(f);
      else missingRequired.push(f);
    }
    const sys = systemSuggestionAsLabels(cand);
    // Fill canonical defaults from candidate when present
    if (labels.entityPresent === true) {
      labels.canonicalEntityId = labels.canonicalEntityId || cand.canonicalEntityId || null;
      labels.canonicalEntityName = labels.canonicalEntityName || cand.candidateEntity || null;
    }
    const diff = diffHumanVsSystem(labels, sys);
    const computed =
      diff.matches ? REVIEW_STATUS.CONFIRMED : REVIEW_STATUS.CORRECTED;
    if (requestedStatus === REVIEW_STATUS.CONFIRMED && computed === REVIEW_STATUS.CORRECTED) {
      statusMismatches.push({
        caseId,
        code: "CONFIRM_WHEN_DIFFERS",
        message: "File marked CONFIRMED but labels differ from system — will recompute to CORRECTED",
        requestedStatus,
        computedStatus: computed,
      });
    }
    if (computed === REVIEW_STATUS.CONFIRMED) confirmed += 1;
    else corrected += 1;

    const highlight = {
      canonicalEntityChanged: diff.fieldsChanged.includes("canonicalEntityId") ||
        diff.fieldsChanged.includes("canonicalEntityName"),
      recommendationChanged: diff.fieldsChanged.includes("recommendationStatus"),
      firstRecommendationChanged: diff.fieldsChanged.includes("firstRecommendation"),
      citationChanged: diff.fieldsChanged.includes("citationAssociation"),
    };

    changes.push({
      caseId,
      kind: "HUMAN",
      requestedStatus: requestedStatus || computed,
      finalComputedReviewStatus: computed,
      differences: diff.differences,
      fieldsChanged: diff.fieldsChanged,
      highlight,
      humanLabels: labels,
      reviewReason: row.reviewReason || row.humanReview?.notes || null,
      systemSuggestionPreserved: true,
    });
  }

  const systemMatches = changes.filter((c) => c.differsFromSystem === false).length;
  const systemDifferences = changes.filter((c) => c.differsFromSystem === true).length;

  const blockingErrors =
    invalidCaseIds.length +
    inactiveCaseIds.length +
    invalidEnums.length +
    missingRequired.length +
    canonicalMismatches.length +
    duplicates.length +
    (versionMismatches.length ? 1 : 0);

  // statusMismatches for CONFIRM_WHEN_DIFFERS are warnings (we recompute), not blockers
  const hardStatusErrors = statusMismatches.filter((s) => s.code === "INVALID_REVIEW_STATUS");

  let errorCode = null;
  if (versionMismatches.length) errorCode = IMPORT_ERROR_CODES.CANDIDATE_VERSION_MISMATCH;
  else if (invalidCaseIds.length) errorCode = IMPORT_ERROR_CODES.UNKNOWN_CASE_IDS;
  else if (inactiveCaseIds.length) errorCode = IMPORT_ERROR_CODES.CANDIDATE_NOT_ACTIVE;
  else if (canonicalMismatches.length) errorCode = IMPORT_ERROR_CODES.CANONICAL_ENTITY_MISMATCH;
  else if (invalidEnums.length) errorCode = IMPORT_ERROR_CODES.INVALID_ENUM;

  const preview = {
    importVersion:
      kind === "ASSISTED" ? ASSISTED_REVIEW_IMPORT_VERSION : HUMAN_IMPORT_VERSION,
    reviewVersion: fileDoc?.reviewVersion || null,
    preview: true,
    wrote: false,
    kind,
    errorCode,
    PROPOSALS_TOTAL: rows.length,
    MATCHED_ACTIVE_CASES: matched,
    INVALID_CASES:
      invalidEnums.length +
      missingRequired.length +
      canonicalMismatches.length +
      duplicates.length,
    MISSING_CASES: missingRequired.filter((m) => m.field === "caseId").length,
    UNKNOWN_CASE_IDS: invalidCaseIds,
    SUPERSEDED: inactiveCaseIds.length,
    SYSTEM_MATCHES: kind === "ASSISTED" ? systemMatches : confirmed,
    SYSTEM_DIFFERENCES: kind === "ASSISTED" ? systemDifferences : corrected,
    ENTITY_PRESENT_DIFFS: countFieldDiffs(changes, "entityPresent"),
    RECOMMENDATION_DIFFS: countFieldDiffs(changes, "recommendationStatus"),
    FIRST_RECOMMENDATION_DIFFS: countFieldDiffs(changes, "firstRecommendation"),
    QUESTION_STATUS_DIFFS: countFieldDiffs(changes, "questionStatus"),
    CITATION_DIFFS: countFieldDiffs(changes, "citationAssociation"),
    CANONICAL_ENTITY_DIFFS:
      countFieldDiffs(changes, "canonicalEntityId") +
      countFieldDiffs(changes, "canonicalEntityName"),
    TOTAL_ROWS: rows.length,
    MATCHED_CASES: matched,
    CONFIRMED: confirmed,
    CORRECTED: corrected,
    DEFERRED: deferred,
    INVALID_CASE_IDS: invalidCaseIds,
    CANDIDATE_NOT_ACTIVE: inactiveCaseIds,
    INVALID_ENUMS: invalidEnums,
    MISSING_REQUIRED_FIELDS: missingRequired,
    CANONICAL_ENTITY_MISMATCHES: canonicalMismatches,
    VERSION_MISMATCHES: versionMismatches,
    DUPLICATES: duplicates,
    STATUS_NOTES: statusMismatches,
    HARD_STATUS_ERRORS: hardStatusErrors,
    CHANGES_TO_APPLY: changes,
    HIGHLIGHTS: {
      canonicalEntityChanges: changes
        .filter((c) => c.highlight?.canonicalEntityChanged)
        .map((c) => c.caseId),
      recommendationChanges: changes
        .filter((c) => c.highlight?.recommendationChanged)
        .map((c) => c.caseId),
      firstRecommendationChanges: changes
        .filter((c) => c.highlight?.firstRecommendationChanged)
        .map((c) => c.caseId),
      citationChanges: changes.filter((c) => c.highlight?.citationChanged).map((c) => c.caseId),
      assistedDiffersFromSystem: changes
        .filter((c) => c.differsFromSystem)
        .map((c) => c.caseId),
    },
    DISTRIBUTION: distribution,
    canApply: blockingErrors === 0 && hardStatusErrors.length === 0 && matched > 0,
    allOrNothing: true,
    ASSISTED_PROPOSAL_GROUND_TRUTH: false,
    CASES_MARKED_REVIEWED: 0,
    HUMAN_FINAL_CREATED: 0,
    AUTO_APPLY: false,
    AUTO_APPROVALS: 0,
    note:
      kind === "ASSISTED"
        ? "Assisted proposals only. Apply stores ASSISTED PROPOSAL — not Golden Set ground truth. Cases remain UNREVIEWED until Accept/Confirm."
        : "Human import Apply writes CONFIRMED/CORRECTED/DEFERRED with recomputed Confirm vs Correct.",
  };

  if (options.writePreview === true) {
    const paths = bulkRoot(options);
    ensureBulkDirs(paths);
    const p = path.join(paths.importsDir, `golden-set-review-import-preview-${stamp()}.json`);
    fs.writeFileSync(p, JSON.stringify(preview, null, 2), "utf8");
    preview.writtenPreviewPath = p;
  }
  return preview;
}

function storeAssistedProposal(caseId, recommendedHumanReview, meta, options = {}) {
  const paths = bulkRoot(options);
  ensureBulkDirs(paths);
  const record = {
    version: meta.reviewVersion || ASSISTED_REVIEW_IMPORT_VERSION,
    caseId,
    recommendedHumanReview,
    groundTruth: false,
    llmLabelledAsGroundTruth: false,
    externalAssistanceUsed: true,
    assistedProposalAvailable: true,
    reviewStatus: REVIEW_STATUS.UNREVIEWED,
    reviewer: null,
    reviewedAt: null,
    importedAt: new Date().toISOString(),
    importBatchId: meta.importBatchId,
    importedBy: meta.reviewer,
    note: "ASSISTED PROPOSAL — not human ground truth",
  };
  fs.writeFileSync(
    path.join(paths.assistedDir, `${caseId}.json`),
    JSON.stringify(record, null, 2),
    "utf8"
  );
  return record;
}

/**
 * Apply import after explicit human approval.
 * Assisted → store proposals only.
 * Human → submitHumanReview all-or-nothing.
 */
export function applyHumanReviewImport(fileDoc, options = {}) {
  if (options.apply !== true) {
    const err = new Error("EXPLICIT_APPLY_REQUIRED");
    err.code = "EXPLICIT_APPLY_REQUIRED";
    throw err;
  }
  if (!options.reviewer || !String(options.reviewer).trim()) {
    const err = new Error("AUTHORIZED_HUMAN_REQUIRED");
    err.code = "AUTHORIZED_HUMAN_REQUIRED";
    throw err;
  }

  const preview = previewHumanReviewImport(fileDoc, { ...options, writePreview: false });
  if (!preview.canApply) {
    const err = new Error("IMPORT_VALIDATION_FAILED");
    err.code = "IMPORT_VALIDATION_FAILED";
    err.preview = preview;
    throw err;
  }

  const importBatchId =
    options.importBatchId || `imp_${crypto.randomBytes(6).toString("hex")}`;
  const reviewer = String(options.reviewer).trim();
  const results = [];

  if (preview.kind === "ASSISTED") {
    for (const ch of preview.CHANGES_TO_APPLY) {
      const rec = storeAssistedProposal(
        ch.caseId,
        {
          ...ch.proposedLabels,
          entityPresent: firstToExport(ch.proposedLabels.entityPresent),
          firstRecommendation: firstToExport(ch.proposedLabels.firstRecommendation),
          reason: ch.reason || "",
          parentBrandNote: ch.proposedLabels.parentVsBrandNote || "",
        },
        {
          importBatchId,
          reviewer,
          reviewVersion: fileDoc?.reviewVersion || ASSISTED_REVIEW_IMPORT_VERSION,
        },
        options
      );
      results.push({ caseId: ch.caseId, stored: "ASSISTED_PROPOSAL", groundTruth: false });
      void rec;
    }
  } else {
    // All-or-nothing: validate again then write sequentially; on failure stop (prior writes remain — document)
    // Prefer transactional: write to temp then — for file store we write all after validation already passed.
    for (const ch of preview.CHANGES_TO_APPLY) {
      if (ch.finalComputedReviewStatus === REVIEW_STATUS.DEFERRED) {
        const r = submitHumanReview(
          {
            caseId: ch.caseId,
            reviewStatus: REVIEW_STATUS.DEFERRED,
            humanLabels: {},
            reviewer,
            notes: ch.reviewReason || null,
            reviewReason: ch.reviewReason || null,
            importBatchId,
            externalAssistanceUsed: options.externalAssistanceUsed === true,
          },
          options
        );
        results.push({ caseId: ch.caseId, record: r.record });
        continue;
      }
      const r = submitHumanReview(
        {
          caseId: ch.caseId,
          reviewStatus: ch.finalComputedReviewStatus,
          humanLabels: ch.humanLabels,
          reviewer,
          notes: ch.reviewReason || null,
          reviewReason: ch.reviewReason || null,
          importBatchId,
          externalAssistanceUsed: options.externalAssistanceUsed === true,
        },
        options
      );
      results.push({
        caseId: ch.caseId,
        reviewStatus: r.record.reviewStatus,
        fieldsChanged: r.record.fieldsChanged,
      });
    }
  }

  const paths = bulkRoot(options);
  ensureBulkDirs(paths);
  const audit = {
    importBatchId,
    kind: preview.kind,
    reviewer,
    reviewedAt: new Date().toISOString(),
    previewSummary: {
      TOTAL_ROWS: preview.TOTAL_ROWS,
      MATCHED_CASES: preview.MATCHED_CASES,
      CONFIRMED: preview.CONFIRMED,
      CORRECTED: preview.CORRECTED,
      DEFERRED: preview.DEFERRED,
    },
    results,
    AUTO_PROMOTION: false,
    CLASSIFIER_CHANGES: false,
  };
  const auditPath = path.join(paths.importsDir, `${importBatchId}.json`);
  fs.writeFileSync(auditPath, JSON.stringify(audit, null, 2), "utf8");

  return {
    ok: true,
    applied: true,
    importBatchId,
    kind: preview.kind,
    progress: getReviewProgress(options),
    auditPath,
    resultCount: results.length,
    ASSISTED_PROPOSAL_GROUND_TRUTH: false,
    note:
      preview.kind === "ASSISTED"
        ? "Assisted proposals stored. Human must Accept/Confirm per case (or Accept All Valid) before Golden Set promotion."
        : "Human reviews applied. System suggestions preserved. Promotion remains a separate step.",
  };
}

/**
 * Accept assisted proposal(s) as human ground truth (explicit).
 */
export function acceptAssistedProposals(caseIds, options = {}) {
  if (options.apply !== true) {
    const err = new Error("EXPLICIT_APPLY_REQUIRED");
    err.code = "EXPLICIT_APPLY_REQUIRED";
    throw err;
  }
  if (!options.reviewer) {
    const err = new Error("AUTHORIZED_HUMAN_REQUIRED");
    err.code = "AUTHORIZED_HUMAN_REQUIRED";
    throw err;
  }
  const doc = loadCandidateDocument();
  const byCand = Object.fromEntries((doc.cases || []).map((c) => [c.caseId, c]));
  const ids = Array.isArray(caseIds) ? caseIds : [];
  const humanCases = [];
  for (const caseId of ids) {
    const assisted = loadAssistedProposal(caseId, options);
    if (!assisted?.recommendedHumanReview) {
      const err = new Error(`ASSISTED_PROPOSAL_MISSING:${caseId}`);
      err.code = "ASSISTED_PROPOSAL_MISSING";
      throw err;
    }
    const cand = byCand[caseId];
    if (!cand) {
      const err = new Error(`CASE_NOT_FOUND:${caseId}`);
      err.code = "CASE_NOT_FOUND";
      throw err;
    }
    const labels = normalizeHumanLabels(assisted.recommendedHumanReview);
    labels.canonicalEntityId = labels.canonicalEntityId || cand.canonicalEntityId;
    labels.canonicalEntityName = labels.canonicalEntityName || cand.candidateEntity;
    const sys = systemSuggestionAsLabels(cand);
    const diff = diffHumanVsSystem(labels, sys);
    humanCases.push({
      caseId,
      reviewStatus: diff.matches ? REVIEW_STATUS.CONFIRMED : REVIEW_STATUS.CORRECTED,
      humanReview: {
        entityPresent: firstToExport(labels.entityPresent),
        canonicalEntityId: labels.canonicalEntityId,
        canonicalEntityName: labels.canonicalEntityName,
        recommendationStatus: labels.recommendationStatus,
        firstRecommendation: firstToExport(labels.firstRecommendation),
        questionStatus: labels.questionStatus,
        citationAssociation: labels.citationAssociation,
        parentBrandNote: labels.parentVsBrandNote || "",
        notes: assisted.recommendedHumanReview.reason || "",
      },
      reviewReason: assisted.recommendedHumanReview.reason || "",
    });
  }
  return applyHumanReviewImport(
    { reviewVersion: HUMAN_IMPORT_VERSION, candidateVersion: doc.version, cases: humanCases },
    {
      ...options,
      apply: true,
      externalAssistanceUsed: true,
    }
  );
}

/** Queue filter helpers for assisted workflow */
export function enrichQueueWithAssisted(queueCases, options = {}) {
  const assisted = Object.fromEntries(
    listAssistedProposals(options).map((a) => [a.caseId, a])
  );
  return (queueCases || []).map((c) => {
    const a = assisted[c.caseId];
    let assistedDiffers = false;
    if (a?.recommendedHumanReview) {
      const proposed = normalizeHumanLabels(a.recommendedHumanReview);
      assistedDiffers = !diffHumanVsSystem(proposed, systemSuggestionAsLabels(c)).matches;
    }
    return {
      ...c,
      assistedProposalAvailable: !!a,
      assistedProposalDiffersFromSystem: assistedDiffers,
      assistedProposal: a?.recommendedHumanReview || null,
      needsHumanDecision:
        (c.reviewStatus === REVIEW_STATUS.UNREVIEWED || !c.hasHumanReview) && !!a,
    };
  });
}
