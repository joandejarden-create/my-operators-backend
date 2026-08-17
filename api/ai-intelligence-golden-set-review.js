/**
 * Golden Set Review API — internal human labelling + review packets + learning analytics.
 * Bulk export/import. No provider calls. No auto-approval. No auto classifier changes.
 */

import {
  buildReviewQueue,
  getReviewProgress,
  loadCaseEvidence,
  submitHumanReview,
  promoteGoldenSetV2,
  computeCoverageNeeded,
  loadGoldenSetV1Document,
  loadGoldenSetV2Document,
  REVIEW_STATUS,
} from "../lib/ai-visibility/validation/golden-set-human-review.js";
import {
  buildReviewPacket,
  buildNextReviewPackets,
  buildResponseOnlyPacket,
  diffHumanVsSystem,
  systemSuggestionAsLabels,
  TAXONOMY_HELP,
  COPY_NEXT_MAX,
} from "../lib/ai-visibility/validation/golden-set-review-packet.js";
import {
  buildLearningReport,
  exportReviewedCases,
} from "../lib/ai-visibility/validation/golden-set-review-learning.js";
import {
  exportAllReviewCandidates,
  exportCandidatesToCsv,
  exportReviewPacketsJson,
  buildAssistanceReturnTemplate,
  buildHumanReviewReturnTemplate,
  previewHumanReviewImport,
  applyHumanReviewImport,
  acceptAssistedProposals,
  enrichQueueWithAssisted,
  EXPORT_MODE,
  IMPORT_ERROR_CODES,
  ASSISTED_REVIEW_IMPORT_VERSION,
  HUMAN_IMPORT_VERSION,
} from "../lib/ai-visibility/validation/golden-set-review-bulk-export-import.js";

export const AI_INTELLIGENCE_GOLDEN_SET_REVIEW_API_VERSION =
  "ai_intelligence_golden_set_review_api_v3";

function ok(res, data) {
  return res.json({
    ok: true,
    success: true,
    apiVersion: AI_INTELLIGENCE_GOLDEN_SET_REVIEW_API_VERSION,
    ...data,
  });
}

function fail(res, status, code, message, extra = {}) {
  return res.status(status).json({
    ok: false,
    success: false,
    error: code,
    code,
    message,
    ...extra,
  });
}

function wrap(handler) {
  return async (req, res) => {
    try {
      return await handler(req, res);
    } catch (err) {
      console.error("[golden-set-review]", err?.message || err);
      const code = err?.code || "SERVER_ERROR";
      const map = {
        GOLDEN_SET_CANDIDATES_MISSING: 404,
        GOLDEN_SET_CANDIDATES_INVALID: 500,
        GOLDEN_SET_V1_MISSING: 404,
        CASE_NOT_FOUND: 404,
        CASE_ID_REQUIRED: 400,
        INVALID_REVIEW_STATUS: 400,
        INCOMPLETE_HUMAN_LABELS: 400,
        REVIEWER_REQUIRED: 400,
        EXPLICIT_APPLY_REQUIRED: 400,
        AUTHORIZED_HUMAN_REQUIRED: 403,
        IMPORT_VALIDATION_FAILED: 400,
        IMPORT_SCHEMA_UNSUPPORTED: 400,
        IMPORT_FILE_INVALID: 400,
        IMPORT_PAYLOAD_TOO_LARGE: 413,
        ASSISTED_PROPOSAL_MISSING: 404,
        INVALID_REVIEW_CASE_MISSING_SUBJECT: 400,
        CASE_SUPERSEDED_NOT_REVIEWABLE: 400,
        CANDIDATE_NOT_ACTIVE: 400,
        EXPORT_VALIDATION_FAILED: 400,
        EXPORT_HAS_NULL_SUBJECTS: 400,
        EXPORT_HAS_SUPERSEDED: 400,
      };
      const status = map[code] || 500;
      return fail(res, status, code, err.message || "Golden Set Review error", {
        failures: err.failures || undefined,
        preview: err.preview || undefined,
      });
    }
  };
}

function reviewerFromReq(req) {
  const u = req.dealalityUser || {};
  return u.email || u.name || u.id || "internal_reviewer";
}

function parseQueueFilters(req) {
  return {
    provider: req.query.provider || null,
    language: req.query.language || null,
    geography: req.query.geography || null,
    reviewStatus: req.query.reviewStatus || null,
    state: req.query.state || null,
    caseType: req.query.caseType || null,
    hardCasesOnly: req.query.hardCasesOnly === "1" || req.query.hardCasesOnly === "true",
    assistedProposalAvailable:
      req.query.assistedProposalAvailable === "1" ||
      req.query.assistedProposalAvailable === "true",
    assistedProposalDiffersFromSystem:
      req.query.assistedProposalDiffersFromSystem === "1" ||
      req.query.assistedProposalDiffersFromSystem === "true",
    needsHumanDecision:
      req.query.needsHumanDecision === "1" || req.query.needsHumanDecision === "true",
  };
}

export const getGoldenSetReviewQueue = wrap(async (req, res) => {
  const filters = parseQueueFilters(req);
  const queue = buildReviewQueue({ filters });
  let cases = enrichQueueWithAssisted(queue.cases, {});
  if (filters.assistedProposalAvailable) {
    cases = cases.filter((c) => c.assistedProposalAvailable);
  }
  if (filters.assistedProposalDiffersFromSystem) {
    cases = cases.filter((c) => c.assistedProposalDiffersFromSystem);
  }
  if (filters.needsHumanDecision) {
    cases = cases.filter((c) => c.needsHumanDecision);
  }
  queue.cases = cases;
  const learning = buildLearningReport({ write: false });
  return ok(res, {
    queue,
    progress: queue.progress,
    coverageNeeded: queue.coverageNeeded,
    taxonomies: queue.taxonomies,
    taxonomyHelp: TAXONOMY_HELP,
    learning: learning.learning,
    copyNextMax: COPY_NEXT_MAX,
    exportModes: Object.values(EXPORT_MODE),
  });
});

export const getGoldenSetReviewProgress = wrap(async (req, res) => {
  const progress = getReviewProgress({});
  const coverageNeeded = computeCoverageNeeded({});
  const learning = buildLearningReport({ write: true });
  const v1 = loadGoldenSetV1Document();
  const v2 = loadGoldenSetV2Document();
  return ok(res, {
    progress,
    coverageNeeded,
    learning: learning.learning,
    patterns: learning.patterns,
    improvementCandidates: learning.improvementCandidates,
    goldenSets: {
      v1: { version: v1.version, caseCount: v1.caseCount || (v1.cases || []).length },
      v2: v2
        ? { version: v2.version, caseCount: v2.caseCount || (v2.cases || []).length }
        : { version: null, caseCount: 0, status: "NOT_CREATED" },
    },
  });
});

export const getGoldenSetReviewCase = wrap(async (req, res) => {
  const caseId = req.params?.caseId || req.query?.caseId;
  if (!caseId) return fail(res, 400, "CASE_ID_REQUIRED", "caseId required");
  const detail = await loadCaseEvidence(caseId, {});
  const systemLabels = systemSuggestionAsLabels(detail.candidate);
  const humanLabels = detail.review?.humanLabels || null;
  const diff = humanLabels ? diffHumanVsSystem(humanLabels, systemLabels) : null;
  const enriched = enrichQueueWithAssisted(
    [
      {
        ...detail.candidate,
        reviewStatus: detail.review?.reviewStatus || REVIEW_STATUS.UNREVIEWED,
      },
    ],
    {}
  )[0];
  return ok(res, {
    case: detail,
    systemAsLabels: systemLabels,
    diff,
    assistedProposal: enriched?.assistedProposal || null,
    assistedProposalAvailable: !!enriched?.assistedProposalAvailable,
    assistedProposalDiffersFromSystem: !!enriched?.assistedProposalDiffersFromSystem,
    taxonomyHelp: TAXONOMY_HELP,
  });
});

export const getGoldenSetReviewPacket = wrap(async (req, res) => {
  const caseId = req.params?.caseId || req.query?.caseId;
  if (!caseId) return fail(res, 400, "CASE_ID_REQUIRED", "caseId required");
  const mode = req.query?.mode || "full";
  if (mode === "response") {
    const detail = await loadCaseEvidence(caseId, {});
    return ok(res, {
      packet: buildResponseOnlyPacket(caseId, detail.rawResponseText),
      disclosure:
        "External review assistance may help interpret a case, but the final label must be confirmed by the human reviewer. Assisted responses are not automatically treated as ground truth.",
    });
  }
  const packet = await buildReviewPacket(caseId, {});
  return ok(res, {
    packet,
    disclosure:
      "External review assistance may help interpret a case, but the final label must be confirmed by the human reviewer. Assisted responses are not automatically treated as ground truth.",
  });
});

export const postGoldenSetReviewPacketsBatch = wrap(async (req, res) => {
  const body = req.body || {};
  let caseIds = Array.isArray(body.caseIds) ? body.caseIds : [];
  if (!caseIds.length) {
    const queue = buildReviewQueue({
      filters: { reviewStatus: "UNREVIEWED" },
    });
    caseIds = queue.cases.slice(0, COPY_NEXT_MAX).map((c) => c.caseId);
  }
  caseIds = caseIds.slice(0, COPY_NEXT_MAX);
  const batch = await buildNextReviewPackets(caseIds, {});
  return ok(res, {
    batch,
    disclosure:
      "External review assistance may help interpret a case, but the final label must be confirmed by the human reviewer. Assisted responses are not automatically treated as ground truth.",
  });
});

export const postGoldenSetReviewCase = wrap(async (req, res) => {
  const caseId = req.params?.caseId || req.body?.caseId;
  const body = req.body || {};
  const result = submitHumanReview(
    {
      caseId,
      reviewStatus: body.reviewStatus,
      humanLabels: body.humanLabels || {},
      reviewer: body.reviewer || reviewerFromReq(req),
      notes: body.notes || null,
      reviewReason: body.reviewReason || body.notes || null,
      secondReviewRequired: body.secondReviewRequired === true,
      externalAssistanceUsed: body.externalAssistanceUsed === true,
      importBatchId: body.importBatchId || null,
    },
    {}
  );
  const learning = buildLearningReport({ write: true });
  return ok(res, { ...result, learning: learning.learning });
});

export const postGoldenSetPromote = wrap(async (req, res) => {
  const apply = req.body?.apply === true || req.query?.apply === "1";
  const result = promoteGoldenSetV2({ apply });
  return ok(res, {
    promotion: result,
    dryRun: !apply,
    note: apply
      ? "Golden Set v2 written. Re-run npm run ai-intelligence:validate to rescore."
      : "Dry-run only. Pass { apply: true } to write v2.",
  });
});

export const getGoldenSetReviewLearning = wrap(async (req, res) => {
  const report = buildLearningReport({ write: true });
  return ok(res, { learningReport: report });
});

export const getGoldenSetReviewExport = wrap(async (req, res) => {
  const format = String(req.query.format || "json").toLowerCase();
  const exported = exportReviewedCases({});
  if (format === "csv") {
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader(
      "Content-Disposition",
      'attachment; filename="golden-set-reviewed-cases.csv"'
    );
    return res.status(200).send(exported.csv);
  }
  return ok(res, { export: exported });
});

function exportModeFromQuery(req) {
  const raw = String(req.query.mode || req.query.exportMode || EXPORT_MODE.ALL).toUpperCase();
  if (Object.values(EXPORT_MODE).includes(raw)) return raw;
  return EXPORT_MODE.ALL;
}

function filtersFromExportReq(req) {
  return {
    provider: req.query.provider || null,
    language: req.query.language || null,
    geography: req.query.geography || null,
    reviewStatus: req.query.reviewStatus || null,
    state: req.query.state || null,
    hardCasesOnly: req.query.hardCasesOnly === "1" || req.query.hardCasesOnly === "true",
  };
}

export const getGoldenSetReviewExportAll = wrap(async (req, res) => {
  const mode = exportModeFromQuery(req);
  const format = String(req.query.format || "json").toLowerCase();
  const payload = exportAllReviewCandidates({
    mode,
    filters: filtersFromExportReq(req),
    write: req.query.write === "1",
  });
  if (format === "csv") {
    const csv = exportCandidatesToCsv(payload);
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="golden-set-review-candidates-${String(mode).toLowerCase()}.csv"`
    );
    return res.status(200).send(csv);
  }
  return ok(res, { export: payload });
});

export const getGoldenSetReviewExportFiltered = wrap(async (req, res) => {
  req.query.mode = EXPORT_MODE.FILTERED_CURRENT_VIEW;
  return getGoldenSetReviewExportAll(req, res);
});

export const getGoldenSetReviewExportPackets = wrap(async (req, res) => {
  const mode = exportModeFromQuery(req);
  const payload = exportReviewPacketsJson({
    mode,
    filters: filtersFromExportReq(req),
    write: req.query.write === "1",
  });
  return ok(res, { export: payload });
});

export const getGoldenSetReviewAssistanceTemplate = wrap(async (req, res) => {
  const mode = exportModeFromQuery(req);
  const payload = buildAssistanceReturnTemplate({
    mode,
    filters: filtersFromExportReq(req),
    write: req.query.write === "1",
  });
  return ok(res, {
    template: payload,
    humanReturnTemplate: buildHumanReviewReturnTemplate({
      candidateVersion: payload.candidateVersion,
    }),
  });
});

export const postGoldenSetReviewImportPreview = wrap(async (req, res) => {
  const body = req.body || {};
  const fileDoc = body.document || body.import || body;
  if (!fileDoc || typeof fileDoc !== "object" || Array.isArray(fileDoc)) {
    return fail(res, 400, "IMPORT_FILE_INVALID", "Import JSON object required");
  }
  if (!Array.isArray(fileDoc.cases)) {
    return fail(res, 400, "IMPORT_DOCUMENT_REQUIRED", "JSON body with cases[] required");
  }
  const preview = previewHumanReviewImport(fileDoc, {
    writePreview: body.writePreview === true,
  });
  if (preview.errorCode === IMPORT_ERROR_CODES.IMPORT_SCHEMA_UNSUPPORTED) {
    return fail(res, 400, "IMPORT_SCHEMA_UNSUPPORTED", preview.note || "Unsupported import schema", {
      preview,
    });
  }
  return ok(res, {
    preview,
    AUTO_APPLY: false,
    ASSISTED_PROPOSAL_GROUND_TRUTH: false,
    CASES_MARKED_REVIEWED: 0,
    HUMAN_FINAL_CREATED: 0,
    AUTO_APPROVALS: 0,
    assistedReviewImportVersion: ASSISTED_REVIEW_IMPORT_VERSION,
    humanImportVersion: HUMAN_IMPORT_VERSION,
  });
});

export const postGoldenSetReviewImportApply = wrap(async (req, res) => {
  const body = req.body || {};
  if (body.apply !== true) {
    return fail(
      res,
      400,
      "EXPLICIT_APPLY_REQUIRED",
      "Pass { apply: true } after reviewing import preview. No auto-apply."
    );
  }
  const fileDoc = body.document || body.import || null;
  if (!fileDoc || !Array.isArray(fileDoc.cases)) {
    return fail(res, 400, "IMPORT_DOCUMENT_REQUIRED", "JSON body.document.cases[] required");
  }
  const result = applyHumanReviewImport(fileDoc, {
    apply: true,
    reviewer: body.reviewer || reviewerFromReq(req),
    externalAssistanceUsed: body.externalAssistanceUsed === true,
  });
  const learning = buildLearningReport({ write: true });
  return ok(res, { ...result, learning: learning.learning });
});

export const postGoldenSetReviewAcceptAssisted = wrap(async (req, res) => {
  const body = req.body || {};
  if (body.apply !== true) {
    return fail(
      res,
      400,
      "EXPLICIT_APPLY_REQUIRED",
      "Pass { apply: true } to accept assisted proposals as human labels."
    );
  }
  const caseIds = Array.isArray(body.caseIds) ? body.caseIds : [];
  if (!caseIds.length) {
    return fail(res, 400, "CASE_IDS_REQUIRED", "caseIds[] required");
  }
  const result = acceptAssistedProposals(caseIds, {
    apply: true,
    reviewer: body.reviewer || reviewerFromReq(req),
  });
  const learning = buildLearningReport({ write: true });
  return ok(res, { ...result, learning: learning.learning });
});

export const postGoldenSetReviewDiffPreview = wrap(async (req, res) => {
  const caseId = req.params?.caseId || req.body?.caseId;
  if (!caseId) return fail(res, 400, "CASE_ID_REQUIRED", "caseId required");
  const detail = await loadCaseEvidence(caseId, {});
  const systemLabels = systemSuggestionAsLabels(detail.candidate);
  const humanLabels = req.body?.humanLabels || {};
  const diff = diffHumanVsSystem(humanLabels, systemLabels);
  return ok(res, {
    suggestedAction: diff.suggestedAction,
    differences: diff.differences,
    matches: diff.matches,
    humanLabelsToSave: humanLabels,
    autoSubmission: false,
  });
});

export { REVIEW_STATUS, EXPORT_MODE };
