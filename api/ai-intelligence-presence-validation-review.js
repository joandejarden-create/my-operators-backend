/**
 * Presence validation human review API — PRESENT / NOT_PRESENT / INVALID / DEFER.
 * Export is read-only assist packets. No recommendation labels. No Holdout v2 scoring. No auto-label.
 */

import {
  loadPresenceValidationCandidates,
  savePresenceValidationReview,
  summarizePresenceValidationReview,
} from "../lib/ai-visibility/validation/presence-validation-candidates.js";
import {
  buildPresenceValidationReviewExport,
  formatPresenceValidationExportMarkdown,
  filterPresenceValidationCases,
  loadPresenceValidationCasesForExport,
  parseExportLimit,
  PRESENCE_VALIDATION_EXPORT_VERSION,
} from "../lib/ai-visibility/validation/presence-validation-review-export.js";
import {
  importAssistedProposals,
  validateAssistedProposalDocument,
  loadAssistedProposals,
  classifyAssistedBulkApproval,
  applyAssistedBulkApproval,
} from "../lib/ai-visibility/validation/presence-validation-assisted-proposals.js";

const API_VERSION = "ai_intelligence_presence_validation_review_v1";

function ok(res, data) {
  return res.json({ ok: true, success: true, apiVersion: API_VERSION, ...data });
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
      console.error("[presence-validation-review]", err?.message || err);
      const map = {
        CASE_NOT_FOUND: 404,
        INVALID_PRESENCE_REVIEW_ACTION: 400,
        REVIEWER_REQUIRED: 400,
        CANDIDATES_MISSING: 404,
        IMPORT_VALIDATION_FAILED: 400,
        EXPLICIT_CONFIRMATION_REQUIRED: 400,
      };
      return fail(res, map[err?.code] || 500, err?.code || "SERVER_ERROR", err?.message || "error");
    }
  };
}

function filtersFromReq(req) {
  return {
    status: req.query?.status || "pending",
    provider: req.query?.provider || null,
    language: req.query?.language || null,
    geography: req.query?.geography || null,
    candidateType: req.query?.candidateType || null,
    primary: req.query?.primary ?? "1",
    assisted: req.query?.assisted || "all",
  };
}

export function getPresenceValidationReviewQueue(req, res) {
  return wrap(async () => {
    const loaded = loadPresenceValidationCasesForExport();
    if (!loaded) {
      return fail(res, 404, "CANDIDATES_MISSING", "Run fresh presence validation generation first");
    }
    const summary = summarizePresenceValidationReview();
    const filters = filtersFromReq(req);
    const cases = filterPresenceValidationCases(loaded.cases, filters);

    return ok(res, {
      summary,
      guidance: {
        PRESENT:
          "The specific canonical entity is actually represented in the response.",
        NOT_PRESENT: "The canonical entity does not appear.",
        INVALID:
          "Invalid identity association — do not use as positive or negative validation.",
        examples: [
          "Playa del Carmen ≠ Playa Hotels & Resorts",
          "IHG ≠ IHG Hotels & Resorts (Managed)",
          "Marriott ≠ Autograph Collection",
          "Collection ≠ Luxury Collection / Curio",
        ],
      },
      cases,
      TOTAL_IN_FILTER: cases.length,
    });
  })(req, res);
}

export function postPresenceValidationReviewDecision(req, res) {
  return wrap(async () => {
    const body = req.body || {};
    const caseId = body.caseId;
    if (!caseId) return fail(res, 400, "CASE_ID_REQUIRED", "caseId required");
    const saved = savePresenceValidationReview(caseId, {
      action: body.action,
      reviewer: body.reviewer,
      notes: body.notes,
      acceptAssisted: body.acceptAssisted === true,
      humanAction: body.humanAction || null,
    });
    return ok(res, {
      saved,
      summary: summarizePresenceValidationReview(),
      AUTO_APPLIED: 0,
    });
  })(req, res);
}

export function getPresenceValidationReviewSummary(req, res) {
  return wrap(async () => {
    const cand = loadPresenceValidationCandidates();
    if (!cand) {
      return fail(res, 404, "CANDIDATES_MISSING", "Candidates not generated yet");
    }
    return ok(res, { summary: summarizePresenceValidationReview() });
  })(req, res);
}

/**
 * Import ChatGPT assisted proposals — ASSISTED_PROPOSAL only, never human GT.
 */
export function postPresenceValidationAssistedImport(req, res) {
  return wrap(async () => {
    const body = req.body || {};
    const doc = body.document || body.import || body;
    if (!doc || typeof doc !== "object" || !Array.isArray(doc.proposals)) {
      return fail(res, 400, "IMPORT_DOCUMENT_REQUIRED", "JSON with proposals[] required");
    }
    const validation = validateAssistedProposalDocument(doc);
    if (!validation.ok) {
      return fail(res, 400, "IMPORT_VALIDATION_FAILED", validation.stopReason || "validation failed", {
        validation,
        HUMAN_FINAL_LABELS_CHANGED: 0,
        AUTO_APPLIED: 0,
      });
    }
    if (body.confirmImport !== true && body.apply !== true) {
      return ok(res, {
        preview: true,
        validation,
        AUTO_APPLIED: 0,
        HUMAN_FINAL_LABELS_CHANGED: 0,
        note: "Pass confirmImport:true to write assisted proposals (still not human ground truth).",
      });
    }
    const result = importAssistedProposals(doc, {
      sourceFile: body.sourceFile || null,
    });
    return ok(res, {
      ...result,
      summary: summarizePresenceValidationReview(),
    });
  })(req, res);
}

export function getPresenceValidationAssistedProposals(req, res) {
  return wrap(async () => {
    const store = loadAssistedProposals();
    return ok(res, {
      store: {
        proposalVersion: store.proposalVersion,
        sourceExport: store.sourceExport,
        importedAt: store.importedAt,
        proposalCount: Object.keys(store.proposals || {}).length,
        ASSISTED_PROPOSAL_IS_NOT_GROUND_TRUTH: true,
      },
      summary: summarizePresenceValidationReview(),
    });
  })(req, res);
}

/**
 * Classify assisted proposals for bulk approval (no label writes).
 */
export function getPresenceValidationBulkApprovalPreview(req, res) {
  return wrap(async () => {
    const useActiveScope =
      req.query?.scope === "all" || req.query?.unscoped === "1" ? false : true;
    const classification = classifyAssistedBulkApproval({
      useActiveScope,
      proposalVersion: req.query?.proposalVersion || undefined,
      batchId: req.query?.batchId || undefined,
    });
    return ok(res, {
      phase: "PRESENCE_ASSISTED_BULK_APPROVAL_READY",
      ...classification,
      BULK_APPROVAL_UI_READY: true,
      HUMAN_CONFIRMATION_REQUIRED: true,
      AUTO_APPLIED: 0,
      note: "No labels applied. Pass confirmToken=CONFIRM_BULK_APPROVAL to apply. Default scope = latest assisted import / active review batch.",
    });
  })(req, res);
}

/**
 * Apply bulk approval after explicit human confirmation.
 * Body: { reviewer, confirmToken: "CONFIRM_BULK_APPROVAL", caseIds?: [] }
 */
export function postPresenceValidationBulkApproval(req, res) {
  return wrap(async () => {
    const body = req.body || {};
    const result = applyAssistedBulkApproval({
      reviewer: body.reviewer,
      confirmToken: body.confirmToken,
      caseIds: body.caseIds || null,
    });
    return ok(res, {
      ...result,
      summary: summarizePresenceValidationReview(),
      defaultFilterAfter: "manual",
      AUTO_GROUND_TRUTH: 0,
    });
  })(req, res);
}

/**
 * GET export — filter-aware Markdown or JSON. No label mutation.
 * Query: format=md|json, mode=pending|filter, limit=25|50|100|all,
 *        status, provider, language, geography, candidateType, primary,
 *        download=1
 */
export function getPresenceValidationReviewExport(req, res) {
  return wrap(async () => {
    const format = String(req.query?.format || "json").toLowerCase();
    const mode = String(req.query?.mode || "filter").toLowerCase();
    const limit = parseExportLimit(req.query?.limit);
    const filters = filtersFromReq(req);
    const payload = buildPresenceValidationReviewExport({
      mode,
      limit,
      ...filters,
      writeAudit: true,
    });

    if (format === "md" || format === "markdown") {
      const md = formatPresenceValidationExportMarkdown(payload);
      if (String(req.query?.download || "") === "1") {
        const stamp = String(payload.exportedAt).replace(/[:.]/g, "-");
        res.setHeader("Content-Type", "text/markdown; charset=utf-8");
        res.setHeader(
          "Content-Disposition",
          `attachment; filename="presence-validation-review-${stamp}.md"`
        );
        return res.status(200).send(md);
      }
      return ok(res, {
        exportVersion: PRESENCE_VALIDATION_EXPORT_VERSION,
        format: "markdown",
        caseCount: payload.caseCount,
        uniqueResponseCount: payload.uniqueResponseCount,
        filters: payload.filters,
        markdown: md,
        AUTO_LABELING: false,
        ASSISTED_IMPORT_SUPPORTED: false,
      });
    }

    if (String(req.query?.download || "") === "1") {
      const stamp = String(payload.exportedAt).replace(/[:.]/g, "-");
      res.setHeader("Content-Type", "application/json; charset=utf-8");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="presence-validation-review-${stamp}.json"`
      );
      return res.status(200).send(JSON.stringify(payload, null, 2) + "\n");
    }

    return ok(res, {
      export: payload,
      AUTO_LABELING: false,
      ASSISTED_IMPORT_SUPPORTED: false,
    });
  })(req, res);
}

/**
 * Preview export counts for current filters without downloading.
 */
export function getPresenceValidationReviewExportPreview(req, res) {
  return wrap(async () => {
    const mode = String(req.query?.mode || "filter").toLowerCase();
    const limit = parseExportLimit(req.query?.limit);
    const filters = filtersFromReq(req);
    const payload = buildPresenceValidationReviewExport({
      mode,
      limit,
      ...filters,
      writeAudit: false,
    });
    return ok(res, {
      caseCount: payload.caseCount,
      uniqueResponseCount: payload.uniqueResponseCount,
      filters: payload.filters,
      mode: payload.mode,
      limit,
    });
  })(req, res);
}
