/**
 * Recommendation Taxonomy Review API — 52 flagged DEV cases only.
 * No auto-apply. No holdout. No provider calls.
 */

import {
  buildTaxonomyReviewQueue,
  setTaxonomyReviewDecision,
  acceptAllValidTaxonomyProposals,
  previewTaxonomyReviewApply,
  applyTaxonomyReviewDecisions,
  getTaxonomyReviewReadySummary,
  validateTaxonomyReviewArtifact,
  TAXONOMY_REVIEW_VERSION,
  TAXONOMY_REVIEW_ACTIONS,
} from "../lib/ai-visibility/validation/recommendation-taxonomy-review.js";

function ok(res, data) {
  return res.json({
    ok: true,
    success: true,
    apiVersion: TAXONOMY_REVIEW_VERSION,
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
      console.error("[taxonomy-review]", err?.message || err);
      const code = err?.code || "SERVER_ERROR";
      const map = {
        TAXONOMY_REVIEW_ARTIFACT_MISSING: 404,
        HOLDOUT_CASES_IN_TAXONOMY_REVIEW: 403,
        CASE_ID_REQUIRED: 400,
        INVALID_TAXONOMY_REVIEW_ACTION: 400,
        CASE_NOT_IN_TAXONOMY_REVIEW: 404,
        INVALID_EDITED_LABEL: 400,
        TAXONOMY_REVIEW_ALREADY_APPLIED: 409,
        REVIEWER_REQUIRED: 400,
        TAXONOMY_REVIEW_NOT_VALID: 400,
        EXPLICIT_APPLY_REQUIRED: 400,
        CONFIRM_TOKEN_REQUIRED: 400,
        TAXONOMY_APPLY_BLOCKED: 400,
        HOLDOUT_CASE_AMENDMENT_BLOCKED: 403,
        GOLDEN_SET_V2_MISSING: 404,
      };
      return fail(res, map[code] || 500, code, err.message || "Taxonomy review error", {
        details: err.details || undefined,
      });
    }
  };
}

export const getTaxonomyReviewReady = wrap(async (req, res) => {
  return ok(res, getTaxonomyReviewReadySummary());
});

export const getTaxonomyReviewValidate = wrap(async (req, res) => {
  return ok(res, validateTaxonomyReviewArtifact());
});

export const getTaxonomyReviewQueue = wrap(async (req, res) => {
  const filter = req.query.filter || "ALL";
  return ok(res, buildTaxonomyReviewQueue({ filter }));
});

export const postTaxonomyReviewDecision = wrap(async (req, res) => {
  const body = req.body || {};
  const result = setTaxonomyReviewDecision({
    caseId: body.caseId || req.params.caseId,
    action: body.action,
    editedLabel: body.editedLabel || null,
    notes: body.notes || null,
    reviewer:
      body.reviewer ||
      req.dealalityUser?.email ||
      req.memberstackAuth?.email ||
      null,
  });
  return ok(res, result);
});

export const postTaxonomyReviewAcceptAllProposals = wrap(async (req, res) => {
  const body = req.body || {};
  const result = acceptAllValidTaxonomyProposals({
    reviewer:
      body.reviewer ||
      req.dealalityUser?.email ||
      req.memberstackAuth?.email ||
      null,
    includeKeep: body.includeKeep === true,
  });
  return ok(res, result);
});

export const postTaxonomyReviewPreviewApply = wrap(async (req, res) => {
  return ok(res, previewTaxonomyReviewApply());
});

export const postTaxonomyReviewApply = wrap(async (req, res) => {
  const body = req.body || {};
  const result = applyTaxonomyReviewDecisions({
    explicitApply: body.explicitApply === true,
    confirmToken: body.confirmToken,
    reviewer:
      body.reviewer ||
      req.dealalityUser?.email ||
      req.memberstackAuth?.email ||
      null,
  });
  return ok(res, result);
});

export { TAXONOMY_REVIEW_ACTIONS };
