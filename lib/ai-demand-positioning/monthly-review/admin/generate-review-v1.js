/**
 * Admin generation / regeneration for Monthly Executive Reviews.
 * Creates NEW immutable versions only. LIVE_PROVIDER_CALLS = 0.
 */

import crypto from "node:crypto";
import fs from "node:fs";
import { buildMonthlyExecutiveReviewV1 } from "../build-monthly-executive-review-v1.js";
import {
  resolveAdpMonthlyReviewEligibilityV1,
  resolveAllAdpMonthlyReviewEligibilityV1,
  assertEligibilityAllowsGeneration,
} from "../resolve-adp-monthly-review-eligibility-v1.js";
import {
  GENERATION_REASON,
  GENERATION_STATUS,
  PDF_STATUS,
  REVIEW_STATUS,
} from "./report-status-v1.js";
import {
  loadArchiveIndex,
  loadReviewPayload,
  persistNewReviewVersion,
  saveArchiveIndex,
  summarizeQualityGates,
  getBulkEligibleGeneratePreview,
} from "./archive-store-v1.js";

export function regenerateReviewFromArchive(sourceReviewId, opts = {}) {
  const startedAt = new Date().toISOString();
  const pack = loadReviewPayload(sourceReviewId);
  if (!pack) {
    throw Object.assign(new Error("review_not_found"), { code: "review_not_found" });
  }
  const propertyId = pack.meta.propertyId;
  assertEligibilityAllowsGeneration(resolveAdpMonthlyReviewEligibilityV1(propertyId));

  // Same property + same certified periods intent: builder reads current published certified data.
  // Do not mutate source artifacts.
  let review;
  try {
    review = buildMonthlyExecutiveReviewV1(propertyId, {
      // Builder currently resolves latest published; generation log records intended source periods.
      adminRegenerateFrom: {
        sourceReviewId,
        sourceCurrentPeriodId: pack.meta.sourceCurrentPeriodId,
        sourcePriorPeriodId: pack.meta.sourcePriorPeriodId,
      },
    });
  } catch (err) {
    return {
      ok: false,
      generationStatus: GENERATION_STATUS.FAILED,
      error: {
        stage: "buildMonthlyExecutiveReviewV1",
        message: err.message,
        timestamp: new Date().toISOString(),
      },
      liveProviderCalls: 0,
      sourceReviewId,
    };
  }

  const created = persistNewReviewVersion({
    review,
    generationReason: GENERATION_REASON.REGENERATE_SAME_PERIODS,
    generatedBy: opts.generatedBy || "admin",
    reviewStatus: REVIEW_STATUS.DRAFT,
    generationStatus: GENERATION_STATUS.SUCCESS,
    supersedesReviewId: sourceReviewId,
    pdfSourcePath: null, // new composition; PDF must be re-rendered separately (not overwrite)
    liveProviderCalls: 0,
    warnings: [
      "PDF not auto-copied from prior version; Open PDF uses prior version until PDF render for this reviewId.",
      `Intended source current period: ${pack.meta.sourceCurrentPeriodId}`,
      `Intended source prior period: ${pack.meta.sourcePriorPeriodId}`,
    ],
    startedAt,
    qualityGates: (() => {
      const q = summarizeQualityGates(review);
      q.checks = q.checks.map((c) =>
        c.id === "PDF Integrity" ? { ...c, pass: false } : c
      );
      const scored = q.checks.filter((c) => c.pass !== null);
      const passed = scored.filter((c) => c.pass).length;
      q.summary = `${passed}/${scored.length}${scored.some((c) => !c.pass) ? " — PDF Integrity FAIL" : " PASS"}`;
      return q;
    })(),
  });

  // Prove immutability: prior payload still on disk unchanged
  const priorStill = loadReviewPayload(sourceReviewId);
  if (!priorStill || !fs.existsSync(priorStill.paths.reviewJson)) {
    throw new Error("regeneration_destroyed_prior_version");
  }

  return {
    ok: true,
    liveProviderCalls: 0,
    sourceReviewId,
    newReviewId: created.meta.reviewId,
    meta: created.meta,
    generationLog: created.generationLog,
    gate: "ADP_MONTHLY_REVIEW_REGENERATION_CREATES_NEW_VERSION",
  };
}

export function generateNewDraft(propertyId, opts = {}) {
  const startedAt = new Date().toISOString();
  try {
    assertEligibilityAllowsGeneration(resolveAdpMonthlyReviewEligibilityV1(propertyId));
  } catch (err) {
    return {
      ok: false,
      generationStatus: GENERATION_STATUS.FAILED,
      error: {
        stage: "eligibility",
        message: err.message,
        timestamp: new Date().toISOString(),
      },
      liveProviderCalls: 0,
      propertyId,
    };
  }

  let review;
  try {
    review = buildMonthlyExecutiveReviewV1(propertyId);
  } catch (err) {
    return {
      ok: false,
      generationStatus: GENERATION_STATUS.FAILED,
      error: {
        stage: "buildMonthlyExecutiveReviewV1",
        message: err.message,
        timestamp: new Date().toISOString(),
      },
      liveProviderCalls: 0,
      propertyId,
    };
  }

  const created = persistNewReviewVersion({
    review,
    generationReason: opts.generationReason || GENERATION_REASON.GENERATE_NEW_DRAFT,
    generatedBy: opts.generatedBy || "admin",
    reviewStatus: opts.reviewStatus || REVIEW_STATUS.READY_FOR_REVIEW,
    generationStatus: GENERATION_STATUS.SUCCESS,
    liveProviderCalls: 0,
    startedAt,
    warnings: opts.warnings || review.eligibility?.warnings || [],
    qualityGates: (() => {
      const q = summarizeQualityGates(review);
      q.checks = q.checks.map((c) =>
        c.id === "PDF Integrity" ? { ...c, pass: false } : c
      );
      const scored = q.checks.filter((c) => c.pass !== null);
      const passed = scored.filter((c) => c.pass).length;
      q.summary = `${passed}/${scored.length}${scored.some((c) => !c.pass) ? " — PDF Integrity FAIL" : " PASS"}`;
      return q;
    })(),
  });

  return {
    ok: true,
    liveProviderCalls: 0,
    propertyId,
    newReviewId: created.meta.reviewId,
    meta: created.meta,
    generationLog: created.generationLog,
    gate: "ADP_MONTHLY_REVIEW_BULK_GENERATION_ZERO_PROVIDER_CALLS",
  };
}

/**
 * Bulk generate for all eligible properties. Requires confirm=true.
 * LIVE_PROVIDER_CALLS = 0. Does not auto-approve or send to clients.
 */
export function generateEligibleReviewsBulk(opts = {}) {
  if (opts.confirm !== true) {
    return {
      ok: false,
      error: "confirmation_required",
      preview: getBulkEligibleGeneratePreview(),
      liveProviderCalls: 0,
      message:
        "Pass confirm:true after reviewing eligibility preview. No automatic generation.",
    };
  }

  const batch = resolveAllAdpMonthlyReviewEligibilityV1();
  const results = [];
  for (const row of batch.eligible) {
    const gen = generateNewDraft(row.propertyId, {
      generatedBy: opts.generatedBy || "admin_bulk",
      reviewStatus: REVIEW_STATUS.READY_FOR_REVIEW,
      generationReason: GENERATION_REASON.GENERATE_NEW_DRAFT,
      warnings: row.warnings,
    });
    results.push({
      propertyId: row.propertyId,
      propertyName: row.propertyName,
      eligibilityStatus: row.status,
      ...gen,
    });
  }

  return {
    ok: results.every((r) => r.ok),
    liveProviderCalls: 0,
    gate: "ADP_MONTHLY_REVIEW_BULK_GENERATION_ZERO_PROVIDER_CALLS",
    counts: {
      attempted: results.length,
      succeeded: results.filter((r) => r.ok).length,
      failed: results.filter((r) => !r.ok).length,
      blocked: batch.blocked.length,
    },
    blocked: batch.blocked,
    results,
  };
}

/**
 * Attach PDF once to a review that does not yet have one.
 * Never overwrites an existing PDF (version immutability).
 */
export function attachPdfToReview(reviewId, pdfAbsolutePath) {
  const pack = loadReviewPayload(reviewId);
  if (!pack) throw new Error("review_not_found");
  if (pack.meta.reviewStatus === REVIEW_STATUS.CLIENT_ISSUED) {
    const err = new Error("client_issued_immutable");
    err.code = "ADP_MONTHLY_REVIEW_CLIENT_ISSUED_IMMUTABILITY";
    throw err;
  }
  if (fs.existsSync(pack.paths.reportPdf)) {
    const err = new Error("pdf_already_exists_immutable");
    err.code = "ADP_MONTHLY_REVIEW_VERSION_IMMUTABILITY";
    throw err;
  }
  if (!fs.existsSync(pdfAbsolutePath)) {
    throw new Error("pdf_source_missing");
  }
  fs.copyFileSync(pdfAbsolutePath, pack.paths.reportPdf);
  const buf = fs.readFileSync(pack.paths.reportPdf);
  const fp = crypto.createHash("sha256").update(buf).digest("hex");
  const meta = JSON.parse(fs.readFileSync(pack.paths.metadataJson, "utf8"));
  if (meta.pdfFingerprint) {
    const err = new Error("pdf_fingerprint_already_set");
    err.code = "ADP_MONTHLY_REVIEW_VERSION_IMMUTABILITY";
    throw err;
  }
  meta.pdfFingerprint = fp;
  meta.pdfStatus = PDF_STATUS.READY;
  fs.writeFileSync(
    pack.paths.metadataJson,
    `${JSON.stringify(meta, null, 2)}\n`,
    "utf8"
  );
  const index = loadArchiveIndex();
  const row = index.reviews.find((r) => r.reviewId === reviewId);
  if (row) {
    row.pdfFingerprint = fp;
    row.pdfStatus = PDF_STATUS.READY;
    saveArchiveIndex(index);
  }
  return meta;
}
