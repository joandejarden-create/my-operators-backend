#!/usr/bin/env node
/**
 * Attach Performance Review PDFs for already-generated Aimbridge CALA drafts.
 * LIVE_PROVIDER_CALLS=0. Does not authorize customer publication.
 */
import "dotenv/config";
import { renderAndAttachMonthlyReviewPdfV1 } from "../lib/ai-demand-positioning/monthly-review/admin/render-and-attach-monthly-review-pdf-v1.mjs";
import { buildPublishedAdpReviewCoverageV1 } from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";

const BASE_URL = (process.env.ADP_PDF_BASE_URL || "http://localhost:8080").replace(
  /\/$/,
  ""
);

const JOBS = [
  {
    propertyId: "adp_sheraton_guadalajara_expo",
    reviewId: "adp_mr_sheraton_guadalajara_exp_2026-11_v1",
  },
  {
    propertyId: "adp_wyndham_alltra_punta_cana",
    reviewId: "adp_mr_wyndham_alltra_punta_can_2026-11_v1",
  },
  {
    propertyId: "adp_voco_cancun_zona_hotelera",
    reviewId: "adp_mr_voco_cancun_zona_hoteler_2026-11_v1",
  },
];

const results = [];
for (const job of JOBS) {
  const row = { ...job, pdf: null, error: null, coverageAfter: null };
  try {
    row.pdf = await renderAndAttachMonthlyReviewPdfV1({
      reviewId: job.reviewId,
      propertyId: job.propertyId,
      baseUrl: BASE_URL,
    });
  } catch (err) {
    row.error = String(err?.message || err).slice(0, 500);
  }
  results.push(row);
}

const cov = buildPublishedAdpReviewCoverageV1({});
for (const row of results) {
  const c = (cov.properties || []).find((p) => p.propertyId === row.propertyId);
  row.coverageAfter = c
    ? {
        status: c.coverageStatus,
        reason: c.coverageReason,
        pdfStatus: c.pdfStatus || null,
        reviewId: c.latestReviewId || c.reviewId || null,
      }
    : null;
}

const summary = {
  ok: results.every(
    (r) => r.pdf?.ok && r.coverageAfter?.status === "READY"
  ),
  baseUrl: BASE_URL,
  reviewReadyCount: cov.reviewReadyCount,
  needsRebuildCount: (cov.properties || []).filter(
    (p) => p.coverageStatus === "NEEDS_REBUILD"
  ).length,
  results,
};
console.log(JSON.stringify(summary, null, 2));
process.exit(summary.ok ? 0 : 1);
