#!/usr/bin/env node
/**
 * Deterministic rebuild of AI Demand Performance Review + PDF for Aimbridge CALA
 * properties from existing durable published ADP reports (LIVE_PROVIDER_CALLS=0).
 *
 * Does not authorize customer publication.
 */
import "dotenv/config";
import { generateNewDraft } from "../lib/ai-demand-positioning/monthly-review/admin/generate-review-v1.js";
import { renderAndAttachMonthlyReviewPdfV1 } from "../lib/ai-demand-positioning/monthly-review/admin/render-and-attach-monthly-review-pdf-v1.mjs";
import { buildPublishedAdpReviewCoverageV1 } from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";
import { ADP_AIMBRIDGE_CALA_INTERNAL_HOTELS } from "../lib/ai-demand-positioning/governance/adp-aimbridge-cala-internal-inventory-v1.js";

const BASE_URL = (process.env.ADP_PDF_BASE_URL || "http://localhost:8080").replace(
  /\/$/,
  ""
);

const results = [];

for (const hotel of ADP_AIMBRIDGE_CALA_INTERNAL_HOTELS) {
  const propertyId = hotel.propertyId;
  const row = {
    propertyId,
    name: hotel.name,
    baselinePeriod: hotel.measurementPeriodId,
    draft: null,
    pdf: null,
    coverageAfter: null,
    error: null,
  };
  try {
    const draft = generateNewDraft(propertyId, {
      generatedBy: "aimbridge_cala_deterministic_rebuild_v1",
      warnings: [
        "Rebuilt from durable published ADP analytics — no live provider calls",
        "Not customer-publication authorized",
        ...((hotel.outstandingWork || []).map((w) => `outstanding:${w}`)),
      ],
    });
    row.draft = {
      ok: draft.ok === true,
      reviewId: draft.newReviewId || null,
      error: draft.error || null,
      liveProviderCalls: draft.liveProviderCalls,
    };
    if (!draft.ok || !draft.newReviewId) {
      results.push(row);
      continue;
    }
    const pdf = await renderAndAttachMonthlyReviewPdfV1({
      reviewId: draft.newReviewId,
      propertyId,
      baseUrl: BASE_URL,
    });
    row.pdf = {
      ok: pdf.ok === true,
      alreadyAttached: pdf.alreadyAttached === true,
      viewUrl: pdf.viewUrl || null,
      downloadUrl: pdf.downloadUrl || null,
      pdfBytes: pdf.pdfBytes || null,
    };
  } catch (err) {
    row.error = String(err?.message || err).slice(0, 300);
  }
  results.push(row);
}

const cov = buildPublishedAdpReviewCoverageV1({});
const ids = new Set(ADP_AIMBRIDGE_CALA_INTERNAL_HOTELS.map((h) => h.propertyId));
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
  ok: results.every((r) => r.draft?.ok && r.pdf?.ok && r.coverageAfter?.status === "READY"),
  liveProviderCalls: 0,
  baseUrl: BASE_URL,
  publishedCount: cov.publishedCount,
  reviewReadyCount: cov.reviewReadyCount,
  needsRebuildCount: (cov.properties || []).filter(
    (p) => p.coverageStatus === "NEEDS_REBUILD"
  ).length,
  blockedCount: cov.blockedCount,
  cala: results,
  remainingNeedsRebuild: (cov.properties || [])
    .filter((p) => p.coverageStatus === "NEEDS_REBUILD" && !ids.has(p.propertyId))
    .map((p) => ({
      propertyId: p.propertyId,
      name: p.propertyName || p.name,
      reason: p.coverageReason,
    })),
};

console.log(JSON.stringify(summary, null, 2));
process.exit(summary.ok ? 0 : 1);
