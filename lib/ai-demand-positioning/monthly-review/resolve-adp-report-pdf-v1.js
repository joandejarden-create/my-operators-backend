/**
 * Canonical ADP Performance Review PDF resolver.
 *
 * Admin View PDF / Download / Review PDF / historical Open PDF all resolve
 * through this module — never window.print(), never Admin DOM print, never
 * current-report web-print PDF (`current-report-pdf`).
 *
 * Doctrine:
 *   SINGLE_CANONICAL_AI_DEMAND_PERFORMANCE_REVIEW_PDF_RESOLVER_V1
 *   AI_DEMAND_ADMIN_PDF_IS_PERFORMANCE_REVIEW_ARTIFACT
 *   AI_DEMAND_ADMIN_PDF_NEVER_FALLS_BACK_TO_PLATFORM_PRINT
 */

import fs from "node:fs";
import {
  loadReviewPayload,
  pdfFingerprint,
} from "./admin/archive-store-v1.js";
import { PDF_STATUS } from "./admin/report-status-v1.js";
import { AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1 } from "./ai-demand-performance-review-template-v1.js";

export const SINGLE_CANONICAL_AI_DEMAND_PERFORMANCE_REVIEW_PDF_RESOLVER_V1 =
  "SINGLE_CANONICAL_AI_DEMAND_PERFORMANCE_REVIEW_PDF_RESOLVER_V1";
export const AI_DEMAND_ADMIN_PDF_IS_PERFORMANCE_REVIEW_ARTIFACT =
  "AI_DEMAND_ADMIN_PDF_IS_PERFORMANCE_REVIEW_ARTIFACT";
export const AI_DEMAND_ADMIN_PDF_NEVER_FALLS_BACK_TO_PLATFORM_PRINT =
  "AI_DEMAND_ADMIN_PDF_NEVER_FALLS_BACK_TO_PLATFORM_PRINT";

const API_BASE = "/api/admin/ai-demand-positioning/monthly-reviews";

/**
 * @param {{
 *   reviewId?: string|null,
 *   propertyId?: string|null,
 *   publishedPeriodId?: string|null,
 *   requirePublishedPeriodMatch?: boolean,
 * }} opts
 * @returns {{
 *   ok: boolean,
 *   available: boolean,
 *   reason: string|null,
 *   reviewId: string|null,
 *   propertyId: string|null,
 *   periodId: string|null,
 *   reportingMonthKey: string|null,
 *   versionNumber: number|null,
 *   versionLabel: string|null,
 *   generatedAt: string|null,
 *   pdfStatus: string|null,
 *   pdfFingerprint: string|null,
 *   pdfBytes: number|null,
 *   pdfAbsolutePath: string|null,
 *   viewUrl: string|null,
 *   downloadUrl: string|null,
 *   contentType: 'application/pdf',
 *   templateId: string,
 *   gate: string,
 *   matchesPublishedPeriod: boolean|null,
 * }}
 */
export function resolveAdpReportPdf(opts = {}) {
  const reviewId = String(opts.reviewId || "").trim() || null;
  const publishedPeriodId = opts.publishedPeriodId
    ? String(opts.publishedPeriodId).trim()
    : null;
  const requireMatch = opts.requirePublishedPeriodMatch === true;

  const base = {
    ok: true,
    available: false,
    reason: null,
    reviewId,
    propertyId: opts.propertyId ? String(opts.propertyId).trim() : null,
    periodId: null,
    reportingMonthKey: null,
    versionNumber: null,
    versionLabel: null,
    generatedAt: null,
    pdfStatus: null,
    pdfFingerprint: null,
    pdfBytes: null,
    pdfAbsolutePath: null,
    viewUrl: null,
    downloadUrl: null,
    contentType: "application/pdf",
    templateId: AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1,
    gate: SINGLE_CANONICAL_AI_DEMAND_PERFORMANCE_REVIEW_PDF_RESOLVER_V1,
    matchesPublishedPeriod: null,
  };

  if (!reviewId) {
    return {
      ...base,
      reason: "review_id_required",
    };
  }

  let pack;
  try {
    pack = loadReviewPayload(reviewId);
  } catch (err) {
    return {
      ...base,
      reason: `review_load_failed:${String(err?.message || err).slice(0, 120)}`,
    };
  }
  if (!pack) {
    return { ...base, reason: "review_not_found" };
  }

  const meta = pack.meta || {};
  const periodId =
    meta.sourceCurrentPeriodId ||
    pack.review?.editionLock?.periodId ||
    null;
  const pdfPath = pack.paths?.reportPdf || null;
  const pdfExists = Boolean(pdfPath && fs.existsSync(pdfPath));
  const pdfStatus = pdfExists
    ? PDF_STATUS.READY
    : meta.pdfStatus || PDF_STATUS.MISSING;
  const fingerprint = pdfExists
    ? meta.pdfFingerprint || pdfFingerprint(pdfPath)
    : null;
  const pdfBytes = pdfExists ? fs.statSync(pdfPath).size : null;

  const matchesPublishedPeriod =
    publishedPeriodId && periodId
      ? String(periodId) === String(publishedPeriodId)
      : publishedPeriodId && periodId
        ? false
        : null;

  if (requireMatch && publishedPeriodId && matchesPublishedPeriod === false) {
    return {
      ...base,
      propertyId: meta.propertyId || base.propertyId,
      periodId,
      reportingMonthKey: meta.reportingMonthKey || null,
      versionNumber: meta.versionNumber ?? null,
      versionLabel: meta.versionLabel || (meta.versionNumber != null ? `V${meta.versionNumber}` : null),
      generatedAt: meta.generatedAt || null,
      pdfStatus,
      pdfFingerprint: fingerprint,
      pdfBytes,
      pdfAbsolutePath: pdfExists ? pdfPath : null,
      matchesPublishedPeriod: false,
      reason: "performance_review_pdf_stale_vs_published_period",
    };
  }

  if (!pdfExists || (pdfStatus !== PDF_STATUS.READY && pdfStatus !== "READY")) {
    return {
      ...base,
      propertyId: meta.propertyId || base.propertyId,
      periodId,
      reportingMonthKey: meta.reportingMonthKey || null,
      versionNumber: meta.versionNumber ?? null,
      versionLabel: meta.versionLabel || (meta.versionNumber != null ? `V${meta.versionNumber}` : null),
      generatedAt: meta.generatedAt || null,
      pdfStatus,
      matchesPublishedPeriod,
      reason: "pdf_not_available",
    };
  }

  const viewUrl = `${API_BASE}/${encodeURIComponent(reviewId)}/pdf`;
  const downloadUrl = `${viewUrl}?download=1`;

  return {
    ...base,
    available: true,
    reason: null,
    propertyId: meta.propertyId || base.propertyId,
    periodId,
    reportingMonthKey: meta.reportingMonthKey || null,
    versionNumber: meta.versionNumber ?? null,
    versionLabel:
      meta.versionLabel ||
      (meta.versionNumber != null ? `V${meta.versionNumber}` : null),
    generatedAt: meta.generatedAt || null,
    pdfStatus: PDF_STATUS.READY,
    pdfFingerprint: fingerprint,
    pdfBytes,
    pdfAbsolutePath: pdfPath,
    viewUrl,
    downloadUrl,
    matchesPublishedPeriod,
  };
}
