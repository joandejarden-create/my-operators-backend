/**
 * Shared Performance Review PDF → archive registration.
 *
 * generate draft JSON
 *   → render PDF (Playwright)
 *   → attachPdfToReview (archive report.pdf + index pdfStatus READY)
 *   → Reviews tab coverage resolves hasPdf
 *
 * Never Hilton-specific. Never overwrites an existing archive PDF.
 */

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { generateMonthlyReviewPdfV1 } from "../generate-monthly-review-pdf-v1.mjs";
import { attachPdfToReview } from "./generate-review-v1.js";
import { loadReviewPayload } from "./archive-store-v1.js";
import { resolveAdpReportPdf } from "../resolve-adp-report-pdf-v1.js";

function defaultBaseUrl() {
  return (
    process.env.ADP_PDF_BASE_URL ||
    process.env.PUBLIC_BASE_URL ||
    "http://localhost:8080"
  ).replace(/\/$/, "");
}

/**
 * @param {{
 *   reviewId: string,
 *   propertyId?: string,
 *   baseUrl?: string,
 *   pdfSourcePath?: string|null,
 *   skipRender?: boolean,
 * }} opts
 */
export async function renderAndAttachMonthlyReviewPdfV1(opts = {}) {
  const reviewId = String(opts.reviewId || "").trim();
  if (!reviewId) throw new Error("reviewId_required");

  const pack = loadReviewPayload(reviewId);
  if (!pack) throw new Error("review_not_found");

  const propertyId = String(opts.propertyId || pack.meta.propertyId || "").trim();
  if (!propertyId) throw new Error("propertyId_required");

  if (fs.existsSync(pack.paths.reportPdf)) {
    const resolved = resolveAdpReportPdf({ reviewId, propertyId });
    return {
      ok: true,
      alreadyAttached: true,
      reviewId,
      propertyId,
      meta: pack.meta,
      pdfAbsolutePath: pack.paths.reportPdf,
      viewUrl: resolved.viewUrl,
      downloadUrl: resolved.downloadUrl,
      pdfBytes: resolved.pdfBytes,
    };
  }

  let sourcePath = opts.pdfSourcePath
    ? path.resolve(String(opts.pdfSourcePath))
    : null;

  let rendered = null;
  if (!sourcePath || !fs.existsSync(sourcePath)) {
    if (opts.skipRender === true) {
      throw new Error("pdf_source_missing");
    }
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "adp-mr-pdf-"));
    const tmpPdf = path.join(tmpDir, "report.pdf");
    rendered = await generateMonthlyReviewPdfV1({
      baseUrl: opts.baseUrl || defaultBaseUrl(),
      propertyId,
      source: "archive",
      reviewId,
      outPath: tmpPdf,
    });
    sourcePath = tmpPdf;
  }

  const meta = attachPdfToReview(reviewId, sourcePath);
  const resolved = resolveAdpReportPdf({ reviewId, propertyId });

  return {
    ok: true,
    alreadyAttached: false,
    reviewId,
    propertyId,
    meta,
    pdfAbsolutePath: resolved.pdfAbsolutePath,
    viewUrl: resolved.viewUrl,
    downloadUrl: resolved.downloadUrl,
    pdfBytes: resolved.pdfBytes,
    filename: rendered?.filename || null,
    gate: "ADP_MONTHLY_REVIEW_PDF_ATTACHED_TO_ARCHIVE_V1",
  };
}
