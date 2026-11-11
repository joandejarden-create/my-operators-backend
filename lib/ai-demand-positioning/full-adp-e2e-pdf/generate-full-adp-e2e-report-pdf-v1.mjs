/**
 * Full ADP E2E executive PDF — canonical portfolio production line.
 *
 * Same process as original 31-hotel archive PDFs (Bethesda / Mayflower / …):
 *   buildMonthlyExecutiveReviewV1 (staged + Full ADP metric enrichments)
 *   → persistNewReviewVersion (immutable archive)
 *   → generateMonthlyReviewPdfV1 (AdpMonthlyReviewReportPdfV1 + report-system CSS)
 *   → attachPdfToReview
 *   → registerFullAdpE2ePdfArtifact (AI Admin Full ADP PDF action)
 *
 * NOT a Playwright print of the dark interactive owner dashboard.
 * NOT a parallel greenfield renderer.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  registerFullAdpE2ePdfArtifact,
  fullAdpE2ePdfPaths,
  ADP_FULL_E2E_PDF_ARTIFACT_STORE_V1,
  sha256File,
} from "./adp-full-e2e-pdf-artifact-store-v1.js";
import {
  ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1,
  buildFullAdpExecutiveReportFromStagedV1,
  PORTFOLIO_CANONICAL_COVER_SUBTITLE,
} from "./adp-executive-report-pdf-bethesda-master-v1.js";
import { generateMonthlyReviewPdfV1 } from "../monthly-review/generate-monthly-review-pdf-v1.mjs";
import { persistNewReviewVersion } from "../monthly-review/admin/archive-store-v1.js";
import { attachPdfToReview } from "../monthly-review/admin/generate-review-v1.js";
import {
  GENERATION_REASON,
  GENERATION_STATUS,
  REVIEW_STATUS,
} from "../monthly-review/admin/report-status-v1.js";
import {
  AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1,
  SINGLE_CANONICAL_AI_DEMAND_PERFORMANCE_REVIEW_RENDERER,
} from "../monthly-review/ai-demand-performance-review-template-v1.js";

export const ADP_CANONICAL_PORTFOLIO_PDF_PROCESS_V1 =
  "ADP_CANONICAL_PORTFOLIO_PDF_PROCESS_V1";

/**
 * @param {{
 *   baseUrl?: string,
 *   propertyId: string,
 *   stagedVersionId?: string|null,
 *   outPath?: string|null,
 *   generatedBy?: string,
 *   register?: boolean,
 *   replaceWithHistory?: boolean,
 *   skipArchivePersist?: boolean,
 *   existingReviewId?: string|null,
 * }} opts
 */
export async function generateFullAdpE2eReportPdfV1(opts = {}) {
  const baseUrl = String(
    opts.baseUrl ||
      process.env.ADP_PDF_BASE_URL ||
      process.env.PUBLIC_BASE_URL ||
      "http://127.0.0.1:8080"
  ).replace(/\/$/, "");
  const propertyId = String(opts.propertyId || "").trim();
  if (!propertyId) throw new Error("propertyId_required");

  const built = buildFullAdpExecutiveReportFromStagedV1(propertyId, {
    expectedStagedVersionId: opts.stagedVersionId || null,
  });
  const { review, stagedVersionId, periodId } = built;

  // Hard lock: portfolio cover label only.
  review.coverSubtitle = PORTFOLIO_CANONICAL_COVER_SUBTITLE;
  if (review._adpExecutivePdf) {
    review._adpExecutivePdf.coverSubtitle = PORTFOLIO_CANONICAL_COVER_SUBTITLE;
    review._adpExecutivePdf.productionLine =
      "buildMonthlyExecutiveReviewV1 → archive persist → generateMonthlyReviewPdfV1";
  }

  let reviewId = opts.existingReviewId || null;
  let archiveMeta = null;

  if (!opts.skipArchivePersist && !reviewId) {
    const persisted = persistNewReviewVersion({
      review,
      generationReason: GENERATION_REASON.GENERATE_NEW_DRAFT,
      generatedBy:
        opts.generatedBy || "canonical_portfolio_full_adp_pdf_process_v1",
      reviewStatus: REVIEW_STATUS.READY_FOR_REVIEW,
      generationStatus: GENERATION_STATUS.SUCCESS,
      liveProviderCalls: 0,
      warnings: [
        "ADP_CANONICAL_PORTFOLIO_PDF_PROCESS_V1",
        `stagedVersionId=${stagedVersionId}`,
      ],
    });
    reviewId = persisted.meta.reviewId;
    archiveMeta = persisted.meta;
  }

  if (!reviewId) {
    throw new Error("archive_reviewId_required_for_canonical_pdf_process");
  }

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "adp-canonical-pdf-"));
  const tmpPdf = path.join(tmpDir, "report.pdf");

  const rendered = await generateMonthlyReviewPdfV1({
    baseUrl,
    propertyId,
    source: "archive",
    reviewId,
    outPath: tmpPdf,
  });

  if (!fs.existsSync(tmpPdf) || fs.statSync(tmpPdf).size < 5000) {
    throw new Error("pdf_render_failed_or_empty");
  }

  // Reject accidental dashboard print shells
  const pdfjs = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const data = new Uint8Array(fs.readFileSync(tmpPdf));
  const doc = await pdfjs.getDocument({ data }).promise;
  let text = "";
  for (let i = 1; i <= doc.numPages; i += 1) {
    const pg = await doc.getPage(i);
    const content = await pg.getTextContent();
    text += ` ${content.items.map((x) => x.str).join(" ")}`;
  }
  if (/Load Report|Staged review requested but not authorized/i.test(text)) {
    throw new Error("DASHBOARD_SHELL_DETECTED_IN_PDF");
  }
  if (/AI Demand Positioning/i.test(text) && !/AI Demand Performance Review/i.test(text)) {
    throw new Error("DASHBOARD_POSITIONING_SHELL_DETECTED_IN_PDF");
  }
  if (!/Executive Performance|AI Demand Performance Review/i.test(text)) {
    throw new Error("BETHESDA_EXECUTIVE_SECTIONS_MISSING");
  }
  const collapsed = text.replace(/\s+/g, "");
  if (/FULLADPEXECUTIVEREPORT/i.test(collapsed)) {
    throw new Error("NON_PORTFOLIO_FULL_ADP_COVER_SUBTITLE");
  }
  if (!/MONTHLYEXECUTIVEREVIEW/i.test(collapsed)) {
    throw new Error("MISSING_PORTFOLIO_MONTHLY_EXECUTIVE_REVIEW_COVER");
  }

  try {
    attachPdfToReview(reviewId, tmpPdf);
  } catch (err) {
    if (err?.code !== "ADP_MONTHLY_REVIEW_VERSION_IMMUTABILITY") {
      throw err;
    }
  }

  const outPath =
    opts.outPath ||
    fullAdpE2ePdfPaths(propertyId, stagedVersionId).reportPdf + ".building.pdf";
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  if (path.resolve(outPath) !== path.resolve(tmpPdf)) {
    fs.copyFileSync(tmpPdf, outPath);
  }

  let registered = null;
  if (opts.register !== false) {
    registered = registerFullAdpE2ePdfArtifact({
      propertyId,
      stagedVersionId,
      pdfAbsolutePath: outPath,
      periodId,
      generatedBy:
        opts.generatedBy || "canonical_portfolio_full_adp_pdf_process_v1",
      sourceNote:
        "Canonical portfolio PDF process (archive + generateMonthlyReviewPdfV1). Not dashboard print.",
      replaceWithHistory: opts.replaceWithHistory === true,
      templateId: AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1,
      pageCount: doc.numPages,
      renderer:
        rendered?.renderer ||
        SINGLE_CANONICAL_AI_DEMAND_PERFORMANCE_REVIEW_RENDERER,
      visualQaStatus: "PENDING_FOUNDER_DESIGN_ACCEPTANCE",
    });
  }

  return {
    ok: true,
    propertyId,
    stagedVersionId,
    periodId,
    reviewId,
    archiveMeta,
    outPath,
    pages: doc.numPages,
    pdfSha256: sha256File(outPath),
    templateId: AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1,
    designAuthority: ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1,
    renderer:
      rendered?.renderer ||
      SINGLE_CANONICAL_AI_DEMAND_PERFORMANCE_REVIEW_RENDERER,
    productionLine: ADP_CANONICAL_PORTFOLIO_PDF_PROCESS_V1,
    coverSubtitle: PORTFOLIO_CANONICAL_COVER_SUBTITLE,
    pageMeta: rendered?.structural || null,
    registered,
    liveProviderCalls: 0,
    gate: ADP_FULL_E2E_PDF_ARTIFACT_STORE_V1,
  };
}
