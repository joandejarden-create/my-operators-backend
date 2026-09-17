/**
 * Current ADP report PDF store (admin).
 * Separate from historical Monthly Review archive PDFs.
 *
 * Doctrine:
 *   ADP_PDF_IS_A_PRINT_REPRESENTATION_OF_THE_CURRENT_PUBLISHED_ADP_REPORT
 *   ADP_ADMIN_PDF_USES_CURRENT_REPORT_PRINT_RENDERER
 *   ADP_CURRENT_PDF_AND_ACTION_PLAN_DERIVE_FROM_SAME_PUBLISHED_EDITION
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { loadPublishedManifest, loadPublishedReport } from "../published-snapshot.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const ADP_PDF_IS_A_PRINT_REPRESENTATION_OF_THE_CURRENT_PUBLISHED_ADP_REPORT =
  "ADP_PDF_IS_A_PRINT_REPRESENTATION_OF_THE_CURRENT_PUBLISHED_ADP_REPORT";
export const ADP_ADMIN_PDF_USES_CURRENT_REPORT_PRINT_RENDERER =
  "ADP_ADMIN_PDF_USES_CURRENT_REPORT_PRINT_RENDERER";
export const ADP_CURRENT_PDF_AND_ACTION_PLAN_DERIVE_FROM_SAME_PUBLISHED_EDITION =
  "ADP_CURRENT_PDF_AND_ACTION_PLAN_DERIVE_FROM_SAME_PUBLISHED_EDITION";
export const ADP_SINGLE_CANONICAL_PRIORITY_ACTION_SET =
  "ADP_SINGLE_CANONICAL_PRIORITY_ACTION_SET";
export const ADP_ACTIONS_LIVE_PDF_ADMIN_CSV_PARITY =
  "ADP_ACTIONS_LIVE_PDF_ADMIN_CSV_PARITY";

export const CURRENT_REPORT_PDF_ROOT = path.join(
  ROOT,
  "reports/ai-demand-positioning/current-report-pdf"
);

export function currentReportPdfPaths(propertyId) {
  const dir = path.join(CURRENT_REPORT_PDF_ROOT, String(propertyId || "").trim());
  return {
    dir,
    reportPdf: path.join(dir, "report.pdf"),
    metaJson: path.join(dir, "meta.json"),
  };
}

export function loadCurrentReportPdfMeta(propertyId) {
  const { metaJson } = currentReportPdfPaths(propertyId);
  if (!fs.existsSync(metaJson)) return null;
  return JSON.parse(fs.readFileSync(metaJson, "utf8"));
}

export function hasCurrentReportPdf(propertyId) {
  const { reportPdf, metaJson } = currentReportPdfPaths(propertyId);
  return fs.existsSync(reportPdf) && fs.existsSync(metaJson);
}

/**
 * READY when a current PDF exists for this property and its period matches
 * the latest published edition (when period is known).
 */
export function resolveCurrentReportPdfStatus(propertyId) {
  const paths = currentReportPdfPaths(propertyId);
  if (!fs.existsSync(paths.reportPdf)) {
    return {
      hasPdf: false,
      pdfStatus: "MISSING",
      coverageStatus: "NEEDS_REBUILD",
      coverageReason: "current_adp_pdf_missing",
      meta: null,
      paths,
    };
  }
  const meta = loadCurrentReportPdfMeta(propertyId);
  const manifest = loadPublishedManifest(propertyId);
  const report = loadPublishedReport(propertyId);
  const publishedPeriodId =
    manifest?.latestPeriodId ||
    manifest?.monitoringPeriodId ||
    report?.period?.periodId ||
    null;
  if (
    publishedPeriodId &&
    meta?.periodId &&
    String(meta.periodId) !== String(publishedPeriodId)
  ) {
    return {
      hasPdf: true,
      pdfStatus: "STALE",
      coverageStatus: "NEEDS_REBUILD",
      coverageReason: "current_adp_pdf_stale_vs_published_period",
      meta,
      paths,
      publishedPeriodId,
    };
  }
  return {
    hasPdf: true,
    pdfStatus: "READY",
    coverageStatus: "READY",
    coverageReason: null,
    meta,
    paths,
    publishedPeriodId,
  };
}

export function writeCurrentReportPdf(propertyId, pdfAbsolutePath, opts = {}) {
  const id = String(propertyId || "").trim();
  if (!id) throw new Error("propertyId_required");
  if (!fs.existsSync(pdfAbsolutePath)) throw new Error("pdf_source_missing");

  const paths = currentReportPdfPaths(id);
  fs.mkdirSync(paths.dir, { recursive: true });
  fs.copyFileSync(pdfAbsolutePath, paths.reportPdf);

  const buf = fs.readFileSync(paths.reportPdf);
  const fingerprint = crypto.createHash("sha256").update(buf).digest("hex");
  const report = loadPublishedReport(id);
  const manifest = loadPublishedManifest(id);
  const meta = {
    schema: "adp_current_report_pdf_meta_v1",
    gate: ADP_PDF_IS_A_PRINT_REPRESENTATION_OF_THE_CURRENT_PUBLISHED_ADP_REPORT,
    rendererGate: ADP_ADMIN_PDF_USES_CURRENT_REPORT_PRINT_RENDERER,
    editionGate: ADP_CURRENT_PDF_AND_ACTION_PLAN_DERIVE_FROM_SAME_PUBLISHED_EDITION,
    propertyId: id,
    propertyName: report?.property?.name || opts.propertyName || id,
    periodId:
      opts.periodId ||
      report?.period?.periodId ||
      manifest?.latestPeriodId ||
      null,
    publishedAt: manifest?.latestPublishedAt || null,
    generatedAt: new Date().toISOString(),
    generatedBy: opts.generatedBy || "adp-current-report-pdf",
    pdfFingerprint: fingerprint,
    pdfBytes: buf.length,
    liveProviderCalls: 0,
    source: "current_published_adp",
    actionSource: "payload.actions",
    historicalMonthlyReviewPdf: "retained_separately_untouched",
  };
  fs.writeFileSync(paths.metaJson, `${JSON.stringify(meta, null, 2)}\n`, "utf8");
  return meta;
}

export function customerPdfFilename(propertyId) {
  const report = loadPublishedReport(propertyId);
  const name = String(report?.property?.name || propertyId)
    .replace(/[^a-zA-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 72);
  const period = String(report?.period?.periodId || "current")
    .replace(/[^a-zA-Z0-9_-]+/g, "_")
    .slice(0, 48);
  return `AI_Demand_Report_${name}_${period}.pdf`;
}
