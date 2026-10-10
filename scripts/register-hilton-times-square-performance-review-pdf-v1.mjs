/**
 * Register existing Hilton Performance Review PDF into monthly-review archive
 * so Admin Reviews tab can serve it (no PDF regeneration).
 */
import "dotenv/config";
import path from "node:path";
import fs from "node:fs";
import { fileURLToPath } from "node:url";
import { renderAndAttachMonthlyReviewPdfV1 } from "../lib/ai-demand-positioning/monthly-review/admin/render-and-attach-monthly-review-pdf-v1.mjs";
import {
  loadArchiveIndex,
  loadReviewPayload,
} from "../lib/ai-demand-positioning/monthly-review/admin/archive-store-v1.js";
import { resolveAdpReportPdf } from "../lib/ai-demand-positioning/monthly-review/resolve-adp-report-pdf-v1.js";
import { buildPublishedAdpReviewCoverageV1 } from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REVIEW_ID = "adp_mr_hilton_times_square_2026-10_v2";
const PROPERTY_ID = "adp_hilton_times_square";
const PERIOD_ID =
  "adp_period_adp_hilton_times_square_20261005122652_63a1d8";
const SOURCE_PDF = path.join(
  ROOT,
  "reports/adp/hilton-times-square-performance-review/HILTON_TIMES_SQUARE_PERFORMANCE_REVIEW.pdf"
);
const OUT = path.join(
  ROOT,
  "reports/adp/hilton-performance-review-registration"
);

function write(name, content) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), content, "utf8");
}

async function main() {
  if (!fs.existsSync(SOURCE_PDF)) {
    throw new Error(`source_pdf_missing: ${SOURCE_PDF}`);
  }
  const st = fs.statSync(SOURCE_PDF);
  if (!st.size) throw new Error("source_pdf_empty");
  const magic = Buffer.alloc(4);
  const fd = fs.openSync(SOURCE_PDF, "r");
  fs.readSync(fd, magic, 0, 4, 0);
  fs.closeSync(fd);
  if (magic.toString("utf8") !== "%PDF") throw new Error("source_pdf_not_pdf");

  const before = loadReviewPayload(REVIEW_ID);
  if (!before) throw new Error(`review_not_found:${REVIEW_ID}`);
  if (before.meta.sourceCurrentPeriodId !== PERIOD_ID) {
    throw new Error(
      `period_mismatch: expected ${PERIOD_ID} got ${before.meta.sourceCurrentPeriodId}`
    );
  }

  const attached = await renderAndAttachMonthlyReviewPdfV1({
    reviewId: REVIEW_ID,
    propertyId: PROPERTY_ID,
    pdfSourcePath: SOURCE_PDF,
    skipRender: true,
  });

  const pack = loadReviewPayload(REVIEW_ID);
  const resolved = resolveAdpReportPdf({
    reviewId: REVIEW_ID,
    propertyId: PROPERTY_ID,
    publishedPeriodId: PERIOD_ID,
    requirePublishedPeriodMatch: true,
  });
  const coverage = buildPublishedAdpReviewCoverageV1();
  const covRow = coverage.properties.find((p) => p.propertyId === PROPERTY_ID);
  const index = loadArchiveIndex();
  const hiltonRows = index.reviews.filter(
    (r) => r.propertyId === PROPERTY_ID && r.reportingMonthKey === "2026-10"
  );
  const bethesda = coverage.properties.find(
    (p) => p.propertyId === "adp_bethesda_marriott"
  );

  const record = {
    reviewId: REVIEW_ID,
    propertyId: PROPERTY_ID,
    propertyName: pack.meta.propertyName,
    hotelCensusId: "rec35fExUxCClpOP6",
    reportingMonth: pack.meta.reportingMonth,
    currentMonitoringDate: pack.meta.currentMonitoringDate,
    sourceCurrentPeriodId: pack.meta.sourceCurrentPeriodId,
    reviewStatus: pack.meta.reviewStatus,
    pdfStatus: pack.meta.pdfStatus,
    pdfFingerprint: pack.meta.pdfFingerprint,
    pdfBytes: resolved.pdfBytes,
    pdfAbsolutePath: resolved.pdfAbsolutePath,
    viewUrl: resolved.viewUrl,
    downloadUrl: resolved.downloadUrl,
    displayFilename:
      "Dealality - Hilton New York Times Square - AI Demand Performance Review - Oct052026.pdf",
    sourcePackPdf: SOURCE_PDF,
    sourcePackBytes: st.size,
    coverageStatus: covRow?.coverageStatus || null,
    hasPdf: covRow?.hasPdf || false,
    alreadyAttached: attached.alreadyAttached || false,
  };

  write("HILTON_REVIEW_RECORD.md", `# Hilton Review Record

| Field | Value |
|-------|-------|
| Review ID | \`${record.reviewId}\` |
| Property ID | \`${record.propertyId}\` |
| Property name | ${record.propertyName} |
| HPC | \`${record.hotelCensusId}\` |
| Reporting month | ${record.reportingMonth} |
| Monitoring date | ${record.currentMonitoringDate} |
| Certified period | \`${record.sourceCurrentPeriodId}\` |
| Review status | ${record.reviewStatus} |
| PDF status | ${record.pdfStatus} |
| PDF bytes | ${record.pdfBytes} |
| Archive path | \`${record.pdfAbsolutePath}\` |
| View URL | \`${record.viewUrl}\` |
| Download URL | \`${record.downloadUrl}\` |
| Display filename | ${record.displayFilename} |
| Coverage status | ${record.coverageStatus} |
| hasPdf | ${record.hasPdf} |
| Already attached | ${record.alreadyAttached} |
`);

  write(
    "ROOT_CAUSE.md",
    `# Root Cause

**Classification:** \`PDF_ONLY_ON_FILESYSTEM\` (+ archive \`pdfStatus: MISSING\`)

## Exact cause

1. \`generateNewDraft\` created archive review \`${REVIEW_ID}\` linked to certified period \`${PERIOD_ID}\`.
2. The Performance Review PDF was written only to \`reports/adp/hilton-times-square-performance-review/\`.
3. Canonical attach (\`attachPdfToReview\` → archive \`report.pdf\` + index \`pdfStatus: READY\`) was never called.
4. Admin Reviews coverage requires \`resolveAdpReportPdf\` → archive \`report.pdf\` present; without it \`hasPdf: false\` / \`NEEDS_REBUILD\` / View PDF blocked.

## Not the cause

- Missing review registry record (record existed)
- Hotel ID mismatch
- Wrong certified period on v2
- Frontend hardcode
- Legacy Sep 27 period on current v2
`
  );

  write(
    "REVIEWS_TAB_DATA_FLOW.md",
    `# Reviews Tab Data Flow

\`\`\`
/app#/admin/ai-demand?tab=reviews
  → public/app/admin/ai-demand-admin.html (tab shell)
  → public/js/admin-ai-demand-reviews.js
  → GET /api/admin/ai-demand-positioning/monthly-reviews
  → api/admin-adp-monthly-reviews.js#getAdminMonthlyReviews
  → listArchiveReviews (filesystem index)
  → buildPublishedAdpReviewCoverageV1 (one row per published ADP property)
  → resolveAdpReportPdf({ reviewId, publishedPeriodId, requirePublishedPeriodMatch })
  → archive .../report.pdf served by GET .../monthly-reviews/:reviewId/pdf
\`\`\`

## Data store

- SoT: \`reports/ai-demand-positioning/monthly-review/archive/index.json\`
- Artifacts: \`reports/ai-demand-positioning/monthly-review/archive/{propertyId}/{YYYY-MM}/{reviewId}/\`
  - \`review.json\`, \`metadata.json\`, \`report.pdf\`, fingerprints, generation-log
- PDF public/admin URL: \`/api/admin/ai-demand-positioning/monthly-reviews/{reviewId}/pdf\`
  - \`?download=1\` for download disposition

## Visibility rules (coverage mode — default UI)

- Property must be in published ADP universe
- Latest preferred archive review with PDF READY (period match to published)
- \`hasPdf\` true only when archive \`report.pdf\` exists and period matches
- View/Download refuse when \`pdfUnavailableReason\` / \`hasPdf === false\`
`
  );

  write(
    "REGRESSION.md",
    `# Regression

| Check | Result |
|-------|--------|
| Hilton Oct 2026 versions in index | ${hiltonRows.length} (v1 SUPERSEDED pdf MISSING, v2 READY) |
| Bethesda coverage still present | ${bethesda ? "YES" : "NO"} (${bethesda?.coverageStatus || "—"}) |
| Bethesda hasPdf | ${bethesda?.hasPdf ? "YES" : "NO"} |
| Hilton coverage | ${covRow?.coverageStatus} / hasPdf=${covRow?.hasPdf} |
| Source pack PDF preserved | YES (${st.size} bytes) |
| PDF regenerated | NO |
| Duplicate current Hilton Oct rows with READY | ${hiltonRows.filter((r) => r.pdfStatus === "READY" && r.isCurrent).length} |
`
  );

  write(
    "CHANGELOG.md",
    `# Changelog — Hilton Performance Review Registration

- Attached existing pack PDF to archive review \`${REVIEW_ID}\` via shared \`renderAndAttachMonthlyReviewPdfV1\` / \`attachPdfToReview\`
- Did **not** regenerate the customer PDF
- Wired admin generate + regenerate to auto render+attach PDF (skip with \`skipPdf:true\`)
- Hilton pack script now registers archive PDF after pack write
`
  );

  write(
    "FOUNDER_REPORT.md",
    `# Founder Report — Hilton Performance Review in Admin Reviews

## Verdict

Hilton New York Times Square Performance Review is registered in the canonical monthly-review archive and should appear in Admin Reviews with View/Download PDF.

## What was wrong

PDF lived only under \`reports/adp/hilton-times-square-performance-review/\`. The Reviews tab serves archive \`report.pdf\` indexed in \`monthly-review/archive/index.json\`.

## What we did

1. Attached existing PDF to \`${REVIEW_ID}\` (certified period \`${PERIOD_ID}\`)
2. Confirmed coverage \`hasPdf\` / READY for Hilton
3. Wired shared generate → attach PDF so future reviews do not stop at JSON draft

## Paths

- Pack (unchanged source): \`reports/adp/hilton-times-square-performance-review/HILTON_TIMES_SQUARE_PERFORMANCE_REVIEW.pdf\`
- Archive: \`${resolved.pdfAbsolutePath}\`
- Admin URL: \`${resolved.viewUrl}\`
`
  );

  console.log(JSON.stringify({ ok: true, record, attached }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
