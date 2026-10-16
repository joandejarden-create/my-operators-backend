/**
 * Westin and Wyndham ADP Admin rows are monthly performance reviews,
 * not a relabel of the current-report PDF.
 *
 *   node scripts/test-admin-six-combo-review-rows.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import crypto from "node:crypto";
import { buildPublishedAdpReviewCoverageV1 } from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";
import { resolveAdpReportPdf } from "../lib/ai-demand-positioning/monthly-review/resolve-adp-report-pdf-v1.js";

function sha256(filePath) {
  return crypto.createHash("sha256").update(fs.readFileSync(filePath)).digest("hex");
}

const coverage = buildPublishedAdpReviewCoverageV1();
assert.equal(coverage.liveProviderCalls, 0);

const expected = [
  {
    propertyId: "adp_westin_tampa_waterside",
    reviewId: "adp_mr_westin_tampa_waterside_2026-10_v1",
    periodId: "adp_period_adp_westin_tampa_waterside_20261013205651_c27cab",
  },
  {
    propertyId: "adp_wyndham_san_diego_bayside",
    reviewId: "adp_mr_wyndham_san_diego_baysid_2026-10_v1",
    periodId: "adp_period_adp_wyndham_san_diego_bayside_20261013221340_775a48",
  },
];

for (const spec of expected) {
  const row = coverage.properties.find((p) => p.propertyId === spec.propertyId);
  assert.ok(row, spec.propertyId);
  assert.equal(row.coverageStatus, "READY");
  assert.equal(row.coverageReason, null);
  assert.equal(row.reportingMonth, "October 2026");
  assert.equal(row.currentMonitoringDate, "2026-10-13");
  assert.equal(row.versionLabel, "V1");
  assert.equal(row.reviewId, spec.reviewId);
  assert.equal(row.hasPdf, true);
  assert.equal(row.pdfKind, "ai_demand_performance_review");
  assert.equal(row.reviewPeriodId, spec.periodId);

  const pdf = resolveAdpReportPdf({
    reviewId: spec.reviewId,
    propertyId: spec.propertyId,
    publishedPeriodId: spec.periodId,
    requirePublishedPeriodMatch: true,
  });
  assert.equal(pdf.available, true);
  assert.match(pdf.pdfAbsolutePath, /monthly-review[\\/]archive[\\/]/);
  assert.doesNotMatch(pdf.pdfAbsolutePath, /current-report-pdf/);
  const magic = fs.readFileSync(pdf.pdfAbsolutePath).subarray(0, 4).toString("utf8");
  assert.equal(magic, "%PDF");

  const currentPdf = `reports/ai-demand-positioning/current-report-pdf/${spec.propertyId}/report.pdf`;
  assert.equal(fs.existsSync(currentPdf), true);
  assert.notEqual(sha256(pdf.pdfAbsolutePath), sha256(currentPdf));
}

const renaissance = coverage.properties.find(
  (p) => p.propertyId === "adp_renaissance_times_square"
);
assert.equal(renaissance.coverageStatus, "READY");
assert.equal(renaissance.reviewId, "adp_mr_renaissance_times_square_2026-10_v2");

const mayflower = coverage.properties.find(
  (p) => p.propertyId === "adp_mayflower_hotel_washington_dc"
);
assert.equal(mayflower.coverageStatus, "NEEDS_REBUILD");
assert.equal(mayflower.coverageReason, "review_artifact_missing");

assert.equal(
  fs.existsSync(
    "data/group-demand-intelligence/hotels/recdmo0CnhIZAwHGC/opportunities.json"
  ),
  false,
  "Westin GDI stored records stay unpublished"
);

console.log(
  JSON.stringify({
    ok: true,
    westin: expected[0].reviewId,
    wyndham: expected[1].reviewId,
    renaissance: renaissance.reviewId,
    mayflower: mayflower.coverageReason,
  })
);
