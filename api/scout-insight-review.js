/**
 * GET /api/scout/insight-review
 * Read-only Scout insight calibration and evidence review.
 */

import { buildInsightReviewReport } from "../lib/scout/market-insights.js";
import { ensurePlatformConfig } from "../lib/hotel-census/platform-base.js";

export async function getScoutInsightReview(req, res) {
  if (!ensurePlatformConfig(res)) return;

  try {
    const report = await buildInsightReviewReport(req.query || {});

    if (!report.ok) {
      return res.status(500).json({ success: false, error: report.error });
    }

    return res.json({
      success: true,
      filters: report.filters,
      summary: report.summary,
      insightReviews: report.insightReviews,
      suppressedInsights: report.suppressedInsights,
      insightQualitySummary: report.insightQualitySummary,
      dataQualityNotes: report.dataQualityNotes,
      warnings: report.warnings,
      source: report.source,
    });
  } catch (error) {
    console.error("[scout-insight-review]", error);
    return res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message,
    });
  }
}
