/**
 * GET /api/scout/market-insights
 * Read-only Scout Market Insight Engine.
 */

import { buildMarketInsightsReport } from "../lib/scout/market-insights.js";
import { ensurePlatformConfig } from "../lib/hotel-census/platform-base.js";

export async function getScoutMarketInsights(req, res) {
  if (!ensurePlatformConfig(res)) return;

  try {
    const report = await buildMarketInsightsReport(req.query || {});

    if (!report.ok) {
      return res.status(500).json({ success: false, error: report.error });
    }

    return res.json({
      success: true,
      filters: report.filters,
      summary: report.summary,
      insights: report.insights,
      rankedOpportunities: report.rankedOpportunities,
      warnings: report.warnings,
      source: report.source,
      ...(report.insightQualitySummary != null
        ? { insightQualitySummary: report.insightQualitySummary }
        : {}),
      ...(report.dataQualityNotes != null ? { dataQualityNotes: report.dataQualityNotes } : {}),
      ...(report.suppressedInsightCount != null
        ? { suppressedInsightCount: report.suppressedInsightCount }
        : {}),
      ...(report.insightReviews != null ? { insightReviews: report.insightReviews } : {}),
      ...(report.suppressedInsights != null ? { suppressedInsights: report.suppressedInsights } : {}),
    });
  } catch (error) {
    console.error("[scout-market-insights]", error);
    return res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message,
    });
  }
}
