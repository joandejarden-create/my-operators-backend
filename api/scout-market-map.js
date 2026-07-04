/**
 * GET /api/scout/market-map
 * Read-only map markers + clusters for Scout Market Map page.
 */

import { buildMarketMapReport } from "../lib/scout/market-map.js";
import { ensurePlatformConfig } from "../lib/hotel-census/platform-base.js";

export async function getScoutMarketMap(req, res) {
  if (!ensurePlatformConfig(res)) return;

  try {
    const report = await buildMarketMapReport(req.query || {});

    if (!report.ok) {
      return res.status(500).json({ success: false, error: report.error });
    }

    return res.json({
      success: true,
      filters: report.filters,
      summary: report.summary,
      hotelMarkers: report.hotelMarkers,
      signalMarkers: report.signalMarkers,
      savedSignalMarkers: report.savedSignalMarkers,
      marketClusters: report.marketClusters,
      generatedSignals: report.generatedSignals || [],
      demandOverlayMarkers: report.demandOverlayMarkers || [],
      demandOverlayMarkersWithoutCoordinates:
        report.demandOverlayMarkersWithoutCoordinates || [],
      demandOverlaySummary: report.demandOverlaySummary || null,
      insightSummary: report.insightSummary || null,
      insights: report.insights || [],
      rankedOpportunities: report.rankedOpportunities || [],
      warnings: report.warnings,
      source: report.source,
    });
  } catch (error) {
    console.error("[scout-market-map]", error);
    return res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message,
    });
  }
}
