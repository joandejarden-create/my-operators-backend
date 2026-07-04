/**
 * GET /api/scout/demand-overlays
 * Read-only demand driver overlays for Scout Market Map.
 */

import { buildDemandOverlaysReport } from "../lib/scout/demand-overlays.js";
import { getTravelInfrastructureBaseId } from "../lib/travel-infrastructure/travel-infrastructure-base.js";

function hasPlatformConfig() {
  return Boolean(process.env.AIRTABLE_API_KEY && getTravelInfrastructureBaseId());
}

export async function getScoutDemandOverlays(req, res) {
  if (!hasPlatformConfig()) {
    return res.status(500).json({
      success: false,
      error: "Platform Airtable not configured (AIRTABLE_API_KEY + AIRTABLE_BASE_ID_ALT)",
    });
  }

  try {
    const report = await buildDemandOverlaysReport(req.query || {});

    if (!report.ok) {
      return res.status(500).json({ success: false, error: report.error });
    }

    return res.json({
      success: true,
      filters: report.filters,
      summary: report.summary,
      overlayMarkers: report.overlayMarkers,
      overlayMarkersWithoutCoordinates: report.overlayMarkersWithoutCoordinates || [],
      warnings: report.warnings,
      source: report.source,
    });
  } catch (error) {
    console.error("[scout-demand-overlays]", error);
    return res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message,
    });
  }
}
