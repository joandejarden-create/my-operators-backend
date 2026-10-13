/**
 * GET /api/scout/opportunity-signals
 * Read-only Scout opportunity signal generation from Hotel Census.
 * Annotates each signal with saved watchlist metadata when present (no writes).
 *
 * Query: country, market, submarket, parentCompany, brand, chainScale, locationType,
 *        signalType, minRooms, includePipeline=1, limit=100
 */

import { buildOpportunitySignalsReport } from "../lib/scout/opportunity-signals.js";
import { annotateGeneratedSignalsWithSavedStatus } from "../lib/scout/scout-signal-watchlist.js";
import { ensurePlatformConfig } from "../lib/hotel-census/platform-base.js";

export async function getScoutOpportunitySignals(req, res) {
  if (!ensurePlatformConfig(res)) return;

  try {
    const report = await buildOpportunitySignalsReport(req.query || {});

    if (!report.ok) {
      const status = /required|Invalid signalType/i.test(report.error || "") ? 400 : 500;
      return res.status(status).json({
        success: false,
        error: report.error,
      });
    }

    const signals = await annotateGeneratedSignalsWithSavedStatus(report.signals);

    return res.json({
      success: true,
      filters: report.filters,
      summary: report.summary,
      signals,
      warnings: report.warnings,
      source: report.source,
    });
  } catch (error) {
    console.error("[scout-opportunity-signals]", error);
    return res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message,
    });
  }
}
