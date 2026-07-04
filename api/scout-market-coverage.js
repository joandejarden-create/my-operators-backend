/**
 * GET /api/scout/market-coverage
 * Read-only Scout market coverage + white-space intelligence from Hotel Census.
 *
 * Query: country, city, market, strMarket, submarket, strSubmarket, parentCompany, brand,
 *        chainScale, locationType, status, includePipeline=1
 *
 * STR geography: strMarket/market → Hotel Census Market; strSubmarket/submarket → Submarket.
 */

import { buildMarketCoverageReport } from "../lib/scout/market-coverage.js";
import { ensurePlatformConfig } from "../lib/hotel-census/platform-base.js";

export async function getScoutMarketCoverage(req, res) {
  if (!ensurePlatformConfig(res)) return;

  try {
    const report = await buildMarketCoverageReport(req.query || {});

    if (!report.ok) {
      const status = report.error?.includes("required") ? 400 : 500;
      return res.status(status).json({
        success: false,
        error: report.error,
      });
    }

    return res.json({
      success: true,
      filters: report.filters,
      metrics: report.metrics,
      breakdowns: report.breakdowns,
      whiteSpace: report.whiteSpace,
      recordsSample: report.recordsSample,
      warnings: report.warnings,
      source: report.source,
    });
  } catch (error) {
    console.error("[scout-market-coverage]", error);
    return res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message,
    });
  }
}
