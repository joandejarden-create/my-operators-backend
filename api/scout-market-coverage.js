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
import { ensurePlatformConfig, getPlatformBase } from "../lib/hotel-census/platform-base.js";
import {
  buildFixtureMarketCoverage,
  isAirtableCredentialError,
  shouldUseMexicoRadarFixtureFallback,
} from "../lib/hotel-intelligence/golden-demo/mexico-radar-fixture-fallback.js";

export async function getScoutMarketCoverage(req, res) {
  const query = req.query || {};

  function fixtureResponse() {
    if (!shouldUseMexicoRadarFixtureFallback(query)) return null;
    return buildFixtureMarketCoverage(query);
  }

  if (!getPlatformBase()) {
    const fixture = fixtureResponse();
    if (fixture) return res.json(fixture);
    return ensurePlatformConfig(res);
  }

  try {
    const report = await buildMarketCoverageReport(query);

    if (!report.ok) {
      const fixture = fixtureResponse();
      if (fixture) return res.json(fixture);
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
    const fixture = fixtureResponse();
    if (fixture) return res.json(fixture);
    if (isAirtableCredentialError(error) && fixtureResponse()) {
      return res.json(fixtureResponse());
    }
    return res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message,
    });
  }
}
