/**
 * GET /api/brand-presence-summary
 * Read-only census rollups for Brand Explorer (Deal Capture Platform).
 *
 * Query: brand (required), parentCompany (optional)
 */

import { buildBrandCensusSummary } from "../lib/hotel-census/build-brand-census-summary.js";
import { ensurePlatformConfig } from "../lib/hotel-census/platform-base.js";

export async function getBrandPresenceSummary(req, res) {
  if (!ensurePlatformConfig(res)) return;

  try {
    const brand = (req.query.brand || "").trim();
    const parentCompany = (req.query.parentCompany || req.query.parent_company || "").trim();

    if (!brand) {
      return res.status(400).json({
        success: false,
        error: "Query parameter brand is required",
      });
    }

    const censusSummary = await buildBrandCensusSummary(brand, parentCompany || null);
    if (!censusSummary.available && censusSummary.warnings?.[0]?.startsWith("CENSUS_SUMMARY_ERROR: brand")) {
      return res.status(400).json({ success: false, error: "Query parameter brand is required" });
    }
    if (!censusSummary.available && censusSummary.warnings?.[0]?.includes("not configured")) {
      return res.status(500).json({
        success: false,
        error: "Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT for Platform / Hotel Census",
      });
    }

    return res.json({
      success: true,
      brand: censusSummary.match?.canonicalBrandName || brand,
      requestedBrand: censusSummary.match?.requestedBrand || brand,
      fallbackRecommended: censusSummary.fallbackRecommended,
      parentCompany: censusSummary.match?.parentCompany || parentCompany || null,
      metrics: censusSummary.metrics,
      countryBreakdown: censusSummary.breakdowns?.country,
      dealalityRegionBreakdown: censusSummary.breakdowns?.dealalityRegion,
      chainScaleMix: censusSummary.breakdowns?.chainScale,
      locationTypeMix: censusSummary.breakdowns?.locationType,
      pipelinePhaseMix: censusSummary.breakdowns?.pipelinePhase,
      dataConfidenceBreakdown: censusSummary.dataConfidenceBreakdown,
      governance: censusSummary.governance,
      alias: censusSummary.alias,
      source: censusSummary.source,
      lastUpdated: censusSummary.source?.aggregatedAt,
      dataConfidenceNotes: censusSummary.dataConfidenceNotes,
      warnings: censusSummary.warnings,
      census: censusSummary.census,
    });
  } catch (error) {
    console.error("[brand-presence-summary]", error);
    return res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message,
    });
  }
}
