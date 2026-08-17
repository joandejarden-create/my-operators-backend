/**
 * Build read-only census summary payload for Brand Library / Brand Explorer.
 * Used by GET /api/brand-presence-summary and GET /api/brand-library/brand (feature flag).
 */

import {
  resolveBrandAffiliationMatchers,
  normalizeParentCompanyKey,
} from "./brand-alias-resolve.js";
import { aggregateCensusPresenceSummary } from "./aggregate-presence-summary.js";
import { BRAND_ALIAS_TABLE } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";

function computeFallbackRecommended(resolution, summary) {
  return (
    !resolution.usedAliasTable ||
    resolution.warnings.some((w) =>
      /^(ALIAS_TABLE_UNAVAILABLE|NO_ACTIVE_ALIAS_ROWS|NO_ALIAS_FOR_REQUESTED_BRAND|NO_ALIAS_ROWS_FOR_CANONICAL)/.test(
        w
      )
    ) ||
    (summary.metrics.totalOpenHotels === 0 && summary.metrics.totalPipelineHotels === 0)
  );
}

/**
 * @param {string} requestedBrand MVP Brand Name / Brand Explorer display name
 * @param {string|null|undefined} parentCompany MVP Parent Company (optional filter)
 * @returns {Promise<object>} censusSummary shape for brand.censusSummary
 */
export async function buildBrandCensusSummary(requestedBrand, parentCompany) {
  const brand = (requestedBrand || "").trim();
  const parent = (parentCompany || "").trim() || null;

  if (!brand) {
    return {
      available: false,
      fallbackRecommended: true,
      warnings: ["CENSUS_SUMMARY_ERROR: brand name is required"],
    };
  }

  if (!getPlatformBase()) {
    return {
      available: false,
      fallbackRecommended: true,
      warnings: ["CENSUS_SUMMARY_ERROR: AIRTABLE_BASE_ID_ALT not configured"],
    };
  }

  try {
    let resolution = await resolveBrandAffiliationMatchers(brand, parent);
    if (!resolution.ok) {
      return {
        available: false,
        fallbackRecommended: true,
        warnings: [`CENSUS_SUMMARY_ERROR: ${resolution.error}`],
      };
    }

    let censusParentFilter = parent;

    if (
      parent &&
      resolution.canonicalBrandName &&
      !resolution.usedAliasTable &&
      resolution.warnings.some((w) => w.startsWith("NO_ALIAS_ROWS_FOR_CANONICAL"))
    ) {
      const retry = await resolveBrandAffiliationMatchers(brand, null);
      if (retry.ok && retry.usedAliasTable) {
        resolution = retry;
        resolution.warnings.push(
          `PARENT_COMPANY_ALIAS_MISMATCH: MVP parent "${parent}" did not match alias rows; affiliation matchers loaded without Parent Company filter on aliases`
        );
      }
    }

    if (parent && resolution.aliasRecordsUsed?.length) {
      const normReq = normalizeParentCompanyKey(parent);
      const matched = resolution.aliasRecordsUsed.find(
        (r) => normalizeParentCompanyKey(r.parentCompany) === normReq
      );
      if (matched?.parentCompany) {
        censusParentFilter = matched.parentCompany;
      } else if (normReq) {
        censusParentFilter = null;
        resolution.warnings.push(
          `PARENT_COMPANY_CENSUS_FILTER_SKIPPED: MVP parent "${parent}" did not match alias Parent Company for census filter`
        );
      }
    } else if (
      parent &&
      resolution.canonicalBrandName &&
      !resolution.usedAliasTable
    ) {
      censusParentFilter = null;
    }

    let summary = await aggregateCensusPresenceSummary({
      affiliationMatchers: resolution.affiliationMatchers,
      parentCompany: censusParentFilter,
    });

    if (
      summary.ok &&
      censusParentFilter &&
      resolution.usedAliasTable &&
      summary.metrics.totalOpenHotels === 0 &&
      summary.metrics.totalPipelineHotels === 0
    ) {
      const relaxed = await aggregateCensusPresenceSummary({
        affiliationMatchers: resolution.affiliationMatchers,
        parentCompany: null,
      });
      if (
        relaxed.ok &&
        (relaxed.metrics.totalOpenHotels > 0 || relaxed.metrics.totalPipelineHotels > 0)
      ) {
        summary = relaxed;
        censusParentFilter = null;
        resolution.warnings.push(
          `CENSUS_PARENT_FILTER_RELAXED: no rows for Parent Company "${parent}"; census rollup without Parent Company filter`
        );
      }
    }

    if (!summary.ok) {
      return {
        available: false,
        fallbackRecommended: true,
        warnings: [`CENSUS_SUMMARY_ERROR: ${summary.error}`],
      };
    }

    const fallbackRecommended = computeFallbackRecommended(resolution, summary);

    return {
      available: true,
      fallbackRecommended,
      source: {
        ...summary.source,
        aliasTable: BRAND_ALIAS_TABLE,
        aggregatedAt: summary.source.aggregatedAt,
      },
      alias: {
        canonicalBrandName: resolution.canonicalBrandName,
        affiliationMatchers: resolution.affiliationMatchers,
        matchPath: resolution.matchPath,
        recordsUsed: resolution.aliasRecordsUsed,
        usedAliasTable: resolution.usedAliasTable,
      },
      match: {
        requestedBrand: resolution.requestedBrand,
        canonicalBrandName: resolution.canonicalBrandName,
        parentCompany: parent,
        parentCompanyUsedForAliasFilter: resolution.parentCompany,
        parentCompanyUsedForCensusFilter: censusParentFilter,
        matchPath: resolution.matchPath,
      },
      metrics: summary.metrics,
      breakdowns: {
        country: summary.countryBreakdown,
        dealalityRegion: summary.dealalityRegionBreakdown,
        chainScale: summary.chainScaleMix,
        locationType: summary.locationTypeMix,
        pipelinePhase: summary.pipelinePhaseMix,
        dataConfidence: summary.dataConfidenceBreakdown,
      },
      dataConfidenceBreakdown: summary.dataConfidenceBreakdown,
      governance: summary.governance,
      warnings: [...resolution.warnings],
      census: {
        recordsMatched: summary.censusRecordsMatched,
        excludedIndependent: summary.excludedIndependent,
        excludedIncludeInBrandExplorer: summary.excludedIncludeInBrandExplorer ?? 0,
      },
      dataConfidenceNotes: summary.dataConfidenceNotes,
    };
  } catch (err) {
    return {
      available: false,
      fallbackRecommended: true,
      warnings: [`CENSUS_SUMMARY_ERROR: ${err?.message || String(err)}`],
    };
  }
}
