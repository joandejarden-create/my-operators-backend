/**
 * Plan Hilton brand directory enrichment for one brand (no Airtable writes).
 */

import { crawlHiltonBrandDirectory } from "../hilton-brand-directory-extract.js";
import { affiliationHintsForBrand } from "../hilton-brand-registry.js";
import { resolveBrandAffiliationMatchers } from "./brand-alias-resolve.js";
import {
  buildEnrichmentFieldActions,
  directoryRowToCensusFields,
  ENRICHMENT_SOURCE_HILTON_DIRECTORY,
  summarizeAmenityFlags,
} from "./brand-directory-enrichment-contract.js";
import { formatAmenitiesText } from "../hilton-amenity-map.js";
import {
  loadCensusRowsForAffiliations,
  matchDirectoryRowsToCensus,
} from "./match-brand-directory-to-census.js";

export async function resolveHiltonBrandAffiliationMatchers(brandConfig, parentCompany = "Hilton") {
  const alias = await resolveBrandAffiliationMatchers(
    brandConfig.canonicalBrandName,
    parentCompany
  );
  const hints = affiliationHintsForBrand(brandConfig);
  const merged = [...new Set([...(alias.ok ? alias.affiliationMatchers : []), ...hints])];
  return {
    ...alias,
    ok: true,
    affiliationMatchers: merged,
    hintMatchers: hints,
  };
}

export async function planHiltonBrandEnrichment(opts) {
  const {
    brandConfig,
    parentCompany = "Hilton",
    minConfidence = "low",
    crawlDelayMs = 200,
    onProgress,
  } = opts;

  const brand = brandConfig.canonicalBrandName;
  const alias = await resolveHiltonBrandAffiliationMatchers(brandConfig, parentCompany);

  if (onProgress) onProgress(`Crawling ${brand} (${brandConfig.brandCode})...`);
  const crawl = await crawlHiltonBrandDirectory({
    brandConfig,
    delayMs: crawlDelayMs,
    onProgress,
  });

  const censusLoad = await loadCensusRowsForAffiliations(alias.affiliationMatchers);
  if (censusLoad.totalLoaded === 0) {
    return {
      brand,
      brandConfig,
      alias,
      minConfidence,
      crawlSummary: {
        hotelsFound: crawl.hotelsFound,
        countryPages: crawl.countryPageCount,
        fetchErrors: crawl.fetchErrors,
      },
      censusRowsLoaded: 0,
      matched: 0,
      readyToApply: 0,
      noChanges: 0,
      unmatchedDirectory: crawl.hotelsFound,
      unmatchedCensus: [],
      planRows: [],
      skippedReason: "no_census_rows_for_brand",
    };
  }

  const { matches, unmatchedCensus, unmatchedDirectory } = matchDirectoryRowsToCensus(
    crawl.hotels,
    censusLoad.rows,
    { minConfidence }
  );

  const planRows = [];
  for (const match of matches) {
    const { directoryRow, censusRow, confidence, reason, score, nameSim, distanceMeters } =
      match;
    if (!censusRow) {
      planRows.push({
        brand,
        brandCode: brandConfig.brandCode,
        censusRecordId: "",
        censusName: "",
        directoryName: directoryRow.name,
        directoryBrandPropertyCode: directoryRow.brandPropertyCode,
        matchConfidence: confidence,
        matchScore: score,
        matchReason: reason,
        nameSimilarity: nameSim,
        distanceMeters,
        fieldActions: [],
        applyFields: {},
        amenityFlagsSuggested: summarizeAmenityFlags(directoryRow.amenityIds),
        amenitiesTextSuggested: formatAmenitiesText(directoryRow.amenityIds),
        source: ENRICHMENT_SOURCE_HILTON_DIRECTORY,
        sourceUrl: directoryRow.sourceUrl,
        status: "unmatched_directory",
      });
      continue;
    }

    const proposed = directoryRowToCensusFields(directoryRow, {
      affiliation: censusRow.affiliation || brand,
      parentCompany: censusRow.parentCompany || "Hilton Worldwide",
    });
    const { actions, applyFields } = buildEnrichmentFieldActions(censusRow.fields, proposed, {
      fillBlankOnly: true,
      includeBrandPropertyCode: false,
    });

    planRows.push({
      brand,
      brandCode: brandConfig.brandCode,
      censusRecordId: censusRow.recordId,
      censusName: censusRow.name,
      directoryName: directoryRow.name,
      directoryBrandPropertyCode: directoryRow.brandPropertyCode,
      matchConfidence: confidence,
      matchScore: score,
      matchReason: reason,
      nameSimilarity: nameSim,
      distanceMeters,
      fieldActions: actions,
      applyFields,
      amenityFlagsSuggested: summarizeAmenityFlags(directoryRow.amenityIds),
      amenitiesTextSuggested: formatAmenitiesText(directoryRow.amenityIds),
      source: ENRICHMENT_SOURCE_HILTON_DIRECTORY,
      sourceUrl: directoryRow.website || directoryRow.sourceUrl,
      status: Object.keys(applyFields).length ? "ready" : "no_changes",
    });
  }

  return {
    brand,
    brandConfig,
    alias,
    minConfidence,
    crawlSummary: {
      hotelsFound: crawl.hotelsFound,
      countryPages: crawl.countryPageCount,
      fetchErrors: crawl.fetchErrors,
    },
    censusRowsLoaded: censusLoad.totalLoaded,
    matched: planRows.filter((r) => r.censusRecordId).length,
    readyToApply: planRows.filter((r) => r.status === "ready").length,
    noChanges: planRows.filter((r) => r.status === "no_changes").length,
    unmatchedDirectory: unmatchedDirectory.length,
    unmatchedCensus: unmatchedCensus.map((r) => ({
      recordId: r.recordId,
      name: r.name,
      city: r.city,
      country: r.country,
      affiliation: r.affiliation,
    })),
    planRows,
  };
}
