/**
 * Match Choice census rows to choicehotels.com sitemap URLs (verified source).
 * Uses existing CALA URL extract CSV + name/geo scoring — no invented URLs.
 */

import { readFileSync, existsSync } from "node:fs";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import {
  mapCensusRowForDirectoryMatch,
  scoreDirectoryAgainstCensus,
} from "./match-brand-directory-to-census.js";
import { isBlankCensusValue } from "./brand-directory-enrichment-contract.js";
import { choicePropertyIdFromUrl } from "../choice-hotel-content-fetch.js";
import {
  loadPropertyUrlExtractReport,
  filterChoicePropertiesForMatch,
  deriveInferredHotelName,
} from "../independent-census/match-brand-directory-properties.js";

const CHOICE_PARENT_FORMULA = `FIND("Choice", {${CENSUS_FIELDS.parentCompany}})`;
const DEFAULT_JSON =
  "reports/independent-census-choice-property-url-extract-cala-2026-05-20.json";
const DEFAULT_CSV =
  "reports/independent-census-choice-property-url-extract-cala-2026-05-20.csv";

/**
 * Prefer JSON extract (city/country/brand metadata); fall back to CSV URLs only.
 * @param {string} [jsonPath]
 * @param {string} [csvPath]
 */
export function loadChoiceSitemapDirectoryRows(jsonPath = DEFAULT_JSON, csvPath = DEFAULT_CSV) {
  if (existsSync(jsonPath)) {
    const { rows } = loadPropertyUrlExtractReport(jsonPath);
    const filtered = filterChoicePropertiesForMatch(rows);
    return filtered.map((row) => ({
      ...row,
      inferredHotelName: deriveInferredHotelName(row),
      source: "choice_sitemap_json",
    }));
  }

  if (!existsSync(csvPath)) return [];
  const text = readFileSync(csvPath, "utf8");
  const lines = text.split(/\r?\n/).slice(1);
  /** @type {object[]} */
  const rows = [];
  for (const line of lines) {
    const urlMatch = line.match(/(https:\/\/www\.choicehotels\.com\/[^,\s]+)/);
    if (!urlMatch) continue;
    const url = urlMatch[1];
    const calaMatch = line.match(/,(included|excluded_non_cala|uncertain),/);
    const calaStatus = calaMatch ? calaMatch[1] : "uncertain";
    const brandMatch = line.match(/^Choice Hotels International,([^,]+),/);
    rows.push({
      propertyUrl: url,
      propertyId: choicePropertyIdFromUrl(url),
      calaFilterStatus: calaStatus,
      matchedBrandSetupBrand: brandMatch ? brandMatch[1].trim() : "",
      inferredHotelName: brandMatch ? brandMatch[1].trim() : choicePropertyIdFromUrl(url),
      source: "choice_sitemap_csv",
    });
  }
  return rows;
}

/** @deprecated use loadChoiceSitemapDirectoryRows */
export function loadChoiceSitemapRowsFromCsv(csvPath = DEFAULT_CSV) {
  return loadChoiceSitemapDirectoryRows(DEFAULT_JSON, csvPath);
}

/**
 * @param {object} [opts]
 */
export async function planChoiceCensusSitemapMatch(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const jsonPath = opts.jsonPath || DEFAULT_JSON;
  const csvPath = opts.csvPath || DEFAULT_CSV;
  const directoryRows = loadChoiceSitemapDirectoryRows(jsonPath, csvPath);
  const calaOnly = opts.calaOnly !== false;
  const filteredDirectory = calaOnly
    ? directoryRows.filter((r) => r.calaFilterStatus === "included")
    : directoryRows;

  const selectFields = await getCensusEnrichmentSelectFields(base);
  for (const f of ["Website", "Amenities", "Property ID"]) {
    if (!selectFields.includes(f)) selectFields.push(f);
  }

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: selectFields,
      filterByFormula: CHOICE_PARENT_FORMULA,
      pageSize: 100,
    })
    .all();

  const censusRows = records.map(mapCensusRowForDirectoryMatch);
  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];

  for (const censusRow of censusRows) {
    let best = null;
    let bestScore = 0;

    for (const dir of filteredDirectory) {
      const cityFromSlug = String(dir.citySlug || "").replace(/-/g, " ");
      const directoryRow = {
        name: dir.inferredHotelName || deriveInferredHotelName(dir),
        city: cityFromSlug,
        country: dir.inferredCountry || dir.countryOrRegionSegment || "",
        website: dir.propertyUrl,
        propertyUrl: dir.propertyUrl,
        brandPropertyCode: dir.propertyId?.toUpperCase(),
        source: dir.source,
      };
      const scored = scoreDirectoryAgainstCensus(directoryRow, censusRow);
      if (scored.score > bestScore) {
        bestScore = scored.score;
        best = { dir, scored };
      }
    }

    const minScore = opts.minScore ?? 55;
    if (!best || bestScore < minScore) {
      skipped.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        reason: best ? "below_min_score" : "no_directory_candidate",
        bestScore,
      });
      continue;
    }

    const applyFields = {};
    const website = best.dir.propertyUrl;
    if (isBlankCensusValue(censusRow.fields?.Website) && website) {
      applyFields.Website = website;
    }
    if (isBlankCensusValue(censusRow.fields?.["Property ID"]) && best.dir.propertyId) {
      applyFields["Property ID"] = best.dir.propertyId.toUpperCase();
    }

    if (!Object.keys(applyFields).length && isBlankCensusValue(censusRow.fields?.Amenities)) {
      skipped.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        reason: "no_blank_fields_to_fill",
        matchConfidence: best.scored.confidence,
      });
      continue;
    }

    planRows.push({
      censusRecordId: censusRow.recordId,
      censusName: censusRow.name,
      propertyId: best.dir.propertyId,
      propertyUrl: website,
      matchScore: bestScore,
      matchConfidence: best.scored.confidence,
      matchReason: best.scored.reason,
      applyFields,
      status: "ready",
    });
  }

  return {
    jsonPath,
    csvPath,
    directoryRowsLoaded: directoryRows.length,
    directoryRowsFiltered: filteredDirectory.length,
    censusRowsScanned: censusRows.length,
    readyToApply: planRows.length,
    planRows,
    skipped,
  };
}
