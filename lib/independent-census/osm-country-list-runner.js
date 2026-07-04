/**
 * Phase 4M — Run hotel-focused OSM dry-run / apply across a country list.
 */

import { join } from "path";
import { writeCsv, writeJson } from "../str-census-import/report-utils.mjs";
import {
  fetchOsmHotelCandidates,
  summarizeCandidates,
  DEFAULT_MAX_ELEMENTS,
} from "./sources/osm.js";
import { sourcePolicySummaryForReport } from "./source-registry.js";
import {
  createCandidateRecords,
  loadExistingCandidateKeys,
  parseMinQualityTier,
} from "./candidate-apply.js";
import { CANDIDATES_TABLE, SOURCE_TYPES } from "./fields.js";
import { getIndependentCensusBase } from "./platform-base.js";

const QUALITY_TIER_TO_MIN_SCORE = {
  minimal: 0,
  low: 25,
  medium: 45,
  high: 70,
};

export function parseCountryList(countriesStr) {
  return String(countriesStr || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

export function countryToSlug(country) {
  return country
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

export function batchIdForCountry(country, runSuffix = "choice-cala-2026-05-20") {
  return `osm-${countryToSlug(country)}-hotel-focused-${runSuffix}`;
}

export function minQualityScoreFromTier(tierStr) {
  if (!tierStr) return 0;
  const tier = parseMinQualityTier(tierStr);
  return QUALITY_TIER_TO_MIN_SCORE[tier] ?? 0;
}

const CSV_COLUMNS = [
  "sourceRecordId",
  "rawHotelName",
  "rawCity",
  "rawCountry",
  "rawLatitude",
  "rawLongitude",
  "qualityTier",
  "qualityScore",
  "importBatchId",
];

function candidateToCsvRow(c) {
  return {
    sourceRecordId: c.sourceRecordId,
    rawHotelName: c.rawHotelName,
    rawCity: c.rawCity,
    rawCountry: c.rawCountry,
    rawLatitude: c.rawLatitude ?? "",
    rawLongitude: c.rawLongitude ?? "",
    qualityTier: c.qualityTier ?? "",
    qualityScore: c.qualityScore ?? "",
    importBatchId: c.importBatchId,
  };
}

function stripInternalFields(c) {
  const {
    _osmElementType,
    _osmTourismTag,
    _missingFieldFlags,
    _missingFields,
    _hasRoomsTag,
    _hasStarsTag,
    _hasBrandOrOperator,
    ...rest
  } = c;
  return {
    ...rest,
    osmElementType: _osmElementType,
    osmTourismTag: _osmTourismTag,
    missingFields: _missingFields || c.missingFields,
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {string} country
 * @param {object} opts
 */
export async function runOsmCountryDryRun(country, opts) {
  const {
    reportsDir,
    runSuffix = "choice-cala-2026-05-20",
    hotelFocused = true,
    includeApartments = false,
    includeUnnamed = false,
    minQualityTier = "medium",
    maxElements = DEFAULT_MAX_ELEMENTS,
    limit = null,
    delayMs = 0,
  } = opts;

  if (delayMs > 0) await sleep(delayMs);

  const batchId = batchIdForCountry(country, runSuffix);
  const minQuality = minQualityScoreFromTier(minQualityTier);

  let result;
  try {
    result = await fetchOsmHotelCandidates({
    country,
    batchId,
    hotelFocused,
    includeApartments,
    includeUnnamed,
    minQuality,
    maxElements,
    limit,
    });
  } catch (firstErr) {
    const msg = firstErr.message || String(firstErr);
    if (!/504|timeout|runtime/i.test(msg)) throw firstErr;
    await sleep(15000);
    result = await fetchOsmHotelCandidates({
      country,
      batchId,
      hotelFocused,
      includeApartments,
      includeUnnamed,
      minQuality,
      maxElements,
      limit,
    });
  }

  const summary = summarizeCandidates(result.candidates, {
    filtering: result.filtering,
    capping: result.capping,
    sourcePolicy: sourcePolicySummaryForReport(),
  });

  const jsonPath = join(reportsDir, `independent-census-osm-dry-run-${batchId}.json`);
  const csvPath = join(reportsDir, `independent-census-osm-dry-run-${batchId}.csv`);

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "4M-osm-country-dry-run",
    batchId,
    country,
    filterOptions: {
      hotelFocused,
      includeApartments,
      includeUnnamed,
      minQualityTier,
      minQualityScore: minQuality,
      maxElements,
    },
    dryRun: true,
    airtableWrites: false,
    candidateCount: result.candidates.length,
    filtering: result.filtering,
    capping: result.capping,
    summary,
    reportFiles: { json: jsonPath, csv: csvPath },
    candidates: result.candidates.map(stripInternalFields),
  };

  writeJson(jsonPath, report);
  writeCsv(csvPath, result.candidates.map(candidateToCsvRow), CSV_COLUMNS);

  return {
    country,
    batchId,
    candidateCount: result.candidates.length,
    jsonPath,
    csvPath,
    report,
    candidates: result.candidates,
    error: null,
  };
}

/**
 * @param {object} countryResult — from dry-run
 * @param {import('airtable').Base} base
 */
export async function applyOsmCountryCandidates(countryResult, base) {
  const { batchId, candidates } = countryResult;
  const existingKeys = await loadExistingCandidateKeys(
    base,
    batchId,
    CANDIDATES_TABLE
  );

  const rows = candidates.map((candidate) => ({
    candidate: {
      ...candidate,
      sourceType: candidate.sourceType || SOURCE_TYPES.OSM,
      importBatchId: batchId,
    },
    matchRow: null,
  }));

  const result = await createCandidateRecords(
    base,
    CANDIDATES_TABLE,
    rows,
    existingKeys
  );

  return {
    country: countryResult.country,
    batchId,
    written: result.writtenCount,
    skippedDuplicate: result.skippedDuplicate.length,
    created: result.created,
  };
}

/**
 * @param {string[]} countries
 * @param {object} opts
 */
export async function runOsmCountryList(countries, opts) {
  const {
    reportsDir,
    summaryBatchId = "choice-cala-osm-expansion-2026-05-20",
    apply = false,
    delayBetweenCountriesMs = 8000,
    ...dryRunOpts
  } = opts;

  const countryResults = [];
  const errors = [];

  for (let i = 0; i < countries.length; i++) {
    const country = countries[i];
    try {
      const delayMs = i > 0 ? delayBetweenCountriesMs : 0;
      const result = await runOsmCountryDryRun(country, {
        reportsDir,
        delayMs,
        ...dryRunOpts,
      });
      countryResults.push(result);
    } catch (err) {
      errors.push({ country, error: err.message || String(err) });
      countryResults.push({
        country,
        batchId: batchIdForCountry(country, dryRunOpts.runSuffix),
        candidateCount: 0,
        error: err.message || String(err),
      });
    }
  }

  const applyResults = [];
  let totalWritten = 0;
  let totalSkippedDuplicate = 0;

  if (apply) {
    const base = getIndependentCensusBase();
    if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

    for (const cr of countryResults) {
      if (cr.error || !cr.candidates?.length) {
        applyResults.push({
          country: cr.country,
          batchId: cr.batchId,
          written: 0,
          skippedDuplicate: 0,
          skippedReason: cr.error || "no candidates",
        });
        continue;
      }
      const ar = await applyOsmCountryCandidates(cr, base);
      applyResults.push(ar);
      totalWritten += ar.written;
      totalSkippedDuplicate += ar.skippedDuplicate;
    }
  }

  const byCountry = {};
  for (const cr of countryResults) {
    byCountry[cr.country] = cr.candidateCount ?? 0;
  }

  const summary = {
    generatedAt: new Date().toISOString(),
    phase: "4M-osm-country-list",
    summaryBatchId,
    countries,
    countryCount: countries.length,
    dryRun: !apply,
    apply,
    totalCandidatesDryRun: countryResults.reduce(
      (s, c) => s + (c.candidateCount || 0),
      0
    ),
    candidateCountByCountry: byCountry,
    totalWritten,
    totalSkippedDuplicate,
    errors,
    countryResults: countryResults.map((cr) => ({
      country: cr.country,
      batchId: cr.batchId,
      candidateCount: cr.candidateCount ?? 0,
      jsonPath: cr.jsonPath || null,
      csvPath: cr.csvPath || null,
      error: cr.error || null,
    })),
    applyResults,
    filterOptions: {
      hotelFocused: dryRunOpts.hotelFocused !== false,
      includeApartments: !!dryRunOpts.includeApartments,
      includeUnnamed: !!dryRunOpts.includeUnnamed,
      minQualityTier: dryRunOpts.minQualityTier || "medium",
      maxElements: dryRunOpts.maxElements ?? DEFAULT_MAX_ELEMENTS,
    },
    airtableWrites: apply,
    tablesWritten: apply ? [CANDIDATES_TABLE] : [],
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    verifiedTableWrites: false,
    evidenceTableWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
  };

  const summaryJsonPath = join(
    reportsDir,
    `independent-census-osm-country-list-${summaryBatchId}.json`
  );
  const summaryCsvPath = join(
    reportsDir,
    `independent-census-osm-country-list-${summaryBatchId}.csv`
  );

  const csvRows = countryResults.map((cr) => ({
    country: cr.country,
    batchId: cr.batchId,
    candidateCount: cr.candidateCount ?? 0,
    written: applyResults.find((a) => a.country === cr.country)?.written ?? "",
    skippedDuplicate:
      applyResults.find((a) => a.country === cr.country)?.skippedDuplicate ?? "",
    error: cr.error || "",
    dryRunJson: cr.jsonPath || "",
  }));

  writeJson(summaryJsonPath, {
    ...summary,
    reportFiles: { json: summaryJsonPath, csv: summaryCsvPath },
  });
  writeCsv(summaryCsvPath, csvRows, [
    "country",
    "batchId",
    "candidateCount",
    "written",
    "skippedDuplicate",
    "error",
    "dryRunJson",
  ]);

  return {
    summary,
    summaryJsonPath,
    summaryCsvPath,
    countryResults,
  };
}
