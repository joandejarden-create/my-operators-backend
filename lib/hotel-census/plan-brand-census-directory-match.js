/**
 * Shared brand directory → Hotel Census match planner (Phase 0 expansion).
 */

import { writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import {
  mapCensusRowForDirectoryMatch,
  scoreDirectoryAgainstCensus,
} from "./match-brand-directory-to-census.js";
import { isBlankCensusValue } from "./brand-directory-enrichment-contract.js";
import { countriesMatch, normalizeCountry } from "../independent-census/match-current-census.js";
import { writeCsv } from "../str-census-import/report-utils.mjs";
import {
  normalizeAccorDirectoryName,
  accorCanonicalPropertyUrl,
} from "./accor-directory-name-normalize.js";

const CONFIDENCE_RANK = { high: 3, medium: 2, low: 1, none: 0 };

/**
 * @param {object} dir
 */
export function mapExtractRowToDirectoryMatchRow(dir, opts = {}) {
  const rawName = dir.inferredHotelName || dir.name || "";
  const isAccor = opts.scoringProfile === "accor" || dir.source === "accor_sitemap";
  const matchName = isAccor ? normalizeAccorDirectoryName(rawName) : rawName;
  const propertyId = String(dir.propertyId || dir.brandPropertyCode || "").toUpperCase();
  const propertyUrl =
    (isAccor && propertyId ? accorCanonicalPropertyUrl(propertyId) : "") ||
    dir.propertyUrl ||
    dir.website ||
    "";

  return {
    name: rawName,
    matchName,
    city: dir.city || String(dir.citySlug || "").replace(/-/g, " "),
    country: dir.country || dir.inferredCountry || "",
    website: propertyUrl,
    brandPropertyCode: propertyId,
    latitude: dir.latitude ?? dir.lat ?? null,
    longitude: dir.longitude ?? dir.lng ?? null,
    source: dir.source || "brand_directory",
    scoringProfile: opts.scoringProfile || "",
    amenitiesText: dir.amenitiesText || "",
    propertyUrl,
    propertyId,
  };
}

/**
 * Greedy one-to-one assignment by score (highest first).
 * @param {object[]} pairs
 */
export function assignGreedyOneToOne(pairs) {
  const sorted = [...pairs].sort((a, b) => b.score - a.score);
  const usedCensus = new Set();
  const usedDirectory = new Set();
  /** @type {typeof pairs} */
  const assigned = [];

  for (const pair of sorted) {
    const dirKey = pair.directoryKey;
    if (usedCensus.has(pair.censusRecordId) || usedDirectory.has(dirKey)) continue;
    usedCensus.add(pair.censusRecordId);
    usedDirectory.add(dirKey);
    assigned.push(pair);
  }
  return assigned;
}

/**
 * @param {object} opts
 * @param {string} opts.parentFormula Airtable filterByFormula
 * @param {object[]} opts.directoryRows raw extract rows (CALA-filtered)
 * @param {number} [opts.minScore]
 * @param {boolean} [opts.requireCountryMatch]
 * @param {string} [opts.minApplyConfidence] high|medium|low
 * @param {boolean} [opts.includeAmenitiesFromCache]
 */
export async function planBrandCensusDirectoryMatch(opts) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const minScore = opts.minScore ?? 50;
  const requireCountryMatch = opts.requireCountryMatch !== false;
  const minApplyRank = CONFIDENCE_RANK[opts.minApplyConfidence || "medium"] ?? 2;
  const includeAmenities = opts.includeAmenitiesFromCache !== false;

  const selectFields = await getCensusEnrichmentSelectFields(base);
  for (const f of ["Website", "Amenities", "Property ID", CENSUS_FIELDS.city, CENSUS_FIELDS.country]) {
    if (!selectFields.includes(f)) selectFields.push(f);
  }

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: selectFields,
      filterByFormula: opts.parentFormula,
      pageSize: 100,
    })
    .all();

  const censusRows = records.map(mapCensusRowForDirectoryMatch);
  const directoryMatchRows = opts.directoryRows.map((dir) => ({
    raw: dir,
    match: mapExtractRowToDirectoryMatchRow(dir, {
      scoringProfile: opts.scoringProfile || "",
    }),
    directoryKey: String(dir.propertyUrl || dir.propertyId || "").toLowerCase(),
  }));

  /** @type {object[]} */
  const pairCandidates = [];
  for (const censusRow of censusRows) {
    for (const { raw, match, directoryKey } of directoryMatchRows) {
      if (requireCountryMatch) {
        const dirCountry = normalizeCountry(match.country);
        const censusCountry = censusRow.countryNorm || normalizeCountry(censusRow.country);
        if (dirCountry && censusCountry && !countriesMatch(match.country, censusRow.country)) {
          continue;
        }
      }
      const scored = scoreDirectoryAgainstCensus(match, censusRow);
      if (scored.score < minScore) continue;
      pairCandidates.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        censusCity: censusRow.city,
        censusCountry: censusRow.country,
        directoryKey,
        directoryName: match.name,
        directoryCity: match.city,
        directoryCountry: match.country,
        propertyUrl: raw.propertyUrl,
        propertyId: raw.propertyId,
        amenitiesText: raw.amenitiesText || "",
        score: scored.score,
        matchConfidence: scored.confidence,
        matchReason: scored.reason,
        nameSim: scored.nameSim,
        distanceMeters: scored.distanceMeters,
        censusRow,
        raw,
      });
    }
  }

  const assigned = assignGreedyOneToOne(
    pairCandidates.map((p) => ({ ...p, score: p.score }))
  );

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const stewardRows = [];
  /** @type {object[]} */
  const skipped = [];

  const assignedCensusIds = new Set(assigned.map((a) => a.censusRecordId));

  for (const row of assigned) {
    const applyRank = CONFIDENCE_RANK[row.matchConfidence] ?? 0;
    const applyFields = {};
    const f = row.censusRow.fields || {};

    if (isBlankCensusValue(f.Website) && row.propertyUrl) {
      applyFields.Website = row.propertyUrl;
    }
    if (isBlankCensusValue(f["Property ID"]) && row.propertyId) {
      applyFields["Property ID"] = String(row.propertyId).toUpperCase();
    }
    if (
      includeAmenities &&
      isBlankCensusValue(f.Amenities) &&
      row.amenitiesText &&
      applyRank >= minApplyRank
    ) {
      applyFields.Amenities = row.amenitiesText;
    }

    const entry = {
      censusRecordId: row.censusRecordId,
      censusName: row.censusName,
      censusCity: row.censusCity,
      censusCountry: row.censusCountry,
      directoryHotelName: row.directoryName,
      directoryCity: row.directoryCity,
      directoryCountry: row.directoryCountry,
      propertyId: row.propertyId,
      propertyUrl: row.propertyUrl,
      matchScore: row.score,
      matchConfidence: row.matchConfidence,
      matchReason: row.matchReason,
      nameSim: row.nameSim,
      distanceMeters: row.distanceMeters,
      applyFields,
      autoApply: applyRank >= minApplyRank && Object.keys(applyFields).length > 0,
      status: applyRank >= minApplyRank ? "ready" : "steward_review",
    };

    if (entry.autoApply) planRows.push(entry);
    else stewardRows.push(entry);
  }

  for (const censusRow of censusRows) {
    if (assignedCensusIds.has(censusRow.recordId)) continue;
    const hasWebsite = !isBlankCensusValue(censusRow.fields?.Website);
    const hasAmenities = !isBlankCensusValue(censusRow.fields?.Amenities);
    if (hasWebsite && hasAmenities) continue;

    let bestScore = 0;
    let best = null;
    for (const { match, raw } of directoryMatchRows) {
      if (requireCountryMatch) {
        const dirCountry = normalizeCountry(match.country);
        const censusCountry = censusRow.countryNorm || normalizeCountry(censusRow.country);
        if (dirCountry && censusCountry && !countriesMatch(match.country, censusRow.country)) {
          continue;
        }
      }
      const scored = scoreDirectoryAgainstCensus(match, censusRow);
      if (scored.score > bestScore) {
        bestScore = scored.score;
        best = { match, raw, scored };
      }
    }
    skipped.push({
      censusRecordId: censusRow.recordId,
      censusName: censusRow.name,
      reason: bestScore < minScore ? "below_min_score" : "unassigned_after_greedy",
      bestScore,
      bestDirectoryName: best?.match?.name || "",
      bestConfidence: best?.scored?.confidence || "none",
    });
  }

  return {
    directoryRowsLoaded: directoryMatchRows.length,
    censusRowsScanned: censusRows.length,
    pairCandidates: pairCandidates.length,
    assignedCount: assigned.length,
    readyToApply: planRows.length,
    stewardReviewCount: stewardRows.length,
    skippedCount: skipped.length,
    planRows,
    stewardRows,
    skipped,
  };
}

export const STEWARD_CSV_COLUMNS = [
  "censusRecordId",
  "censusName",
  "censusCity",
  "censusCountry",
  "directoryHotelName",
  "directoryCity",
  "directoryCountry",
  "propertyUrl",
  "propertyId",
  "matchScore",
  "matchConfidence",
  "matchReason",
  "nameSim",
  "distanceMeters",
  "recommendedAction",
];

/**
 * @param {object} plan
 * @param {string} reportBasename e.g. wyndham-census-match-expansion
 * @param {string} [reportsDir]
 */
export function writeBrandCensusMatchExpansionReports(plan, reportBasename, reportsDir = "reports") {
  mkdirSync(reportsDir, { recursive: true });
  const jsonPath = join(reportsDir, `${reportBasename}-plan.json`);
  writeFileSync(
    jsonPath,
    JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2)
  );

  const stewardPath = join(reportsDir, `${reportBasename}-steward-review.csv`);
  writeCsv(
    stewardPath,
    plan.stewardRows.map((r) => ({
      censusRecordId: r.censusRecordId,
      censusName: r.censusName,
      censusCity: r.censusCity,
      censusCountry: r.censusCountry,
      directoryHotelName: r.directoryHotelName,
      directoryCity: r.directoryCity,
      directoryCountry: r.directoryCountry,
      propertyUrl: r.propertyUrl,
      propertyId: r.propertyId,
      matchScore: r.matchScore,
      matchConfidence: r.matchConfidence,
      matchReason: r.matchReason,
      nameSim: r.nameSim,
      distanceMeters: r.distanceMeters ?? "",
      recommendedAction: "manual_verify_before_apply",
    })),
    STEWARD_CSV_COLUMNS
  );

  return { jsonPath, stewardPath };
}
