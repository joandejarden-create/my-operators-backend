/**
 * Match Accor census rows to all.accor.com sitemap hotel URLs (metadata-enriched).
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

const ACCOR_PARENT_FORMULA = `FIND("Accor", {${CENSUS_FIELDS.parentCompany}})`;
export const DEFAULT_ACCOR_EXTRACT_JSON = "reports/accor-property-directory-extract.json";

/**
 * @param {string} jsonPath
 */
export function loadAccorDirectoryRows(jsonPath = DEFAULT_ACCOR_EXTRACT_JSON) {
  if (!existsSync(jsonPath)) return [];
  const data = JSON.parse(readFileSync(jsonPath, "utf8"));
  const rows = Array.isArray(data.propertyRows) ? data.propertyRows : [];
  return rows.filter(
    (r) =>
      r.calaFilterStatus === "included" &&
      String(r.country || "").trim() &&
      String(r.inferredHotelName || r.propertyId || "").trim()
  );
}

/**
 * @param {object} [opts]
 */
export async function planAccorCensusSitemapMatch(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const jsonPath = opts.jsonPath || DEFAULT_ACCOR_EXTRACT_JSON;
  const directoryRows = loadAccorDirectoryRows(jsonPath);
  if (!directoryRows.length) {
    throw new Error(`No Accor directory rows at ${jsonPath}. Run extract with --fetch-metadata first.`);
  }

  const selectFields = await getCensusEnrichmentSelectFields(base);
  for (const f of ["Website", "Amenities", "Property ID"]) {
    if (!selectFields.includes(f)) selectFields.push(f);
  }

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: selectFields,
      filterByFormula: ACCOR_PARENT_FORMULA,
      pageSize: 100,
    })
    .all();

  const censusRows = records.map(mapCensusRowForDirectoryMatch);
  const minScore = opts.minScore ?? 58;
  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];

  for (const censusRow of censusRows) {
    let best = null;
    let bestScore = 0;

    for (const dir of directoryRows) {
      const directoryRow = {
        name: dir.inferredHotelName,
        city: dir.city || "",
        country: dir.country || "",
        website: dir.propertyUrl,
        brandPropertyCode: dir.propertyId?.toUpperCase(),
        latitude: dir.latitude ?? null,
        longitude: dir.longitude ?? null,
        source: dir.source,
      };
      const scored = scoreDirectoryAgainstCensus(directoryRow, censusRow);
      if (scored.score > bestScore) {
        bestScore = scored.score;
        best = { dir, scored };
      }
    }

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
    if (isBlankCensusValue(censusRow.fields?.Website) && best.dir.propertyUrl) {
      applyFields.Website = best.dir.propertyUrl;
    }
    if (isBlankCensusValue(censusRow.fields?.["Property ID"]) && best.dir.propertyId) {
      applyFields["Property ID"] = String(best.dir.propertyId).toUpperCase();
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
      propertyUrl: best.dir.propertyUrl,
      directoryHotelName: best.dir.inferredHotelName,
      matchScore: bestScore,
      matchConfidence: best.scored.confidence,
      matchReason: best.scored.reason,
      applyFields,
      status: "ready",
    });
  }

  return {
    jsonPath,
    directoryRowsLoaded: directoryRows.length,
    censusRowsScanned: censusRows.length,
    readyToApply: planRows.length,
    planRows,
    skipped,
  };
}
