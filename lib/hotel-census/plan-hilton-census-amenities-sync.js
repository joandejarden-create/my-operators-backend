/**
 * Plan Amenities column sync for Hilton-parent Hotel Census rows from hilton.com directory.
 */

import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import { mapCensusRowForDirectoryMatch } from "./match-brand-directory-to-census.js";
import { resolveCensusCtyhocn } from "./plan-hilton-census-descriptions.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../hilton-amenity-map.js";
import {
  loadHiltonAmenityIndex,
  normalizeAmenitiesCompare,
} from "./hilton-amenity-index.js";
import { isBlankCensusValue } from "./brand-directory-enrichment-contract.js";
import { buildCityPageAmenityFallbackIndex } from "./hilton-amenity-city-fallback.js";

const HILTON_PARENT_FORMULA = `FIND("Hilton", {${CENSUS_FIELDS.parentCompany}})`;

/**
 * @param {object} [opts]
 */
export async function planHiltonCensusAmenitiesSync(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const index = await loadHiltonAmenityIndex({
    refreshCrawl: opts.refreshCrawl,
    crawlDelayMs: opts.crawlDelayMs,
    brandCodes: opts.brandCodes,
    enrichmentPlanPath: opts.enrichmentPlanPath,
    onProgress: opts.onProgress,
  });

  const planPath = opts.enrichmentPlanPath || "reports/hilton-census-enrichment-plan-all-brands.json";
  const planByCensusId = new Map();
  const { readFileSync, existsSync } = await import("node:fs");
  if (existsSync(planPath)) {
    const plan = JSON.parse(readFileSync(planPath, "utf8"));
    for (const row of plan.planRows || []) {
      const id = String(row.censusRecordId || "").trim();
      const text = String(row.amenitiesTextSuggested || "").trim();
      if (id && text) planByCensusId.set(id, text);
    }
  }

  const selectFields = await getCensusEnrichmentSelectFields(base);
  if (!selectFields.includes(CENSUS_AMENITIES_TEXT_FIELD)) {
    selectFields.push(CENSUS_AMENITIES_TEXT_FIELD);
  }

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: selectFields,
      filterByFormula: HILTON_PARENT_FORMULA,
      pageSize: 100,
    })
    .all();

  const planRows = [];
  const skipped = [];
  /** @type {object[]} */
  const cityFallbackCandidates = [];

  for (const rec of records) {
    const censusRow = mapCensusRowForDirectoryMatch(rec);
    const current = String(rec.fields?.[CENSUS_AMENITIES_TEXT_FIELD] || "").trim();
    let ctyhocn = resolveCensusCtyhocn(censusRow);
    let amenitiesText = "";
    let source = "";

    if (ctyhocn && index.has(ctyhocn)) {
      amenitiesText = index.get(ctyhocn).amenitiesText;
      source = "hilton_directory_ctyhocn";
    } else if (planByCensusId.has(rec.id)) {
      amenitiesText = planByCensusId.get(rec.id);
      source = "enrichment_plan_census_match";
      if (!ctyhocn) ctyhocn = "";
    }

    if (!amenitiesText && ctyhocn) {
      cityFallbackCandidates.push({
        censusRecordId: rec.id,
        censusName: censusRow.name,
        ctyhocn,
        affiliation: censusRow.affiliation,
        city: censusRow.city,
        country: censusRow.country,
        currentAmenities: current,
      });
      continue;
    }

    if (!amenitiesText) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: censusRow.name,
        ctyhocn,
        reason: ctyhocn ? "no_directory_amenities" : "no_ctyhocn",
        currentAmenities: current,
      });
      continue;
    }

    const unchanged = normalizeAmenitiesCompare(current) === normalizeAmenitiesCompare(amenitiesText);
    if (unchanged && !opts.force) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: censusRow.name,
        ctyhocn,
        reason: "already_correct",
        currentAmenities: current,
      });
      continue;
    }

    if (opts.fillBlankOnly && !isBlankCensusValue(current)) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: censusRow.name,
        ctyhocn,
        reason: "has_value_fill_blank_only",
        currentAmenities: current,
      });
      continue;
    }

    planRows.push({
      censusRecordId: rec.id,
      censusName: censusRow.name,
      ctyhocn,
      currentAmenities: current,
      amenitiesText,
      source,
      status: "ready",
      applyFields: {
        [CENSUS_AMENITIES_TEXT_FIELD]: amenitiesText,
      },
    });
  }

  if (cityFallbackCandidates.length && opts.useCityFallback !== false) {
    const cityIndex = await buildCityPageAmenityFallbackIndex(cityFallbackCandidates, {
      pageDelayMs: opts.pageDelayMs ?? 150,
      onProgress: opts.onProgress,
    });

    for (const row of cityFallbackCandidates) {
      const amenitiesText = cityIndex.get(row.ctyhocn) || "";
      if (!amenitiesText) {
        skipped.push({
          censusRecordId: row.censusRecordId,
          censusName: row.censusName,
          ctyhocn: row.ctyhocn,
          reason: "no_directory_amenities",
          currentAmenities: row.currentAmenities,
        });
        continue;
      }

      const unchanged =
        normalizeAmenitiesCompare(row.currentAmenities) === normalizeAmenitiesCompare(amenitiesText);
      if (unchanged && !opts.force) {
        skipped.push({
          censusRecordId: row.censusRecordId,
          censusName: row.censusName,
          ctyhocn: row.ctyhocn,
          reason: "already_correct",
          currentAmenities: row.currentAmenities,
        });
        continue;
      }

      if (opts.fillBlankOnly && !isBlankCensusValue(row.currentAmenities)) {
        skipped.push({
          censusRecordId: row.censusRecordId,
          censusName: row.censusName,
          ctyhocn: row.ctyhocn,
          reason: "has_value_fill_blank_only",
          currentAmenities: row.currentAmenities,
        });
        continue;
      }

      planRows.push({
        censusRecordId: row.censusRecordId,
        censusName: row.censusName,
        ctyhocn: row.ctyhocn,
        currentAmenities: row.currentAmenities,
        amenitiesText,
        source: "hilton_city_page_ctyhocn",
        status: "ready",
        applyFields: {
          [CENSUS_AMENITIES_TEXT_FIELD]: amenitiesText,
        },
      });
    }
  }

  return {
    indexSize: index.size,
    censusRowsScanned: records.length,
    readyToApply: planRows.length,
    skipped,
    planRows,
  };
}
