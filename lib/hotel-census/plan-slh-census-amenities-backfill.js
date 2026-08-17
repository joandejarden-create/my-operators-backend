/**
 * Plan Amenities backfill for CALA SLH census rows from slh.com keyFeatures.
 */

import {
  fetchSlhCalaProperties,
  formatSlhAmenitiesText,
  SLH_AFFILIATION,
  isCalaCountry,
  scoreSlhCensusMatch,
} from "../slh-census-enrichment.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { isBlankCensusValue } from "./brand-directory-enrichment-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../hilton-amenity-map.js";
import { CENSUS_DESCRIPTION_FIELD } from "./hilton-description-enrichment-contract.js";

/**
 * @param {import('airtable').Record} census
 * @param {ReturnType<import("../slh-census-enrichment.js").normalizeSlhHotelItem>[]} sources
 */
function pickSlhSourceForCensus(census, sources) {
  const website = String(census.fields.Website || "").toLowerCase();
  for (const source of sources) {
    if (website && source.propertyUrl && website.includes(source.slug)) return { source, score: 100, reason: "website" };
  }

  let best = null;
  for (const source of sources) {
    if (census.fields[CENSUS_FIELDS.country] !== source.censusCountry) continue;
    const { score, reason } = scoreSlhCensusMatch(source, census);
    if (score >= 80 && (!best || score > best.score)) best = { source, score, reason };
  }
  return best;
}

/**
 * @param {object} [opts]
 * @param {boolean} [opts.refreshAmenities]
 * @param {string[]} [opts.recordIds]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function planSlhCensusAmenitiesBackfill(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const fields = [
    "name",
    "Website",
    CENSUS_FIELDS.country,
    CENSUS_FIELDS.city,
    CENSUS_FIELDS.affiliation,
    CENSUS_AMENITIES_TEXT_FIELD,
    CENSUS_DESCRIPTION_FIELD,
  ];

  const records = await base(HOTEL_CENSUS_TABLE).select({ fields, pageSize: 100 }).all();
  let targets = records.filter(
    (r) =>
      isCalaCountry(r.fields[CENSUS_FIELDS.country]) &&
      r.fields[CENSUS_FIELDS.affiliation] === SLH_AFFILIATION
  );
  if (opts.recordIds?.length) {
    const want = new Set(opts.recordIds);
    targets = targets.filter((r) => want.has(r.id));
  }

  opts.onProgress?.(`Load SLH directory for ${targets.length} census rows…`);
  const sources = await fetchSlhCalaProperties({ onProgress: opts.onProgress });

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];

  for (const rec of targets) {
    const hit = pickSlhSourceForCensus(rec, sources);
    if (!hit) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        reason: "no_slh_source_match",
      });
      continue;
    }

    const amenitiesText = formatSlhAmenitiesText(hit.source.keyFeatures || []);
    /** @type {Record<string, unknown>} */
    const applyFields = {};

    const currentAmenities = rec.fields[CENSUS_AMENITIES_TEXT_FIELD];
    if (amenitiesText) {
      if (opts.refreshAmenities || isBlankCensusValue(currentAmenities)) {
        applyFields[CENSUS_AMENITIES_TEXT_FIELD] = amenitiesText;
      }
    }

    if (
      hit.source.description &&
      isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD])
    ) {
      applyFields[CENSUS_DESCRIPTION_FIELD] = String(hit.source.description).slice(0, 2000);
    }

    if (!Object.keys(applyFields).length) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        reason: "no_fill_blank_fields",
        amenitiesFromSlh: amenitiesText,
      });
      continue;
    }

    planRows.push({
      censusRecordId: rec.id,
      censusName: rec.fields.name,
      propertyUrl: hit.source.propertyUrl,
      matchScore: hit.score,
      matchReason: hit.reason,
      amenitiesText,
      applyFields,
    });
  }

  return {
    generatedAt: new Date().toISOString(),
    censusRowsScanned: targets.length,
    readyToApply: planRows.length,
    skipped,
    planRows,
  };
}

export async function auditSlhAmenitiesCoverage() {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");
  const fields = [
    "name",
    CENSUS_FIELDS.country,
    CENSUS_FIELDS.affiliation,
    CENSUS_AMENITIES_TEXT_FIELD,
  ];
  const records = await base(HOTEL_CENSUS_TABLE).select({ fields, pageSize: 100 }).all();
  const rows = records.filter(
    (r) =>
      isCalaCountry(r.fields[CENSUS_FIELDS.country]) &&
      r.fields[CENSUS_FIELDS.affiliation] === SLH_AFFILIATION
  );
  const blank = rows.filter((r) => isBlankCensusValue(r.fields[CENSUS_AMENITIES_TEXT_FIELD]));
  return {
    total: rows.length,
    blankAmenities: blank.length,
    blankNames: blank.map((r) => r.fields.name),
  };
}
