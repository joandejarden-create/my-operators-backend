/**
 * Match Wyndham census rows to wyndhamhotels.com sitemap URLs.
 * brandSlug-gated for Wave 1 soft brands (dazzler, trademark).
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
import { nameSimilarity } from "../independent-census/match-current-census.js";
import { isCalaCountry } from "../design-hotels-census-enrichment.js";

const WYNDHAM_PARENT_FORMULA = `FIND("Wyndham", {${CENSUS_FIELDS.parentCompany}})`;
export const DEFAULT_WYNDHAM_EXTRACT_JSON = "reports/wyndham-property-directory-extract.json";

/** Exact Brand Setup Affiliation → allowed sitemap brandSlug values. */
export const WYNDHAM_AFFILIATION_BRAND_SLUGS = {
  "Dazzler by Wyndham": ["dazzler"],
  "Trademark Collection by Wyndham": ["trademark"],
};

export const WYNDHAM_WAVE1_AFFILIATIONS = Object.keys(WYNDHAM_AFFILIATION_BRAND_SLUGS);

export const MAP_WYNDHAM_CENSUS_BACKFILL = {
  website: "Website",
  propertyId: "Property ID",
  amenities: "Amenities",
};

/**
 * Normalize Trademark / Dazzler / Viva display names for matching.
 * @param {string} name
 */
export function normalizeWyndhamHotelNameForMatch(name) {
  return String(name || "")
    .replace(/\u200b/g, "")
    .replace(/[-–—]/g, " ")
    .replace(/\ba\s+trademark\s+(?:collection\s+)?(?:by\s+wyndham\s+)?all\s+inclusive(?:\s+resort)?\b/gi, " ")
    .replace(/\btrademark\s+collection\s+by\s+wyndham\b/gi, " ")
    .replace(/\btrademark\s+by\s+wyndham\b/gi, " ")
    .replace(/\btrademark\s+collection\b/gi, " ")
    .replace(/\bdazzler\s+by\s+wyndham\b/gi, "dazzler")
    .replace(/\bby\s+wyndham\b/gi, " ")
    .replace(/\bonly\s+adults?\b/gi, " ")
    .replace(/\badults?\s+all\s+inclusive\b/gi, " ")
    .replace(/\ball\s+inclusive(?:\s+resort)?\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {string} affiliation
 * @returns {string[]|null} null = no gate (all brandSlugs allowed)
 */
export function wyndhamAllowedBrandSlugs(affiliation) {
  const aff = String(affiliation || "").trim();
  if (WYNDHAM_AFFILIATION_BRAND_SLUGS[aff]) return WYNDHAM_AFFILIATION_BRAND_SLUGS[aff];
  return null;
}

/**
 * @param {string} jsonPath
 */
export function loadWyndhamDirectoryRows(jsonPath = DEFAULT_WYNDHAM_EXTRACT_JSON) {
  if (!existsSync(jsonPath)) return [];
  const data = JSON.parse(readFileSync(jsonPath, "utf8"));
  const rows = Array.isArray(data.propertyRows) ? data.propertyRows : [];
  return rows
    .filter((r) => r.calaFilterStatus === "included" || !r.calaFilterStatus)
    .map((row) => ({
      ...row,
      inferredHotelName: row.inferredHotelName || row.propertySlug || "",
    }));
}

/**
 * @param {object} [opts]
 * @param {string} [opts.jsonPath]
 * @param {string[]} [opts.affiliations] — exact Affiliation filter (Wave 1)
 * @param {boolean} [opts.calaOnly]
 * @param {number} [opts.minScore]
 * @param {number} [opts.minNameSim]
 * @param {string} [opts.minConfidence]
 */
export async function planWyndhamCensusSitemapMatch(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const jsonPath = opts.jsonPath || DEFAULT_WYNDHAM_EXTRACT_JSON;
  let directoryRows = loadWyndhamDirectoryRows(jsonPath);
  if (!directoryRows.length) {
    throw new Error(`No Wyndham directory rows at ${jsonPath}. Run extract first.`);
  }

  const affiliationFilter = Array.isArray(opts.affiliations)
    ? opts.affiliations.filter(Boolean)
    : null;
  if (affiliationFilter?.length) {
    const allowedSlugs = new Set(
      affiliationFilter.flatMap((a) => WYNDHAM_AFFILIATION_BRAND_SLUGS[a] || [])
    );
    if (allowedSlugs.size) {
      directoryRows = directoryRows.filter((d) =>
        allowedSlugs.has(String(d.brandSlug || "").toLowerCase())
      );
    }
  }

  const selectFields = await getCensusEnrichmentSelectFields(base);
  for (const f of ["Website", "Amenities", "Property ID", CENSUS_FIELDS.affiliation]) {
    if (!selectFields.includes(f)) selectFields.push(f);
  }

  let filterByFormula = WYNDHAM_PARENT_FORMULA;
  if (affiliationFilter?.length) {
    filterByFormula = `OR(${affiliationFilter
      .map((a) => `{${CENSUS_FIELDS.affiliation}}="${a}"`)
      .join(",")})`;
  }

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: selectFields,
      filterByFormula,
      pageSize: 100,
    })
    .all();

  let censusRows = records.map(mapCensusRowForDirectoryMatch);
  if (opts.calaOnly !== false) {
    censusRows = censusRows.filter((r) =>
      isCalaCountry(r.fields?.[CENSUS_FIELDS.country] || r.country)
    );
  }

  const minScore = opts.minScore ?? 55;
  const minNameSim = opts.minNameSim ?? 0.5;
  const applyMinScore = opts.applyMinScore ?? 60;
  const applyMinNameSim = opts.applyMinNameSim ?? 0.55;
  const confRank = { none: 0, low: 1, medium: 2, high: 3 };
  const applyConfRank = confRank[opts.minConfidence || "medium"] ?? 2;

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];
  /** @type {object[]} */
  const stewardReview = [];
  const usedDir = new Set();
  const usedCensus = new Set();

  /** @type {object[]} */
  const candidates = [];

  for (const censusRow of censusRows) {
    const aff = String(
      censusRow.fields?.[CENSUS_FIELDS.affiliation] || censusRow.affiliation || ""
    ).trim();
    const allowedSlugs = wyndhamAllowedBrandSlugs(aff);
    const websiteBlank = isBlankCensusValue(censusRow.fields?.Website);
    const pidBlank = isBlankCensusValue(censusRow.fields?.["Property ID"]);
    if (!websiteBlank && !pidBlank) {
      skipped.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        affiliation: aff,
        reason: "no_blank_website_or_property_id",
      });
      continue;
    }

    for (const dir of directoryRows) {
      const slug = String(dir.brandSlug || "").toLowerCase();
      if (allowedSlugs && !allowedSlugs.includes(slug)) continue;

      const directoryRow = {
        name: normalizeWyndhamHotelNameForMatch(dir.inferredHotelName),
        city: dir.city || String(dir.citySlug || "").replace(/-/g, " "),
        country: dir.country || dir.inferredCountry || "",
        website: dir.propertyUrl,
        brandPropertyCode: dir.propertyId?.toUpperCase(),
        latitude: dir.latitude ?? null,
        longitude: dir.longitude ?? null,
        source: dir.source,
      };
      const scored = scoreDirectoryAgainstCensus(directoryRow, {
        ...censusRow,
        name: normalizeWyndhamHotelNameForMatch(censusRow.name),
      });
      const nameSim = nameSimilarity(
        normalizeWyndhamHotelNameForMatch(dir.inferredHotelName),
        normalizeWyndhamHotelNameForMatch(censusRow.name)
      );
      candidates.push({
        censusRow,
        dir,
        scored: { ...scored, nameSim: Math.max(scored.nameSim || 0, nameSim) },
        nameSim: Math.max(scored.nameSim || 0, nameSim),
        affiliation: aff,
      });
    }
  }

  candidates.sort((a, b) => b.scored.score - a.scored.score || b.nameSim - a.nameSim);

  for (const c of candidates) {
    const cid = c.censusRow.recordId;
    const did = c.dir.propertyUrl;
    if (usedCensus.has(cid) || usedDir.has(did)) continue;
    if (c.scored.score < minScore || c.nameSim < minNameSim) continue;

    usedCensus.add(cid);
    usedDir.add(did);

    const applyFields = {};
    if (isBlankCensusValue(c.censusRow.fields?.Website) && c.dir.propertyUrl) {
      applyFields.Website = c.dir.propertyUrl;
    }
    if (isBlankCensusValue(c.censusRow.fields?.["Property ID"]) && c.dir.propertyId) {
      applyFields["Property ID"] = String(c.dir.propertyId).toUpperCase();
    }

    const row = {
      censusRecordId: cid,
      censusName: c.censusRow.name,
      censusCountry: c.censusRow.country,
      affiliation: c.affiliation,
      brandSlug: c.dir.brandSlug,
      propertyId: c.dir.propertyId,
      propertyUrl: c.dir.propertyUrl,
      matchScore: c.scored.score,
      matchConfidence: c.scored.confidence,
      nameSim: c.nameSim,
      matchReason: c.scored.reason,
      applyFields,
      status: "ready",
    };

    if (!Object.keys(applyFields).length) {
      skipped.push({ ...row, reason: "no_blank_fields_to_fill" });
      continue;
    }

    if (
      isBlankCensusValue(c.censusRow.fields?.Website) &&
      !applyFields.Website &&
      (confRank[c.scored.confidence] ?? 0) >= 2
    ) {
      stewardReview.push({ ...row, reason: "medium_match_missing_website" });
      continue;
    }

    const passes =
      c.scored.score >= applyMinScore &&
      c.nameSim >= applyMinNameSim &&
      (confRank[c.scored.confidence] ?? 0) >= applyConfRank;

    if (!passes) {
      stewardReview.push({ ...row, status: "steward_review", reason: "below_apply_gate" });
      skipped.push({ ...row, reason: "below_apply_gate_steward_only" });
      continue;
    }

    planRows.push(row);
  }

  for (const c of censusRows) {
    if (usedCensus.has(c.recordId)) continue;
    if (skipped.some((s) => s.censusRecordId === c.recordId)) continue;
    skipped.push({
      censusRecordId: c.recordId,
      censusName: c.name,
      censusCountry: c.country,
      affiliation: c.fields?.[CENSUS_FIELDS.affiliation],
      reason: "no_directory_match",
    });
  }

  return {
    fieldMapping: MAP_WYNDHAM_CENSUS_BACKFILL,
    jsonPath,
    affiliationFilter: affiliationFilter || null,
    directoryRowsLoaded: directoryRows.length,
    censusRowsScanned: censusRows.length,
    readyToApply: planRows.length,
    planRows,
    stewardReview,
    skipped,
  };
}
