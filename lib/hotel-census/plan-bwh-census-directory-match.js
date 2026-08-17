/**
 * Match CALA BWH Hotel Census (BW Premier / BW Signature) to official directory seed.
 * Fill-blank Website + Property ID only. No Premier ↔ Signature cross-match.
 */

import {
  BWH_AFFILIATION_BY_FAMILY,
  BWH_PARENT_COMPANY,
  DEFAULT_BWH_SEED_JSON,
  loadBwhDirectorySeed,
  normalizeBwhPropertyCode,
} from "../bwh-brand-directory-extract.js";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import {
  mapCensusRowForDirectoryMatch,
  scoreDirectoryAgainstCensus,
} from "./match-brand-directory-to-census.js";
import { isBlankCensusValue } from "./brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "./hilton-property-id-contract.js";
import { isCalaCountry } from "../design-hotels-census-enrichment.js";
import { nameSimilarity } from "../independent-census/match-current-census.js";

export const MAP_BWH_CENSUS_BACKFILL = {
  website: "Website",
  propertyId: CENSUS_PROPERTY_ID_FIELD,
  amenities: "Amenities",
  affiliation: CENSUS_FIELDS.affiliation,
  parentCompany: CENSUS_FIELDS.parentCompany,
};

export const BWH_WAVE1_AFFILIATIONS = [
  "BW Premier Collection",
  "BW Signature Collection",
];

/**
 * @param {string} name
 */
export function normalizeBwhHotelNameForMatch(name) {
  return String(name || "")
    .replace(/\u200b/g, "")
    .replace(/[-–—]/g, " ")
    .replace(/\bbw\s+premier\s+collection\b/gi, " ")
    .replace(/\bbw\s+signature\s+collection\b/gi, " ")
    .replace(/\bbest\s+western\s+premier\b/gi, " ")
    .replace(/\bbest\s+western\b/gi, " ")
    .replace(/\bhotel\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {string} affiliation
 * @param {string} brandFamily
 */
export function bwhAffiliationAllowsFamily(affiliation, brandFamily) {
  const aff = String(affiliation || "").trim();
  const expected = BWH_AFFILIATION_BY_FAMILY[brandFamily];
  if (!expected) return false;
  return aff === expected;
}

/**
 * @param {object} [opts]
 */
export async function planBwhCensusDirectoryMatch(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const seedPath = opts.seedPath || DEFAULT_BWH_SEED_JSON;
  const directoryAll = loadBwhDirectorySeed(seedPath);
  const directoryRows = directoryAll.filter(
    (d) => d.statusOpen !== false && d.propertyCode && d.propertyUrl && /^https:\/\/www\.bestwestern\.com\//i.test(d.propertyUrl)
  );

  if (!directoryRows.length) {
    throw new Error(`No usable BWH directory rows at ${seedPath}`);
  }

  const selectFields = await getCensusEnrichmentSelectFields(base);
  for (const f of [MAP_BWH_CENSUS_BACKFILL.website, MAP_BWH_CENSUS_BACKFILL.propertyId, MAP_BWH_CENSUS_BACKFILL.amenities]) {
    if (!selectFields.includes(f)) selectFields.push(f);
  }

  const affFormula = `OR(${BWH_WAVE1_AFFILIATIONS.map((a) => `{${CENSUS_FIELDS.affiliation}}="${a}"`).join(",")})`;
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: selectFields,
      filterByFormula: affFormula,
      pageSize: 100,
    })
    .all();

  let censusRows = records.map(mapCensusRowForDirectoryMatch);
  if (opts.calaOnly !== false) {
    censusRows = censusRows.filter((r) =>
      isCalaCountry(r.fields?.[CENSUS_FIELDS.country] || r.country)
    );
  }

  const websiteField = MAP_BWH_CENSUS_BACKFILL.website;
  const propField = MAP_BWH_CENSUS_BACKFILL.propertyId;
  const minScore = opts.minScore ?? 62;
  const minNameSim = opts.minNameSim ?? 0.55;
  const applyMinScore = opts.applyMinScore ?? 68;
  const applyMinNameSim = opts.applyMinNameSim ?? 0.6;
  const confRank = { none: 0, low: 1, medium: 2, high: 3 };
  const applyConfRank = confRank[opts.minConfidence || "medium"] ?? 2;

  const claimedCodes = new Set();
  for (const r of censusRows) {
    const existing = r.fields?.[propField];
    if (!isBlankCensusValue(existing)) {
      const n = normalizeBwhPropertyCode(existing);
      if (n) claimedCodes.add(n);
    }
  }

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const stewardReview = [];
  /** @type {object[]} */
  const skipped = [];
  const usedDir = new Set();
  const usedCensus = new Set();

  /** Build scored candidates with affiliation gate */
  /** @type {object[]} */
  const candidates = [];
  for (const censusRow of censusRows) {
    const aff = String(censusRow.fields?.[CENSUS_FIELDS.affiliation] || censusRow.affiliation || "").trim();
    const websiteBlank = isBlankCensusValue(censusRow.fields?.[websiteField]);
    const pidBlank = isBlankCensusValue(censusRow.fields?.[propField]);
    if (!websiteBlank && !pidBlank) {
      skipped.push({
        censusRecordId: censusRow.recordId,
        censusName: censusRow.name,
        reason: "no_blank_fields_to_fill",
      });
      continue;
    }

    for (const dir of directoryRows) {
      if (!bwhAffiliationAllowsFamily(aff, dir.brandFamily)) continue;
      const directoryRow = {
        name: normalizeBwhHotelNameForMatch(dir.name),
        city: dir.city,
        country: dir.country,
        website: dir.propertyUrl,
        brandPropertyCode: dir.propertyCode,
      };
      const scored = scoreDirectoryAgainstCensus(directoryRow, {
        ...censusRow,
        name: normalizeBwhHotelNameForMatch(censusRow.name),
      });
      const nameSim = nameSimilarity(
        normalizeBwhHotelNameForMatch(dir.name),
        normalizeBwhHotelNameForMatch(censusRow.name)
      );
      candidates.push({
        censusRow,
        dir,
        scored: { ...scored, nameSim: Math.max(scored.nameSim || 0, nameSim) },
        nameSim: Math.max(scored.nameSim || 0, nameSim),
      });
    }
  }

  candidates.sort((a, b) => b.scored.score - a.scored.score || b.nameSim - a.nameSim);

  for (const c of candidates) {
    const cid = c.censusRow.recordId;
    const did = c.dir.propertyCode;
    if (usedCensus.has(cid) || usedDir.has(did)) continue;
    if (c.scored.score < minScore || c.nameSim < minNameSim) continue;
    if (claimedCodes.has(did) && isBlankCensusValue(c.censusRow.fields?.[propField])) {
      // Code already on another census row — skip reassignment
      continue;
    }

    usedCensus.add(cid);
    usedDir.add(did);

    const applyFields = {};
    if (isBlankCensusValue(c.censusRow.fields?.[websiteField]) && c.dir.propertyUrl) {
      applyFields[websiteField] = c.dir.propertyUrl;
    }
    if (isBlankCensusValue(c.censusRow.fields?.[propField]) && c.dir.propertyCode) {
      applyFields[propField] = c.dir.propertyCode;
    }

    const row = {
      censusRecordId: cid,
      censusName: c.censusRow.name,
      censusCountry: c.censusRow.country,
      censusCity: c.censusRow.city,
      affiliation: c.censusRow.fields?.[CENSUS_FIELDS.affiliation],
      propertyId: c.dir.propertyCode,
      propertyUrl: c.dir.propertyUrl,
      brandFamily: c.dir.brandFamily,
      directoryName: c.dir.name,
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

    const passes =
      c.scored.score >= applyMinScore &&
      c.nameSim >= applyMinNameSim &&
      (confRank[c.scored.confidence] ?? 0) >= applyConfRank;

    // Require Website on medium+ when website was blank — already in applyFields when blank
    if (
      isBlankCensusValue(c.censusRow.fields?.[websiteField]) &&
      !applyFields[websiteField]
    ) {
      stewardReview.push({ ...row, reason: "medium_match_missing_website" });
      continue;
    }

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

  const unmatchedDirectory = directoryRows.filter((d) => !usedDir.has(d.propertyCode));

  return {
    fieldMapping: MAP_BWH_CENSUS_BACKFILL,
    parentCompanyDefault: BWH_PARENT_COMPANY,
    seedPath,
    directoryRowsLoaded: directoryRows.length,
    censusRowsScanned: censusRows.length,
    readyToApply: planRows.length,
    planRows,
    stewardReview,
    skipped,
    unmatchedDirectoryCount: unmatchedDirectory.length,
    unmatchedDirectorySample: unmatchedDirectory.map((d) => ({
      propertyId: d.propertyCode,
      name: d.name,
      country: d.country,
      affiliation: d.affiliation,
      propertyUrl: d.propertyUrl,
    })),
  };
}

/**
 * @param {object} planRow
 */
export function validateBwhCensusApplyRow(planRow) {
  const errors = [];
  if (!planRow?.censusRecordId) errors.push("missing censusRecordId");
  if (!planRow?.applyFields || !Object.keys(planRow.applyFields).length) {
    errors.push("no applyFields");
  }
  const website = planRow.applyFields?.[MAP_BWH_CENSUS_BACKFILL.website];
  const propId = planRow.applyFields?.[MAP_BWH_CENSUS_BACKFILL.propertyId];
  if (website != null) {
    if (typeof website !== "string" || !/^https:\/\/www\.bestwestern\.com\//i.test(website)) {
      errors.push("Website must be https://www.bestwestern.com/… official property URL");
    }
    if (!/propertyCode\.\d{4,6}|hotel-details\.\d{4,6}/i.test(website)) {
      errors.push("Website must include propertyCode.NNNNN or hotel-details.NNNNN");
    }
  }
  if (propId != null) {
    if (!/^\d{4,6}$/.test(String(propId))) {
      errors.push("Property ID must be numeric Best Western propertyCode");
    }
  }
  if (planRow.matchConfidence === "none") errors.push("match confidence too low");
  return { pass: errors.length === 0, errors };
}
