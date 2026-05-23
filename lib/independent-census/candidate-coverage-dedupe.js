/**
 * Phase 4O — Read-only Independent Hotel Source Candidates coverage, dedupe, retention.
 */

import {
  CANDIDATES_TABLE,
  CANDIDATE_FIELDS,
  SOURCE_TYPES,
  VERIFIED_TABLE,
  VERIFIED_FIELDS,
} from "./fields.js";
import { getIndependentCensusBase } from "./platform-base.js";
import { mapCandidateRecord } from "./promotion-review.js";
import {
  computeQualityScore,
  qualityTier,
  analyzeMissingFields,
  normalizeKey,
  normalizeText,
} from "./normalize-candidate.js";
import {
  normalizeCountry,
  websiteHost,
  nameSimilarity,
  countriesMatch,
  citiesMatch,
} from "./match-current-census.js";

export const RETENTION = {
  KEEP_HIGH: "keep_high_priority",
  KEEP_MATCHING: "keep_for_matching",
  ENRICH_NEXT: "enrich_next",
  DUPLICATE_REVIEW: "duplicate_review",
  LOW_HOLD: "low_priority_hold",
  ARCHIVE_LATER: "possible_archive_later",
};

const EXCESSIVE_COUNTRY_THRESHOLD = 5000;
const CHOICE_HOST = "choicehotels.com";

function roundCoord(n, decimals = 4) {
  const x = Number(n);
  if (!Number.isFinite(x)) return "";
  const f = 10 ** decimals;
  return String(Math.round(x * f) / f);
}

function normNameKey(name) {
  return normalizeKey(name);
}

function clusterKeyNameCountryCoords(c) {
  const n = normNameKey(c.rawHotelName);
  const co = normalizeCountry(c.rawCountry);
  const lat = roundCoord(c.rawLatitude);
  const lng = roundCoord(c.rawLongitude);
  if (!n || !co || !lat || !lng) return null;
  return `ncc|${n}|${co}|${lat}|${lng}`;
}

function clusterKeyNameCountryCity(c) {
  const n = normNameKey(c.rawHotelName);
  const co = normalizeCountry(c.rawCountry);
  const city = normNameKey(c.rawCity);
  if (!n || !co || !city) return null;
  return `nccity|${n}|${co}|${city}`;
}

function clusterKeyWebsiteHost(c) {
  const h = websiteHost(c.rawWebsite);
  if (!h || h === CHOICE_HOST) return null;
  return `web|${h}`;
}

function clusterKeySourceRecord(c) {
  const st = normalizeKey(c.sourceType);
  const sr = normalizeKey(c.sourceRecordId);
  if (!st || !sr) return null;
  return `sr|${st}|${sr}`;
}

function incrementMap(map, key, delta = 1) {
  if (!key) return;
  map[key] = (map[key] || 0) + delta;
}

function parentCompanyFromPayload(c) {
  const p = c.payload || {};
  return normalizeText(p.parentCompany || p.matchedBrandSetupBrand || "");
}

function brandFromCandidate(c) {
  return (
    normalizeText(c.rawBrand) ||
    normalizeText(c.payload?.matchedBrandSetupBrand) ||
    normalizeText(c.payload?.inferredBrandName) ||
    ""
  );
}

/**
 * @param {Array<object>} brandDirectoryRows
 * @returns {Set<string>} airtableRecordIds of OSM candidates flagged enrich_next
 */
export function findOsmOverlappingBrandDirectory(brandDirectoryRows, osmRows) {
  const enrichOsmIds = new Set();
  const osmByCountry = new Map();

  for (const o of osmRows) {
    const co = normalizeCountry(o.rawCountry);
    if (!co) continue;
    if (!osmByCountry.has(co)) osmByCountry.set(co, []);
    osmByCountry.get(co).push(o);
  }

  for (const bd of brandDirectoryRows) {
    const co = normalizeCountry(bd.rawCountry);
    const pool = osmByCountry.get(co) || [];
    const bdName = bd.rawHotelName;
    const bdCity = bd.rawCity;
    const bdBrand = brandFromCandidate(bd);

    for (const o of pool) {
      const ns = nameSimilarity(bdName, o.rawHotelName);
      const cityOk = citiesMatch(bdCity, o.rawCity);
      const brandHay = normalizeKey(`${o.rawHotelName} ${o.rawBrand}`);
      const brandHit =
        bdBrand && normalizeKey(bdBrand).length > 2 && brandHay.includes(normalizeKey(bdBrand));

      if (ns >= 0.72 || (ns >= 0.5 && cityOk) || (cityOk && brandHit)) {
        enrichOsmIds.add(o.airtableRecordId);
      }
    }
  }

  return enrichOsmIds;
}

/**
 * @param {object} c
 * @param {object} ctx
 */
export function assignRetentionRecommendation(c, ctx) {
  const {
    duplicateClusterIds = new Set(),
    enrichOsmIds = new Set(),
  } = ctx;

  const tier = c.qualityTier;
  const missing = c.missingFields || [];
  const hasCoords =
    Number.isFinite(c.rawLatitude) && Number.isFinite(c.rawLongitude);
  const hasWebsite = !missing.includes("missingWebsite");
  const hasCity = !missing.includes("missingCity");
  const hasBrand = !missing.includes("missingBrand");
  const hasPhone = !missing.includes("missingPhone");
  const hasName = !missing.includes("missingName");
  const st = normalizeKey(c.sourceType);

  if (duplicateClusterIds.has(c.airtableRecordId)) {
    return RETENTION.DUPLICATE_REVIEW;
  }

  if (st === SOURCE_TYPES.BRAND_DIRECTORY) {
    const parent = parentCompanyFromPayload(c);
    const brand = brandFromCandidate(c);
    if (parent || brand || normalizeKey(c.rawWebsite).includes(CHOICE_HOST)) {
      return RETENTION.KEEP_HIGH;
    }
    return RETENTION.KEEP_HIGH;
  }

  if (enrichOsmIds.has(c.airtableRecordId)) {
    return RETENTION.ENRICH_NEXT;
  }

  if (st === SOURCE_TYPES.OSM) {
    if (tier === "high" && hasWebsite) return RETENTION.KEEP_HIGH;
    if (
      hasName &&
      hasCoords &&
      !hasWebsite &&
      !hasPhone &&
      !hasBrand &&
      !hasCity
    ) {
      return RETENTION.LOW_HOLD;
    }
    if (tier === "minimal" && !hasCoords) return RETENTION.ARCHIVE_LATER;
    if (tier === "minimal") return RETENTION.LOW_HOLD;
    if (!hasCoords && !hasWebsite && !hasCity) return RETENTION.ARCHIVE_LATER;
    if ((tier === "high" || tier === "medium") && hasCoords) {
      return RETENTION.KEEP_MATCHING;
    }
    if (hasCoords && hasName) return RETENTION.KEEP_MATCHING;
    return RETENTION.LOW_HOLD;
  }

  if (st === SOURCE_TYPES.WIKIDATA) {
    if (tier === "high" || tier === "medium") return RETENTION.KEEP_MATCHING;
    return RETENTION.LOW_HOLD;
  }

  if (tier === "minimal") return RETENTION.ARCHIVE_LATER;
  return RETENTION.KEEP_MATCHING;
}

function buildDuplicateClusters(rows) {
  const clusterMaps = {
    nameCountryCoords: new Map(),
    nameCountryCity: new Map(),
    websiteHost: new Map(),
    sourceRecord: new Map(),
  };

  const keyFns = [
    ["nameCountryCoords", clusterKeyNameCountryCoords],
    ["nameCountryCity", clusterKeyNameCountryCity],
    ["websiteHost", clusterKeyWebsiteHost],
    ["sourceRecord", clusterKeySourceRecord],
  ];

  for (const row of rows) {
    for (const [kind, fn] of keyFns) {
      const k = fn(row);
      if (!k) continue;
      if (!clusterMaps[kind].has(k)) clusterMaps[kind].set(k, []);
      clusterMaps[kind].get(k).push(row.airtableRecordId);
    }
  }

  const duplicateRecordIds = new Set();
  const clusters = [];

  for (const [kind, map] of Object.entries(clusterMaps)) {
    for (const [clusterKey, ids] of map.entries()) {
      if (ids.length < 2) continue;
      clusters.push({ kind, clusterKey, size: ids.length, recordIds: ids });
      for (const id of ids) duplicateRecordIds.add(id);
    }
  }

  return { clusters, duplicateRecordIds, clusterCount: clusters.length };
}

export async function loadAllCandidatesForCoverage() {
  const base = getIndependentCensusBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const fields = [
    CANDIDATE_FIELDS.sourceName,
    CANDIDATE_FIELDS.sourceType,
    CANDIDATE_FIELDS.sourceRecordId,
    CANDIDATE_FIELDS.sourceUrl,
    CANDIDATE_FIELDS.rawHotelName,
    CANDIDATE_FIELDS.rawAddress,
    CANDIDATE_FIELDS.rawCity,
    CANDIDATE_FIELDS.rawCountry,
    CANDIDATE_FIELDS.rawLatitude,
    CANDIDATE_FIELDS.rawLongitude,
    CANDIDATE_FIELDS.rawWebsite,
    CANDIDATE_FIELDS.rawPhone,
    CANDIDATE_FIELDS.rawBrand,
    CANDIDATE_FIELDS.rawPayloadJson,
    CANDIDATE_FIELDS.importBatchId,
    CANDIDATE_FIELDS.reviewStatus,
    CANDIDATE_FIELDS.possibleMatchConfidence,
    CANDIDATE_FIELDS.recommendedAction,
    CANDIDATE_FIELDS.candidateDedupeKey,
  ];

  const records = await base(CANDIDATES_TABLE).select({ fields, pageSize: 100 }).all();
  const rows = records.map((rec) => {
    const c = mapCandidateRecord(rec);
    const score = computeQualityScore(c);
    const { flags, list } = analyzeMissingFields(c);
    if (!list.includes("missingBrand") && !normalizeKey(c.rawBrand)) {
      list.push("missingBrand");
      flags.missingBrand = true;
    }
    return {
      ...c,
      qualityScore: score,
      qualityTier: qualityTier(score),
      missingFieldFlags: flags,
      missingFields: list,
      websiteHost: websiteHost(c.rawWebsite),
      countryNorm: normalizeCountry(c.rawCountry),
      parentCompany: parentCompanyFromPayload(c),
      brandLabel: brandFromCandidate(c),
    };
  });

  return { totalLoaded: records.length, rows };
}

export async function loadVerifiedByCountry() {
  const base = getIndependentCensusBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const fields = [
    VERIFIED_FIELDS.verifiedCountry,
    VERIFIED_FIELDS.verifiedHotelName,
  ];

  const records = await base(VERIFIED_TABLE).select({ fields, pageSize: 100 }).all();
  const byCountry = {};
  for (const rec of records) {
    const co =
      normalizeCountry(rec.fields[VERIFIED_FIELDS.verifiedCountry]) || "(unknown)";
    byCountry[co] = (byCountry[co] || 0) + 1;
  }
  return { total: records.length, byCountry };
}

/**
 * @param {Array<object>} rows
 */
export function analyzeCandidateCoverage(rows) {
  const bySourceType = {};
  const byCountry = {};
  const byBatch = {};
  const byQualityTier = {};
  const byRecommendedAction = {};
  const missingTotals = {
    missingName: 0,
    missingCountry: 0,
    missingCity: 0,
    missingCoordinates: 0,
    missingWebsite: 0,
    missingPhone: 0,
    missingBrand: 0,
  };
  const brandDirectoryByParent = {};
  const brandDirectoryByBrand = {};

  const brandDirectoryRows = rows.filter(
    (r) => normalizeKey(r.sourceType) === SOURCE_TYPES.BRAND_DIRECTORY
  );
  const osmRows = rows.filter((r) => normalizeKey(r.sourceType) === SOURCE_TYPES.OSM);

  for (const c of rows) {
    const st = normalizeKey(c.sourceType) || "(unknown)";
    incrementMap(bySourceType, st);
    const co = c.countryNorm || "(unknown)";
    incrementMap(byCountry, co);
    incrementMap(byBatch, c.importBatchId || "(none)");
    incrementMap(byQualityTier, c.qualityTier || "unknown");
    incrementMap(byRecommendedAction, c.recommendedAction || "(none)");

    for (const m of Object.keys(missingTotals)) {
      if (c.missingFieldFlags?.[m]) missingTotals[m]++;
    }

    if (st === SOURCE_TYPES.BRAND_DIRECTORY) {
      const parent = c.parentCompany || "(unknown parent)";
      const brand = c.brandLabel || "(unknown brand)";
      incrementMap(brandDirectoryByParent, parent);
      incrementMap(brandDirectoryByBrand, brand);
    }
  }

  const { clusters, duplicateRecordIds, clusterCount } = buildDuplicateClusters(rows);
  const enrichOsmIds = findOsmOverlappingBrandDirectory(brandDirectoryRows, osmRows);

  const analyzedRows = rows.map((c) => {
    const retentionRecommendation = assignRetentionRecommendation(c, {
      duplicateClusterIds: duplicateRecordIds,
      enrichOsmIds,
    });
    return {
      airtableRecordId: c.airtableRecordId,
      sourceType: c.sourceType,
      sourceRecordId: c.sourceRecordId,
      importBatchId: c.importBatchId,
      rawHotelName: c.rawHotelName,
      rawCity: c.rawCity,
      rawCountry: c.rawCountry,
      rawLatitude: c.rawLatitude,
      rawLongitude: c.rawLongitude,
      rawWebsite: c.rawWebsite,
      rawBrand: c.brandLabel,
      parentCompany: c.parentCompany,
      qualityTier: c.qualityTier,
      qualityScore: c.qualityScore,
      recommendedAction: c.recommendedAction,
      reviewStatus: c.reviewStatus,
      missingFields: c.missingFields.join("; "),
      websiteHost: c.websiteHost,
      retentionRecommendation,
      inDuplicateCluster: duplicateRecordIds.has(c.airtableRecordId),
      likelyOsmEnrichForBrandDirectory: enrichOsmIds.has(c.airtableRecordId),
    };
  });

  const byRetention = {};
  for (const r of analyzedRows) {
    incrementMap(byRetention, r.retentionRecommendation);
  }

  const excessiveCountries = Object.entries(byCountry)
    .filter(([, n]) => n >= EXCESSIVE_COUNTRY_THRESHOLD)
    .map(([country, count]) => ({ country, count }))
    .sort((a, b) => b.count - a.count);

  const usefulForValidation =
    byRetention[RETENTION.KEEP_HIGH] +
    byRetention[RETENTION.KEEP_MATCHING] +
    byRetention[RETENTION.ENRICH_NEXT];

  return {
    totalCandidates: rows.length,
    usefulForValidationEstimate: usefulForValidation,
    bySourceType,
    byCountry,
    byBatch,
    byQualityTier,
    byRecommendedAction,
    missingTotals,
    duplicateRiskClusterCount: clusterCount,
    duplicateRiskRecordCount: duplicateRecordIds.size,
    duplicateClustersSample: clusters.slice(0, 50),
    brandDirectoryByParent,
    brandDirectoryByBrand,
    brandDirectoryCount: brandDirectoryRows.length,
    osmCount: osmRows.length,
    osmOverlappingBrandDirectoryCount: enrichOsmIds.size,
    byRetention,
    excessiveCountries,
    rows: analyzedRows,
  };
}

export function mergeVerifiedCoverageGap(candidateByCountry, verifiedByCountry) {
  const gaps = [];
  for (const [country, candCount] of Object.entries(candidateByCountry)) {
    const v = verifiedByCountry[country] || 0;
    if (candCount >= 100 && v === 0) {
      gaps.push({ country, candidateCount: candCount, verifiedCount: v });
    }
  }
  gaps.sort((a, b) => b.candidateCount - a.candidateCount);
  return gaps;
}

export const COVERAGE_CSV_COLUMNS = [
  "airtableRecordId",
  "sourceType",
  "sourceRecordId",
  "importBatchId",
  "rawHotelName",
  "rawCity",
  "rawCountry",
  "qualityTier",
  "qualityScore",
  "recommendedAction",
  "parentCompany",
  "rawBrand",
  "missingFields",
  "retentionRecommendation",
  "inDuplicateCluster",
  "likelyOsmEnrichForBrandDirectory",
];

export function coverageRowToCsv(r) {
  return {
    airtableRecordId: r.airtableRecordId,
    sourceType: r.sourceType,
    sourceRecordId: r.sourceRecordId,
    importBatchId: r.importBatchId,
    rawHotelName: r.rawHotelName,
    rawCity: r.rawCity,
    rawCountry: r.rawCountry,
    qualityTier: r.qualityTier,
    qualityScore: r.qualityScore,
    recommendedAction: r.recommendedAction,
    parentCompany: r.parentCompany,
    rawBrand: r.rawBrand,
    missingFields: r.missingFields,
    retentionRecommendation: r.retentionRecommendation,
    inDuplicateCluster: r.inDuplicateCluster ? "yes" : "no",
    likelyOsmEnrichForBrandDirectory: r.likelyOsmEnrichForBrandDirectory ? "yes" : "no",
  };
}
