/**
 * Phase Choice-B — Match Choice legacy targets to existing OSM staging candidates.
 */

import { readFileSync } from "fs";
import {
  nameSimilarity,
  normalizeCountry,
  normalizeKey,
  normalizeText,
  parseCoords,
  countriesMatch,
  citiesMatch,
  websiteHost,
  distanceMeters,
} from "./match-current-census.js";
import { loadCandidateRetentionReport } from "./match-brand-directory-properties.js";
import { parseRetentionIncludeExclude } from "./backwards-census-match.js";
import { SOURCE_TYPES } from "./fields.js";
import { loadVerifiedIndexWithPolicy } from "./verified-dedupe-index.js";
import { getIndependentCensusBase } from "./platform-base.js";
import { VERIFIED_TABLE } from "./fields.js";

const STRONG_NAME = 0.85;
const GOOD_NAME = 0.7;
const PARTIAL_NAME = 0.5;

function normalizePhone(raw) {
  return normalizeKey(String(raw || "").replace(/\D/g, ""));
}

function brandOverlap(targetBrand, osmBrand, targetAff, osmAff) {
  const tb = normalizeKey(targetBrand || targetAff);
  const ob = normalizeKey(osmBrand);
  if (!tb || !ob) return false;
  return tb === ob || tb.includes(ob) || ob.includes(tb);
}

export function loadChoiceTargetList(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const targets = Array.isArray(data.targets) ? data.targets : [];
  return { data, targets };
}

export function mapRetentionRowToOsmCandidate(r) {
  return {
    airtableRecordId: r.airtableRecordId,
    sourceType: r.sourceType,
    sourceRecordId: r.sourceRecordId,
    rawHotelName: r.rawHotelName,
    rawCity: r.rawCity || "",
    rawCountry: r.rawCountry,
    rawLatitude: r.rawLatitude,
    rawLongitude: r.rawLongitude,
    rawWebsite: r.rawWebsite || "",
    rawPhone: r.rawPhone || "",
    rawBrand: r.rawBrand || "",
    retentionRecommendation: r.retentionRecommendation || "",
    inDuplicateCluster: !!r.inDuplicateCluster,
    parentCompany: r.parentCompany || "",
  };
}

/**
 * Score legacy target vs one OSM candidate (target = benchmark, OSM = independent source).
 */
export function scoreTargetAgainstOsm(target, osm) {
  const nameSim = nameSimilarity(target.name, osm.rawHotelName);
  const countryOk = countriesMatch(target.country, osm.rawCountry);
  const cityResult = citiesMatch(target.city, osm.rawCity);

  const tCoords = parseCoords(target.lat, target.lng);
  const oCoords = parseCoords(osm.rawLatitude, osm.rawLongitude);
  const distM = tCoords && oCoords ? distanceMeters(tCoords, oCoords) : null;

  const webT = websiteHost(target.website);
  const webO = websiteHost(osm.rawWebsite);
  const websiteMatch = webT && webO && webT === webO;

  const phoneT = normalizePhone(target.telephone);
  const phoneO = normalizePhone(osm.rawPhone);
  const phoneMatch = phoneT && phoneO && phoneT === phoneO;

  const brandMatch = brandOverlap(
    target.targetBrand,
    osm.rawBrand,
    target.affiliation,
    osm.rawBrand
  );

  let score = Math.round(nameSim * 55);
  if (countryOk && normalizeCountry(target.country)) score += 10;
  if (cityResult === true) score += 10;
  if (distM != null) {
    if (distM <= 250) score += 25;
    else if (distM <= 750) score += 15;
    else if (distM <= 2000) score += 5;
  }
  if (websiteMatch) score += 20;
  if (phoneMatch) score += 15;
  if (brandMatch) score += 8;
  score = Math.min(100, score);

  return {
    nameSim,
    countryOk,
    cityMatch: cityResult,
    distanceMeters: distM != null ? Math.round(distM) : null,
    websiteMatch,
    phoneMatch,
    brandMatch,
    score,
  };
}

export function assignTargetOsmConfidence(scored) {
  const { nameSim, countryOk, distanceMeters, websiteMatch, phoneMatch } = scored;

  if (!countryOk && normalizeCountry(scored._osmCountry)) {
    return { confidence: "none", reason: "Country does not match OSM candidate" };
  }

  if ((websiteMatch || phoneMatch) && countryOk) {
    return {
      confidence: "high",
      reason: websiteMatch
        ? "Exact website host match, same country"
        : "Exact phone match, same country",
    };
  }

  if (
    nameSim >= STRONG_NAME &&
    countryOk &&
    distanceMeters != null &&
    distanceMeters <= 250
  ) {
    return {
      confidence: "high",
      reason: `Strong name (${(nameSim * 100).toFixed(0)}%), same country, ${distanceMeters}m`,
    };
  }

  if (
    nameSim >= GOOD_NAME &&
    countryOk &&
    distanceMeters != null &&
    distanceMeters <= 750
  ) {
    return {
      confidence: "medium",
      reason: `Good name (${(nameSim * 100).toFixed(0)}%), same country, ${distanceMeters}m`,
    };
  }

  if (
    (nameSim >= PARTIAL_NAME || scored.brandMatch) &&
    countryOk &&
    distanceMeters != null &&
    distanceMeters <= 2000
  ) {
    return {
      confidence: "low",
      reason: `Partial name/brand match within ${distanceMeters}m, same country`,
    };
  }

  if (distanceMeters == null && countryOk) {
    const cityOk = scored.cityMatch === true;
    if (nameSim >= STRONG_NAME && cityOk) {
      return {
        confidence: "high",
        reason: `Strong name (${(nameSim * 100).toFixed(0)}%), same country and city (no legacy coordinates)`,
      };
    }
    if (nameSim >= GOOD_NAME && cityOk) {
      return {
        confidence: "medium",
        reason: `Good name (${(nameSim * 100).toFixed(0)}%), same country and city (no legacy coordinates)`,
      };
    }
    if (nameSim >= GOOD_NAME) {
      return {
        confidence: "low",
        reason: `Good name (${(nameSim * 100).toFixed(0)}%), same country (no legacy coordinates; city not aligned)`,
      };
    }
  }

  return { confidence: "none", reason: "No credible OSM match for Choice target" };
}

export function loadOsmCandidatesForChoiceMatch(opts) {
  const retention = loadCandidateRetentionReport(opts.retentionReportPath);
  const { includeSet, excludeSet } = parseRetentionIncludeExclude({
    includeRetention:
      opts.includeRetention ||
      "keep_high_priority,enrich_next,keep_for_matching,low_priority_hold",
    excludeRetention: opts.excludeRetention || "",
  });

  const requireCoordsForLowHold = opts.requireCoordsForLowHold !== false;
  const candidates = [];

  for (const r of retention.rows) {
    if (normalizeKey(r.sourceType) !== SOURCE_TYPES.OSM) continue;
    const rec = normalizeKey(r.retentionRecommendation);
    if (!includeSet.has(rec)) continue;
    if (excludeSet.has(rec)) continue;
    if (rec === "duplicate_review" && !opts.includeDuplicateReview) continue;

    if (rec === "low_priority_hold" && requireCoordsForLowHold) {
      const coords = parseCoords(r.rawLatitude, r.rawLongitude);
      const hasName = !!normalizeKey(r.rawHotelName);
      if (!coords || !hasName) continue;
    }

    candidates.push(mapRetentionRowToOsmCandidate(r));
  }

  const byCountry = new Map();
  for (const c of candidates) {
    const co = normalizeCountry(c.rawCountry) || "(unknown)";
    if (!byCountry.has(co)) byCountry.set(co, []);
    byCountry.get(co).push(c);
  }

  return {
    candidates,
    byCountry,
    retentionRowsTotal: retention.rows.length,
    osmCandidatesLoaded: candidates.length,
  };
}

export function matchChoiceTargetToOsm(target, osmPool) {
  const countryNorm = normalizeCountry(target.country);
  let pool = osmPool;
  if (countryNorm) {
    pool = osmPool.filter(
      (o) => normalizeCountry(o.rawCountry) === countryNorm
    );
    if (!pool.length) pool = osmPool;
  }

  const scoredList = pool
    .map((osm) => {
      const s = scoreTargetAgainstOsm(target, osm);
      return { osm, ...s, _osmCountry: osm.rawCountry };
    })
    .filter((s) => s.nameSim >= 0.35 || s.websiteMatch || s.phoneMatch || s.brandMatch)
    .sort((a, b) => b.score - a.score);

  if (!scoredList.length) {
    return {
      matchConfidence: "none",
      matchScore: 0,
      matchReason: "No OSM candidate with sufficient similarity",
      osmCandidateRecordId: "",
      osmName: "",
      osmSourceRecordId: "",
      distanceMeters: null,
    };
  }

  const best = scoredList[0];
  const { confidence, reason } = assignTargetOsmConfidence(best);

  return {
    matchConfidence: confidence,
    matchScore: best.score,
    matchReason: reason,
    osmCandidateRecordId:
      confidence === "none" ? "" : best.osm.airtableRecordId,
    osmName: confidence === "none" ? "" : best.osm.rawHotelName,
    osmCity: confidence === "none" ? "" : best.osm.rawCity,
    osmCountry: confidence === "none" ? "" : best.osm.rawCountry,
    osmSourceRecordId: confidence === "none" ? "" : best.osm.sourceRecordId,
    osmWebsite: confidence === "none" ? "" : best.osm.rawWebsite,
    distanceMeters: best.distanceMeters,
    websiteMatch: best.websiteMatch,
    phoneMatch: best.phoneMatch,
    brandMatch: best.brandMatch,
    nameSimilarity: best.nameSim,
    retentionRecommendation: best.osm?.retentionRecommendation || "",
    inDuplicateCluster: !!best.osm?.inDuplicateCluster,
    alternateOsmCount: Math.max(0, scoredList.length - 1),
  };
}

/**
 * @param {object} opts
 */
export async function runChoiceTargetOsmMatch(opts) {
  const { targets } = loadChoiceTargetList(opts.targetListPath);
  const osmLoad = loadOsmCandidatesForChoiceMatch({
    retentionReportPath: opts.retentionReportPath,
    includeRetention: opts.includeRetention,
    excludeRetention: opts.excludeRetention,
    includeDuplicateReview: !!opts.includeDuplicateReview,
  });

  let verifiedIndex = {
    candidateLinks: new Set(),
    dedupeKeys: new Set(),
  };
  let verifiedIndexMeta = null;
  const base = getIndependentCensusBase();
  if (base) {
    const loaded = await loadVerifiedIndexWithPolicy(base, {
      apply: false,
      allowMissingVerifiedIndex: false,
    });
    verifiedIndex = loaded.index;
    verifiedIndexMeta = loaded.meta;
  }

  const matchRows = [];
  const confidenceCounts = { high: 0, medium: 0, low: 0, none: 0 };

  for (const target of targets) {
    const m = matchChoiceTargetToOsm(target, osmLoad.candidates);
    confidenceCounts[m.matchConfidence] =
      (confidenceCounts[m.matchConfidence] || 0) + 1;

    const alreadyVerified = m.osmCandidateRecordId
      ? verifiedIndex.candidateLinks.has(m.osmCandidateRecordId)
      : false;

    matchRows.push({
      legacyRecordId: target.recordId,
      legacyHotelName: target.name,
      legacyCity: target.city,
      legacyCountry: target.country,
      legacyLatitude: target.lat,
      legacyLongitude: target.lng,
      targetBrand: target.targetBrand,
      targetCountry: target.targetCountry,
      targetMatchKey: target.targetMatchKey,
      ...m,
      alreadyVerified,
      duplicateRisk: !!m.inDuplicateCluster,
    });
  }

  const noOsmMatch = matchRows.filter((r) => r.matchConfidence === "none").length;
  const gapByCountry = {};
  for (const r of matchRows) {
    if (r.matchConfidence !== "none") continue;
    const co = r.targetCountry || "(unknown)";
    gapByCountry[co] = (gapByCountry[co] || 0) + 1;
  }

  return {
    batchId: opts.batchId,
    choiceTargetCount: targets.length,
    osmCandidatesLoaded: osmLoad.osmCandidatesLoaded,
    confidenceCounts,
    noOsmMatchCount: noOsmMatch,
    alreadyVerifiedCount: matchRows.filter((r) => r.alreadyVerified).length,
    duplicateRiskCount: matchRows.filter((r) => r.duplicateRisk).length,
    gapByCountry,
    matchRows,
    verifiedIndexMeta,
    dryRun: true,
    candidateTableReads: false,
    candidateSource: "retention_report_json",
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
  };
}

export function choiceOsmMatchRowToCsv(row) {
  return {
    legacyRecordId: row.legacyRecordId,
    legacyHotelName: row.legacyHotelName,
    legacyCity: row.legacyCity,
    legacyCountry: row.legacyCountry,
    targetBrand: row.targetBrand,
    matchConfidence: row.matchConfidence,
    matchScore: row.matchScore,
    osmCandidateRecordId: row.osmCandidateRecordId,
    osmName: row.osmName,
    osmSourceRecordId: row.osmSourceRecordId,
    distanceMeters: row.distanceMeters ?? "",
    alreadyVerified: row.alreadyVerified ? "yes" : "no",
    duplicateRisk: row.duplicateRisk ? "yes" : "no",
    matchReason: row.matchReason,
  };
}

export const CHOICE_OSM_MATCH_CSV_COLUMNS = [
  "legacyRecordId",
  "legacyHotelName",
  "legacyCity",
  "legacyCountry",
  "targetBrand",
  "matchConfidence",
  "matchScore",
  "osmCandidateRecordId",
  "osmName",
  "osmSourceRecordId",
  "distanceMeters",
  "alreadyVerified",
  "duplicateRisk",
  "matchReason",
];
