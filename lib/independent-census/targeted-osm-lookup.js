/**
 * Phase Choice-C — Targeted Overpass lookup near legacy Choice target coordinates.
 */

import { readFileSync } from "fs";
import {
  fetchOverpassElements,
  fetchOsmElementBySourceRecordId,
  normalizeOsmElement,
  buildOverpassAroundQuery,
  OSM_HOTEL_FOCUSED_TOURISM,
} from "./sources/osm.js";
import {
  nameSimilarity,
  normalizeCountry,
  normalizeKey,
  normalizeText,
  parseCoords,
} from "./match-current-census.js";
import {
  scoreTargetAgainstOsm,
  assignTargetOsmConfidence,
  mapRetentionRowToOsmCandidate,
} from "./match-choice-targets-to-osm.js";
import { loadCandidateRetentionReport } from "./match-brand-directory-properties.js";
import {
  SOURCE_TYPES,
  CANDIDATES_TABLE,
  RECOMMENDED_ACTION,
} from "./fields.js";
import {
  candidateDuplicateKey,
  loadExistingCandidateKeys,
  createCandidateRecords,
} from "./candidate-apply.js";
import { getIndependentCensusBase, isIndependentCensusPipelineEnabled } from "./platform-base.js";
import { computeQualityScore, qualityTier } from "./normalize-candidate.js";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function loadChoiceTargetMatchReport(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const matchRows = Array.isArray(data.matchRows) ? data.matchRows : [];
  return { data, matchRows };
}

export function loadTargetedLookupReport(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const results = Array.isArray(data.results) ? data.results : [];
  return { data, results };
}

export function loadChoiceTargetListIndex(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const byId = new Map();
  for (const t of data.targets || []) {
    byId.set(t.recordId, t);
  }
  return byId;
}

function osmCandidateHasCoreFields(candidate) {
  const coords = parseCoords(candidate.rawLatitude, candidate.rawLongitude);
  return (
    !!normalizeKey(candidate.rawHotelName) &&
    !!normalizeKey(candidate.rawCountry) &&
    !!coords
  );
}

function isApplyEligibleLookupRow(row) {
  if (!row.wouldCreateCuratedCandidate) return false;
  if (row.candidateAlreadyExists) return false;
  const conf = row.matchConfidence || "none";
  if (conf !== "high" && conf !== "medium") return false;
  if (!row.bestOsmSourceRecordId) return false;
  return true;
}

function indexOsmBySourceId(rows) {
  const m = new Map();
  for (const r of rows) {
    if (normalizeKey(r.sourceType) !== SOURCE_TYPES.OSM) continue;
    if (r.sourceRecordId) m.set(r.sourceRecordId, mapRetentionRowToOsmCandidate(r));
    if (r.airtableRecordId) m.set(`rec:${r.airtableRecordId}`, mapRetentionRowToOsmCandidate(r));
  }
  return m;
}

function targetNeedsLookup(row, minConfidence) {
  const rank = { high: 3, medium: 2, low: 1, none: 0 };
  const conf = row.matchConfidence || "none";
  const minRank = rank[minConfidence] ?? 2;
  return rank[conf] < minRank;
}

function nameStronglyMatchesTarget(targetName, osmName) {
  return nameSimilarity(targetName, osmName) >= 0.75;
}

function filterNearbyElements(elements, target, includeUnnamed) {
  const kept = [];
  for (const el of elements) {
    const c = normalizeOsmElement(el, {
      batchId: "lookup",
      defaultCountry: target.country,
      importedAt: new Date().toISOString(),
    });
    if (!includeUnnamed && !normalizeKey(c.rawHotelName)) continue;
    if (!nameStronglyMatchesTarget(target.name, c.rawHotelName)) {
      if (!normalizeKey(c.rawHotelName)) continue;
    }
    kept.push({ el, candidate: c });
  }
  return kept;
}

function buildCuratedCandidateFromOsm(osmCandidate, target, matchMeta, batchId) {
  const importedAt = new Date().toISOString();
  const qualityScore = computeQualityScore(osmCandidate);
  const recommendedAction =
    matchMeta.matchConfidence === "high"
      ? RECOMMENDED_ACTION.PROMOTE
      : RECOMMENDED_ACTION.NEEDS_RESEARCH;

  const payload = {
    ...(typeof osmCandidate.rawPayload === "object"
      ? osmCandidate.rawPayload
      : {}),
    choiceTargetRecovery: {
      legacyRecordId: target.recordId || target.legacyRecordId,
      legacyHotelName: target.name || target.legacyHotelName,
      legacyCountry: target.country || target.legacyCountry,
      matchConfidence: matchMeta.matchConfidence,
      matchReason: matchMeta.matchReason,
      lookupBatchId: batchId,
      note: "OSM fields only. Legacy coordinate used for search only.",
    },
  };

  return {
    ...osmCandidate,
    sourceType: SOURCE_TYPES.OSM,
    importBatchId: batchId,
    importedAt,
    rawPayload: payload,
    qualityScore,
    qualityTier: qualityTier(qualityScore),
    recommendedAction,
    reviewStatus: "pending",
    possibleMatchConfidence: matchMeta.matchConfidence,
  };
}

/**
 * @param {object} opts
 */
export async function runTargetedOsmLookupForChoice(opts) {
  const { matchRows } = loadChoiceTargetMatchReport(opts.targetMatchReportPath);
  const minConfidence = (opts.minMatchConfidence || "medium").toLowerCase();

  const targetsToLookup = matchRows.filter((r) => targetNeedsLookup(r, minConfidence));
  const limit = opts.limitLookups != null ? Number(opts.limitLookups) : null;
  const lookupTargets = limit
    ? targetsToLookup.slice(0, limit)
    : targetsToLookup;

  let existingBySource = new Map();
  if (opts.retentionReportPath) {
    const retention = loadCandidateRetentionReport(opts.retentionReportPath);
    existingBySource = indexOsmBySourceId(retention.rows);
  }

  const radiusMeters = Number(opts.radiusMeters) || 500;
  const delayMs = Number(opts.requestDelayMs) || 400;
  const results = [];
  let overpassErrors = 0;

  for (let i = 0; i < lookupTargets.length; i++) {
    const target = lookupTargets[i];
    const coords = parseCoords(
      target.lat ?? target.legacyLatitude,
      target.lng ?? target.legacyLongitude
    );

    if (!coords) {
      results.push({
        legacyRecordId: target.legacyRecordId,
        legacyHotelName: target.legacyHotelName,
        targetCountry: target.targetCountry,
        lookupStatus: "skipped_no_coordinates",
        nearbyCount: 0,
        bestOsmSourceRecordId: "",
        matchConfidence: "none",
        matchScore: 0,
        matchReason: "Target missing coordinates for Overpass lookup",
        candidateAlreadyExists: false,
        wouldCreateCuratedCandidate: false,
      });
      continue;
    }

    if (i > 0 && delayMs > 0) await sleep(delayMs);

    let nearby = [];
    try {
      const query = buildOverpassAroundQuery({
        lat: coords.lat,
        lng: coords.lng,
        radiusMeters,
        maxElements: 40,
        tourismTags: OSM_HOTEL_FOCUSED_TOURISM,
      });
      const elements = await fetchOverpassElements(query);
      nearby = filterNearbyElements(elements, target, false);
    } catch (err) {
      overpassErrors++;
      results.push({
        legacyRecordId: target.legacyRecordId,
        legacyHotelName: target.legacyHotelName,
        targetCountry: target.targetCountry,
        lookupStatus: "overpass_error",
        nearbyCount: 0,
        bestOsmSourceRecordId: "",
        matchConfidence: "none",
        matchScore: 0,
        matchReason: err.message || String(err),
        candidateAlreadyExists: false,
        wouldCreateCuratedCandidate: false,
      });
      continue;
    }

    const targetForScore = {
      recordId: target.legacyRecordId,
      name: target.legacyHotelName,
      city: target.legacyCity,
      country: target.legacyCountry,
      lat: coords.lat,
      lng: coords.lng,
      website: target.website,
      telephone: target.telephone,
      affiliation: target.legacyAffiliation,
      targetBrand: target.targetBrand,
    };

    let best = null;
    let bestMeta = { matchConfidence: "none", matchScore: 0, matchReason: "No nearby OSM match" };

    for (const item of nearby) {
      const scored = scoreTargetAgainstOsm(targetForScore, item.candidate);
      const { confidence, reason } = assignTargetOsmConfidence({
        ...scored,
        _osmCountry: item.candidate.rawCountry,
      });
      if (!best || scored.score > best.score) {
        best = item.candidate;
        bestMeta = {
          matchConfidence: confidence,
          matchScore: scored.score,
          matchReason: reason,
          distanceMeters: scored.distanceMeters,
        };
      }
    }

    const existing =
      best?.sourceRecordId && existingBySource.has(best.sourceRecordId);
    const wouldCreate =
      bestMeta.matchConfidence === "high" || bestMeta.matchConfidence === "medium";

    results.push({
      legacyRecordId: target.legacyRecordId,
      legacyHotelName: target.legacyHotelName,
      legacyCity: target.legacyCity,
      legacyCountry: target.legacyCountry,
      targetBrand: target.targetBrand,
      targetCountry: target.targetCountry,
      lookupStatus: nearby.length ? "ok" : "no_nearby_osm",
      nearbyCount: nearby.length,
      bestOsmSourceRecordId: best?.sourceRecordId || "",
      bestOsmName: best?.rawHotelName || "",
      matchConfidence: bestMeta.matchConfidence,
      matchScore: bestMeta.matchScore,
      matchReason: bestMeta.matchReason,
      distanceMeters: bestMeta.distanceMeters,
      candidateAlreadyExists: !!existing,
      wouldCreateCuratedCandidate: wouldCreate && !existing,
      _bestCandidate: best,
      _target: targetForScore,
      _matchMeta: bestMeta,
    });
  }

  const wouldCreateRows = results.filter((r) => r.wouldCreateCuratedCandidate);
  let writtenCount = 0;
  let skippedDuplicate = 0;
  const apply = !!opts.apply;

  if (apply) {
    if (!isIndependentCensusPipelineEnabled()) {
      throw new Error("Apply requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true");
    }
    const base = getIndependentCensusBase();
    if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

    const existingKeys = await loadExistingCandidateKeys(
      base,
      opts.batchId,
      CANDIDATES_TABLE
    );

    const payloads = [];
    for (const r of wouldCreateRows) {
      if (!r._bestCandidate) continue;
      const curated = buildCuratedCandidateFromOsm(
        r._bestCandidate,
        r._target,
        r._matchMeta,
        opts.batchId
      );
      payloads.push({ candidate: curated, matchRow: null, reportRow: r });
    }

    const toWrite = [];
    for (const p of payloads) {
      const key = candidateDuplicateKey(
        p.candidate.sourceType,
        p.candidate.sourceRecordId,
        p.candidate.importBatchId
      );
      if (existingKeys.has(key)) {
        skippedDuplicate++;
        continue;
      }
      toWrite.push(p);
      existingKeys.add(key);
    }

    const mapped = toWrite.map((p) => ({
      candidate: p.candidate,
      matchRow: {
        matchConfidence: p.reportRow._matchMeta.matchConfidence,
        recommendedAction: p.candidate.recommendedAction,
      },
    }));

    const result = await createCandidateRecords(
      base,
      CANDIDATES_TABLE,
      mapped,
      existingKeys
    );
    writtenCount = result.writtenCount;
  }

  const wouldCreateCount = wouldCreateRows.length;

  return {
    batchId: opts.batchId,
    minMatchConfidence: minConfidence,
    targetsNeedingLookup: targetsToLookup.length,
    lookupsPerformed: lookupTargets.length,
    overpassErrors,
    radiusMeters,
    wouldCreateCuratedCount: wouldCreateCount,
    writtenCount,
    skippedDuplicateApply: skippedDuplicate,
    highFromLookup: results.filter((r) => r.matchConfidence === "high").length,
    mediumFromLookup: results.filter((r) => r.matchConfidence === "medium").length,
    lowFromLookup: results.filter((r) => r.matchConfidence === "low").length,
    noneFromLookup: results.filter((r) => r.matchConfidence === "none").length,
    results: results.map(({ _bestCandidate, _target, _matchMeta, ...rest }) => rest),
    apply,
    dryRun: !apply,
    airtableWrites: apply && writtenCount > 0,
    candidateTableWrites: apply && writtenCount > 0,
    tablesWritten: apply && writtenCount > 0 ? [CANDIDATES_TABLE] : [],
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    verifiedTableWrites: false,
    evidenceTableWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
  };
}

/**
 * Apply curated candidates from a completed targeted lookup report (no full re-query).
 * @param {object} opts
 */
export async function applyCuratedCandidatesFromLookupReport(opts) {
  const { results } = loadTargetedLookupReport(opts.lookupReportPath);
  const targetById = opts.targetListPath
    ? loadChoiceTargetListIndex(opts.targetListPath)
    : new Map();

  const selected = results.filter(isApplyEligibleLookupRow);
  const delayMs = Number(opts.requestDelayMs) || 350;
  const batchId = opts.batchId || "choice-targeted-osm-lookup-apply";

  const globalOsmSourceIds = new Set();
  if (opts.retentionReportPath) {
    const retention = loadCandidateRetentionReport(opts.retentionReportPath);
    for (const r of retention.rows) {
      if (normalizeKey(r.sourceType) !== SOURCE_TYPES.OSM) continue;
      if (r.sourceRecordId) globalOsmSourceIds.add(r.sourceRecordId);
    }
  }

  let fetchFailed = 0;
  let skippedExistingOsm = 0;
  let missingCoreFields = 0;
  const prepared = [];

  for (let i = 0; i < selected.length; i++) {
    const row = selected[i];
    const target = targetById.get(row.legacyRecordId);
    const targetForPayload = {
      recordId: row.legacyRecordId,
      name: row.legacyHotelName,
      city: row.legacyCity || target?.city,
      country: row.legacyCountry || target?.country,
      lat: target?.lat,
      lng: target?.lng,
      website: target?.website,
      telephone: target?.telephone,
      affiliation: target?.affiliation,
      targetBrand: row.targetBrand,
    };

    if (i > 0 && delayMs > 0) await sleep(delayMs);

    let element;
    try {
      element = await fetchOsmElementBySourceRecordId(row.bestOsmSourceRecordId);
    } catch {
      fetchFailed++;
      continue;
    }
    if (!element) {
      fetchFailed++;
      continue;
    }

    const osmCandidate = normalizeOsmElement(element, {
      batchId,
      defaultCountry: row.legacyCountry || target?.country,
      importedAt: new Date().toISOString(),
    });

    if (!osmCandidateHasCoreFields(osmCandidate)) {
      missingCoreFields++;
      continue;
    }

    if (
      globalOsmSourceIds.size &&
      globalOsmSourceIds.has(osmCandidate.sourceRecordId)
    ) {
      skippedExistingOsm++;
      continue;
    }

    const matchMeta = {
      matchConfidence: row.matchConfidence,
      matchReason: row.matchReason,
    };
    prepared.push({
      row,
      targetForPayload,
      osmCandidate,
      matchMeta,
    });
  }

  let writtenCount = 0;
  let skippedDuplicate = 0;
  const apply = !!opts.apply;

  if (apply) {
    if (!isIndependentCensusPipelineEnabled()) {
      throw new Error("Apply requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true");
    }
    const base = getIndependentCensusBase();
    if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

    const existingKeys = await loadExistingCandidateKeys(
      base,
      batchId,
      CANDIDATES_TABLE
    );

    const pendingKeys = new Set(existingKeys);
    const toWrite = [];
    for (const p of prepared) {
      const curated = buildCuratedCandidateFromOsm(
        p.osmCandidate,
        p.targetForPayload,
        p.matchMeta,
        batchId
      );
      const key = candidateDuplicateKey(
        curated.sourceType,
        curated.sourceRecordId,
        curated.importBatchId
      );
      if (pendingKeys.has(key)) {
        skippedDuplicate++;
        continue;
      }
      toWrite.push({ candidate: curated, reportRow: p.row });
      pendingKeys.add(key);
    }

    const mapped = toWrite.map((p) => ({
      candidate: p.candidate,
      matchRow: {
        matchConfidence: p.reportRow.matchConfidence,
        recommendedAction: p.candidate.recommendedAction,
      },
    }));

    const result = await createCandidateRecords(
      base,
      CANDIDATES_TABLE,
      mapped,
      existingKeys
    );
    writtenCount = result.writtenCount;
    skippedDuplicate += result.skippedDuplicate?.length || 0;
  }

  const byCountry = {};
  const byBrand = {};
  for (const p of prepared) {
    const co = normalizeCountry(p.row.targetCountry || p.row.legacyCountry) || "(unknown)";
    byCountry[co] = (byCountry[co] || 0) + 1;
    const br = p.row.targetBrand || "(unknown)";
    byBrand[br] = (byBrand[br] || 0) + 1;
  }

  return {
    batchId,
    lookupReportPath: opts.lookupReportPath,
    curatedSelected: selected.length,
    preparedForWrite: prepared.length,
    writtenCount,
    skippedDuplicateApply: skippedDuplicate,
    skippedExistingOsmSourceId: skippedExistingOsm,
    fetchFailed,
    missingCoreFields,
    byCountry,
    byBrand,
    apply,
    dryRun: !apply,
    airtableWrites: apply && writtenCount > 0,
    candidateTableWrites: apply && writtenCount > 0,
    tablesWritten: apply && writtenCount > 0 ? [CANDIDATES_TABLE] : [],
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    verifiedTableWrites: false,
    evidenceTableWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
  };
}

export const TARGETED_LOOKUP_CSV_COLUMNS = [
  "legacyRecordId",
  "legacyHotelName",
  "legacyCountry",
  "targetBrand",
  "lookupStatus",
  "nearbyCount",
  "bestOsmSourceRecordId",
  "bestOsmName",
  "matchConfidence",
  "matchScore",
  "distanceMeters",
  "candidateAlreadyExists",
  "wouldCreateCuratedCandidate",
  "matchReason",
];
