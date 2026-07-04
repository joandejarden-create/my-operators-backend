/**
 * Phase 4E — Read-only coverage benchmark: legacy Hotel Census vs independent pipeline.
 *
 * No writes. No STR/CoStar fields. Reuses safe match logic from match-current-census.js.
 */

import {
  CANDIDATES_TABLE,
  EVIDENCE_TABLE,
  VERIFIED_TABLE,
  CANDIDATE_FIELDS,
  EVIDENCE_FIELDS,
  VERIFIED_FIELDS,
} from "./fields.js";
import { getIndependentCensusBase } from "./platform-base.js";
import { mapCandidateRecord } from "./promotion-review.js";
import {
  loadHotelCensusReadOnly,
  matchCandidateToCensus,
  normalizeCountry,
  normalizeKey,
  normalizeText,
  parseCoords,
  nameSimilarity,
  distanceMeters,
  MATCH_RECOMMENDED_ACTIONS,
} from "./match-current-census.js";

const NEARBY_DUPLICATE_METERS = 150;
const STRONG_MATCH_CONFIDENCE = new Set(["high", "medium"]);

export function mapVerifiedRecord(record) {
  const f = record.fields || {};
  const lat = f[VERIFIED_FIELDS.verifiedLatitude];
  const lng = f[VERIFIED_FIELDS.verifiedLongitude];
  const link = f[VERIFIED_FIELDS.primarySourceCandidate];
  return {
    airtableRecordId: record.id,
    verifiedHotelName: normalizeText(f[VERIFIED_FIELDS.verifiedHotelName]),
    verifiedCity: normalizeText(f[VERIFIED_FIELDS.verifiedCity]),
    verifiedCountry: normalizeText(f[VERIFIED_FIELDS.verifiedCountry]),
    verifiedLatitude: Number.isFinite(lat) ? lat : null,
    verifiedLongitude: Number.isFinite(lng) ? lng : null,
    verifiedWebsite: normalizeText(f[VERIFIED_FIELDS.verifiedWebsite]),
    verifiedPhone: normalizeText(f[VERIFIED_FIELDS.verifiedPhone]),
    verifiedBrandLabel: normalizeText(f[VERIFIED_FIELDS.verifiedBrandLabel]),
    primarySourceCandidateId: Array.isArray(link) ? link[0] : link || "",
    countryNorm: normalizeCountry(f[VERIFIED_FIELDS.verifiedCountry]),
  };
}

function verifiedAsMatchShape(v) {
  return {
    rawHotelName: v.verifiedHotelName,
    rawCity: v.verifiedCity,
    rawCountry: v.verifiedCountry,
    rawLatitude: v.verifiedLatitude,
    rawLongitude: v.verifiedLongitude,
    rawWebsite: v.verifiedWebsite,
    rawPhone: v.verifiedPhone,
  };
}

function candidateAsMatchShape(c) {
  return {
    rawHotelName: c.rawHotelName,
    rawCity: c.rawCity,
    rawCountry: c.rawCountry,
    rawLatitude: c.rawLatitude,
    rawLongitude: c.rawLongitude,
    rawWebsite: c.rawWebsite,
    rawPhone: c.rawPhone,
  };
}

/**
 * @param {{ countryFilter?: string, importBatchId?: string }} [opts]
 */
export async function loadAllCandidates(opts = {}) {
  const base = getIndependentCensusBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const fields = [
    CANDIDATE_FIELDS.sourceName,
    CANDIDATE_FIELDS.sourceType,
    CANDIDATE_FIELDS.sourceRecordId,
    CANDIDATE_FIELDS.rawHotelName,
    CANDIDATE_FIELDS.rawCity,
    CANDIDATE_FIELDS.rawCountry,
    CANDIDATE_FIELDS.rawLatitude,
    CANDIDATE_FIELDS.rawLongitude,
    CANDIDATE_FIELDS.rawWebsite,
    CANDIDATE_FIELDS.rawPhone,
    CANDIDATE_FIELDS.rawBrand,
    CANDIDATE_FIELDS.importBatchId,
    CANDIDATE_FIELDS.reviewStatus,
    CANDIDATE_FIELDS.candidateDedupeKey,
  ];

  const records = await base(CANDIDATES_TABLE).select({ fields, pageSize: 100 }).all();
  let rows = records.map(mapCandidateRecord);

  if (opts.importBatchId) {
    rows = rows.filter((r) => r.importBatchId === opts.importBatchId);
  }
  if (opts.countryFilter) {
    const norm = normalizeCountry(opts.countryFilter);
    rows = rows.filter((r) => normalizeCountry(r.rawCountry) === norm);
  }

  return { totalLoaded: records.length, rows, fieldsLoaded: fields };
}

/**
 * Load all evidence rows (candidate links only).
 */
export async function loadAllEvidence() {
  const base = getIndependentCensusBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const fields = [
    "Name",
    EVIDENCE_FIELDS.candidate,
    EVIDENCE_FIELDS.evidenceType,
    EVIDENCE_FIELDS.matchScore,
  ];

  const records = await base(EVIDENCE_TABLE).select({ fields, pageSize: 100 }).all();
  const candidateIds = new Set();
  const rows = [];

  for (const rec of records) {
    const f = rec.fields || {};
    const links = f[EVIDENCE_FIELDS.candidate];
    const ids = Array.isArray(links) ? links : links ? [links] : [];
    for (const cid of ids) {
      if (cid) candidateIds.add(cid);
    }
    rows.push({
      evidenceRecordId: rec.id,
      evidenceName: f.Name || "",
      candidateIds: ids,
      evidenceType: f[EVIDENCE_FIELDS.evidenceType] || "",
      matchScore: f[EVIDENCE_FIELDS.matchScore],
    });
  }

  return { totalLoaded: records.length, rows, evidenceSupportedCandidateIds: candidateIds };
}

/**
 * @param {{ countryFilter?: string }} [opts]
 */
export async function loadAllVerified(opts = {}) {
  const base = getIndependentCensusBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const fields = [
    VERIFIED_FIELDS.verifiedHotelName,
    VERIFIED_FIELDS.verifiedCity,
    VERIFIED_FIELDS.verifiedCountry,
    VERIFIED_FIELDS.verifiedLatitude,
    VERIFIED_FIELDS.verifiedLongitude,
    VERIFIED_FIELDS.verifiedWebsite,
    VERIFIED_FIELDS.verifiedPhone,
    VERIFIED_FIELDS.verifiedBrandLabel,
    VERIFIED_FIELDS.primarySourceCandidate,
    VERIFIED_FIELDS.active,
  ];

  const records = await base(VERIFIED_TABLE).select({ fields, pageSize: 100 }).all();
  let rows = records.map(mapVerifiedRecord);

  if (opts.countryFilter) {
    const norm = normalizeCountry(opts.countryFilter);
    rows = rows.filter((r) => r.countryNorm === norm);
  }

  return { totalLoaded: records.length, rows, fieldsLoaded: fields };
}

function countByCountry(rows, countryField) {
  const map = {};
  for (const r of rows) {
    const ck = normalizeCountry(r[countryField]) || "(unknown)";
    map[ck] = (map[ck] || 0) + 1;
  }
  return map;
}

function censusCountByCountry(censusRows) {
  const map = {};
  for (const r of censusRows) {
    const ck = r.countryNorm || "(unknown)";
    map[ck] = (map[ck] || 0) + 1;
  }
  return map;
}

function missingFieldsForRows(rows, kind) {
  const stats = {
    total: rows.length,
    missingName: 0,
    missingCity: 0,
    missingCountry: 0,
    missingCoordinates: 0,
    missingWebsite: 0,
    missingPhone: 0,
  };

  for (const r of rows) {
    let name = "";
    let city = "";
    let country = "";
    let coords = null;
    let website = "";
    let phone = "";

    if (kind === "legacy") {
      name = r.name;
      city = r.city;
      country = r.country;
      coords = r.coords;
      website = r.websiteHost ? "set" : "";
      phone = r.phoneNorm ? "set" : "";
    } else if (kind === "candidate") {
      name = r.rawHotelName;
      city = r.rawCity;
      country = r.rawCountry;
      coords = parseCoords(r.rawLatitude, r.rawLongitude);
      website = r.rawWebsite;
      phone = r.rawPhone;
    } else if (kind === "verified") {
      name = r.verifiedHotelName;
      city = r.verifiedCity;
      country = r.verifiedCountry;
      coords = parseCoords(r.verifiedLatitude, r.verifiedLongitude);
      website = r.verifiedWebsite;
      phone = r.verifiedPhone;
    }

    if (!normalizeKey(name)) stats.missingName++;
    if (!normalizeKey(city)) stats.missingCity++;
    if (!normalizeKey(country)) stats.missingCountry++;
    if (!coords) stats.missingCoordinates++;
    if (!normalizeKey(website) && kind !== "legacy") stats.missingWebsite++;
    if (kind === "legacy" && !website) stats.missingWebsite++;
    if (!normalizeKey(phone) && kind !== "legacy") stats.missingPhone++;
    if (kind === "legacy" && !phone) stats.missingPhone++;
  }

  return stats;
}

function isStrongMatch(match) {
  return STRONG_MATCH_CONFIDENCE.has(match.matchConfidence);
}

/**
 * @param {Array<object>} candidates
 * @param {object} censusData
 */
export function matchCandidatesToLegacy(candidates, censusData) {
  const matches = [];
  for (const c of candidates) {
    const m = matchCandidateToCensus(candidateAsMatchShape(c), censusData);
    matches.push({
      candidateAirtableRecordId: c.airtableRecordId,
      sourceRecordId: c.sourceRecordId,
      rawHotelName: c.rawHotelName,
      rawCity: c.rawCity,
      rawCountry: c.rawCountry,
      importBatchId: c.importBatchId,
      ...m,
    });
  }
  return matches;
}

export function matchVerifiedToLegacy(verifiedRows, censusData) {
  const matches = [];
  for (const v of verifiedRows) {
    const m = matchCandidateToCensus(verifiedAsMatchShape(v), censusData);
    matches.push({
      verifiedAirtableRecordId: v.airtableRecordId,
      verifiedHotelName: v.verifiedHotelName,
      verifiedCity: v.verifiedCity,
      verifiedCountry: v.verifiedCountry,
      primarySourceCandidateId: v.primarySourceCandidateId,
      ...m,
    });
  }
  return matches;
}

export function findDuplicateRiskClusters(candidates, candidateMatches) {
  const clusters = [];
  const matchById = new Map(
    candidateMatches.map((m) => [m.candidateAirtableRecordId, m])
  );

  const byCensus = new Map();
  for (const c of candidates) {
    const m = matchById.get(c.airtableRecordId);
    if (!m?.matchedCensusRecordId || !isStrongMatch(m)) continue;
    const id = m.matchedCensusRecordId;
    if (!byCensus.has(id)) byCensus.set(id, []);
    byCensus.get(id).push({
      candidateAirtableRecordId: c.airtableRecordId,
      rawHotelName: c.rawHotelName,
      matchScore: m.matchScore,
      matchConfidence: m.matchConfidence,
    });
  }

  for (const [censusRecordId, list] of byCensus) {
    if (list.length > 1) {
      clusters.push({
        clusterType: "multiple_candidates_same_legacy",
        censusRecordId,
        candidateCount: list.length,
        candidates: list,
      });
    }
  }

  const geoBuckets = [];
  for (let i = 0; i < candidates.length; i++) {
    const a = candidates[i];
    const ca = parseCoords(a.rawLatitude, a.rawLongitude);
    if (!ca || !normalizeKey(a.rawHotelName)) continue;
    const na = normalizeKey(a.rawHotelName);

    for (let j = i + 1; j < candidates.length; j++) {
      const b = candidates[j];
      const cb = parseCoords(b.rawLatitude, b.rawLongitude);
      if (!cb) continue;
      const dist = distanceMeters(ca, cb);
      if (dist == null || dist > NEARBY_DUPLICATE_METERS) continue;
      const ns = nameSimilarity(a.rawHotelName, b.rawHotelName);
      if (ns < 0.55) continue;
      geoBuckets.push({
        clusterType: "nearby_similar_name",
        distanceMeters: Math.round(dist),
        nameSimilarity: Number(ns.toFixed(2)),
        candidateAirtableRecordIds: [a.airtableRecordId, b.airtableRecordId],
        names: [a.rawHotelName, b.rawHotelName],
      });
    }
  }

  return [...clusters, ...geoBuckets.slice(0, 200)];
}

function pct(numerator, denominator) {
  if (!denominator) return null;
  return Number(((numerator / denominator) * 100).toFixed(2));
}

function buildCountryMetrics({
  countryKey,
  legacyCount,
  candidateCount,
  evidenceSupportedCount,
  verifiedCount,
  legacyMatchedByCandidates,
  legacyMatchedByVerified,
  independentOnlyCandidates,
  legacyOnlyRecords,
  duplicateRiskClusters,
}) {
  return {
    country: countryKey,
    legacyRecordCount: legacyCount,
    independentCandidateCount: candidateCount,
    evidenceSupportedCandidateCount: evidenceSupportedCount,
    verifiedRecordCount: verifiedCount,
    legacyMatchedByCandidates: legacyMatchedByCandidates,
    legacyMatchedByVerified: legacyMatchedByVerified,
    candidateCoveragePct: pct(legacyMatchedByCandidates, legacyCount),
    verifiedCoveragePct: pct(legacyMatchedByVerified, legacyCount),
    independentOnlyCandidateCount: independentOnlyCandidates,
    legacyOnlyRecordCount: legacyOnlyRecords,
    duplicateRiskClusterCount: duplicateRiskClusters,
  };
}

function recommendNextValidationSource(dr) {
  const recs = [];
  if (dr.legacyOnlyRecordCount > dr.verifiedMatchedToLegacy) {
    recs.push("brand_directory");
  }
  if (dr.candidatesMissingCity > 0) {
    recs.push("manual_city_geocoding");
  }
  if (dr.independentOnlyCandidateCount > dr.evidenceSupportedCandidateCount) {
    recs.push("wikidata_evidence_expansion");
  }
  if (dr.legacyRecordCount > dr.legacyMatchedByCandidates) {
    recs.push("government_registry");
  }
  if (!recs.length) recs.push("continue_osm_wikidata_two_source_review");
  return recs;
}

/**
 * @param {object} input
 */
export function buildCoverageBenchmark(input) {
  const {
    censusData,
    allCandidates,
    allEvidence,
    allVerified,
    focusCountry,
    candidateBatchId,
  } = input;

  const focusNorm = focusCountry ? normalizeCountry(focusCountry) : "";

  const censusAll = censusData.rows;
  const censusFocus = focusNorm
    ? censusAll.filter((r) => r.countryNorm === focusNorm)
    : censusAll;

  const candidatesAll = allCandidates.rows;
  const candidatesFocus = focusNorm
    ? candidatesAll.filter((c) => normalizeCountry(c.rawCountry) === focusNorm)
    : candidatesAll;

  const verifiedAll = allVerified.rows;
  const verifiedFocus = focusNorm
    ? verifiedAll.filter((v) => v.countryNorm === focusNorm)
    : verifiedAll;

  const evidenceIds = allEvidence.evidenceSupportedCandidateIds;
  const evidenceSupportedAll = candidatesAll.filter((c) =>
    evidenceIds.has(c.airtableRecordId)
  );
  const evidenceSupportedFocus = candidatesFocus.filter((c) =>
    evidenceIds.has(c.airtableRecordId)
  );

  const censusPoolAll = { ...censusData, rows: censusAll };
  const censusPoolFocus = { ...censusData, rows: censusFocus };

  const candidateMatchesAll = matchCandidatesToLegacy(candidatesAll, censusPoolAll);
  const candidateMatchesFocus = matchCandidatesToLegacy(candidatesFocus, censusPoolFocus);
  const verifiedMatchesAll = matchVerifiedToLegacy(verifiedAll, censusPoolAll);
  const verifiedMatchesFocus = matchVerifiedToLegacy(verifiedFocus, censusPoolFocus);

  const legacyMatchedByCandidatesAll = new Set();
  const legacyMatchedByCandidatesFocus = new Set();
  let likelyLegacyMatchesAll = 0;
  let likelyLegacyMatchesFocus = 0;
  let independentOnlyAll = 0;
  let independentOnlyFocus = 0;

  for (const m of candidateMatchesAll) {
    if (isStrongMatch(m)) {
      likelyLegacyMatchesAll++;
      if (m.matchedCensusRecordId) legacyMatchedByCandidatesAll.add(m.matchedCensusRecordId);
    } else if (
      m.recommendedAction === MATCH_RECOMMENDED_ACTIONS.LIKELY_NEW_CANDIDATE ||
      m.matchConfidence === "none"
    ) {
      independentOnlyAll++;
    }
  }

  for (const m of candidateMatchesFocus) {
    if (isStrongMatch(m)) {
      likelyLegacyMatchesFocus++;
      if (m.matchedCensusRecordId) legacyMatchedByCandidatesFocus.add(m.matchedCensusRecordId);
    } else if (
      m.recommendedAction === MATCH_RECOMMENDED_ACTIONS.LIKELY_NEW_CANDIDATE ||
      m.matchConfidence === "none"
    ) {
      independentOnlyFocus++;
    }
  }

  const legacyMatchedByVerifiedAll = new Set();
  const legacyMatchedByVerifiedFocus = new Set();
  for (const m of verifiedMatchesAll) {
    if (isStrongMatch(m) && m.matchedCensusRecordId) {
      legacyMatchedByVerifiedAll.add(m.matchedCensusRecordId);
    }
  }
  for (const m of verifiedMatchesFocus) {
    if (isStrongMatch(m) && m.matchedCensusRecordId) {
      legacyMatchedByVerifiedFocus.add(m.matchedCensusRecordId);
    }
  }

  const legacyOnlyAll = censusAll.filter(
    (r) =>
      !legacyMatchedByCandidatesAll.has(r.recordId) &&
      !legacyMatchedByVerifiedAll.has(r.recordId)
  );
  const legacyOnlyFocus = censusFocus.filter(
    (r) =>
      !legacyMatchedByCandidatesFocus.has(r.recordId) &&
      !legacyMatchedByVerifiedFocus.has(r.recordId)
  );

  const dupClustersAll = findDuplicateRiskClusters(candidatesAll, candidateMatchesAll);
  const dupClustersFocus = findDuplicateRiskClusters(candidatesFocus, candidateMatchesFocus);

  const legacyByCountry = censusCountByCountry(censusAll);
  const candidateByCountry = countByCountry(candidatesAll, "rawCountry");
  const verifiedByCountry = countByCountry(verifiedAll, "verifiedCountry");

  const evidenceByCountry = {};
  for (const c of candidatesAll) {
    if (!evidenceIds.has(c.airtableRecordId)) continue;
    const ck = normalizeCountry(c.rawCountry) || "(unknown)";
    evidenceByCountry[ck] = (evidenceByCountry[ck] || 0) + 1;
  }

  const countryKeys = new Set([
    ...Object.keys(legacyByCountry),
    ...Object.keys(candidateByCountry),
    ...Object.keys(verifiedByCountry),
    ...Object.keys(evidenceByCountry),
  ]);

  const byCountry = [];
  for (const ck of [...countryKeys].sort()) {
    const legacyCount = legacyByCountry[ck] || 0;
    const candSubset = candidatesAll.filter(
      (c) => (normalizeCountry(c.rawCountry) || "(unknown)") === ck
    );
    const candMatches = matchCandidatesToLegacy(candSubset, {
      ...censusData,
      rows: censusAll.filter((r) => (r.countryNorm || "(unknown)") === ck),
    });
    const verSubset = verifiedAll.filter(
      (v) => (v.countryNorm || "(unknown)") === ck
    );
    const verMatches = matchVerifiedToLegacy(verSubset, {
      ...censusData,
      rows: censusAll.filter((r) => (r.countryNorm || "(unknown)") === ck),
    });

    const legacyMatchedCand = new Set();
    let indepOnly = 0;
    for (const m of candMatches) {
      if (isStrongMatch(m) && m.matchedCensusRecordId) {
        legacyMatchedCand.add(m.matchedCensusRecordId);
      } else if (
        m.recommendedAction === MATCH_RECOMMENDED_ACTIONS.LIKELY_NEW_CANDIDATE
      ) {
        indepOnly++;
      }
    }
    const legacyMatchedVer = new Set();
    for (const m of verMatches) {
      if (isStrongMatch(m) && m.matchedCensusRecordId) {
        legacyMatchedVer.add(m.matchedCensusRecordId);
      }
    }
    const legacyOnly = legacyCount - legacyMatchedCand.size;

    const dupCount = findDuplicateRiskClusters(candSubset, candMatches).filter(
      (c) => c.clusterType === "multiple_candidates_same_legacy"
    ).length;

    byCountry.push(
      buildCountryMetrics({
        countryKey: ck,
        legacyCount,
        candidateCount: candidateByCountry[ck] || 0,
        evidenceSupportedCount: evidenceByCountry[ck] || 0,
        verifiedCount: verifiedByCountry[ck] || 0,
        legacyMatchedByCandidates: legacyMatchedCand.size,
        legacyMatchedByVerified: legacyMatchedVer.size,
        independentOnlyCandidates: indepOnly,
        legacyOnlyRecords: Math.max(0, legacyOnly),
        duplicateRiskClusters: dupCount,
      })
    );
  }

  const lowestVerifiedCoverage = [...byCountry]
    .filter((c) => c.legacyRecordCount >= 5)
    .sort((a, b) => (a.verifiedCoveragePct ?? 0) - (b.verifiedCoveragePct ?? 0))
    .slice(0, 15);

  const dominicanRepublic = {
    country: focusCountry || "Dominican Republic",
    candidateBatchId: candidateBatchId || null,
    legacyRecordCount: censusFocus.length,
    independentCandidateCount: candidatesFocus.length,
    evidenceSupportedCandidateCount: evidenceSupportedFocus.length,
    verifiedRecordCount: verifiedFocus.length,
    likelyMatchedToLegacy: likelyLegacyMatchesFocus,
    likelyMissingFromIndependentCandidates: legacyOnlyFocus.length,
    likelyIndependentOnly: independentOnlyFocus,
    legacyMatchedByCandidates: legacyMatchedByCandidatesFocus.size,
    legacyMatchedByVerified: legacyMatchedByVerifiedFocus.size,
    candidateCoveragePct: pct(legacyMatchedByCandidatesFocus.size, censusFocus.length),
    verifiedCoveragePct: pct(legacyMatchedByVerifiedFocus.size, censusFocus.length),
    candidatesMissingCity: candidatesFocus.filter((c) => !normalizeKey(c.rawCity)).length,
    verifiedMatchedToLegacy: legacyMatchedByVerifiedFocus.size,
    duplicateRiskClusterCount: dupClustersFocus.length,
    duplicateRiskClusters: dupClustersFocus.slice(0, 50),
    legacyOnlySample: legacyOnlyFocus.slice(0, 25).map((r) => ({
      censusRecordId: r.recordId,
      name: r.name,
      city: r.city,
      country: r.country,
    })),
    independentOnlySample: candidateMatchesFocus
      .filter(
        (m) =>
          !isStrongMatch(m) &&
          m.recommendedAction === MATCH_RECOMMENDED_ACTIONS.LIKELY_NEW_CANDIDATE
      )
      .slice(0, 25)
      .map((m) => ({
        candidateAirtableRecordId: m.candidateAirtableRecordId,
        rawHotelName: m.rawHotelName,
        rawCity: m.rawCity,
        matchScore: m.matchScore,
      })),
    recommendedNextValidationSources: [],
  };
  dominicanRepublic.recommendedNextValidationSources =
    recommendNextValidationSource(dominicanRepublic);

  const missingFieldsBySource = {
    legacy: missingFieldsForRows(censusFocus.length ? censusFocus : censusAll, "legacy"),
    candidates: missingFieldsForRows(
      candidatesFocus.length ? candidatesFocus : candidatesAll,
      "candidate"
    ),
    verified: missingFieldsForRows(
      verifiedFocus.length ? verifiedFocus : verifiedAll,
      "verified"
    ),
  };

  return {
    generatedAt: new Date().toISOString(),
    phase: "4E-coverage-benchmark",
    focusCountry: focusCountry || null,
    candidateBatchId: candidateBatchId || null,
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    verifiedTableWrites: false,
    candidateTableWrites: false,
    evidenceTableWrites: false,
    brandAliasWrites: false,
    strFieldsUsed: false,
    censusFieldsRead: censusData.fieldsLoaded,
    tablesRead: [
      censusData.table,
      CANDIDATES_TABLE,
      EVIDENCE_TABLE,
      VERIFIED_TABLE,
    ],
    totals: {
      legacyHotelCensus: censusData.totalLoaded,
      legacyInFocusCountry: censusFocus.length,
      independentCandidates: allCandidates.totalLoaded,
      independentCandidatesInFocusCountry: candidatesFocus.length,
      evidenceRows: allEvidence.totalLoaded,
      evidenceSupportedCandidates: evidenceSupportedAll.length,
      evidenceSupportedInFocusCountry: evidenceSupportedFocus.length,
      verifiedRecords: allVerified.totalLoaded,
      verifiedInFocusCountry: verifiedFocus.length,
    },
    overlap: {
      global: {
        likelyLegacyMatches: likelyLegacyMatchesAll,
        likelyIndependentOnlyCandidates: independentOnlyAll,
        likelyLegacyOnlyRecords: legacyOnlyAll.length,
        legacyMatchedByCandidates: legacyMatchedByCandidatesAll.size,
        legacyMatchedByVerified: legacyMatchedByVerifiedAll.size,
        candidateCoveragePct: pct(
          legacyMatchedByCandidatesAll.size,
          censusAll.length
        ),
        verifiedCoveragePct: pct(
          legacyMatchedByVerifiedAll.size,
          censusAll.length
        ),
        duplicateRiskClusterCount: dupClustersAll.length,
      },
      focusCountry: {
        likelyLegacyMatches: likelyLegacyMatchesFocus,
        likelyIndependentOnlyCandidates: independentOnlyFocus,
        likelyLegacyOnlyRecords: legacyOnlyFocus.length,
        legacyMatchedByCandidates: legacyMatchedByCandidatesFocus.size,
        legacyMatchedByVerified: legacyMatchedByVerifiedFocus.size,
        candidateCoveragePct: pct(
          legacyMatchedByCandidatesFocus.size,
          censusFocus.length
        ),
        verifiedCoveragePct: pct(
          legacyMatchedByVerifiedFocus.size,
          censusFocus.length
        ),
        duplicateRiskClusterCount: dupClustersFocus.length,
      },
    },
    countsByCountry: {
      legacy: legacyByCountry,
      candidates: candidateByCountry,
      verified: verifiedByCountry,
      evidenceSupportedCandidates: evidenceByCountry,
    },
    byCountry,
    lowestVerifiedCoverageCountries: lowestVerifiedCoverage,
    dominicanRepublicSection: dominicanRepublic,
    missingFieldsBySource,
    duplicateRiskClusters: dupClustersFocus.slice(0, 100),
  };
}

export function benchmarkToCsvRows(benchmark) {
  return benchmark.byCountry.map((c) => ({
    Country: c.country,
    "Legacy Record Count": c.legacyRecordCount,
    "Independent Candidate Count": c.independentCandidateCount,
    "Evidence Supported Candidate Count": c.evidenceSupportedCandidateCount,
    "Verified Record Count": c.verifiedRecordCount,
    "Legacy Matched By Candidates": c.legacyMatchedByCandidates,
    "Legacy Matched By Verified": c.legacyMatchedByVerified,
    "Candidate Coverage Pct": c.candidateCoveragePct ?? "",
    "Verified Coverage Pct": c.verifiedCoveragePct ?? "",
    "Independent Only Candidates": c.independentOnlyCandidateCount,
    "Legacy Only Records": c.legacyOnlyRecordCount,
    "Duplicate Risk Clusters": c.duplicateRiskClusterCount,
  }));
}

export const BENCHMARK_CSV_COLUMNS = [
  "Country",
  "Legacy Record Count",
  "Independent Candidate Count",
  "Evidence Supported Candidate Count",
  "Verified Record Count",
  "Legacy Matched By Candidates",
  "Legacy Matched By Verified",
  "Candidate Coverage Pct",
  "Verified Coverage Pct",
  "Independent Only Candidates",
  "Legacy Only Records",
  "Duplicate Risk Clusters",
];
