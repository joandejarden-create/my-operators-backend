/**
 * Read-only match: Wikidata dry-run candidates ↔ staging Independent Hotel Source Candidates.
 * Phase 2E — report only; no Airtable writes.
 */

import {
  CANDIDATES_TABLE,
  CANDIDATE_FIELDS,
  SOURCE_TYPES,
} from "./fields.js";
import { getIndependentCensusBase } from "./platform-base.js";
import {
  nameSimilarity,
  distanceMeters,
  parseCoords,
  websiteHost,
  countriesMatch,
  citiesMatch,
  normalizeText,
  normalizeKey,
  normalizeCountry,
} from "./match-current-census.js";

export const STAGING_MATCH_CONFIDENCE = ["high", "medium", "low", "none"];

export const STAGING_MATCH_ACTIONS = {
  LIKELY_SAME_PROPERTY: "likely_same_property",
  POSSIBLE_SAME_PROPERTY: "possible_same_property",
  LIKELY_NEW_WIKIDATA: "likely_new_wikidata",
  NEEDS_RESEARCH: "needs_research",
};

/**
 * Load staging candidates for a batch (read-only).
 * @param {{ importBatchId: string, sourceType?: string, countryFilter?: string }} opts
 */
export async function loadStagingCandidatesReadOnly(opts) {
  const base = getIndependentCensusBase();
  if (!base) {
    throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");
  }

  const batchEsc = String(opts.importBatchId).replace(/'/g, "\\'");
  let formula = `{${CANDIDATE_FIELDS.importBatchId}} = '${batchEsc}'`;
  if (opts.sourceType) {
    const st = String(opts.sourceType).replace(/'/g, "\\'");
    formula = `AND(${formula}, {${CANDIDATE_FIELDS.sourceType}} = '${st}')`;
  }

  const fields = [
    CANDIDATE_FIELDS.sourceRecordId,
    CANDIDATE_FIELDS.sourceName,
    CANDIDATE_FIELDS.sourceType,
    CANDIDATE_FIELDS.rawHotelName,
    CANDIDATE_FIELDS.rawCity,
    CANDIDATE_FIELDS.rawCountry,
    CANDIDATE_FIELDS.rawLatitude,
    CANDIDATE_FIELDS.rawLongitude,
    CANDIDATE_FIELDS.rawWebsite,
    CANDIDATE_FIELDS.rawBrand,
    CANDIDATE_FIELDS.candidateDedupeKey,
    CANDIDATE_FIELDS.importBatchId,
  ];

  const rows = [];
  await new Promise((resolve, reject) => {
    base(CANDIDATES_TABLE)
      .select({ filterByFormula: formula, fields })
      .eachPage(
        (records, fetchNextPage) => {
          for (const rec of records) {
            const f = rec.fields;
            const coords = parseCoords(
              f[CANDIDATE_FIELDS.rawLatitude],
              f[CANDIDATE_FIELDS.rawLongitude]
            );
            rows.push({
              airtableRecordId: rec.id,
              sourceRecordId: f[CANDIDATE_FIELDS.sourceRecordId] || "",
              sourceType: f[CANDIDATE_FIELDS.sourceType] || "",
              rawHotelName: normalizeText(f[CANDIDATE_FIELDS.rawHotelName]),
              rawCity: normalizeText(f[CANDIDATE_FIELDS.rawCity]),
              rawCountry: normalizeText(f[CANDIDATE_FIELDS.rawCountry]),
              coords,
              websiteHost: websiteHost(f[CANDIDATE_FIELDS.rawWebsite]),
              rawBrand: normalizeText(f[CANDIDATE_FIELDS.rawBrand]),
              candidateDedupeKey: f[CANDIDATE_FIELDS.candidateDedupeKey] || "",
            });
          }
          fetchNextPage();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });

  let filtered = rows;
  if (opts.countryFilter) {
    const want = normalizeCountry(opts.countryFilter);
    filtered = rows.filter((r) => countriesMatch(r.rawCountry, want));
  }

  return {
    rows: filtered,
    totalLoaded: rows.length,
    matchingPoolSize: filtered.length,
    importBatchId: opts.importBatchId,
    sourceType: opts.sourceType || null,
  };
}

function scoreStagingMatch(wd, staging) {
  const reasons = [];
  let score = 0;

  const ns = nameSimilarity(wd.rawHotelName, staging.rawHotelName);
  if (ns >= 0.92) {
    score += 45;
    reasons.push(`name=${ns.toFixed(2)}`);
  } else if (ns >= 0.75) {
    score += 30;
    reasons.push(`name=${ns.toFixed(2)}`);
  } else if (ns >= 0.55) {
    score += 15;
    reasons.push(`name=${ns.toFixed(2)}`);
  }

  if (!countriesMatch(wd.rawCountry, staging.rawCountry)) {
    return { score: 0, confidence: "none", reason: "Country mismatch", ns };
  }
  score += 10;

  const cityMatch = citiesMatch(wd.rawCity, staging.rawCity);
  if (cityMatch === true) {
    score += 15;
    reasons.push("city");
  } else if (cityMatch === false) {
    score -= 5;
  }

  const wdCoords = parseCoords(wd.rawLatitude, wd.rawLongitude);
  const dist = distanceMeters(wdCoords, staging.coords);
  if (dist != null) {
    if (dist <= 75) {
      score += 35;
      reasons.push(`geo=${Math.round(dist)}m`);
    } else if (dist <= 250) {
      score += 22;
      reasons.push(`geo=${Math.round(dist)}m`);
    } else if (dist <= 800) {
      score += 10;
      reasons.push(`geo=${Math.round(dist)}m`);
    }
  }

  const wh = websiteHost(wd.rawWebsite);
  if (wh && staging.websiteHost && wh === staging.websiteHost) {
    score += 25;
    reasons.push("website");
  }

  let confidence = "none";
  if (score >= 70) confidence = "high";
  else if (score >= 48) confidence = "medium";
  else if (score >= 28) confidence = "low";

  return {
    score,
    confidence,
    reason: reasons.length ? reasons.join("; ") : "Weak or no match signals",
    distanceMeters: dist,
    nameSimilarity: ns,
  };
}

function assignStagingAction(confidence, ns) {
  if (confidence === "high") return STAGING_MATCH_ACTIONS.LIKELY_SAME_PROPERTY;
  if (confidence === "medium") return STAGING_MATCH_ACTIONS.POSSIBLE_SAME_PROPERTY;
  if (confidence === "low" && ns >= 0.5) return STAGING_MATCH_ACTIONS.NEEDS_RESEARCH;
  return STAGING_MATCH_ACTIONS.LIKELY_NEW_WIKIDATA;
}

/**
 * @param {Array<object>} wikidataCandidates
 * @param {{ rows: Array<object> }} stagingPool
 */
export function matchWikidataToStaging(wikidataCandidates, stagingPool) {
  const rows = [];
  const summary = {
    totalWikidata: wikidataCandidates.length,
    stagingPoolSize: stagingPool.rows.length,
    high: 0,
    medium: 0,
    low: 0,
    none: 0,
    likely_same_property: 0,
    possible_same_property: 0,
    likely_new_wikidata: 0,
    needs_research: 0,
    averageDistanceMetersHigh: null,
  };

  const highDists = [];

  for (const wd of wikidataCandidates) {
    let best = null;
    let bestStaging = null;

    for (const st of stagingPool.rows) {
      const m = scoreStagingMatch(wd, st);
      if (!best || m.score > best.score) {
        best = m;
        bestStaging = st;
      }
    }

    const confidence = best?.confidence || "none";
    const action = assignStagingAction(confidence, best?.nameSimilarity || 0);

    summary[confidence] = (summary[confidence] || 0) + 1;
    summary[action] = (summary[action] || 0) + 1;
    if (confidence === "high" && best?.distanceMeters != null) {
      highDists.push(best.distanceMeters);
    }

    rows.push({
      wikidataQid: wd.sourceRecordId || wd._wikidataQid,
      wikidataName: wd.rawHotelName,
      wikidataCity: wd.rawCity,
      wikidataCountry: wd.rawCountry,
      wikidataLatitude: wd.rawLatitude,
      wikidataLongitude: wd.rawLongitude,
      wikidataWebsite: wd.rawWebsite,
      matchedStagingRecordId: bestStaging?.airtableRecordId || "",
      matchedStagingSourceRecordId: bestStaging?.sourceRecordId || "",
      matchedStagingName: bestStaging?.rawHotelName || "",
      matchedStagingCity: bestStaging?.rawCity || "",
      matchConfidence: confidence,
      matchScore: best?.score ?? 0,
      matchReason: best?.reason || "",
      distanceMeters: best?.distanceMeters ?? "",
      nameSimilarity: best?.nameSimilarity != null ? Number(best.nameSimilarity.toFixed(3)) : "",
      recommendedAction: action,
    });
  }

  if (highDists.length) {
    summary.averageDistanceMetersHigh = Math.round(
      highDists.reduce((a, b) => a + b, 0) / highDists.length
    );
  }

  return { rows, summary };
}
