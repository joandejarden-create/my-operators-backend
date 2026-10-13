/**
 * Resolve portfolio properties against Dealality Census `dhl_*` (P1.7).
 * Light matcher — does not mass-create hotel_id mappings for the full Census pool.
 * Ambiguous matches never auto-create edges.
 */

import {
  nameSimilarity,
  citiesMatch,
  countriesMatch,
  websiteHost,
} from "../../../independent-census/match-current-census.js";
import { MAP_CENSUS_FIELDS } from "../../map_hotel_intelligence_fields.js";

export const PORTFOLIO_CENSUS_MATCH_VERSION = "ownership-portfolio-census-match-v2";

export const PORTFOLIO_MATCH_STATUS = Object.freeze({
  EXACT: "EXACT",
  STRONG: "STRONG",
  PROBABLE: "PROBABLE",
  AMBIGUOUS: "AMBIGUOUS",
  NOT_FOUND: "NOT_FOUND",
});

function censusName(rec) {
  const f = rec?.fields || {};
  return f[MAP_CENSUS_FIELDS.officialName] || f[MAP_CENSUS_FIELDS.propertyName] || null;
}

function scoreAssetAgainstRecord(asset, rec) {
  const f = rec?.fields || {};
  const name = censusName(rec);
  if (!name || !asset?.name) return null;
  if (asset.country && f[MAP_CENSUS_FIELDS.country]) {
    if (!countriesMatch(asset.country, f[MAP_CENSUS_FIELDS.country])) return null;
  }
  let score = nameSimilarity(asset.name, name);
  const city = f[MAP_CENSUS_FIELDS.city];
  if (asset.city && city) {
    if (citiesMatch(asset.city, city)) score = Math.min(1, score + 0.12);
    else score *= 0.65;
  }
  if (asset.brand && f[MAP_CENSUS_FIELDS.brandName]) {
    if (nameSimilarity(asset.brand, f[MAP_CENSUS_FIELDS.brandName]) >= 0.7) {
      score = Math.min(1, score + 0.05);
    }
  }
  const aHost = websiteHost(asset.website);
  const cHost = websiteHost(f[MAP_CENSUS_FIELDS.website]);
  if (aHost && cHost && aHost === cHost) score = Math.min(1, score + 0.1);
  return { score, name, city, country: f[MAP_CENSUS_FIELDS.country] || null };
}

/**
 * @param {object} asset
 * @param {object[]} censusRecords
 * @param {{ idRegistry?: object }} [opts]
 */
export function matchPortfolioAssetToCensus(asset, censusRecords = [], opts = {}) {
  const scored = [];
  for (const rec of censusRecords || []) {
    const s = scoreAssetAgainstRecord(asset, rec);
    if (!s || s.score < 0.45) continue;
    scored.push({ rec, ...s });
  }
  scored.sort((a, b) => b.score - a.score);
  const top = scored.slice(0, 5);

  if (!top.length) {
    return {
      match_status: PORTFOLIO_MATCH_STATUS.NOT_FOUND,
      hotel_id: null,
      match_score: 0,
      review_required: false,
      matching_reasons: ["no_candidate_above_floor"],
      candidate_matches: [],
      census_name: null,
      census_record_id: null,
      auto_edge_eligible: false,
    };
  }

  const best = top[0];
  const second = top[1];
  const ambiguous =
    second &&
    best.score - second.score < 0.08 &&
    best.score >= 0.7 &&
    second.score >= 0.7;

  let status = PORTFOLIO_MATCH_STATUS.NOT_FOUND;
  if (ambiguous) status = PORTFOLIO_MATCH_STATUS.AMBIGUOUS;
  else if (best.score >= 0.92 && (!asset.city || citiesMatch(asset.city, best.city))) {
    status = PORTFOLIO_MATCH_STATUS.EXACT;
  } else if (best.score >= 0.8) status = PORTFOLIO_MATCH_STATUS.STRONG;
  else if (best.score >= 0.62) status = PORTFOLIO_MATCH_STATUS.PROBABLE;
  else status = PORTFOLIO_MATCH_STATUS.NOT_FOUND;

  let hotelId = null;
  if (
    (status === PORTFOLIO_MATCH_STATUS.EXACT || status === PORTFOLIO_MATCH_STATUS.STRONG) &&
    opts.idRegistry?.ensureHotelIdForAirtable
  ) {
    hotelId = opts.idRegistry.ensureHotelIdForAirtable(best.rec.id, {
      property_identity_key:
        best.rec.fields?.[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
    });
  }

  return {
    match_status: status,
    hotel_id: hotelId,
    match_score: Number(best.score.toFixed(3)),
    review_required: status === PORTFOLIO_MATCH_STATUS.AMBIGUOUS,
    matching_reasons: [`name_city_score:${best.score.toFixed(3)}`],
    candidate_matches: top.map((t) => ({
      airtable_record_id: t.rec.id,
      match_score: Number(t.score.toFixed(3)),
      name: t.name,
      city: t.city,
      country: t.country,
    })),
    census_name: best.name,
    census_record_id: best.rec.id,
    auto_edge_eligible:
      status === PORTFOLIO_MATCH_STATUS.EXACT || status === PORTFOLIO_MATCH_STATUS.STRONG,
  };
}
