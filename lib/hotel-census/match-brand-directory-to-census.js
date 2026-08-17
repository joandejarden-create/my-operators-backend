/**
 * Match brand directory rows to existing Hotel Census records (read-only scoring).
 */

import {
  nameSimilarity,
  normalizeCountry,
  countriesMatch,
  citiesMatch,
  parseCoords,
  distanceMeters,
  websiteHost,
  normalizePhone,
  normalizeText,
} from "../independent-census/match-current-census.js";
import { censusCountryToSitemapSlug } from "../marriott-brand-directory-extract.js";
import { MAP_DIRECTORY_ENRICHMENT } from "./brand-directory-enrichment-contract.js";
import { HOTEL_CENSUS_TABLE } from "./fields.js";
import {
  accorBrandTokenOverlap,
  accorPropertyIdFromWebsite,
} from "./accor-directory-name-normalize.js";
import { getPlatformBase } from "./platform-base.js";
import {
  CENSUS_AMENITIES_TEXT_FIELD,
  CENSUS_AMENITY_YN_COLUMNS,
} from "../hilton-amenity-map.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";

const F = MAP_DIRECTORY_ENRICHMENT;

export function ctyhocnFromWebsite(url) {
  const s = String(url || "");
  const m = s.match(/\/hotels\/([a-z0-9]+)-/i);
  return m ? m[1].toUpperCase() : "";
}

/**
 * @param {object} directoryRow
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>} censusRow
 */
export function directoryCountryPageMatchesCensus(directoryRow, censusRow) {
  if (!directoryRow?.countryPage) return false;
  const slug = censusCountryToSitemapSlug(censusRow.country);
  return Boolean(slug && slug === directoryRow.countryPage);
}

/**
 * @param {import('airtable').Record} record
 */
export function mapCensusRowForDirectoryMatch(record) {
  const f = record.fields || {};
  const coords = parseCoords(f[F.lat], f[F.lng]);
  return {
    recordId: record.id,
    name: normalizeText(f[F.name]),
    city: normalizeText(f[F.city]),
    country: normalizeText(f[F.country]),
    countryNorm: normalizeCountry(f[F.country]),
    coords,
    lat: coords?.lat ?? null,
    lng: coords?.lng ?? null,
    website: normalizeText(f[F.website]),
    websiteHost: websiteHost(f[F.website]),
    websiteCtyhocn: ctyhocnFromWebsite(f[F.website]),
    telephone: normalizeText(f[F.telephone]),
    phoneNorm: normalizePhone(f[F.telephone]),
    affiliation: normalizeText(f[F.affiliation]),
    parentCompany: normalizeText(f[F.parentCompany]),
    status: normalizeText(f[F.status]),
    brandPropertyCode: normalizeText(f[F.brandPropertyCode]).toUpperCase(),
    accorPropertyId: accorPropertyIdFromWebsite(f[F.website]),
    fields: f,
  };
}

/**
 * @param {string[]} affiliationMatchers
 */
export function buildAffiliationFilterFormula(affiliationMatchers) {
  const parts = affiliationMatchers
    .map((a) => String(a || "").trim())
    .filter(Boolean)
    .map((a) => `{${F.affiliation}}='${a.replace(/'/g, "\\'")}'`);
  if (!parts.length) return "";
  return parts.length === 1 ? parts[0] : `OR(${parts.join(",")})`;
}

/**
 * @param {string[]} affiliationMatchers
 */
export async function loadCensusRowsForAffiliations(affiliationMatchers) {
  const base = getPlatformBase();
  if (!base) {
    throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");
  }

  const selectFields = await getCensusEnrichmentSelectFields(base);

  const formula = buildAffiliationFilterFormula(affiliationMatchers);
  const selectOpts = { fields: selectFields, pageSize: 100 };
  if (formula) selectOpts.filterByFormula = formula;

  const records = await base(HOTEL_CENSUS_TABLE).select(selectOpts).all();

  return {
    table: HOTEL_CENSUS_TABLE,
    totalLoaded: records.length,
    rows: records.map(mapCensusRowForDirectoryMatch),
  };
}

/**
 * @param {object} directoryRow
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>} censusRow
 */
export function scoreDirectoryAgainstCensus(directoryRow, censusRow) {
  const reasons = [];

  if (
    directoryRow.brandPropertyCode &&
    censusRow.brandPropertyCode &&
    directoryRow.brandPropertyCode === censusRow.brandPropertyCode
  ) {
    return {
      score: 100,
      confidence: "high",
      reason: "Exact Brand Property Code match",
      nameSim: 1,
      distanceMeters: null,
    };
  }

  if (
    directoryRow.brandPropertyCode &&
    censusRow.websiteCtyhocn &&
    directoryRow.brandPropertyCode === censusRow.websiteCtyhocn
  ) {
    return {
      score: 98,
      confidence: "high",
      reason: "ctyhocn in census Website URL",
      nameSim: nameSimilarity(directoryRow.name, censusRow.name),
      distanceMeters: null,
    };
  }

  if (
    directoryRow.brandPropertyCode &&
    censusRow.accorPropertyId &&
    directoryRow.brandPropertyCode === censusRow.accorPropertyId
  ) {
    return {
      score: 99,
      confidence: "high",
      reason: "Accor property code in census Website URL",
      nameSim: nameSimilarity(directoryRow.matchName || directoryRow.name, censusRow.name),
      distanceMeters: null,
    };
  }

  const directoryName = directoryRow.matchName || directoryRow.name;
  const nameSim = nameSimilarity(directoryName, censusRow.name);
  const countryOk = countriesMatch(directoryRow.country, censusRow.country);
  const cityResult = citiesMatch(directoryRow.city, censusRow.city);

  const dirCoords = parseCoords(directoryRow.latitude, directoryRow.longitude);
  const distM =
    dirCoords && censusRow.coords ? distanceMeters(dirCoords, censusRow.coords) : null;

  const webHostDir = websiteHost(directoryRow.website);
  const websiteMatch =
    webHostDir && censusRow.websiteHost && webHostDir === censusRow.websiteHost;

  const phoneDir = normalizePhone(directoryRow.phone);
  const phoneMatch = phoneDir && censusRow.phoneNorm && phoneDir === censusRow.phoneNorm;

  let score = Math.round(nameSim * 55);
  if (countryOk && normalizeCountry(directoryRow.country)) score += 10;
  if (cityResult === true) score += 10;
  if (distM != null) {
    if (distM <= 250) score += 25;
    else if (distM <= 750) score += 15;
    else if (distM <= 2000) score += 5;
  }
  if (websiteMatch) {
    score += 20;
    reasons.push("website host match");
  }
  if (phoneMatch) {
    score += 15;
    reasons.push("phone match");
  }

  if (
    directoryRow.source === "marriott_country_sitemap" &&
    directoryCountryPageMatchesCensus(directoryRow, censusRow)
  ) {
    score = Math.min(100, score + 22);
    if (normalizeCountry(censusRow.country)) score = Math.min(100, score + 8);
    reasons.push("marriott country sitemap page");
  }

  score = Math.min(100, score);

  let confidence = "none";
  let reason = "Below match threshold";
  if (websiteMatch && nameSim >= 0.35) {
    confidence = "high";
    reason = "Website host + name similarity";
  } else if (score >= 80 && nameSim >= 0.65) {
    confidence = "high";
    reason = "Strong name + geo/country";
  } else if (score >= 65 && nameSim >= 0.5) {
    confidence = "medium";
    reason = "Moderate name + location signals";
  } else if (score >= 50 && nameSim >= 0.4) {
    confidence = "low";
    reason = "Weak match — needs review";
  } else if (
    directoryRow.source === "marriott_country_sitemap" &&
    directoryCountryPageMatchesCensus(directoryRow, censusRow) &&
    score >= 50 &&
    nameSim >= 0.48
  ) {
    confidence = "low";
    reason = "Marriott country sitemap page + name similarity";
  } else if (
    (directoryRow.source === "accor_sitemap" || directoryRow.scoringProfile === "accor") &&
    countryOk &&
    cityResult === true &&
    distM != null &&
    distM <= 100 &&
    score >= 55 &&
    (nameSim >= 0.35 || accorBrandTokenOverlap(censusRow.name, directoryName))
  ) {
    confidence = "medium";
    reason = "Accor geo-anchored match (city + ≤100m)";
  } else if (
    (directoryRow.source === "accor_sitemap" || directoryRow.scoringProfile === "accor") &&
    countryOk &&
    cityResult === true &&
    distM != null &&
    distM <= 250 &&
    score >= 60 &&
    accorBrandTokenOverlap(censusRow.name, directoryName)
  ) {
    confidence = "medium";
    reason = "Accor brand token + geo match";
  }

  return {
    score,
    confidence,
    reason: reasons.length ? `${reason}; ${reasons.join(", ")}` : reason,
    nameSim,
    distanceMeters: distM != null ? Math.round(distM) : null,
  };
}

/**
 * @param {object[]} directoryRows
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>[]} censusRows
 * @param {{ minConfidence?: string }} [opts]
 */
export function matchDirectoryRowsToCensus(directoryRows, censusRows, opts = {}) {
  const minRank = { high: 3, medium: 2, low: 1, none: 0 };
  const minConfidence = opts.minConfidence || "low";

  /** @type {{ directoryRow: object, censusRow: object, score: number, confidence: string, reason: string, nameSim: number, distanceMeters: number|null }[]} */
  const pairs = [];
  for (const directoryRow of directoryRows) {
    for (const censusRow of censusRows) {
      const scored = scoreDirectoryAgainstCensus(directoryRow, censusRow);
      if (minRank[scored.confidence] < minRank[minConfidence]) continue;
      pairs.push({ directoryRow, censusRow, ...scored });
    }
  }
  pairs.sort((a, b) => b.score - a.score);

  const usedDirectoryCodes = new Set();
  const usedCensusIds = new Set();
  /** @type {typeof pairs} */
  const assigned = [];

  for (const pair of pairs) {
    const code = pair.directoryRow.brandPropertyCode || pair.directoryRow.ctyhocn;
    if (usedDirectoryCodes.has(code) || usedCensusIds.has(pair.censusRow.recordId)) continue;
    usedDirectoryCodes.add(code);
    usedCensusIds.add(pair.censusRow.recordId);
    assigned.push(pair);
  }

  const matches = [];
  for (const directoryRow of directoryRows) {
    const code = directoryRow.brandPropertyCode || directoryRow.ctyhocn;
    const hit = assigned.find(
      (p) => (p.directoryRow.brandPropertyCode || p.directoryRow.ctyhocn) === code
    );
    if (hit) {
      matches.push(hit);
      continue;
    }

    let best = null;
    for (const censusRow of censusRows) {
      const scored = scoreDirectoryAgainstCensus(directoryRow, censusRow);
      if (!best || scored.score > best.score) {
        best = { directoryRow, censusRow, ...scored };
      }
    }

    matches.push({
      directoryRow,
      censusRow: null,
      score: best?.score ?? 0,
      confidence: "none",
      reason: best
        ? `No assignment above ${minConfidence} confidence (best: ${best.reason})`
        : "No census candidates",
      nameSim: best?.nameSim ?? 0,
      distanceMeters: best?.distanceMeters ?? null,
    });
  }

  const unmatchedCensus = censusRows.filter((r) => !usedCensusIds.has(r.recordId));
  const unmatchedDirectory = matches.filter((m) => !m.censusRow);

  return { matches, unmatchedCensus, unmatchedDirectory };
}
