/**
 * Read-only match: OSM / intake candidates ↔ Hotel Property Census (production SoT).
 *
 * NEVER uses legacy Hotel Census. Deduping for independent / known-brand intake
 * must only consult Hotel Property Census (tbl9aY5ijiuIzzWam).
 */

import {
  resolvePat,
  resolveTargetBase,
} from "../research-engine-v2/production-census-schema-create.js";
import {
  TABLE_IDS,
} from "../research-engine-v2/production-census-write.js";
import {
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../research-engine-v2/production-census-source-of-truth.js";
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
  normalizePhone,
  MATCH_RECOMMENDED_ACTIONS,
  MATCH_CONFIDENCE_LEVELS,
} from "./match-current-census.js";

export const HPC_MATCH_VERSION = "hotel-property-census-match-v1";

/** Production Census fields used for dedupe (read-only). */
export const HPC_READ_FIELDS = Object.freeze([
  "Property Name",
  "Canonical Property Name",
  "Property Identity Key",
  "City",
  "State / Region",
  "Country",
  "Latitude",
  "Longitude",
  "Official Property URL",
  "Source URL",
  "Phone",
  "Current Brand",
  "Affiliation Status",
]);

export { MATCH_RECOMMENDED_ACTIONS, MATCH_CONFIDENCE_LEVELS };

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function airtableListAll(baseId, token, tableId, fields) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(
        `Hotel Property Census list ${res.status}: ${JSON.stringify(json.error || json)}`
      );
    }
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

function mapHpcRecord(rec) {
  const f = rec.fields || {};
  const name = normalizeText(f["Property Name"] || f["Canonical Property Name"]);
  const website =
    f["Official Property URL"] || f["Source URL"] || "";
  return {
    id: rec.id,
    name,
    city: normalizeText(f.City),
    country: normalizeText(f.Country),
    countryNorm: normalizeCountry(f.Country),
    coords: parseCoords(f.Latitude, f.Longitude),
    websiteHost: websiteHost(website),
    phoneNorm: normalizePhone(f.Phone),
    identityKey: normalizeText(f["Property Identity Key"]),
    currentBrand: normalizeText(f["Current Brand"]),
    affiliationStatus: normalizeText(f["Affiliation Status"]),
  };
}

/**
 * Load Hotel Property Census only (production). Forbidden: legacy Hotel Census.
 * @param {{ countryFilter?: string }} [opts]
 */
export async function loadHotelPropertyCensusReadOnly(opts = {}) {
  const token = resolvePat();
  const bases = resolveTargetBase();
  const baseId = bases?.target_base_id;
  const tableId =
    productionHotelPropertyCensus.tableId ||
    TABLE_IDS["Hotel Property Census"] ||
    PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

  if (!token || !baseId) {
    throw new Error(
      "Missing Airtable credentials for Hotel Property Census read (AIRTABLE_API_KEY + AIRTABLE_BASE_ID_ALT)"
    );
  }
  if (tableId !== PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID) {
    throw new Error(
      `Refusing non-production census table id: ${tableId} (expected ${PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID})`
    );
  }

  const records = await airtableListAll(baseId, token, tableId, [...HPC_READ_FIELDS]);
  let rows = records.map(mapHpcRecord);

  const filterNorm = opts.countryFilter ? normalizeCountry(opts.countryFilter) : "";
  if (filterNorm) {
    rows = rows.filter((r) => !r.countryNorm || r.countryNorm === filterNorm);
  }

  const byCountry = new Map();
  const byIdentityKey = new Map();
  for (const row of rows) {
    const ck = row.countryNorm || "(unknown)";
    if (!byCountry.has(ck)) byCountry.set(ck, []);
    byCountry.get(ck).push(row);
    if (row.identityKey) byIdentityKey.set(row.identityKey.toLowerCase(), row);
  }

  return {
    table: productionHotelPropertyCensus.tableName,
    tableId,
    baseName: productionHotelPropertyCensus.baseName,
    legacy_hotel_census_used: false,
    totalLoaded: records.length,
    rows,
    byCountry,
    byIdentityKey,
    readOnly: true,
    fieldsLoaded: [...HPC_READ_FIELDS],
  };
}

function scoreAgainstHpc(candidate, census) {
  const nameSim = nameSimilarity(candidate.rawHotelName, census.name);
  const countryOk = countriesMatch(candidate.rawCountry, census.country);
  const cityResult = citiesMatch(candidate.rawCity, census.city);
  const osmCoords = parseCoords(candidate.rawLatitude, candidate.rawLongitude);
  const distM =
    osmCoords && census.coords ? distanceMeters(osmCoords, census.coords) : null;
  const webHostOsm = websiteHost(candidate.rawWebsite);
  const websiteMatch =
    webHostOsm && census.websiteHost && webHostOsm === census.websiteHost;
  const phoneOsm = normalizePhone(candidate.rawPhone);
  const phoneMatch = phoneOsm && census.phoneNorm && phoneOsm === census.phoneNorm;

  let score = Math.round(nameSim * 55);
  if (countryOk && normalizeCountry(candidate.rawCountry)) score += 10;
  if (cityResult === true) score += 10;
  if (distM != null) {
    if (distM <= 250) score += 25;
    else if (distM <= 750) score += 15;
    else if (distM <= 2000) score += 5;
  }
  if (websiteMatch) score += 20;
  if (phoneMatch) score += 15;
  score = Math.min(100, score);

  return {
    census,
    nameSim,
    countryOk,
    cityMatch: cityResult,
    distanceMeters: distM != null ? Math.round(distM) : null,
    websiteMatch,
    phoneMatch,
    score,
  };
}

function assignConfidence(scored) {
  const { nameSim, countryOk, distanceMeters, websiteMatch, census } = scored;
  if (!countryOk && normalizeCountry(census?.country)) {
    return { confidence: "none", reason: "Country does not match Hotel Property Census row" };
  }
  if (websiteMatch && nameSim >= 0.4) {
    return {
      confidence: "medium",
      reason: "Exact website host match with partial name similarity",
    };
  }
  if (nameSim >= 0.85 && distanceMeters != null && distanceMeters <= 250) {
    return {
      confidence: "high",
      reason: `Strong name match (${(nameSim * 100).toFixed(0)}%) and ${distanceMeters}m apart`,
    };
  }
  if (nameSim >= 0.7 && distanceMeters != null && distanceMeters <= 750) {
    return {
      confidence: "medium",
      reason: `Good name match (${(nameSim * 100).toFixed(0)}%) and ${distanceMeters}m apart`,
    };
  }
  if (nameSim >= 0.5 && distanceMeters != null && distanceMeters <= 2000) {
    return {
      confidence: "low",
      reason: `Partial name match (${(nameSim * 100).toFixed(0)}%) within ${distanceMeters}m`,
    };
  }
  if (nameSim >= 0.6 && countryOk && distanceMeters == null) {
    return {
      confidence: "low",
      reason: `Partial name match (${(nameSim * 100).toFixed(0)}%) same country (no distance)`,
    };
  }
  return { confidence: "none", reason: "No credible name, geo, or website match" };
}

function assignAction(candidate, confidence, alternates) {
  if (!normalizeKey(candidate.rawHotelName)) {
    return MATCH_RECOMMENDED_ACTIONS.SKIP_MISSING_NAME;
  }
  const altStrong = alternates.filter(
    (a) => a.nameSim >= 0.65 && (a.distanceMeters == null || a.distanceMeters <= 750)
  );
  if (confidence === "high") {
    if (altStrong.length > 0) return MATCH_RECOMMENDED_ACTIONS.POSSIBLE_DUPLICATE_REVIEW;
    return MATCH_RECOMMENDED_ACTIONS.LIKELY_EXISTING;
  }
  if (confidence === "medium") {
    if (altStrong.length > 0) return MATCH_RECOMMENDED_ACTIONS.POSSIBLE_DUPLICATE_REVIEW;
    return MATCH_RECOMMENDED_ACTIONS.LIKELY_EXISTING;
  }
  if (confidence === "low") return MATCH_RECOMMENDED_ACTIONS.NEEDS_RESEARCH;
  return MATCH_RECOMMENDED_ACTIONS.LIKELY_NEW_CANDIDATE;
}

/**
 * @param {object} candidate
 * @param {Awaited<ReturnType<typeof loadHotelPropertyCensusReadOnly>>} censusData
 * @param {{ proposedIdentityKey?: string }} [opts]
 */
export function matchCandidateToHotelPropertyCensus(candidate, censusData, opts = {}) {
  if (!normalizeKey(candidate.rawHotelName)) {
    return {
      matchConfidence: "none",
      matchScore: 0,
      matchReason: "Missing hotel name",
      recommendedAction: MATCH_RECOMMENDED_ACTIONS.SKIP_MISSING_NAME,
      matchedCensusRecordId: "",
      matchedCensusName: "",
      matchedCensusCity: "",
      matchedCensusCountry: "",
      matchedIdentityKey: "",
      matchedCurrentBrand: "",
      matchedAffiliationStatus: "",
      distanceMeters: null,
      identityKeyCollision: false,
    };
  }

  const proposedKey = String(opts.proposedIdentityKey || "").toLowerCase();
  if (proposedKey && censusData.byIdentityKey.has(proposedKey)) {
    const hit = censusData.byIdentityKey.get(proposedKey);
    return {
      matchConfidence: "high",
      matchScore: 100,
      matchReason: "Property Identity Key already exists in Hotel Property Census",
      recommendedAction: MATCH_RECOMMENDED_ACTIONS.LIKELY_EXISTING,
      matchedCensusRecordId: hit.id,
      matchedCensusName: hit.name,
      matchedCensusCity: hit.city,
      matchedCensusCountry: hit.country,
      matchedIdentityKey: hit.identityKey,
      matchedCurrentBrand: hit.currentBrand,
      matchedAffiliationStatus: hit.affiliationStatus,
      distanceMeters: null,
      identityKeyCollision: true,
    };
  }

  const countryNorm = normalizeCountry(candidate.rawCountry);
  let pool = censusData.rows;
  if (countryNorm) {
    pool = censusData.byCountry.get(countryNorm) || [];
    // If no country rows yet (e.g. DR empty in production), fall back to full pool
    // so we still catch cross-country misfiles; empty pool → likely new.
    if (!pool.length) pool = [];
  }

  const scored = pool
    .map((row) => scoreAgainstHpc(candidate, row))
    .filter((s) => s.score > 0)
    .sort((a, b) => b.score - a.score);

  const best = scored[0];
  if (!best) {
    return {
      matchConfidence: "none",
      matchScore: 0,
      matchReason: pool.length
        ? "No credible match in Hotel Property Census country pool"
        : "No Hotel Property Census rows in country filter (treat as new pending global check)",
      recommendedAction: MATCH_RECOMMENDED_ACTIONS.LIKELY_NEW_CANDIDATE,
      matchedCensusRecordId: "",
      matchedCensusName: "",
      matchedCensusCity: "",
      matchedCensusCountry: "",
      matchedIdentityKey: "",
      matchedCurrentBrand: "",
      matchedAffiliationStatus: "",
      distanceMeters: null,
      identityKeyCollision: false,
      productionCensusCountryPoolSize: pool.length,
    };
  }

  // Also score against global pool if country pool was empty of good matches
  // and candidate country filter emptied the pool — already handled.

  const conf = assignConfidence(best);
  const alternates = scored.slice(1, 4);
  const action = assignAction(candidate, conf.confidence, alternates);

  return {
    matchConfidence: conf.confidence,
    matchScore: best.score,
    matchReason: conf.reason,
    recommendedAction: action,
    matchedCensusRecordId: best.census.id,
    matchedCensusName: best.census.name,
    matchedCensusCity: best.census.city,
    matchedCensusCountry: best.census.country,
    matchedIdentityKey: best.census.identityKey,
    matchedCurrentBrand: best.census.currentBrand,
    matchedAffiliationStatus: best.census.affiliationStatus,
    distanceMeters: best.distanceMeters,
    identityKeyCollision: false,
    productionCensusCountryPoolSize: pool.length,
  };
}

/**
 * @param {object[]} candidates
 * @param {Awaited<ReturnType<typeof loadHotelPropertyCensusReadOnly>>} censusData
 * @param {{ identityKeyFn?: (c: object) => string }} [opts]
 */
export function matchAllCandidatesToHotelPropertyCensus(candidates, censusData, opts = {}) {
  const rows = [];
  const summary = {
    totalCandidates: candidates.length,
    productionCensusTotal: censusData.totalLoaded,
    productionCensusMatchingPool: censusData.rows.length,
    high: 0,
    medium: 0,
    low: 0,
    none: 0,
    likely_existing: 0,
    possible_duplicate_review: 0,
    likely_new_candidate: 0,
    needs_research: 0,
    skip_missing_name: 0,
    identity_key_collisions: 0,
    averageDistanceMetersHighMatches: null,
  };
  const highDists = [];

  for (const c of candidates) {
    const key = opts.identityKeyFn ? opts.identityKeyFn(c) : "";
    const m = matchCandidateToHotelPropertyCensus(c, censusData, {
      proposedIdentityKey: key,
    });
    summary[m.matchConfidence] = (summary[m.matchConfidence] || 0) + 1;
    summary[m.recommendedAction] = (summary[m.recommendedAction] || 0) + 1;
    if (m.identityKeyCollision) summary.identity_key_collisions += 1;
    if (m.matchConfidence === "high" && m.distanceMeters != null) {
      highDists.push(m.distanceMeters);
    }
    rows.push({
      sourceRecordId: c.sourceRecordId || "",
      rawHotelName: c.rawHotelName || "",
      rawCity: c.rawCity || "",
      rawCountry: c.rawCountry || "",
      rawLatitude: c.rawLatitude ?? "",
      rawLongitude: c.rawLongitude ?? "",
      rawWebsite: c.rawWebsite || "",
      proposedIdentityKey: key,
      ...m,
    });
  }

  if (highDists.length) {
    summary.averageDistanceMetersHighMatches = Math.round(
      highDists.reduce((a, b) => a + b, 0) / highDists.length
    );
  }

  return { rows, summary };
}
