/**
 * Read-only matching: independent census candidates ↔ existing Hotel Census.
 *
 * Phase 2C: report-only. Does not write to Airtable.
 * Does not use STR Number, Property ID, or any STR-derived fields.
 */

import { getPlatformBase } from "../hotel-census/platform-base.js";
import { HOTEL_CENSUS_TABLE } from "../hotel-census/fields.js";

/** Allowed Hotel Census fields for read-only matching (no STR/CoStar). */
export const CENSUS_READ_FIELDS = {
  name: "name",
  city: "city",
  country: "country",
  lat: "Latitude",
  lng: "Longitude",
  website: "Website",
  telephone: "Telephone",
  affiliation: "Affiliation",
  parentCompany: "Parent Company",
  status: "status",
  rooms: "rooms",
};

export const MATCH_CONFIDENCE_LEVELS = ["high", "medium", "low", "none"];

export const MATCH_RECOMMENDED_ACTIONS = {
  LIKELY_EXISTING: "likely_existing",
  POSSIBLE_DUPLICATE_REVIEW: "possible_duplicate_review",
  LIKELY_NEW_CANDIDATE: "likely_new_candidate",
  NEEDS_RESEARCH: "needs_research",
  SKIP_MISSING_NAME: "skip_missing_name",
};

const COUNTRY_ALIASES = {
  do: "dominican republic",
  "dominican republic": "dominican republic",
  "rep dominicana": "dominican republic",
  "republica dominicana": "dominican republic",
  mx: "mexico",
  mexico: "mexico",
  jm: "jamaica",
  jamaica: "jamaica",
  cr: "costa rica",
  "costa rica": "costa rica",
  br: "brazil",
  brazil: "brazil",
  us: "united states",
  "united states": "united states",
  "united states of america": "united states",
};

export function normalizeText(raw) {
  return String(raw ?? "")
    .trim()
    .replace(/\s+/g, " ");
}

export function normalizeKey(raw) {
  return normalizeText(raw)
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "");
}

export function normalizeCountry(raw) {
  const k = normalizeKey(raw);
  if (!k) return "";
  return COUNTRY_ALIASES[k] || k;
}

export function countriesMatch(a, b) {
  const na = normalizeCountry(a);
  const nb = normalizeCountry(b);
  if (!na || !nb) return true;
  return na === nb;
}

export function citiesMatch(a, b) {
  const ca = normalizeKey(a);
  const cb = normalizeKey(b);
  if (!ca || !cb) return null;
  if (ca === cb) return true;
  if (ca.includes(cb) || cb.includes(ca)) return true;
  return false;
}

function tokenSet(text) {
  return new Set(
    normalizeKey(text)
      .split(/\s+/)
      .filter((t) => t.length > 1)
  );
}

/** Token Jaccard similarity 0–1. */
export function nameSimilarity(a, b) {
  const na = normalizeKey(a);
  const nb = normalizeKey(b);
  if (!na || !nb) return 0;
  if (na === nb) return 1;
  if (na.includes(nb) || nb.includes(na)) {
    const ratio = Math.min(na.length, nb.length) / Math.max(na.length, nb.length);
    return Math.max(0.75, ratio);
  }
  const A = tokenSet(a);
  const B = tokenSet(b);
  if (!A.size && !B.size) return 0;
  let inter = 0;
  for (const t of A) {
    if (B.has(t)) inter++;
  }
  const union = A.size + B.size - inter;
  return union ? inter / union : 0;
}

export function parseCoords(lat, lng) {
  const la = Number(lat);
  const lo = Number(lng);
  if (!Number.isFinite(la) || !Number.isFinite(lo)) return null;
  if (la === 0 && lo === 0) return null;
  return { lat: la, lng: lo };
}

/** Haversine distance in meters. */
export function distanceMeters(a, b) {
  if (!a || !b) return null;
  const R = 6371000;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}

export function websiteHost(url) {
  if (!url) return "";
  let s = normalizeText(url);
  if (!s) return "";
  if (!/^https?:\/\//i.test(s)) s = `https://${s}`;
  try {
    const u = new URL(s);
    return normalizeKey(u.hostname.replace(/^www\./, ""));
  } catch {
    return normalizeKey(s.replace(/^www\./, ""));
  }
}

export function normalizePhone(raw) {
  const digits = String(raw ?? "").replace(/\D/g, "");
  if (digits.length < 7) return "";
  return digits.slice(-10);
}

/**
 * Map Airtable record → read-only census row (no STR fields).
 */
export function mapCensusRecord(record) {
  const f = record.fields || {};
  const coords = parseCoords(f[CENSUS_READ_FIELDS.lat], f[CENSUS_READ_FIELDS.lng]);
  return {
    recordId: record.id,
    name: normalizeText(f[CENSUS_READ_FIELDS.name]),
    city: normalizeText(f[CENSUS_READ_FIELDS.city]),
    country: normalizeText(f[CENSUS_READ_FIELDS.country]),
    countryNorm: normalizeCountry(f[CENSUS_READ_FIELDS.country]),
    coords,
    lat: coords?.lat ?? null,
    lng: coords?.lng ?? null,
    website: normalizeText(f[CENSUS_READ_FIELDS.website]),
    telephone: normalizeText(f[CENSUS_READ_FIELDS.telephone]),
    websiteHost: websiteHost(f[CENSUS_READ_FIELDS.website]),
    phoneNorm: normalizePhone(f[CENSUS_READ_FIELDS.telephone]),
    affiliation: normalizeText(f[CENSUS_READ_FIELDS.affiliation]),
    parentCompany: normalizeText(f[CENSUS_READ_FIELDS.parentCompany]),
    status: normalizeText(f[CENSUS_READ_FIELDS.status]),
    rooms: f[CENSUS_READ_FIELDS.rooms],
  };
}

/**
 * Load Hotel Census read-only with allowed fields only.
 * @param {{ countryFilter?: string }} [opts] - optional normalized country to narrow scan
 */
export async function loadHotelCensusReadOnly(opts = {}) {
  const base = getPlatformBase();
  if (!base) {
    throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT for read-only Hotel Census load");
  }

  const selectFields = Object.values(CENSUS_READ_FIELDS);
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({ fields: selectFields, pageSize: 100 })
    .all();

  let rows = records.map(mapCensusRecord);
  const filterNorm = opts.countryFilter ? normalizeCountry(opts.countryFilter) : "";
  if (filterNorm) {
    rows = rows.filter((r) => !r.countryNorm || r.countryNorm === filterNorm);
  }

  const byCountry = new Map();
  for (const row of rows) {
    const ck = row.countryNorm || "(unknown)";
    if (!byCountry.has(ck)) byCountry.set(ck, []);
    byCountry.get(ck).push(row);
  }

  return {
    table: HOTEL_CENSUS_TABLE,
    totalLoaded: records.length,
    rows,
    byCountry,
    readOnly: true,
    fieldsLoaded: selectFields,
  };
}

/**
 * Score one OSM candidate against one census row.
 */
export function scoreCandidateAgainstCensus(candidate, census) {
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

export function assignMatchConfidence(scored) {
  const { nameSim, countryOk, distanceMeters, websiteMatch } = scored;
  if (!countryOk && normalizeCountry(scored.census?.country)) {
    return { confidence: "none", reason: "Country does not match census row" };
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
      reason: `Partial name match (${(nameSim * 100).toFixed(0)}%) within ${distanceMeters}m, same country`,
    };
  }

  if (nameSim >= 0.6 && countryOk && distanceMeters == null) {
    return {
      confidence: "low",
      reason: `Partial name match (${(nameSim * 100).toFixed(0)}%) in same country (no distance)`,
    };
  }

  return { confidence: "none", reason: "No credible name, geo, or website match" };
}

export function assignRecommendedAction(candidate, match, alternates = []) {
  if (!normalizeKey(candidate.rawHotelName)) {
    return MATCH_RECOMMENDED_ACTIONS.SKIP_MISSING_NAME;
  }
  const { confidence } = match;
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
 * Find best census match for one OSM candidate.
 * @param {object} candidate - OSM dry-run candidate
 * @param {{ rows: object[], byCountry: Map }} censusData
 */
export function matchCandidateToCensus(candidate, censusData) {
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
      distanceMeters: null,
      alternateMatchCount: 0,
    };
  }

  const countryNorm = normalizeCountry(candidate.rawCountry);
  let pool = censusData.rows;
  if (countryNorm) {
    pool = censusData.byCountry.get(countryNorm) || [];
    if (!pool.length) pool = censusData.rows;
  }

  const scoredList = pool
    .map((census) => scoreCandidateAgainstCensus(candidate, census))
    .filter((s) => s.nameSim >= 0.35 || s.websiteMatch || s.phoneMatch)
    .sort((a, b) => b.score - a.score);

  if (!scoredList.length) {
    return {
      matchConfidence: "none",
      matchScore: 0,
      matchReason: "No census row with sufficient similarity",
      recommendedAction: MATCH_RECOMMENDED_ACTIONS.LIKELY_NEW_CANDIDATE,
      matchedCensusRecordId: "",
      matchedCensusName: "",
      matchedCensusCity: "",
      matchedCensusCountry: "",
      distanceMeters: null,
      alternateMatchCount: 0,
    };
  }

  const best = scoredList[0];
  const { confidence, reason } = assignMatchConfidence(best);
  const alternates = scoredList.slice(1, 4).filter((s) => s.score >= best.score - 15);
  const recommendedAction = assignRecommendedAction(candidate, { confidence }, alternates);

  return {
    matchConfidence: confidence,
    matchScore: best.score,
    matchReason: reason,
    recommendedAction,
    matchedCensusRecordId: confidence === "none" ? "" : best.census.recordId,
    matchedCensusName: confidence === "none" ? "" : best.census.name,
    matchedCensusCity: confidence === "none" ? "" : best.census.city,
    matchedCensusCountry: confidence === "none" ? "" : best.census.country,
    distanceMeters: best.distanceMeters,
    alternateMatchCount: alternates.length,
    _bestNameSim: best.nameSim,
  };
}

/**
 * Match all OSM candidates; build summary.
 * @param {Array<object>} candidates
 * @param {{ rows, byCountry }} censusData
 */
export function matchAllCandidates(candidates, censusData) {
  const rows = [];
  const summary = {
    totalCandidates: candidates.length,
    high: 0,
    medium: 0,
    low: 0,
    none: 0,
    likely_existing: 0,
    possible_duplicate_review: 0,
    likely_new_candidate: 0,
    needs_research: 0,
    skip_missing_name: 0,
    highMatchDistances: [],
    duplicateRiskNames: new Map(),
  };

  for (const c of candidates) {
    const m = matchCandidateToCensus(c, censusData);
    const row = {
      sourceRecordId: c.sourceRecordId,
      rawHotelName: c.rawHotelName,
      rawCity: c.rawCity,
      rawCountry: c.rawCountry,
      rawLatitude: c.rawLatitude,
      rawLongitude: c.rawLongitude,
      osmTourismTag: c.osmTourismTag || c._osmTourismTag || "",
      candidateDedupeKey: c.candidateDedupeKey,
      matchConfidence: m.matchConfidence,
      matchScore: m.matchScore,
      matchReason: m.matchReason,
      matchedCensusRecordId: m.matchedCensusRecordId,
      matchedCensusName: m.matchedCensusName,
      matchedCensusCity: m.matchedCensusCity,
      matchedCensusCountry: m.matchedCensusCountry,
      distanceMeters: m.distanceMeters,
      recommendedAction: m.recommendedAction,
    };
    rows.push(row);

    summary[m.matchConfidence] = (summary[m.matchConfidence] || 0) + 1;
    summary[m.recommendedAction] = (summary[m.recommendedAction] || 0) + 1;

    if (m.matchConfidence === "high" && m.distanceMeters != null) {
      summary.highMatchDistances.push(m.distanceMeters);
    }
    if (
      m.recommendedAction === MATCH_RECOMMENDED_ACTIONS.POSSIBLE_DUPLICATE_REVIEW ||
      (m.matchConfidence === "high" && m.alternateMatchCount > 0)
    ) {
      const nk = normalizeKey(c.rawHotelName);
      if (nk) summary.duplicateRiskNames.set(nk, (summary.duplicateRiskNames.get(nk) || 0) + 1);
    }
  }

  const dists = summary.highMatchDistances;
  summary.averageDistanceMetersHighMatches = dists.length
    ? Math.round(dists.reduce((a, b) => a + b, 0) / dists.length)
    : null;

  summary.topDuplicateRiskNames = [...summary.duplicateRiskNames.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15)
    .map(([name, count]) => ({ name, count }));

  delete summary.highMatchDistances;
  delete summary.duplicateRiskNames;

  return { rows, summary };
}
