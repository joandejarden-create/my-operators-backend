import { evaluatePlaceMatchConfidence, nameSimilarity } from "./place-match-confidence.js";

const GOOGLE_ENDPOINT = "https://places.googleapis.com/v1/places:searchText";
const GOOGLE_DETAILS_BASE = "https://places.googleapis.com/v1/";
const FIELD_MASK = [
  "places.id",
  "places.displayName",
  "places.formattedAddress",
  "places.location",
  "places.types",
  "places.businessStatus",
  "places.googleMapsUri",
].join(",");

/** @deprecated import from google-api-config.js */
export function resolveGoogleApiKey() {
  const places = String(process.env.GOOGLE_PLACES_API_KEY || "").trim();
  const maps = String(process.env.GOOGLE_MAPS_API_KEY || "").trim();
  return places || maps || "";
}

function toGoogleCandidate(row) {
  return {
    candidateName: row.name || row["Demand Anchor Name"] || "",
    candidateCity: row.city || row.City || "",
    candidateCountry: row.country || row.Country || "",
    candidateLatitude: Number.isFinite(Number(row.latitude ?? row.Latitude))
      ? Number(row.latitude ?? row.Latitude)
      : null,
    candidateLongitude: Number.isFinite(Number(row.longitude ?? row.Longitude))
      ? Number(row.longitude ?? row.Longitude)
      : null,
    candidatePointType: row.pointType || row["Point Type"] || "",
    sourceRow: row,
  };
}

function normalizePlaceFromSearch(place) {
  const loc = place?.location || {};
  return {
    googlePlaceId: place?.id || "",
    googleName: place?.displayName?.text || "",
    googleLatitude: Number.isFinite(Number(loc.latitude)) ? Number(loc.latitude) : null,
    googleLongitude: Number.isFinite(Number(loc.longitude)) ? Number(loc.longitude) : null,
    googleFormattedAddress: place?.formattedAddress || "",
    googleMapsUri: place?.googleMapsUri || "",
    googleTypes: Array.isArray(place?.types) ? place.types : [],
    googleBusinessStatus: place?.businessStatus || "",
    raw: place,
  };
}

export async function googleTextSearch(query, apiKey, maxResults = 5) {
  const res = await fetch(GOOGLE_ENDPOINT, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Goog-Api-Key": apiKey,
      "X-Goog-FieldMask": FIELD_MASK,
    },
    body: JSON.stringify({
      textQuery: query,
      pageSize: Math.max(1, Math.min(20, Number(maxResults) || 5)),
    }),
  });
  if (!res.ok) throw new Error(`Google Text Search HTTP ${res.status}`);
  const data = await res.json();
  const places = (data.places || []).map(normalizePlaceFromSearch);
  const limit = Math.max(1, Number(maxResults) || 5);
  return places.slice(0, limit);
}

export async function googlePlaceDetails(placeId, apiKey) {
  if (!placeId) return null;
  const url = `${GOOGLE_DETAILS_BASE}${placeId}`;
  const res = await fetch(url, {
    headers: {
      "X-Goog-Api-Key": apiKey,
      "X-Goog-FieldMask": FIELD_MASK.replace(/^places\./g, ""),
    },
  });
  if (!res.ok) throw new Error(`Google Place Details HTTP ${res.status}`);
  const data = await res.json();
  return normalizePlaceFromSearch(data);
}

function buildSearchQueries(candidate) {
  const row = candidate.sourceRow || {};
  const override = String(row.googleSearchQuery || "").trim();
  if (override) {
    const city = String(candidate.candidateCity || "").trim();
    const country = String(candidate.candidateCountry || "").trim();
    const queries = [override];
    if (city && country && !override.toLowerCase().includes(city.toLowerCase())) {
      queries.push(`${override}, ${city}, ${country}`);
    }
    return [...new Set(queries)];
  }

  const name = String(candidate.candidateName || "").trim();
  const city = String(candidate.candidateCity || "").trim();
  const country = String(candidate.candidateCountry || "").trim();
  const queries = [];
  if (name && city && country) queries.push(`${name}, ${city}, ${country}`);
  if (name && country) queries.push(`${name}, ${country}`);
  if (name) queries.push(name);
  return [...new Set(queries)];
}

function pickBestGooglePlace(candidate, hits) {
  if (!hits?.length) return null;
  let best = hits[0];
  let bestScore = -1;
  for (const hit of hits) {
    const score = nameSimilarity(candidate.candidateName, hit.googleName || "");
    if (score > bestScore) {
      bestScore = score;
      best = hit;
    }
  }
  return best;
}

function resolveRecommendedName(candidate, best, confidence) {
  const candidateName = candidate.candidateName || "";
  const googleName = best?.googleName || "";
  if (confidence.verificationStatus !== "Verified" || confidence.matchConfidence !== "High") {
    return candidateName;
  }
  if (!googleName) return candidateName;
  if (nameSimilarity(candidateName, googleName) >= 0.92) return candidateName;
  return googleName;
}

function copyGovernanceFields(candidate, out) {
  const keys = [
    "scopeLevel",
    "relevanceTier",
    "useCaseTags",
    "defaultMapVisibility",
    "externalVisibilityLevel",
    "projectRelevanceLogic",
    "dealSpecificNotes",
    "Scope Level",
    "Relevance Tier",
    "Use Case Tags",
    "Default Map Visibility",
    "External Visibility Level",
    "Project Relevance Logic",
    "Deal-Specific Notes",
  ];
  for (const key of keys) {
    if (candidate[key] != null && candidate[key] !== "") out[key] = candidate[key];
  }
}

export async function verifyCandidateWithGoogle(candidateRow, options = {}) {
  const apiKey = options.apiKey || resolveGoogleApiKey();
  const maxResults = options.maxResults ?? 5;
  const searchFn =
    options.searchTextFn ||
    ((q) => googleTextSearch(q, apiKey, maxResults));
  const detailsFn = options.placeDetailsFn || ((id) => googlePlaceDetails(id, apiKey));

  const c = toGoogleCandidate(candidateRow);
  if (!apiKey && !options.searchTextFn) {
    return {
      ...c,
      verificationStatus: "No Match",
      matchConfidence: "Low",
      verificationNotes: "Google API key missing.",
      recommendedName: c.candidateName,
      recommendedLatitude: c.candidateLatitude,
      recommendedLongitude: c.candidateLongitude,
    };
  }

  let best = null;
  let allCandidates = [];
  let queryUsed = "";
  for (const q of buildSearchQueries(c)) {
    queryUsed = q;
    const hits = await searchFn(q);
    if (hits.length) {
      allCandidates = hits;
      best = pickBestGooglePlace(c, hits);
      break;
    }
  }

  if (best?.googlePlaceId && (!best.googleLatitude || !best.googleFormattedAddress)) {
    try {
      const details = await detailsFn(best.googlePlaceId);
      if (details) best = details;
    } catch {
      // Keep best search result.
    }
  }

  const confidence = evaluatePlaceMatchConfidence({
    candidateName: c.candidateName,
    candidateCity: c.candidateCity,
    candidateCountry: c.candidateCountry,
    candidateLatitude: c.candidateLatitude,
    candidateLongitude: c.candidateLongitude,
    candidatePointType: c.candidatePointType,
    result: best,
    competingResults: allCandidates.filter((hit) => hit !== best),
  });

  const recommendedName = resolveRecommendedName(c, best, confidence);
  const recommendedLatitude =
    confidence.verificationStatus === "Verified" && Number.isFinite(best?.googleLatitude)
      ? best.googleLatitude
      : c.candidateLatitude;
  const recommendedLongitude =
    confidence.verificationStatus === "Verified" && Number.isFinite(best?.googleLongitude)
      ? best.googleLongitude
      : c.candidateLongitude;

  return {
    ...c,
    queryUsed,
    googlePlaceId: best?.googlePlaceId || "",
    googleName: best?.googleName || "",
    googleLatitude: best?.googleLatitude ?? null,
    googleLongitude: best?.googleLongitude ?? null,
    googleFormattedAddress: best?.googleFormattedAddress || "",
    googleMapsUri: best?.googleMapsUri || "",
    googleTypes: best?.googleTypes || [],
    businessStatus: best?.googleBusinessStatus || "",
    verificationStatus: confidence.verificationStatus,
    matchConfidence: confidence.matchConfidence,
    verificationNotes: confidence.verificationNotes,
    recommendedName,
    recommendedLatitude,
    recommendedLongitude,
  };
}

export function buildVerifiedCleanPoint(candidate, verification, options = {}) {
  const allowMedium = options.allowMedium === true;
  const notes = String(candidate.notes || candidate.Notes || "");
  const manualVerified =
    candidate.manuallyVerified === true ||
    (/manually verified|manual override|manual review approved/i.test(notes) &&
      ["High", "Medium"].includes(String(candidate.dataConfidence || "")) &&
      String(candidate.sourceReference || candidate["Source URL / Reference"] || "").trim());

  const isHighVerified =
    verification.verificationStatus === "Verified" && verification.matchConfidence === "High";
  const isMediumAllowed =
    allowMedium &&
    verification.matchConfidence === "Medium" &&
    verification.verificationStatus !== "Ambiguous Match" &&
    verification.verificationStatus !== "No Match";

  const include = isHighVerified || isMediumAllowed || manualVerified;
  if (!include) return null;

  const cleanName =
    verification.recommendedName ||
    candidate.name ||
    candidate["Demand Anchor Name"] ||
    "";

  let verificationNote = "";
  if (manualVerified && !isHighVerified) {
    verificationNote = "";
  } else if (isMediumAllowed && !manualVerified) {
    verificationNote =
      "Google Maps verification returned medium confidence; manually reviewed before import.";
  } else {
    verificationNote = "Location verified against Google Maps during pre-import QA.";
  }

  const useCandidateCoords = candidate.manuallyVerified === true;
  const out = {
    name: manualVerified ? candidate.name || candidate["Demand Anchor Name"] || cleanName : cleanName,
    pointType: candidate.pointType || candidate["Point Type"],
    city: candidate.city || candidate.City,
    country: candidate.country || candidate.Country,
    region: candidate.region || candidate.Region,
    submarket: candidate.submarket || candidate.Submarket,
    latitude: useCandidateCoords
      ? candidate.latitude ?? candidate.Latitude
      : Number.isFinite(verification.recommendedLatitude)
        ? verification.recommendedLatitude
        : candidate.latitude ?? candidate.Latitude,
    longitude: useCandidateCoords
      ? candidate.longitude ?? candidate.Longitude
      : Number.isFinite(verification.recommendedLongitude)
        ? verification.recommendedLongitude
        : candidate.longitude ?? candidate.Longitude,
    source: candidate.source || "Public Source",
    sourceReference:
      candidate.sourceReference || candidate["Source URL / Reference"] || candidate.sourceUrl || "",
    dataConfidence: candidate.dataConfidence || candidate["Data Confidence"] || "Medium",
    notes: `${notes ? notes.trim() + " " : ""}${verificationNote}`.trim(),
  };

  copyGovernanceFields(candidate, out);
  return out;
}
