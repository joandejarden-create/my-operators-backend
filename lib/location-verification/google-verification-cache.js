/**
 * Local cache for Google Places pre-import verification (fixture/report only).
 * Never stores API keys or secrets.
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DEFAULT_CACHE_PATH = join(__dirname, "../../data/google-place-verification-cache.json");

function normalizeCacheName(name) {
  return String(name || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {object} candidate
 */
export function buildVerificationCacheKey(candidate) {
  const country = normalizeCacheName(candidate.country || candidate.candidateCountry || "");
  const city = normalizeCacheName(candidate.city || candidate.candidateCity || "");
  const name = normalizeCacheName(
    candidate.name || candidate["Demand Anchor Name"] || candidate.candidateName || ""
  );
  return `${country}|${city}|${name}`;
}

function emptyCacheDocument() {
  return { version: 1, entries: {} };
}

/**
 * @param {string} [cachePath]
 */
export function loadVerificationCache(cachePath = DEFAULT_CACHE_PATH) {
  if (!existsSync(cachePath)) return emptyCacheDocument();
  try {
    const raw = JSON.parse(readFileSync(cachePath, "utf8"));
    if (!raw || typeof raw !== "object") return emptyCacheDocument();
    return {
      version: raw.version || 1,
      entries: raw.entries && typeof raw.entries === "object" ? raw.entries : {},
    };
  } catch {
    return emptyCacheDocument();
  }
}

/**
 * @param {object} cacheDoc
 * @param {string} [cachePath]
 */
export function saveVerificationCache(cacheDoc, cachePath = DEFAULT_CACHE_PATH) {
  const dir = dirname(cachePath);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(cachePath, JSON.stringify(cacheDoc, null, 2) + "\n", "utf8");
}

/**
 * @param {object} verificationResult
 */
export function serializeCacheEntry(verificationResult, queryUsed = "") {
  return {
    verifiedAt: new Date().toISOString(),
    queryUsed: queryUsed || "",
    matchConfidence: verificationResult.matchConfidence || "",
    verificationStatus: verificationResult.verificationStatus || "",
    verificationNotes: verificationResult.verificationNotes || "",
    recommendedName: verificationResult.recommendedName || "",
    recommendedLatitude: verificationResult.recommendedLatitude ?? null,
    recommendedLongitude: verificationResult.recommendedLongitude ?? null,
    googleResultSummary: {
      googleName: verificationResult.googleName || "",
      googleLatitude: verificationResult.googleLatitude ?? null,
      googleLongitude: verificationResult.googleLongitude ?? null,
      googleFormattedAddress: verificationResult.googleFormattedAddress || "",
      googleMapsUri: verificationResult.googleMapsUri || "",
      googleTypes: verificationResult.googleTypes || [],
    },
    result: {
      candidateName: verificationResult.candidateName,
      candidateCity: verificationResult.candidateCity,
      candidateCountry: verificationResult.candidateCountry,
      candidateLatitude: verificationResult.candidateLatitude,
      candidateLongitude: verificationResult.candidateLongitude,
      candidatePointType: verificationResult.candidatePointType,
      googlePlaceId: verificationResult.googlePlaceId || "",
      googleName: verificationResult.googleName || "",
      googleLatitude: verificationResult.googleLatitude ?? null,
      googleLongitude: verificationResult.googleLongitude ?? null,
      googleFormattedAddress: verificationResult.googleFormattedAddress || "",
      googleMapsUri: verificationResult.googleMapsUri || "",
      googleTypes: verificationResult.googleTypes || [],
      verificationStatus: verificationResult.verificationStatus,
      matchConfidence: verificationResult.matchConfidence,
      verificationNotes: verificationResult.verificationNotes,
      recommendedName: verificationResult.recommendedName,
      recommendedLatitude: verificationResult.recommendedLatitude,
      recommendedLongitude: verificationResult.recommendedLongitude,
    },
  };
}

/**
 * @param {object} entry
 */
export function deserializeCacheEntry(entry) {
  if (!entry?.result) return null;
  return { ...entry.result, _cacheHit: true, _cacheVerifiedAt: entry.verifiedAt || "" };
}

export { DEFAULT_CACHE_PATH as GOOGLE_VERIFICATION_CACHE_PATH };
