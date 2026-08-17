/**
 * Shared Independent Hotel Source Candidate normalization (Phase 2D).
 * Used by OSM, Wikidata, brand directory, and future source adapters.
 */

import {
  REVIEW_STATUS,
  MATCH_CONFIDENCE,
  RECOMMENDED_ACTION,
} from "./fields.js";
import { attachSourcePolicyFlags } from "./source-policy.js";

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

function roundCoord(n, decimals = 4) {
  const x = Number(n);
  if (!Number.isFinite(x)) return "";
  const f = 10 ** decimals;
  return String(Math.round(x * f) / f);
}

/**
 * @param {string} name
 * @param {string} city
 * @param {string} country
 * @param {number|null} lat
 * @param {number|null} lng
 */
export function computeCandidateDedupeKey(name, city, country, lat, lng) {
  const n = normalizeKey(name);
  const c = normalizeKey(city);
  const co = normalizeKey(country);
  if (n && c && co) return `${n}|${c}|${co}`;
  if (n && lat != null && lng != null) {
    return `${n}|${roundCoord(lat)}|${roundCoord(lng)}`;
  }
  if (c && co && lat != null && lng != null) {
    return `${c}|${co}|${roundCoord(lat)}|${roundCoord(lng)}`;
  }
  return n || `unknown|${roundCoord(lat)}|${roundCoord(lng)}`;
}

const MISSING_CHECKS = [
  { key: "missingName", test: (c) => !normalizeKey(c.rawHotelName) },
  { key: "missingCity", test: (c) => !normalizeKey(c.rawCity) },
  { key: "missingCountry", test: (c) => !normalizeKey(c.rawCountry) },
  {
    key: "missingCoordinates",
    test: (c) =>
      !Number.isFinite(c.rawLatitude) || !Number.isFinite(c.rawLongitude),
  },
  { key: "missingWebsite", test: (c) => !normalizeKey(c.rawWebsite) },
  { key: "missingPhone", test: (c) => !normalizeKey(c.rawPhone) },
];

export function analyzeMissingFields(candidate) {
  const flags = {};
  const list = [];
  for (const { key, test } of MISSING_CHECKS) {
    const missing = test(candidate);
    flags[key] = missing;
    if (missing) list.push(key);
  }
  return { flags, list };
}

/**
 * Heuristic quality score 0–100 for filtering and steward prioritization.
 */
export function computeQualityScore(candidate) {
  let score = 0;
  if (normalizeKey(candidate.rawHotelName)) score += 25;
  if (normalizeKey(candidate.rawCity)) score += 15;
  if (normalizeKey(candidate.rawCountry)) score += 10;
  if (Number.isFinite(candidate.rawLatitude) && Number.isFinite(candidate.rawLongitude)) {
    score += 20;
  }
  if (normalizeKey(candidate.rawWebsite)) score += 15;
  if (normalizeKey(candidate.rawPhone)) score += 10;
  if (normalizeKey(candidate.rawBrand)) score += 10;
  if (normalizeKey(candidate.rawAddress)) score += 5;
  return Math.min(100, score);
}

export function qualityTier(score) {
  if (score >= 70) return "high";
  if (score >= 45) return "medium";
  if (score >= 25) return "low";
  return "minimal";
}

/**
 * Build standard candidate record + internal report metadata.
 * @param {object} input
 * @param {string} input.sourceName
 * @param {string} input.sourceType
 * @param {string} input.sourceLicense
 * @param {string} input.sourceUrl
 * @param {string} input.sourceRecordId
 * @param {string} [input.rawHotelName]
 * @param {string} [input.rawAddress]
 * @param {string} [input.rawCity]
 * @param {string} [input.rawCountry]
 * @param {number|null} [input.rawLatitude]
 * @param {number|null} [input.rawLongitude]
 * @param {string} [input.rawWebsite]
 * @param {string} [input.rawPhone]
 * @param {string} [input.rawBrand]
 * @param {object|string} [input.rawPayload]
 * @param {string} input.importBatchId
 * @param {string} [input.importedAt]
 * @param {string} [input.reviewStatus]
 * @param {string} [input.possibleMatchConfidence]
 * @param {string} [input.recommendedAction]
 * @param {object} [input.internalMeta] - source-specific (_osmTourismTag, etc.)
 */
export function buildIndependentCandidate(input) {
  const lat = input.rawLatitude;
  const lng = input.rawLongitude;
  const payload =
    typeof input.rawPayload === "string"
      ? input.rawPayload
      : JSON.stringify(input.rawPayload ?? {});

  const core = {
    sourceName: input.sourceName,
    sourceType: input.sourceType,
    sourceLicense: input.sourceLicense,
    sourceUrl: input.sourceUrl || "",
    sourceRecordId: input.sourceRecordId,
    rawHotelName: normalizeText(input.rawHotelName),
    rawAddress: normalizeText(input.rawAddress),
    rawCity: normalizeText(input.rawCity),
    rawCountry: normalizeText(input.rawCountry),
    rawLatitude: Number.isFinite(lat) ? lat : null,
    rawLongitude: Number.isFinite(lng) ? lng : null,
    rawWebsite: normalizeText(input.rawWebsite),
    rawPhone: normalizeText(input.rawPhone),
    rawBrand: normalizeText(input.rawBrand),
    rawPayloadJson: payload,
    importBatchId: input.importBatchId,
    importedAt: input.importedAt || new Date().toISOString(),
    reviewStatus: input.reviewStatus || REVIEW_STATUS.PENDING,
    possibleMatchConfidence: input.possibleMatchConfidence || MATCH_CONFIDENCE.NONE,
    recommendedAction:
      input.recommendedAction || RECOMMENDED_ACTION.NEEDS_RESEARCH,
    candidateDedupeKey: computeCandidateDedupeKey(
      input.rawHotelName,
      input.rawCity,
      input.rawCountry,
      lat,
      lng
    ),
  };

  const { flags, list } = analyzeMissingFields(core);
  const qualityScore = computeQualityScore(core);
  const withMeta = {
    ...core,
    ...input.internalMeta,
    missingFieldFlags: flags,
    missingFields: list,
    qualityScore,
    qualityTier: qualityTier(qualityScore),
  };

  return attachSourcePolicyFlags(withMeta);
}
