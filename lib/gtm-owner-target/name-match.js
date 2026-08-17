/**
 * Safer company / owner name matching for GTM footprint linkage.
 * Avoids false positives from generic hospitality phrases (e.g. "Hotel Group").
 */
import { normalizeOwnerKey } from "./normalize.js";

/** Legal suffixes only — excludes "grupo"/"group" (often part of the core name). */
const LEGAL_SUFFIX_RE =
  /\b(sa de cv|s a de c v|sas|s a s|sa|srl|ltda|llc|inc|corp|gmbh|limited|holdings?|plc|ag|bv|nv|lp|llp)\b/g;

/** Minimum length for aligned multi-token phrase matches. */
const MIN_PARTIAL_MATCH_LEN = 8;

/**
 * True Owner names too short/ambiguous for fuzzy partial matching.
 * These only match on exact normalized name (or alias exact), never partial.
 */
export const CALA_PARTIAL_EXACT_ONLY_OWNERS = new Set(
  ["G Hotels", "GR Group", "Grupo DG", "Grupo CC", "Grupo RT"].map(normalizeOwnerKey)
);

/**
 * Tokens too generic to support a partial match on their own.
 * A valid partial match needs ≥1 distinctive (non-generic) shared token.
 */
export const GENERIC_MATCH_TOKENS = new Set([
  "hotel",
  "hotels",
  "resort",
  "resorts",
  "hospitality",
  "group",
  "grupo",
  "management",
  "international",
  "global",
  "holdings",
  "partners",
  "partner",
  "investments",
  "investment",
  "properties",
  "property",
  "real",
  "estate",
  "company",
  "collection",
  "limited",
  "lodging",
  "suites",
  "capital",
  "development",
  "world",
  "worldwide",
  "americas",
  "america",
  "luxury",
  "select",
  "service",
  "services",
  "operator",
  "operators",
  "owner",
  "owners",
  "corporation",
  "enterprises",
  "enterprise",
  "brasil",
  "brazil",
  "tourism",
  "grand",
  "asset",
  "palm",
  "family",
  "first",
  "east",
  "bay",
  "holding",
  "holdings",
  "commercial",
  "community",
  "planet",
  "urban",
  "executive",
  "event",
  "chatrium",
  "companies",
  "residences",
  "administracion",
  "empresarial",
]);

/**
 * @param {string} name
 */
export function normalizeForMatch(name) {
  return normalizeOwnerKey(name).replace(LEGAL_SUFFIX_RE, " ").replace(/\s+/g, " ").trim();
}

/**
 * @param {string} value
 */
function allTokens(value) {
  return String(value || "")
    .split(/\s+/)
    .filter(Boolean);
}

/**
 * @param {string} value
 */
function significantTokens(value) {
  return allTokens(value).filter((token) => token.length >= 4);
}

/**
 * @param {string} token
 */
function isDistinctiveToken(token) {
  return token.length >= 5 && !GENERIC_MATCH_TOKENS.has(token);
}

/**
 * @param {string} value
 */
function hasDistinctiveToken(value) {
  return allTokens(value).some(isDistinctiveToken);
}

/**
 * @param {string} longer
 * @param {string} shorter
 */
function hasAlignedTokenPhrase(longer, shorter) {
  if (!shorter || shorter.length < MIN_PARTIAL_MATCH_LEN) return false;
  if (!hasDistinctiveToken(shorter)) return false;

  const longerTokens = allTokens(longer);
  const shorterTokens = allTokens(shorter);
  if (!shorterTokens.length || shorterTokens.length > longerTokens.length) return false;

  for (let i = 0; i <= longerTokens.length - shorterTokens.length; i++) {
    let aligned = true;
    for (let j = 0; j < shorterTokens.length; j++) {
      if (longerTokens[i + j] !== shorterTokens[j]) {
        aligned = false;
        break;
      }
    }
    if (aligned) return true;
  }
  return false;
}

/**
 * @param {string} shorter
 * @param {string} longer
 */
function hasDistinctiveSharedTokens(shorter, longer) {
  const tokensShorter = significantTokens(shorter);
  const tokensLonger = new Set(significantTokens(longer));
  if (!tokensShorter.length || !tokensLonger.size) return false;

  const shared = tokensShorter.filter((token) => tokensLonger.has(token));
  const distinctiveShared = shared.filter(isDistinctiveToken);

  if (distinctiveShared.length >= 2) return true;
  if (distinctiveShared.length >= 1 && shared.length >= 2) return true;
  return false;
}

/**
 * Token-aware partial match between normalized name strings.
 * @param {string} a
 * @param {string} b
 */
export function isPartialNormalizedMatch(a, b) {
  if (!a || !b || a.length < 4 || b.length < 4) return false;
  if (a === b) return true;

  const [shorter, longer] = a.length <= b.length ? [a, b] : [b, a];

  if (hasAlignedTokenPhrase(longer, shorter)) return true;

  const tokensShorter = significantTokens(shorter);
  const tokensLonger = new Set(significantTokens(longer));
  if (!tokensShorter.length || !tokensLonger.size) return false;

  if (
    tokensShorter.length === 1 &&
    tokensShorter[0] === shorter &&
    tokensLonger.has(tokensShorter[0]) &&
    isDistinctiveToken(tokensShorter[0])
  ) {
    return true;
  }

  return hasDistinctiveSharedTokens(shorter, longer);
}

/**
 * @param {string} ownerKey
 */
export function isExactOnlyOwnerKey(ownerKey) {
  return CALA_PARTIAL_EXACT_ONLY_OWNERS.has(ownerKey);
}

/**
 * Explain why two names partial-match (dev / audit).
 * @param {string} a
 * @param {string} b
 */
export function explainPartialMatch(a, b) {
  if (a === b) return "exact";
  const [shorter, longer] = a.length <= b.length ? [a, b] : [b, a];
  if (hasAlignedTokenPhrase(longer, shorter)) return "aligned_phrase";
  const tokensShorter = significantTokens(shorter);
  const tokensLonger = new Set(significantTokens(longer));
  if (
    tokensShorter.length === 1 &&
    tokensShorter[0] === shorter &&
    tokensLonger.has(tokensShorter[0]) &&
    isDistinctiveToken(tokensShorter[0])
  ) {
    return "single_distinctive_token";
  }
  if (hasDistinctiveSharedTokens(shorter, longer)) return "distinctive_shared_tokens";
  return "none";
}
