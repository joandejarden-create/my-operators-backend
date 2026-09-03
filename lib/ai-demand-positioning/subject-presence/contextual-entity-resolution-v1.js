/**
 * Contextual entity resolution — NOT global aliases.
 * Short forms may resolve to a canonical property only with strong same-observation location evidence.
 *
 * Founder 2026-08-21: "Renaissance NYC" is REJECTED as a standalone alias.
 * It may resolve to Renaissance New York Times Square only with Times Square–anchored context
 * in the same observation, tied to the hotel mention.
 */

import {
  normalizeSubjectHaystack,
  findTokenBoundaryIndex,
} from "../execution/response-parser.js";

export const CONTEXTUAL_ENTITY_RESOLUTION_VERSION = "adp_contextual_entity_resolution_v1";

const RTS_ID = "adp_renaissance_times_square";
const CANONICAL_RTS = "Renaissance New York Times Square";
const SHORT_FORM = "renaissance nyc";

/** Collision / exclusion patterns (normalized haystack). */
const HARD_ABSENT = [
  /\bdesign\s+renaissance\b/,
  /\brenaissance\s+new\s+york\s+midtown\b/,
  /\brenaissance\s+nyc\s+midtown\b/,
  /\brenaissance\s+downtown\b/,
];

/**
 * Strong Times Square location evidence near a short-form mention.
 * Requires location-bearing phrases — bare city "New York" is not enough.
 */
export function hasStrongTimesSquareContextNear(haystack, mentionIndex, mentionLen) {
  const h = String(haystack || "");
  const start = Math.max(0, mentionIndex - 100);
  const end = Math.min(h.length, mentionIndex + mentionLen + 140);
  const window = h.slice(start, end);

  if (/times\s+square/.test(window)) return true;
  if (/blocks?\s+from\s+ts\b/.test(window)) return true;
  if (/\bfrom\s+ts\b/.test(window) && /blocks?|near|heart|at|in\b/.test(window)) return true;
  if (/\b2\s+blocks?\s+from\s+ts\b/.test(window)) return true;
  if (/\bts\b/.test(window) && /blocks?\s+from/.test(window)) return true;
  if (/heart\s+of\s+ts\b/.test(window)) return true;
  if (/\bat\s+two\s+times\s+square\b/.test(window)) return true;
  return false;
}

/**
 * Try contextual resolution for RTS short-form "Renaissance NYC".
 * @returns {{ mentioned: true, matchedVariant, matchReason, context, position } | null}
 */
export function tryContextualRenaissanceNycToRts(rawResponse, propertyProfile) {
  if (propertyProfile?.propertyId !== RTS_ID) return null;
  const raw = String(rawResponse || "");
  if (!raw.trim()) return null;

  const haystack = normalizeSubjectHaystack(raw);

  for (const re of HARD_ABSENT) {
    if (re.test(haystack)) {
      // Midtown / design noun present — contextual short-form must not override
      // (approved Times Square aliases still win via normal detect path first)
      return null;
    }
  }

  // Bare brand / Renaissance New York without NYC short-form — not this resolver
  const idx = findTokenBoundaryIndex(haystack, SHORT_FORM);
  if (idx === -1) return null;

  // If "renaissance nyc times square" already present, normal alias path should have hit first
  if (findTokenBoundaryIndex(haystack, "renaissance nyc times square") !== -1) {
    return null;
  }

  if (!hasStrongTimesSquareContextNear(haystack, idx, SHORT_FORM.length)) {
    return null;
  }

  // Prefer raw context slice around "Renaissance NYC"
  const lower = raw.toLowerCase();
  const rawIdx = lower.search(/\brenaissance\s+nyc\b/);
  const useIdx = rawIdx >= 0 ? rawIdx : 0;
  const contextStart = Math.max(0, useIdx - 50);
  const contextEnd = Math.min(raw.length, useIdx + 80);
  const context = raw.slice(contextStart, contextEnd).trim();

  return {
    mentioned: true,
    matchedVariant: CANONICAL_RTS,
    matchReason: "CONTEXTUAL_ENTITY_RESOLUTION:Renaissance NYC→RTS",
    context,
    position: null,
    contextualResolution: {
      version: CONTEXTUAL_ENTITY_RESOLUTION_VERSION,
      shortForm: "Renaissance NYC",
      resolvedCanonical: CANONICAL_RTS,
      propertyId: RTS_ID,
      standaloneAlias: false,
    },
  };
}

/**
 * Hook after primary alias/variant detection fails.
 */
export function tryContextualSubjectResolution(rawResponse, propertyProfile) {
  return tryContextualRenaissanceNycToRts(rawResponse, propertyProfile);
}
