/**
 * Competitor explicit-rank extraction (quality control — not subject Path-A methodology).
 *
 * Distinguishes:
 *   EXPLICIT_RANK | ORDERED_LIST | UNORDERED_MENTION | PROSE_MENTION
 *
 * Does not invent rank when structure does not support it.
 */

import { extractAndResolveCompetitors } from "../intelligence/competitor-name-resolution.js";
import { canonicalizeForProperty } from "./adp-property-entity-registries.js";

export const COMPETITOR_RANK_CLASS = Object.freeze({
  EXPLICIT_RANK: "EXPLICIT_RANK",
  ORDERED_LIST: "ORDERED_LIST",
  UNORDERED_MENTION: "UNORDERED_MENTION",
  PROSE_MENTION: "PROSE_MENTION",
});

const TOP_REC_RE =
  /\b(?:the\s+)?(?:top\s+recommendation|top\s+choice|best\s+overall|first\s+choice|number\s+one|#\s*1)\b[^.!\n]{0,120}?\b(?:is|:\s*)\s*\*{0,2}([^*\n]{4,80}?)\*{0,2}/gi;

const NUMBERED_LINE_RE = /^\s*(\d+)[\.\)]\s+\*{0,2}([^*\n]{4,100}?)\*{0,2}\s*$/gim;

/**
 * Extract competitor ranks from raw response using governed patterns only.
 * Returns [{ rawObservedName, canonicalHotelId, rank, rankClass, confidence }]
 */
export function extractCompetitorExplicitRanks(rawResponse, propertyProfile) {
  const text = String(rawResponse || "");
  if (!text.trim()) return [];
  const propertyId = propertyProfile?.propertyId;
  const out = [];
  const seen = new Set();

  function push(name, rank, rankClass, confidence) {
    const cleaned = String(name || "")
      .replace(/\*\*/g, "")
      .replace(/\s+/g, " ")
      .replace(/[.,;:]+$/, "")
      .trim();
    if (!cleaned || cleaned.length < 4) return;
    const canonicalHotelId = propertyId ? canonicalizeForProperty(propertyId, cleaned) : null;
    if (!canonicalHotelId || seen.has(canonicalHotelId)) return;
    // Must also appear in competitor extract set (avoid random phrases)
    const known = extractAndResolveCompetitors(text, propertyProfile);
    const knownCanon = new Set(
      known.map((n) => canonicalizeForProperty(propertyId, n)).filter(Boolean)
    );
    if (!knownCanon.has(canonicalHotelId)) return;
    seen.add(canonicalHotelId);
    out.push({
      rawObservedName: cleaned,
      canonicalHotelId,
      rank,
      rankClass,
      confidence,
    });
  }

  let m;
  while ((m = TOP_REC_RE.exec(text)) !== null) {
    push(m[1], 1, COMPETITOR_RANK_CLASS.EXPLICIT_RANK, "high");
  }

  while ((m = NUMBERED_LINE_RE.exec(text)) !== null) {
    const rank = Number(m[1]);
    if (!Number.isFinite(rank) || rank < 1 || rank > 10) continue;
    push(m[2], rank, COMPETITOR_RANK_CLASS.ORDERED_LIST, "medium");
  }

  return out.sort((a, b) => a.rank - b.rank);
}
