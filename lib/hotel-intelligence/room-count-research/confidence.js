/**
 * Transparent room-count confidence model.
 * Factors: officiality × agreement × explicit wording × identity certainty.
 */

import {
  SOURCE_CATEGORIES,
  baseConfidenceForCategory,
} from "./trust.js";

export const ROOM_COUNT_CONFIDENCE_VERSION = "room-count-confidence-v1";

export const RESEARCH_STATUS = Object.freeze({
  NOT_STARTED: "NOT_STARTED",
  SEARCHING: "SEARCHING",
  FOUND_SINGLE_SOURCE: "FOUND_SINGLE_SOURCE",
  FOUND_MULTI_SOURCE: "FOUND_MULTI_SOURCE",
  CONFLICT: "CONFLICT",
  NO_EVIDENCE: "NO_EVIDENCE",
  MANUAL_REVIEW: "MANUAL_REVIEW",
});

/**
 * @param {Array<{
 *   value: number,
 *   source_category: string,
 *   quote?: string|null,
 *   confidence_label?: string,
 *   rejected?: boolean,
 * }>} observations
 * @param {{ identity_confidence?: number }} [ctx]
 */
export function scoreRoomCountResearch(observations, ctx = {}) {
  const usable = (observations || []).filter(
    (o) => o && !o.rejected && Number.isFinite(Number(o.value)) && Number(o.value) > 0
  );

  if (!usable.length) {
    return {
      candidate_room_count: null,
      confidence: 0,
      research_status: RESEARCH_STATUS.NO_EVIDENCE,
      review_required: true,
      supporting_sources: [],
      conflicts: [],
      explanation: "no_explicit_room_count_evidence",
    };
  }

  // Group by value
  const byValue = new Map();
  for (const o of usable) {
    const v = Number(o.value);
    if (!byValue.has(v)) byValue.set(v, []);
    byValue.get(v).push(o);
  }

  const groups = [...byValue.entries()].map(([value, rows]) => {
    const bestCat = rows.reduce((best, r) => {
      const a = baseConfidenceForCategory(r.source_category);
      const b = baseConfidenceForCategory(best.source_category);
      return a >= b ? r : best;
    }, rows[0]);
    let score = baseConfidenceForCategory(bestCat.source_category);
    // Agreement bonus
    if (rows.length >= 3) score += 0.06;
    else if (rows.length === 2) score += 0.04;
    // Explicit quote bonus
    if (rows.some((r) => r.quote && String(r.quote).length > 10)) score += 0.03;
    // Official category bonus
    if (
      [
        SOURCE_CATEGORIES.OFFICIAL_HOTEL,
        SOURCE_CATEGORIES.OFFICIAL_BRAND,
        SOURCE_CATEGORIES.OFFICIAL_OWNER,
      ].includes(bestCat.source_category)
    ) {
      score += 0.02;
    }
    // Identity certainty
    const idc = Number(ctx.identity_confidence);
    if (Number.isFinite(idc)) {
      if (idc >= 0.85) score += 0.02;
      else if (idc < 0.7) score -= 0.08;
    }
    // Weak extractor confidence label
    if (rows.every((r) => r.confidence_label === "Medium")) score -= 0.05;
    if (rows.some((r) => r.confidence_label === "Hold")) score -= 0.15;

    return {
      value,
      rows,
      score: Math.round(Math.min(0.99, Math.max(0, score)) * 100) / 100,
      best_category: bestCat.source_category,
    };
  });

  groups.sort((a, b) => b.score - a.score || b.rows.length - a.rows.length);
  const top = groups[0];
  const conflicts = groups.slice(1).filter((g) => Math.abs(g.value - top.value) >= 3);

  let research_status = RESEARCH_STATUS.FOUND_SINGLE_SOURCE;
  if (top.rows.length >= 2 && conflicts.length === 0) {
    research_status = RESEARCH_STATUS.FOUND_MULTI_SOURCE;
  }
  if (conflicts.length > 0) {
    research_status = RESEARCH_STATUS.CONFLICT;
  }

  let review_required =
    research_status === RESEARCH_STATUS.CONFLICT ||
    top.score < 0.85 ||
    top.best_category === SOURCE_CATEGORIES.NEWS ||
    top.best_category === SOURCE_CATEGORIES.OTHER;

  if (top.score < 0.7) {
    research_status = RESEARCH_STATUS.MANUAL_REVIEW;
    review_required = true;
  }

  return {
    candidate_room_count: top.value,
    confidence: top.score,
    research_status,
    review_required,
    supporting_sources: top.rows.map((r) => ({
      value: r.value,
      source_category: r.source_category,
      url: r.url || null,
      quote: r.quote || null,
      language: r.language || null,
      method: r.method || null,
      observed_at: r.observed_at || null,
    })),
    conflicts: conflicts.map((g) => ({
      value: g.value,
      score: g.score,
      sources: g.rows.length,
      categories: [...new Set(g.rows.map((r) => r.source_category))],
    })),
    explanation: `best=${top.value};sources=${top.rows.length};category=${top.best_category};status=${research_status}`,
  };
}
