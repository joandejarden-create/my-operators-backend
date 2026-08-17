/**
 * Independent metric recomputation for AI Intelligence validation.
 * Explicit audit loops — does not call production calculateVisibilityMetrics /
 * brand-read-service aggregation. Formula intent mirrors governed contracts /
 * metrics.js definitions (duplicated here for independence).
 */

import { isPositiveRecommendationRole } from "../metrics.js";
import { METRIC_CONTRACTS } from "./metric-contracts.js";
import { METRIC_VALIDATION_STATE } from "./validation-status.js";

export const INDEPENDENT_RECOMPUTE_VERSION = "ai_intelligence_independent_recompute_v1";

const RATE_TOLERANCE = 1e-9;

function mentionEntityId(m) {
  return m?.canonicalEntityId || m?.entityId || m?.resolvedEntityId || null;
}

function safeDiv(n, d) {
  if (!d) return null;
  return n / d;
}

/**
 * Independently rebuild observation rows from stored evidence mentions.
 * Mirrors buildObservationFromExtractions sorting/dedupe rules without importing it.
 * @param {object[]} evidenceRows
 */
export function auditBuildObservationsFromEvidence(evidenceRows) {
  const observations = [];
  for (const row of evidenceRows || []) {
    const mentions = Array.isArray(row.mentions) ? row.mentions : [];
    const presentEntityIds = [
      ...new Set(mentions.filter((m) => m.canonicalEntityId).map((m) => m.canonicalEntityId)),
    ];

    const recommendedMentions = mentions
      .filter((m) => {
        const id = mentionEntityId(m);
        if (!id) return false;
        if (isPositiveRecommendationRole(m.role)) return true;
        return m.explicitRecommendation === true && (m.role == null || m.role === "");
      })
      .slice()
      .sort((a, b) => {
        const ap = a.recommendationPosition ?? Number.MAX_SAFE_INTEGER;
        const bp = b.recommendationPosition ?? Number.MAX_SAFE_INTEGER;
        if (ap !== bp) return ap - bp;
        if (a.role === "first_recommendation" && b.role !== "first_recommendation") return -1;
        if (b.role === "first_recommendation" && a.role !== "first_recommendation") return 1;
        return (a.mentionPosition ?? 0) - (b.mentionPosition ?? 0);
      });

    const recommendedEntityIds = [];
    const seen = new Set();
    for (const m of recommendedMentions) {
      const id = mentionEntityId(m);
      if (!id || seen.has(id)) continue;
      seen.add(id);
      recommendedEntityIds.push(id);
    }

    const top3RecommendedEntityIds = [];
    const seenTop3 = new Set();
    for (const m of recommendedMentions) {
      const id = mentionEntityId(m);
      if (!id || seenTop3.has(id)) continue;
      const pos = m.recommendationPosition;
      const isFirst = m.role === "first_recommendation" || pos === 1;
      const inTop3 =
        isFirst || (typeof pos === "number" && Number.isFinite(pos) && pos >= 1 && pos <= 3);
      if (!inTop3) continue;
      seenTop3.add(id);
      top3RecommendedEntityIds.push(id);
    }

    observations.push({
      observationId: row.evidenceId,
      evidenceId: row.evidenceId,
      promptId: row.promptId,
      success: true,
      presentEntityIds,
      recommendedEntityIds,
      top3RecommendedEntityIds,
      firstRecommendationEntityId: recommendedEntityIds[0] || null,
      provider: row.provider || null,
      language: row.language || null,
      geographyKey: row.geographyKey || null,
      slot: row.slot || null,
    });
  }
  return observations;
}

/**
 * Independent metric pack for one entity id (audit path).
 * @param {object[]} observations
 * @param {string} entityId
 */
export function auditComputeEntityMetrics(observations, entityId) {
  const relevant = (observations || []).filter((o) => o.success);
  const denom = relevant.length;
  const promptIds = [...new Set(relevant.map((o) => o.promptId).filter(Boolean))];

  let presenceHits = 0;
  let recPromptHits = 0;
  let top3Hits = 0;
  let firstHits = 0;
  let entityRecSlots = 0;
  let allRecSlots = 0;

  for (const o of relevant) {
    if ((o.presentEntityIds || []).includes(entityId)) presenceHits += 1;

    const recs = o.recommendedEntityIds || [];
    allRecSlots += recs.length;
    entityRecSlots += recs.filter((id) => id === entityId).length;

    if (recs.includes(entityId)) recPromptHits += 1;
    if ((o.top3RecommendedEntityIds || []).includes(entityId)) top3Hits += 1;
    // Contract / metrics.js: first = ordered recommended list head
    if (recs[0] === entityId) firstHits += 1;
  }

  // Questions Won — sole first-recommendation leader per prompt (ties excluded)
  const won = [];
  const missing = [];
  for (const promptId of promptIds) {
    const obs = relevant.filter((o) => o.promptId === promptId);
    const anyPresence = obs.some((o) => (o.presentEntityIds || []).includes(entityId));
    if (!anyPresence) missing.push(promptId);

    /** @type {Record<string, number>} */
    const firstCounts = {};
    for (const o of obs) {
      const first = (o.recommendedEntityIds || [])[0];
      if (!first) continue;
      firstCounts[first] = (firstCounts[first] || 0) + 1;
    }
    const entries = Object.entries(firstCounts).sort(
      (a, b) => b[1] - a[1] || a[0].localeCompare(b[0])
    );
    if (!entries.length) continue;
    const topScore = entries[0][1];
    const leaders = entries.filter(([, c]) => c === topScore).map(([id]) => id);
    if (leaders.length === 1 && leaders[0] === entityId) won.push(promptId);
  }

  return {
    entityId,
    denominator: denom,
    presence: safeDiv(presenceHits, denom),
    numeratorPresence: presenceHits,
    denominatorPresence: denom,
    recommendationShare: safeDiv(entityRecSlots, allRecSlots),
    recommendationRate: safeDiv(recPromptHits, denom),
    top3RecommendationRate: safeDiv(top3Hits, denom),
    firstRecommendationRate: safeDiv(firstHits, denom),
    questionsWon: won.length,
    questionsMissing: missing.length,
  };
}

function valuesClose(a, b, unit) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  if (unit === "count" || unit === "rank") return Number(a) === Number(b);
  return Math.abs(Number(a) - Number(b)) <= RATE_TOLERANCE;
}

/**
 * Compare production summary entity metrics vs audit recomputation.
 * Never silently normalizes mismatches.
 */
export function reconcileEntityMetrics(productionEntity, auditEntity) {
  const comparisons = [];
  const fieldMap = [
    ["AI_PRESENCE", "presence"],
    ["RECOMMENDATION_SHARE", "recommendationShare"],
    ["RECOMMENDATION_RATE", "recommendationRate"],
    ["TOP3_RECOMMENDATION_RATE", "top3RecommendationRate"],
    ["FIRST_RECOMMENDATION_RATE", "firstRecommendationRate"],
    ["QUESTIONS_WON_COUNT", "questionsWon"],
    ["QUESTIONS_MISSING_COUNT", "questionsMissing"],
  ];

  let mismatches = 0;
  for (const [metricId, field] of fieldMap) {
    const contract = METRIC_CONTRACTS[metricId];
    const production = productionEntity?.[field];
    const audit = auditEntity?.[field];
    if (production == null && audit == null) continue;
    if (production == null) continue; // production field absent — skip, do not invent
    const match = valuesClose(production, audit, contract.unit);
    if (!match) mismatches += 1;
    comparisons.push({
      metricId,
      field,
      productionValue: production ?? null,
      auditValue: audit ?? null,
      match,
      state: match
        ? METRIC_VALIDATION_STATE.RECONCILED
        : METRIC_VALIDATION_STATE.VALIDATION_FAILED,
    });
  }

  return {
    entityId: auditEntity?.entityId || productionEntity?.id || null,
    comparisons,
    reconciledCount: comparisons.length - mismatches,
    mismatchCount: mismatches,
    allReconciled: mismatches === 0,
  };
}

/**
 * Bounds checks against contracts.
 */
export function auditMetricBounds(entityMetrics) {
  const violations = [];
  const denom = entityMetrics.denominatorPresence ?? entityMetrics.denominator ?? null;

  for (const [metricId, contract] of Object.entries(METRIC_CONTRACTS)) {
    if (contract.unit === "rank") continue;
    const field = contract.summaryField;
    const value = entityMetrics[field];
    if (value == null || typeof value !== "number" || Number.isNaN(value)) continue;
    if (contract.unit === "rate") {
      if (value < 0 || value > 1) {
        violations.push({
          metricId,
          reason: "rate_out_of_bounds",
          value,
          expected: "[0,1]",
        });
      }
    }
    if (contract.unit === "count" && denom != null && value > denom) {
      violations.push({
        metricId,
        reason: "count_exceeds_denominator",
        value,
        denominator: denom,
      });
    }
    if (contract.unit === "count" && value < 0) {
      violations.push({ metricId, reason: "negative_count", value });
    }
  }
  return violations;
}
