/**
 * Deterministic AI Visibility metrics — version ai_visibility_metrics_v1.
 * Phase 3A.4 adds Recommendation Rate + Top-3 additively (existing formulas unchanged).
 * No composite GEO score. Competitive Position ranks by AI Presence Rate.
 * Competitive Recommendation Share is NOT a separate product metric — use Recommendation Share.
 */

import { METRIC_VERSION } from "./config.js";

/**
 * Roles that count as a positive recommendation for Recommendation Rate.
 * associated_option / comparator / discussed / passing_mention / etc. are excluded.
 */
export const POSITIVE_RECOMMENDATION_ROLES = Object.freeze([
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
]);

const POSITIVE_ROLE_SET = new Set(POSITIVE_RECOMMENDATION_ROLES);

/**
 * @typedef {Object} Observation
 * @property {string} observationId
 * @property {string} promptId
 * @property {string} [provider]
 * @property {string} [periodKey]
 * @property {boolean} success
 * @property {string[]} presentEntityIds
 * @property {string[]} recommendedEntityIds  // positive recs; ordered when positions exist
 * @property {string[]} [top3RecommendedEntityIds] // positions 1–3 only; never inferred
 * @property {string[]} firstPartyCitationEntityIds
 */

function safeDiv(n, d) {
  if (!d) return null;
  return n / d;
}

function mentionEntityId(m) {
  return m?.canonicalEntityId || m?.entityId || m?.resolvedEntityId || null;
}

/** @param {string} [role] */
export function isPositiveRecommendationRole(role) {
  return POSITIVE_ROLE_SET.has(role);
}

/**
 * @param {Observation[]} observations
 * @param {string} entityId
 */
export function computeAiPresenceRate(observations, entityId) {
  const relevant = (observations || []).filter((o) => o.success);
  const hits = relevant.filter((o) => (o.presentEntityIds || []).includes(entityId));
  return {
    metricVersion: METRIC_VERSION,
    metric: "ai_presence_rate",
    entityId,
    numerator: hits.length,
    denominator: relevant.length,
    value: safeDiv(hits.length, relevant.length),
  };
}

/**
 * @param {Observation[]} observations
 * @param {string} entityId
 */
export function computeRecommendationShare(observations, entityId) {
  const relevant = (observations || []).filter((o) => o.success);
  let entityRecs = 0;
  let allRecs = 0;
  for (const o of relevant) {
    const recs = o.recommendedEntityIds || [];
    allRecs += recs.length;
    entityRecs += recs.filter((id) => id === entityId).length;
  }
  return {
    metricVersion: METRIC_VERSION,
    metric: "recommendation_share",
    entityId,
    numerator: entityRecs,
    denominator: allRecs,
    value: safeDiv(entityRecs, allRecs),
    eligibilityModel: "cohort_level",
    note: "Product name is Recommendation Share (includes competitive cohort share). Do not expose a separate Competitive Recommendation Share.",
  };
}

/**
 * Recommendation Rate — successful cohort observations where Brand has ≥1 positive
 * recommendation role, counted once per observation.
 * Eligibility today = governed monitored cohort (not full entity-specific fit).
 *
 * @param {Observation[]} observations
 * @param {string} entityId
 */
export function computeRecommendationRate(observations, entityId) {
  const relevant = (observations || []).filter((o) => o.success);
  const hits = relevant.filter((o) => (o.recommendedEntityIds || []).includes(entityId));
  return {
    metricVersion: METRIC_VERSION,
    metric: "recommendation_rate",
    entityId,
    numerator: hits.length,
    denominator: relevant.length,
    value: safeDiv(hits.length, relevant.length),
    positiveRoles: [...POSITIVE_RECOMMENDATION_ROLES],
    eligibilityModel: "cohort_level",
    eligibilityNote:
      "Denominator is successful cohort-eligible monitored responses. Not yet full brand/asset-specific eligibility.",
  };
}

/**
 * Top-3 Recommendation Rate — Brand in positions 1–3 only when rank is deterministic.
 * Unranked recommendations count for Recommendation Rate but NOT Top-3.
 *
 * @param {Observation[]} observations
 * @param {string} entityId
 */
export function computeTop3RecommendationRate(observations, entityId) {
  const relevant = (observations || []).filter((o) => o.success);
  const hits = relevant.filter((o) => (o.top3RecommendedEntityIds || []).includes(entityId));
  return {
    metricVersion: METRIC_VERSION,
    metric: "top3_recommendation_rate",
    entityId,
    numerator: hits.length,
    denominator: relevant.length,
    value: safeDiv(hits.length, relevant.length),
    eligibilityModel: "cohort_level",
    note: "Requires recommendationPosition 1–3 or first_recommendation. No inferred ranks. Top-5 is Future Ready.",
  };
}

/**
 * @param {Observation[]} observations
 * @param {string} entityId
 */
export function computeFirstRecommendationRate(observations, entityId) {
  const relevant = (observations || []).filter((o) => o.success);
  const hits = relevant.filter((o) => (o.recommendedEntityIds || [])[0] === entityId);
  return {
    metricVersion: METRIC_VERSION,
    metric: "first_recommendation_rate",
    entityId,
    numerator: hits.length,
    denominator: relevant.length,
    value: safeDiv(hits.length, relevant.length),
  };
}

/**
 * Questions Won: per prompt, leading entity = highest first-recommendation count
 * across successful observations for that prompt.
 * Ties → coWon: true, no sole winner.
 *
 * @param {Observation[]} observations
 * @param {string} entityId
 * @param {string[]} [promptIds]
 */
export function computeQuestionsWon(observations, entityId, promptIds) {
  const relevant = (observations || []).filter((o) => o.success);
  const prompts = promptIds
    ? [...new Set(promptIds)]
    : [...new Set(relevant.map((o) => o.promptId))];

  const won = [];
  const tied = [];
  const missingLead = [];

  for (const promptId of prompts) {
    const obs = relevant.filter((o) => o.promptId === promptId);
    if (!obs.length) {
      missingLead.push(promptId);
      continue;
    }
    /** @type {Record<string, number>} */
    const firstCounts = {};
    for (const o of obs) {
      const first = (o.recommendedEntityIds || [])[0];
      if (!first) continue;
      firstCounts[first] = (firstCounts[first] || 0) + 1;
    }
    const entries = Object.entries(firstCounts).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
    if (!entries.length) {
      missingLead.push(promptId);
      continue;
    }
    const topScore = entries[0][1];
    const leaders = entries.filter(([, c]) => c === topScore).map(([id]) => id);
    if (leaders.length === 1) {
      if (leaders[0] === entityId) won.push(promptId);
    } else if (leaders.includes(entityId)) {
      tied.push(promptId);
    }
  }

  return {
    metricVersion: METRIC_VERSION,
    metric: "questions_won",
    entityId,
    wonPromptIds: won,
    tiedPromptIds: tied,
    count: won.length,
    tieCount: tied.length,
    // Ties are NOT counted as sole wins.
    value: won.length,
  };
}

/**
 * @param {Observation[]} observations
 * @param {string} entityId
 * @param {string[]} [promptIds]
 */
export function computeQuestionsMissing(observations, entityId, promptIds) {
  const relevant = (observations || []).filter((o) => o.success);
  const prompts = promptIds
    ? [...new Set(promptIds)]
    : [...new Set(relevant.map((o) => o.promptId))];

  const missing = [];
  for (const promptId of prompts) {
    const obs = relevant.filter((o) => o.promptId === promptId);
    const anyPresence = obs.some((o) => (o.presentEntityIds || []).includes(entityId));
    if (!anyPresence) missing.push(promptId);
  }

  return {
    metricVersion: METRIC_VERSION,
    metric: "questions_missing",
    entityId,
    missingPromptIds: missing,
    count: missing.length,
    value: missing.length,
  };
}

/**
 * Competitive Position: rank by AI Presence Rate within peer set.
 * Ties share rank (competition rank: 1,2,2,4).
 * Null / unmeasured presence sorts last and never receives rank 0.
 *
 * @param {Observation[]} observations
 * @param {string[]} peerEntityIds
 */
export function computeCompetitivePosition(observations, peerEntityIds) {
  const peers = [...new Set(peerEntityIds || [])];
  const rows = peers.map((entityId) => {
    const presence = computeAiPresenceRate(observations, entityId);
    return {
      entityId,
      presenceRate: presence.value,
      numerator: presence.numerator,
      denominator: presence.denominator,
    };
  });

  rows.sort((a, b) => {
    const aNull = a.presenceRate == null;
    const bNull = b.presenceRate == null;
    if (aNull && bNull) return a.entityId.localeCompare(b.entityId);
    if (aNull) return 1;
    if (bNull) return -1;
    if (b.presenceRate !== a.presenceRate) return b.presenceRate - a.presenceRate;
    return a.entityId.localeCompare(b.entityId);
  });

  let lastRateKey = undefined;
  let lastRank = 0;
  const ranked = rows.map((row, idx) => {
    // Use a string key so null === null advances rank (unlike `null !== null`).
    const rateKey = row.presenceRate == null ? "__null__" : String(row.presenceRate);
    if (rateKey !== lastRateKey) {
      lastRank = idx + 1;
      lastRateKey = rateKey;
    }
    return { ...row, rank: lastRank, tied: false };
  });

  // Mark ties
  for (const row of ranked) {
    row.tied = ranked.filter((r) => r.rank === row.rank).length > 1;
  }

  return {
    metricVersion: METRIC_VERSION,
    metric: "competitive_position",
    rankingMetric: "ai_presence_rate",
    peers: ranked,
  };
}

/**
 * @param {Observation[]} observations
 * @param {string} entityId
 */
export function computeCitationRate(observations, entityId) {
  const relevant = (observations || []).filter((o) => o.success);
  const hits = relevant.filter((o) => {
    const ids =
      Array.isArray(o.associatedCitationEntityIds) && o.associatedCitationEntityIds.length
        ? o.associatedCitationEntityIds
        : o.firstPartyCitationEntityIds || [];
    return ids.includes(entityId);
  });
  return {
    metricVersion: METRIC_VERSION,
    metric: "citation_rate",
    entityId,
    numerator: hits.length,
    denominator: relevant.length,
    value: safeDiv(hits.length, relevant.length),
  };
}

/**
 * Build an Observation from extracted mentions/citations for one response.
 */
export function buildObservationFromExtractions({
  observationId,
  promptId,
  provider,
  periodKey,
  success = true,
  mentions = [],
  citations = [],
  geography = null,
  intentTerritory = null,
}) {
  const presentEntityIds = [
    ...new Set(
      (mentions || [])
        .filter((m) => m.canonicalEntityId)
        .map((m) => m.canonicalEntityId)
    ),
  ];

  const recommendedMentions = (mentions || [])
    .filter((m) => {
      const id = mentionEntityId(m);
      if (!id) return false;
      if (isPositiveRecommendationRole(m.role)) return true;
      // Legacy rows: explicitRecommendation flag without a role string
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

  const recommended = recommendedMentions;

  // Dedupe recommended list preserving first occurrence order (once per entity per observation)
  const recommendedEntityIds = [];
  const seen = new Set();
  for (const m of recommended) {
    const id = mentionEntityId(m);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    recommendedEntityIds.push(id);
  }

  // Top-3: only deterministic positions 1–3 or first_recommendation — never infer from list order alone
  const top3RecommendedEntityIds = [];
  const seenTop3 = new Set();
  for (const m of recommended) {
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

  const citationList = Array.isArray(citations) ? citations.slice() : [];

  const firstPartyCitationEntityIds = [
    ...new Set(
      citationList
        .filter((c) => c.firstParty && c.entityAssociation)
        .map((c) => c.entityAssociation)
    ),
  ];

  const associatedCitationEntityIds = [
    ...new Set(
      citationList
        .filter((c) => c.entityAssociation)
        .map((c) => c.entityAssociation)
    ),
  ];

  return {
    observationId,
    promptId,
    provider: provider || null,
    periodKey: periodKey || null,
    success: Boolean(success),
    presentEntityIds,
    recommendedEntityIds,
    top3RecommendedEntityIds,
    firstPartyCitationEntityIds,
    associatedCitationEntityIds,
    // Canonical citation evidence for Citation Rate metrics.
    // Same governed rows as evidence.payload.citations / response.citations —
    // do not re-parse raw provider annotations here.
    citations: citationList,
    geography: geography || null,
    intentTerritory: intentTerritory || null,
  };
}

/** Correct spelling + legacy Extrcted alias. Canonical export remains `buildObservationFromExtractions`. */
export const buildObservationFromExtracted = buildObservationFromExtractions;
export const buildObservationFromExtrcted = buildObservationFromExtractions;

export { METRIC_VERSION };
