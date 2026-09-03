/**
 * Top Observed AI Alternative V1 — customer-facing KPI aligned to Overall Competitive Overview.
 *
 * Grain: unique-per-observation presence (MAX 1 credit per hotel per response).
 * Scope: comparable observations, Overall (full property period).
 * Identity: resolveCompetitiveEntityId (same as Competitive Overview).
 * Tie rule: higher appearances first; on equal appearances, alphabetical by display name (A→Z).
 *
 * RAW_COMPETITOR_MENTION_COUNT is diagnostic-only and must not drive this KPI.
 */

import { roundAdpPercent } from "../format-percent.js";
import { filterComparableObservations } from "../metrics/grain-governance.js";
import { hotelById } from "../metrics/presence-benchmark-v1.js";
import {
  countCanonicalPresenceAppearances,
  resolveCompetitiveEntityId,
  SUBJECT_PRESENCE_KEY,
  MAX_PRESENCE_CREDIT_PER_CANONICAL_HOTEL_PER_OBSERVATION,
} from "./canonical-presence-per-observation-v1.js";
import { buildOverallCompetitiveRanking } from "./competitive-ranking-overall-view-v1.js";

export const TOP_OBSERVED_AI_ALTERNATIVE_VERSION = "adp_top_observed_ai_alternative_overall_presence_v1";
export const RAW_COMPETITOR_MENTION_COUNT = "RAW_COMPETITOR_MENTION_COUNT";
export const TOP_ALTERNATIVE_RANKING_BASIS = "OVERALL_UNIQUE_PER_OBSERVATION_PRESENCE";

/** Same tie rule as Competitive Overview Overall competitor sort. */
export function comparePresenceThenName(a, b) {
  const ap = Number(a.appearances || 0);
  const bp = Number(b.appearances || 0);
  if (bp !== ap) return bp - ap;
  return String(a.name || "").localeCompare(String(b.name || ""));
}

export function entityDisplayNameForCompetitiveId(entityId, propertyProfile) {
  const hotel = hotelById(entityId, propertyProfile);
  return hotel?.canonical || String(entityId || "").replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * Rank non-subject hotels by Overall unique-per-observation AI Presence.
 */
export function rankCompetitorsByOverallPresence(observations, propertyProfile) {
  const scoped = filterComparableObservations(observations || []);
  const n = scoped.length;
  const { counts, subjectKey } = countCanonicalPresenceAppearances(scoped, propertyProfile);
  const rows = [];
  for (const [entityId, appearances] of Object.entries(counts)) {
    if (entityId === subjectKey || entityId === SUBJECT_PRESENCE_KEY) continue;
    rows.push({
      entityId,
      name: entityDisplayNameForCompetitiveId(entityId, propertyProfile),
      appearances,
      aiPresenceRate: n ? appearances / n : null,
      aiPresencePct: n ? roundAdpPercent((appearances / n) * 100) : null,
      rankingBasis: TOP_ALTERNATIVE_RANKING_BASIS,
      maxPresenceCreditPerObservation: MAX_PRESENCE_CREDIT_PER_CANONICAL_HOTEL_PER_OBSERVATION,
    });
  }
  rows.sort(comparePresenceThenName);
  return { rows, comparableN: n, subjectKey };
}

/**
 * Customer-facing Top Observed AI Alternative = Overall presence leader (non-subject).
 */
export function deriveTopObservedAiAlternative(observations, propertyProfile, options = {}) {
  const overall =
    options.overallRanking ||
    (options.scenarios
      ? buildOverallCompetitiveRanking(observations, options.scenarios, propertyProfile)
      : null);

  let leader = null;
  let tiedMaxSet = [];

  if (overall?.displayRows?.length || overall?.observedRanked?.length) {
    const ranked = (overall.displayRows || overall.observedRanked || []).filter((r) => !r.isSubject);
    const sorted = [...ranked].sort(comparePresenceThenName);
    leader = sorted[0] || null;
    if (leader) {
      const maxApp = leader.appearances;
      tiedMaxSet = sorted.filter((r) => r.appearances === maxApp);
    }
  } else {
    const { rows } = rankCompetitorsByOverallPresence(observations, propertyProfile);
    leader = rows[0] || null;
    if (leader) {
      tiedMaxSet = rows.filter((r) => r.appearances === leader.appearances);
    }
  }

  if (!leader) {
    return {
      version: TOP_OBSERVED_AI_ALTERNATIVE_VERSION,
      topObservedAlternative: null,
      tiedMaxSet: [],
      rankingBasis: TOP_ALTERNATIVE_RANKING_BASIS,
    };
  }

  const topObservedAlternative = {
    entityId: leader.entityId,
    name: leader.name,
    appearances: leader.appearances,
    aiPresencePct: leader.aiPresencePct,
    aiPresenceRate: leader.aiPresenceRate,
    rankingBasis: TOP_ALTERNATIVE_RANKING_BASIS,
    // Legacy field name retained for UI/consumers that read .mentions — equals unique appearances.
    mentions: leader.appearances,
  };

  return {
    version: TOP_OBSERVED_AI_ALTERNATIVE_VERSION,
    topObservedAlternative,
    tiedMaxSet: tiedMaxSet.map((r) => ({
      entityId: r.entityId,
      name: r.name,
      appearances: r.appearances,
    })),
    rankingBasis: TOP_ALTERNATIVE_RANKING_BASIS,
  };
}

/**
 * Hard invariant: topObservedAlternative.entityId === highestNonSubjectOverallPresence.entityId
 * On ties, top must be a member of the tied maximum set under governed tie rule (first after sort).
 */
export function assertTopAlternativeMatchesOverallLeader(topObservedAlternative, overallRanking) {
  const ranked = (overallRanking?.displayRows || overallRanking?.observedRanked || []).filter(
    (r) => !r.isSubject
  );
  const sorted = [...ranked].sort(comparePresenceThenName);
  const highest = sorted[0] || null;

  if (!topObservedAlternative && !highest) {
    return { pass: true, enforced: true, reason: "both_empty" };
  }
  if (!topObservedAlternative || !highest) {
    return {
      pass: false,
      enforced: true,
      reason: "one_missing",
      topObservedAlternative,
      highestNonSubjectOverallPresence: highest,
    };
  }

  const maxApp = highest.appearances;
  const tiedMaxSet = sorted.filter((r) => r.appearances === maxApp);
  const inTiedSet = tiedMaxSet.some((r) => r.entityId === topObservedAlternative.entityId);
  const exactLeader = topObservedAlternative.entityId === highest.entityId;

  // Governed tie: sorted[0] is the canonical display leader; KPI must equal that entity.
  const pass = exactLeader && inTiedSet;

  return {
    pass,
    enforced: true,
    reason: pass ? "match" : "entity_mismatch",
    topObservedAlternative: {
      entityId: topObservedAlternative.entityId,
      name: topObservedAlternative.name,
      appearances: topObservedAlternative.appearances,
    },
    highestNonSubjectOverallPresence: {
      entityId: highest.entityId,
      name: highest.name,
      appearances: highest.appearances,
      aiPresencePct: highest.aiPresencePct,
    },
    tiedMaxSet: tiedMaxSet.map((r) => ({ entityId: r.entityId, name: r.name, appearances: r.appearances })),
  };
}

/**
 * RAW_COMPETITOR_MENTION_COUNT — diagnostic only (inflates on multi-mention within one response).
 * Must not feed Top Observed AI Alternative.
 */
export function computeRawCompetitorMentionCounts(observations, propertyProfile) {
  const byId = Object.create(null);
  for (const obs of observations || []) {
    for (const name of obs.competitorsMentioned || []) {
      const entityId = resolveCompetitiveEntityId(name, propertyProfile);
      if (!entityId) continue;
      if (!byId[entityId]) {
        byId[entityId] = {
          entityId,
          name: entityDisplayNameForCompetitiveId(entityId, propertyProfile),
          rawMentionCount: 0,
          metric: RAW_COMPETITOR_MENTION_COUNT,
        };
      }
      byId[entityId].rawMentionCount += 1;
    }
  }
  return Object.values(byId).sort((a, b) => b.rawMentionCount - a.rawMentionCount || a.name.localeCompare(b.name));
}
