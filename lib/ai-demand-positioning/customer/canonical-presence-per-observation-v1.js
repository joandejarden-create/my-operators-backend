/**
 * Canonical presence per observation — Competitive Overview contract.
 *
 * MAX_PRESENCE_CREDIT_PER_CANONICAL_HOTEL_PER_OBSERVATION = 1
 *
 * Aligns Competitive Overview AI Presence with governed CORE benchmark inputs
 * (peerAppearsInObservation / .some), without changing provider scope or
 * benchmark formulas.
 */

import { canonicalizeForProperty } from "../metrics/adp-property-entity-registries.js";
import { isGovernedNonWaterstoneProperty } from "../metrics/property-core-governance-data.js";
import { canonicalizeToEntityId } from "../metrics/south-florida-entity-registry.js";
import { peerAppearsInObservation } from "../metrics/presence-index-v2.js";

export const MAX_PRESENCE_CREDIT_PER_CANONICAL_HOTEL_PER_OBSERVATION = 1;
export const SUBJECT_PRESENCE_KEY = "__subject__";
export const CANONICAL_PRESENCE_PER_OBSERVATION_VERSION =
  "adp_competitive_overview_canonical_presence_deduplication_v1";

export function resolveCompetitiveEntityId(name, propertyProfile) {
  const propertyId = propertyProfile?.propertyId;
  if (propertyId && isGovernedNonWaterstoneProperty(propertyId)) {
    return canonicalizeForProperty(propertyId, name);
  }
  return canonicalizeToEntityId(name);
}

/**
 * Canonical hotel IDs present in one AI response (competitors only).
 * Multiple aliases for the same ID collapse to one entry.
 */
export function canonicalCompetitorIdsInObservation(obs, propertyProfile = null) {
  const ids = new Set();
  let unresolvedAliasCount = 0;
  let rawAliasMentions = 0;
  for (const name of obs?.competitorsMentioned || []) {
    rawAliasMentions += 1;
    const id = resolveCompetitiveEntityId(name, propertyProfile);
    if (!id) {
      unresolvedAliasCount += 1;
      continue;
    }
    ids.add(id);
  }
  return {
    ids,
    unresolvedAliasCount,
    rawAliasMentions,
    aliasDuplicateMentions: Math.max(0, rawAliasMentions - unresolvedAliasCount - ids.size),
  };
}

/**
 * Unique-per-observation presence counts for Competitive Overview.
 * Subject uses binary obs.mentioned (same as governed subject presence).
 * Competitors use canonical ID dedupe before credit.
 */
export function countCanonicalPresenceAppearances(scoped, propertyProfile = null) {
  const counts = Object.create(null);
  const subjectKey = SUBJECT_PRESENCE_KEY;
  let aliasDuplicateEvents = 0;
  let unresolvedAliasCount = 0;

  for (const obs of scoped || []) {
    if (obs?.mentioned) {
      counts[subjectKey] = (counts[subjectKey] || 0) + 1;
    }

    const { ids, unresolvedAliasCount: unresolved, aliasDuplicateMentions } =
      canonicalCompetitorIdsInObservation(obs, propertyProfile);
    unresolvedAliasCount += unresolved;
    if (aliasDuplicateMentions > 0) {
      aliasDuplicateEvents += aliasDuplicateMentions;
    }
    for (const id of ids) {
      counts[id] = (counts[id] || 0) + 1;
    }
  }

  return {
    counts,
    subjectKey,
    aliasDuplicateEvents,
    unresolvedAliasCount,
    version: CANONICAL_PRESENCE_PER_OBSERVATION_VERSION,
  };
}

/** Legacy alias-inflating counter — audit / before-after only. Do not use for product. */
export function countAppearancesAliasInflating(scoped, entityId, isSubject, propertyProfile = null) {
  let appearances = 0;
  for (const obs of scoped || []) {
    if (isSubject) {
      if (obs?.mentioned) appearances += 1;
      continue;
    }
    for (const name of obs?.competitorsMentioned || []) {
      const id = resolveCompetitiveEntityId(name, propertyProfile);
      if (id === entityId) appearances += 1;
    }
  }
  return appearances;
}

export function uniqueAppearancesForEntity(scoped, entityId, isSubject, propertyProfile = null) {
  if (isSubject) {
    return (scoped || []).filter((o) => o?.mentioned).length;
  }
  return (scoped || []).filter((o) => peerAppearsInObservation(o, entityId, propertyProfile)).length;
}
