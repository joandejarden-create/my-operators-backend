/**
 * Canonical presence per observation — Competitive Overview contract.
 *
 * MAX_PRESENCE_CREDIT_PER_CANONICAL_HOTEL_PER_OBSERVATION = 1
 *
 * Aligns Competitive Overview AI Presence with governed CORE benchmark inputs
 * (peerAppearsInObservation / .some), without changing provider scope or
 * benchmark formulas.
 */

import {
  canonicalizeForProperty,
  getEntityRegistryForProperty,
} from "../metrics/adp-property-entity-registries.js";
import { isGovernedNonWaterstoneProperty } from "../metrics/property-core-governance-data.js";
import { canonicalizeToEntityId } from "../metrics/south-florida-entity-registry.js";
import { peerAppearsInObservation } from "../metrics/presence-index-v2.js";

export const MAX_PRESENCE_CREDIT_PER_CANONICAL_HOTEL_PER_OBSERVATION = 1;
export const SUBJECT_PRESENCE_KEY = "__subject__";
export const CANONICAL_PRESENCE_PER_OBSERVATION_VERSION =
  "adp_competitive_overview_canonical_presence_deduplication_v1";

function competitorNameText(name) {
  if (typeof name === "string") return name;
  return String(name?.name || name?.canonicalName || name?.raw || "").trim();
}

/**
 * Resolve competitor mention → canonical entityId.
 * Prefer property market registry whenever one exists (Rome, Munich, NYC, …).
 * Governed non-Waterstone properties stay fail-closed on that registry (no SF fallback).
 * Legacy South Florida canonicalize remains the fallback only when no property registry exists.
 */
export function resolveCompetitiveEntityId(name, propertyProfile) {
  const text = competitorNameText(name);
  if (!text) return null;
  const propertyId = propertyProfile?.propertyId;
  if (propertyId && getEntityRegistryForProperty(propertyId)) {
    return canonicalizeForProperty(propertyId, text);
  }
  if (propertyId && isGovernedNonWaterstoneProperty(propertyId)) {
    return canonicalizeForProperty(propertyId, text);
  }
  return canonicalizeToEntityId(text);
}

/**
 * Canonical hotel IDs present in one AI response (competitors only).
 * Multiple aliases for the same ID collapse to one entry.
 */
function subjectEntityIds(propertyProfile) {
  const propertyId = propertyProfile?.propertyId;
  if (!propertyId) return new Set();
  const reg = getEntityRegistryForProperty(propertyId);
  const ids = new Set();
  for (const h of reg?.hotels || []) {
    if (h.subject) ids.add(h.entityId);
  }
  // Common subject keys used in registries
  if (propertyId.startsWith("adp_")) {
    const slug = propertyId.replace(/^adp_/, "");
    ids.add(slug);
  }
  return ids;
}

export function canonicalCompetitorIdsInObservation(obs, propertyProfile = null) {
  const ids = new Set();
  let unresolvedAliasCount = 0;
  let rawAliasMentions = 0;
  const subjectIds = subjectEntityIds(propertyProfile);
  for (const name of obs?.competitorsMentioned || []) {
    rawAliasMentions += 1;
    const id = resolveCompetitiveEntityId(name, propertyProfile);
    if (!id) {
      unresolvedAliasCount += 1;
      continue;
    }
    // Subject aliases sometimes leak into competitorsMentioned — never credit as competitor
    if (subjectIds.has(id)) continue;
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
