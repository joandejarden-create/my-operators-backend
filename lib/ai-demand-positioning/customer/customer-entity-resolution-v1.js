/**
 * Customer-facing ADP entity resolution — single path for Competitive Set,
 * Displacement, Top Alternative, and related owner-visible hotel names.
 * Preserves raw extraction internally; only customer display/aggregation uses this.
 */

import {
  canonicalizeCompetitorName,
  isNonHotelEntity,
} from "../intelligence/competitor-name-resolution.js";
import {
  canonicalizeForProperty,
  registryHotelById,
  classifyObservedForProperty,
} from "../metrics/adp-property-entity-registries.js";
import {
  canonicalizeToEntityId as sfCanonicalizeToEntityId,
  classifyObservedEntity as sfClassifyObservedEntity,
} from "../metrics/south-florida-entity-registry.js";
import { isLikelyArtifactEntity } from "../metrics/entity-quality.js";

const PROSE_FRAGMENT_PATTERNS = [
  /^many suites$/i,
  /^this (hotel|resort|iconic\b.*)$/i,
  /^this iconic hotel$/i,
  /^located in\b/i,
  /^situated (in|on|at)\b/i,
  /^choose from\b/i,
  /^choose the\b/i,
  /^depending on\b/i,
  /^based on\b/i,
  /^here are\b/i,
  /^for example\b/i,
  /^recommended hotels?\b/i,
  /^book the\b/i,
  /^like the\b/i,
  /^\d+\.\s/,
  /^with only\b/i,
  /,.*\bor\b/i,
];

const SOUTH_FLORIDA_PROPERTY_IDS = new Set(["adp_waterstone_boca_raton"]);

const NYC_GEO_RE = /\b(new york|manhattan|times square|noho|midtown)\b/i;

function isCrossMarketLeak(displayName, propertyProfile) {
  const market = String(propertyProfile?.market || "").toLowerCase();
  const name = String(displayName || "");
  if (!market || !name) return false;
  const isNycProperty =
    market.includes("new york") || market === "nyc" || market.includes("noho") || market.includes("times square");
  if (!isNycProperty && NYC_GEO_RE.test(name)) return true;
  return false;
}

function classifyRaw(propertyId, raw) {
  const propertyClassified = propertyId ? classifyObservedForProperty(propertyId, raw) : null;
  if (propertyClassified) return { source: "property", classified: propertyClassified };
  if (SOUTH_FLORIDA_PROPERTY_IDS.has(propertyId) || /boca|palm beach|miami|fort lauderdale/i.test(raw)) {
    return { source: "south_florida", classified: sfClassifyObservedEntity(raw) };
  }
  return { source: null, classified: null };
}

/**
 * @returns {{
 *   ok: boolean,
 *   displayName: string|null,
 *   entityId: string|null,
 *   mergeKey: string|null,
 *   rejected: boolean,
 *   reason: string|null,
 *   raw: string
 * }}
 */
export function resolveCustomerFacingEntity(rawName, propertyProfile = {}) {
  const raw = String(rawName || "").trim();
  if (!raw) {
    return { ok: false, displayName: null, entityId: null, mergeKey: null, rejected: true, reason: "empty", raw };
  }

  const propertyId = propertyProfile?.propertyId || null;
  // Registry / canonical hotels first — some legal names ("The Boca Raton") would otherwise
  // trip the "starts with The" artifact heuristic.
  const { classified } = classifyRaw(propertyId, raw);
  if (classified?.identityOk && classified.entityId) {
    const hotel =
      (propertyId && registryHotelById(propertyId, classified.entityId)) ||
      classified.hotel ||
      null;
    const displayName = hotel?.canonical || classified.canonical || raw;
    if (isCrossMarketLeak(displayName, propertyProfile)) {
      return { ok: false, displayName: null, entityId: null, mergeKey: null, rejected: true, reason: "cross_market_leak", raw };
    }
    return {
      ok: true,
      displayName,
      entityId: classified.entityId,
      mergeKey: classified.entityId,
      rejected: false,
      reason: null,
      raw,
    };
  }
  if (classified && ["GENERIC_PHRASE", "VENUE_ONLY", "LOCATION", "NON_HOTEL_ENTITY"].includes(classified.class)) {
    return {
      ok: false,
      displayName: null,
      entityId: null,
      mergeKey: null,
      rejected: true,
      reason: classified.class,
      raw,
    };
  }

  if (isNonHotelEntity(raw) || isLikelyArtifactEntity(raw) || PROSE_FRAGMENT_PATTERNS.some((p) => p.test(raw))) {
    return { ok: false, displayName: null, entityId: null, mergeKey: null, rejected: true, reason: "artifact_or_non_hotel", raw };
  }

  if (propertyId) {
    const entityId =
      canonicalizeForProperty(propertyId, raw) ||
      (SOUTH_FLORIDA_PROPERTY_IDS.has(propertyId) ? sfCanonicalizeToEntityId(raw) : null);
    if (entityId) {
      const hotel = registryHotelById(propertyId, entityId) || classified?.hotel || null;
      const displayName = hotel?.canonical || classified?.canonical || raw;
      if (isCrossMarketLeak(displayName, propertyProfile)) {
        return { ok: false, displayName: null, entityId: null, mergeKey: null, rejected: true, reason: "cross_market_leak", raw };
      }
      return {
        ok: true,
        displayName,
        entityId,
        mergeKey: entityId,
        rejected: false,
        reason: null,
        raw,
      };
    }
  }

  const resolved = canonicalizeCompetitorName(raw, { market: propertyProfile?.market }) || raw;
  if (
    isLikelyArtifactEntity(resolved) ||
    PROSE_FRAGMENT_PATTERNS.some((p) => p.test(resolved)) ||
    isCrossMarketLeak(resolved, propertyProfile)
  ) {
    return { ok: false, displayName: null, entityId: null, mergeKey: null, rejected: true, reason: "artifact_after_resolve", raw };
  }

  // Bare brand fragments without geography are not customer-safe when a fuller local name may exist.
  if (/^(hampton inn(&?\s*suites)?|residence inn|courtyard|home2 suites|holiday inn|point suites)$/i.test(resolved)) {
    return { ok: false, displayName: null, entityId: null, mergeKey: null, rejected: true, reason: "bare_brand_fragment", raw };
  }

  const mergeKey = resolved
    .toLowerCase()
    .replace(/\b(hotel|resort|inn|suites?|lodge|club|spa|the|a|an|by|at|&|and)\b/g, "")
    .replace(/[^a-z0-9]/g, "");

  if (!mergeKey || mergeKey.length < 4) {
    return { ok: false, displayName: null, entityId: null, mergeKey: null, rejected: true, reason: "weak_merge_key", raw };
  }

  return {
    ok: true,
    displayName: resolved,
    entityId: null,
    mergeKey,
    rejected: false,
    reason: null,
    raw,
  };
}

/** Merge count maps keyed by customer mergeKey; prefer longer/canonical display names. */
export function mergeCustomerEntityCounts(entries) {
  const byKey = new Map();
  for (const entry of entries || []) {
    const resolved = entry.resolved || resolveCustomerFacingEntity(entry.name, entry.profile);
    if (!resolved.ok || resolved.rejected) continue;
    const key = resolved.mergeKey;
    const prev = byKey.get(key);
    const nextCount = (prev?.count || 0) + (entry.count || 1);
    const preferDisplay =
      !prev ||
      (resolved.displayName || "").length > (prev.displayName || "").length ||
      Boolean(resolved.entityId && !prev.entityId);
    byKey.set(key, {
      name: preferDisplay ? resolved.displayName : prev.displayName,
      count: nextCount,
      entityId: resolved.entityId || prev?.entityId || null,
      rawNames: [...new Set([...(prev?.rawNames || []), resolved.raw])],
    });
  }
  return [...byKey.values()];
}

export function filterCustomerFacingEntityNames(names, propertyProfile) {
  return mergeCustomerEntityCounts(
    (names || []).map((name) => ({ name, count: 1, profile: propertyProfile }))
  ).map((r) => r.name);
}
