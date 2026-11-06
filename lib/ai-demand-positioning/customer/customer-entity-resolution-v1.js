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
  /^(while|although|because|since)\b/i,
  /^(its|their|our)\b/i,
  /^(one|many|some|all)\s+of\b/i,
  /\b(is presented as|is known as|is considered)\b/i,
  /^[a-z]/,
  /^rooms?\s*\/?\s*suites?:?$/i,
  /^(spacious|executive|boutique|deluxe|junior|presidential|family|corner|bellair|penthouse)\s+suites?$/i,
  /^rooms and suites$/i,
  /\btopgolf\b/i,
  /^swing suite$/i,
  /^this hotel$/i,
  /^luxury hotel$/i,
  /^downtown hotel$/i,
  /^lots of hotel$/i,
  /^now hotel$/i,
  /^with hotel$/i,
  /^s suite$/i,
  /^[a-z]+\s+by\s+(library|hilton|marriott|hyatt)\b/i,
  /^overview hotel$/i,
  /^indias hotel$/i,
  /^(want|travelers?|looking|expect|provides?|houses?|a massive|historically)\b/i,
  /\b(proximity to|looking for|ocean views)\b/i,
  /^(a|an|the)\s+\w+,\s+(hotel|resort)$/i,
  /^(relaxing|massive|active|quieter),?\s+(hotel|resort)$/i,
];

const SOUTH_FLORIDA_PROPERTY_IDS = new Set(["adp_waterstone_boca_raton"]);

const NYC_GEO_RE = /\b(new york|manhattan|times square|noho|midtown)\b/i;
/** Always foreign for non-Guadalajara subjects (Expo Guadalajara prompt-bank contamination). */
const GUADALAJARA_GEO_RE = /\bguadalajara\b/i;
/** Geo tokens that imply a foreign metro when the subject hotel is elsewhere. */
const MARKET_GEO_TOKENS = Object.freeze([
  {
    subject: /\b(charlotte|north carolina|\bnc\b)\b/i,
    foreign:
      /\b(boca raton|boca\b|miami|fort lauderdale|palm beach|delray|tampa|orlando|san diego|washington|bethesda|new york|manhattan|times square|noho|kansas city|monterrey|rome|bermuda|guadalajara)\b/i,
  },
  {
    subject: /\b(boca|palm beach|miami|fort lauderdale|south florida)\b/i,
    foreign:
      /\b(charlotte|kansas city|san diego|tampa waterside|times square|bethesda|washington|monterrey|rome|bermuda|guadalajara)\b/i,
  },
  {
    subject: /\b(new york|manhattan|times square|noho)\b/i,
    foreign: /\b(boca|miami|charlotte|tampa|san diego|bethesda|kansas city|monterrey|guadalajara)\b/i,
  },
  {
    subject: /\b(toronto|ontario|harbourfront|queens quay|gta)\b/i,
    foreign:
      /\b(guadalajara|mexico city|cdmx|cancun|cancún|monterrey|boca raton|miami|charlotte|kansas city|bethesda|times square)\b/i,
  },
  {
    subject: /\bguadalajara\b/i,
    foreign:
      /\b(toronto|harbourfront|boca raton|miami|charlotte|kansas city|bethesda|times square|new york|cancun|cancún|punta cana|cap cana|uvero alto)\b/i,
  },
  {
    subject: /\b(cancun|cancún|zona hotelera|riviera maya)\b/i,
    foreign:
      /\b(guadalajara|toronto|punta cana|cap cana|uvero alto|mexico city|cdmx|monterrey|boca raton|charlotte)\b/i,
  },
  {
    subject: /\b(punta cana|uvero alto|cap cana|altagracia|dominican)\b/i,
    foreign:
      /\b(guadalajara|cancun|cancún|toronto|mexico city|cdmx|monterrey|boca raton|charlotte|bethesda)\b/i,
  },
]);

function subjectMarketBlob(propertyProfile) {
  return [
    propertyProfile?.market,
    propertyProfile?.city,
    propertyProfile?.state,
    propertyProfile?.region,
    propertyProfile?.name,
    propertyProfile?.propertyId,
  ]
    .map((v) => String(v || ""))
    .join(" ");
}

/**
 * Generic cross-market competitor filter — applies to every ADP hotel.
 * Evidence-backed exceptions: allow when subject market also matches the foreign token
 * (e.g. Waterstone may legitimately surface Boca peers).
 */
export function isCrossMarketLeak(displayName, propertyProfile) {
  const market = subjectMarketBlob(propertyProfile);
  const name = String(displayName || "");
  if (!market || !name) return false;
  const isNycProperty =
    /\b(new york|nyc|noho|times square|manhattan)\b/i.test(market);
  if (!isNycProperty && NYC_GEO_RE.test(name)) return true;
  const isGuadalajaraProperty = GUADALAJARA_GEO_RE.test(market);
  if (!isGuadalajaraProperty && GUADALAJARA_GEO_RE.test(name)) return true;
  for (const rule of MARKET_GEO_TOKENS) {
    if (rule.subject.test(market) && rule.foreign.test(name)) return true;
  }
  return false;
}

/** Subject hotel must never appear as a customer-facing competitor/displacement entity. */
function isSubjectPropertyName(rawName, propertyProfile) {
  const raw = String(rawName || "").trim().toLowerCase();
  const subject = String(propertyProfile?.name || "").trim().toLowerCase();
  if (!raw || !subject) return false;
  if (raw === subject) return true;
  if (raw.includes(subject) || subject.includes(raw)) return true;
  const subjectCore = subject.replace(/,.*/, "").trim();
  if (subjectCore.length >= 8 && raw.includes(subjectCore)) return true;
  // Soft-brand expansions of the subject (e.g. "Hotel Phillips …, Curio Collection by Hilton")
  if (subjectCore.length >= 8 && raw.startsWith(subjectCore) && /\b(curio|collection|hilton|marriott|hyatt|autograph|tribute)\b/i.test(raw)) {
    return true;
  }
  return false;
}

function classifyRaw(propertyId, raw) {
  const propertyClassified = propertyId ? classifyObservedForProperty(propertyId, raw) : null;
  // Prefer property registry even when class is brand/unresolved — prevents SF bleed.
  if (propertyClassified) return { source: "property", classified: propertyClassified };
  // South Florida classifier only for Waterstone (or when no propertyId and raw is SF-local).
  if (SOUTH_FLORIDA_PROPERTY_IDS.has(propertyId)) {
    return { source: "south_florida", classified: sfClassifyObservedEntity(raw) };
  }
  if (!propertyId && /boca|palm beach|miami|fort lauderdale/i.test(raw)) {
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

  if (isSubjectPropertyName(raw, propertyProfile)) {
    return {
      ok: false,
      displayName: null,
      entityId: null,
      mergeKey: null,
      rejected: true,
      reason: "subject_property",
      raw,
    };
  }

  const propertyId = propertyProfile?.propertyId || null;
  // Reject known truncation / amenity artifacts before registry bind (contaminated auto-registries).
  if (
    isNonHotelEntity(raw) ||
    isLikelyArtifactEntity(raw) ||
    PROSE_FRAGMENT_PATTERNS.some((p) => p.test(raw))
  ) {
    return {
      ok: false,
      displayName: null,
      entityId: null,
      mergeKey: null,
      rejected: true,
      reason: "artifact_or_non_hotel",
      raw,
    };
  }
  if (isCrossMarketLeak(raw, propertyProfile)) {
    return {
      ok: false,
      displayName: null,
      entityId: null,
      mergeKey: null,
      rejected: true,
      reason: "cross_market_leak",
      raw,
    };
  }
  // Registry / canonical hotels first — some legal names ("The Boca Raton") would otherwise
  // trip the "starts with The" artifact heuristic.
  const { classified } = classifyRaw(propertyId, raw);
  if (classified?.identityOk && classified.entityId) {
    const hotel =
      (propertyId && registryHotelById(propertyId, classified.entityId)) ||
      classified.hotel ||
      null;
    const displayName = hotel?.canonical || classified.canonical || raw;
    if (hotel?.subject) {
      return {
        ok: false,
        displayName: null,
        entityId: null,
        mergeKey: null,
        rejected: true,
        reason: "subject_property",
        raw,
      };
    }
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
  if (
    classified &&
    ["GENERIC_PHRASE", "VENUE_ONLY", "LOCATION", "NON_HOTEL_ENTITY", "BRAND_NOT_PROPERTY", "AMBIGUOUS"].includes(
      classified.class
    )
  ) {
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

  // ADP_NO_UNBOUND_ANALYTICAL_COMPETITOR_ENTITIES — fail closed.
  // Real hotels must bind via market registry; prose/ambiguous strings must not enter analytics.
  return {
    ok: false,
    displayName: null,
    entityId: null,
    mergeKey: null,
    rejected: true,
    reason: "unbound_fail_closed",
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
