/**
 * Shared competitive entity resolution — fail closed across markets.
 *
 * Invariant: a non-Waterstone ADP property MUST NOT inherit South Florida /
 * Waterstone entity IDs merely because its own registry is absent or a mention
 * happens to match a Boca alias.
 */

import {
  canonicalizeForProperty,
  getEntityRegistryForProperty,
} from "./adp-property-entity-registries.js";
import { isGovernedNonWaterstoneProperty } from "./property-core-governance-data.js";
import { canonicalizeToEntityId as sfCanonicalizeToEntityId } from "./south-florida-entity-registry.js";

export const ADP_COMPETITIVE_ENTITY_RESOLVE_VERSION =
  "adp_competitive_entity_resolve_fail_closed_v1";

export const WATERSTONE_PROPERTY_ID = "adp_waterstone_boca_raton";

function mentionText(name) {
  if (typeof name === "string") return name;
  return String(name?.name || name?.canonicalName || name?.raw || "").trim();
}

/**
 * @param {string|object} name
 * @param {{ propertyId?: string }|null} propertyProfile
 * @returns {string|null} canonical entityId or null when unresolved / fail-closed
 */
export function resolveCompetitiveEntityIdFailClosed(name, propertyProfile = null) {
  const text = mentionText(name);
  if (!text) return null;
  const propertyId = propertyProfile?.propertyId || null;

  // Prefer the property's own market registry whenever one exists.
  if (propertyId && getEntityRegistryForProperty(propertyId)) {
    return canonicalizeForProperty(propertyId, text);
  }

  // Governed non-Waterstone without a loaded registry: still fail closed
  // (canonicalizeForProperty returns null when registry missing).
  if (propertyId && isGovernedNonWaterstoneProperty(propertyId)) {
    return canonicalizeForProperty(propertyId, text);
  }

  // South Florida canonicalize is Waterstone-local only.
  if (!propertyId || propertyId === WATERSTONE_PROPERTY_ID) {
    return sfCanonicalizeToEntityId(text);
  }

  // Known non-Waterstone property without a market registry: do NOT borrow Boca.
  return null;
}

/**
 * Competitive analysis may be certified only when a market registry exists
 * (or the property is Waterstone with the SF registry).
 */
export function competitiveAnalysisCertificationGate(propertyId) {
  if (!propertyId) {
    return {
      certified: false,
      status: "NOT_CERTIFIED",
      reason: "MISSING_PROPERTY_ID",
      marketRegistryPresent: false,
    };
  }
  if (propertyId === WATERSTONE_PROPERTY_ID) {
    return {
      certified: true,
      status: "ELIGIBLE",
      reason: "WATERSTONE_SOUTH_FLORIDA_REGISTRY",
      marketRegistryPresent: true,
    };
  }
  const reg = getEntityRegistryForProperty(propertyId);
  if (!reg) {
    return {
      certified: false,
      status: "NOT_CERTIFIED",
      reason: "MARKET_REGISTRY_MISSING",
      marketRegistryPresent: false,
      setupRequired: true,
    };
  }
  return {
    certified: true,
    status: "ELIGIBLE",
    reason: "PROPERTY_MARKET_REGISTRY_PRESENT",
    marketRegistryPresent: true,
    registryVersion: reg.version || null,
  };
}
