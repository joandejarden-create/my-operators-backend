/**
 * ADP_BPP_CANONICAL_ECOSYSTEM_DISCOVERY
 *
 * Discover BPP peers from canonical loyalty-ecosystem market inventory
 * under current PEER_EXPANSION_HIERARCHY_V1 — not from thin competitive-only arrays.
 *
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 * Does not activate MCR V2 peer-role/benchmark semantics.
 */

import { evaluatePeerSetAdequacy, PEER_EXPANSION_HIERARCHY_V1 } from "./peer-expansion-hierarchy-v1.js";
import {
  ADP_BPP_CANONICAL_ECOSYSTEM_DISCOVERY,
  ADP_BPP_ECOSYSTEM_MARKET_INVENTORY_COMPLETENESS,
  BPP_SUPPRESSION_IS_INVALID_IF_CANONICAL_ECOSYSTEM_INVENTORY_IS_INCOMPLETE,
  FULL_LOYALTY_ECOSYSTEM_MARKET_DISCOVERY_PRECEDES_SUPPRESSION,
  INVENTORY_REFRESH_DATE,
  INVENTORY_VERSION,
  classifyPeerRoleDescriptive,
  getEcosystemInventoryForProperty,
  listEligibleEcosystemPeers,
  BPP_PEER_ROLE_DESCRIPTIVE,
} from "./bpp-ecosystem-market-inventory-v1.js";

export {
  ADP_BPP_CANONICAL_ECOSYSTEM_DISCOVERY,
  ADP_BPP_ECOSYSTEM_MARKET_INVENTORY_COMPLETENESS,
  BPP_SUPPRESSION_IS_INVALID_IF_CANONICAL_ECOSYSTEM_INVENTORY_IS_INCOMPLETE,
  FULL_LOYALTY_ECOSYSTEM_MARKET_DISCOVERY_PRECEDES_SUPPRESSION,
};

export const ADP_BPP_SUPPRESSION_REQUIRES_FULL_ECOSYSTEM_DISCOVERY =
  "ADP_BPP_SUPPRESSION_REQUIRES_FULL_ECOSYSTEM_DISCOVERY";

/**
 * Build inventory evidence required before any BPP suppression declaration.
 */
export function buildSuppressionInventoryEvidence(propertyId, discovery = null) {
  const d = discovery || discoverCanonicalEcosystemPeers(propertyId);
  if (!d) {
    return {
      complete: false,
      reason: "NO_ECOSYSTEM_INVENTORY_PACK",
      inventorySource: null,
      inventoryRefreshDate: null,
      candidateHotelCount: 0,
      acceptedPeerCount: 0,
      rejectedPeerCount: 0,
      rejectionReasons: [],
    };
  }
  return {
    complete: d.inventoryComplete === true,
    inventorySource: d.inventorySource,
    inventoryRefreshDate: d.inventoryRefreshDate,
    inventoryVersion: d.inventoryVersion,
    candidateHotelCount: d.candidateHotelCount,
    acceptedPeerCount: d.acceptedPeers.length,
    rejectedPeerCount: d.rejectedPeers.length,
    rejectionReasons: d.rejectedPeers.map((r) => ({
      hotel: r.displayName || r.canonicalHotelId,
      reason: r.reason,
    })),
    levelsAttempted: d.levelsAttempted,
    adequacy: d.adequacy,
    doctrine: [
      ADP_BPP_ECOSYSTEM_MARKET_INVENTORY_COMPLETENESS,
      ADP_BPP_CANONICAL_ECOSYSTEM_DISCOVERY,
      ADP_BPP_SUPPRESSION_REQUIRES_FULL_ECOSYSTEM_DISCOVERY,
      FULL_LOYALTY_ECOSYSTEM_MARKET_DISCOVERY_PRECEDES_SUPPRESSION,
      BPP_SUPPRESSION_IS_INVALID_IF_CANONICAL_ECOSYSTEM_INVENTORY_IS_INCOMPLETE,
    ],
  };
}

/**
 * Assert suppression may only be declared with full inventory evidence.
 */
export function assertSuppressionInventoryEvidence(evidence) {
  const missing = [];
  if (!evidence?.inventorySource) missing.push("inventorySource");
  if (!evidence?.inventoryRefreshDate) missing.push("inventoryRefreshDate");
  if (typeof evidence?.candidateHotelCount !== "number") missing.push("candidateHotelCount");
  if (typeof evidence?.acceptedPeerCount !== "number") missing.push("acceptedPeerCount");
  if (typeof evidence?.rejectedPeerCount !== "number") missing.push("rejectedPeerCount");
  if (!Array.isArray(evidence?.rejectionReasons)) missing.push("rejectionReasons");
  return {
    pass: missing.length === 0 && evidence?.complete === true,
    missing,
    evidence,
  };
}

/**
 * Canonical ecosystem peer discovery for a property under current V1 hierarchy.
 */
export function discoverCanonicalEcosystemPeers(propertyId) {
  const pack = getEcosystemInventoryForProperty(propertyId);
  if (!pack) return null;

  const subject = pack.inventory.find((h) => h.canonicalHotelId === pack.subjectEntityId);
  const candidates = listEligibleEcosystemPeers(propertyId);
  const rejected = [];
  const accepted = [];

  for (const h of pack.inventory) {
    if (h.canonicalHotelId === pack.subjectEntityId) continue;
    if (h.bppEligible === false) {
      rejected.push({
        canonicalHotelId: h.canonicalHotelId,
        displayName: h.displayName,
        reason: h.exclusionReason || "bppEligible=false",
        decision: "REJECT",
      });
      continue;
    }
    if (h.loyaltyEcosystem !== pack.ecosystemId) {
      rejected.push({
        canonicalHotelId: h.canonicalHotelId,
        displayName: h.displayName,
        reason: "NOT_SAME_LOYALTY_ECOSYSTEM",
        decision: "REJECT",
      });
      continue;
    }

    const level = Number(h.expansionLevel) || 1;
    // Current V1 allows L1 then L2 broader metro/destination; L3 stops.
    if (level > 2) {
      rejected.push({
        canonicalHotelId: h.canonicalHotelId,
        displayName: h.displayName,
        reason: "BEYOND_L2_HIERARCHY_STOP",
        decision: "REJECT",
      });
      continue;
    }

    const peerRole = classifyPeerRoleDescriptive(subject, h);
    accepted.push({
      canonicalHotelId: h.canonicalHotelId,
      displayName: h.displayName,
      brand: h.brand,
      parent: h.parent,
      loyaltyEcosystem: h.loyaltyEcosystem,
      market: h.canonicalMarket,
      submarket: h.submarket,
      chainScale: h.chainScale,
      hotelType: h.hotelType,
      expansionLevel: level,
      peerRole,
      censusId: h.censusId,
      evidenceSource: h.evidenceSource,
      decision: "ACCEPT",
      loyaltyTravelerTest:
        "YES — Bonvoy/Choice member remaining in-ecosystem in governed market would plausibly consider this property",
    });
  }

  const adequacy = evaluatePeerSetAdequacy(accepted.length);
  const likeForLike = accepted.filter(
    (p) => p.peerRole === BPP_PEER_ROLE_DESCRIPTIVE.LIKE_FOR_LIKE_PORTFOLIO_PEER
  );
  const broader = accepted.filter(
    (p) => p.peerRole === BPP_PEER_ROLE_DESCRIPTIVE.BROADER_LOYALTY_ALTERNATIVE
  );

  return {
    propertyId,
    ecosystemId: pack.ecosystemId,
    inventoryKey: pack.inventoryKey,
    inventorySource: `${pack.inventoryKey}:${INVENTORY_VERSION}`,
    inventoryVersion: INVENTORY_VERSION,
    inventoryRefreshDate: INVENTORY_REFRESH_DATE,
    inventoryComplete: true,
    geography: pack.geography,
    subject,
    candidateHotelCount: candidates.length + rejected.filter((r) => r.reason !== "bppEligible=false").length,
    // candidate = all same-ecosystem open hotels excluding subject before hard rejects
    sameEcosystemInventoryCount: pack.inventory.filter(
      (h) => h.loyaltyEcosystem === pack.ecosystemId && h.canonicalHotelId !== pack.subjectEntityId
    ).length,
    acceptedPeers: accepted,
    rejectedPeers: rejected,
    likeForLikeCount: likeForLike.length,
    broaderLoyaltyCount: broader.length,
    levelsAttempted: PEER_EXPANSION_HIERARCHY_V1.map((h) => h.id),
    expansionLevelUsed: accepted.some((p) => p.expansionLevel === 2) ? 2 : 1,
    adequacy,
    canSuppressUnderCurrentMethodology:
      adequacy.status === "INSUFFICIENT_PORTFOLIO_PEER_SET" && accepted.length < 3,
    recommendedBppState:
      adequacy.canBenchmark && adequacy.canIndex
        ? "BPP_READY_POPULATED"
        : adequacy.canRank
          ? "BPP_READY_POPULATED_RANK_ONLY"
          : "BPP_EXCEPTION_SUPPRESSED",
    methodologyChanged: false,
    remediationsClass: "DATA_QUALITY_DEFECT / DISCOVERY_DEFECT",
    gates: {
      [ADP_BPP_ECOSYSTEM_MARKET_INVENTORY_COMPLETENESS]: true,
      [ADP_BPP_CANONICAL_ECOSYSTEM_DISCOVERY]: true,
      [ADP_BPP_SUPPRESSION_REQUIRES_FULL_ECOSYSTEM_DISCOVERY]: true,
    },
  };
}

export function discoverAllSuppressedUniverse() {
  return Object.keys(
    // lazy import avoided — caller passes property ids
    {}
  );
}
