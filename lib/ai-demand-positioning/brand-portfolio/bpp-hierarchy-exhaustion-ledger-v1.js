/**
 * BPP peer-discovery hierarchy exhaustion ledger.
 * BPP_POPULATION_IS_EXPECTED_FOR_AFFILIATED_HOTELS.
 * Suppression is allowed only after L1→L2→L3 stop with no methodology-compliant peer set
 * AND full canonical ecosystem-market inventory evidence.
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 */

import { PEER_EXPANSION_HIERARCHY_V1 } from "./peer-expansion-hierarchy-v1.js";
import {
  ADP_BPP_SUPPRESSION_REQUIRES_FULL_ECOSYSTEM_DISCOVERY,
  buildSuppressionInventoryEvidence,
  discoverCanonicalEcosystemPeers,
} from "./bpp-canonical-ecosystem-discovery-v1.js";
import {
  INVENTORY_REFRESH_DATE,
  INVENTORY_VERSION,
} from "./bpp-ecosystem-market-inventory-v1.js";

export const BPP_HIERARCHY_EXHAUSTION_LEDGER_VERSION =
  "ADP_BPP_HIERARCHY_EXHAUSTION_LEDGER_V1_INVENTORY_COMPLETE_20260910";

/**
 * Affiliated properties where full canonical ecosystem discovery still cannot
 * certify ≥3 same-ecosystem peers under current PEER_EXPANSION_HIERARCHY_V1.
 * Do not invent peers. Properties repaired by inventory enrichment are removed.
 */
export const BPP_HIERARCHY_EXHAUSTION_LEDGER_V1 = Object.freeze({
  adp_radisson_santo_domingo: Object.freeze({
    propertyId: "adp_radisson_santo_domingo",
    ecosystemId: "choice_privileges",
    market: "Greater Santo Domingo",
    run: true,
    hierarchyExhausted: true,
    levelsAttempted: PEER_EXPANSION_HIERARCHY_V1.map((h) => h.id),
    candidateCount: 0,
    certifiedPeerCount: 0,
    acceptedPeers: Object.freeze([]),
    rejectedPeers: Object.freeze([
      {
        name: "JW Marriott / Renaissance / Marriott Piantini / Hilton / Sheraton",
        reason: "Wrong loyalty ecosystem for Choice Privileges lens",
      },
    ]),
    suppressionReason:
      "NO_METHODOLOGY_COMPLIANT_CERTIFIED_PEER_SET_AFTER_HIERARCHY_EXHAUSTION",
    sourceRegistry: "santo_domingo_choice_inventory_v1",
    note:
      "Full Choice Privileges search across Greater Santo Domingo: only Radisson Hotel Santo Domingo confirmed active. Inventory complete; peer count remains 0.",
    inventoryEvidence: Object.freeze({
      inventorySource: `santo_domingo_choice:${INVENTORY_VERSION}`,
      inventoryRefreshDate: INVENTORY_REFRESH_DATE,
      candidateHotelCount: 0,
      acceptedPeerCount: 0,
      rejectedPeerCount: 1,
      rejectionReasons: Object.freeze([
        {
          hotel: "Non-Choice Greater Santo Domingo hotels",
          reason: "NOT_SAME_LOYALTY_ECOSYSTEM",
        },
      ]),
      [ADP_BPP_SUPPRESSION_REQUIRES_FULL_ECOSYSTEM_DISCOVERY]: true,
    }),
    remediationsClass: "NONE_REMAINING_AFTER_FULL_INVENTORY",
  }),

  adp_faranda_collection_bogota: Object.freeze({
    propertyId: "adp_faranda_collection_bogota",
    ecosystemId: "choice_privileges",
    market: "Bogotá",
    run: true,
    hierarchyExhausted: true,
    levelsAttempted: PEER_EXPANSION_HIERARCHY_V1.map((h) => h.id),
    candidateCount: 1,
    certifiedPeerCount: 0,
    acceptedPeers: Object.freeze([
      {
        name: "Radisson Bogotá Metrotel",
        entityId: "radisson_bogota_metrotel",
        level: "L1_SAME_ECOSYSTEM_SAME_HOTEL_MARKET",
        decision: "INSUFFICIENT_ALONE",
        note: "DATA_QUALITY repair — Metrotel now in canonical Bogotá Choice inventory; count 1 < rankUsable 3",
      },
    ]),
    rejectedPeers: Object.freeze([
      {
        name: "JW Marriott Bogotá / W Bogotá / Bogotá Marriott",
        reason: "Marriott Bonvoy — wrong ecosystem for Choice Privileges lens",
      },
      {
        name: "Faranda Collection Zona G / Faranda Express Belvedere",
        reason: "Choice Privileges membership not confirmed on canonical Choice inventory",
      },
    ]),
    suppressionReason:
      "NO_METHODOLOGY_COMPLIANT_CERTIFIED_PEER_SET_AFTER_HIERARCHY_EXHAUSTION",
    sourceRegistry: "bogota_choice_inventory_v1",
    note:
      "Full Choice Privileges Bogotá inventory: Faranda Collection Bogotá + Radisson Bogotá Metrotel only. Metrotel added as DATA_QUALITY_DEFECT fix; still below rank≥3.",
    inventoryEvidence: Object.freeze({
      inventorySource: `bogota_choice:${INVENTORY_VERSION}`,
      inventoryRefreshDate: INVENTORY_REFRESH_DATE,
      candidateHotelCount: 1,
      acceptedPeerCount: 1,
      rejectedPeerCount: 2,
      rejectionReasons: Object.freeze([
        {
          hotel: "JW Marriott Bogotá / W Bogotá / Bogotá Marriott",
          reason: "NOT_SAME_LOYALTY_ECOSYSTEM",
        },
        {
          hotel: "Faranda Collection Zona G / Faranda Express Belvedere",
          reason: "CHOICE_PRIVILEGES_MEMBERSHIP_UNCONFIRMED",
        },
      ]),
      [ADP_BPP_SUPPRESSION_REQUIRES_FULL_ECOSYSTEM_DISCOVERY]: true,
    }),
    remediationsClass: "DATA_QUALITY_DEFECT_FIXED_STILL_INSUFFICIENT_PEER_COUNT",
  }),
});

export function getBppHierarchyExhaustion(propertyId) {
  const row = BPP_HIERARCHY_EXHAUSTION_LEDGER_V1[propertyId] || null;
  if (!row) return null;
  // Prefer live discovery evidence when inventory pack exists
  const discovery = discoverCanonicalEcosystemPeers(propertyId);
  if (!discovery) return row;
  const evidence = buildSuppressionInventoryEvidence(propertyId, discovery);
  return {
    ...row,
    candidateCount: discovery.acceptedPeers.length,
    acceptedPeers: discovery.acceptedPeers.map((p) =>
      Object.freeze({
        name: p.displayName,
        entityId: p.canonicalHotelId,
        level: p.expansionLevel === 2 ? "L2_SAME_ECOSYSTEM_BROADER_DESTINATION" : "L1_SAME_ECOSYSTEM_SAME_HOTEL_MARKET",
        decision: discovery.adequacy?.canRank ? "ACCEPTED" : "INSUFFICIENT_WITH_SET",
        peerRole: p.peerRole,
      })
    ),
    inventoryEvidence: evidence,
  };
}
