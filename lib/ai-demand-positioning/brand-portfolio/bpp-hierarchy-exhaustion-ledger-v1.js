/**
 * BPP peer-discovery hierarchy exhaustion ledger.
 * BPP_POPULATION_IS_EXPECTED_FOR_AFFILIATED_HOTELS.
 * Suppression is allowed only after L1→L2→L3 stop with no methodology-compliant peer set.
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 */

import { PEER_EXPANSION_HIERARCHY_V1 } from "./peer-expansion-hierarchy-v1.js";

export const BPP_HIERARCHY_EXHAUSTION_LEDGER_VERSION =
  "ADP_BPP_HIERARCHY_EXHAUSTION_LEDGER_V1";

/**
 * Affiliated properties where governed L1/L2 discovery inside market entity
 * registries cannot certify ≥3 same-ecosystem peers. Do not invent peers.
 */
export const BPP_HIERARCHY_EXHAUSTION_LEDGER_V1 = Object.freeze({
  adp_st_regis_cap_cana: Object.freeze({
    propertyId: "adp_st_regis_cap_cana",
    ecosystemId: "marriott_bonvoy",
    market: "Cap Cana / Punta Cana",
    run: true,
    hierarchyExhausted: true,
    levelsAttempted: PEER_EXPANSION_HIERARCHY_V1.map((h) => h.id),
    candidateCount: 1,
    certifiedPeerCount: 0,
    acceptedPeers: Object.freeze([
      {
        name: "The Westin Puntacana Resort & Club",
        entityId: "westin_punta_cana",
        level: "L1_OR_L2_SAME_DESTINATION",
        decision: "INSUFFICIENT_ALONE",
        note: "Only Bonvoy-named peer in Cap Cana registry — count 1 < rankUsable 3",
      },
    ]),
    rejectedPeers: Object.freeze([
      {
        name: "Eden Roc Cap Cana",
        reason: "Different loyalty ecosystem / independent luxury",
      },
      {
        name: "Sanctuary Cap Cana",
        reason: "Hilton Honors — wrong ecosystem",
      },
    ]),
    suppressionReason:
      "NO_METHODOLOGY_COMPLIANT_CERTIFIED_PEER_SET_AFTER_HIERARCHY_EXHAUSTION",
    sourceRegistry: "cap_cana_entity_registry_v1",
  }),

  adp_jw_marriott_monterrey_valle: Object.freeze({
    propertyId: "adp_jw_marriott_monterrey_valle",
    ecosystemId: "marriott_bonvoy",
    market: "Monterrey Valle",
    run: true,
    hierarchyExhausted: true,
    levelsAttempted: PEER_EXPANSION_HIERARCHY_V1.map((h) => h.id),
    candidateCount: 2,
    certifiedPeerCount: 0,
    acceptedPeers: Object.freeze([
      {
        name: "The Westin Monterrey Valle",
        entityId: "westin_monterrey_valle",
        level: "L1_SAME_ECOSYSTEM_SAME_HOTEL_MARKET",
        decision: "INSUFFICIENT_WITH_PAIR",
      },
      {
        name: "AC Hotel by Marriott Monterrey Valle",
        entityId: "ac_hotel_monterrey_valle",
        level: "L1_SAME_ECOSYSTEM_SAME_HOTEL_MARKET",
        decision: "INSUFFICIENT_WITH_PAIR",
      },
    ]),
    rejectedPeers: Object.freeze([
      {
        name: "Hilton Garden Inn / Holiday Inn / Novotel peers",
        reason: "Different loyalty ecosystems",
      },
    ]),
    suppressionReason:
      "NO_METHODOLOGY_COMPLIANT_CERTIFIED_PEER_SET_AFTER_HIERARCHY_EXHAUSTION",
    sourceRegistry: "monterrey_valle_entity_registry_v1",
    note: "Only 2 Bonvoy peers in governed registry — below rankUsable 3",
  }),

  adp_westin_monterrey_valle: Object.freeze({
    propertyId: "adp_westin_monterrey_valle",
    ecosystemId: "marriott_bonvoy",
    market: "Monterrey Valle",
    run: true,
    hierarchyExhausted: true,
    levelsAttempted: PEER_EXPANSION_HIERARCHY_V1.map((h) => h.id),
    candidateCount: 2,
    certifiedPeerCount: 0,
    acceptedPeers: Object.freeze([
      {
        name: "JW Marriott Hotel Monterrey Valle",
        entityId: "jw_marriott_monterrey_valle",
        level: "L1_SAME_ECOSYSTEM_SAME_HOTEL_MARKET",
        decision: "INSUFFICIENT_WITH_PAIR",
      },
      {
        name: "AC Hotel by Marriott Monterrey Valle",
        entityId: "ac_hotel_monterrey_valle",
        level: "L1_SAME_ECOSYSTEM_SAME_HOTEL_MARKET",
        decision: "INSUFFICIENT_WITH_PAIR",
      },
    ]),
    rejectedPeers: Object.freeze([
      {
        name: "Non-Bonvoy Valle peers",
        reason: "Different loyalty ecosystems",
      },
    ]),
    suppressionReason:
      "NO_METHODOLOGY_COMPLIANT_CERTIFIED_PEER_SET_AFTER_HIERARCHY_EXHAUSTION",
    sourceRegistry: "monterrey_valle_entity_registry_v1",
    note: "Only 2 Bonvoy peers in governed registry — below rankUsable 3",
  }),

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
        name: "JW Marriott / Renaissance / Marriott Piantini / Hilton",
        reason: "Wrong loyalty ecosystem for Choice Privileges lens",
      },
    ]),
    suppressionReason:
      "NO_METHODOLOGY_COMPLIANT_CERTIFIED_PEER_SET_AFTER_HIERARCHY_EXHAUSTION",
    sourceRegistry: "santo_domingo_entity_registry_v1",
    note: "Zero same-market Choice Privileges peers in governed registry",
  }),

  adp_faranda_collection_bogota: Object.freeze({
    propertyId: "adp_faranda_collection_bogota",
    ecosystemId: "choice_privileges",
    market: "Bogotá Norte",
    run: true,
    hierarchyExhausted: true,
    levelsAttempted: PEER_EXPANSION_HIERARCHY_V1.map((h) => h.id),
    candidateCount: 0,
    certifiedPeerCount: 0,
    acceptedPeers: Object.freeze([]),
    rejectedPeers: Object.freeze([
      {
        name: "JW Marriott Bogotá / W Bogotá / Bogotá Marriott",
        reason: "Marriott Bonvoy — wrong ecosystem for Choice Privileges lens",
      },
    ]),
    suppressionReason:
      "NO_METHODOLOGY_COMPLIANT_CERTIFIED_PEER_SET_AFTER_HIERARCHY_EXHAUSTION",
    sourceRegistry: "bogota_norte_entity_registry_v1",
    note: "Zero same-market Choice Privileges peers in governed registry",
  }),

  adp_hotel_caribe_faranda_grand: Object.freeze({
    propertyId: "adp_hotel_caribe_faranda_grand",
    ecosystemId: "choice_privileges",
    market: "Cartagena / Bocagrande",
    run: true,
    hierarchyExhausted: true,
    levelsAttempted: PEER_EXPANSION_HIERARCHY_V1.map((h) => h.id),
    candidateCount: 0,
    certifiedPeerCount: 0,
    acceptedPeers: Object.freeze([]),
    rejectedPeers: Object.freeze([
      {
        name: "Hilton / Sofitel / Estelar / independent luxury peers",
        reason: "Wrong loyalty ecosystem for Choice Privileges lens",
      },
    ]),
    suppressionReason:
      "NO_METHODOLOGY_COMPLIANT_CERTIFIED_PEER_SET_AFTER_HIERARCHY_EXHAUSTION",
    sourceRegistry: "cartagena_bocagrande_entity_registry_v1",
    note: "Zero same-market Choice Privileges peers in governed registry",
  }),
});

export function getBppHierarchyExhaustion(propertyId) {
  return BPP_HIERARCHY_EXHAUSTION_LEDGER_V1[propertyId] || null;
}
