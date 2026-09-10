/**
 * Bethesda Marriott CORE peer governance — ADP_CORE_PEER_GOVERNANCE_V1 pattern.
 * Methodology unchanged; MIN_CORE = 4 where numeric benchmark applies.
 */

import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { BETHESDA_NORTH_ENTITY_ID } from "../execution/bethesda-marriott-foundation-v1.js";

const CC = Object.freeze({
  CORE_COMPETITOR: "CORE_COMPETITOR",
  SECONDARY_ALTERNATIVE: "SECONDARY_ALTERNATIVE",
  CONDITIONAL: "CONDITIONAL",
  NON_COMPARABLE: "NON_COMPARABLE",
});

export const BETHESDA_MARRIOTT_ROLE_OVERRIDES = Object.freeze({
  hyatt_regency_bethesda: {
    [TRAVELER_INTENTS.BUSINESS]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.LEISURE]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.COUPLES]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.FAMILY]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.GROUP_MEETING]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.WELLNESS]: CC.SECONDARY_ALTERNATIVE,
    [TRAVELER_INTENTS.ADVENTURE]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.CELEBRATION]: CC.CORE_COMPETITOR,
  },
  the_bethesdan_hotel: {
    [TRAVELER_INTENTS.BUSINESS]: CC.SECONDARY_ALTERNATIVE,
    [TRAVELER_INTENTS.LEISURE]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.COUPLES]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.FAMILY]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.GROUP_MEETING]: CC.SECONDARY_ALTERNATIVE,
    [TRAVELER_INTENTS.WELLNESS]: CC.SECONDARY_ALTERNATIVE,
    [TRAVELER_INTENTS.ADVENTURE]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.CELEBRATION]: CC.CORE_COMPETITOR,
  },
  marriott_bethesda_downtown: {
    [TRAVELER_INTENTS.BUSINESS]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.LEISURE]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.COUPLES]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.FAMILY]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.GROUP_MEETING]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.WELLNESS]: CC.SECONDARY_ALTERNATIVE,
    [TRAVELER_INTENTS.ADVENTURE]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.CELEBRATION]: CC.CORE_COMPETITOR,
  },
  [BETHESDA_NORTH_ENTITY_ID]: {
    [TRAVELER_INTENTS.BUSINESS]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.LEISURE]: CC.SECONDARY_ALTERNATIVE,
    [TRAVELER_INTENTS.COUPLES]: CC.NON_COMPARABLE,
    [TRAVELER_INTENTS.FAMILY]: CC.SECONDARY_ALTERNATIVE,
    [TRAVELER_INTENTS.GROUP_MEETING]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.WELLNESS]: CC.NON_COMPARABLE,
    [TRAVELER_INTENTS.ADVENTURE]: CC.NON_COMPARABLE,
    [TRAVELER_INTENTS.CELEBRATION]: CC.CONDITIONAL,
  },
  ac_hotel_bethesda_downtown: {
    [TRAVELER_INTENTS.BUSINESS]: CC.SECONDARY_ALTERNATIVE,
    [TRAVELER_INTENTS.LEISURE]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.COUPLES]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.FAMILY]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.GROUP_MEETING]: CC.NON_COMPARABLE,
    [TRAVELER_INTENTS.WELLNESS]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.ADVENTURE]: CC.CORE_COMPETITOR,
    [TRAVELER_INTENTS.CELEBRATION]: CC.CORE_COMPETITOR,
  },
  hilton_garden_inn_bethesda: {
    [TRAVELER_INTENTS.BUSINESS]: CC.SECONDARY_ALTERNATIVE,
    [TRAVELER_INTENTS.LEISURE]: CC.SECONDARY_ALTERNATIVE,
    [TRAVELER_INTENTS.COUPLES]: CC.SECONDARY_ALTERNATIVE,
    [TRAVELER_INTENTS.FAMILY]: CC.SECONDARY_ALTERNATIVE,
    [TRAVELER_INTENTS.GROUP_MEETING]: CC.SECONDARY_ALTERNATIVE,
    [TRAVELER_INTENTS.WELLNESS]: CC.NON_COMPARABLE,
    [TRAVELER_INTENTS.ADVENTURE]: CC.SECONDARY_ALTERNATIVE,
    [TRAVELER_INTENTS.CELEBRATION]: CC.SECONDARY_ALTERNATIVE,
  },
  residence_inn_bethesda_downtown: Object.fromEntries(
    Object.values(TRAVELER_INTENTS).map((i) => [i, CC.NON_COMPARABLE])
  ),
});

/** Frozen CORE peer lists — peers only (subject excluded). */
export const BETHESDA_MARRIOTT_STABILIZED_CORE_IDS = Object.freeze({
  [TRAVELER_INTENTS.BUSINESS]: [
    "hyatt_regency_bethesda",
    "marriott_bethesda_downtown",
    BETHESDA_NORTH_ENTITY_ID,
    "the_bethesdan_hotel",
  ],
  [TRAVELER_INTENTS.LEISURE]: [
    "hyatt_regency_bethesda",
    "the_bethesdan_hotel",
    "marriott_bethesda_downtown",
    "ac_hotel_bethesda_downtown",
  ],
  [TRAVELER_INTENTS.COUPLES]: [
    "the_bethesdan_hotel",
    "ac_hotel_bethesda_downtown",
    "hyatt_regency_bethesda",
    "marriott_bethesda_downtown",
  ],
  [TRAVELER_INTENTS.FAMILY]: [
    "hyatt_regency_bethesda",
    "marriott_bethesda_downtown",
    "the_bethesdan_hotel",
    "ac_hotel_bethesda_downtown",
  ],
  [TRAVELER_INTENTS.GROUP_MEETING]: [
    "hyatt_regency_bethesda",
    BETHESDA_NORTH_ENTITY_ID,
    "marriott_bethesda_downtown",
    "the_bethesdan_hotel",
  ],
  [TRAVELER_INTENTS.WELLNESS]: [
    "ac_hotel_bethesda_downtown",
    "hyatt_regency_bethesda",
    "the_bethesdan_hotel",
    "marriott_bethesda_downtown",
  ],
  [TRAVELER_INTENTS.ADVENTURE]: [
    "hyatt_regency_bethesda",
    "the_bethesdan_hotel",
    "ac_hotel_bethesda_downtown",
    "marriott_bethesda_downtown",
  ],
  [TRAVELER_INTENTS.CELEBRATION]: [
    "hyatt_regency_bethesda",
    "the_bethesdan_hotel",
    "marriott_bethesda_downtown",
    "ac_hotel_bethesda_downtown",
  ],
});

export const BETHESDA_PEER_CHALLENGE = Object.freeze({
  rule: "ADP_CORE_PEER_GOVERNANCE_V1",
  PEER_CONFIDENCE: Object.freeze({
    hyatt_regency_bethesda: "HIGH",
    the_bethesdan_hotel: "HIGH",
    marriott_bethesda_downtown: "HIGH",
    [BETHESDA_NORTH_ENTITY_ID]: "MEDIUM",
    ac_hotel_bethesda_downtown: "MEDIUM",
    hilton_garden_inn_bethesda: "MEDIUM_SECONDARY",
    residence_inn_bethesda_downtown: "HIGH_EXCLUDE",
  }),
  noLowConfidenceCoreFrozen: true,
});
