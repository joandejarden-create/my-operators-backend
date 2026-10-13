/**
 * Apify mapped to 10 Bases — repo inventory only.
 * Apify output = SIGNAL until page-validated. Never invent actor IDs.
 */

import { buildApifyActorInventory, enrichCompHotelsViaTripadvisor } from "../complete-demand-packet-v8/apify-contribution.js";
import { GDI_BASE_OF_DEMAND } from "./taxonomy.js";

const B = GDI_BASE_OF_DEMAND;

export function buildApifyTenBasesInventory() {
  const base = buildApifyActorInventory();
  return {
    ...base,
    baseMapping: [
      {
        capability: "LINKEDIN_EVENTS",
        bases: [B.PUBLISHED_EVENT_DECOMPOSITION, B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING, B.RECURRING_CORPORATE_MEETINGS, B.PHARMA_MEDICAL_ECOSYSTEM],
        inRepo: false,
        status: "NOT_AVAILABLE",
      },
      {
        capability: "LINKEDIN_COMPANY",
        bases: [B.RECURRING_CORPORATE_MEETINGS, B.CORPORATE_TRIGGER_DEMAND, B.PROJECT_WORKFORCE_DEMAND],
        inRepo: false,
        status: "NOT_AVAILABLE",
      },
      {
        capability: "EVENTBRITE_MEETUP",
        bases: [B.PUBLISHED_EVENT_DECOMPOSITION, B.PARTICIPANT_EXHIBITOR_SPONSOR_MINING, B.RECURRING_CORPORATE_MEETINGS, B.SPORTS_ENTERTAINMENT_PRODUCTION],
        inRepo: false,
        status: "NOT_AVAILABLE",
      },
      {
        capability: "GOOGLE_MAPS_PLACES",
        bases: [B.HOTEL_HISTORY_LOOKALIKE, B.PROJECT_WORKFORCE_DEMAND],
        use: "comp_sets|DMCs|agencies|contractors|phone_address_pivots",
        inRepo: false,
        status: "NOT_AVAILABLE",
      },
      {
        capability: "TICKETED_EVENTS",
        bases: [B.SPORTS_ENTERTAINMENT_PRODUCTION],
        inRepo: false,
        status: "NOT_AVAILABLE",
      },
      {
        capability: "TRIPADVISOR_HOTEL_IDENTITY",
        bases: [B.HOTEL_HISTORY_LOOKALIKE],
        inRepo: true,
        status: "AVAILABLE",
        actorRef: "maxcopell~tripadvisor",
        truthPolicy: "SIGNAL_UNTIL_PAGE_VALIDATED",
      },
    ],
  };
}

export { enrichCompHotelsViaTripadvisor };
