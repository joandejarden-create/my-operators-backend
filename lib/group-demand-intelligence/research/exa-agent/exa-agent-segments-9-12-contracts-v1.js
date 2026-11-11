/**
 * Exa Agent Discovery + Deep Research contracts for Segments 9–12.
 * Schema/version contracts only — dispatch via shared exa-agent-provider-adapter-v1.
 * Never invents API parameters. Live HTTP requires dual authorization separately.
 */

export const EXA_SEGMENTS_9_12_CONTRACTS_V1 =
  "gdi-exa-segments-9-12-contracts-v1";

const SHARED_DISCOVERY_SCHEMA = Object.freeze({
  type: "object",
  required: ["candidates"],
  properties: {
    candidates: {
      type: "array",
      items: {
        type: "object",
        required: ["organizationName", "demandMechanismSummary", "evidenceUrls"],
        properties: {
          organizationName: { type: "string" },
          programmeName: { type: "string" },
          demandMechanismSummary: { type: "string" },
          revenueTypes: { type: "array", items: { type: "string" } },
          guestroomDemandStatus: {
            type: "string",
            enum: ["NONE_CONFIRMED", "UNKNOWN", "EVIDENCE_SUPPORTED"],
          },
          relevantBuyerRoles: { type: "array", items: { type: "string" } },
          evidenceUrls: { type: "array", items: { type: "string" } },
          uncertainties: { type: "array", items: { type: "string" } },
          routingNotes: { type: "string" },
        },
      },
    },
  },
});

function contract(id, label, discoveryFocus, deepFocus, revenueHints) {
  return Object.freeze({
    version: EXA_SEGMENTS_9_12_CONTRACTS_V1,
    discoveryContractId: id.discovery,
    deepResearchContractId: id.deep,
    segmentLabel: label,
    discoveryObjective: discoveryFocus,
    deepResearchObjective: deepFocus,
    preferredRevenueTypes: Object.freeze([...revenueHints]),
    guestroomRequired: false,
    discoverySchema: SHARED_DISCOVERY_SCHEMA,
    multilingualHint:
      "Use local-language hospitality and meeting terminology appropriate to the hotel market; do not hardcode one country.",
    prohibition:
      "Do not invent buyers, volumes, RFPs, committed bookings, or next editions. Do not qualify from website marketing copy alone.",
  });
}

export const SEGMENT_9_LOCAL_CATERING_CONTRACTS = contract(
  {
    discovery: "gdi-exa-local-catering-day-meetings-discovery-v1",
    deep: "gdi-exa-local-catering-day-meetings-deep-research-v1",
  },
  "Local Catering & Day Meetings",
  "Discover organizations and recurring local programmes that can generate hotel meeting-space, catering, breakfast, workshop or private-dining revenue WITHOUT requiring overnight guestrooms.",
  "Verify activity evidence, facility fit, named commercially relevant contact, catering/meeting revenue pathway, frequency/cycle, competitor venues, and a concrete sales approach. Zero room nights is allowed.",
  ["meeting_space_rental", "local_catering", "private_dining", "av_event_ancillary"],
);

export const SEGMENT_10_CORPORATE_CELEBRATIONS_CONTRACTS = contract(
  {
    discovery: "gdi-exa-corporate-celebrations-discovery-v1",
    deep: "gdi-exa-corporate-celebrations-deep-research-v1",
  },
  "Corporate Celebrations & Private Events",
  "Discover corporate-sponsored celebrations and private business events (holiday parties, recognition galas, product launches, client appreciation) with banquet/catering demand. Distinguish from incentive trips, day training, association congresses, and family weddings.",
  "Verify event type, historical venue, whether a future edition is confirmed (do not invent), banquet demand, relevant HR/marketing/events contact, and hotel banquet fit. Guestrooms optional.",
  ["banquets", "local_catering", "private_dining", "av_event_ancillary", "meeting_space_rental"],
);

export const SEGMENT_11_TRAVEL_TRADE_CONTRACTS = contract(
  {
    discovery: "gdi-exa-travel-trade-intermediaries-discovery-v1",
    deep: "gdi-exa-travel-trade-intermediaries-deep-research-v1",
  },
  "Travel Trade & Group Intermediaries",
  "Discover DMCs, tour operators, incentive houses and group intermediaries with evidence of destination-relevant group production for this hotel — not websites that merely claim group services.",
  "Distinguish intermediary account development from a specific group programme. Link programmes without duplicating. Verify buyer/account manager, destination fit, and realistic production pathway.",
  ["group_guestrooms", "resort_all_inclusive_group", "meeting_space_rental", "banquets"],
);

export const SEGMENT_12_RELOCATION_CONTRACTS = contract(
  {
    discovery: "gdi-exa-relocation-extended-stay-discovery-v1",
    deep: "gdi-exa-relocation-extended-stay-deep-research-v1",
  },
  "Relocation & Extended Stay",
  "Discover verified corporate relocation, mobility, expatriate or temporary assignment activity that may require temporary lodging — not general hiring announcements.",
  "Verify assignment mechanism, duration, accommodation type, mobility/HR contact, hotel product fit, and distinguish temporary lodging from permanent housing. Coordinate with Projects (Seg 7) when construction deployment is the principal mechanism.",
  ["extended_stay", "guestrooms"],
);

export const SEGMENTS_9_12_CONTRACTS_BY_ID = Object.freeze({
  local_catering_day_meetings: SEGMENT_9_LOCAL_CATERING_CONTRACTS,
  corporate_celebrations_private_events: SEGMENT_10_CORPORATE_CELEBRATIONS_CONTRACTS,
  travel_trade_group_intermediaries: SEGMENT_11_TRAVEL_TRADE_CONTRACTS,
  relocation_extended_stay: SEGMENT_12_RELOCATION_CONTRACTS,
});

export function getSegment912ExaContracts(segmentId) {
  return SEGMENTS_9_12_CONTRACTS_BY_ID[segmentId] || null;
}

/** Schema-valid mock discovery payload for zero-paid adapter boundary tests. */
export function buildMockDiscoveryStructured(segmentId, hotelId = "") {
  const c = getSegment912ExaContracts(segmentId);
  if (!c) return null;
  return {
    version: c.discoveryContractId,
    segmentId,
    hotelId,
    candidates: [
      {
        organizationName: "Fixture Organization",
        programmeName: `${c.segmentLabel} fixture programme`,
        demandMechanismSummary: c.discoveryObjective.slice(0, 180),
        revenueTypes: [...c.preferredRevenueTypes],
        guestroomDemandStatus:
          segmentId === "relocation_extended_stay" ||
          segmentId === "travel_trade_group_intermediaries"
            ? "UNKNOWN"
            : "NONE_CONFIRMED",
        relevantBuyerRoles: ["Meeting Planner", "Events Coordinator", "Account Manager"],
        evidenceUrls: [],
        uncertainties: [
          "Live Exa research NOT VERIFIED — mock fixture only",
          "Do not contact — zero-paid certification fixture",
        ],
        routingNotes: "Isolated fixture; not for customer inventory.",
      },
    ],
    note: "Zero-paid mock at provider adapter boundary",
  };
}
