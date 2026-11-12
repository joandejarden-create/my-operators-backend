/**
 * Versioned Exa Discovery + Deep Research contracts for Professional Event Planners.
 * Zero-paid by default — contracts only; no live provider calls from this module.
 */
export const EXA_PROFESSIONAL_EVENT_PLANNER_DISCOVERY_V1 =
  "gdi-exa-professional-event-planner-discovery-v1";
export const EXA_PROFESSIONAL_EVENT_PLANNER_DEEP_V1 =
  "gdi-exa-professional-event-planner-deep-research-v1";

export const PLANNER_DISCOVERY_CHANNELS = Object.freeze([
  "official_planner_websites",
  "verified_planner_portfolios",
  "wedding_and_event_case_studies",
  "event_venues_naming_planners",
  "professional_event_organizations",
  "industry_publications",
  "official_wedding_event_vendor_directories",
  "public_event_credits",
  "planner_produced_event_announcements",
  "event_photos_with_planner_attribution",
  "venue_hotel_supplier_directories",
  "public_professional_profiles",
]);

/** Multilingual / local terminology seeds — adapt per country/market. */
export const PLANNER_TERMINOLOGY_BY_MARKET = Object.freeze({
  mexico: [
    "organizadores de bodas",
    "wedding planner Ciudad de México",
    "organizadores de eventos corporativos",
    "agencias de eventos",
    "producción de eventos",
    "bodas destino",
    "coordinadores de eventos",
    "planeación de celebraciones",
    "eventos empresariales",
    "banquetes y eventos",
  ],
  "mexico-cancun": [
    "destination wedding planner Cancún",
    "organizadores de bodas Cancún",
    "bodas destino Riviera Maya",
    "wedding planner Cancun",
    "resort wedding planner Quintana Roo",
  ],
  "us-bethesda": [
    "wedding planner Bethesda Maryland",
    "event planner Bethesda DC metro",
    "corporate event planner Montgomery County",
    "social event producer Washington DC suburbs",
  ],
  "us-san-diego": [
    "wedding planner San Diego downtown",
    "event planner San Diego",
    "corporate celebration planner San Diego",
    "private event producer San Diego Bay",
  ],
  default: [
    "wedding planner",
    "destination wedding planner",
    "corporate event management company",
    "event planning agency",
    "meeting planner",
    "boutique event producer",
  ],
});

export function resolvePlannerTerminology({ hotel = {}, marketKey = null } = {}) {
  const key =
    marketKey ||
    hotel.plannerTerminologyKey ||
    (/\bcanc[uú]n|riviera maya|quintana roo/i.test(
      `${hotel.market || ""} ${hotel.city || ""} ${hotel.name || ""}`,
    )
      ? "mexico-cancun"
      : /\bm[eé]xico|cdmx|ciudad de m[eé]xico|chapultepec/i.test(
            `${hotel.market || ""} ${hotel.city || ""} ${hotel.name || ""}`,
          )
        ? "mexico"
        : /\bbethesda/i.test(`${hotel.market || ""} ${hotel.city || ""}`)
          ? "us-bethesda"
          : /\bsan diego/i.test(`${hotel.market || ""} ${hotel.city || ""}`)
            ? "us-san-diego"
            : "default");
  return PLANNER_TERMINOLOGY_BY_MARKET[key] || PLANNER_TERMINOLOGY_BY_MARKET.default;
}

export function buildProfessionalEventPlannerDiscoveryPrompt({
  hotel = {},
  catchment = {},
} = {}) {
  const hotelName = hotel.name || hotel.displayName || "the hotel";
  const address = hotel.address || hotel.formattedAddress || "verified hotel address";
  const market = hotel.market || hotel.city || "the hotel market";
  const terms = resolvePlannerTerminology({ hotel });
  return {
    version: EXA_PROFESSIONAL_EVENT_PLANNER_DISCOVERY_V1,
    task: "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP_DISCOVERY",
    hotelCentric: true,
    instructions: [
      `Discover actual planning companies, DMCs, meeting planners, and event-management businesses serving ${hotelName} (${address}, ${market}) that can influence hotel bookings — guestroom blocks, wedding accommodation, banquets/catering, meeting space, private dining, or resort celebrations.`,
      "The commercial target is the planner / intermediary firm — not the conference, association, or host organization alone.",
      "Conferences, galas, and published events are evidence of planner activity; use them to identify the credited planner, PCO, DMC, or event-management company. Do not substitute the conference brand for the planner identity.",
      "Prioritize firms with evidence of recurring or multi-client event production and genuine hotel-sourcing influence — not directory popularity rankings.",
      "Include planners physically based in the market AND planners demonstrably producing events in the destination even if their office is elsewhere.",
      "Do NOT classify generic decorators, florists, photographers, equipment suppliers, or entertainment vendors as demand producers unless hotel-sourcing influence is independently evidenced.",
      "Separate office location, verified service area, and actual event destination history.",
      "Do not invent client counts, annual weddings, contracts, or production volume.",
    ],
    discoveryChannels: PLANNER_DISCOVERY_CHANNELS,
    localTerminology: terms,
    catchmentHints: {
      officeCoreMaxMi: catchment.coreOfficeMaxMi ?? 15,
      officeSecondaryMaxMi: catchment.secondaryOfficeMaxMi ?? 40,
      note: "Service area and destination production outweigh arbitrary office radius.",
    },
    outputSchemaHints: [
      "plannerCompanyName",
      "officialWebsite",
      "officeLocation",
      "operatingMarkets",
      "specialization",
      "eventTypes",
      "destinationEvidence",
      "hotelInfluenceSignals",
      "namedContacts",
      "sourceUrls",
    ],
  };
}

export function buildProfessionalEventPlannerDeepResearchPrompt({
  hotel = {},
  planner = {},
} = {}) {
  return {
    version: EXA_PROFESSIONAL_EVENT_PLANNER_DEEP_V1,
    task: "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP_DEEP",
    instructions: [
      `Deep-research planner/agency "${planner.name || planner.companyName}" as a Professional Event Planner Partnership prospect for hotel "${hotel.name || hotel.displayName}".`,
      "Verify business name, website, service market, specialization, founder/planning team, and aliases.",
      "Document verified portfolio events, types, destinations, historic venues, recurrence — sizes only when published.",
      "Assess hotel revenue mechanisms (guestrooms, wedding blocks, banquets, meeting space, private dining, resort programmes) with evidence — at least one credible type; none universally mandatory.",
      "Determine commercial influence carefully: venue selection, hotel recommendation, group accommodation negotiation, guestroom blocks, destination wedding management, referrals without contracting control. Do not infer booking authority solely from being an event planner.",
      "Identify named commercially relevant contacts (owner, lead planner, events director, partnerships) with public profile sources.",
      "Look for preferred venues / hotel partnerships without claiming exclusivity without evidence.",
      "Separate established facts from uncertainties. Do not invent volumes or partnership agreements.",
    ],
  };
}

/** Mock discovery boundary for zero-paid FullE2E — never claim live Exa PASS. */
export function buildMockProfessionalEventPlannerDiscovery({ hotel = {} } = {}) {
  return {
    version: EXA_PROFESSIONAL_EVENT_PLANNER_DISCOVERY_V1,
    mock: true,
    liveExa: false,
    hotelId: hotel.hotelId || null,
    candidates: [],
    note: "Mock boundary only — no planners invented as live discoveries. Use labeled certification fixtures for zero-paid FullE2E.",
  };
}
