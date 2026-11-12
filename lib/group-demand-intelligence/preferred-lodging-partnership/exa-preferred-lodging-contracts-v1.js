/**
 * Versioned Exa Discovery + Deep Research contracts for Preferred Lodging Partnership.
 * Zero-paid by default — contracts only; no live provider calls from this module.
 */
export const EXA_PREFERRED_LODGING_DISCOVERY_V1 =
  "gdi-exa-preferred-lodging-partnership-discovery-v1";
export const EXA_PREFERRED_LODGING_DEEP_V1 =
  "gdi-exa-preferred-lodging-partnership-deep-research-v1";

export const PREFERRED_LODGING_VENUE_TYPES = Object.freeze([
  "wedding_reception_venue",
  "standalone_banquet_hall",
  "private_event_facility",
  "independent_conference_event_venue",
  "museum_event_rental",
  "garden_or_historic_property",
  "country_club_or_private_club_outside_events",
  "cultural_institution_event_space",
  "entertainment_destination_event_venue",
  "waterfront_or_rooftop_event_venue",
]);

export function buildPreferredLodgingDiscoveryPrompt({ hotel = {}, catchment = {} } = {}) {
  const hotelName = hotel.name || hotel.displayName || "the hotel";
  const address = hotel.address || hotel.formattedAddress || "verified hotel address";
  const market = hotel.market || hotel.city || "the hotel market";
  return {
    version: EXA_PREFERRED_LODGING_DISCOVERY_V1,
    task: "PREFERRED_LODGING_PARTNERSHIP_VENUE_DISCOVERY",
    hotelCentric: true,
    instructions: [
      `Find businesses and venues near ${hotelName} (${address}, ${market}) that could refer hotel guestrooms or groups — weddings, private celebrations, banquets, cultural/private functions, clubs, museums, entertainment destinations, and complementary event facilities.`,
      "A competing event venue can be a valuable Preferred Lodging partner when it has no guestrooms or only complementary / overflow lodging need.",
      "Do NOT reject a venue merely because it offers meeting space, banquet halls, or catering — those often create overnight guest referral demand for a nearby hotel.",
      "Prefer venues without a full competing guestroom inventory; still assess limited on-site lodging for overflow partnership potential (use UNKNOWN when not evidenced).",
      "Exclude generic restaurants unless a documented event format credibly creates overnight visitors.",
      "Return venue name, address, website, event types, on-site lodging status if published, and any lodging-partner language.",
      "Do not invent distances, drive times, annual wedding counts, or room-night forecasts.",
    ],
    venueTypes: PREFERRED_LODGING_VENUE_TYPES,
    catchmentHints: {
      core: catchment.coreMaxMi ?? 1.5,
      secondary: catchment.secondaryMaxMi ?? 3.5,
      extended: catchment.extendedMaxMi ?? 8,
      note: "Provisional bands only — classify with verified coordinates and market policy.",
    },
    outputSchemaHints: [
      "venueName",
      "venueAddress",
      "website",
      "venueType",
      "eventTypes",
      "onsiteLodgingMention",
      "preferredHotelLanguage",
      "sourceUrls",
    ],
  };
}

export function buildPreferredLodgingDeepResearchPrompt({
  hotel = {},
  venue = {},
} = {}) {
  return {
    version: EXA_PREFERRED_LODGING_DEEP_V1,
    task: "PREFERRED_LODGING_PARTNERSHIP_VENUE_DEEP",
    instructions: [
      `Deep-research venue "${venue.name || venue.venueName}" as a Preferred Lodging Partnership prospect for hotel "${hotel.name || hotel.displayName}".`,
      "Verify venue identity, address, website, event activity, capacity if published, seasonality, and any preferred-hotel language.",
      "Determine on-site lodging status using controlled taxonomy; use UNKNOWN when not evidenced — never infer NO_ONSITE from silence alone.",
      "Identify named venue contacts (owner, GM, events director, wedding sales) with source URLs. General inbox alone is insufficient for sales-ready.",
      "Separate established facts from uncertainties. Do not invent partnership agreements, rates, or referral volumes.",
    ],
    lodgingStatusTaxonomy: [
      "NO_ONSITE_LODGING",
      "LIMITED_ONSITE_LODGING",
      "ONSITE_LODGING_AVAILABLE",
      "UNKNOWN",
    ],
  };
}

/** Mock discovery boundary for zero-paid FullE2E — never claim live Exa PASS. */
export function buildMockPreferredLodgingDiscovery({ hotel = {} } = {}) {
  return {
    version: EXA_PREFERRED_LODGING_DISCOVERY_V1,
    mock: true,
    liveExa: false,
    hotelId: hotel.hotelId || null,
    candidates: [],
    note: "Mock boundary only — no venues invented. Use historical audited fixtures for certification.",
  };
}
