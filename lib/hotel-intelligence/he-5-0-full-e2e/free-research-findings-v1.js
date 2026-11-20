/**
 * HE 5.0 free-first research findings (Wave A Bethesda + Wave B seven candidates).
 * Cost USD = 0. No Surfe. No production Airtable writes.
 * Ownership for non-Bethesda hotels: PROPOSED portfolio association only.
 */

import { HE50_BETHESDA_HPC_ID, FIELD_CLASS } from "./contract.js";

export const FREE_RESEARCH_PASS_VERSION = "he-5-0-free-research-pass-v1";
export const FREE_RESEARCH_SPEND_USD = 0;

/** Bethesda physical + ownership free findings. */
export const BETHESDA_FREE_FINDINGS = Object.freeze({
  pilotKey: "bethesda_marriott",
  hpcId: HE50_BETHESDA_HPC_ID,
  spendUsd: 0,
  findings: [
    {
      fieldKey: "yearBuilt",
      value: 1979,
      classification: "NEWLY_VERIFIED",
      confidence: "HIGH",
      sources: [
        {
          url: "https://www.travelweekly.com/Hotels/Bethesda-MD/Bethesda-Marriott-p55149814",
          publisher: "Travel Weekly",
          supported: "Year Built: 1979",
        },
        {
          url: "https://cmbs.news/property/bethesda-marriott-bethesda-md/",
          publisher: "CMBS.news servicer AL",
          supported: "Built 1979",
        },
        {
          url: "https://www.cvent.com/venues/bethesda/hotel/bethesda-marriott/venue-759e1222-3f0f-4739-9375-e9cb7962c6a4",
          publisher: "Cvent",
          supported: "Built 1979",
        },
      ],
      persistence: { table: "HPC", field: "Year Built", action: "UPDATE_NULL_FILL" },
    },
    {
      fieldKey: "lastRenovation",
      value: {
        majorRenovationCompletedApprox: 2015,
        guestroomUnveilPress: 2017,
        scope:
          "Guest rooms, lobby, lounge, meeting spaces, Cooper's Mill restaurant (full-scale)",
      },
      classification: "EVENT_LEVEL_DUAL_DATES",
      confidence: "HIGH",
      note: "RESOLVED: 2015 and 2017 are different public milestones of the same renovation program — not contradictory single-year facts. Preserve both events.",
      sources: [
        {
          url: "https://bethesdamagazine.com/2015/10/08/coopers-mill-opens-at-bethesda-marriott/",
          publisher: "Bethesda Magazine",
          date: "2015-10-08",
          supported: "Full-scale renovation nearly complete; rooms/lobby/lounge/meeting spaces",
        },
        {
          url: "https://www.einpresswire.com/article/363219772/open-the-door-to-the-unexpected-bethesda-marriott-unveils-contemporary-design",
          publisher: "EIN / Marketwired",
          date: "2017-01-23",
          supported: "Guestroom + ballroom/meeting/restaurant/lobby transformation unveil",
        },
        {
          url: "https://www.travelweekly.com/Hotels/Bethesda-MD/Bethesda-Marriott-p55149814",
          publisher: "Travel Weekly",
          supported: "Year Last Renovated: 2015",
        },
      ],
      persistence: {
        table: "HPC + HI Graph Evidence",
        field: "Last Renovation / evidence notes",
        action: "UPDATE_WITH_DUAL_DATES_OR_EVIDENCE_ONLY",
      },
    },
    {
      fieldKey: "rooms",
      value: 407,
      classification: "REUSED_VERIFIED",
      confidence: "HIGH",
      sources: [
        {
          url: "https://hodgeswardelliott.com/news-posts/hodges-ward-elliott-represents-owner-in-the-sale-of-the-407-key-bethesda-marriott-in-bethesda-maryland/",
          publisher: "Hodges Ward Elliott",
          date: "2018-12-03",
          supported: "407-key Bethesda Marriott",
        },
      ],
      persistence: { table: "HPC", field: "Rooms / Keys", action: "NO_OP_IF_MATCHES" },
    },
    {
      fieldKey: "cmbsFinancingNote",
      value: {
        loanUsd: 39800000,
        ratePct: 7.472,
        maturity: "2029-02-01",
        status: "current_as_of_oct_2026_servicer",
      },
      classification: "EVIDENCE_ONLY_NOT_GRAPH_LENDER",
      confidence: "MEDIUM",
      note: "CMBS terms are financing evidence; do not invent current lender org edge without servicer/lender identity confirmation.",
      sources: [
        {
          url: "https://cmbs.news/property/bethesda-marriott-bethesda-md/",
          publisher: "CMBS.news",
          supported: "$39.8M CMBS loan; built 1979 renovated 2015; 407 units",
        },
      ],
      persistence: { table: "HI Graph Evidence", action: "CREATE_EVIDENCE" },
    },
  ],
});

/**
 * Wave B — minimum identity packets for seven portfolio candidates.
 * Mullen association = PROPOSED until independently verified legal ownership.
 */
export const SEVEN_HOTEL_FREE_IDENTITY_PACKETS = Object.freeze([
  {
    pilotKey: "ac_hotel_newtown_square",
    hotelName: "AC Hotel Newtown Square",
    identityOutcome: "MATCHED_HPC",
    hpcRecordId: "recF7DQBPDW5aUDoC",
    officialName: "AC Hotel Newtown Square",
    brand: "AC Hotels by Marriott",
    parentBrand: "Marriott International",
    status: "Open",
    address: "200 Squire Drive, Newtown Square, Pennsylvania 19073, USA",
    city: "Newtown Square",
    country: "United States",
    phone: "+1 484-424-6950",
    website: "https://www.marriott.com/en-us/hotels/phlqc-ac-hotel-newtown-square/overview/",
    rooms: {
      value: 140,
      confidence: "HIGH",
      note: "Resolved: Delco Times opening (86 king + 50 queen + 4 suites = 140) + Cvent + Hotel Business. Discard outdated OTA 146.",
      discardedAlt: 146,
      disposition: "RESOLVED",
    },
    operator: "Gulph Creek Hotels (press — not auto HI edge)",
    developerOpenerPress: "Equus Capital Partners (opening press)",
    mullenAssociation: "PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING",
    proposedHpcAction: "UPDATE_GEOGRAPHY_NULL_FILL_ONLY",
    sources: [
      "https://www.marriott.com/en-us/hotels/phlqc-ac-hotel-newtown-square/overview/",
      "https://www.delcotimes.com/2024/09/27/ac-hotel-by-marriott-at-ellis-preserve-holds-grand-opening/",
      "https://www.cvent.com/venues/newtown-square/hotel/ac-hotel-newtown-square/venue-bbfcb889-1474-4e7c-afda-54dfadd1e929",
      "https://hotelbusiness.com/equus-capital-opens-philadelphia-area-ac-hotel/",
    ],
  },
  {
    pilotKey: "carton_house_fairmont",
    hotelName: "Carton House, a Fairmont Hotel",
    identityOutcome: "MATCHED_HPC",
    hpcRecordId: "recYf7HZWmCdWPaSd",
    officialName: "Carton House, A Fairmont Hotel",
    brand: "Fairmont",
    parentBrand: "Accor / Fairmont",
    status: "Open",
    address: "Carton Demesne, Maynooth, Co. Kildare, W23 TD98, Ireland",
    city: "Maynooth",
    country: "Ireland",
    phone: "+353 1 505 2000",
    website: "https://www.cartonhouse.com/",
    rooms: { value: 169, confidence: "HIGH", note: "Official: 169 bedrooms, 18 in The House" },
    operator: "Fairmont managed",
    mullenAssociation: "PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING",
    proposedHpcAction: "UPDATE_GEOGRAPHY_NULL_FILL_ONLY",
    sources: [
      "https://www.cartonhouse.com/the-resort",
      "https://www.fairmont.com/en/hotels/county-kildare/carton-house.html",
      "https://all.accor.com/hotel/B7D0/index.en.shtml",
    ],
  },
  {
    pilotKey: "hgi_newtown_square_radnor",
    hotelName: "Hilton Garden Inn Newtown Square Radnor",
    identityOutcome: "MATCHED_HPC",
    hpcRecordId: "recgaBAwa24wsl3xF",
    officialName: "Hilton Garden Inn Newtown Square Radnor",
    brand: "Hilton Garden Inn",
    parentBrand: "Hilton",
    status: "Open",
    address: "3743 West Chester Pike, Newtown Square, Pennsylvania 19073, USA",
    city: "Newtown Square",
    country: "United States",
    phone: "+1 484-420-7500",
    website: "https://www.hilton.com/en/hotels/phlnsgi-hilton-garden-inn-newtown-square-radnor/",
    rooms: {
      value: 138,
      confidence: "HIGH",
      note: "Autonomous free pass: Cvent + Hilton events + Expedia + Ellis + IMC converge on 138",
    },
    operator: null,
    mullenAssociation: "PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING",
    proposedHpcAction: "UPDATE_GEOGRAPHY_NULL_FILL_ONLY",
    sources: [
      "https://www.hilton.com/en/hotels/phlnsgi-hilton-garden-inn-newtown-square-radnor/",
      "https://www.hilton.com/en-gb/hotels/phlnsgi-hilton-garden-inn-newtown-square-radnor/events/",
      "https://www.cvent.com/venues/newtown-square/hotel/hilton-garden-inn-newtown-square-radnor/venue-80f98513-8db5-4046-a20c-7881d33eb20a",
      "https://newtownsquarehotels.com/hotels/hilton-garden-inn/",
    ],
  },
  {
    pilotKey: "jw_marriott_costa_elena",
    hotelName: "JW Marriott Costa Elena Resort All-Inclusive",
    identityOutcome: "MATCHED_HPC",
    hpcRecordId: "recq9KWr8UE7z2549",
    officialName: "JW Marriott Costa Elena Resort & Spa, All-Inclusive",
    brand: "JW Marriott",
    parentBrand: "Marriott International",
    status: "Open",
    address: "Playa El Jobo, Guanacaste, Costa Rica",
    city: null,
    country: "Costa Rica",
    website:
      "https://www.marriott.com/en-us/hotels/lircj-jw-marriott-costa-elena-resort-and-spa-all-inclusive/overview/",
    rooms: { value: 415, confidence: "HIGH", note: "Marriott news: 415 rooms; formerly Dreams Las Mareas (~447)" },
    aliases: ["Dreams Las Mareas", "Dreams Las Mareas Costa Rica"],
    operator: "Mullen Hospitality Management (trade press — verify before HI edge)",
    mullenAssociation: "TRADE_PRESS_CLAIMS_OWNER_OPERATOR_FAMILY — VERIFY_BEFORE_GRAPH",
    proposedHpcAction: "UPDATE_ALIAS_AND_ROOMS_NULL_FILL",
    sources: [
      "https://news.marriott.com/news/2026/10/06/jw-marriott-debuts-its-first-all-inclusive-resort-with-the-opening-of-jw-marriott-costa-elena-resort-and-spa",
      "https://thehotelspotter.com/openings/north-america/jw-marriott-costa-elena-opens-all-inclusive-costa-rica/",
    ],
  },
  {
    pilotKey: "secrets_cap_cana",
    hotelName: "Secrets Cap Cana Resort & Spa",
    identityOutcome: "MATCHED_HPC",
    hpcRecordId: "recd7XAM6tLjzQhaI",
    officialName: "Secrets Cap Cana Resort & Spa",
    brand: "Secrets (Hyatt Inclusive Collection)",
    parentBrand: "Hyatt Inclusive Collection",
    status: "Open",
    city: "Cap Cana",
    country: "Dominican Republic",
    latitude: 18.4843873,
    longitude: -68.3942656,
    website:
      "https://www.hyattinclusivecollection.com/en/resorts-hotels/secrets/dominican-republic/cap-cana/",
    rooms: {
      value: 457,
      confidence: "HIGH",
      note: "Official Hyatt Inclusive rooms + meetings pages: 457 suites; aligns with Hotel Census prior hit",
    },
    dreamsNeighborHpcId: "reclS16CMUS6eJgpC",
    lowercaseAliasNeighborHpcId: "recFP9eslejVXbyuR",
    mullenAssociation: "PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING",
    proposedHpcAction: "UPDATE_ROOMS_NULL_FILL_IDENTITY_ALREADY_VERIFIED",
    sources: [
      "live HPC read recd7XAM6tLjzQhaI vs Dreams reclS16CMUS6eJgpC",
      "https://www.hyattinclusivecollection.com/en/resorts-hotels/secrets/dominican-republic/cap-cana-resort-spa/accommodations/rooms-suites/",
      "https://www.hyattinclusivecollection.com/en/resorts-hotels/secrets/dominican-republic/cap-cana-resort-spa/events/meetings-incentives/",
    ],
    note: "Identity ≠ Dreams VERIFIED. Rooms 457 autonomous free pass. Steward lowercase alias separately.",
  },
  {
    pilotKey: "secrets_papagayo",
    hotelName: "Secrets Papagayo Costa Rica",
    identityOutcome: "MATCHED_HPC",
    hpcRecordId: "recFQ0WvGLbfK5Gpa",
    /** Historical provenance only — not a runtime canonical id (legacy census independence). */
    legacyHotelCensusProvenanceId: "recyX1wLsHBJYRPw7",
    hotelCensusRecordId: null,
    officialName: "Secrets Papagayo Costa Rica",
    brand: "Secrets",
    parentBrand: "Hyatt Inclusive Collection",
    status: "Open",
    city: "Guanacaste Province",
    country: "Costa Rica",
    address: "Papagayo Gulf, 3 Km North West from Panama Beach",
    website:
      "https://www.hyattinclusivecollection.com/en/resorts-hotels/secrets/costa-rica/papagayo-costa-rica/",
    rooms: { value: 202, confidence: "HIGH", note: "Official Hyatt Inclusive: 202 rooms and suites" },
    mullenAssociation: "PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING",
    proposedHpcAction: "UPDATE_GEOGRAPHY_NULL_FILL_ONLY",
    sources: [
      "https://www.hyattinclusivecollection.com/en/resorts-hotels/secrets/costa-rica/papagayo-costa-rica/accommodations/rooms-suites/",
      "historical_legacy_census_provenance:recyX1wLsHBJYRPw7 (not runtime SoT)",
    ],
    note: "Canonical HPC recFQ0WvGLbfK5Gpa already created — never re-CREATE; do not use legacy census id as SoT",
  },
  {
    pilotKey: "secrets_st_lucia",
    hotelName: "Secrets St. Lucia",
    identityOutcome: "MATCHED_HPC",
    hpcRecordId: "recwZJuMcx6kA7f7I",
    officialName: "Secrets St. Lucia Resort & Spa",
    brand: "Secrets",
    parentBrand: "Hyatt Inclusive Collection",
    status: "Open",
    address: "Choc Bay, Castries–Gros Islet Highway, Saint Lucia",
    city: "Castries / Choc Bay",
    country: "Saint Lucia",
    website:
      "https://www.hyattinclusivecollection.com/en/resorts-hotels/secrets/st-lucia/st-lucia-resort-spa/",
    rooms: { value: 355, confidence: "HIGH", note: "Official: 355 rooms and suites" },
    mullenAssociation: "PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING",
    proposedHpcAction: "UPDATE_GEOGRAPHY_NULL_FILL_ONLY",
    sources: [
      "https://www.hyattinclusivecollection.com/en/resorts-hotels/secrets/st-lucia/st-lucia-resort-spa/accommodations/rooms-suites/",
      "https://www.slutourism.govt.lc/certified-operator/secrets-resort-and-spa-st-lucia",
    ],
  },
]);

export function buildFreeResearchLedgerEntries() {
  const entries = [
    {
      hotel_id: HE50_BETHESDA_HPC_ID,
      pilot_key: "bethesda_marriott",
      provider: "public_web_free",
      research_category: "property_identity_physical",
      actual_usd: 0,
      verified_findings: BETHESDA_FREE_FINDINGS.findings.map((f) => f.fieldKey),
      result_class: "FREE_PASS",
    },
  ];
  for (const p of SEVEN_HOTEL_FREE_IDENTITY_PACKETS) {
    entries.push({
      hotel_id: p.hpcRecordId || p.pilotKey,
      pilot_key: p.pilotKey,
      provider: "public_web_free",
      research_category: "portfolio_hpc_matching",
      actual_usd: 0,
      verified_findings: [
        "official_identity",
        p.rooms?.value != null ? "rooms" : null,
        "website",
      ].filter(Boolean),
      result_class: "FREE_PASS",
    });
  }
  return entries;
}
