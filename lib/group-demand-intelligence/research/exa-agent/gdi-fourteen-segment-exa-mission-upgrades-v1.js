/**
 * Fourteen-segment Exa discovery mission upgrades V1.
 * Differentiated segment instructions + hotel-dependent catchment.
 * Deterministic context builders — no paid calls.
 */

import { listFourteenWorkflowSegmentIds } from "../universal-contact-research-contract-v1.js";
import {
  buildLocalDemandTrackContextBlock,
  normalizeLocalDemandTrack,
} from "../gdi-local-demand-research-tracks-v1.js";

export const GDI_FOURTEEN_SEGMENT_EXA_MISSION_UPGRADES_V1 =
  "gdi-fourteen-segment-exa-mission-upgrades-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Hotel-dependent catchment. Not Bethesda-hardcoded for all hotels.
 */
export function resolveHotelDiscoveryCatchment(hotel = {}) {
  const city = text(hotel.city || hotel.marketCity);
  const country = text(hotel.country || hotel.marketCountry);
  const name = text(hotel.displayName || hotel.name).toLowerCase();
  const territory = text(hotel.demandTerritory?.label || hotel.market || "");

  const cityLower = city.toLowerCase();
  let hotelType = "urban_meeting";
  if (/airport|aeropuerto/.test(name) || /airport/i.test(territory)) {
    hotelType = "airport";
  } else if (/resort|all.?inclusive|beach|island/i.test(name + territory)) {
    hotelType = "destination_resort";
  } else if (/extended|residence|suites/i.test(name)) {
    hotelType = "extended_stay";
  } else if (/convention|marriott|hilton|hyatt/i.test(name) && /downtown|centre|center/i.test(territory + name)) {
    hotelType = "convention_urban";
  }

  // Bethesda / DMV specialisation when city matches — not applied to Geneva/London/etc.
  if (/bethesda|rockville|north bethesda/i.test(cityLower + " " + territory)) {
    return {
      hotelType,
      primaryMarket: "Bethesda / North Bethesda / Rockville",
      secondaryMarkets: ["Montgomery County", "Washington DC (overflow / secondary only)"],
      broaderCatchment: ["Northern Virginia when demand specifically justifies"],
      overflowMarkets: ["Washington DC primary venues — only with overflow evidence"],
      negativeFilters: [
        "Do not treat every DC metro national convention as Bethesda demand",
        "Require overflow / secondary hotel evidence for National Mall / downtown DC primaries",
        "Exclude distant US cities (Chicago, Dallas, Atlanta, etc.) unless org has local Bethesda activity",
      ],
      forwardLookingTriggers: [
        "office expansion in Rockville/Bethesda",
        "federal contractor training cohorts",
        "NIH / medical-scientific local activity",
        "Montgomery County projects and workforce",
      ],
    };
  }

  if (/geneva|genève/i.test(cityLower)) {
    return {
      hotelType,
      primaryMarket: city || "Geneva",
      secondaryMarkets: ["Lake Geneva corridor", "Nyon", "Annemasse"],
      broaderCatchment: ["Swiss/French border when lodging overflow is evidenced"],
      overflowMarkets: ["Larger Geneva convention hotels — overflow only with evidence"],
      negativeFilters: [
        "Do not apply US/DMV geography rules",
        "Exclude distant EU capitals without Geneva lodging thesis",
      ],
      forwardLookingTriggers: [
        "IO/NGO meetings",
        "watchmaking / corporate training",
        "association rotations in Geneva",
      ],
    };
  }

  if (/london/i.test(cityLower)) {
    return {
      hotelType,
      primaryMarket: city || "London",
      secondaryMarkets: ["Canary Wharf", "Westminster", "City of London"],
      broaderCatchment: ["Greater London when transport-accessible"],
      overflowMarkets: ["Excel / large venue overflow with evidence"],
      negativeFilters: ["Do not apply Bethesda/DMV filters", "Avoid US-only national calendars"],
      forwardLookingTriggers: [
        "corporate HQ moves",
        "association UK rotations",
        "project workforce lodging",
      ],
    };
  }

  return {
    hotelType,
    primaryMarket: city || territory || "hotel local market",
    secondaryMarkets: territory ? [territory] : [],
    broaderCatchment: country ? [country] : [],
    overflowMarkets: ["Primary venue overflow only with evidence"],
    negativeFilters: [
      "Exclude distant markets without a lodging thesis for this hotel",
      "Do not invent room nights from hiring announcements alone",
    ],
    forwardLookingTriggers: [
      "office/facility openings",
      "training cohorts",
      "project mobilizations",
      "association rotations",
      "government procurement activity",
    ],
  };
}

/** Segment-true discovery instructions (not just title changes). */
export const SEGMENT_EXA_MISSION_UPGRADES = Object.freeze({
  associations_conferences_events: {
    targets: ["associations with dated future editions", "housing lists", "overflow language"],
    earlySignals: ["destination shortlist", "RFP open", "rotation announcements"],
    exclusions: ["undated perpetual calendars", "primary host cities without overflow path"],
    contacts: ["meeting planner", "housing bureau", "association meetings staff"],
  },
  corporate_accounts_scaling: {
    targets: ["office/facility openings", "HQ moves", "contract wins with local footprint"],
    earlySignals: ["expansion press", "training academy", "project team lodging"],
    exclusions: ["hiring-only growth with no lodging thesis"],
    contacts: ["admin / workplace", "travel manager", "project PMO"],
  },
  corporate_meetings_incentives: {
    targets: ["kickoffs", "leadership offsites", "distributor meetings near hotel market"],
    earlySignals: ["sales meeting RFPs", "incentive destination shortlists"],
    exclusions: ["national association annuals better routed to associations segment"],
    contacts: ["sales ops", "event agency", "corporate meeting planner"],
  },
  government_public_sector: {
    targets: ["agency programs", "contractor mobilizations", "public hearings with lodging"],
    earlySignals: ["procurement awards", "program office relocations"],
    exclusions: ["generic political news without lodging demand"],
    contacts: ["program admin", "meeting coordinator"],
  },
  sports_entertainment: {
    targets: ["tournament overflow", "league events with multi-hotel housing"],
    earlySignals: ["host city awards", "housing partner RFPs"],
    exclusions: ["national leagues without local lodging requirement"],
    contacts: ["tournament housing", "team travel"],
  },
  education_scientific: {
    targets: ["symposiums", "lab consortia", "medical society local activity"],
    earlySignals: ["call for abstracts with housing", "campus conference calendars"],
    exclusions: ["duplicate published NIH/PDA-style inventory without novelty"],
    contacts: ["conference secretariat", "CME planner"],
  },
  projects_infrastructure: {
    targets: ["capital projects", "contractor camps", "utility/infrastructure mobilizations"],
    earlySignals: ["NTP awards", "workforce deployment"],
    exclusions: ["speculative zoning news without workforce lodging"],
    contacts: ["project controls", "site admin"],
  },
  social_special_events: {
    targets: ["public galas", "fundraising celebrations", "cultural festivals with lodging"],
    earlySignals: ["ticketed event dates", "venue preferred-hotel notes"],
    exclusions: ["invented private family events"],
    contacts: ["event chair", "venue coordinator"],
  },
  local_catering_day_meetings: {
    targets: ["chamber events", "corporate campus day meetings", "hospital board/day programs"],
    earlySignals: ["catering RFPs", "half-day agendas"],
    exclusions: ["destination multi-day conferences"],
    contacts: ["office manager", "catering buyer"],
  },
  corporate_celebrations_private_events: {
    targets: ["milestone corporate celebrations", "award dinners", "private client events"],
    earlySignals: ["sponsorship dinners", "venue holds"],
    exclusions: ["generic holiday party speculation"],
    contacts: ["executive assistant", "event agency"],
  },
  travel_trade_group_intermediaries: {
    targets: ["DMCs", "tour operators", "incentive houses"],
    earlySignals: ["preferred hotel lists", "group series"],
    exclusions: ["unrelated leisure OTA listings"],
    contacts: ["DMC operations", "group desk"],
  },
  relocation_extended_stay: {
    targets: ["workforce relocation", "extended project stays", "temporary housing programs"],
    earlySignals: ["relocation RFPs", "temp housing bids"],
    exclusions: ["single overnight transient leisure"],
    contacts: ["relocation firm", "HR mobility"],
  },
  preferred_lodging_partnerships: {
    targets: [
      "venues without verified guestrooms",
      "overflow partners",
      "hospital / clinical visitor lodging lists",
      "performing-arts preferred hotel notes",
    ],
    earlySignals: [
      "preferred hotel language",
      "room-block partners",
      "complimentary shuttle-to-venue hotel listings",
      "official visitor lodging pages",
    ],
    exclusions: [
      "assume no rooms without verification — use UNKNOWN",
      "proximity listing alone without partnership or shuttle evidence",
    ],
    contacts: ["venue GM", "events director", "patient/visitor services"],
  },
  professional_event_planners: {
    targets: [
      "meeting planners",
      "wedding planners",
      "production agencies",
      "tournament / housing agencies that book multi-hotel blocks",
    ],
    earlySignals: [
      "planner portfolios",
      "preferred hotel partnerships",
      "stay-to-play housing partner pages",
    ],
    exclusions: [
      "new opportunity each week for same planner org without new demand",
      "duplicate the same planner org across segments without distinct demand activity",
    ],
    contacts: ["principal planner", "account manager", "housing coordinator"],
  },
});

export function buildSegmentMissionBlock(segmentId = "") {
  const m = SEGMENT_EXA_MISSION_UPGRADES[segmentId];
  if (!m) {
    return `Segment ${segmentId}: seek credible forward-looking hotel demand with evidence; exclude speculative hiring-only noise.`;
  }
  return [
    `SEGMENT MISSION — ${segmentId}:`,
    `- Targets: ${m.targets.join("; ")}`,
    `- Early signals: ${m.earlySignals.join("; ")}`,
    `- Exclusions: ${m.exclusions.join("; ")}`,
    `- Contact types: ${m.contacts.join("; ")}`,
    "- Require source URLs. Do not invent buyer authority or room-night counts.",
  ].join("\n");
}

export function buildCatchmentBlock(hotel = {}) {
  const c = resolveHotelDiscoveryCatchment(hotel);
  return [
    "HOTEL CATCHMENT (hotel-specific — do not reuse another hotel's geography):",
    `Hotel type posture: ${c.hotelType}`,
    `Primary market: ${c.primaryMarket}`,
    c.secondaryMarkets?.length
      ? `Secondary: ${c.secondaryMarkets.join("; ")}`
      : null,
    c.broaderCatchment?.length
      ? `Broader: ${c.broaderCatchment.join("; ")}`
      : null,
    c.overflowMarkets?.length
      ? `Overflow: ${c.overflowMarkets.join("; ")}`
      : null,
    `Negative filters: ${(c.negativeFilters || []).join("; ")}`,
    `Forward-looking triggers: ${(c.forwardLookingTriggers || []).join("; ")}`,
  ]
    .filter(Boolean)
    .join("\n");
}

/**
 * Enhanced discovery context for Exa Agent (deterministic).
 */
export function buildUpgradedExaDiscoveryContextBlock(
  hotel = {},
  segmentId = "",
  opts = {},
) {
  const trackId = normalizeLocalDemandTrack(opts.researchTrack);
  const trackBlock = trackId
    ? buildLocalDemandTrackContextBlock(trackId, hotel)
    : opts.researchTrackContextBlock || null;
  return [
    "HOTEL RESEARCH CONTEXT",
    `Hotel ID: ${hotel.hotelId || ""}`,
    `Hotel name: ${hotel.displayName || hotel.name || ""}`,
    `City: ${hotel.city || ""}`,
    `Country: ${hotel.country || ""}`,
    `Demand territory: ${hotel.demandTerritory?.label || ""}`,
    `Segment: ${segmentId}`,
    buildCatchmentBlock(hotel),
    buildSegmentMissionBlock(segmentId),
    trackBlock,
    "Prefer unique relevant organizations over volume. Deduplicate against known local inventory when described.",
    "Return evidence-backed demand signals only. Do not invent buyer contacts.",
    `Contract: ${GDI_FOURTEEN_SEGMENT_EXA_MISSION_UPGRADES_V1}`,
  ]
    .filter((line) => line != null && String(line).trim() !== "")
    .join("\n");
}

export function assertAllFourteenSegmentsHaveMissions() {
  const missing = listFourteenWorkflowSegmentIds().filter(
    (s) => !SEGMENT_EXA_MISSION_UPGRADES[s],
  );
  return { ok: missing.length === 0, missing };
}

export default {
  GDI_FOURTEEN_SEGMENT_EXA_MISSION_UPGRADES_V1,
  resolveHotelDiscoveryCatchment,
  SEGMENT_EXA_MISSION_UPGRADES,
  buildSegmentMissionBlock,
  buildCatchmentBlock,
  buildUpgradedExaDiscoveryContextBlock,
  assertAllFourteenSegmentsHaveMissions,
};
