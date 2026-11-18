/**
 * Three local demand research tracks — missions on existing fourteen segments.
 * Not new top-level segments. Not parallel engines.
 */

export const GDI_LOCAL_DEMAND_RESEARCH_TRACKS_V1 =
  "gdi-local-demand-research-tracks-v1";

export const LOCAL_DEMAND_TRACK = Object.freeze({
  SOCIAL_CELEBRATIONS: "social_celebrations",
  PLANNER_PARTNERSHIPS: "planner_partnerships",
  VENUE_LODGING_PARTNERSHIPS: "venue_lodging_partnerships",
});

/**
 * Track → existing fourteen-segment IDs (no new segment registry).
 */
export const LOCAL_DEMAND_TRACK_SEGMENTS = Object.freeze({
  [LOCAL_DEMAND_TRACK.SOCIAL_CELEBRATIONS]: [
    "social_special_events",
    "corporate_celebrations_private_events",
    "local_catering_day_meetings",
  ],
  [LOCAL_DEMAND_TRACK.PLANNER_PARTNERSHIPS]: [
    "professional_event_planners",
    "travel_trade_group_intermediaries",
    "preferred_lodging_partnerships",
    "corporate_meetings_incentives",
  ],
  [LOCAL_DEMAND_TRACK.VENUE_LODGING_PARTNERSHIPS]: [
    "preferred_lodging_partnerships",
    "local_catering_day_meetings",
    "social_special_events",
    "professional_event_planners",
  ],
});

export const LOCAL_DEMAND_TRACK_MISSIONS = Object.freeze({
  [LOCAL_DEMAND_TRACK.SOCIAL_CELEBRATIONS]: {
    title: "Weddings, social celebrations, and private events",
    objective:
      "Identify publicly evidenced future social/private-event demand that can generate guestrooms, catering, or meeting space — without inventing private family events.",
    targetEntities: [
      "wedding venues announcing calendars",
      "cultural/community galas",
      "fundraising celebrations",
      "milestone public events",
      "corporate private celebrations",
    ],
    demandTriggers: [
      "publicly announced dates",
      "ticketed galas",
      "venue preferred-hotel language",
      "room-block invitations",
    ],
    exclusions: [
      "invented private weddings",
      "personal contact scraping",
      "venue existence alone as a booking lead",
    ],
    commercialOutcomes: [
      "room blocks",
      "bridal-party lodging",
      "rehearsal dining",
      "preferred referral relationships",
    ],
    qualificationHonesty:
      "Partnership/account-development intelligence is allowed without a confirmed booking; do not label as CONFIRMED_DEMAND without evidence.",
  },
  [LOCAL_DEMAND_TRACK.PLANNER_PARTNERSHIPS]: {
    title: "Meeting and wedding planner partnerships",
    objective:
      "Identify planners and agencies that influence recurring hotel demand; preserve partner identity across runs.",
    targetEntities: [
      "independent meeting planners",
      "wedding planners",
      "DMCs",
      "association meeting planners",
      "incentive planners",
    ],
    captureFields: [
      "organization",
      "service geography",
      "event types",
      "named contact when public",
      "website",
      "why this hotel fits",
      "partnership approach",
    ],
    exclusions: [
      "invented booking authority",
      "fabricated event volumes",
      "new opportunity every week for the same planner org",
    ],
    contactClasses: [
      "DIRECT_BUYER",
      "RELEVANT_COMMERCIAL_ENTRY_POINT",
      "REFERRAL_SOURCE",
      "ACCOUNT_DEVELOPMENT_TARGET",
    ],
  },
  [LOCAL_DEMAND_TRACK.VENUE_LODGING_PARTNERSHIPS]: {
    title: "Local event venues without guestrooms",
    objective:
      "Identify event venues that generate lodging demand but lack verified on-site guestrooms; use UNKNOWN when rooms cannot be verified.",
    targetEntities: [
      "historic mansions",
      "museums",
      "wedding estates",
      "banquet halls",
      "conference centers without hotels",
      "gardens / cultural venues",
    ],
    requiredVerification: [
      "venue identity",
      "address",
      "event types",
      "onsite guestroom status (YES/NO/UNKNOWN)",
      "proximity to hotel",
      "partnership feasibility",
    ],
    exclusions: [
      "assume no rooms merely because lodging is unmentioned",
      "label partnership as confirmed room-night demand without evidence",
    ],
  },
});

export function normalizeLocalDemandTrack(raw) {
  const t = String(raw || "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
  if (!t) return null;
  if (
    t === LOCAL_DEMAND_TRACK.SOCIAL_CELEBRATIONS ||
    t === "weddings" ||
    t === "social_events" ||
    t === "celebrations"
  ) {
    return LOCAL_DEMAND_TRACK.SOCIAL_CELEBRATIONS;
  }
  if (
    t === LOCAL_DEMAND_TRACK.PLANNER_PARTNERSHIPS ||
    t === "planners" ||
    t === "meeting_planners" ||
    t === "wedding_planners"
  ) {
    return LOCAL_DEMAND_TRACK.PLANNER_PARTNERSHIPS;
  }
  if (
    t === LOCAL_DEMAND_TRACK.VENUE_LODGING_PARTNERSHIPS ||
    t === "venues" ||
    t === "venue_partnerships" ||
    t === "preferred_venues"
  ) {
    return LOCAL_DEMAND_TRACK.VENUE_LODGING_PARTNERSHIPS;
  }
  return null;
}

export function resolveLocalDemandTrack(trackId) {
  const id = normalizeLocalDemandTrack(trackId);
  if (!id) return null;
  return {
    version: GDI_LOCAL_DEMAND_RESEARCH_TRACKS_V1,
    trackId: id,
    segments: [...LOCAL_DEMAND_TRACK_SEGMENTS[id]],
    mission: LOCAL_DEMAND_TRACK_MISSIONS[id],
  };
}

export function listLocalDemandTracks() {
  return Object.values(LOCAL_DEMAND_TRACK).map((id) => resolveLocalDemandTrack(id));
}

/**
 * Build Exa/context mission block for a track (deterministic string).
 */
export function buildLocalDemandTrackContextBlock(trackId, hotelProfile = {}) {
  const track = resolveLocalDemandTrack(trackId);
  if (!track) return "";
  const hotelName = hotelProfile.displayName || hotelProfile.name || "the hotel";
  const city = hotelProfile.city || hotelProfile.marketCity || "the local market";
  const m = track.mission;
  return [
    `## LOCAL DEMAND RESEARCH TRACK: ${m.title}`,
    `Hotel: ${hotelName} (${city})`,
    `Objective: ${m.objective}`,
    `Segments in scope: ${track.segments.join(", ")}`,
    `Targets: ${(m.targetEntities || []).join("; ")}`,
    m.demandTriggers ? `Triggers: ${m.demandTriggers.join("; ")}` : null,
    m.exclusions ? `Exclusions: ${m.exclusions.join("; ")}` : null,
    m.qualificationHonesty ? `Honesty: ${m.qualificationHonesty}` : null,
    "Do not invent private events, booking authority, or room-night counts.",
  ]
    .filter(Boolean)
    .join("\n");
}

export default {
  GDI_LOCAL_DEMAND_RESEARCH_TRACKS_V1,
  LOCAL_DEMAND_TRACK,
  LOCAL_DEMAND_TRACK_SEGMENTS,
  resolveLocalDemandTrack,
  listLocalDemandTracks,
  buildLocalDemandTrackContextBlock,
  normalizeLocalDemandTrack,
};
