/**
 * Hotel commercial strategy profiles V1 — configuration only.
 *
 * Adjusts shared Deep Research prioritization weights using the hotel's verified
 * commercial profile. Does NOT create new demand segments or hotel-specific engines.
 * Does NOT hardcode organization names.
 */
export const HOTEL_COMMERCIAL_STRATEGY_PROFILES_V1 =
  "gdi-hotel-commercial-strategy-profiles-v1";

export const YOTEL_GENEVA_LAKE_HOTEL_ID = "recrPQcZg7SFARRb2";
export const SPICE_ISLAND_HOTEL_ID = "recKRJjcPnb4tVDDS";

/** Shared mechanism taxonomy (hotel-generic labels). */
export const COMMERCIAL_MECHANISM_PRIORITY = Object.freeze({
  CORPORATE_ACCOMMODATION_PROGRAMMES: "corporate_accommodation_programmes",
  GROUP_MEETINGS_TRAINING: "group_meetings_training",
  EXECUTIVE_RETREATS: "executive_retreats",
  INCENTIVE_TRAVEL: "incentive_travel",
  ASSOCIATION_MEETINGS: "association_meetings",
  DESTINATION_WEDDINGS: "destination_weddings",
  RECURRING_LODGING_PARTNERSHIPS: "recurring_lodging_partnerships",
  LOCAL_CATERING_CELEBRATIONS: "local_catering_celebrations",
  PROFESSIONAL_PLANNERS: "professional_planners",
});

const YOTEL_PROFILE = Object.freeze({
  hotelId: YOTEL_GENEVA_LAKE_HOTEL_ID,
  hotelName: "YOTEL Geneva Lake",
  geographyTokens: Object.freeze([
    "founex",
    "nyon",
    "la côte",
    "la cote",
    "gva",
    "geneva airport",
    "aéroport de genève",
    "coppet",
    "mies",
    "celigny",
    "céligny",
  ]),
  geographyPenalties: Object.freeze([
    // Lausanne without airport/La Côte linkage is weak for Founex product
    { pattern: /\blausanne\b/i, unless: /founex|nyon|la c[oô]te|airport|gva/i },
  ]),
  preferredMechanisms: Object.freeze([
    COMMERCIAL_MECHANISM_PRIORITY.CORPORATE_ACCOMMODATION_PROGRAMMES,
    COMMERCIAL_MECHANISM_PRIORITY.GROUP_MEETINGS_TRAINING,
    COMMERCIAL_MECHANISM_PRIORITY.ASSOCIATION_MEETINGS,
    COMMERCIAL_MECHANISM_PRIORITY.EXECUTIVE_RETREATS,
    COMMERCIAL_MECHANISM_PRIORITY.RECURRING_LODGING_PARTNERSHIPS,
    COMMERCIAL_MECHANISM_PRIORITY.PROFESSIONAL_PLANNERS,
    COMMERCIAL_MECHANISM_PRIORITY.LOCAL_CATERING_CELEBRATIONS,
  ]),
  preferredWorkflowBoost: Object.freeze({
    preferred_lodging: 8,
    scaling_companies: 7,
    local_catering_day_meetings: 6,
    corporate_meetings_incentives: 5,
    professional_event_planners: 5,
    association_events: 4,
    travel_trade_group_intermediaries: 3,
  }),
  mechanismPatterns: Object.freeze({
    [COMMERCIAL_MECHANISM_PRIORITY.CORPORATE_ACCOMMODATION_PROGRAMMES]:
      /visiting (?:employee|staff)|preferred lodging|corporate (?:rate|housing)|transient lodging|relocati/i,
    [COMMERCIAL_MECHANISM_PRIORITY.GROUP_MEETINGS_TRAINING]:
      /training|workshop|day.?meeting|meeting room|board.?meeting|seminar/i,
    [COMMERCIAL_MECHANISM_PRIORITY.EXECUTIVE_RETREATS]:
      /executive retreat|leadership offsite|board retreat|management offsite/i,
    [COMMERCIAL_MECHANISM_PRIORITY.ASSOCIATION_MEETINGS]:
      /association|congress|symposium|forum|society meeting/i,
    [COMMERCIAL_MECHANISM_PRIORITY.RECURRING_LODGING_PARTNERSHIPS]:
      /preferred.?hotel|recurring (?:programme|program|meeting)|annual (?:offsite|meeting)/i,
    [COMMERCIAL_MECHANISM_PRIORITY.PROFESSIONAL_PLANNERS]:
      /\b(pco|dmc|planner|agency)\b/i,
    [COMMERCIAL_MECHANISM_PRIORITY.LOCAL_CATERING_CELEBRATIONS]:
      /catering|banquet|celebration|gala|reception/i,
  }),
  note: "Founex / Nyon / La Côte corporate + meetings product; airport-corridor access realism required.",
});

const SPICE_PROFILE = Object.freeze({
  hotelId: SPICE_ISLAND_HOTEL_ID,
  hotelName: "Spice Island Beach Resort",
  geographyTokens: Object.freeze([
    "grenada",
    "grand anse",
    "st. george",
    "st george",
    "spice island",
    "windward",
    "caribbean",
  ]),
  geographyPenalties: Object.freeze([]),
  preferredMechanisms: Object.freeze([
    COMMERCIAL_MECHANISM_PRIORITY.INCENTIVE_TRAVEL,
    COMMERCIAL_MECHANISM_PRIORITY.EXECUTIVE_RETREATS,
    COMMERCIAL_MECHANISM_PRIORITY.DESTINATION_WEDDINGS,
    COMMERCIAL_MECHANISM_PRIORITY.PROFESSIONAL_PLANNERS,
    COMMERCIAL_MECHANISM_PRIORITY.LOCAL_CATERING_CELEBRATIONS,
    COMMERCIAL_MECHANISM_PRIORITY.GROUP_MEETINGS_TRAINING,
    COMMERCIAL_MECHANISM_PRIORITY.RECURRING_LODGING_PARTNERSHIPS,
  ]),
  preferredWorkflowBoost: Object.freeze({
    corporate_meetings_incentives: 8,
    corporate_celebrations_private_events: 7,
    social_special_events: 7,
    professional_event_planners: 6,
    travel_trade_group_intermediaries: 6,
    preferred_lodging: 4,
  }),
  mechanismPatterns: Object.freeze({
    [COMMERCIAL_MECHANISM_PRIORITY.INCENTIVE_TRAVEL]:
      /incentive|reward travel|sales incentive|recognition trip/i,
    [COMMERCIAL_MECHANISM_PRIORITY.EXECUTIVE_RETREATS]:
      /executive retreat|leadership retreat|board retreat|buyout/i,
    [COMMERCIAL_MECHANISM_PRIORITY.DESTINATION_WEDDINGS]:
      /destination wedding|wedding|honeymoon|bridal/i,
    [COMMERCIAL_MECHANISM_PRIORITY.PROFESSIONAL_PLANNERS]:
      /\b(dmc|planner|incentive house|luxury travel)\b/i,
    [COMMERCIAL_MECHANISM_PRIORITY.LOCAL_CATERING_CELEBRATIONS]:
      /gala|celebration|anniversary|private dining|buyout/i,
    [COMMERCIAL_MECHANISM_PRIORITY.GROUP_MEETINGS_TRAINING]:
      /small.?group|executive meeting|board meeting|retreat meeting/i,
  }),
  largeConventionPenalty:
    /convention center|1000.+delegates|large congress|trade show hall|mass.?market/i,
  note: "64-suite luxury all-inclusive — prioritize incentive/retreat/wedding/DMC; penalize mass convention demand.",
});

const BY_HOTEL = Object.freeze({
  [YOTEL_GENEVA_LAKE_HOTEL_ID]: YOTEL_PROFILE,
  [SPICE_ISLAND_HOTEL_ID]: SPICE_PROFILE,
});

export function getHotelCommercialStrategyProfile(hotelId = "") {
  const id = String(hotelId || "").trim();
  return BY_HOTEL[id] || null;
}

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Score adjustment (−20..+20) from hotel strategy profile.
 * Hotel-generic patterns only — no org-name allowlists.
 */
export function applyHotelStrategyScoreAdjustment(candidate = {}, hotelId = "") {
  const profile = getHotelCommercialStrategyProfile(hotelId);
  if (!profile) {
    return { delta: 0, reasons: ["no_hotel_strategy_profile"], mechanismsMatched: [] };
  }

  const blob = [
    candidate.organizationName,
    candidate.organizingEntity,
    candidate.eventIdentity,
    candidate.programmeName,
    candidate.demandMechanismSummary,
    candidate.hotelDemandThesis,
    candidate.preliminaryHotelFit,
    candidate.activityGeography,
    candidate.eventLocationCity,
    candidate.destination,
  ]
    .map(text)
    .join(" ");

  let delta = 0;
  const reasons = [];
  const mechanismsMatched = [];

  const wf = String(candidate.workflowId || "");
  const wfBoost = Number(profile.preferredWorkflowBoost?.[wf] || 0);
  if (wfBoost) {
    delta += wfBoost;
    reasons.push(`workflow_strategy_boost_${wf}`);
  }

  for (const [mech, re] of Object.entries(profile.mechanismPatterns || {})) {
    if (re.test(blob)) {
      delta += 4;
      mechanismsMatched.push(mech);
      reasons.push(`mechanism_${mech}`);
    }
  }

  const geoHit = (profile.geographyTokens || []).some((t) =>
    blob.toLowerCase().includes(String(t).toLowerCase()),
  );
  if (geoHit) {
    delta += 5;
    reasons.push("geography_token_match");
  }

  for (const pen of profile.geographyPenalties || []) {
    if (pen.pattern.test(blob) && !(pen.unless && pen.unless.test(blob))) {
      delta -= 10;
      reasons.push("geography_penalty");
    }
  }

  if (profile.largeConventionPenalty && profile.largeConventionPenalty.test(blob)) {
    if (!/incentive|retreat|wedding|buyout|leadership|small.?group/i.test(blob)) {
      delta -= 12;
      reasons.push("large_convention_fit_penalty");
    }
  }

  // Clamp adjustment
  delta = Math.max(-20, Math.min(20, delta));
  return {
    version: HOTEL_COMMERCIAL_STRATEGY_PROFILES_V1,
    hotelId: profile.hotelId,
    delta,
    reasons,
    mechanismsMatched,
  };
}
