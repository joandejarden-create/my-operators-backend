/**
 * GDI Surface Eligibility V1 — organizer-controlled lodging vs directory noise.
 * Generic, hotel-agnostic. Does not invent openness from missing hotel names.
 */

export const SURFACE_CLASS = Object.freeze({
  ORGANIZER_CONTROLLED_HOUSING: "ORGANIZER_CONTROLLED_HOUSING",
  OFFICIAL_EVENT_ACCOMMODATION: "OFFICIAL_EVENT_ACCOMMODATION",
  OFFICIAL_EVENT_PAGE: "OFFICIAL_EVENT_PAGE",
  HOST_HOTEL_PAGE: "HOST_HOTEL_PAGE",
  OFFICIAL_VENUE_PAGE: "OFFICIAL_VENUE_PAGE",
  SPORTS_TEAM_HOUSING: "SPORTS_TEAM_HOUSING",
  OFFICIAL_REGISTRATION_PAGE: "OFFICIAL_REGISTRATION_PAGE",
  OFFICIAL_TRAVEL_PAGE: "OFFICIAL_TRAVEL_PAGE",
  ORGANIZATION_PROGRAM_PAGE: "ORGANIZATION_PROGRAM_PAGE",
  GOVERNMENT_PROGRAM_PAGE: "GOVERNMENT_PROGRAM_PAGE",
  UNIVERSITY_PROGRAM_PAGE: "UNIVERSITY_PROGRAM_PAGE",
  OTA: "OTA",
  HOTEL_DIRECTORY: "HOTEL_DIRECTORY",
  DESTINATION_HOTEL_LIST: "DESTINATION_HOTEL_LIST",
  TOURISM_DIRECTORY: "TOURISM_DIRECTORY",
  GENERIC_HOTELS_NEARBY: "GENERIC_HOTELS_NEARBY",
  VENUE_HOTEL_WIDGET: "VENUE_HOTEL_WIDGET",
  SEARCH_RESULT_PAGE: "SEARCH_RESULT_PAGE",
  AGGREGATOR: "AGGREGATOR",
  BLOG: "BLOG",
  TRAVEL_ARTICLE: "TRAVEL_ARTICLE",
  UNRELATED_HOTEL_PAGE: "UNRELATED_HOTEL_PAGE",
  OTHER_NOISE: "OTHER_NOISE",
});

export const LODGING_RELATIONSHIP = Object.freeze({
  OFFICIAL_ROOM_BLOCK: "OFFICIAL_ROOM_BLOCK",
  OFFICIAL_HOST_HOTEL: "OFFICIAL_HOST_HOTEL",
  OFFICIAL_PREFERRED_HOTEL: "OFFICIAL_PREFERRED_HOTEL",
  HOUSING_BUREAU: "HOUSING_BUREAU",
  TEAM_HOTEL: "TEAM_HOTEL",
  DELEGATION_HOTEL: "DELEGATION_HOTEL",
  OFFICIAL_GROUP_RATE: "OFFICIAL_GROUP_RATE",
  OFFICIAL_ACCOMMODATION_PROGRAM: "OFFICIAL_ACCOMMODATION_PROGRAM",
  OVERFLOW_EVIDENCED: "OVERFLOW_EVIDENCED",
  HOTEL_SELECTION_OPEN: "HOTEL_SELECTION_OPEN",
  VENUE_SET_HOTEL_OPEN: "VENUE_SET_HOTEL_OPEN",
  DESTINATION_SET_HOTEL_OPEN: "DESTINATION_SET_HOTEL_OPEN",
  ATTENDEE_SELF_BOOK: "ATTENDEE_SELF_BOOK",
  GENERIC_NEARBY_HOTELS: "GENERIC_NEARBY_HOTELS",
  NO_LODGING_RELATIONSHIP: "NO_LODGING_RELATIONSHIP",
  UNKNOWN: "UNKNOWN",
});

export const LODGING_GRADE = Object.freeze({
  A: "A",
  B: "B",
  C: "C",
  D: "D",
  E: "E",
});

export const ORGANIZER_CONTROL = Object.freeze({
  ORGANIZER_CONTROLLED: "ORGANIZER_CONTROLLED",
  HOUSING_PROVIDER_CONTROLLED: "HOUSING_PROVIDER_CONTROLLED",
  TEAM_FEDERATION_CONTROLLED: "TEAM/FEDERATION_CONTROLLED",
  DMC_PLANNER_CONTROLLED: "DMC/PLANNER_CONTROLLED",
  VENUE_CONTROLLED: "VENUE_CONTROLLED",
  ATTENDEE_SELF_BOOK: "ATTENDEE_SELF_BOOK",
  UNKNOWN: "UNKNOWN",
});

export const COMMERCIAL_STATUS_V2 = Object.freeze({
  OPEN_UNRESOLVED: "OPEN / UNRESOLVED",
  RFP_ACTIVE: "RFP / ACTIVE SOURCING",
  HOTEL_TBD: "HOTEL TBD",
  VENUE_TBD: "VENUE TBD",
  DESTINATION_SELECTED_HOTEL_OPEN: "DESTINATION SELECTED / HOTEL OPEN",
  PARTIALLY_PLACED: "PARTIALLY PLACED",
  PRIMARY_OVERFLOW: "PRIMARY HOTEL SELECTED / OVERFLOW POSSIBLE",
  PRIMARY_NO_OVERFLOW: "PRIMARY HOTEL SELECTED / NO OVERFLOW EVIDENCE",
  FULLY_PLACED: "FULLY PLACED",
  CURRENT_CYCLE_CLOSED: "CURRENT CYCLE CLOSED",
  FUTURE_NOT_SOURCED: "FUTURE CYCLE NOT YET SOURCED",
  UNKNOWN: "UNKNOWN",
});

export const WINNABILITY = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
  NONE: "NONE",
  UNKNOWN: "UNKNOWN",
});

export const SURFACE_ELIGIBILITY = Object.freeze({
  ELIGIBLE: "ELIGIBLE",
  WATCH_ELIGIBLE: "WATCH_ELIGIBLE",
  INELIGIBLE: "INELIGIBLE",
});

export const FINAL_CLASS = Object.freeze({
  CUSTOMER_READY: "CUSTOMER_READY",
  HIGH_QUALITY_WATCH: "HIGH_QUALITY_WATCH",
  FUTURE_WATCH: "FUTURE_WATCH",
  CLOSED_FULLY_PLACED: "CLOSED / FULLY_PLACED",
  SURFACE_NOISE: "SURFACE_NOISE",
  INVALID: "INVALID",
});

export const REASON_CODE = Object.freeze({
  DIRECTORY_SURFACE: "DIRECTORY_SURFACE",
  OTA_SURFACE: "OTA_SURFACE",
  GENERIC_HOTEL_LIST: "GENERIC_HOTEL_LIST",
  NO_EVENT_SPECIFIC_LODGING: "NO_EVENT_SPECIFIC_LODGING",
  NO_ORGANIZER_CONTROL: "NO_ORGANIZER_CONTROL",
  ATTENDEE_SELF_BOOK_ONLY: "ATTENDEE_SELF_BOOK_ONLY",
  HOTEL_ALREADY_SELECTED: "HOTEL_ALREADY_SELECTED",
  FULLY_PLACED: "FULLY_PLACED",
  CURRENT_CYCLE_CLOSED: "CURRENT_CYCLE_CLOSED",
  FUTURE_CYCLE_UNRESOLVED: "FUTURE_CYCLE_UNRESOLVED",
  OPEN_HOTEL_DECISION: "OPEN_HOTEL_DECISION",
  OVERFLOW_SUPPORTED: "OVERFLOW_SUPPORTED",
  OFFICIAL_BLOCK: "OFFICIAL_BLOCK",
  OFFICIAL_HOUSING: "OFFICIAL_HOUSING",
  EVENT_IDENTITY_AMBIGUOUS: "EVENT_IDENTITY_AMBIGUOUS",
  WRONG_MARKET: "WRONG_MARKET",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
  OTHER: "OTHER",
});

const OTA_HOST_RE =
  /booking\.com|expedia\.|hotels\.com|tripadvisor\.|trivago\.|kayak\.|agoda\.|hoteles\.com|hostelworld|google\.com\/travel\/hotels|hotelscombined|priceline|orbitz|travelocity|marriott\.com\/hotels\/travel|hilton\.com\/[a-z]{2}\/hotels/i;

const DIRECTORY_HOST_RE =
  /quierohotel|hoteles\.[a-z]+\/|viajeselcorteingles|tripadvisor|booking\.com|hotels\.com|expedia|toctocrooms\.com\/blog\/cms\/alojamiento|galiciaenconcierto\.com\/hoteles|hotelsnear|best-hotels|top-hotels/i;

/**
 * Semantic directory / OTA detection — not domain-only.
 */
export function detectDirectoryFeatures(text = "", url = "") {
  const t = String(text || "");
  const u = String(url || "").toLowerCase();
  const features = [];
  if (OTA_HOST_RE.test(u)) features.push("ota_host");
  if (DIRECTORY_HOST_RE.test(u)) features.push("directory_host");
  if (/\$\d+|€\d+|desde\s+\d+|from\s+\$?\d+/i.test(t) && /night|noche|per\s*night/i.test(t)) {
    features.push("price_compare");
  }
  if (/book\s*now|reserv(e|ar)\s*now|check\s*availability|ver\s*disponibilidad/i.test(t)) {
    features.push("availability_widget");
  }
  if (/affiliate|utm_campaign=hotel|click\.|redirect.*hotel/i.test(u + t)) {
    features.push("affiliate");
  }
  if (
    /(hoteles?\s+(cerca|near|en)|hotels?\s+near|mejores\s+hoteles|best\s+hotels|top\s+\d+\s+hotels)/i.test(
      t
    )
  ) {
    features.push("nearby_list");
  }
  if ((t.match(/\b\d(\.\d)?\s*★|\bstars?\b|\breviews?\b|\bvaloraciones?\b/gi) || []).length >= 3) {
    features.push("star_reviews");
  }
  // Multiple hotel brand/name listing pattern
  const hotelHits = (t.match(/\bHotel\s+[A-ZÁÉÍÓÚ][A-Za-záéíóúñ]+/g) || []).length;
  if (hotelHits >= 4 && !/room\s*block|host\s*hotel|official\s*hotel|housing/i.test(t)) {
    features.push("multi_hotel_list");
  }
  if (/d[oó]nde\s+dormir|where\s+to\s+stay|alojamiento\s+en\s+[A-Z]/i.test(t) && hotelHits >= 2) {
    features.push("destination_inventory");
  }
  return features;
}

export function isNoiseSurface(surface) {
  return [
    SURFACE_CLASS.OTA,
    SURFACE_CLASS.HOTEL_DIRECTORY,
    SURFACE_CLASS.DESTINATION_HOTEL_LIST,
    SURFACE_CLASS.TOURISM_DIRECTORY,
    SURFACE_CLASS.GENERIC_HOTELS_NEARBY,
    SURFACE_CLASS.VENUE_HOTEL_WIDGET,
    SURFACE_CLASS.SEARCH_RESULT_PAGE,
    SURFACE_CLASS.AGGREGATOR,
    SURFACE_CLASS.BLOG,
    SURFACE_CLASS.TRAVEL_ARTICLE,
    SURFACE_CLASS.UNRELATED_HOTEL_PAGE,
    SURFACE_CLASS.OTHER_NOISE,
  ].includes(surface);
}

/**
 * Classify primary surface from URL + title + page text.
 */
export function classifySurface({ url = "", title = "", text = "", eventName = "" } = {}) {
  const blob = `${url} ${title} ${String(text).slice(0, 8000)}`.toLowerCase();
  const features = detectDirectoryFeatures(text, url);

  if (OTA_HOST_RE.test(url) || features.includes("ota_host")) {
    return { surface: SURFACE_CLASS.OTA, features, reason: REASON_CODE.OTA_SURFACE };
  }

  // Organizer-controlled housing first (before directory heuristics)
  if (
    /\/(accommodation|housing|hotel-block|room-block|hospedaje|alojamiento)(\/|$|\?)/i.test(url) ||
    /official\s*(room\s*)?block|host\s*hotel|housing\s*bureau|reserv(e|ation)\s*using\s*code|group\s*rate\s*code/i.test(
      blob
    )
  ) {
    if (!features.includes("multi_hotel_list") || /group\s*rate|room\s*block|housing\s*bureau/i.test(blob)) {
      return {
        surface: SURFACE_CLASS.ORGANIZER_CONTROLLED_HOUSING,
        features,
        reason: REASON_CODE.OFFICIAL_HOUSING,
      };
    }
  }

  if (/\/travel(\/|$)|travel\s*information|travel\s*&\s*hotel/i.test(blob) && /conference|event|meeting/i.test(blob)) {
    return { surface: SURFACE_CLASS.OFFICIAL_TRAVEL_PAGE, features, reason: REASON_CODE.OFFICIAL_HOUSING };
  }

  if (features.includes("nearby_list") || /hoteles?\s+cerca|hotels?\s+near\s+the\s+venue/i.test(blob)) {
    return {
      surface: SURFACE_CLASS.GENERIC_HOTELS_NEARBY,
      features,
      reason: REASON_CODE.GENERIC_HOTEL_LIST,
    };
  }

  if (
    features.includes("directory_host") ||
    features.includes("destination_inventory") ||
    features.includes("multi_hotel_list") ||
    /\/hoteles\//i.test(url)
  ) {
    return {
      surface: SURFACE_CLASS.HOTEL_DIRECTORY,
      features,
      reason: REASON_CODE.DIRECTORY_SURFACE,
    };
  }

  if (/blog|article|news.*hotel/i.test(url) && !/official|conference|congress/i.test(blob)) {
    return { surface: SURFACE_CLASS.BLOG, features, reason: REASON_CODE.OTHER };
  }

  if (/turismo|visit[a-z]*\.|tourism/i.test(url) && /hotel|alojamiento/i.test(blob)) {
    return {
      surface: SURFACE_CLASS.TOURISM_DIRECTORY,
      features,
      reason: REASON_CODE.DIRECTORY_SURFACE,
    };
  }

  if (/icehotels\.bnetwork|passkey|onpeak|housing\./i.test(url)) {
    return {
      surface: SURFACE_CLASS.ORGANIZER_CONTROLLED_HOUSING,
      features,
      reason: REASON_CODE.OFFICIAL_HOUSING,
    };
  }

  if (/regfox|eventmobi|cvent|eventsair/i.test(url)) {
    if (/accommodation|hotel|housing/i.test(blob)) {
      return {
        surface: SURFACE_CLASS.OFFICIAL_EVENT_ACCOMMODATION,
        features,
        reason: REASON_CODE.OFFICIAL_HOUSING,
      };
    }
    return {
      surface: SURFACE_CLASS.OFFICIAL_REGISTRATION_PAGE,
      features,
      reason: REASON_CODE.OTHER,
    };
  }

  if (/tournament|regatta|championship|team\s*hotel|federation/i.test(blob)) {
    if (/hotel|housing|accommodation|alojamiento/i.test(blob)) {
      return {
        surface: SURFACE_CLASS.SPORTS_TEAM_HOUSING,
        features,
        reason: REASON_CODE.OTHER,
      };
    }
  }

  if (/\.edu|universidad|university/i.test(blob)) {
    return {
      surface: SURFACE_CLASS.UNIVERSITY_PROGRAM_PAGE,
      features,
      reason: REASON_CODE.OTHER,
    };
  }

  if (/gov\.|gob\.|\.gal\/|poderjudicial|xunta|ministerio/i.test(blob)) {
    return {
      surface: SURFACE_CLASS.GOVERNMENT_PROGRAM_PAGE,
      features,
      reason: REASON_CODE.OTHER,
    };
  }

  if (/palexco|expocoruña|convention.?center|recinto|venue/i.test(blob) && /hotel/i.test(blob)) {
    if (features.includes("nearby_list") || features.includes("multi_hotel_list")) {
      return {
        surface: SURFACE_CLASS.VENUE_HOTEL_WIDGET,
        features,
        reason: REASON_CODE.GENERIC_HOTEL_LIST,
      };
    }
    return { surface: SURFACE_CLASS.OFFICIAL_VENUE_PAGE, features, reason: REASON_CODE.OTHER };
  }

  if (/\/events?\/|conference|congress|congreso|summit|forum|symposium|annual.?meeting/i.test(blob)) {
    if (/accommodation|housing|hotel\s*block|alojamiento/i.test(blob)) {
      return {
        surface: SURFACE_CLASS.OFFICIAL_EVENT_ACCOMMODATION,
        features,
        reason: REASON_CODE.OFFICIAL_HOUSING,
      };
    }
    return { surface: SURFACE_CLASS.OFFICIAL_EVENT_PAGE, features, reason: REASON_CODE.OTHER };
  }

  if (/association|asociaci|ispe\.org|society/i.test(blob)) {
    return {
      surface: SURFACE_CLASS.ORGANIZATION_PROGRAM_PAGE,
      features,
      reason: REASON_CODE.OTHER,
    };
  }

  // Keyword-only lodging with no organizer semantics
  if (/hotel|alojamiento|accommodation|lodging/i.test(blob) && !eventName) {
    return {
      surface: SURFACE_CLASS.OTHER_NOISE,
      features,
      reason: REASON_CODE.NO_EVENT_SPECIFIC_LODGING,
    };
  }

  return { surface: SURFACE_CLASS.OTHER_NOISE, features, reason: REASON_CODE.OTHER };
}

/**
 * Lodging relationship — requires organizer/event semantics, not bare "hotel" keyword.
 */
export function classifyLodgingRelationship({ text = "", url = "", surface = "" } = {}) {
  const t = `${url} ${text}`.toLowerCase();

  if (isNoiseSurface(surface)) {
    if (
      surface === SURFACE_CLASS.OTA ||
      surface === SURFACE_CLASS.HOTEL_DIRECTORY ||
      surface === SURFACE_CLASS.GENERIC_HOTELS_NEARBY ||
      surface === SURFACE_CLASS.DESTINATION_HOTEL_LIST ||
      surface === SURFACE_CLASS.TOURISM_DIRECTORY ||
      surface === SURFACE_CLASS.VENUE_HOTEL_WIDGET
    ) {
      return LODGING_RELATIONSHIP.GENERIC_NEARBY_HOTELS;
    }
  }

  if (/room\s*block|hotel\s*block|bloque\s*de\s*habitaciones|código\s*de\s*reserva\s*grup/i.test(t)) {
    return LODGING_RELATIONSHIP.OFFICIAL_ROOM_BLOCK;
  }
  if (/host\s*hotel|hotel\s*oficial|official\s*hotel/i.test(t)) {
    return LODGING_RELATIONSHIP.OFFICIAL_HOST_HOTEL;
  }
  if (/preferred\s*hotel|hotel\s*preferente/i.test(t)) {
    return LODGING_RELATIONSHIP.OFFICIAL_PREFERRED_HOTEL;
  }
  if (/housing\s*bureau|oficina\s*de\s*alojamiento|passkey|onpeak/i.test(t)) {
    return LODGING_RELATIONSHIP.HOUSING_BUREAU;
  }
  if (/team\s*hotel|hotel\s*del\s*equipo/i.test(t)) {
    return LODGING_RELATIONSHIP.TEAM_HOTEL;
  }
  if (/delegation\s*hotel|hotel\s*de\s*la\s*delegaci/i.test(t)) {
    return LODGING_RELATIONSHIP.DELEGATION_HOTEL;
  }
  if (/group\s*rate|tarifa\s*de\s*grupo|special\s*rate\s*code|booking\s*code/i.test(t)) {
    return LODGING_RELATIONSHIP.OFFICIAL_GROUP_RATE;
  }
  if (/overflow|hoteles?\s*adicionales|alternate\s*hotel/i.test(t) && /official|host|primary|block/i.test(t)) {
    return LODGING_RELATIONSHIP.OVERFLOW_EVIDENCED;
  }
  if (
    /hotel\s*tbd|hotel\s*por\s*determinar|housing\s*(coming\s*soon|forthcoming|to\s*be\s*announced)|accommodation\s*(tba|tbd|to\s*follow)/i.test(
      t
    )
  ) {
    return LODGING_RELATIONSHIP.HOTEL_SELECTION_OPEN;
  }
  if (/venue\s*(selected|confirmed|set).{0,40}hotel.{0,20}(open|tbd|pending)/i.test(t)) {
    return LODGING_RELATIONSHIP.VENUE_SET_HOTEL_OPEN;
  }
  if (
    surface === SURFACE_CLASS.ORGANIZER_CONTROLLED_HOUSING ||
    surface === SURFACE_CLASS.OFFICIAL_EVENT_ACCOMMODATION
  ) {
    return LODGING_RELATIONSHIP.OFFICIAL_ACCOMMODATION_PROGRAM;
  }
  if (/book\s*your\s*own|self.?book|attendees?\s*arrange|reserv(e|ar)\s*por\s*su\s*cuenta/i.test(t)) {
    return LODGING_RELATIONSHIP.ATTENDEE_SELF_BOOK;
  }
  if (
    /hoteles?\s+(cerca|near|en)|hotels?\s+near|several\s+hotels|close\s+to\s+several\s+hotels|mejores\s+hoteles|d[oó]nde\s+dormir/i.test(
      t
    )
  ) {
    return LODGING_RELATIONSHIP.GENERIC_NEARBY_HOTELS;
  }
  if (/hotel|alojamiento|accommodation|lodging/i.test(t)) {
    return LODGING_RELATIONSHIP.ATTENDEE_SELF_BOOK;
  }
  return LODGING_RELATIONSHIP.NO_LODGING_RELATIONSHIP;
}

export function gradeLodgingEvidence(relationship, organizerControl) {
  const strong = new Set([
    LODGING_RELATIONSHIP.OFFICIAL_ROOM_BLOCK,
    LODGING_RELATIONSHIP.OFFICIAL_HOST_HOTEL,
    LODGING_RELATIONSHIP.HOUSING_BUREAU,
    LODGING_RELATIONSHIP.OFFICIAL_GROUP_RATE,
    LODGING_RELATIONSHIP.TEAM_HOTEL,
    LODGING_RELATIONSHIP.DELEGATION_HOTEL,
    LODGING_RELATIONSHIP.OVERFLOW_EVIDENCED,
  ]);
  const bSet = new Set([
    LODGING_RELATIONSHIP.OFFICIAL_PREFERRED_HOTEL,
    LODGING_RELATIONSHIP.OFFICIAL_ACCOMMODATION_PROGRAM,
    LODGING_RELATIONSHIP.HOTEL_SELECTION_OPEN,
    LODGING_RELATIONSHIP.VENUE_SET_HOTEL_OPEN,
    LODGING_RELATIONSHIP.DESTINATION_SET_HOTEL_OPEN,
  ]);
  if (strong.has(relationship) && organizerControl === ORGANIZER_CONTROL.ORGANIZER_CONTROLLED) {
    return LODGING_GRADE.A;
  }
  if (strong.has(relationship) || bSet.has(relationship)) {
    return LODGING_GRADE.B;
  }
  if (
    relationship === LODGING_RELATIONSHIP.ATTENDEE_SELF_BOOK &&
    organizerControl !== ORGANIZER_CONTROL.UNKNOWN
  ) {
    return LODGING_GRADE.C;
  }
  if (
    relationship === LODGING_RELATIONSHIP.GENERIC_NEARBY_HOTELS ||
    relationship === LODGING_RELATIONSHIP.ATTENDEE_SELF_BOOK
  ) {
    return LODGING_GRADE.D;
  }
  return LODGING_GRADE.E;
}

export function classifyOrganizerControl({ text = "", surface = "", relationship = "" } = {}) {
  const t = String(text || "").toLowerCase();
  if (isNoiseSurface(surface)) return ORGANIZER_CONTROL.ATTENDEE_SELF_BOOK;
  if (
    /room\s*block|housing\s*bureau|host\s*hotel|official\s*hotel|group\s*rate\s*code|reserv(e|ation)\s*through\s*(the\s*)?(organizer|association|committee)/i.test(
      t
    ) ||
    [
      LODGING_RELATIONSHIP.OFFICIAL_ROOM_BLOCK,
      LODGING_RELATIONSHIP.OFFICIAL_HOST_HOTEL,
      LODGING_RELATIONSHIP.HOUSING_BUREAU,
      LODGING_RELATIONSHIP.OFFICIAL_GROUP_RATE,
      LODGING_RELATIONSHIP.OFFICIAL_ACCOMMODATION_PROGRAM,
    ].includes(relationship)
  ) {
    return ORGANIZER_CONTROL.ORGANIZER_CONTROLLED;
  }
  if (/team\s*hotel|federation/i.test(t) || relationship === LODGING_RELATIONSHIP.TEAM_HOTEL) {
    return ORGANIZER_CONTROL.TEAM_FEDERATION_CONTROLLED;
  }
  if (/dmc|destination\s*management|incentive\s*planner/i.test(t)) {
    return ORGANIZER_CONTROL.DMC_PLANNER_CONTROLLED;
  }
  if (/venue\s*(hotel|partner)|hotel\s*on.?site/i.test(t)) {
    return ORGANIZER_CONTROL.VENUE_CONTROLLED;
  }
  if (
    relationship === LODGING_RELATIONSHIP.ATTENDEE_SELF_BOOK ||
    relationship === LODGING_RELATIONSHIP.GENERIC_NEARBY_HOTELS
  ) {
    return ORGANIZER_CONTROL.ATTENDEE_SELF_BOOK;
  }
  return ORGANIZER_CONTROL.UNKNOWN;
}

/**
 * Openness requires affirmative language — not absence of a named hotel.
 */
export function classifyCommercialStatusV2({ text = "", relationship = "" } = {}) {
  const t = String(text || "").toLowerCase();
  if (/fully\s*placed|sold\s*out|block\s*closed|registration\s*closed|evento\s*finalizado/i.test(t)) {
    return COMMERCIAL_STATUS_V2.FULLY_PLACED;
  }
  if (/\b(201[0-9]|202[0-4])\b/.test(t) && !/\b202[6-9]\b/.test(t)) {
    return COMMERCIAL_STATUS_V2.CURRENT_CYCLE_CLOSED;
  }
  if (/hotel\s*tbd|hotel\s*por\s*determinar|accommodation\s*tbd|housing\s*tba/i.test(t)) {
    return COMMERCIAL_STATUS_V2.HOTEL_TBD;
  }
  if (/venue\s*tbd|sede\s*por\s*determinar|location\s*tbd/i.test(t)) {
    return COMMERCIAL_STATUS_V2.VENUE_TBD;
  }
  if (/rfp|request\s*for\s*proposal|site\s*selection|hosting\s*bid/i.test(t)) {
    return COMMERCIAL_STATUS_V2.RFP_ACTIVE;
  }
  if (
    /housing\s*(coming\s*soon|forthcoming|not\s*yet\s*open|to\s*be\s*announced)|accommodation\s*(details\s*)?(to\s*follow|pending)/i.test(
      t
    )
  ) {
    return COMMERCIAL_STATUS_V2.FUTURE_NOT_SOURCED;
  }
  if (
    relationship === LODGING_RELATIONSHIP.OVERFLOW_EVIDENCED ||
    (/overflow/i.test(t) && /official|host|primary/i.test(t))
  ) {
    return COMMERCIAL_STATUS_V2.PRIMARY_OVERFLOW;
  }
  if (
    relationship === LODGING_RELATIONSHIP.HOTEL_SELECTION_OPEN ||
    relationship === LODGING_RELATIONSHIP.VENUE_SET_HOTEL_OPEN
  ) {
    return COMMERCIAL_STATUS_V2.DESTINATION_SELECTED_HOTEL_OPEN;
  }
  if (
    /official\s*hotel|host\s*hotel|room\s*block/i.test(t) &&
    !/overflow|tbd|open|forthcoming/i.test(t)
  ) {
    return COMMERCIAL_STATUS_V2.PRIMARY_NO_OVERFLOW;
  }
  // Do NOT default to OPEN merely because no hotel named
  return COMMERCIAL_STATUS_V2.UNKNOWN;
}

export function classifyWinnability(status, grade) {
  if (
    status === COMMERCIAL_STATUS_V2.FULLY_PLACED ||
    status === COMMERCIAL_STATUS_V2.CURRENT_CYCLE_CLOSED ||
    status === COMMERCIAL_STATUS_V2.PRIMARY_NO_OVERFLOW
  ) {
    return WINNABILITY.NONE;
  }
  if (grade === LODGING_GRADE.D || grade === LODGING_GRADE.E) return WINNABILITY.NONE;
  if (
    status === COMMERCIAL_STATUS_V2.HOTEL_TBD ||
    status === COMMERCIAL_STATUS_V2.RFP_ACTIVE ||
    status === COMMERCIAL_STATUS_V2.PRIMARY_OVERFLOW ||
    status === COMMERCIAL_STATUS_V2.DESTINATION_SELECTED_HOTEL_OPEN
  ) {
    return grade === LODGING_GRADE.A || grade === LODGING_GRADE.B
      ? WINNABILITY.HIGH
      : WINNABILITY.MEDIUM;
  }
  if (status === COMMERCIAL_STATUS_V2.FUTURE_NOT_SOURCED) return WINNABILITY.MEDIUM;
  if (status === COMMERCIAL_STATUS_V2.OPEN_UNRESOLVED && (grade === "A" || grade === "B")) {
    return WINNABILITY.MEDIUM;
  }
  return WINNABILITY.UNKNOWN;
}

/**
 * Core surface eligibility gate.
 */
export function isGdiSurfaceEligible(input = {}) {
  const {
    url = "",
    title = "",
    text = "",
    eventName = "",
    organizer = "",
    futureCycle = "",
  } = input;

  const surfaceInfo = classifySurface({ url, title, text, eventName });
  const relationship = classifyLodgingRelationship({
    text,
    url,
    surface: surfaceInfo.surface,
  });
  const organizerControl = classifyOrganizerControl({
    text,
    surface: surfaceInfo.surface,
    relationship,
  });
  const grade = gradeLodgingEvidence(relationship, organizerControl);
  const commercialStatus = classifyCommercialStatusV2({ text, relationship });
  const winnability = classifyWinnability(commercialStatus, grade);

  const reasons = [];
  if (isNoiseSurface(surfaceInfo.surface)) {
    reasons.push(surfaceInfo.reason || REASON_CODE.DIRECTORY_SURFACE);
  }
  if (!eventName && !/conference|congress|tournament|meeting|summit|symposium|regatta/i.test(`${title} ${text.slice(0, 500)}`)) {
    reasons.push(REASON_CODE.EVENT_IDENTITY_AMBIGUOUS);
  }
  if (organizerControl === ORGANIZER_CONTROL.ATTENDEE_SELF_BOOK) {
    reasons.push(REASON_CODE.ATTENDEE_SELF_BOOK_ONLY);
  }
  if (organizerControl === ORGANIZER_CONTROL.UNKNOWN && grade !== LODGING_GRADE.A) {
    reasons.push(REASON_CODE.NO_ORGANIZER_CONTROL);
  }
  if (grade === LODGING_GRADE.D || grade === LODGING_GRADE.E) {
    reasons.push(REASON_CODE.NO_EVENT_SPECIFIC_LODGING);
  }
  if (
    commercialStatus === COMMERCIAL_STATUS_V2.FULLY_PLACED ||
    commercialStatus === COMMERCIAL_STATUS_V2.PRIMARY_NO_OVERFLOW
  ) {
    reasons.push(REASON_CODE.FULLY_PLACED);
  }
  if (commercialStatus === COMMERCIAL_STATUS_V2.CURRENT_CYCLE_CLOSED) {
    reasons.push(REASON_CODE.CURRENT_CYCLE_CLOSED);
  }
  if (
    commercialStatus === COMMERCIAL_STATUS_V2.HOTEL_TBD ||
    commercialStatus === COMMERCIAL_STATUS_V2.DESTINATION_SELECTED_HOTEL_OPEN ||
    commercialStatus === COMMERCIAL_STATUS_V2.RFP_ACTIVE
  ) {
    reasons.push(REASON_CODE.OPEN_HOTEL_DECISION);
  }
  if (relationship === LODGING_RELATIONSHIP.OVERFLOW_EVIDENCED) {
    reasons.push(REASON_CODE.OVERFLOW_SUPPORTED);
  }
  if (
    relationship === LODGING_RELATIONSHIP.OFFICIAL_ROOM_BLOCK ||
    relationship === LODGING_RELATIONSHIP.OFFICIAL_HOST_HOTEL
  ) {
    reasons.push(REASON_CODE.OFFICIAL_BLOCK);
  }
  if (commercialStatus === COMMERCIAL_STATUS_V2.FUTURE_NOT_SOURCED) {
    reasons.push(REASON_CODE.FUTURE_CYCLE_UNRESOLVED);
  }

  let eligibility = SURFACE_ELIGIBILITY.INELIGIBLE;
  if (isNoiseSurface(surfaceInfo.surface) || grade === LODGING_GRADE.D || grade === LODGING_GRADE.E) {
    eligibility = SURFACE_ELIGIBILITY.INELIGIBLE;
  } else if (
    (grade === LODGING_GRADE.A || grade === LODGING_GRADE.B) &&
    (winnability === WINNABILITY.HIGH || winnability === WINNABILITY.MEDIUM) &&
    organizerControl !== ORGANIZER_CONTROL.ATTENDEE_SELF_BOOK &&
    commercialStatus !== COMMERCIAL_STATUS_V2.UNKNOWN &&
    commercialStatus !== COMMERCIAL_STATUS_V2.FULLY_PLACED &&
    commercialStatus !== COMMERCIAL_STATUS_V2.PRIMARY_NO_OVERFLOW
  ) {
    eligibility = SURFACE_ELIGIBILITY.ELIGIBLE;
  } else if (
    grade === LODGING_GRADE.A ||
    grade === LODGING_GRADE.B ||
    grade === LODGING_GRADE.C ||
    commercialStatus === COMMERCIAL_STATUS_V2.FUTURE_NOT_SOURCED ||
    relationship === LODGING_RELATIONSHIP.HOTEL_SELECTION_OPEN
  ) {
    eligibility = SURFACE_ELIGIBILITY.WATCH_ELIGIBLE;
  }

  return {
    eligibility,
    surface: surfaceInfo.surface,
    features: surfaceInfo.features,
    lodgingRelationship: relationship,
    lodgingGrade: grade,
    organizerControl,
    commercialStatus,
    winnability,
    reasons: [...new Set(reasons)],
    eventName: eventName || null,
    organizer: organizer || null,
    futureCycle: futureCycle || null,
  };
}

/**
 * Map eligibility + grades to final corpus class.
 */
export function finalClassification(evalResult = {}, { readinessOk = false } = {}) {
  const { eligibility, lodgingGrade, commercialStatus, winnability, surface } = evalResult;
  if (readinessOk && eligibility === SURFACE_ELIGIBILITY.ELIGIBLE) {
    return FINAL_CLASS.CUSTOMER_READY;
  }
  if (isNoiseSurface(surface) || lodgingGrade === LODGING_GRADE.D || lodgingGrade === LODGING_GRADE.E) {
    return FINAL_CLASS.SURFACE_NOISE;
  }
  if (
    commercialStatus === COMMERCIAL_STATUS_V2.FULLY_PLACED ||
    commercialStatus === COMMERCIAL_STATUS_V2.CURRENT_CYCLE_CLOSED ||
    commercialStatus === COMMERCIAL_STATUS_V2.PRIMARY_NO_OVERFLOW ||
    winnability === WINNABILITY.NONE
  ) {
    return FINAL_CLASS.CLOSED_FULLY_PLACED;
  }
  if (eligibility === SURFACE_ELIGIBILITY.ELIGIBLE || eligibility === SURFACE_ELIGIBILITY.WATCH_ELIGIBLE) {
    if (
      commercialStatus === COMMERCIAL_STATUS_V2.FUTURE_NOT_SOURCED ||
      commercialStatus === COMMERCIAL_STATUS_V2.HOTEL_TBD ||
      commercialStatus === COMMERCIAL_STATUS_V2.DESTINATION_SELECTED_HOTEL_OPEN
    ) {
      return lodgingGrade === LODGING_GRADE.A || lodgingGrade === LODGING_GRADE.B
        ? FINAL_CLASS.HIGH_QUALITY_WATCH
        : FINAL_CLASS.FUTURE_WATCH;
    }
    return FINAL_CLASS.HIGH_QUALITY_WATCH;
  }
  if (evalResult.reasons?.includes(REASON_CODE.EVENT_IDENTITY_AMBIGUOUS)) {
    return FINAL_CLASS.INVALID;
  }
  return FINAL_CLASS.INVALID;
}

/**
 * Stricter lodging evidence — "hotel"/"accommodation" alone ≠ DIRECT.
 * Used to replace proven-source keyword DIRECT inflation.
 */
export function classifyLodgingEvidenceStrict(text = "", url = "") {
  const surface = classifySurface({ url, text }).surface;
  const relationship = classifyLodgingRelationship({ text, url, surface });
  const control = classifyOrganizerControl({ text, surface, relationship });
  const grade = gradeLodgingEvidence(relationship, control);
  if (grade === LODGING_GRADE.A) return "DIRECT";
  if (grade === LODGING_GRADE.B) return "STRONG_INFERENCE";
  if (grade === LODGING_GRADE.C) return "WEAK_INFERENCE";
  return "NONE";
}
