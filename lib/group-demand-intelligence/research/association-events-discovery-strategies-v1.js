/**
 * Association / Conference / Event-Driven discovery strategies V1.
 *
 * Reuses Event-to-Demand domain taxonomy — does NOT invent a new RESEARCH_SEGMENT.
 * Primary segment axis: associations_government (+ medical_academic / exhibitor when domain fits).
 */

import { RESEARCH_SEGMENT } from "../workflow/hotel-segment-research-plan-v1.js";
import {
  EVENT_DEMAND_DOMAIN,
  EVENT_TO_DEMAND_VERSION,
  primarySegmentForDomain,
} from "./event-to-demand/event-to-demand-constants-v1.js";
import {
  detectScalingHotelDemandMode,
  SCALING_HOTEL_DEMAND_MODE,
} from "./scaling-destination-demand-discovery-v1.js";

export const ASSOCIATION_EVENTS_DISCOVERY_V1 =
  "gdi-association-events-discovery-strategies-v1";

export const ASSOCIATION_EVENTS_LAUNCHER_SEGMENT = "associations_government";

/** Lodging/DMC/hotel-brand negatives shared with Scaling destination discovery. */
export const ASSOCIATION_EVENTS_NEGATIVE_TERMS =
  '-hotel -hoteles -resort -resorts -dmc -"destination management" -oasis -iberostar -marriott -hilton -hyatt -kempinski -"city express" -fiesta americana -allinclusive -"todo incluido" promo';

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function isCancunDestination(hotel = {}) {
  const blob = `${hotel.displayName || ""} ${hotel.city || ""} ${hotel.demandTerritory?.label || ""}`.toLowerCase();
  return /canc[uú]n|riviera maya|condesa cancun|all.?inclusive/.test(blob);
}

function isMexicoCityUrban(hotel = {}) {
  const blob = `${hotel.displayName || ""} ${hotel.city || ""} ${hotel.demandTerritory?.label || ""}`.toLowerCase();
  return /mexico city|ciudad de m[eé]xico|cdmx|chapultepec|polanco|reforma/.test(blob);
}

/**
 * Hotel-adaptive association/event discovery strategies (EN + ES).
 */
export function buildAssociationEventsDiscoveryStrategies(hotel = {}) {
  const city = text(hotel.city || hotel.demandTerritory?.label || "market");
  const neg = ASSOCIATION_EVENTS_NEGATIVE_TERMS;
  const destination = isCancunDestination(hotel);
  const cdmx = isMexicoCityUrban(hotel);
  const mode = detectScalingHotelDemandMode(hotel);
  const strategies = [];

  const push = (row) => {
    strategies.push({
      ...row,
      version: ASSOCIATION_EVENTS_DISCOVERY_V1,
      eventToDemandVersion: EVENT_TO_DEMAND_VERSION,
      researchSegment:
        row.researchSegment ||
        primarySegmentForDomain(row.domain) ||
        RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      pathway: row.pathway || "EVENT_LED",
      demandSeeking: true,
      track: "NEW_DISCOVERY",
    });
  };

  if (destination || mode === SCALING_HOTEL_DEMAND_MODE.DESTINATION_CORPORATE_MEETINGS) {
    const destClause = `"${city}" OR "Riviera Maya" OR Cancun OR Cancún`;
    push({
      intent: "assoc_destination_congress_en",
      domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
      researchQuestion: `(${destClause}) (association OR society OR federation OR colegio) (congress OR conference OR "annual meeting" OR convención OR congreso) (2025 OR 2026 OR 2027) ${neg}`,
      discoveryWeight: 7,
    });
    push({
      intent: "assoc_destination_medical_en",
      domain: EVENT_DEMAND_DOMAIN.MEDICAL_SCIENTIFIC_ACADEMIC,
      researchSegment: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      researchQuestion: `(${destClause}) (medical OR scientific OR academic OR médico OR científico) (congress OR symposium OR conference OR congreso) (association OR society OR colegio) (2025 OR 2026 OR 2027) ${neg}`,
      discoveryWeight: 7,
    });
    push({
      intent: "assoc_destination_incentive_residential_en",
      domain: EVENT_DEMAND_DOMAIN.CORPORATE_EVENT_MEETING,
      researchQuestion: `(${destClause}) ("incentive travel" OR "sales kickoff" OR "distributor meeting" OR "leadership retreat" OR "residential training") (association OR company OR corporate OR brand) ${neg}`,
      discoveryWeight: 6,
    });
    push({
      intent: "assoc_destination_congress_es",
      language: "es",
      domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
      researchQuestion: `(${destClause}) (asociación OR colegio OR federación OR sociedad) (congreso OR convención OR "reunión anual" OR "asamblea") (2025 OR 2026 OR 2027) ${neg}`,
      discoveryWeight: 7,
    });
    push({
      intent: "assoc_destination_medico_es",
      language: "es",
      domain: EVENT_DEMAND_DOMAIN.MEDICAL_SCIENTIFIC_ACADEMIC,
      researchSegment: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      researchQuestion: `(${destClause}) (congreso médico OR simposio OR "congreso científico" OR "jornadas médicas") (asociación OR colegio OR sociedad) (2025 OR 2026 OR 2027) ${neg}`,
      discoveryWeight: 6,
    });
  }

  if (cdmx || mode === SCALING_HOTEL_DEMAND_MODE.LOCAL_CORPORATE_GROWTH || !destination) {
    const geo = cdmx
      ? `"Ciudad de México" OR CDMX OR "Mexico City" OR Chapultepec OR Polanco OR Reforma`
      : `"${city}"`;
    push({
      intent: "assoc_city_congress_en",
      domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
      researchQuestion: `(${geo}) (association OR society OR federation OR colegio OR cámara) (congress OR conference OR "annual meeting" OR board meeting OR workshop) (2025 OR 2026 OR 2027) ${neg}`,
      discoveryWeight: 6,
    });
    push({
      intent: "assoc_city_midsize_meetings_en",
      domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
      researchQuestion: `(${geo}) (association OR society) ("committee meeting" OR "board meeting" OR "executive meeting" OR "professional development" OR workshop OR "training seminar") (hotel OR venue OR "meeting space" OR lodging) ${neg}`,
      discoveryWeight: 5,
    });
    push({
      intent: "assoc_city_medical_en",
      domain: EVENT_DEMAND_DOMAIN.MEDICAL_SCIENTIFIC_ACADEMIC,
      researchSegment: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      researchQuestion: `(${geo}) (medical OR scientific OR academic) (congress OR symposium OR conference) (association OR society OR colegio) (2025 OR 2026 OR 2027) ${neg}`,
      discoveryWeight: 6,
    });
    push({
      intent: "assoc_citywide_ancillary_en",
      domain: EVENT_DEMAND_DOMAIN.DESTINATION_CITYWIDE,
      researchQuestion: `(${geo}) ("convention center" OR "centro de convenciones" OR Centro Citibanamex OR Banamex) (congress OR exhibition OR trade show) (hotel OR overflow OR speakers OR lodging OR "room block") (2025 OR 2026 OR 2027) ${neg}`,
      discoveryWeight: 5,
    });
    push({
      intent: "assoc_city_congress_es",
      language: "es",
      domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
      researchQuestion: `(${geo}) (asociación OR colegio OR cámara OR federación) (congreso OR convención OR asamblea OR "junta de consejo" OR taller) (2025 OR 2026 OR 2027) ${neg}`,
      discoveryWeight: 6,
    });
    push({
      intent: "assoc_city_medico_es",
      language: "es",
      domain: EVENT_DEMAND_DOMAIN.MEDICAL_SCIENTIFIC_ACADEMIC,
      researchSegment: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
      researchQuestion: `(${geo}) (congreso médico OR simposio OR "jornadas académicas") (asociación OR colegio OR sociedad) (2025 OR 2026 OR 2027) ${neg}`,
      discoveryWeight: 5,
    });
    // Mexico-localized housing / sede / prior-host investigation (Bethesda-parity depth)
    push({
      intent: "assoc_city_housing_sede_es",
      language: "es",
      domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
      researchQuestion: `(${geo}) (congreso OR convención OR expo OR cumbre) ("hotel sede" OR "sede hotel" OR "hoteles oficiales" OR hospedaje OR alojamiento OR "bloque de habitaciones" OR "agencia de viajes" OR PCO OR "organizador de congresos") (2025 OR 2026 OR 2027)`,
      discoveryWeight: 7,
    });
    push({
      intent: "assoc_city_prior_host_es",
      language: "es",
      domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
      researchQuestion: `(${geo}) (congreso OR convención) ("edición anterior" OR "ediciones anteriores" OR "se realizó en" OR "tuvo lugar en" OR "hotel sede") (Marriott OR Hilton OR Hyatt OR "Fiesta Americana" OR Westin OR InterContinental OR Live Aqua)`,
      discoveryWeight: 6,
    });
  }

  // Organizer / PCO / meetings department follow-on (always useful)
  push({
    intent: "assoc_organizer_pco_en",
    domain: EVENT_DEMAND_DOMAIN.EVENT_INTERMEDIARY,
    researchSegment: RESEARCH_SEGMENT.PROFESSIONAL_PLANNERS_DMC,
    researchQuestion: `"${city}" (PCO OR "professional conference organizer" OR "event organizer" OR "congresos" OR "organización de eventos") (association OR asociación) (hotel OR venue OR lodging) ${neg}`,
    discoveryWeight: 4,
    commercialOpportunityTrack: "TRACK_A_UPCOMING_VENUE",
  });

  // Track B — recurring / historical competitive capture (not limited to announced future dates)
  const geoOrDest = destination
    ? `"${city}" OR "Riviera Maya" OR Cancun OR Cancún`
    : cdmx
      ? `"Ciudad de México" OR CDMX OR "Mexico City"`
      : `"${city}"`;
  push({
    intent: "events_recurring_competitive_capture_en",
    domain: EVENT_DEMAND_DOMAIN.CORPORATE_EVENT_MEETING,
    researchQuestion: `(${geoOrDest}) ("annual incentive" OR "president's club" OR "sales incentive" OR "recognition trip" OR "dealer trip" OR "gold trip" OR "passport to paradise") (hotel OR resort OR "hosted at" OR "took place") ${neg}`,
    discoveryWeight: 7,
    commercialOpportunityTrack: "TRACK_B_RECURRING_COMPETITIVE_CAPTURE",
    track: "RECURRING_COMPETITIVE_CAPTURE",
  });
  push({
    intent: "events_recurring_congress_history_es",
    language: "es",
    domain: EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS,
    researchQuestion: `(${geoOrDest}) (congreso OR convención OR "viaje de incentivo" OR "retiro corporativo") ("edición" OR anual OR "ediciones anteriores" OR "hotel sede") (2023 OR 2024 OR 2025 OR 2026)`,
    discoveryWeight: 6,
    commercialOpportunityTrack: "TRACK_B_RECURRING_COMPETITIVE_CAPTURE",
    track: "RECURRING_COMPETITIVE_CAPTURE",
  });
  push({
    intent: "events_incentive_viaje_es",
    language: "es",
    domain: EVENT_DEMAND_DOMAIN.CORPORATE_EVENT_MEETING,
    researchQuestion: `(${geoOrDest}) ("viaje de incentivo" OR "viaje de reconocimiento" OR "premio a distribuidores" OR "convención de ventas") (resort OR hotel OR destino) ${neg}`,
    discoveryWeight: 6,
    commercialOpportunityTrack: "TRACK_B_RECURRING_COMPETITIVE_CAPTURE",
    track: "RECURRING_COMPETITIVE_CAPTURE",
  });

  return strategies.sort((a, b) => (b.discoveryWeight || 0) - (a.discoveryWeight || 0));
}

export function hotelAssociationEventsFitProfile(hotel = {}) {
  const id = String(hotel.hotelId || hotel.id || "");
  if (id === "recterKmNPdntCm0v" || /chapultepec/i.test(hotel.displayName || "")) {
    return {
      hotelId: id || "recterKmNPdntCm0v",
      positioning: "urban_executive_meetings",
      meetingRooms: 7,
      ballroomCapacityUpTo: 220,
      strengths: [
        "Chapultepec / Polanco / Reforma location",
        "Seven meeting rooms and executive facilities",
        "Ballroom capacity up to ~220",
        "Catering and business-oriented positioning",
      ],
      limitations: [
        "Not positioned as host for every large citywide convention",
        "Best for mid-size conferences, board/committee meetings, speakers, ancillary events, delegate blocks",
      ],
      preferredMechanisms: [
        "HOST_EVENT",
        "OFFSITE_MEETING",
        "EXECUTIVE_DELEGATION",
        "SMALL_GROUP_LODGING",
        "ANCILLARY_EVENT_BUSINESS",
      ],
      sourceUrl:
        "https://www.fiestamericanatravelty.com/en/grand-fiesta-americana/hotels/grand-fiesta-americana-chapultepec/meeting-events/",
    };
  }
  if (id === "recIEcSWuhSCoNLFN" || /condesa cancun/i.test(hotel.displayName || "")) {
    return {
      hotelId: id || "recIEcSWuhSCoNLFN",
      positioning: "destination_all_inclusive_meetings",
      meetingRooms: 12,
      eventSpaceSqFt: 21600,
      strengths: [
        "All-inclusive beachfront destination",
        "12 meeting rooms / 21,600+ sq ft event space",
        "Indoor and outdoor settings",
        "Residential multiday conference suitability",
      ],
      limitations: [
        "Organization need not be Cancún-based — destination program must fit resort format",
        "Already-contracted venues are not inventable capture opportunities",
      ],
      preferredMechanisms: [
        "HOST_EVENT",
        "SMALL_GROUP_LODGING",
        "EXECUTIVE_DELEGATION",
        "ANCILLARY_EVENT_BUSINESS",
      ],
      sourceUrl:
        "https://www.fiestamericanatravelty.com/en/fiesta-americana/hotels/fiesta-americana-condesa-cancun-all-inclusive/meeting-events",
    };
  }
  if (id === "recC2kbCaS48jFjFF" || /nobu.*london.*portman/i.test(hotel.displayName || "")) {
    return {
      hotelId: id || "recC2kbCaS48jFjFF",
      positioning: "urban_marylebone_meetings",
      guestrooms: 243,
      ballroomReceptionUpTo: 700,
      strengths: [
        "Marylebone / Portman Square luxury lifestyle location",
        "243 rooms and suites",
        "Ballroom reception capacity ~700 plus boardrooms and private dining",
        "Credible for mid-size association meetings, executive dinners, speaker/VIP blocks",
      ],
      limitations: [
        "Not every London citywide congress is hostable on-property",
        "ExCeL / Olympia / airport programmes need evidenced Marylebone overnight",
        "Association contacts are not automatically planners",
      ],
      preferredMechanisms: [
        "HOST_EVENT",
        "OFFSITE_MEETING",
        "EXECUTIVE_DELEGATION",
        "SMALL_GROUP_LODGING",
        "ANCILLARY_EVENT_BUSINESS",
      ],
      sourceUrl: "https://www.nobuhotels.com/london-portman/",
    };
  }
  return {
    hotelId: id,
    positioning: "generic",
    strengths: [],
    limitations: [],
    preferredMechanisms: ["HOST_EVENT", "SMALL_GROUP_LODGING", "ANCILLARY_EVENT_BUSINESS"],
  };
}
