/**
 * Universal Scaling Corporate Accounts — local-growth vs destination-demand discovery.
 *
 * Two complementary commercial signals (same segment, same qualification gates):
 *   A. LOCAL_CORPORATE_GROWTH — office/facility/ops expansion in the hotel market
 *   B. DESTINATION_CORPORATE_MEETINGS — incentive / kickoff / retreat / recognition
 *      programs that may book rooms away from the company's home market
 *
 * Hotel-adaptive: all-inclusive / resort / hotel-zone profiles prefer B;
 * urban corporate hotels prefer A (and may still use light B for meetings).
 *
 * Does not redefine Scaling Companies taxonomy. Does not invent programs.
 * Does not call paid providers.
 */

import { DISCOVERY_PATHWAY } from "./autonomous-demand-discovery-v5.js";

export const SCALING_DESTINATION_DEMAND_DISCOVERY_V1 =
  "gdi-scaling-destination-demand-discovery-v1";

export const SCALING_HOTEL_DEMAND_MODE = Object.freeze({
  LOCAL_CORPORATE_GROWTH: "LOCAL_CORPORATE_GROWTH",
  DESTINATION_CORPORATE_MEETINGS: "DESTINATION_CORPORATE_MEETINGS",
  MIXED: "MIXED",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function lower(v) {
  return text(v).toLowerCase();
}

/** Shared negative terms — keep lodging operators / DMCs out of discovery queries. */
export const SCALING_DISCOVERY_NEGATIVE_TERMS =
  '-hotel -hoteles -resort -resorts -dmc -"destination management" -oasis -iberostar -marriott -hilton -hyatt -kempinski -"city express" -fiesta -allinclusive -"todo incluido" promo';

/**
 * Detect whether this hotel's Scaling discovery should emphasize destination
 * corporate meetings/incentives vs local office growth.
 */
export function detectScalingHotelDemandMode(hotel = {}) {
  const blob = lower(
    [
      hotel.displayName,
      hotel.name,
      hotel.city,
      hotel.demandTerritory?.label,
      ...(hotel.demandTerritory?.includes || []),
      ...(hotel.commercialPriorities?.demandAnchorFocus || []),
      ...(hotel.commercialPriorities?.targetSegments || []),
      hotel.capabilityProfile?.propertyType,
      hotel.capabilityProfile?.serviceLevel,
    ]
      .map(text)
      .join(" "),
  );

  const destinationHeavy =
    /all.?inclusive|todo incluido|incentive|retreat|hotel zone|resort|beach|riviera maya|caribbean|destination/.test(
      blob,
    );
  const urbanCorporate =
    /chapultepec|reforma|downtown|centro|business district|financial district|polanco|santa fe|cbd/.test(
      blob,
    ) ||
    (!destinationHeavy &&
      Number(hotel.capabilityProfile?.meetingRoomsIndoor || 0) >= 4 &&
      Number(hotel.capabilityProfile?.totalGuestrooms || 0) >= 80);

  if (destinationHeavy && !urbanCorporate) {
    return SCALING_HOTEL_DEMAND_MODE.DESTINATION_CORPORATE_MEETINGS;
  }
  if (destinationHeavy && urbanCorporate) {
    return SCALING_HOTEL_DEMAND_MODE.MIXED;
  }
  return SCALING_HOTEL_DEMAND_MODE.LOCAL_CORPORATE_GROWTH;
}

export function isDestinationCorporateDemandHotel(hotel = {}) {
  const mode = detectScalingHotelDemandMode(hotel);
  return (
    mode === SCALING_HOTEL_DEMAND_MODE.DESTINATION_CORPORATE_MEETINGS ||
    mode === SCALING_HOTEL_DEMAND_MODE.MIXED
  );
}

/**
 * Program / destination activity signals (not office expansion).
 * Used for pre-ORS evidence when hotel demand mode is destination-oriented.
 */
export const DESTINATION_PROGRAM_ACTIVITY_RE =
  /\b(incentive(?:\s+travel|\s+trip|\s+program)?|viajes? de incentivo|convenci[oó]n(?:es)? de ventas|kickoff(?:\s+comercial|\s+de ventas|\s+sales)?|sales kickoff|leadership retreat|retiro(?:s)? de liderazgo|corporate retreat|distributor(?:s)? meeting|convenci[oó]n(?:es)? de distribuidores|employee recognition|programa(?:s)? de reconocimiento|premio a empleados|residential training|capacitaci[oó]n residencial|annual (?:sales )?meeting|company gathering|partner (?:meeting|summit)|evento(?:s)? corporativ(?:o|os)|reuniones? corporativas?|multi-?day (?:sales|leadership|training))\b/i;

/**
 * Company-first + program-first discovery strategies for destination demand.
 * Destination city appears as the meeting location — company need not be local.
 */
export function buildDestinationCorporateDemandStrategies(hotel = {}) {
  const city = text(hotel.city || hotel.demandTerritory?.label || "destination");
  const geoAlt = [
    city,
    ...((hotel.demandTerritory?.includes || [])
      .map((s) => text(String(s).split(/\s+where\b/)[0]))
      .filter((s) => s.length >= 3 && s.length <= 40)),
  ]
    .filter(Boolean)
    .slice(0, 4);
  const destClause = geoAlt.map((g) => `"${g}"`).join(" OR ");
  const neg = SCALING_DISCOVERY_NEGATIVE_TERMS;
  const spanishMarket =
    /mexico|méxico|mx|canc[uú]n|riviera/i.test(
      `${hotel.country || ""} ${city} ${hotel.demandTerritory?.label || ""}`,
    );

  const strategies = [
    {
      pathway: DISCOVERY_PATHWAY.TRIGGER_LED,
      segment: "scaling_companies",
      intent: "destination_incentive_program_en",
      demandMode: SCALING_HOTEL_DEMAND_MODE.DESTINATION_CORPORATE_MEETINGS,
      researchQuestion: `(${destClause}) ("incentive travel" OR "incentive trip" OR "incentive program" OR "sales incentive" OR "employee incentive") (company OR corporate OR "sales team" OR distributor) ${neg}`,
      sourceTypes: ["corp_news", "incentive_case_studies", "hr_program_pages"],
      discoveryWeight: 6,
      underrepresentedPriority: true,
      notes: "Program-first: destination is meeting location, not company HQ requirement",
    },
    {
      pathway: DISCOVERY_PATHWAY.TRIGGER_LED,
      segment: "scaling_companies",
      intent: "destination_sales_kickoff_retreat_en",
      demandMode: SCALING_HOTEL_DEMAND_MODE.DESTINATION_CORPORATE_MEETINGS,
      researchQuestion: `(${destClause}) ("sales kickoff" OR "annual sales meeting" OR "leadership retreat" OR "corporate retreat" OR "partner summit" OR "distributor meeting") (company OR corporate OR brand) ${neg}`,
      sourceTypes: ["corp_news", "sales_ops_announcements"],
      discoveryWeight: 6,
      underrepresentedPriority: true,
    },
    {
      pathway: DISCOVERY_PATHWAY.ORGANIZATION_LED,
      segment: "scaling_companies",
      intent: "destination_company_first_recognition_training_en",
      demandMode: SCALING_HOTEL_DEMAND_MODE.DESTINATION_CORPORATE_MEETINGS,
      researchQuestion: `"${city}" (company OR corporat*) ("employee recognition" OR "presidents club" OR "winner's trip" OR "residential training" OR "customer advisory board") (Cancun OR Cancún OR destination OR resort-style) ${neg}`,
      sourceTypes: ["hr_program_pages", "corp_news"],
      discoveryWeight: 5,
      underrepresentedPriority: true,
    },
  ];

  if (spanishMarket) {
    strategies.push(
      {
        pathway: DISCOVERY_PATHWAY.TRIGGER_LED,
        segment: "scaling_companies",
        intent: "destination_incentive_es",
        language: "es",
        demandMode: SCALING_HOTEL_DEMAND_MODE.DESTINATION_CORPORATE_MEETINGS,
        researchQuestion: `(${destClause}) ("viaje de incentivo" OR "viajes de incentivo empresariales" OR "programa de incentivos" OR "premio a empleados" OR "programa de reconocimiento") (empresa OR corporativo OR comercial) ${neg}`,
        sourceTypes: ["corp_news_es", "hr_program_pages_es"],
        discoveryWeight: 6,
        underrepresentedPriority: true,
      },
      {
        pathway: DISCOVERY_PATHWAY.TRIGGER_LED,
        segment: "scaling_companies",
        intent: "destination_convencion_kickoff_es",
        language: "es",
        demandMode: SCALING_HOTEL_DEMAND_MODE.DESTINATION_CORPORATE_MEETINGS,
        researchQuestion: `(${destClause}) ("convención de ventas" OR "kickoff comercial" OR "kickoff de ventas" OR "retiro de liderazgo" OR "convención de distribuidores" OR "reunión de socios") (empresa OR marca OR corporativo) ${neg}`,
        sourceTypes: ["corp_news_es", "sales_ops_es"],
        discoveryWeight: 6,
        underrepresentedPriority: true,
      },
      {
        pathway: DISCOVERY_PATHWAY.ORGANIZATION_LED,
        segment: "scaling_companies",
        intent: "destination_capacitacion_residencial_es",
        language: "es",
        demandMode: SCALING_HOTEL_DEMAND_MODE.DESTINATION_CORPORATE_MEETINGS,
        researchQuestion: `(${destClause}) (empresa OR corporativo) ("capacitación residencial" OR "entrenamiento residencial" OR "evento corporativo" OR "reuniones corporativas") (hospedaje OR "todo incluido" OR resort) ${neg}`,
        sourceTypes: ["training_calendars_es"],
        discoveryWeight: 5,
        underrepresentedPriority: true,
      },
    );
  }

  return strategies.map((s) => ({
    ...s,
    version: SCALING_DESTINATION_DEMAND_DISCOVERY_V1,
    demandSeeking: true,
    track: "NEW_DISCOVERY",
  }));
}

/**
 * Local-growth strategies with lodging/DMC negatives (city / corporate hotels).
 */
export function buildLocalCorporateGrowthStrategies(hotel = {}) {
  const city = text(hotel.city || "market");
  const neg = SCALING_DISCOVERY_NEGATIVE_TERMS;
  const spanishMarket = /mexico|méxico|mx/i.test(
    `${hotel.country || ""} ${city}`,
  );
  const strategies = [
    {
      pathway: DISCOVERY_PATHWAY.TRIGGER_LED,
      segment: "scaling_companies",
      intent: "local_office_expansion_en",
      demandMode: SCALING_HOTEL_DEMAND_MODE.LOCAL_CORPORATE_GROWTH,
      researchQuestion: `"${city}" ("opens office" OR "opened an office" OR "new office" OR "regional hub" OR headquarters) (company OR corporate OR firm) (training OR meetings OR lodging OR "project team") ${neg}`,
      sourceTypes: ["corp_announcements"],
      discoveryWeight: 4,
    },
    {
      pathway: DISCOVERY_PATHWAY.ORGANIZATION_LED,
      segment: "scaling_companies",
      intent: "local_training_meetings_en",
      demandMode: SCALING_HOTEL_DEMAND_MODE.LOCAL_CORPORATE_GROWTH,
      researchQuestion: `"${city}" (company OR corporate) ("sales kickoff" OR offsite OR "leadership meeting" OR "customer training" OR "project team") (hotel OR lodging OR meetings) ${neg}`,
      sourceTypes: ["training_calendars", "corp_news"],
      discoveryWeight: 4,
    },
  ];
  if (spanishMarket) {
    strategies.push({
      pathway: DISCOVERY_PATHWAY.TRIGGER_LED,
      segment: "scaling_companies",
      intent: "local_office_expansion_es",
      language: "es",
      demandMode: SCALING_HOTEL_DEMAND_MODE.LOCAL_CORPORATE_GROWTH,
      researchQuestion: `"${city}" ("nueva oficina" OR "apertura de oficinas" OR "nueva sede" OR "hub regional") (empresa OR corporativo) (capacitación OR reuniones OR hospedaje OR "equipo de proyecto") ${neg}`,
      sourceTypes: ["corp_announcements_es"],
      discoveryWeight: 4,
    });
  }
  return strategies.map((s) => ({
    ...s,
    version: SCALING_DESTINATION_DEMAND_DISCOVERY_V1,
    demandSeeking: true,
    track: "NEW_DISCOVERY",
  }));
}

/**
 * Merge hotel-adaptive strategy set: destination-first for resorts, local-first for city.
 */
export function buildAdaptiveScalingDiscoveryStrategies(hotel = {}) {
  const mode = detectScalingHotelDemandMode(hotel);
  if (mode === SCALING_HOTEL_DEMAND_MODE.DESTINATION_CORPORATE_MEETINGS) {
    // Destination resorts: do NOT mix in local office-opening queries — those
    // dominated Cancún yield with lodging competitors and thin geo listings.
    return [...buildDestinationCorporateDemandStrategies(hotel)];
  }
  if (mode === SCALING_HOTEL_DEMAND_MODE.MIXED) {
    return [
      ...buildDestinationCorporateDemandStrategies(hotel).slice(0, 3),
      ...buildLocalCorporateGrowthStrategies(hotel),
    ];
  }
  return [
    ...buildLocalCorporateGrowthStrategies(hotel),
    ...buildDestinationCorporateDemandStrategies(hotel).slice(0, 2),
  ];
}

/**
 * Buyer-research query hints for destination programs (append to investigation).
 */
export function destinationBuyerResearchHints() {
  return [
    "meetings and incentives manager",
    "incentive travel program",
    "sales operations director",
    "director of sales enablement",
    "HR recognition program",
    "gerente de eventos corporativos",
    "director comercial",
    "capacitación corporativa",
  ];
}
