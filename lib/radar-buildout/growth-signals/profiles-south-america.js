/**
 * Growth signal profiles — South America CALA markets.
 */
import { submarketProfile, growthSignal as sig } from "./signal-factory.js";

export const SOUTH_AMERICA_GROWTH_PROFILES = [
  submarketProfile("Colombia", "South America", {
    submarket: "Cartagena",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Resort", "Lifestyle", "Full-Service"],
    ownerBrandSummary:
      "Walled city + Bocagrande/Manga urban resort — supply growing; niche and lifestyle gaps remain.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "accelerating",
        summary:
          "CTG air connectivity expansion supports international leisure and cruise combination trips.",
        ownerBrandTakeaway:
          "Lifestyle/boutique in heritage buffer; large resort only in entitled coastal zones.",
        sourceReference: "https://www.colombia.travel/",
        dataConfidence: "Medium",
      }),
      sig({
        signalType: "cruise_turnaround_growth",
        direction: "stable",
        summary: "Cartagena cruise port supports turnaround and home-port Caribbean itineraries.",
        ownerBrandTakeaway:
          "Select-service near port for cruise nights; walled-city boutique for leisure.",
        sourceReference: "https://www.colombia.travel/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Colombia", "South America", {
    submarket: "Bogotá",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Select-Service", "Full-Service", "Extended-Stay"],
    ownerBrandSummary: "Corporate hub — weekday transient and group; not leisure resort.",
    signals: [
      sig({
        signalType: "mice_capacity_growth",
        direction: "accelerating",
        summary: "Corferias and metro events capacity support group hotel demand.",
        ownerBrandTakeaway:
          "Full-service with meeting inventory near financial and convention corridors.",
        sourceReference: "https://www.colombia.travel/",
        dataConfidence: "Medium",
      }),
      sig({
        signalType: "air_seat_growth",
        direction: "stable",
        summary: "BOG hub connectivity supports corporate and regional transient demand.",
        ownerBrandTakeaway:
          "Select-service near airport and business districts.",
        sourceReference: "https://www.eldorado.aero/",
        dataConfidence: "High",
      }),
    ],
  }),
  submarketProfile("Colombia", "South America", {
    submarket: "Medellín",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium-high",
    primaryBuildProducts: ["Select-Service", "Lifestyle", "Full-Service"],
    ownerBrandSummary: "Innovation district and MICE growth — urban lifestyle opportunity.",
    signals: [
      sig({
        signalType: "mice_capacity_growth",
        direction: "accelerating",
        summary: "Plaza Mayor and metro events infrastructure expanding group demand.",
        ownerBrandTakeaway:
          "Full-service/lifestyle in El Poblado and convention-adjacent nodes.",
        sourceReference: "https://www.colombia.travel/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Peru", "South America", {
    submarket: "Miraflores",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Select-Service", "Full-Service", "Lifestyle"],
    ownerBrandSummary: "Lima leisure/corporate crossover submarket.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "stable",
        linkedAnchorNames: ["Jorge Chávez International Airport"],
        summary: "LIM gateway supports Miraflores corporate and leisure transient demand.",
        ownerBrandTakeaway:
          "Select-service/full-service in Miraflores — verify supply before upscale greenfield.",
        sourceReference: "https://www.lima-airport.com/",
        dataConfidence: "High",
      }),
    ],
  }),
  submarketProfile("Peru", "South America", {
    submarket: "Cusco Historic Center",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Boutique", "Lifestyle", "Select-Service"],
    ownerBrandSummary: "Heritage tourism — boutique scale; entitlement constraints.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "accelerating",
        summary: "CUZ and Sacred Valley air access expanding heritage tourism arrivals.",
        ownerBrandTakeaway:
          "Boutique/lifestyle in historic fabric; large resort outside heritage core.",
        sourceReference: "https://www.peru.travel/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Chile", "South America", {
    submarket: "Las Condes",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Full-Service", "Upper-Upscale", "Select-Service"],
    ownerBrandSummary: "Santiago corporate core — weekday transient.",
    signals: [
      sig({
        signalType: "employer_free_zone_expansion",
        direction: "stable",
        summary: "Financial and corporate office concentration in Las Condes/Providencia.",
        ownerBrandTakeaway:
          "Upper-upscale full-service for corporate accounts; not resort product.",
        sourceReference: "https://www.chile.travel/",
        dataConfidence: "Medium",
      }),
      sig({
        signalType: "mice_capacity_growth",
        direction: "stable",
        linkedAnchorNames: ["Espacio Riesco", "Movistar Arena"],
        summary: "Convention and events corridor supports group compression.",
        ownerBrandTakeaway:
          "Meeting-heavy full-service near Vitacura/convention nodes.",
        sourceReference: "https://www.chile.travel/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Chile", "South America", {
    submarket: "Airport Corridor",
    profileStatus: "researched",
    earlyEntryOpportunity: "medium-high",
    primaryBuildProducts: ["Select-Service", "Extended-Stay"],
    ownerBrandSummary: "SCL airport corridor transit and crew demand.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "stable",
        linkedAnchorNames: ["Arturo Merino Benítez International Airport"],
        summary: "SCL hub growth supports airport-corridor select-service.",
        ownerBrandTakeaway:
          "Build select-service/extended-stay near SCL access before corridor infill.",
        sourceReference: "https://www.nuevopudahuel.cl/",
        dataConfidence: "High",
      }),
    ],
  }),
  submarketProfile("Ecuador", "South America", {
    submarket: "Quito",
    profileStatus: "skeleton",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Select-Service", "Full-Service"],
    ownerBrandSummary: "Corporate and government urban demand.",
    signals: [
      sig({
        signalType: "mice_capacity_growth",
        direction: "emerging",
        summary: "Quito convention and events infrastructure supporting group demand.",
        ownerBrandTakeaway: "Full-service with meetings near business districts.",
        sourceReference: "https://ecuador.travel/",
        dataConfidence: "Low",
      }),
    ],
  }),
  submarketProfile("Uruguay", "South America", {
    submarket: "Punta del Este",
    profileStatus: "skeleton",
    earlyEntryOpportunity: "medium",
    primaryBuildProducts: ["Resort", "Upper-Upscale", "Lifestyle"],
    ownerBrandSummary: "Seasonal luxury leisure — timing around southern cone summer.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "stable",
        summary: "Seasonal air capacity from Argentina/Brazil supports summer compression.",
        ownerBrandTakeaway:
          "Luxury/lifestyle resort with strong seasonal yield strategy required.",
        sourceReference: "https://www.uruguaynatural.com/",
        dataConfidence: "Medium",
      }),
    ],
  }),
  submarketProfile("Brazil", "South America", {
    submarket: "Guarulhos / Airport Corridor",
    profileStatus: "planned",
    earlyEntryOpportunity: "unknown",
    primaryBuildProducts: ["Select-Service", "Extended-Stay"],
    ownerBrandSummary: "Deferred CALA build — skeleton only; GRU corridor is primary São Paulo transit lane.",
    signals: [
      sig({
        signalType: "air_seat_growth",
        direction: "unknown",
        summary: "GRU hub scale supports airport-corridor select-service (deferred deep build).",
        ownerBrandTakeaway: "Defer detailed build until Brazil sequence reactivated.",
        sourceReference: "https://www.gru.com.br/",
        dataConfidence: "Low",
      }),
    ],
  }),
];
