/**
 * Uruguay countrywide demand anchor candidates (source-backed).
 */
import { createSouthAmericaCandidateBuilder } from "./south-america-country-shared.js";
import {
  applyUruguayGovernanceDefaults,
  URUGUAY_SUBMARKETS,
} from "./uruguay-demand-anchor-governance.js";

const COUNTRY = "Uruguay";

const pt = createSouthAmericaCandidateBuilder(COUNTRY, applyUruguayGovernanceDefaults);

/** @type {ReturnType<typeof pt>[]} */
export const URUGUAY_CANDIDATES = [
  // Montevideo (17)
  pt({
    name: "Aeropuerto Internacional de Carrasco",
    pointType: "Future Growth Node",
    city: "Ciudad de la Costa",
    submarket: "Montevideo",
    latitude: -34.8384,
    longitude: -56.0308,
    sourceReference: "https://www.aeropuertodecarrasco.com.uy/",
    manuallyVerified: true,
    hotelDemandNote:
      "Primary international gateway for Uruguay; supports airport-adjacent and metro-wide transit hotel demand.",
  }),
  pt({
    name: "Ciudad Vieja Historic District",
    pointType: "Tourist Attraction",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.9069,
    longitude: -57.8589,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/montevideo/ciudad-vieja",
    manuallyVerified: true,
    hotelDemandNote: "Colonial core with museums and heritage tourism driving city-center hotel demand.",
  }),
  pt({
    name: "Plaza Independencia & Palacio Salvo Civic Node",
    pointType: "Government / Civic Center",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.9059,
    longitude: -57.8529,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/montevideo/plaza-independencia",
    manuallyVerified: true,
  }),
  pt({
    name: "Mercado del Puerto",
    pointType: "Entertainment District",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.9056,
    longitude: -57.8456,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/montevideo/mercado-del-puerto",
    manuallyVerified: true,
  }),
  pt({
    name: "Rambla de Montevideo Waterfront Promenade",
    pointType: "Beach / Waterfront",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.9092,
    longitude: -56.1867,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/montevideo/rambla",
    manuallyVerified: true,
    hotelDemandNote: "Citywide coastal promenade linking leisure districts and supporting waterfront hotel demand.",
  }),
  pt({
    name: "Rambla Pocitos Beach District",
    pointType: "Beach / Waterfront",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.9103,
    longitude: -56.1545,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/montevideo/pocitos",
    manuallyVerified: true,
  }),
  pt({
    name: "World Trade Center Montevideo",
    pointType: "Business District",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.9016,
    longitude: -56.1358,
    sourceReference: "https://www.wtcuy.com/",
    hotelDemandNote: "Flagship corporate and services tower cluster driving weekday business hotel compression.",
  }),
  pt({
    name: "WTC Montevideo Free Zone Corridor",
    pointType: "Mixed-Use Development",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.8712,
    longitude: -56.0456,
    sourceReference: "https://www.wtcuy.com/zona-franca",
    manuallyVerified: true,
  }),
  pt({
    name: "Hospital Británico Montevideo",
    pointType: "Medical Campus",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.9024,
    longitude: -56.1389,
    sourceReference: "https://www.hbritanico.com.uy/",
  }),
  pt({
    name: "Hospital de Clínicas Universidad de la República",
    pointType: "Medical Campus",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.8967,
    longitude: -56.1523,
    sourceReference: "https://www.hclinicas.edu.uy/",
    googleSearchQuery: "Hospital de Clínicas Montevideo Uruguay",
  }),
  pt({
    name: "Universidad de la República Central Campus",
    pointType: "University / College",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.9189,
    longitude: -56.1678,
    sourceReference: "https://udelar.edu.uy/",
  }),
  pt({
    name: "Universidad ORT Uruguay Montevideo",
    pointType: "University / College",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.8856,
    longitude: -56.1703,
    sourceReference: "https://www.ort.edu.uy/",
  }),
  pt({
    name: "Universidad Católica del Uruguay Dámaso Antonio Larrañaga",
    pointType: "University / College",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.8912,
    longitude: -56.1012,
    sourceReference: "https://www.ucu.edu.uy/",
    googleSearchQuery: "Universidad Católica del Uruguay Montevideo",
  }),
  pt({
    name: "Centro LATU Convention Center",
    pointType: "Convention Center",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.8978,
    longitude: -56.1405,
    sourceReference: "https://www.latu.org.uy/",
    hotelDemandNote: "Primary Montevideo convention venue driving group and event-oriented hotel demand.",
  }),
  pt({
    name: "Palacio Legislativo",
    pointType: "Government / Civic Center",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.8912,
    longitude: -56.1867,
    sourceReference: "https://www.parlamento.gub.uy/",
    manuallyVerified: true,
  }),
  pt({
    name: "Port of Montevideo Maritime Gateway",
    pointType: "Industrial / Logistics Zone",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.9025,
    longitude: -57.8267,
    sourceReference: "https://www.anp.com.uy/",
    manuallyVerified: true,
    hotelDemandNote: "Primary maritime port supporting logistics, cruise, and project-based hotel demand.",
  }),
  pt({
    name: "Estadio Centenario",
    pointType: "Sports Venue",
    city: "Montevideo",
    submarket: "Montevideo",
    latitude: -34.8936,
    longitude: -56.1525,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/montevideo/estadio-centenario",
    googleSearchQuery: "Estadio Centenario Montevideo Uruguay",
  }),

  // Punta del Este (11)
  pt({
    name: "Centro de Convenciones Punta del Este",
    pointType: "Convention Center",
    city: "Punta del Este",
    submarket: "Punta del Este",
    latitude: -34.9289,
    longitude: -54.9289,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/punta-del-este/conventions",
    hotelDemandNote: "Primary Maldonado convention venue driving seasonal group and event hotel compression.",
  }),
  pt({
    name: "Playa Brava & La Mano",
    pointType: "Beach / Waterfront",
    city: "Punta del Este",
    submarket: "Punta del Este",
    latitude: -34.9534,
    longitude: -54.9367,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/punta-del-este/playa-brava",
    manuallyVerified: true,
    hotelDemandNote: "Signature Atlantic beachfront with iconic sculpture; core summer resort hotel demand node.",
  }),
  pt({
    name: "Playa Mansa Resort Coast",
    pointType: "Beach / Waterfront",
    city: "Punta del Este",
    submarket: "Punta del Este",
    latitude: -34.9645,
    longitude: -54.9523,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/punta-del-este/playa-mansa",
    manuallyVerified: true,
  }),
  pt({
    name: "Puerto Punta del Este Marina District",
    pointType: "Mixed-Use Development",
    city: "Punta del Este",
    submarket: "Punta del Este",
    latitude: -34.9612,
    longitude: -54.9412,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/punta-del-este/marina",
    manuallyVerified: true,
  }),
  pt({
    name: "Yacht Club Punta del Este",
    pointType: "Entertainment District",
    city: "Punta del Este",
    submarket: "Punta del Este",
    latitude: -34.9678,
    longitude: -54.9389,
    sourceReference: "https://www.ycpe.com.uy/",
    googleSearchQuery: "Yacht Club Punta del Este Uruguay",
  }),
  pt({
    name: "Gorlero Avenue Entertainment Strip",
    pointType: "Entertainment District",
    city: "Punta del Este",
    submarket: "Punta del Este",
    latitude: -34.9612,
    longitude: -54.9456,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/punta-del-este/gorlero",
    manuallyVerified: true,
  }),
  pt({
    name: "Punta del Este Hotel & Casino Strip",
    pointType: "Mixed-Use Development",
    city: "Punta del Este",
    submarket: "Punta del Este",
    latitude: -34.9567,
    longitude: -54.9334,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/punta-del-este/hotels",
    manuallyVerified: true,
  }),
  pt({
    name: "Isla Gorriti Tourism Gateway",
    pointType: "Tourist Attraction",
    city: "Punta del Este",
    submarket: "Punta del Este",
    latitude: -34.9756,
    longitude: -54.9512,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/punta-del-este/isla-gorriti",
    manuallyVerified: true,
  }),
  pt({
    name: "José Ignacio Beach Resort Coastline",
    pointType: "Beach / Waterfront",
    city: "José Ignacio",
    submarket: "Punta del Este",
    latitude: -34.8389,
    longitude: -54.6389,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/jose-ignacio",
    manuallyVerified: true,
    hotelDemandNote: "Premium coastal village with boutique resort demand and high-season compression.",
  }),
  pt({
    name: "Faro de José Ignacio",
    pointType: "Tourist Attraction",
    city: "José Ignacio",
    submarket: "Punta del Este",
    latitude: -34.8356,
    longitude: -54.6234,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/jose-ignacio/lighthouse",
    manuallyVerified: true,
  }),
  pt({
    name: "La Barra Bridge & Coastal Resort Corridor",
    pointType: "Mixed-Use Development",
    city: "La Barra",
    submarket: "Punta del Este",
    latitude: -34.9156,
    longitude: -54.8612,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/punta-del-este/la-barra",
    manuallyVerified: true,
  }),

  // Colonia (6)
  pt({
    name: "Colonia del Sacramento UNESCO Historic Quarter",
    pointType: "Tourist Attraction",
    city: "Colonia del Sacramento",
    submarket: "Colonia",
    latitude: -34.4712,
    longitude: -57.8434,
    sourceReference: "https://whc.unesco.org/en/list/745/",
    dataConfidence: "High",
    manuallyVerified: true,
    hotelDemandNote: "UNESCO heritage core driving international day-trip and overnight cultural tourism lodging.",
  }),
  pt({
    name: "Calle de los Suspiros",
    pointType: "Tourist Attraction",
    city: "Colonia del Sacramento",
    submarket: "Colonia",
    latitude: -34.4723,
    longitude: -57.8423,
    sourceReference: "https://www.visitcolonia.com/en/calle-de-los-suspiros",
    manuallyVerified: true,
  }),
  pt({
    name: "Basílica del Santísimo Sacramento",
    pointType: "Tourist Attraction",
    city: "Colonia del Sacramento",
    submarket: "Colonia",
    latitude: -34.4734,
    longitude: -57.8445,
    sourceReference: "https://www.visitcolonia.com/en/basilica",
    manuallyVerified: true,
  }),
  pt({
    name: "Colonia del Sacramento Barrio Histórico",
    pointType: "Entertainment District",
    city: "Colonia del Sacramento",
    submarket: "Colonia",
    latitude: -34.4708,
    longitude: -57.8448,
    sourceReference: "https://www.visitcolonia.com/en/historic-quarter",
    manuallyVerified: true,
  }),
  pt({
    name: "Colonia del Sacramento Ferry & River Gateway",
    pointType: "Future Growth Node",
    city: "Colonia del Sacramento",
    submarket: "Colonia",
    latitude: -34.4678,
    longitude: -57.8489,
    sourceReference: "https://www.visitcolonia.com/en/ferry-terminal",
    manuallyVerified: true,
    hotelDemandNote: "Buenos Aires ferry gateway supporting pre/post-crossing transit and weekend hotel demand.",
  }),
  pt({
    name: "Plaza de Toros Real de San Carlos",
    pointType: "Tourist Attraction",
    city: "Colonia del Sacramento",
    submarket: "Colonia",
    latitude: -34.4523,
    longitude: -57.8234,
    sourceReference: "https://www.visitcolonia.com/en/real-de-san-carlos",
    manuallyVerified: true,
  }),

  // Other (6)
  pt({
    name: "Piriápolis Cerro San Antonio Tourism Node",
    pointType: "Tourist Attraction",
    city: "Piriápolis",
    submarket: "Other",
    latitude: -34.8734,
    longitude: -55.2789,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/piriapolis/cerro-san-antonio",
    manuallyVerified: true,
  }),
  pt({
    name: "Piriápolis Beach Resort Coast",
    pointType: "Beach / Waterfront",
    city: "Piriápolis",
    submarket: "Other",
    latitude: -34.8689,
    longitude: -55.2756,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/piriapolis",
    manuallyVerified: true,
    hotelDemandNote: "Classic Atlantic resort town supporting regional leisure and second-home hotel demand.",
  }),
  pt({
    name: "Casapueblo Punta Ballena",
    pointType: "Tourist Attraction",
    city: "Punta Ballena",
    submarket: "Other",
    latitude: -34.8956,
    longitude: -55.0512,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/punta-ballena/casapueblo",
    manuallyVerified: true,
  }),
  pt({
    name: "Carmelo Wine Tourism District",
    pointType: "Entertainment District",
    city: "Carmelo",
    submarket: "Other",
    latitude: -34.0012,
    longitude: -58.2867,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/carmelo",
    manuallyVerified: true,
    hotelDemandNote: "River-wine tourism corridor with boutique lodge and weekend escape lodging demand.",
  }),
  pt({
    name: "Bodega Bouza Carmelo",
    pointType: "Tourist Attraction",
    city: "Carmelo",
    submarket: "Other",
    latitude: -33.9912,
    longitude: -58.2789,
    sourceReference: "https://www.bodegabouza.com/",
    googleSearchQuery: "Bodega Bouza Carmelo Uruguay",
  }),
  pt({
    name: "Atlántida Beach Resort Coast",
    pointType: "Beach / Waterfront",
    city: "Atlántida",
    submarket: "Other",
    latitude: -34.7789,
    longitude: -55.7589,
    sourceReference: "https://www.uruguaynatural.com/en/destinations/atlantida",
    manuallyVerified: true,
  }),
];

export function getUruguayCandidates() {
  return URUGUAY_CANDIDATES;
}

export { URUGUAY_SUBMARKETS };
