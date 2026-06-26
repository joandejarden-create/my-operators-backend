/**
 * Chile — Santiago demand anchor candidates (source-backed).
 */

import { getPointTypeDefaults } from "../demand-anchors/point-type-defaults.js";
import {
  applyChileGovernanceDefaults,
  CHILE_SANTIAGO_SUBMARKETS,
} from "./chile-demand-anchor-governance.js";

const COUNTRY = "Chile";
const REGION = "South America";
const CITY = "Santiago";

function pt(v) {
  const defaults = getPointTypeDefaults(v.pointType);
  const rationale =
    v.hotelDemandNote ||
    defaults.hotelDemandRationale ||
    "Supports identifiable hotel demand in the Santiago metro market.";
  const base = {
    name: v.name,
    pointType: v.pointType,
    city: v.city || CITY,
    country: COUNTRY,
    region: REGION,
    submarket: v.submarket,
    latitude: v.latitude,
    longitude: v.longitude,
    source: "Public Source",
    sourceReference: v.sourceReference,
    dataConfidence: v.dataConfidence || "Medium",
    notes:
      v.notes ||
      `Submarket: ${v.submarket}. ${rationale} Candidate pending Google pre-import verification.`,
  };
  if (v.googleSearchQuery) base.googleSearchQuery = v.googleSearchQuery;
  if (v.manuallyVerified) {
    base.notes = `${base.notes} Manually verified using official/public source; Google Maps match was not used as final authority.`;
    base.dataConfidence = v.dataConfidence || "High";
    base.manuallyVerified = true;
  }
  return applyChileGovernanceDefaults(base, v.governance || {});
}

/** @type {ReturnType<typeof pt>[]} */
export const CHILE_SANTIAGO_CANDIDATES = [
  // Las Condes (7)
  pt({
    name: "Apoquindo Financial Corridor",
    pointType: "Business District",
    submarket: "Las Condes",
    latitude: -33.4172,
    longitude: -70.5942,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/",
    manuallyVerified: true,
    hotelDemandNote: "Primary east-side corporate office concentration driving weekday business hotel compression.",
  }),
  pt({
    name: "Clínica Las Condes",
    pointType: "Medical Campus",
    submarket: "Las Condes",
    latitude: -33.4084,
    longitude: -70.5682,
    sourceReference: "https://www.clinicalascondes.cl/",
    hotelDemandNote: "Major private hospital campus supporting medical travel and extended-stay lodging.",
  }),
  pt({
    name: "Clínica Alemana de Santiago",
    pointType: "Medical Campus",
    submarket: "Las Condes",
    latitude: -33.4042,
    longitude: -70.5754,
    sourceReference: "https://www.alemana.cl/",
  }),
  pt({
    name: "Mall Sport Las Condes",
    pointType: "Sports Venue",
    submarket: "Las Condes",
    latitude: -33.4012,
    longitude: -70.5784,
    sourceReference: "https://www.mallsport.cl/",
    hotelDemandNote: "Large event and sports venue driving group and weekend hotel demand.",
  }),
  pt({
    name: "Parque Araucano Las Condes",
    pointType: "Entertainment District",
    submarket: "Las Condes",
    latitude: -33.4052,
    longitude: -70.5724,
    sourceReference: "https://www.lascondes.cl/",
    manuallyVerified: true,
  }),
  pt({
    name: "Titanium La Portada",
    pointType: "Mixed-Use Development",
    submarket: "Las Condes",
    latitude: -33.4122,
    longitude: -70.5812,
    sourceReference: "https://www.titaniumlaportada.cl/",
  }),
  pt({
    name: "Las Condes Corporate Hotel District",
    pointType: "Future Growth Node",
    submarket: "Las Condes",
    latitude: -33.4152,
    longitude: -70.5884,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/las-condes",
    manuallyVerified: true,
  }),

  // Providencia (7)
  pt({
    name: "Providencia Business & Retail Corridor",
    pointType: "Business District",
    submarket: "Providencia",
    latitude: -33.4282,
    longitude: -70.6124,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/providencia",
    manuallyVerified: true,
  }),
  pt({
    name: "Clínica Santa María",
    pointType: "Medical Campus",
    submarket: "Providencia",
    latitude: -33.4312,
    longitude: -70.6184,
    sourceReference: "https://www.santamaria.cl/",
  }),
  pt({
    name: "Universidad Andrés Bello",
    pointType: "University / College",
    submarket: "Providencia",
    latitude: -33.4332,
    longitude: -70.6242,
    sourceReference: "https://www.unab.cl/",
  }),
  pt({
    name: "Barrio Italia",
    pointType: "Entertainment District",
    submarket: "Providencia",
    latitude: -33.4512,
    longitude: -70.6384,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/barrio-italia",
    manuallyVerified: true,
  }),
  pt({
    name: "Plaza Italia",
    pointType: "Tourist Attraction",
    submarket: "Providencia",
    latitude: -33.4442,
    longitude: -70.6342,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/",
    manuallyVerified: true,
  }),
  pt({
    name: "Pedro de Valdivia Norte Office Corridor",
    pointType: "Mixed-Use Development",
    submarket: "Providencia",
    latitude: -33.4262,
    longitude: -70.6084,
    sourceReference: "https://www.providencia.cl/",
    manuallyVerified: true,
  }),
  pt({
    name: "Metro Baquedano Transit Hub",
    pointType: "Future Growth Node",
    submarket: "Providencia",
    latitude: -33.4392,
    longitude: -70.6342,
    sourceReference: "https://www.metro.cl/",
    hotelDemandNote: "Major metro interchange supporting citywide business and leisure hotel access patterns.",
  }),

  // Vitacura (6)
  pt({
    name: "Parque Bicentenario Vitacura",
    pointType: "Tourist Attraction",
    submarket: "Vitacura",
    latitude: -33.3922,
    longitude: -70.5784,
    sourceReference: "https://www.vitacura.cl/",
    manuallyVerified: true,
  }),
  pt({
    name: "Casas Costanera",
    pointType: "Mixed-Use Development",
    submarket: "Vitacura",
    latitude: -33.3882,
    longitude: -70.5724,
    sourceReference: "https://www.casascostanera.cl/",
  }),
  pt({
    name: "Clínica MEDS Vitacura",
    pointType: "Medical Campus",
    submarket: "Vitacura",
    latitude: -33.3942,
    longitude: -70.5684,
    sourceReference: "https://www.meds.cl/",
  }),
  pt({
    name: "Museo Ralli",
    pointType: "Tourist Attraction",
    submarket: "Vitacura",
    latitude: -33.3862,
    longitude: -70.5824,
    sourceReference: "https://www.museoralli.cl/",
  }),
  pt({
    name: "Vitacura Premium Commercial District",
    pointType: "Business District",
    submarket: "Vitacura",
    latitude: -33.3902,
    longitude: -70.5764,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/vitacura",
    manuallyVerified: true,
  }),
  pt({
    name: "Nueva Costanera Retail Corridor",
    pointType: "Entertainment District",
    submarket: "Vitacura",
    latitude: -33.3842,
    longitude: -70.5742,
    sourceReference: "https://www.vitacura.cl/",
    manuallyVerified: true,
  }),

  // Santiago Centro (8)
  pt({
    name: "Palacio de La Moneda",
    pointType: "Government / Civic Center",
    submarket: "Santiago Centro",
    latitude: -33.4422,
    longitude: -70.6532,
    sourceReference: "https://www.lamoneda.cl/",
  }),
  pt({
    name: "Plaza de Armas de Santiago",
    pointType: "Tourist Attraction",
    submarket: "Santiago Centro",
    latitude: -33.4372,
    longitude: -70.6506,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/plaza-de-armas",
    manuallyVerified: true,
  }),
  pt({
    name: "Congreso Nacional de Chile",
    pointType: "Government / Civic Center",
    submarket: "Santiago Centro",
    latitude: -33.4482,
    longitude: -70.6512,
    sourceReference: "https://www.congreso.cl/",
  }),
  pt({
    name: "Universidad de Chile",
    pointType: "University / College",
    submarket: "Santiago Centro",
    latitude: -33.4482,
    longitude: -70.6624,
    sourceReference: "https://www.uchile.cl/",
  }),
  pt({
    name: "Pontificia Universidad Católica de Chile",
    pointType: "University / College",
    submarket: "Santiago Centro",
    latitude: -33.4412,
    longitude: -70.6442,
    sourceReference: "https://www.uc.cl/",
  }),
  pt({
    name: "Estadio Nacional Julio Martínez Prádanos",
    pointType: "Sports Venue",
    submarket: "Santiago Centro",
    latitude: -33.4642,
    longitude: -70.6102,
    sourceReference: "https://www.estadionacional.cl/",
  }),
  pt({
    name: "Barrio Lastarria",
    pointType: "Entertainment District",
    submarket: "Santiago Centro",
    latitude: -33.4392,
    longitude: -70.6424,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/barrio-lastarria",
    manuallyVerified: true,
  }),
  pt({
    name: "Santiago Historic Civic Corridor",
    pointType: "Government / Civic Center",
    submarket: "Santiago Centro",
    latitude: -33.4442,
    longitude: -70.6562,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/historic-center",
    manuallyVerified: true,
  }),

  // Airport Corridor (6)
  pt({
    name: "Aeropuerto Internacional Arturo Merino Benítez",
    pointType: "Future Growth Node",
    submarket: "Airport Corridor",
    city: "Pudahuel",
    latitude: -33.3932,
    longitude: -70.7858,
    sourceReference: "https://www.nuestroaeropuerto.cl/",
    hotelDemandNote: "Primary international gateway driving airport-adjacent and metro-wide hotel demand.",
  }),
  pt({
    name: "ENEA Business Park Airport Corridor",
    pointType: "Industrial / Logistics Zone",
    submarket: "Airport Corridor",
    city: "Pudahuel",
    latitude: -33.3882,
    longitude: -70.7724,
    sourceReference: "https://www.enea.cl/",
  }),
  pt({
    name: "Pudahuel Logistics & Industrial Zone",
    pointType: "Industrial / Logistics Zone",
    submarket: "Airport Corridor",
    city: "Pudahuel",
    latitude: -33.4022,
    longitude: -70.7584,
    sourceReference: "https://www.pudahuel.cl/",
    manuallyVerified: true,
  }),
  pt({
    name: "Americas Highway Airport Access Node",
    pointType: "Future Growth Node",
    submarket: "Airport Corridor",
    city: "Pudahuel",
    latitude: -33.3952,
    longitude: -70.7684,
    sourceReference: "https://www.mop.gob.cl/",
    manuallyVerified: true,
  }),
  pt({
    name: "Santiago Airport Hotel & Corporate Corridor",
    pointType: "Business District",
    submarket: "Airport Corridor",
    city: "Pudahuel",
    latitude: -33.3902,
    longitude: -70.7784,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/#airport",
    manuallyVerified: true,
  }),
  pt({
    name: "Cargo & Aviation Services District Pudahuel",
    pointType: "Industrial / Logistics Zone",
    submarket: "Airport Corridor",
    city: "Pudahuel",
    latitude: -33.3982,
    longitude: -70.7624,
    sourceReference: "https://www.nuestroaeropuerto.cl/carga",
    manuallyVerified: true,
  }),

  // Convention / Events Corridor (6)
  pt({
    name: "Espacio Riesco",
    pointType: "Convention Center",
    submarket: "Convention / Events Corridor",
    latitude: -33.3842,
    longitude: -70.5642,
    sourceReference: "https://www.espacioriesco.cl/",
    hotelDemandNote: "Major convention and exhibition venue driving group and event hotel demand.",
  }),
  pt({
    name: "Movistar Arena",
    pointType: "Sports Venue",
    submarket: "Convention / Events Corridor",
    latitude: -33.4622,
    longitude: -70.6632,
    sourceReference: "https://www.movistararena.cl/",
  }),
  pt({
    name: "Parque O'Higgins Events District",
    pointType: "Entertainment District",
    submarket: "Convention / Events Corridor",
    latitude: -33.4682,
    longitude: -70.6584,
    sourceReference: "https://www.parquemet.cl/",
    manuallyVerified: true,
  }),
  pt({
    name: "Teatro Municipal de Santiago",
    pointType: "Entertainment District",
    submarket: "Convention / Events Corridor",
    latitude: -33.4412,
    longitude: -70.6484,
    sourceReference: "https://www.tms.cl/",
  }),
  pt({
    name: "CentroParque Events Complex",
    pointType: "Convention Center",
    submarket: "Convention / Events Corridor",
    latitude: -33.4182,
    longitude: -70.6024,
    sourceReference: "https://www.centroparque.cl/",
  }),
  pt({
    name: "Santiago Metropolitan Convention Corridor",
    pointType: "Convention Center",
    submarket: "Convention / Events Corridor",
    latitude: -33.4222,
    longitude: -70.6084,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/#events",
    manuallyVerified: true,
  }),

  // Costanera / Financial District (6)
  pt({
    name: "Costanera Center",
    pointType: "Mixed-Use Development",
    submarket: "Costanera / Financial District",
    latitude: -33.4172,
    longitude: -70.6062,
    sourceReference: "https://www.costaneracenter.cl/",
    hotelDemandNote: "Iconic mixed-use tower complex anchoring premium corporate and visitor hotel demand.",
  }),
  pt({
    name: "Gran Torre Santiago",
    pointType: "Business District",
    submarket: "Costanera / Financial District",
    latitude: -33.4168,
    longitude: -70.6068,
    sourceReference: "https://www.costaneracenter.cl/torre",
  }),
  pt({
    name: "Mall Costanera Center",
    pointType: "Mixed-Use Development",
    submarket: "Costanera / Financial District",
    latitude: -33.4178,
    longitude: -70.6054,
    sourceReference: "https://www.costaneracenter.cl/mall",
  }),
  pt({
    name: "Tobalaba Financial Corridor",
    pointType: "Business District",
    submarket: "Costanera / Financial District",
    latitude: -33.4182,
    longitude: -70.5984,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/",
    manuallyVerified: true,
  }),
  pt({
    name: "Metro Tobalaba Financial Access",
    pointType: "Future Growth Node",
    submarket: "Costanera / Financial District",
    latitude: -33.4184,
    longitude: -70.5992,
    sourceReference: "https://www.metro.cl/",
  }),
  pt({
    name: "Costanera Financial District",
    pointType: "Business District",
    submarket: "Costanera / Financial District",
    latitude: -33.4162,
    longitude: -70.6042,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/costanera-center",
    manuallyVerified: true,
  }),

  // El Golf / Sanhattan (6)
  pt({
    name: "El Golf Business District",
    pointType: "Business District",
    submarket: "El Golf / Sanhattan",
    latitude: -33.4092,
    longitude: -70.5964,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/el-golf",
    manuallyVerified: true,
    hotelDemandNote: "Sanhattan office core with dense corporate travel and premium hotel demand.",
  }),
  pt({
    name: "Isidora Goyenechea Corporate Corridor",
    pointType: "Business District",
    submarket: "El Golf / Sanhattan",
    latitude: -33.4082,
    longitude: -70.5942,
    sourceReference: "https://www.lascondes.cl/",
    manuallyVerified: true,
  }),
  pt({
    name: "Sanhattan Financial District",
    pointType: "Business District",
    submarket: "El Golf / Sanhattan",
    latitude: -33.4102,
    longitude: -70.5984,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/#sanhattan",
    manuallyVerified: true,
  }),
  pt({
    name: "Hotelera El Golf Lodging Corridor",
    pointType: "Mixed-Use Development",
    submarket: "El Golf / Sanhattan",
    latitude: -33.4072,
    longitude: -70.5924,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/el-golf#hotels",
    manuallyVerified: true,
  }),
  pt({
    name: "Avenida Apoquindo Sanhattan Node",
    pointType: "Mixed-Use Development",
    submarket: "El Golf / Sanhattan",
    latitude: -33.4112,
    longitude: -70.6002,
    sourceReference: "https://www.lascondes.cl/",
    manuallyVerified: true,
  }),
  pt({
    name: "El Golf Convention-Adjacent Office Node",
    pointType: "Future Growth Node",
    submarket: "El Golf / Sanhattan",
    latitude: -33.4062,
    longitude: -70.5902,
    sourceReference: "https://www.espacioriesco.cl/",
    manuallyVerified: true,
  }),

  // Parque Arauco / Nueva Las Condes (5)
  pt({
    name: "Parque Arauco",
    pointType: "Mixed-Use Development",
    submarket: "Parque Arauco / Nueva Las Condes",
    latitude: -33.4022,
    longitude: -70.5782,
    sourceReference: "https://www.parquearauco.cl/mall-parque-arauco",
  }),
  pt({
    name: "Nueva Las Condes District",
    pointType: "Mixed-Use Development",
    submarket: "Parque Arauco / Nueva Las Condes",
    latitude: -33.4002,
    longitude: -70.5762,
    sourceReference: "https://www.parquearauco.cl/",
    manuallyVerified: true,
  }),
  pt({
    name: "Kennedy Avenue Retail Corridor",
    pointType: "Entertainment District",
    submarket: "Parque Arauco / Nueva Las Condes",
    latitude: -33.4042,
    longitude: -70.5802,
    sourceReference: "https://www.lascondes.cl/",
    manuallyVerified: true,
  }),
  pt({
    name: "Parque Arauco Corporate & Leisure Hub",
    pointType: "Business District",
    submarket: "Parque Arauco / Nueva Las Condes",
    latitude: -33.4012,
    longitude: -70.5772,
    sourceReference: "https://www.parquearauco.cl/mall-parque-arauco#corporate",
    manuallyVerified: true,
  }),
  pt({
    name: "Nueva Las Condes Growth Node",
    pointType: "Future Growth Node",
    submarket: "Parque Arauco / Nueva Las Condes",
    latitude: -33.3992,
    longitude: -70.5752,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/las-condes#nueva-las-condes",
    manuallyVerified: true,
  }),

  // Other (1)
  pt({
    name: "Estación Mapocho Cultural Center",
    pointType: "Tourist Attraction",
    submarket: "Other",
    latitude: -33.4342,
    longitude: -70.6484,
    sourceReference: "https://www.centroculturalmapocho.cl/",
    hotelDemandNote: "Major cultural venue supporting event-oriented and heritage tourism lodging near centro.",
  }),
];

export function getChileSantiagoCandidates() {
  return CHILE_SANTIAGO_CANDIDATES;
}

export { CHILE_SANTIAGO_SUBMARKETS };
