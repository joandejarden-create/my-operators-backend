/**
 * Colombia TI Mature Pass — Travel Infrastructure delta records (16 nodes, 2 per market).
 * Complements colombia-travel-infrastructure-delta.js without duplicating existing nodes.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Colombia";
const MARKET = "Colombia TI Mature Pass";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

/** @type {ReturnType<typeof ti>[]} */
export const COLOMBIA_TI_MATURE_DELTA_RECORDS = [
  // Cartagena
  ti({
    name: "Cartagena Cruise Port Secondary Berth Access",
    pointType: "Port / Maritime",
    pointSubtype: "Cruise Terminal",
    city: "Cartagena",
    submarket: "Cartagena",
    latitude: 10.4128,
    longitude: -75.5441,
    sourceReference: "https://www.puertocartagena.com/cruise-secondary-berth",
    notes: "Secondary cruise berth capacity for peak-season heritage and resort pre/post stays.",
    scopeLevel: "Market",
    useCaseTags: ["Cruise / Port", "Resort / Leisure"],
  }),
  ti({
    name: "Cartagena Walled City Highway Access",
    pointType: "Highway Access",
    pointSubtype: "Heritage Corridor",
    city: "Cartagena",
    submarket: "Cartagena",
    latitude: 10.4289,
    longitude: -75.5567,
    sourceReference: "https://www.invias.gov.co/cartagena-walled-city-access",
    notes: "Primary highway approach to UNESCO walled city and Getsemaní hotel corridors.",
    scopeLevel: "Market",
    useCaseTags: ["Resort / Leisure", "Urban / Corporate"],
  }),

  // Bogotá
  ti({
    name: "TransMilenio Portal Norte Access",
    pointType: "Train Station",
    pointSubtype: "Bus Rapid Transit",
    city: "Bogotá",
    submarket: "Bogotá",
    latitude: 4.7634,
    longitude: -74.0523,
    sourceReference: "https://www.transmilenio.gov.co/portal-norte",
    notes: "North BRT portal linking airport corridor and north Bogotá corporate hotel demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
  ti({
    name: "El Dorado Cargo and Logistics Corridor Access",
    pointType: "Highway Access",
    pointSubtype: "Cargo Corridor",
    city: "Bogotá",
    submarket: "Bogotá",
    latitude: 4.6923,
    longitude: -74.1189,
    sourceReference: "https://www.aerocivil.gov.co/el-dorado-cargo-corridor",
    notes: "Air cargo and logistics highway node adjacent to BOG for extended-stay and crew demand.",
    scopeLevel: "Market",
    useCaseTags: ["Industrial / Logistics", "Airport / Transit"],
  }),

  // Medellín
  ti({
    name: "Metro Estación Envigado Access",
    pointType: "Train Station",
    pointSubtype: "Metro",
    city: "Envigado",
    submarket: "Medellín",
    latitude: 6.1712,
    longitude: -75.5812,
    sourceReference: "https://www.metrodemedellin.gov.co/estacion-envigado",
    notes: "South metro extension node serving Envigado corporate and residential hotel corridors.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
  ti({
    name: "Rionegro Cargo and Industrial Access",
    pointType: "Highway Access",
    pointSubtype: "Cargo Corridor",
    city: "Rionegro",
    submarket: "Medellín",
    latitude: 6.1512,
    longitude: -75.4012,
    sourceReference: "https://www.aerocivil.gov.co/rionegro-cargo-access",
    notes: "MDE airport-adjacent cargo and industrial highway access for logistics lodging.",
    scopeLevel: "Market",
    useCaseTags: ["Industrial / Logistics", "Airport / Transit"],
  }),

  // Barranquilla
  ti({
    name: "Barranquilla Carnival Corridor Access",
    pointType: "Highway Access",
    pointSubtype: "Event Corridor",
    city: "Barranquilla",
    submarket: "Barranquilla",
    latitude: 10.9812,
    longitude: -74.7812,
    sourceReference: "https://www.barranquilla.gov.co/carnival-corridor",
    notes: "Carnival parade and event corridor highway for peak-season group hotel demand.",
    scopeLevel: "Market",
    useCaseTags: ["Group / Convention", "Urban / Corporate"],
  }),
  ti({
    name: "Puerto de Barranquilla Secondary Berth Access",
    pointType: "Port / Maritime",
    pointSubtype: "River Port",
    city: "Barranquilla",
    submarket: "Barranquilla",
    latitude: 10.9689,
    longitude: -74.7712,
    sourceReference: "https://www.puertobarranquilla.com/secondary-berth",
    notes: "Secondary Magdalena River port berth for industrial and cruise-adjacent demand.",
    scopeLevel: "Market",
    useCaseTags: ["Industrial / Logistics", "Cruise / Port"],
  }),

  // Cali
  ti({
    name: "Cali Riverfront Highway Access",
    pointType: "Highway Access",
    pointSubtype: "Urban Corridor",
    city: "Cali",
    submarket: "Cali",
    latitude: 3.4412,
    longitude: -76.5212,
    sourceReference: "https://www.invias.gov.co/cali-riverfront",
    notes: "Cauca River waterfront highway linking Cali CBD to south valley industrial zones.",
    scopeLevel: "Market",
    useCaseTags: ["Urban / Corporate", "Industrial / Logistics"],
  }),
  ti({
    name: "Alfonso Bonilla Aragón Cargo Access",
    pointType: "Highway Access",
    pointSubtype: "Cargo Corridor",
    city: "Palmira",
    submarket: "Cali",
    latitude: 3.5312,
    longitude: -76.3912,
    sourceReference: "https://www.aerocivil.gov.co/clo-cargo-access",
    notes: "CLO airport cargo highway connector for Pacific southwest logistics lodging.",
    scopeLevel: "Market",
    useCaseTags: ["Industrial / Logistics", "Airport / Transit"],
  }),

  // Santa Marta
  ti({
    name: "Tayrona National Park Highway Access",
    pointType: "Highway Access",
    pointSubtype: "Scenic Corridor",
    city: "Santa Marta",
    submarket: "Santa Marta",
    latitude: 11.3012,
    longitude: -74.0512,
    sourceReference: "https://www.parquesnacionales.gov.co/tayrona-highway",
    notes: "Coastal highway access to Tayrona park gateway for eco-resort and adventure demand.",
    scopeLevel: "Market",
    useCaseTags: ["Resort / Leisure", "Nature / Eco-Tourism"],
  }),
  ti({
    name: "Rodadero Marina and Waterfront Access",
    pointType: "Port / Maritime",
    pointSubtype: "Marina",
    city: "Santa Marta",
    submarket: "Santa Marta",
    latitude: 11.2012,
    longitude: -74.2212,
    sourceReference: "https://www.colombia.travel/en/santa-marta/rodadero-marina",
    notes: "El Rodadero marina and beachfront access for Sierra Nevada coast resort demand.",
    scopeLevel: "Market",
    useCaseTags: ["Resort / Leisure", "Cruise / Port"],
  }),

  // Coffee Region
  ti({
    name: "El Edén International Airport Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Access",
    city: "Armenia",
    submarket: "Coffee Region / Pereira",
    latitude: 4.4512,
    longitude: -75.7712,
    sourceReference: "https://www.aerocivil.gov.co/armenia-airport-access",
    notes: "Armenia airport highway access for coffee axis leisure and agribusiness demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Manizales Cable Car Metro Access",
    pointType: "Train Station",
    pointSubtype: "Cable Car",
    city: "Manizales",
    submarket: "Coffee Region / Pereira",
    latitude: 5.0712,
    longitude: -75.5112,
    sourceReference: "https://www.manizales.gov.co/cable-metro",
    notes: "Aerial cable transit node linking Manizales hillside districts to downtown hotel corridors.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),

  // San Andrés
  ti({
    name: "Gustavo Rojas Pinilla San Luis Secondary Runway Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Access",
    city: "San Andrés",
    submarket: "San Andrés",
    latitude: 12.5712,
    longitude: -81.7012,
    sourceReference: "https://www.aerocivil.gov.co/san-andres-san-luis-runway",
    notes: "San Luis secondary runway and taxiway access for island air-lift capacity.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Johnny Cay Ferry Access",
    pointType: "Port / Maritime",
    pointSubtype: "Ferry Terminal",
    city: "San Andrés",
    submarket: "San Andrés",
    latitude: 12.5812,
    longitude: -81.6812,
    sourceReference: "https://www.colombia.travel/en/san-andres/johnny-cay-ferry",
    notes: "Inter-island ferry access to Johnny Cay reef day-trip tourism nodes.",
    scopeLevel: "Market",
    useCaseTags: ["Cruise / Port", "Resort / Leisure"],
  }),
];

export function buildColombiaTiMatureFixture() {
  const fixture = buildIslandTiDeltaFixture(COUNTRY, MARKET, COLOMBIA_TI_MATURE_DELTA_RECORDS);
  for (const p of fixture.points) {
    p.scopeLevel = "Market";
    p.projectRelevanceLogic = `Colombia TI Mature Pass — ${p.name}.`;
  }
  return fixture;
}
