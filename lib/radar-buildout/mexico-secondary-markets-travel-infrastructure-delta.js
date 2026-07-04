/**
 * Mexico Secondary Markets Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Mexico";
const MARKET = "Mexico Secondary Markets";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

/** @type {ReturnType<typeof ti>[]} */
export const MEXICO_SECONDARY_MARKETS_TI_DELTA_RECORDS = [
  // Oaxaca (3)
  ti({
    name: "Xoxocotlán International Airport (OAX) Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Access",
    city: "Oaxaca",
    submarket: "Oaxaca",
    latitude: 16.9999,
    longitude: -96.7264,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/oaxaca",
    notes: "Primary air gateway for Oaxaca heritage, convention, and valley tourism hotel demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
  ti({
    name: "Central de Autobuses Oaxaca Access",
    pointType: "Bus Terminal",
    pointSubtype: "Intercity Terminal",
    city: "Oaxaca",
    submarket: "Oaxaca",
    latitude: 17.0689,
    longitude: -96.7312,
    sourceReference: "https://www.oaxaca.travel/en/",
    notes: "Intercity bus terminal supporting centro and valley transit-oriented hotel demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
  ti({
    name: "Carretera Internacional Oaxaca-Monte Albán Access",
    pointType: "Highway Access",
    pointSubtype: "Tourism Corridor",
    city: "Oaxaca",
    submarket: "Oaxaca",
    latitude: 17.0512,
    longitude: -96.7512,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/oaxaca",
    notes: "Highway connector from Oaxaca centro to Monte Albán and eastern valley attractions.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),

  // Querétaro (3)
  ti({
    name: "Querétaro Intercontinental Airport (QRO) Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Access",
    city: "Colón",
    submarket: "Querétaro",
    latitude: 20.6173,
    longitude: -100.1858,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/queretaro",
    notes: "Bajío air gateway for Querétaro corporate, convention, and industrial corridor demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
  ti({
    name: "Terminal de Autobuses Querétaro Access",
    pointType: "Bus Terminal",
    pointSubtype: "Intercity Terminal",
    city: "Santiago de Querétaro",
    submarket: "Querétaro",
    latitude: 20.5812,
    longitude: -100.4012,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/queretaro",
    notes: "Central intercity bus terminal for centro and north-side transit hotel demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
  ti({
    name: "Peña de Bernal Tourism Highway Access",
    pointType: "Highway Access",
    pointSubtype: "Tourism Corridor",
    city: "Bernal",
    submarket: "Querétaro",
    latitude: 20.5012,
    longitude: -99.8189,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/queretaro/bernal",
    notes: "Highway access to Bernal Pueblo Mágico for regional leisure and weekend hotel demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),

  // Guanajuato (3)
  ti({
    name: "Del Bajío International Airport (BJX) Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Access",
    city: "Silao",
    submarket: "Guanajuato",
    latitude: 20.9935,
    longitude: -101.4808,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/guanajuato",
    notes: "Shared Bajío air gateway for León industrial, Guanajuato heritage, and convention demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
  ti({
    name: "Central de Autobuses León Access",
    pointType: "Bus Terminal",
    pointSubtype: "Intercity Terminal",
    city: "León",
    submarket: "Guanajuato",
    latitude: 21.1212,
    longitude: -101.6712,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/guanajuato",
    notes: "Primary intercity bus hub linking León, Silao, and Guanajuato city hotel submarkets.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
  ti({
    name: "Downtown Guanajuato Tunnel Highway Access",
    pointType: "Highway Access",
    pointSubtype: "Historic Core",
    city: "Guanajuato",
    submarket: "Guanajuato",
    latitude: 21.0178,
    longitude: -101.2567,
    sourceReference: "https://www.guanajuato.travel/",
    notes: "Tunnel and ring-road access for UNESCO centro heritage tourism hotel demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),

  // Mazatlán (3)
  ti({
    name: "Mazatlán International Airport (MZT) Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Access",
    city: "Mazatlán",
    submarket: "Mazatlán",
    latitude: 23.1614,
    longitude: -106.2661,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/sinaloa/mazatlan",
    notes: "Pacific coast air gateway for Mazatlán resort, cruise, and convention hotel demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Central de Autobuses Mazatlán Access",
    pointType: "Bus Terminal",
    pointSubtype: "Intercity Terminal",
    city: "Mazatlán",
    submarket: "Mazatlán",
    latitude: 23.2112,
    longitude: -106.4112,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/sinaloa/mazatlan",
    notes: "Intercity bus terminal supporting centro and Zona Dorada transit hotel demand.",
    scopeLevel: "Market",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
  ti({
    name: "Mazatlán Cruise and Cargo Port Access",
    pointType: "Port / Maritime",
    pointSubtype: "Cruise Terminal",
    city: "Mazatlán",
    submarket: "Mazatlán",
    latitude: 23.1912,
    longitude: -106.4189,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/sinaloa/mazatlan",
    notes: "Pacific port and cruise terminal access for maritime pre/post-stay and logistics demand.",
    scopeLevel: "Market",
    useCaseTags: ["Cruise / Port", "Resort / Leisure"],
  }),
];

export function buildMexicoSecondaryMarketsTiDeltaFixture() {
  const fixture = buildIslandTiDeltaFixture(COUNTRY, MARKET, MEXICO_SECONDARY_MARKETS_TI_DELTA_RECORDS);
  for (const p of fixture.points) {
    p.scopeLevel = "Market";
    p.projectRelevanceLogic = `Mexico Secondary Markets build — ${p.submarket} TI node: ${p.name}.`;
  }
  return fixture;
}
