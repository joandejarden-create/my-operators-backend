/**
 * Dominican Republic Mature Pass Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Dominican Republic";
const MARKET = "Dominican Republic Mature Pass";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const DOMINICAN_REPUBLIC_MATURE_TI_DELTA_RECORDS = [
  ti({ name: "Autopista del Este — Santo Domingo East Access", pointType: "Highway Access", city: "Santo Domingo", submarket: "Santo Domingo Metro", latitude: 18.4712, longitude: -69.8612, sourceReference: "https://www.mop.gob.do/", pointSubtype: "National Highway", notes: "East capital highway connector to airport and Boca Chica corridor." }),
  ti({ name: "Carretera Mella — Santiago North Access", pointType: "Highway Access", city: "Santiago", submarket: "Santiago / Cibao", latitude: 19.4612, longitude: -70.6712, sourceReference: "https://www.mop.gob.do/", pointSubtype: "National Highway", notes: "Primary Cibao north-south highway node for regional business travel." }),
  ti({ name: "Carretera Samaná — Las Terrenas Coastal Access", pointType: "Highway Access", city: "Las Terrenas", submarket: "Samaná / Las Terrenas", latitude: 19.3212, longitude: -69.5312, sourceReference: "https://www.godominicanrepublic.com/", pointSubtype: "Coastal Highway", notes: "Northeast peninsula coastal road for Samaná resort demand." }),
  ti({ name: "Las Américas International Airport Highway Access", pointType: "Highway Access", city: "Santo Domingo", submarket: "Santo Domingo Metro", latitude: 18.4297, longitude: -69.6689, sourceReference: "https://www.aerodom.com/", pointSubtype: "Airport Access", notes: "SDQ airport highway connector for capital and south-coast air-lift demand." }),
  ti({ name: "La Romana Highway 104 Resort Access", pointType: "Highway Access", city: "La Romana", submarket: "La Romana / Bayahibe", latitude: 18.4212, longitude: -68.9012, sourceReference: "https://www.mop.gob.do/", pointSubtype: "Resort Corridor", notes: "East-south resort highway access to Casa de Campo and Bayahibe corridor." }),
  ti({ name: "Puerto Plata — Sosúa Coastal Road Access", pointType: "Highway Access", city: "Sosúa", submarket: "Puerto Plata / Sosúa / Cabarete", latitude: 19.7512, longitude: -70.5189, sourceReference: "https://www.godominicanrepublic.com/", pointSubtype: "Coastal Highway", notes: "North coast scenic road linking Puerto Plata to Cabarete kite beach demand." }),
  ti({ name: "Miches Coastal Highway — Costa Esmeralda Access", pointType: "Highway Access", city: "Miches", submarket: "Miches / Costa Esmeralda", latitude: 18.9812, longitude: -69.0512, sourceReference: "https://www.godominicanrepublic.com/", pointSubtype: "Emerging Resort Corridor", notes: "New east-coast highway node for Miches/Costa Esmeralda resort growth." }),
  ti({ name: "Jarabacoa Mountain Highway Access", pointType: "Highway Access", city: "Jarabacoa", submarket: "Jarabacoa / Constanza", latitude: 19.1212, longitude: -70.6412, sourceReference: "https://www.godominicanrepublic.com/", pointSubtype: "Mountain Corridor", notes: "Central highland road access for adventure and eco-lodge hotel demand." }),
  ti({ name: "Barahona — Pedernales Southwest Highway Access", pointType: "Highway Access", city: "Barahona", submarket: "Barahona / Pedernales", latitude: 18.2089, longitude: -71.1012, sourceReference: "https://www.godominicanrepublic.com/", pointSubtype: "Coastal Highway", notes: "Southwest coastal highway for eco-tourism and Pedernales border demand." }),
  ti({ name: "Higüey — Punta Cana Inland Connector Access", pointType: "Highway Access", city: "Higüey", submarket: "Punta Cana / Bávaro / Cap Cana", latitude: 18.615, longitude: -68.7089, sourceReference: "https://www.mop.gob.do/", pointSubtype: "Resort Highway Corridor", notes: "Inland connector between Higüey and east-coast resort submarkets." }),
];

export function buildDominicanRepublicMatureTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, DOMINICAN_REPUBLIC_MATURE_TI_DELTA_RECORDS);
}
