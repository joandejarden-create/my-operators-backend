/**
 * El Salvador Countrywide Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "El Salvador";
const MARKET = "El Salvador Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const EL_SALVADOR_TI_DELTA_RECORDS = [
  ti({ name: "Monseñor Óscar Arnulfo Romero International Airport (SAL) Access", pointType: "Highway Access", city: "San Luis Talpa", submarket: "San Salvador", latitude: 13.4409, longitude: -89.0557, sourceReference: "https://www.aies.com.sv/", pointSubtype: "Airport Access", notes: "Primary international gateway for San Salvador metro corporate and transit hotel demand." }),
  ti({ name: "Port of Acajutla Access", pointType: "Port / Maritime", city: "Acajutla", submarket: "Other", latitude: 13.5922, longitude: -89.8278, sourceReference: "https://www.cepa.gob.sv/", pointSubtype: "Commercial Port", notes: "Principal Pacific seaport supporting logistics crew and industrial extended-stay demand.", useCaseTags: ["Industrial / Logistics", "Cruise / Port"] }),
  ti({ name: "Pan-American Highway San Salvador Corridor Access", pointType: "Highway Access", city: "San Salvador", submarket: "San Salvador", latitude: 13.6929, longitude: -89.2182, sourceReference: "https://www.mopt.gob.sv/", pointSubtype: "National Highway", notes: "CA-1 Pan-American node through capital metro linking domestic business and leisure corridors." }),
  ti({ name: "La Libertad Pacific Coast Road Access", pointType: "Highway Access", city: "La Libertad", submarket: "La Libertad Coast", latitude: 13.4883, longitude: -89.3222, sourceReference: "https://www.elsalvador.travel/en/where-to-go/la-libertad", pointSubtype: "Coastal Highway", notes: "CA-2 coastal road serving La Libertad surf-town and weekend leisure lodging demand." }),
  ti({ name: "Santa Ana Volcano Cerro Verde Corridor Access", pointType: "Highway Access", city: "Santa Ana", submarket: "Santa Ana", latitude: 13.8312, longitude: -89.6212, sourceReference: "https://www.marn.gob.sv/", pointSubtype: "Volcano Corridor", notes: "Highland access road to Cerro Verde and Santa Ana volcano adventure tourism lodging." }),
  ti({ name: "CA-2 Coastal Highway Sonsonate Access", pointType: "Highway Access", city: "Sonsonate", submarket: "La Libertad Coast", latitude: 13.7189, longitude: -89.7245, sourceReference: "https://www.mopt.gob.sv/", pointSubtype: "Coastal Highway", notes: "Western Pacific coastal highway node linking surf and beach resort submarkets." }),
  ti({ name: "Ruta de las Flores Highland Corridor Access", pointType: "Highway Access", city: "Juayúa", submarket: "Santa Ana", latitude: 13.8412, longitude: -89.7512, sourceReference: "https://www.elsalvador.travel/en/where-to-go/ruta-de-las-flores", pointSubtype: "Scenic Corridor", notes: "Coffee-route highland road serving boutique lodge and agritourism demand in western highlands." }),
  ti({ name: "Autopista Comalapa Airport Connector Access", pointType: "Highway Access", city: "San Luis Talpa", submarket: "San Salvador", latitude: 13.4512, longitude: -89.0712, sourceReference: "https://www.fsv.gob.sv/", pointSubtype: "Airport Connector", notes: "Controlled-access highway linking SAL airport to San Salvador metro hotel corridors." }),
];

export function buildElSalvadorTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, EL_SALVADOR_TI_DELTA_RECORDS);
}
