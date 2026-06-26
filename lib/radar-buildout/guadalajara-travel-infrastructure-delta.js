/**
 * Guadalajara Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Mexico";
const MARKET = "Guadalajara";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const GUADALAJARA_TI_DELTA_RECORDS = [
  ti({ name: "Guadalajara International Airport Access", pointType: "Highway Access", city: "Tlajomulco de Zúñiga", submarket: "Airport Corridor", latitude: 20.5218, longitude: -103.3112, sourceReference: "https://www.aeropuertogdl.com/", pointSubtype: "Airport Access", notes: "Primary international gateway for Guadalajara corporate and MICE demand." }),
  ti({ name: "Expo Guadalajara Convention Access", pointType: "Highway Access", city: "Guadalajara", submarket: "Expo / Andares", latitude: 20.6742, longitude: -103.3873, sourceReference: "https://www.expoguadalajara.com.mx/", pointSubtype: "Convention Access", notes: "Convention center highway access for group and event compression." }),
  ti({ name: "Centro Histórico Transit Access", pointType: "Highway Access", city: "Guadalajara", submarket: "Centro", latitude: 20.6769, longitude: -103.3472, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/guadalajara", pointSubtype: "Historic Core", notes: "Historic center access for cultural tourism and urban hotel demand." }),
  ti({ name: "Andares Puerta de Hierro Corridor Access", pointType: "Highway Access", city: "Zapopan", submarket: "Expo / Andares", latitude: 20.7097, longitude: -103.4112, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/guadalajara", pointSubtype: "Business District", notes: "Zapopan luxury retail and corporate corridor access." }),
  ti({ name: "Tlaquepaque Crafts District Access", pointType: "Highway Access", city: "San Pedro Tlaquepaque", submarket: "Tlaquepaque", latitude: 20.6403, longitude: -103.3114, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/tlaquepaque", pointSubtype: "Tourism Corridor", notes: "Crafts and dining tourism access south of Guadalajara centro." }),
  ti({ name: "Estadio Akron Event Access", pointType: "Highway Access", city: "Zapopan", submarket: "Zapopan", latitude: 20.6819, longitude: -103.4622, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/guadalajara", pointSubtype: "Sports Venue", notes: "Sports and concert event compression access in western Zapopan." }),
  ti({ name: "Periférico Sur Ring Road Access", pointType: "Highway Access", city: "Guadalajara", submarket: "Other", latitude: 20.6512, longitude: -103.3912, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/guadalajara", pointSubtype: "Urban Artery", notes: "Periférico ring road linking airport, Expo, and Zapopan submarkets." }),
  ti({ name: "Tequila Ruta Tourism Highway Access", pointType: "Highway Access", city: "Tequila", submarket: "Other", latitude: 20.8812, longitude: -103.8345, sourceReference: "https://www.visitmexico.com/en/main-destinations/jalisco/tequila", pointSubtype: "Tourism Corridor", notes: "Day-trip agri-tourism corridor from Guadalajara hotel base." }),
];

export function buildGuadalajaraTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, GUADALAJARA_TI_DELTA_RECORDS);
}
