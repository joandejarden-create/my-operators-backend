/**
 * Guatemala Countrywide Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Guatemala";
const MARKET = "Guatemala Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const GUATEMALA_TI_DELTA_RECORDS = [
  ti({ name: "La Aurora International Airport (GUA) Access", pointType: "Highway Access", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.5833, longitude: -90.5275, sourceReference: "https://www.dgac.gob.gt/", pointSubtype: "Airport Access", notes: "Primary international gateway for Guatemala City metro corporate and leisure demand." }),
  ti({ name: "Puerto Quetzal Cruise and Cargo Port Access", pointType: "Port / Maritime", city: "Puerto Quetzal", submarket: "Other", latitude: 13.9225, longitude: -90.7856, sourceReference: "https://www.apq.com.gt/", pointSubtype: "Cruise Terminal", notes: "Pacific cruise and cargo port supporting pre/post-cruise Antigua and coastal lodging.", useCaseTags: ["Cruise / Port", "Industrial / Logistics"] }),
  ti({ name: "Pan-American Highway Guatemala City Corridor Access", pointType: "Highway Access", city: "Guatemala City", submarket: "Guatemala City", latitude: 14.6349, longitude: -90.5069, sourceReference: "https://www.civ.gob.gt/", pointSubtype: "National Highway", notes: "CA-9 Interamericana node through capital metro linking north-south domestic tourism flows." }),
  ti({ name: "Antigua Guatemala Highway Access Corridor", pointType: "Highway Access", city: "Antigua Guatemala", submarket: "Antigua", latitude: 14.5623, longitude: -90.6512, sourceReference: "https://www.visitguatemala.com/", pointSubtype: "Heritage Corridor", notes: "Primary road access to Antigua UNESCO colonial core and boutique hotel market." }),
  ti({ name: "Flores Petén Tikal Tourism Corridor Access", pointType: "Highway Access", city: "Flores", submarket: "Petén / Tikal", latitude: 16.9267, longitude: -89.8922, sourceReference: "https://www.visitguatemala.com/", pointSubtype: "Archaeology Corridor", notes: "Northern lowlands road hub linking Flores island town to Tikal UNESCO park lodging." }),
  ti({ name: "Mundo Maya International Airport Access", pointType: "Highway Access", city: "Flores", submarket: "Petén / Tikal", latitude: 16.9138, longitude: -89.8664, sourceReference: "https://www.dgac.gob.gt/", pointSubtype: "Airport Access", notes: "Petén regional air gateway for Tikal and Maya Biosphere eco-tourism demand." }),
  ti({ name: "CA-1 Western Highlands Pan-American Access", pointType: "Highway Access", city: "Quetzaltenango", submarket: "Other", latitude: 14.8347, longitude: -91.5181, sourceReference: "https://www.civ.gob.gt/", pointSubtype: "Highland Corridor", notes: "Western highlands Pan-American node serving Xela and Lake Atitlán feeder markets." }),
  ti({ name: "Lake Atitlán Panajachel Access Corridor", pointType: "Highway Access", city: "Panajachel", submarket: "Lake Atitlán", latitude: 14.7419, longitude: -91.1532, sourceReference: "https://www.visitguatemala.com/", pointSubtype: "Lakeshore Corridor", notes: "Primary road access to Lake Atitlán lakeside tourism and boutique lodge inventory." }),
];

export function buildGuatemalaTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, GUATEMALA_TI_DELTA_RECORDS);
}
