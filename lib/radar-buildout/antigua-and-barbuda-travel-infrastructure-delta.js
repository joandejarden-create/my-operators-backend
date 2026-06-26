/**
 * Antigua and Barbuda countrywide Travel Infrastructure delta records (audit gap fill).
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Antigua and Barbuda";
const MARKET = "Antigua and Barbuda Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const ANTIGUA_AND_BARBUDA_TI_DELTA_RECORDS = [
  ti({ name: "V.C. Bird International Airport Access", pointType: "Highway Access", city: "St. John's", submarket: "St. John's", latitude: 17.1367, longitude: -61.7928, sourceReference: "https://www.antigua-barbuda.com/", pointSubtype: "Airport Access", notes: "Primary international gateway for Antigua resort demand." }),
  ti({ name: "Heritage Quay Cruise Terminal Access", pointType: "Port / Maritime", city: "St. John's", submarket: "St. John's", latitude: 17.1234, longitude: -61.8456, sourceReference: "https://www.visitantiguabarbuda.com/", pointSubtype: "Cruise Terminal", notes: "Main cruise terminal in St. John's harbour.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "English Harbour Marina Access", pointType: "Port / Maritime", city: "English Harbour", submarket: "English Harbour", latitude: 17.0289, longitude: -61.7612, sourceReference: "https://www.visitantiguabarbuda.com/", pointSubtype: "Marina", notes: "Sailing and superyacht marina corridor access.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Dickenson Bay Resort Corridor Access", pointType: "Highway Access", city: "Dickenson Bay", submarket: "Dickenson Bay", latitude: 17.1678, longitude: -61.8456, sourceReference: "https://www.visitantiguabarbuda.com/", pointSubtype: "Resort Corridor", notes: "North coast resort strip road access." }),
  ti({ name: "Jolly Harbour Marina Village Access", pointType: "Port / Maritime", city: "Jolly Harbour", submarket: "Dickenson Bay", latitude: 17.0567, longitude: -61.8934, sourceReference: "https://www.visitantiguabarbuda.com/", pointSubtype: "Marina", notes: "Marina village supporting west coast leisure demand." }),
  ti({ name: "Codrington Barbuda Airport Access", pointType: "Highway Access", city: "Codrington", submarket: "Barbuda", latitude: 17.6356, longitude: -61.8289, sourceReference: "https://www.antigua-barbuda.com/", pointSubtype: "Airport Access", notes: "Inter-island air link for Barbuda eco-resort demand." }),
  ti({ name: "Barbuda Ferry Gateway Access", pointType: "Ferry Terminal", city: "Codrington", submarket: "Barbuda", latitude: 17.6289, longitude: -61.8234, sourceReference: "https://www.visitantiguabarbuda.com/", pointSubtype: "Inter-Island Ferry", notes: "Ferry access between Antigua and Barbuda." }),
  ti({ name: "Deep Water Harbour Logistics Access", pointType: "Port / Maritime", city: "St. John's", submarket: "St. John's", latitude: 17.1198, longitude: -61.8512, sourceReference: "https://www.gov.ab/", pointSubtype: "Commercial Port", notes: "Cargo port supporting construction and business travel.", useCaseTags: ["Industrial / Logistics","Airport / Transit"] }),
];

export function buildAntiguaAndBarbudaTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, ANTIGUA_AND_BARBUDA_TI_DELTA_RECORDS);
}
