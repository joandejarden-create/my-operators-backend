/**
 * Saint Lucia countrywide Travel Infrastructure delta records (audit gap fill).
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Saint Lucia";
const MARKET = "Saint Lucia Countrywide";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const SAINT_LUCIA_TI_DELTA_RECORDS = [
  ti({ name: "Hewanorra International Airport Access", pointType: "Highway Access", city: "Vieux Fort", submarket: "Vieux Fort", latitude: 13.7332, longitude: -60.9526, sourceReference: "https://www.slulimited.com/", pointSubtype: "Airport Access", notes: "Primary long-haul stayover gateway for south Saint Lucia resort demand." }),
  ti({ name: "George F. L. Charles Airport Access", pointType: "Highway Access", city: "Castries", submarket: "Castries", latitude: 14.0202, longitude: -60.9929, sourceReference: "https://www.slulimited.com/", pointSubtype: "Airport Access", notes: "Regional airport serving Castries and north resort corridors." }),
  ti({ name: "Castries Cruise Port Access", pointType: "Port / Maritime", city: "Castries", submarket: "Castries", latitude: 14.0107, longitude: -60.9915, sourceReference: "https://www.stlucia.org/", pointSubtype: "Cruise Terminal", notes: "Main cruise terminal supporting day-call and pre/post-stay lodging.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Rodney Bay Marina Access", pointType: "Port / Maritime", city: "Rodney Bay", submarket: "Rodney Bay / Gros Islet", latitude: 14.0742, longitude: -60.9512, sourceReference: "https://www.stlucia.org/", pointSubtype: "Marina", notes: "Marina access for yacht and north-coast leisure demand.", useCaseTags: ["Cruise / Port","Resort / Leisure"] }),
  ti({ name: "Rodney Bay Resort Corridor Access", pointType: "Highway Access", city: "Rodney Bay", submarket: "Rodney Bay / Gros Islet", latitude: 14.0789, longitude: -60.9498, sourceReference: "https://www.stlucia.org/", pointSubtype: "Resort Corridor", notes: "Primary road access serving Reduit Beach resort inventory." }),
  ti({ name: "Soufrière Pitons Scenic Corridor Access", pointType: "Highway Access", city: "Soufrière", submarket: "Soufrière", latitude: 13.8567, longitude: -61.0567, sourceReference: "https://www.stlucia.org/", pointSubtype: "Scenic Corridor", notes: "West coast scenic access linking Pitons resort and heritage demand." }),
  ti({ name: "Marigot Bay Ferry and Harbour Access", pointType: "Ferry Terminal", city: "Marigot Bay", submarket: "Other", latitude: 13.9478, longitude: -61.0267, sourceReference: "https://www.stlucia.org/", pointSubtype: "Harbour Access", notes: "Harbour and water-taxi access for Marigot Bay boutique resort demand." }),
  ti({ name: "Vieux Fort Industrial Free Zone Access", pointType: "Highway Access", city: "Vieux Fort", submarket: "Vieux Fort", latitude: 13.7289, longitude: -60.9612, sourceReference: "https://www.stlucia.gov.lc/", pointSubtype: "Industrial Corridor", notes: "South gateway industrial connector supporting business travel.", useCaseTags: ["Industrial / Logistics","Airport / Transit"] }),
];

export function buildSaintLuciaTiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, SAINT_LUCIA_TI_DELTA_RECORDS);
}
