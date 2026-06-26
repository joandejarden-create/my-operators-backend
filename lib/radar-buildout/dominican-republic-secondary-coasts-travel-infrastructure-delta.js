/**
 * DR Secondary Coasts Mature Pass Travel Infrastructure delta records.
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = "Dominican Republic";
const MARKET = "DR Secondary Coasts Mature Pass";
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const DOMINICAN_REPUBLIC_SECONDARY_COASTS_TI_DELTA_RECORDS = [
  ti({
    name: "Miches Costa Esmeralda Resort Highway Node",
    pointType: "Highway Access",
    pointSubtype: "Emerging Resort Corridor",
    city: "Miches",
    submarket: "Miches / Costa Esmeralda",
    latitude: 18.9924,
    longitude: -69.0386,
    sourceReference: "https://www.mop.gob.do/carreteras/miches-costa-esmeralda",
    notes: "Coastal highway connector linking Higüey to Miches and Costa Esmeralda resort development.",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Barahona Southwest Commercial Port Terminal",
    pointType: "Port / Maritime",
    pointSubtype: "Commercial Port",
    city: "Barahona",
    submarket: "Barahona / Pedernales",
    latitude: 18.1994,
    longitude: -71.0891,
    sourceReference: "https://www.godominicanrepublic.com/destinations/barahona-port",
    notes: "Southwest coastal port and maritime access for Barahona eco-tourism and cargo-adjacent demand.",
    useCaseTags: ["Cruise / Port", "Industrial / Logistics"],
  }),
  ti({
    name: "Constanza Highland Connector from Jarabacoa",
    pointType: "Highway Access",
    pointSubtype: "Mountain Corridor",
    city: "Jarabacoa",
    submarket: "Jarabacoa / Constanza",
    latitude: 19.1086,
    longitude: -70.6584,
    sourceReference: "https://www.godominicanrepublic.com/destinations/jarabacoa-constanza-road",
    notes: "Highland mountain road linking Jarabacoa adventure lodges to Constanza cold-climate tourism.",
    useCaseTags: ["Airport / Transit", "Nature / Eco-Tourism"],
  }),
];

export function buildDominicanRepublicSecondaryCoastsTiDeltaFixture() {
  return buildIslandTiDeltaFixture(
    COUNTRY,
    MARKET,
    DOMINICAN_REPUBLIC_SECONDARY_COASTS_TI_DELTA_RECORDS
  );
}
