/**
 * Dominican Republic Travel Infrastructure — second pass (non-airport/cruise gap fill).
 */

const COUNTRY = "Dominican Republic";
const REGION = "Caribbean";

function pt(opts) {
  return {
    country: COUNTRY,
    region: REGION,
    source: "Public Source",
    visibility: "Internal Only",
    includeOnRadarMap: true,
    dataConfidence: opts.dataConfidence || "High",
    notes: opts.notes || `Submarket: ${opts.submarket}.`,
    ...opts,
  };
}

export const DR_TRAVEL_INFRA_SECOND_PASS = [
  pt({
    name: "DP World Caucedo Multimodal Port",
    pointType: "Port / Maritime",
    pointSubtype: "Container Port",
    latitude: 18.4242,
    longitude: -69.6167,
    city: "Boca Chica",
    submarket: "Boca Chica / Juan Dolio",
    sourceReference: "https://www.dpworld.com/caucedo",
    hotelDemandRationale:
      "Dominican Republic's primary container port; crew, logistics, and extended-stay hotel demand near port access.",
  }),
  pt({
    name: "Port of Sans Souci — Santo Domingo",
    pointType: "Port / Maritime",
    pointSubtype: "Cargo / Cruise Support Port",
    latitude: 18.4485,
    longitude: -69.882,
    city: "Santo Domingo",
    submarket: "Santo Domingo Metro",
    sourceReference: "https://www.godominicanrepublic.com/ports/sans-souci",
    dataConfidence: "Medium",
    hotelDemandRationale:
      "Metro Santo Domingo maritime port; supports port-adjacent crew and logistics hotel demand.",
    notes: "Submarket: Santo Domingo Metro. Sans Souci port area centroid.",
  }),
  pt({
    name: "Sabana de la Mar Ferry Terminal",
    pointType: "Ferry Terminal",
    pointSubtype: "Inter-Island Ferry",
    latitude: 18.976,
    longitude: -69.409,
    city: "Sabana de la Mar",
    submarket: "Samaná / Las Terrenas",
    sourceReference: "https://www.godominicanrepublic.com/destinations/samana",
    dataConfidence: "Medium",
    hotelDemandRationale:
      "Ferry gateway to Samaná Peninsula and Saona excursions; supports Samaná/Miches corridor access demand.",
    notes: "Submarket: Samaná / Las Terrenas. Sabana de la Mar ferry landing centroid.",
  }),
  pt({
    name: "Santa Bárbara de Samaná Maritime Terminal",
    pointType: "Ferry Terminal",
    pointSubtype: "Regional Ferry / Maritime",
    latitude: 19.206,
    longitude: -69.3355,
    city: "Samaná",
    submarket: "Samaná / Las Terrenas",
    sourceReference: "https://www.godominicanrepublic.com/destinations/samana",
    dataConfidence: "Medium",
    hotelDemandRationale:
      "Samaná town maritime access node; whale-season and peninsula ferry-related hotel demand.",
    notes: "Submarket: Samaná / Las Terrenas. Samaná waterfront terminal area.",
  }),
  pt({
    name: "Autopista del Coral — Punta Cana Corridor Access",
    pointType: "Highway Access",
    pointSubtype: "Resort Highway Corridor",
    latitude: 18.615,
    longitude: -68.452,
    city: "Higuey",
    submarket: "Punta Cana / Bávaro / Cap Cana",
    sourceReference: "https://www.mop.gob.do/",
    dataConfidence: "Medium",
    hotelDemandRationale:
      "Primary east-coast resort highway connector (SDQ–Punta Cana); drives air-lift resort corridor accessibility.",
    notes: "Submarket: Punta Cana / Bávaro / Cap Cana. Autopista del Coral corridor node near Higüey access.",
  }),
  pt({
    name: "Autopista Duarte (DR-1) — Santiago Access Node",
    pointType: "Highway Access",
    pointSubtype: "National Highway",
    latitude: 19.42,
    longitude: -70.68,
    city: "Santiago",
    submarket: "Santiago / Cibao",
    sourceReference: "https://www.mop.gob.do/",
    dataConfidence: "Medium",
    hotelDemandRationale:
      "Primary Cibao highway connector to Santo Domingo; supports regional business travel and logistics hotel demand.",
    notes: "Submarket: Santiago / Cibao. DR-1 Santiago metro access centroid.",
  }),
  pt({
    name: "Carretera Sánchez — Barahona Coastal Highway Access",
    pointType: "Highway Access",
    pointSubtype: "Coastal Highway",
    latitude: 18.208,
    longitude: -71.108,
    city: "Barahona",
    submarket: "Barahona / Pedernales",
    sourceReference: "https://www.godominicanrepublic.com/destinations/barahona",
    dataConfidence: "Medium",
    hotelDemandRationale:
      "Southwest coastal highway access to Pedernales eco-tourism; supports southwest lodge and adventure hotel demand.",
    notes: "Submarket: Barahona / Pedernales. Barahona–Pedernales corridor highway node.",
  }),
  pt({
    name: "Expreso Bavaro Bus Terminal",
    pointType: "Bus Terminal",
    pointSubtype: "Intercity Bus",
    latitude: 18.5595,
    longitude: -68.3728,
    city: "Verón",
    submarket: "Punta Cana / Bávaro / Cap Cana",
    sourceReference: "https://www.expresobavaro.com/",
    dataConfidence: "Medium",
    hotelDemandRationale:
      "Intercity bus terminal serving resort corridor workers and budget transient demand.",
    notes: "Submarket: Punta Cana / Bávaro / Cap Cana. Expreso Bávaro terminal area.",
  }),
  pt({
    name: "Caribe Tours Terminal — Santo Domingo",
    pointType: "Bus Terminal",
    pointSubtype: "National Bus Terminal",
    latitude: 18.4758,
    longitude: -69.9125,
    city: "Santo Domingo",
    submarket: "Santo Domingo Metro",
    sourceReference: "https://www.caribetours.com.do/",
    dataConfidence: "Medium",
    hotelDemandRationale:
      "National intercity bus hub; supports budget transient and connector travel hotel demand in capital.",
    notes: "Submarket: Santo Domingo Metro. Caribe Tours terminal centroid.",
  }),
  pt({
    name: "Puerto Plata Maritime Support Port — Maimón",
    pointType: "Port / Maritime",
    pointSubtype: "Cruise Support / Cargo",
    latitude: 19.831,
    longitude: -70.731,
    city: "Maimón",
    submarket: "Puerto Plata / Sosúa / Cabarete",
    sourceReference: "https://www.godominicanrepublic.com/ports/amber-cove",
    dataConfidence: "Medium",
    hotelDemandRationale:
      "North-coast maritime node adjacent to Amber Cove; cruise support and north-coast logistics hotel demand.",
    notes: "Submarket: Puerto Plata / Sosúa / Cabarete. Maimón maritime area — distinct from Amber Cove cruise berth record.",
  }),
];
