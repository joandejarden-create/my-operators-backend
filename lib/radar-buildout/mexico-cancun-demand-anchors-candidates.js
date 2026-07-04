/**
 * Mexico — Cancún / Riviera Maya demand anchor candidates (source-backed).
 */

import { getPointTypeDefaults } from "../demand-anchors/point-type-defaults.js";
import {
  applyMexicoCancunGovernanceDefaults,
  MEXICO_CANCUN_SUBMARKETS,
} from "./mexico-cancun-demand-anchor-governance.js";

import { MEXICO_RADAR_REGION } from "./mexico-radar-region.js";

const COUNTRY = "Mexico";
const REGION = MEXICO_RADAR_REGION;
const MARKET = "Cancún / Riviera Maya";

function pt(v) {
  const defaults = getPointTypeDefaults(v.pointType);
  const rationale =
    v.hotelDemandNote ||
    defaults.hotelDemandRationale ||
    "Supports identifiable resort/leisure hotel demand in the corridor.";
  const base = {
    name: v.name,
    pointType: v.pointType,
    city: v.city,
    country: COUNTRY,
    region: REGION,
    submarket: v.submarket,
    latitude: v.latitude,
    longitude: v.longitude,
    source: "Public Source",
    sourceReference: v.sourceReference,
    dataConfidence: v.dataConfidence || "Medium",
    notes:
      v.notes ||
      `Submarket: ${v.submarket}. ${rationale} Candidate pending Google pre-import verification.`,
  };
  if (v.googleSearchQuery) base.googleSearchQuery = v.googleSearchQuery;
  if (v.manuallyVerified) {
    base.notes = `${base.notes} Manually verified using official/public source; Google Maps match was not used as final authority.`;
    base.dataConfidence = v.dataConfidence || "High";
  }
  return applyMexicoCancunGovernanceDefaults(base, v.governance || {});
}

/** @type {ReturnType<typeof pt>[]} */
export const MEXICO_CANCUN_BATCH_1_CANDIDATES = [
  // Cancún Hotel Zone (6)
  pt({
    name: "Cancún Center Convention Complex",
    pointType: "Convention Center",
    city: "Cancún",
    submarket: "Cancún Hotel Zone",
    latitude: 21.1136,
    longitude: -86.7642,
    sourceReference: "https://www.cancuncenter.com/",
    hotelDemandNote:
      "Primary meeting/group venue for the hotel zone; drives compression nights and convention-oriented resort demand.",
  }),
  pt({
    name: "Playa Delfines",
    pointType: "Beach / Waterfront",
    city: "Cancún",
    submarket: "Cancún Hotel Zone",
    latitude: 21.0479,
    longitude: -86.782,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/quintana-roo/cancun",
    dataConfidence: "High",
  }),
  pt({
    name: "La Isla Cancún Shopping Paradise",
    pointType: "Mixed-Use Development",
    city: "Cancún",
    submarket: "Cancún Hotel Zone",
    latitude: 21.1158,
    longitude: -86.7607,
    sourceReference: "https://www.laislacancun.com/",
  }),
  pt({
    name: "Forum by the Sea Entertainment Complex",
    pointType: "Entertainment District",
    city: "Cancún",
    submarket: "Cancún Hotel Zone",
    latitude: 21.1322,
    longitude: -86.7471,
    sourceReference: "https://www.forumbythesea.com/",
  }),
  pt({
    name: "Hospital Galenia Cancún",
    pointType: "Medical Campus",
    city: "Cancún",
    submarket: "Cancún Hotel Zone",
    latitude: 21.1614,
    longitude: -86.8248,
    sourceReference: "https://www.galenia.com/",
  }),
  pt({
    name: "Plaza Kukulcán Commercial Corridor",
    pointType: "Business District",
    city: "Cancún",
    submarket: "Cancún Hotel Zone",
    latitude: 21.1335,
    longitude: -86.7478,
    sourceReference: "https://www.plazakukulcan.com/",
  }),

  // Puerto Cancún (2)
  pt({
    name: "Puerto Cancún Marina & Golf",
    pointType: "Mixed-Use Development",
    city: "Cancún",
    submarket: "Puerto Cancún",
    latitude: 21.1752,
    longitude: -86.8051,
    sourceReference: "https://www.puertocancun.com.mx/",
  }),
  pt({
    name: "Parque Tarja Puerto Cancún",
    pointType: "Entertainment District",
    city: "Cancún",
    submarket: "Puerto Cancún",
    latitude: 21.1698,
    longitude: -86.8012,
    sourceReference: "https://www.puertocancun.com.mx/",
  }),

  // Costa Mujeres / Playa Mujeres (2)
  pt({
    name: "Playa Mujeres Beach Resort Corridor",
    pointType: "Beach / Waterfront",
    city: "Cancún",
    submarket: "Costa Mujeres / Playa Mujeres",
    latitude: 21.2456,
    longitude: -86.8058,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/quintana-roo/cancun",
    googleSearchQuery: "Playa Mujeres Cancún Quintana Roo Mexico",
  }),
  pt({
    name: "Costa Mujeres Tourism Development Corridor",
    pointType: "Future Growth Node",
    city: "Cancún",
    submarket: "Costa Mujeres / Playa Mujeres",
    latitude: 21.252,
    longitude: -86.812,
    sourceReference: "https://www.gob.mx/sectur",
    hotelDemandNote:
      "Master-planned resort corridor north of Puerto Juárez; supports new-build resort and luxury pipeline demand.",
  }),

  // Riviera Maya / Playa del Carmen (6)
  pt({
    name: "Quinta Avenida Playa del Carmen",
    pointType: "Entertainment District",
    city: "Playa del Carmen",
    submarket: "Riviera Maya / Playa del Carmen",
    latitude: 20.6282,
    longitude: -87.0739,
    sourceReference: "https://www.playadelcarmen.gob.mx/",
  }),
  pt({
    name: "Parque Fundadores Playa del Carmen",
    pointType: "Tourist Attraction",
    city: "Playa del Carmen",
    submarket: "Riviera Maya / Playa del Carmen",
    latitude: 20.6251,
    longitude: -87.0729,
    sourceReference: "https://www.playadelcarmen.gob.mx/",
  }),
  pt({
    name: "Centro de Convenciones Riviera Maya",
    pointType: "Convention Center",
    city: "Playa del Carmen",
    submarket: "Riviera Maya / Playa del Carmen",
    latitude: 20.6158,
    longitude: -87.0892,
    sourceReference: "https://www.ccrivieramaya.com/",
  }),
  pt({
    name: "Hospital Playa del Carmen",
    pointType: "Medical Campus",
    city: "Playa del Carmen",
    submarket: "Riviera Maya / Playa del Carmen",
    latitude: 20.6512,
    longitude: -87.0895,
    sourceReference: "https://www.hospitalplayadelcarmen.com/",
  }),
  pt({
    name: "Xcaret Park",
    pointType: "Tourist Attraction",
    city: "Playa del Carmen",
    submarket: "Riviera Maya / Playa del Carmen",
    latitude: 20.5803,
    longitude: -87.1195,
    sourceReference: "https://www.xcaret.com/",
    dataConfidence: "High",
  }),
  pt({
    name: "Playacar Resort & Business District",
    pointType: "Business District",
    city: "Playa del Carmen",
    submarket: "Riviera Maya / Playa del Carmen",
    latitude: 20.6078,
    longitude: -87.0988,
    sourceReference: "https://www.playacar.com.mx/",
  }),

  // Tulum (4)
  pt({
    name: "Tulum Archaeological Zone",
    pointType: "Tourist Attraction",
    city: "Tulum",
    submarket: "Tulum",
    latitude: 20.2144,
    longitude: -87.4294,
    sourceReference: "https://www.inah.gob.mx/",
    dataConfidence: "High",
  }),
  pt({
    name: "Tulum Beach Hotel Zone",
    pointType: "Beach / Waterfront",
    city: "Tulum",
    submarket: "Tulum",
    latitude: 20.1985,
    longitude: -87.4342,
    sourceReference: "https://www.tulum.gob.mx/",
    googleSearchQuery: "Tulum Beach Hotel Zone Quintana Roo Mexico",
  }),
  pt({
    name: "Aldea Zamá Mixed-Use District",
    pointType: "Mixed-Use Development",
    city: "Tulum",
    submarket: "Tulum",
    latitude: 20.2048,
    longitude: -87.4512,
    sourceReference: "https://www.aldeazama.com/",
  }),
  pt({
    name: "Tulum Municipal Palace Civic Center",
    pointType: "Government / Civic Center",
    city: "Tulum",
    submarket: "Tulum",
    latitude: 20.2115,
    longitude: -87.4654,
    sourceReference: "https://www.tulum.gob.mx/",
  }),

  // Cozumel (3)
  pt({
    name: "San Miguel de Cozumel Downtown Waterfront",
    pointType: "Entertainment District",
    city: "Cozumel",
    submarket: "Cozumel",
    latitude: 20.5083,
    longitude: -86.9458,
    sourceReference: "https://www.cozumel.gob.mx/",
  }),
  pt({
    name: "Punta Sur Eco Beach Park",
    pointType: "Tourist Attraction",
    city: "Cozumel",
    submarket: "Cozumel",
    latitude: 20.2972,
    longitude: -87.0245,
    sourceReference: "https://www.cozumel.gob.mx/",
  }),
  pt({
    name: "Punta Langosta Cruise Pier Plaza",
    pointType: "Tourist Attraction",
    city: "Cozumel",
    submarket: "Cozumel",
    latitude: 20.5112,
    longitude: -86.9512,
    sourceReference: "https://www.puertocozumel.com/",
    hotelDemandNote:
      "Cruise passenger flows support day-stay and pre/post-cruise hotel demand in San Miguel corridor.",
  }),

  // Isla Mujeres (3)
  pt({
    name: "Playa Norte Isla Mujeres",
    pointType: "Beach / Waterfront",
    city: "Isla Mujeres",
    submarket: "Isla Mujeres",
    latitude: 21.2578,
    longitude: -86.7489,
    sourceReference: "https://www.islamujeres.gob.mx/",
    dataConfidence: "High",
  }),
  pt({
    name: "Garrafón Reef Park",
    pointType: "Tourist Attraction",
    city: "Isla Mujeres",
    submarket: "Isla Mujeres",
    latitude: 21.2165,
    longitude: -86.7162,
    sourceReference: "https://www.garrafon.com/",
  }),
  pt({
    name: "Hacienda Mundaca Heritage Park",
    pointType: "Tourist Attraction",
    city: "Isla Mujeres",
    submarket: "Isla Mujeres",
    latitude: 21.2408,
    longitude: -86.7355,
    sourceReference: "https://www.islamujeres.gob.mx/",
  }),
];

/** @type {ReturnType<typeof pt>[]} */
export const MEXICO_CANCUN_BATCH_2_CANDIDATES = [
  // Cancún Hotel Zone extras
  pt({
    name: "Playa Tortugas Cancún",
    pointType: "Beach / Waterfront",
    city: "Cancún",
    submarket: "Cancún Hotel Zone",
    latitude: 21.1389,
    longitude: -86.7542,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/quintana-roo/cancun",
  }),
  pt({
    name: "Playa Chac Mool Cancún",
    pointType: "Beach / Waterfront",
    city: "Cancún",
    submarket: "Cancún Hotel Zone",
    latitude: 21.1265,
    longitude: -86.7588,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/quintana-roo/cancun",
  }),
  pt({
    name: "Amerimed Hospital Cancún",
    pointType: "Medical Campus",
    city: "Cancún",
    submarket: "Cancún Hotel Zone",
    latitude: 21.1421,
    longitude: -86.8512,
    sourceReference: "https://www.amerimedhospitals.com/",
  }),
  pt({
    name: "Universidad del Caribe Cancún Campus",
    pointType: "University / College",
    city: "Cancún",
    submarket: "Cancún Hotel Zone",
    latitude: 21.1618,
    longitude: -86.8515,
    sourceReference: "https://www.unicaribe.edu.mx/",
  }),

  // Puerto Cancún / Costa Mujeres
  pt({
    name: "Marina Town Center Puerto Cancún",
    pointType: "Business District",
    city: "Cancún",
    submarket: "Puerto Cancún",
    latitude: 21.1725,
    longitude: -86.8033,
    sourceReference: "https://www.puertocancun.com.mx/",
  }),
  pt({
    name: "Playa Delfines Costa Mujeres",
    pointType: "Beach / Waterfront",
    city: "Cancún",
    submarket: "Costa Mujeres / Playa Mujeres",
    latitude: 21.2388,
    longitude: -86.7995,
    sourceReference: "https://www.visitmexico.com/en/main-destinations/quintana-roo/cancun",
    googleSearchQuery: "Playa Delfines Costa Mujeres Cancún Mexico",
  }),

  // Mayakoba (4)
  pt({
    name: "Mayakoba Resort Corridor",
    pointType: "Beach / Waterfront",
    city: "Playa del Carmen",
    submarket: "Mayakoba",
    latitude: 20.6912,
    longitude: -87.0285,
    sourceReference: "https://www.mayakoba.com/",
    dataConfidence: "High",
  }),
  pt({
    name: "Mayakoba El Camaleón Golf Club",
    pointType: "Sports Venue",
    city: "Playa del Carmen",
    submarket: "Mayakoba",
    latitude: 20.6888,
    longitude: -87.0312,
    sourceReference: "https://www.mayakoba.com/",
  }),
  pt({
    name: "Mayakoba Mixed-Use Village Center",
    pointType: "Mixed-Use Development",
    city: "Playa del Carmen",
    submarket: "Mayakoba",
    latitude: 20.6895,
    longitude: -87.0268,
    sourceReference: "https://www.mayakoba.com/",
  }),
  pt({
    name: "Mayakoba Future Expansion Node",
    pointType: "Future Growth Node",
    city: "Playa del Carmen",
    submarket: "Mayakoba",
    latitude: 20.6942,
    longitude: -87.0345,
    sourceReference: "https://www.mayakoba.com/",
    manuallyVerified: true,
    hotelDemandNote:
      "Eco-integrated master-planned resort corridor with ongoing luxury pipeline and branded resort expansion.",
  }),

  // Akumal / Puerto Aventuras (4)
  pt({
    name: "Akumal Bay Beach & Snorkel Corridor",
    pointType: "Beach / Waterfront",
    city: "Akumal",
    submarket: "Akumal / Puerto Aventuras",
    latitude: 20.3958,
    longitude: -87.3152,
    sourceReference: "https://www.akumal.mx/",
  }),
  pt({
    name: "Puerto Aventuras Marina & Resort District",
    pointType: "Mixed-Use Development",
    city: "Puerto Aventuras",
    submarket: "Akumal / Puerto Aventuras",
    latitude: 20.4965,
    longitude: -87.2412,
    sourceReference: "https://www.puertoaventuras.com/",
  }),
  pt({
    name: "Xel-Há Natural Park",
    pointType: "Tourist Attraction",
    city: "Akumal",
    submarket: "Akumal / Puerto Aventuras",
    latitude: 20.3188,
    longitude: -87.3565,
    sourceReference: "https://www.xelha.com/",
    dataConfidence: "High",
  }),
  pt({
    name: "Puerto Aventuras Golf Club & Sports Corridor",
    pointType: "Sports Venue",
    city: "Puerto Aventuras",
    submarket: "Akumal / Puerto Aventuras",
    latitude: 20.4988,
    longitude: -87.2365,
    sourceReference: "https://www.puertoaventuras.com/",
  }),

  // More Riviera Maya / Playa del Carmen
  pt({
    name: "Xplor Adventure Park",
    pointType: "Tourist Attraction",
    city: "Playa del Carmen",
    submarket: "Riviera Maya / Playa del Carmen",
    latitude: 20.6022,
    longitude: -87.1288,
    sourceReference: "https://www.xplor.travel/",
  }),
  pt({
    name: "Playa Mamitas Beach Club Corridor",
    pointType: "Beach / Waterfront",
    city: "Playa del Carmen",
    submarket: "Riviera Maya / Playa del Carmen",
    latitude: 20.6315,
    longitude: -87.0688,
    sourceReference: "https://www.playadelcarmen.gob.mx/",
  }),
  pt({
    name: "Playa del Carmen Municipal Palace",
    pointType: "Government / Civic Center",
    city: "Playa del Carmen",
    submarket: "Riviera Maya / Playa del Carmen",
    latitude: 20.6275,
    longitude: -87.0758,
    sourceReference: "https://www.playadelcarmen.gob.mx/",
  }),
  pt({
    name: "Centro Maya Industrial Corridor",
    pointType: "Industrial / Logistics Zone",
    city: "Playa del Carmen",
    submarket: "Riviera Maya / Playa del Carmen",
    latitude: 20.6598,
    longitude: -87.1125,
    sourceReference: "https://www.gob.mx/sectur",
    googleSearchQuery: "Centro Maya industrial park Playa del Carmen Mexico",
  }),

  // More Tulum
  pt({
    name: "Tulum National Park Sian Ka'an Buffer Gateway",
    pointType: "Tourist Attraction",
    city: "Tulum",
    submarket: "Tulum",
    latitude: 20.1288,
    longitude: -87.4625,
    sourceReference: "https://www.conanp.gob.mx/",
    manuallyVerified: true,
    googleSearchQuery: "Sian Ka'an Biosphere Reserve Tulum Quintana Roo",
  }),
  pt({
    name: "Tulum Hotel Zone Future Growth Corridor",
    pointType: "Future Growth Node",
    city: "Tulum",
    submarket: "Tulum",
    latitude: 20.1925,
    longitude: -87.4288,
    sourceReference: "https://www.tulum.gob.mx/",
    manuallyVerified: true,
    hotelDemandNote:
      "Boutique and lifestyle resort pipeline along the coastal hotel zone; strong ADR and leisure demand signal.",
  }),
  pt({
    name: "Tulum International Airport Gateway Area",
    pointType: "Future Growth Node",
    city: "Tulum",
    submarket: "Tulum",
    latitude: 20.1722,
    longitude: -87.6608,
    sourceReference: "https://www.aeropuertosasa.mx/aeropuerto-de-tulum",
    hotelDemandNote:
      "New airport access node reshaping lodging demand between Tulum town and southern Riviera Maya.",
  }),

  // More Cozumel
  pt({
    name: "Chankanaab Beach Adventure Park",
    pointType: "Tourist Attraction",
    city: "Cozumel",
    submarket: "Cozumel",
    latitude: 20.4442,
    longitude: -86.9725,
    sourceReference: "https://www.cozumel.gob.mx/",
  }),
  pt({
    name: "Cozumel Municipal Palace Civic Center",
    pointType: "Government / Civic Center",
    city: "Cozumel",
    submarket: "Cozumel",
    latitude: 20.5088,
    longitude: -86.9465,
    sourceReference: "https://www.cozumel.gob.mx/",
  }),
  pt({
    name: "Cozumel Cruise Terminal SSA",
    pointType: "Mixed-Use Development",
    city: "Cozumel",
    submarket: "Cozumel",
    latitude: 20.5145,
    longitude: -86.9488,
    sourceReference: "https://www.puertocozumel.com/",
  }),

  // More Isla Mujeres
  pt({
    name: "Isla Mujeres Turtle Farm (Tortugranja)",
    pointType: "Tourist Attraction",
    city: "Isla Mujeres",
    submarket: "Isla Mujeres",
    latitude: 21.2225,
    longitude: -86.7312,
    sourceReference: "https://www.islamujeres.gob.mx/",
  }),
  pt({
    name: "Isla Mujeres Downtown Malecón",
    pointType: "Entertainment District",
    city: "Isla Mujeres",
    submarket: "Isla Mujeres",
    latitude: 21.2568,
    longitude: -86.7465,
    sourceReference: "https://www.islamujeres.gob.mx/",
  }),

  // Other / corridor nodes
  pt({
    name: "Puerto Morelos Reef National Park Gateway",
    pointType: "Tourist Attraction",
    city: "Puerto Morelos",
    submarket: "Other",
    latitude: 20.8488,
    longitude: -86.8755,
    sourceReference: "https://www.puertomorelos.gob.mx/",
  }),
  pt({
    name: "Moon Palace Convention Center Complex",
    pointType: "Convention Center",
    city: "Cancún",
    submarket: "Other",
    latitude: 21.0285,
    longitude: -86.8752,
    sourceReference: "https://www.moonpalacecancun.com/",
    hotelDemandNote:
      "Large-format meetings and incentive demand generator for the southern Cancún corridor.",
  }),
  pt({
    name: "Estadio Olímpico Andrés Quintana Roo",
    pointType: "Sports Venue",
    city: "Cancún",
    submarket: "Other",
    latitude: 21.1612,
    longitude: -86.8518,
    sourceReference: "https://www.cancun.gob.mx/",
  }),
  pt({
    name: "Cancún Municipal Palace Civic Center",
    pointType: "Government / Civic Center",
    city: "Cancún",
    submarket: "Other",
    latitude: 21.1615,
    longitude: -86.8275,
    sourceReference: "https://www.cancun.gob.mx/",
  }),
  pt({
    name: "Xoximilco Cancún Entertainment Park",
    pointType: "Entertainment District",
    city: "Cancún",
    submarket: "Other",
    latitude: 21.0588,
    longitude: -86.8512,
    sourceReference: "https://www.xoximilco.com/",
  }),
  pt({
    name: "Ventura Park Cancún",
    pointType: "Entertainment District",
    city: "Cancún",
    submarket: "Other",
    latitude: 21.1618,
    longitude: -86.8288,
    sourceReference: "https://www.venturapark.com/",
  }),
];

export { MEXICO_CANCUN_SUBMARKETS };

export function getMexicoCancunCandidates(batch = "all") {
  if (batch === "1" || batch === 1) return [...MEXICO_CANCUN_BATCH_1_CANDIDATES];
  if (batch === "2" || batch === 2) return [...MEXICO_CANCUN_BATCH_2_CANDIDATES];
  return [...MEXICO_CANCUN_BATCH_1_CANDIDATES, ...MEXICO_CANCUN_BATCH_2_CANDIDATES];
}
