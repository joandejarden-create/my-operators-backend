/**
 * Hotel-demand motion query library — buyer-first, not generic event discovery.
 */

export const DEMAND_MOTION_TERMS = Object.freeze([
  "accommodation",
  "hotel block",
  "room block",
  "housing",
  "lodging",
  "official hotel",
  "delegation accommodation",
  "crew accommodation",
  "group travel",
  "participants hotel",
  "conference housing",
  "event accommodation",
  "visitor lodging",
  "staff lodging",
  "site selection",
  "RFP hotel",
  "hotel tender",
  "accommodation tender",
  "group housing RFP",
  "organizer",
  "meeting planner",
  "travel agency",
  "DMC",
  "housing bureau",
  "overflow hotel",
  "official accommodation partner",
]);

export const LOCALIZED_MOTION = Object.freeze({
  fr: [
    "hébergement",
    "hôtel officiel",
    "bloc de chambres",
    "bureau de logement",
    "appel d'offres hôtel",
    "agence événementielle",
    "sélection de site",
    "hébergement délégués",
  ],
  es: [
    "alojamiento",
    "hotel oficial",
    "bloque de habitaciones",
    "licitación hotel",
    "agencia de eventos",
    "selección de sede",
    "alojamiento delegación",
  ],
  gl: ["aloxamento", "hotel oficial", "licitación hotel", "axencia de eventos"],
  de: ["Unterkunft", "Offizielles Hotel", "Zimmerkontingent", "Hotelausschreibung"],
});

export const BUYER_TYPE_QUERIES = Object.freeze({
  ASSOCIATION: [
    "site selection host city",
    "housing provider official accommodation",
    "meeting planner association secretariat",
    "future congress bidding host proposal",
    "annual rotation committee meetings",
    "conference organizer event agency RFP",
  ],
  CORPORATE: [
    "sales kickoff hotel",
    "leadership meeting accommodation",
    "integration workshop lodging",
    "dealer meeting hotel block",
    "regional training hotel",
    "client event official hotel",
  ],
  PHARMA: [
    "investigator meeting hotel",
    "advisory board lodging",
    "clinical kickoff accommodation",
    "medical training hotel block",
    "regional sales meeting housing",
    "speaker meeting hotel",
  ],
  PROJECT_CREW: [
    "temporary accommodation workforce",
    "crew hotel project",
    "contractor housing",
    "engineer accommodation",
    "staff relocation lodging",
    "commissioning team hotel",
  ],
  SPORTS: [
    "team travel hotel",
    "federation accommodation",
    "broadcast crew hotel",
    "training camp lodging",
    "officials housing",
    "youth tournament hotel block",
  ],
  UNIVERSITY: [
    "executive education housing",
    "summer program accommodation",
    "visiting cohort lodging",
    "faculty retreat hotel",
    "trustee meeting hotel",
    "academic conference housing",
  ],
  PROCUREMENT: [
    "lodging RFP",
    "hotel RFP",
    "accommodation tender",
    "group housing tender",
    "crew accommodation contract",
    "conference hotel procurement",
    "temporary accommodation procurement",
  ],
  COMPETITIVE: [
    "host hotel",
    "official hotel",
    "room block",
    "group stayed",
    "accommodated at",
  ],
});

/**
 * Build buyer-first query set for a hotel market.
 */
export function buildBuyerFirstQueryLibrary(hotel = {}) {
  const dest = hotel.destinationMarket || hotel.market || hotel.city || "";
  const feeders = hotel.feederMarkets || [];
  const langs = hotel.languages || ["en"];
  const rows = [];
  let n = 0;

  const push = (row) => {
    n += 1;
    rows.push({
      queryId: `${hotel.hotelId || hotel.slug || "h"}_q${String(n).padStart(3, "0")}`,
      hotelId: hotel.hotelId || hotel.slug,
      hotelName: hotel.hotelName || hotel.name,
      ...row,
    });
  };

  const markets = [dest, ...feeders].filter(Boolean);
  const engines = [
    "ASSOCIATION",
    "CORPORATE",
    "PHARMA",
    "PROJECT_CREW",
    "SPORTS",
    "UNIVERSITY",
    "PROCUREMENT",
    "COMPETITIVE",
  ];

  for (const market of markets) {
    const isFeeder = market !== dest;
    for (const engine of engines) {
      const templates = BUYER_TYPE_QUERIES[engine] || [];
      for (const tpl of templates.slice(0, engine === "PROCUREMENT" ? 7 : 4)) {
        push({
          engine,
          marketRole: isFeeder ? "FEEDER" : "DESTINATION",
          lodgingMarket: dest,
          originMarket: market,
          language: "en",
          queryLocalized: `${tpl} ${market}`,
          demandMotion: true,
        });
      }
    }

    for (const lang of langs) {
      if (lang === "en") continue;
      const local = LOCALIZED_MOTION[lang] || [];
      for (const term of local.slice(0, 5)) {
        push({
          engine: "MULTILINGUAL_MOTION",
          marketRole: isFeeder ? "FEEDER" : "DESTINATION",
          lodgingMarket: dest,
          originMarket: market,
          language: lang,
          queryLocalized: `${term} ${market}`,
          demandMotion: true,
        });
      }
    }

    // Competitor hotel names × lodging motion
    for (const comp of hotel.competitors || []) {
      push({
        engine: "COMPETITIVE",
        marketRole: "DESTINATION",
        lodgingMarket: dest,
        originMarket: market,
        language: "en",
        queryLocalized: `"${comp}" host hotel OR room block OR official hotel ${market}`,
        competitorHotel: comp,
        demandMotion: true,
      });
      push({
        engine: "COMPETITIVE",
        marketRole: "DESTINATION",
        lodgingMarket: dest,
        originMarket: market,
        language: "en",
        queryLocalized: `group OR congress OR meeting "${comp}" ${market}`,
        competitorHotel: comp,
        demandMotion: true,
      });
    }
  }

  return rows;
}

export function writeQueryLibraryCsv(rows = []) {
  return rows;
}
