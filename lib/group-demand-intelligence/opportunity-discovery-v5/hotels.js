/**
 * Five-hotel pilot config for Opportunity Discovery Rebuild V5.
 */

export const V5_HOTELS = Object.freeze([
  {
    hotelId: "recrPQcZg7SFARRb2",
    hotelKey: "YOTEL",
    hotelName: "YOTEL Geneva Lake",
    label: "YOTEL Geneva Lake",
    destinationMarket: "Geneva",
    market: "Geneva / Lake Geneva / La Côte",
    placeNames: ["Geneva", "Genève", "Nyon", "La Côte", "Palexpo", "Lausanne"],
    geoTokens: ["geneva", "genève", "switzerland", "palexpo", "nyon", "lausanne", "vaud"],
    feederMarkets: ["Geneva", "Lausanne", "Zurich", "Brussels", "Paris", "London"],
    languages: ["fr", "en", "de"],
    competitors: [
      "Ibis Styles Geneva Airport",
      "Hilton Geneva Hotel",
      "Mövenpick Geneva",
      "Fairmont Grand Hotel Geneva",
      "Novotel Genève Centre",
    ],
    defaultFitScore: 52,
    fitLine: "YOTEL Geneva Lake: airport / La Côte corridor overflow and group lodging.",
    serpGl: "ch",
    serpHlPrimary: "fr",
  },
  {
    hotelId: "rec2PVBDavppGpenm",
    hotelKey: "AC",
    hotelName: "AC Hotel A Coruña",
    label: "AC Hotel A Coruña",
    destinationMarket: "A Coruña",
    market: "A Coruña / Galicia",
    placeNames: ["A Coruña", "Coruña", "Galicia", "Santiago de Compostela"],
    geoTokens: ["coruña", "coruna", "galicia", "spain", "santiago"],
    feederMarkets: ["Galicia", "Madrid", "Barcelona", "Portugal", "Lisbon"],
    languages: ["es", "gl", "en"],
    competitors: [
      "Melia Maria Pita",
      "Hotel Attica21 Coruña",
      "NH Collection A Coruña",
      "Parador de Ferrol",
      "Eurostars Ciudad de la Coruña",
    ],
    defaultFitScore: 50,
    fitLine: "AC A Coruña: urban upscale association/corporate lodging.",
    serpGl: "es",
    serpHlPrimary: "es",
  },
  {
    hotelId: "recKRJjcPnb4tVDDS",
    hotelKey: "SPICE",
    hotelName: "Spice Island Beach Resort",
    label: "Spice Island Beach Resort",
    destinationMarket: "Grenada",
    market: "Grenada / Grand Anse",
    placeNames: ["Grenada", "Grand Anse", "St. George's"],
    geoTokens: ["grenada", "grand anse", "st george"],
    feederMarkets: ["Grenada", "Miami", "New York", "Toronto", "London"],
    languages: ["en"],
    competitors: [
      "Sandals Grenada",
      "Calabash Luxury Boutique Hotel",
      "Silversands Grenada",
      "Mount Cinnamon",
      "Radisson Grenada Beach Resort",
    ],
    defaultFitScore: 48,
    fitLine: "Spice Island: luxury incentive / group lodging.",
    serpGl: "us",
    serpHlPrimary: "en",
  },
  {
    hotelId: "recIwaP1etgx2g9nA",
    hotelKey: "CAMBRIDGE",
    hotelName: "Cambridge Beaches Resort & Spa",
    label: "Cambridge Beaches Resort & Spa",
    destinationMarket: "Bermuda",
    market: "Bermuda",
    placeNames: ["Bermuda", "Somerset", "Hamilton"],
    geoTokens: ["bermuda", "somerset", "hamilton"],
    feederMarkets: ["Bermuda", "New York", "Boston", "Toronto", "London"],
    languages: ["en"],
    competitors: [
      "Rosewood Bermuda",
      "Hamilton Princess",
      "Fairmont Southampton",
      "The Loren at Pink Beach",
      "Azura Bermuda",
    ],
    defaultFitScore: 48,
    fitLine: "Cambridge Beaches: cottage resort incentive / group lodging.",
    serpGl: "us",
    serpHlPrimary: "en",
  },
  {
    hotelId: "recGkME49yYuxQl0u",
    hotelKey: "NOW_NOW",
    hotelName: "NOW NOW NoHo",
    label: "NOW NOW NOHO",
    destinationMarket: "New York",
    market: "New York / NoHo",
    placeNames: ["New York", "NoHo", "Manhattan", "NYC"],
    geoTokens: ["new york", "nyc", "manhattan", "noho"],
    feederMarkets: ["NYC", "Boston", "Washington", "Chicago", "California"],
    languages: ["en"],
    competitors: [
      "The Standard East Village",
      "Public Hotel",
      "Freehand New York",
      "The Hoxton Williamsburg",
      "Arlo NoMad",
    ],
    defaultFitScore: 50,
    fitLine: "NOW NOW NoHo: boutique urban group lodging.",
    serpGl: "us",
    serpHlPrimary: "en",
  },
  {
    hotelId: "recUOyzOXn2Zdp98I",
    hotelKey: "RADISSON",
    hotelName: "Radisson Hotel Santo Domingo",
    label: "Radisson Hotel Santo Domingo",
    destinationMarket: "Santo Domingo",
    market: "Santo Domingo / Naco–Tiradentes",
    country: "Dominican Republic",
    placeNames: ["Santo Domingo", "Naco", "Tiradentes", "Piantini", "Distrito Nacional"],
    geoTokens: ["santo domingo", "naco", "tiradentes", "piantini", "dominican"],
    feederMarkets: ["Distrito Nacional", "Piantini", "Caribbean regional", "US Hispanic corporate"],
    languages: ["es", "en"],
    competitors: [],
    defaultFitScore: 50,
    fitLine: "Radisson Santo Domingo: Naco upper-upscale urban / meetings lodging.",
    serpGl: "do",
    serpHlPrimary: "es",
  },
]);

/**
 * Prioritize buyer-first queries for a bounded controlled run.
 * Prefer procurement / lodging motion / competitive / association site-selection.
 */
export function selectPriorityQueries(allQueries = [], { maxQueries = 12 } = {}) {
  const rank = (q) => {
    let s = 0;
    if (q.engine === "PROCUREMENT") s += 100;
    if (q.engine === "COMPETITIVE") s += 90;
    if (q.engine === "ASSOCIATION") s += 70;
    if (q.engine === "PHARMA") s += 60;
    if (q.engine === "PROJECT_CREW") s += 55;
    if (q.engine === "CORPORATE") s += 50;
    if (q.engine === "SPORTS") s += 45;
    if (q.engine === "UNIVERSITY") s += 40;
    if (q.engine === "MULTILINGUAL_MOTION") s += 65;
    if (q.marketRole === "DESTINATION") s += 10;
    if (/room block|housing|accommodation|hébergement|alojamiento|RFP|tender|host hotel/i.test(q.queryLocalized || "")) {
      s += 25;
    }
    return s;
  };
  const sorted = [...allQueries].sort((a, b) => rank(b) - rank(a));
  // Guaranteed mix: procurement + competitive + association/motion (not procurement-only)
  const picked = [];
  const take = (pred, n) => {
    for (const q of sorted) {
      if (picked.length >= maxQueries) break;
      if (picked.includes(q)) continue;
      if (pred(q)) {
        picked.push(q);
        if (picked.filter(pred).length >= n) break;
      }
    }
  };
  take((q) => q.engine === "PROCUREMENT" && q.marketRole === "DESTINATION", 3);
  take((q) => q.engine === "COMPETITIVE" && q.competitorHotel, 3);
  take((q) => q.engine === "ASSOCIATION" || q.engine === "MULTILINGUAL_MOTION", 2);
  take((q) => q.engine === "PHARMA" || q.engine === "PROJECT_CREW", 2);
  take((q) => q.marketRole === "FEEDER", 2);
  for (const q of sorted) {
    if (picked.length >= maxQueries) break;
    if (!picked.includes(q)) picked.push(q);
  }
  return picked.slice(0, maxQueries);
}
