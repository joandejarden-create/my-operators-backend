/**

 * Recommended CALA build sequence after Colombia (country-level tracking).

 * Mexico and Brazil use nextBuildMarket for the active market within the country row.

 * Caribbean island builds follow Jamaica (sequence 10–15); Brazil deferred to 16.

 */



/** @type {Record<string, object>} */

export const POST_COLOMBIA_BUILD_SEQUENCE = {

  "Puerto Rico": {

    recommendedBuildSequence: 1,

    nextBuildMarket: "Completed / Existing",

    buildApproachNotes: "Intelligence Ready proof case. Maintain and QA; no new countrywide build required.",

    firstPassTargetDescription: "Countrywide island build complete (60+ DA, 25+ TI).",

  },

  "Dominican Republic": {

    recommendedBuildSequence: 2,

    nextBuildMarket: "Completed / Deal Ready",

    buildApproachNotes:

      "Mature corridor pass complete with secondary-coasts deepening (Miches, Barahona, Jarabacoa). Maintain corridor QA across Punta Cana, Santo Domingo, and secondary coasts.",

    firstPassTargetDescription: "First-pass corridor build 50–70 DA; mature 100–140 DA.",

  },

  Colombia: {

    recommendedBuildSequence: 3,

    nextBuildMarket: "Completed / Deal Ready",

    buildApproachNotes:

      "All initial markets built and imported (8 cities). TI mature pass adds 2 nodes per market. Maintain market-by-market QA.",

    firstPassTargetDescription: "Per-market first pass ~87 DA nationally; TI mature pass 32 nodes.",

  },

  Mexico: {

    recommendedBuildSequence: 4,

    nextBuildMarket: "Completed / Secondary Markets Started",

    buildApproachNotes:

      "Cancún / Riviera Maya plus Tier-1 initial markets complete (CDMX, Los Cabos, Guadalajara, Monterrey, PV/Nayarit, Mérida). Secondary markets Oaxaca, Querétaro, Guanajuato, Mazatlán mature pass imported.",

    firstPassTargetDescription:

      "Tier-1 markets: 50–80 DA each; Cancún mature pass separate.",

  },

  Panama: {

    recommendedBuildSequence: 5,

    nextBuildMarket: "Completed / Deal Ready",

    buildApproachNotes:

      "Countrywide corridor first pass complete. Maintain Panama City, canal, and leisure corridor QA.",

    firstPassTargetDescription: "First pass 40–60 DA, 15–25 TI, 55–85 total.",

  },

  "Costa Rica": {

    recommendedBuildSequence: 6,

    nextBuildMarket: "Completed / Deal Ready",

    buildApproachNotes:

      "Distributed eco/adventure/leisure demand across Guanacaste, Central Pacific, Arenal, San José, and Caribbean — not a single resort corridor.",

    firstPassTargetDescription: "First pass 60–90 DA, 20–35 TI, 80–125 total.",

  },

  Peru: {

    recommendedBuildSequence: 7,

    nextBuildMarket: "Completed / Deal Ready",

    buildApproachNotes:

      "Lima/Cusco first pass complete. Arequipa and Paracas mature pass imported.",

    firstPassTargetDescription: "Lima 35–50 DA + Cusco 20–35 DA; 70–110 total first pass.",

  },

  Chile: {

    recommendedBuildSequence: 8,

    nextBuildMarket: "Completed / Deal Ready",

    buildApproachNotes: "Santiago first pass complete. Valparaíso / Patagonia mature pass imported.",

    firstPassTargetDescription: "First pass 40–60 DA, 10–20 TI, 50–80 total.",

  },

  Jamaica: {

    recommendedBuildSequence: 9,

    nextBuildMarket: "Completed / Deal Ready",

    buildApproachNotes:

      "Caribbean resort corridor first pass complete. Maintain corridor QA; mature pass can deepen secondary coasts.",

    firstPassTargetDescription: "First pass 50–80 DA, 15–25 TI, 65–105 total.",

  },

  Bahamas: {

    recommendedBuildSequence: 10,

    nextBuildMarket: "Completed / Deal Ready",

    buildApproachNotes:

      "Island countrywide first pass complete. Maintain corridor QA; mature pass can deepen Out Islands.",

    firstPassTargetDescription: "First pass 50–75 DA, 15–25 TI, 65–100 total.",

  },

  Aruba: {

    recommendedBuildSequence: 11,

    nextBuildMarket: "Completed / Deal Ready",

    buildApproachNotes:

      "Compact island first pass complete. Maintain corridor QA; mature pass can deepen San Nicolas and secondary coast nodes.",

    firstPassTargetDescription: "First pass 35–50 DA, 10–18 TI, 45–68 total.",

  },

  Curaçao: {

    recommendedBuildSequence: 12,

    nextBuildMarket: "Completed / Deal Ready",

    buildApproachNotes:

      "Compact island first pass complete. Maintain Willemstad/cruise and west-coast resort corridor QA; mature pass can deepen Banda Abou nodes.",

    firstPassTargetDescription: "First pass 35–50 DA, 10–18 TI, 45–68 total.",

  },

  Barbados: {

    recommendedBuildSequence: 13,

    nextBuildMarket: "Completed / Deal Ready",

    buildApproachNotes:

      "Island countrywide first pass complete. Maintain Bridgetown, west-coast, and south-coast corridor QA.",

    firstPassTargetDescription: "First pass 40–60 DA, 12–20 TI, 55–80 total.",

  },

  "Cayman Islands": {

    recommendedBuildSequence: 14,

    nextBuildMarket: "Completed / Deal Ready",

    buildApproachNotes:

      "Grand Cayman–heavy island first pass complete. Maintain Seven Mile Beach and George Town corridor QA; deepen Brac/Little Cayman as needed.",

    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",

  },

  "Turks & Caicos": {

    recommendedBuildSequence: 15,

    nextBuildMarket: "Completed / Deal Ready",

    buildApproachNotes:

      "Providenciales–heavy island first pass complete. Maintain Grace Bay corridor QA; Grand Turk cruise/civic secondary.",

    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",

  },

  "Saint Lucia": {
    recommendedBuildSequence: 17,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Castries cruise/civic anchor; Rodney Bay resort corridor; Pitons/Soufrière leisure; Vieux Fort airport south. Maintain corridor QA; mature pass can deepen secondary nodes.",
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },

  "Antigua and Barbuda": {
    recommendedBuildSequence: 18,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "St. John's cruise/airport hub; Nelson's Dockyard sailing; Dickenson resort strip; Barbuda eco secondary. Maintain corridor QA; mature pass can deepen secondary nodes.",
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },

  "Grenada": {
    recommendedBuildSequence: 19,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "St. George's cruise/civic; Grand Anse resort benchmark; south and north coast eco/leisure secondary. Maintain corridor QA; mature pass can deepen secondary nodes.",
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },

  "Saint Vincent and the Grenadines": {
    recommendedBuildSequence: 20,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Kingstown cruise/civic; Grenadines yacht/leisure islands; Argyle airport south gateway. Maintain corridor QA; mature pass can deepen secondary nodes.",
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },

  "Dominica": {
    recommendedBuildSequence: 21,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Nature-island eco/adventure positioning; Roseau cruise/civic; Portsmouth north gateway. Maintain corridor QA; mature pass can deepen secondary nodes.",
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },

  "Saint Kitts and Nevis": {
    recommendedBuildSequence: 22,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Basseterre cruise/civic; Frigate Bay resort strip; Nevis heritage/luxury secondary island. Maintain corridor QA; mature pass can deepen secondary nodes.",
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },

  "Trinidad and Tobago": {
    recommendedBuildSequence: 23,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Port of Spain urban/corporate and carnival; East-West Corridor suburban; Tobago leisure secondary. Maintain corridor QA; mature pass can deepen secondary nodes.",
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },

  "British Virgin Islands": {
    recommendedBuildSequence: 24,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Tortola Road Town civic and yacht hub; Virgin Gorda resort; Jost Van Dyke and Anegada leisure satellites. Maintain corridor QA; mature pass can deepen secondary nodes.",
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },

  "Cuba": {
    recommendedBuildSequence: 25,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Havana civic/cultural hub; Varadero beach resort corridor; Trinidad heritage; Santiago east gateway; emerging cay and nature nodes. Island/countrywide first pass complete.",
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },

  "Haiti": {
    recommendedBuildSequence: 26,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Port-au-Prince civic and business hub; Cap-Haïtien north heritage and cruise; Jacmel arts coast; Labadie and Citadelle leisure nodes. Island/countrywide first pass complete.",
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },

  "U.S. Virgin Islands": {
    recommendedBuildSequence: 27,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "St. Thomas cruise and duty-free hub; St. Croix historic and industrial mix; St. John national park leisure; Water Island and outer cays. Island/countrywide first pass complete.",
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },

  "Martinique": {
    recommendedBuildSequence: 28,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Fort-de-France civic and cruise hub; south resort coast (Les Trois-Îlets, Diamant); north Atlantic surf and rum heritage; Mount Pelée volcano corridor. Island/countrywide first pass complete.",
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },

  "Guadeloupe": {
    recommendedBuildSequence: 29,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Pointe-à-Pitre cruise and commercial hub; Grande-Terre beach resort east coast; Basse-Terre rainforest and volcano west; Marie-Galante and Les Saintes satellites. Island/countrywide first pass complete.",
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },

  "Bonaire": {
    recommendedBuildSequence: 30,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Kralendijk cruise and dive hub; Rincon heritage village; Washington Slagbaai national park; Lac Bay windsurf and salt flats eco-tourism. Island/countrywide first pass complete.",
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },

  Belize: {
    recommendedBuildSequence: 31,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Belize City cruise/airport hub; Ambergris Caye and Caye Caulker reef leisure; Placencia peninsula; San Ignacio Maya interior; Great Blue Hole and Caracol secondary nodes.",
    firstPassTargetDescription: "First pass 40–60 DA, 12–20 TI, 55–80 total.",
  },

  Guatemala: {
    recommendedBuildSequence: 32,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Guatemala City corporate/convention; Antigua UNESCO heritage; Lake Atitlán leisure towns; Petén / Tikal archaeology gateway.",
    firstPassTargetDescription: "First pass 40–60 DA, 12–20 TI, 55–80 total.",
  },

  Honduras: {
    recommendedBuildSequence: 33,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Roatán cruise and dive resort island; Tegucigalpa and San Pedro Sula urban corridors; Copán Maya heritage; La Ceiba Caribbean gateway.",
    firstPassTargetDescription: "First pass 40–60 DA, 12–20 TI, 55–80 total.",
  },

  Nicaragua: {
    recommendedBuildSequence: 34,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Managua metro civic/business; Granada colonial lake tourism; San Juan del Sur Pacific surf; León heritage; Ometepe island eco.",
    firstPassTargetDescription: "First pass 40–60 DA, 12–20 TI, 55–80 total.",
  },

  "El Salvador": {
    recommendedBuildSequence: 35,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "San Salvador corporate and civic hub; La Libertad surf coast; Santa Ana volcano/coffee highlands; Suchitoto colonial; Joya de Cerén UNESCO.",
    firstPassTargetDescription: "First pass 35–55 DA, 10–18 TI, 45–70 total.",
  },

  Argentina: {
    recommendedBuildSequence: 36,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Countrywide first pass complete. Mendoza, Bariloche, and Iguazú regional depth mature pass imported.",
    firstPassTargetDescription: "First pass 45–65 DA, 15–25 TI, 60–90 total.",
  },

  Ecuador: {
    recommendedBuildSequence: 37,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Quito UNESCO highland capital; Guayaquil port and business; Galápagos islands; Cuenca heritage; Baños/Otavalo secondary.",
    firstPassTargetDescription: "First pass 40–60 DA, 12–20 TI, 55–80 total.",
  },

  Uruguay: {
    recommendedBuildSequence: 38,
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: "Montevideo civic and business hub; Punta del Este resort and convention; Colonia UNESCO; Piriápolis/Carmelo secondary coast.",
    firstPassTargetDescription: "First pass 35–55 DA, 10–18 TI, 45–70 total.",
  },

  Brazil: {

    recommendedBuildSequence: 16,

    nextBuildMarket: "São Paulo / Rio de Janeiro (deferred)",

    buildApproachNotes:

      "Deferred until Caribbean island layer is stronger. Do not build countrywide. Validate existing travel infrastructure first; add only gaps. Build São Paulo and Rio only, then pause.",

    firstPassTargetDescription:

      "São Paulo 40–60 DA + Rio 40–60 DA first pass; validate TI before expanding.",

  },

};



/**

 * @param {string} country

 */

export function getPostColombiaSequenceMeta(country) {

  return POST_COLOMBIA_BUILD_SEQUENCE[country] || null;

}



/**

 * Ordered list of countries in the post-Colombia recommended sequence.

 */

export function listPostColombiaSequenceCountries() {

  return Object.entries(POST_COLOMBIA_BUILD_SEQUENCE)

    .map(([country, meta]) => ({ country, ...meta }))

    .sort((a, b) => a.recommendedBuildSequence - b.recommendedBuildSequence);

}



/**

 * @param {object} config

 */

export function applySequenceMetaToConfig(config) {

  const meta = config?.country ? getPostColombiaSequenceMeta(config.country) : null;

  if (!meta) return config;

  return {

    ...config,

    recommendedBuildSequence: meta.recommendedBuildSequence,

    nextBuildMarket: meta.nextBuildMarket,

    buildApproachNotes: meta.buildApproachNotes,

    firstPassTargetDescription: meta.firstPassTargetDescription,

  };

}


