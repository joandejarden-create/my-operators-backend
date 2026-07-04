/**
 * CALA Radar Buildout — country configuration registry.
 */

import { BUILD_STRATEGY_TYPES } from "./country-build-strategies.js";
import { getPostColombiaSequenceMeta } from "./post-colombia-build-sequence.js";

const S = BUILD_STRATEGY_TYPES;

function cfg(entry) {
  return {
    region: "Caribbean",
    priorityTier: "Future",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [],
    submarkets: [],
    initialMarkets: [],
    marketSubmarkets: {},
    nextBuildMarket: "",
    targets: null,
    manualStatus: null,
    notes: "",
    buildApproachNotes: "",
    firstPassTargetDescription: "",
    recommendedBuildSequence: null,
    ...entry,
  };
}

function attachSequenceMeta(configs) {
  for (const country of Object.keys(configs)) {
    const seq = getPostColombiaSequenceMeta(country);
    if (seq) Object.assign(configs[country], seq);
  }
  return configs;
}

/** @type {Record<string, object>} */
export const COUNTRY_CONFIGS = attachSequenceMeta({
  "Puerto Rico": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 1",
    primaryHotelDemandProfile: "Mixed-Use / Growth",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Urban / Corporate",
      "Mixed-Use / Growth",
      "Airport / Transit",
    ],
    submarkets: [
      "San Juan Metro",
      "North Coast Resort Corridor",
      "East Coast / Island Access",
      "Vieques / Culebra",
      "South Coast Regional City",
      "West Coast / University & Surf",
      "Northwest Air & Leisure Corridor",
      "Southwest Nature & Beach Corridor",
      "Central / Inland",
      "Other",
    ],
    targets: {
      demandAnchors: { firstPass: { min: 30, max: 50 }, mature: { min: 40, max: 80 } },
      travelInfrastructure: { firstPass: { min: 10, max: 20 }, mature: { min: 15, max: 30 } },
      totalRadarPoints: { firstPass: { min: 40, max: 70 }, mature: { min: 55, max: 110 } },
    },
    manualStatus: "Intelligence Ready",
    notes: "Full-country proof case — Intelligence Ready as of 2026-06.",
  }),

  "Dominican Republic": cfg({
    region: "Caribbean",
    buildStrategy: S.CORRIDOR_BASED,
    priorityTier: "Tier 1",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Urban / Corporate",
      "Cruise / Port",
      "Airport / Transit",
      "Mixed-Use / Growth",
    ],
    submarkets: [
      "Punta Cana / Bávaro / Cap Cana",
      "Santo Domingo Metro",
      "Puerto Plata / Sosúa / Cabarete",
      "La Romana / Bayahibe",
      "Samaná / Las Terrenas",
      "Santiago / Cibao",
      "Miches / Costa Esmeralda",
      "Barahona / Pedernales",
      "Boca Chica / Juan Dolio",
      "Jarabacoa / Constanza",
      "Other",
    ],
    targets: {
      demandAnchors: { firstPass: { min: 50, max: 70 }, mature: { min: 100, max: 140 } },
      travelInfrastructure: { firstPass: { min: 15, max: 25 }, mature: { min: 25, max: 45 } },
      totalRadarPoints: { firstPass: { min: 65, max: 95 }, mature: { min: 125, max: 185 } },
    },
    manualStatus: "Market Ready",
    notes: "Tier-1 corridor build — Market Ready; mature coverage pass ongoing.",
  }),

  Colombia: cfg({
    region: "South America",
    buildStrategy: S.MARKET_BY_MARKET,
    priorityTier: "Tier 1",
    primaryHotelDemandProfile: "Urban / Corporate",
    primaryHotelDemandProfiles: [
      "Urban / Corporate",
      "Resort / Leisure",
      "Mixed-Use / Growth",
      "Government / Institutional",
      "Medical / Education",
    ],
    initialMarkets: [
      "Cartagena",
      "Bogotá",
      "Medellín",
      "Barranquilla",
      "Cali",
      "Santa Marta",
      "Coffee Region / Pereira",
      "San Andrés",
      "Other",
    ],
    submarkets: [
      "Cartagena",
      "Bogotá",
      "Medellín",
      "Barranquilla",
      "Cali",
      "Santa Marta",
      "Coffee Region / Pereira",
      "San Andrés",
      "Other",
    ],
    targets: {
      demandAnchors: { firstPass: { min: 70, max: 100 }, mature: { min: 120, max: 180 } },
      travelInfrastructure: { firstPass: { min: 20, max: 35 }, mature: { min: 35, max: 55 } },
      totalRadarPoints: { firstPass: { min: 90, max: 135 }, mature: { min: 155, max: 235 } },
    },
    notes: "Market-by-market build in progress — 77 demand anchors imported 2026-06.",
  }),

  Mexico: cfg({
    region: "North America",
    buildStrategy: S.MARKET_BY_MARKET,
    priorityTier: "Tier 1",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Urban / Corporate",
      "Mixed-Use / Growth",
      "Airport / Transit",
      "Government / Institutional",
      "Medical / Education",
    ],
    initialMarkets: [
      "Cancún / Riviera Maya",
      "Mexico City",
      "Los Cabos",
      "Guadalajara",
      "Monterrey",
      "Puerto Vallarta / Riviera Nayarit",
      "Mérida / Yucatán",
      "Other",
    ],
    marketSubmarkets: {
      "Cancún / Riviera Maya": [
        "Cancún Hotel Zone",
        "Cancún Airport Corridor",
        "Puerto Juárez / Isla Mujeres Ferry Corridor",
        "Puerto Cancún",
        "Costa Mujeres / Playa Mujeres",
        "Riviera Maya / Playa del Carmen",
        "Mayakoba",
        "Tulum",
        "Akumal / Puerto Aventuras",
        "Cozumel",
        "Isla Mujeres",
        "Other",
      ],
      "Mexico City": [
        "Polanco",
        "Reforma / Juárez",
        "Santa Fe",
        "Condesa / Roma",
        "Centro Histórico",
        "Insurgentes / WTC",
        "Airport Corridor",
        "Coyoacán / San Ángel",
        "Other",
      ],
    },
    submarkets: [
      "Cancún Hotel Zone",
      "Cancún Airport Corridor",
      "Puerto Juárez / Isla Mujeres Ferry Corridor",
      "Puerto Cancún",
      "Costa Mujeres / Playa Mujeres",
      "Riviera Maya / Playa del Carmen",
      "Mayakoba",
      "Tulum",
      "Akumal / Puerto Aventuras",
      "Cozumel",
      "Isla Mujeres",
      "Mexico City",
      "Guadalajara",
      "Monterrey",
      "Los Cabos",
      "Puerto Vallarta / Riviera Nayarit",
      "Mérida / Yucatán",
      "Oaxaca",
      "Puebla",
      "Acapulco",
      "Querétaro",
      "Morelia",
      "Veracruz",
      "Mazatlán",
      "Guanajuato",
      "Other",
    ],
    targets: {
      demandAnchors: { firstPass: { min: 50, max: 70 }, mature: { min: 90, max: 130 } },
      travelInfrastructure: { firstPass: { min: 15, max: 25 }, mature: { min: 25, max: 40 } },
      totalRadarPoints: { firstPass: { min: 65, max: 95 }, mature: { min: 115, max: 170 } },
    },
    marketTargets: {
      "Cancún / Riviera Maya": {
        demandAnchors: { firstPass: { min: 50, max: 70 }, mature: { min: 90, max: 130 } },
        travelInfrastructure: { firstPass: { min: 15, max: 25 }, mature: { min: 25, max: 40 } },
        totalRadarPoints: { firstPass: { min: 65, max: 95 }, mature: { min: 115, max: 170 } },
      },
      "Mexico City": {
        demandAnchors: { firstPass: { min: 50, max: 80 }, mature: { min: 80, max: 120 } },
        travelInfrastructure: { firstPass: { min: 10, max: 20 }, mature: { min: 15, max: 30 } },
        totalRadarPoints: { firstPass: { min: 60, max: 100 }, mature: { min: 95, max: 150 } },
      },
    },
    notes: "Next market: Cancún / Riviera Maya. Mexico City follows as separate urban build.",
  }),

  Panama: cfg({
    region: "Central America",
    buildStrategy: S.CORRIDOR_BASED,
    priorityTier: "Tier 1",
    primaryHotelDemandProfile: "Urban / Corporate",
    primaryHotelDemandProfiles: [
      "Urban / Corporate",
      "Airport / Transit",
      "Cruise / Port",
      "Industrial / Logistics",
      "Government / Institutional",
      "Mixed-Use / Growth",
      "Resort / Leisure",
    ],
    submarkets: [
      "Panama City",
      "Tocumen / Airport Corridor",
      "Canal / Logistics Corridor",
      "Casco Viejo / Waterfront",
      "Costa del Este",
      "Pacific Beaches",
      "Boquete / Highlands",
      "Bocas del Toro",
      "Other",
    ],
    targets: {
      demandAnchors: { firstPass: { min: 40, max: 60 }, mature: { min: 70, max: 100 } },
      travelInfrastructure: { firstPass: { min: 15, max: 25 }, mature: { min: 25, max: 40 } },
      totalRadarPoints: { firstPass: { min: 55, max: 85 }, mature: { min: 95, max: 140 } },
    },
    notes: "Panama City–heavy countrywide build with canal, airport, and financial district anchors.",
  }),

  "Costa Rica": cfg({
    region: "Central America",
    buildStrategy: S.CORRIDOR_BASED,
    priorityTier: "Tier 1",
    primaryHotelDemandProfile: "Nature / Eco-Tourism",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Nature / Eco-Tourism",
      "Urban / Corporate",
      "Airport / Transit",
      "Mixed-Use / Growth",
    ],
    submarkets: [
      "San José Metro",
      "Guanacaste / Papagayo",
      "Tamarindo / North Pacific",
      "Jacó / Herradura",
      "Manuel Antonio / Central Pacific",
      "Arenal / La Fortuna",
      "Caribbean Coast",
      "Monteverde",
      "Other",
    ],
    targets: {
      demandAnchors: { firstPass: { min: 60, max: 90 }, mature: { min: 100, max: 150 } },
      travelInfrastructure: { firstPass: { min: 20, max: 35 }, mature: { min: 30, max: 50 } },
      totalRadarPoints: { firstPass: { min: 80, max: 125 }, mature: { min: 130, max: 200 } },
    },
    notes: "Corridor-based eco/leisure country — distributed demand, not one resort strip.",
  }),

  Peru: cfg({
    region: "South America",
    buildStrategy: S.MARKET_BY_MARKET,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Urban / Corporate",
    primaryHotelDemandProfiles: [
      "Urban / Corporate",
      "Resort / Leisure",
      "Airport / Transit",
      "Government / Institutional",
      "Mixed-Use / Growth",
    ],
    initialMarkets: ["Lima", "Cusco / Sacred Valley", "Arequipa", "Paracas", "Other"],
    marketSubmarkets: {
      Lima: [
        "Miraflores",
        "San Isidro",
        "Barranco",
        "Lima Historic Center",
        "Jorge Chávez Airport Corridor",
        "Surco / Convention / Business Corridor",
        "Callao / Port",
        "Other",
      ],
      "Cusco / Sacred Valley": [
        "Cusco Historic Center",
        "Sacred Valley",
        "Machu Picchu Access",
        "Urubamba",
        "Ollantaytambo",
        "Other",
      ],
    },
    submarkets: [
      "Miraflores",
      "San Isidro",
      "Barranco",
      "Lima Historic Center",
      "Jorge Chávez Airport Corridor",
      "Surco / Convention / Business Corridor",
      "Callao / Port",
      "Cusco Historic Center",
      "Sacred Valley",
      "Machu Picchu Access",
      "Urubamba",
      "Ollantaytambo",
      "Arequipa",
      "Paracas",
      "Trujillo",
      "Other",
    ],
    targets: {
      demandAnchors: { firstPass: { min: 55, max: 85 }, mature: { min: 90, max: 130 } },
      travelInfrastructure: { firstPass: { min: 15, max: 25 }, mature: { min: 25, max: 40 } },
      totalRadarPoints: { firstPass: { min: 70, max: 110 }, mature: { min: 115, max: 170 } },
    },
    marketTargets: {
      Lima: {
        demandAnchors: { firstPass: { min: 35, max: 50 } },
      },
      "Cusco / Sacred Valley": {
        demandAnchors: { firstPass: { min: 20, max: 35 } },
      },
    },
    notes: "Lima urban/corporate + Cusco heritage split build.",
  }),

  Chile: cfg({
    region: "South America",
    buildStrategy: S.MARKET_BY_MARKET,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Urban / Corporate",
    primaryHotelDemandProfiles: [
      "Urban / Corporate",
      "Government / Institutional",
      "Medical / Education",
      "Airport / Transit",
      "Mixed-Use / Growth",
    ],
    initialMarkets: ["Santiago", "Valparaíso / Viña del Mar", "Atacama", "Patagonia", "Other"],
    marketSubmarkets: {
      Santiago: [
        "Las Condes",
        "Providencia",
        "Vitacura",
        "Santiago Centro",
        "Airport Corridor",
        "Convention / Events Corridor",
        "Costanera / Financial District",
        "El Golf / Sanhattan",
        "Parque Arauco / Nueva Las Condes",
        "Other",
      ],
    },
    submarkets: [
      "Las Condes",
      "Providencia",
      "Vitacura",
      "Santiago Centro",
      "Airport Corridor",
      "Convention / Events Corridor",
      "Costanera / Financial District",
      "El Golf / Sanhattan",
      "Parque Arauco / Nueva Las Condes",
      "Valparaíso / Viña del Mar",
      "Atacama",
      "Patagonia Lakes",
      "Other",
    ],
    nextBuildMarket: "Completed / Tier 1 Markets",
    marketTargets: {
      Santiago: {
        demandAnchors: { firstPass: { min: 40, max: 60 } },
        travelInfrastructure: { firstPass: { min: 10, max: 20 } },
        totalRadarPoints: { firstPass: { min: 50, max: 80 } },
      },
    },
    targets: {
      demandAnchors: { firstPass: { min: 40, max: 60 }, mature: { min: 70, max: 100 } },
      travelInfrastructure: { firstPass: { min: 10, max: 20 }, mature: { min: 15, max: 30 } },
      totalRadarPoints: { firstPass: { min: 50, max: 80 }, mature: { min: 85, max: 130 } },
    },
    notes: "Santiago-first corporate/urban benchmark.",
  }),

  Jamaica: cfg({
    region: "Caribbean",
    buildStrategy: S.CORRIDOR_BASED,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Urban / Corporate",
      "Nature / Eco-Tourism",
    ],
    submarkets: [
      "Montego Bay",
      "Ocho Rios",
      "Negril",
      "Kingston",
      "Port Antonio",
      "South Coast",
      "Falmouth",
      "Other",
    ],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 50, max: 80 }, mature: { min: 80, max: 120 } },
      travelInfrastructure: { firstPass: { min: 15, max: 25 }, mature: { min: 25, max: 40 } },
      totalRadarPoints: { firstPass: { min: 65, max: 105 }, mature: { min: 105, max: 160 } },
    },
    notes: "Caribbean resort corridor comparison to PR, DR, Costa Rica, and Cancún.",
  }),

  Brazil: cfg({
    region: "South America",
    buildStrategy: S.MARKET_BY_MARKET,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Urban / Corporate",
    primaryHotelDemandProfiles: [
      "Urban / Corporate",
      "Resort / Leisure",
      "Airport / Transit",
      "Government / Institutional",
      "Mixed-Use / Growth",
    ],
    initialMarkets: ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Recife", "Florianópolis", "Other"],
    marketSubmarkets: {
      "São Paulo": [
        "Paulista / Jardins",
        "Faria Lima / Itaim Bibi",
        "Vila Olímpia / Berrini",
        "Centro",
        "Guarulhos / Airport Corridor",
        "Expo / Convention Corridor",
        "Other",
      ],
      "Rio de Janeiro": [
        "Copacabana",
        "Ipanema / Leblon",
        "Barra da Tijuca",
        "Centro / Porto Maravilha",
        "Santos Dumont / Airport Corridor",
        "Galeão / Airport Corridor",
        "Other",
      ],
    },
    submarkets: [
      "São Paulo",
      "Rio de Janeiro",
      "Brasília",
      "Salvador",
      "Recife",
      "Florianópolis",
      "Curitiba",
      "Fortaleza",
      "Belo Horizonte",
      "Porto Alegre",
      "Natal",
      "Manaus",
      "Other",
    ],
    targets: {
      demandAnchors: { firstPass: { min: 80, max: 120 }, mature: { min: 150, max: 250 } },
      travelInfrastructure: { firstPass: { min: 0, max: 15 }, mature: { min: 20, max: 40 } },
      totalRadarPoints: { firstPass: { min: 90, max: 140 }, mature: { min: 170, max: 290 } },
    },
    marketTargets: {
      "São Paulo": { demandAnchors: { firstPass: { min: 40, max: 60 } } },
      "Rio de Janeiro": { demandAnchors: { firstPass: { min: 40, max: 60 } } },
    },
    notes: "São Paulo + Rio only first pass. Validate travel infrastructure; do not overbuild.",
  }),

  Argentina: cfg({
    region: "South America",
    buildStrategy: S.CORRIDOR_BASED,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Urban / Corporate",
    initialMarkets: ["Buenos Aires", "Mendoza", "Bariloche", "Córdoba", "Other"],
    submarkets: [
      "Buenos Aires",
      "Mendoza",
      "Bariloche",
      "Córdoba",
      "Puerto Iguazú",
      "Mar del Plata",
      "Ushuaia",
      "Salta",
      "Other",
    ],
    targets: {
      demandAnchors: { firstPass: { min: 45, max: 65 }, mature: { min: 80, max: 120 } },
      travelInfrastructure: { firstPass: { min: 15, max: 25 }, mature: { min: 25, max: 40 } },
      totalRadarPoints: { firstPass: { min: 60, max: 90 }, mature: { min: 105, max: 160 } },
    },
  }),

  Bahamas: cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Mixed-Use / Growth",
      "Government / Institutional",
    ],
    submarkets: [
      "Nassau / New Providence",
      "Paradise Island",
      "Cable Beach / Baha Mar",
      "Grand Bahama / Freeport",
      "Exuma",
      "Eleuthera / Harbour Island",
      "Abaco",
      "Bimini",
      "Other",
    ],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 50, max: 75 }, mature: { min: 80, max: 120 } },
      travelInfrastructure: { firstPass: { min: 15, max: 25 }, mature: { min: 20, max: 35 } },
      totalRadarPoints: { firstPass: { min: 65, max: 100 }, mature: { min: 100, max: 155 } },
    },
    notes: "Island countrywide build: Nassau hub + Out Islands resort corridors.",
  }),
  Aruba: cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Mixed-Use / Growth",
      "Urban / Corporate",
    ],
    submarkets: [
      "Palm Beach / High-Rise Hotel Area",
      "Eagle Beach / Low-Rise Hotel Area",
      "Oranjestad / Cruise Port",
      "Noord",
      "San Nicolas / Baby Beach",
      "Airport Corridor",
      "Arashi / Malmok",
      "Other",
    ],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 50 }, mature: { min: 55, max: 85 } },
      travelInfrastructure: { firstPass: { min: 10, max: 18 }, mature: { min: 15, max: 25 } },
      totalRadarPoints: { firstPass: { min: 45, max: 68 }, mature: { min: 70, max: 110 } },
    },
    notes: "Compact island build: Palm Beach, Eagle Beach, Oranjestad cruise, San Nicolas, airport corridor.",
  }),
  Curaçao: cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Mixed-Use / Growth",
      "Urban / Corporate",
    ],
    submarkets: [
      "Willemstad / Punda-Otrobanda",
      "Mambo Beach / Seaquarium",
      "Jan Thiel",
      "Piscadera / Blue Bay",
      "Airport Corridor",
      "Spanish Water / Caracasbaai",
      "Westpunt / Banda Abou",
      "Port / Industrial Corridor",
      "Other",
    ],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 50 }, mature: { min: 55, max: 85 } },
      travelInfrastructure: { firstPass: { min: 10, max: 18 }, mature: { min: 15, max: 25 } },
      totalRadarPoints: { firstPass: { min: 45, max: 68 }, mature: { min: 70, max: 110 } },
    },
    notes: "Compact island build: Willemstad UNESCO/cruise, Mambo/Jan Thiel resorts, Blue Bay, Spanish Water, Westpunt.",
  }),
  Barbados: cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Urban / Corporate",
      "Mixed-Use / Growth",
    ],
    submarkets: ["Bridgetown", "West Coast", "South Coast", "Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 40, max: 60 }, mature: { min: 60, max: 90 } },
      travelInfrastructure: { firstPass: { min: 12, max: 20 }, mature: { min: 18, max: 30 } },
      totalRadarPoints: { firstPass: { min: 55, max: 80 }, mature: { min: 80, max: 120 } },
    },
    notes: "Island countrywide build: Bridgetown cruise/civic, west-coast luxury, south-coast resort strip.",
  }),
  "Cayman Islands": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Urban / Corporate",
      "Mixed-Use / Growth",
    ],
    submarkets: ["Grand Cayman", "Cayman Brac", "Little Cayman", "Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 55, max: 85 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "Grand Cayman–heavy island build: George Town, Seven Mile Beach, East End; Brac and Little Cayman secondary.",
  }),
  "Turks & Caicos": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Mixed-Use / Growth",
    ],
    submarkets: ["Providenciales", "Grand Turk", "Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 55, max: 85 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "Providenciales–heavy island build: Grace Bay resort corridor; Grand Turk cruise/civic secondary.",
  }),
  "Saint Lucia": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Mixed-Use / Growth",
      "Urban / Corporate",
    ],
    submarkets: ["Castries","Rodney Bay / Gros Islet","Soufrière","Vieux Fort","Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "Castries cruise/civic anchor; Rodney Bay resort corridor; Pitons/Soufrière leisure; Vieux Fort airport south.",
  }),
  "Antigua and Barbuda": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Mixed-Use / Growth",
      "Urban / Corporate",
    ],
    submarkets: ["St. John's","English Harbour","Dickenson Bay","Barbuda","Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "St. John's cruise/airport hub; Nelson's Dockyard sailing; Dickenson resort strip; Barbuda eco secondary.",
  }),
  "Grenada": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Mixed-Use / Growth",
      "Urban / Corporate",
    ],
    submarkets: ["St. George's","Grand Anse","South Coast","North Coast","Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "St. George's cruise/civic; Grand Anse resort benchmark; south and north coast eco/leisure secondary.",
  }),
  "Saint Vincent and the Grenadines": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Mixed-Use / Growth",
      "Urban / Corporate",
    ],
    submarkets: ["Kingstown","Grenadines","North Coast","Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "Kingstown cruise/civic; Grenadines yacht/leisure islands; Argyle airport south gateway.",
  }),
  "Dominica": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Mixed-Use / Growth",
      "Urban / Corporate",
    ],
    submarkets: ["Roseau","Portsmouth","East Coast","South Coast","Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "Nature-island eco/adventure positioning; Roseau cruise/civic; Portsmouth north gateway.",
  }),
  "Saint Kitts and Nevis": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Mixed-Use / Growth",
      "Urban / Corporate",
    ],
    submarkets: ["Basseterre","Frigate Bay","Nevis","Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "Basseterre cruise/civic; Frigate Bay resort strip; Nevis heritage/luxury secondary island.",
  }),
  "Trinidad and Tobago": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Mixed-Use / Growth",
      "Urban / Corporate",
    ],
    submarkets: ["Port of Spain","East-West Corridor","Tobago","South Trinidad","Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "Port of Spain urban/corporate and carnival; East-West Corridor suburban; Tobago leisure secondary.",
  }),
  "British Virgin Islands": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Mixed-Use / Growth",
      "Urban / Corporate",
    ],
    submarkets: ["Tortola","Virgin Gorda","Other Islands","Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "Tortola Road Town civic and yacht hub; Virgin Gorda resort; Jost Van Dyke and Anegada leisure satellites.",
  }),
  "Cuba": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 3",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: ["Resort / Leisure", "Cruise / Port", "Urban / Corporate", "Mixed-Use / Growth"],
    submarkets: ["Havana","Varadero","Trinidad","Santiago de Cuba","Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "Havana civic/cultural hub; Varadero beach resort corridor; Trinidad heritage; Santiago east gateway; emerging cay and nature nodes.",
  }),
  "Haiti": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 3",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: ["Resort / Leisure", "Cruise / Port", "Urban / Corporate", "Mixed-Use / Growth"],
    submarkets: ["Port-au-Prince","Cap-Haïtien","Jacmel","Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "Port-au-Prince civic and business hub; Cap-Haïtien north heritage and cruise; Jacmel arts coast; Labadie and Citadelle leisure nodes.",
  }),
  "U.S. Virgin Islands": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 3",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: ["Resort / Leisure", "Cruise / Port", "Urban / Corporate", "Mixed-Use / Growth"],
    submarkets: ["St. Thomas","St. Croix","St. John","Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "St. Thomas cruise and duty-free hub; St. Croix historic and industrial mix; St. John national park leisure; Water Island and outer cays.",
  }),
  "Martinique": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 3",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: ["Resort / Leisure", "Cruise / Port", "Urban / Corporate", "Mixed-Use / Growth"],
    submarkets: ["Fort-de-France","South Coast","North Atlantic","Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "Fort-de-France civic and cruise hub; south resort coast (Les Trois-Îlets, Diamant); north Atlantic surf and rum heritage; Mount Pelée volcano corridor.",
  }),
  "Guadeloupe": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 3",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: ["Resort / Leisure", "Cruise / Port", "Urban / Corporate", "Mixed-Use / Growth"],
    submarkets: ["Pointe-à-Pitre","Grande-Terre","Basse-Terre","Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "Pointe-à-Pitre cruise and commercial hub; Grande-Terre beach resort east coast; Basse-Terre rainforest and volcano west; Marie-Galante and Les Saintes satellites.",
  }),
  "Bonaire": cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 3",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: ["Resort / Leisure", "Cruise / Port", "Urban / Corporate", "Mixed-Use / Growth"],
    submarkets: ["Kralendijk","Rincon","Washington Slagbaai","Other"],
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: "Kralendijk cruise and dive hub; Rincon heritage village; Washington Slagbaai national park; Lac Bay windsurf and salt flats eco-tourism.",
  }),
  Guatemala: cfg({
    region: "Central America",
    buildStrategy: S.CORRIDOR_BASED,
    priorityTier: "Tier 3",
    submarkets: ["Guatemala City", "Antigua", "Lake Atitlán", "Petén / Tikal", "Other"],
    targets: {
      demandAnchors: { firstPass: { min: 40, max: 60 }, mature: { min: 70, max: 100 } },
      travelInfrastructure: { firstPass: { min: 12, max: 20 }, mature: { min: 18, max: 30 } },
      totalRadarPoints: { firstPass: { min: 55, max: 80 }, mature: { min: 90, max: 130 } },
    },
  }),
  "El Salvador": cfg({
    region: "Central America",
    buildStrategy: S.CORRIDOR_BASED,
    priorityTier: "Tier 3",
    submarkets: ["San Salvador", "La Libertad Coast", "Santa Ana", "Suchitoto", "Other"],
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 60, max: 90 } },
      travelInfrastructure: { firstPass: { min: 10, max: 18 }, mature: { min: 15, max: 25 } },
      totalRadarPoints: { firstPass: { min: 45, max: 70 }, mature: { min: 75, max: 115 } },
    },
  }),
  Honduras: cfg({
    region: "Central America",
    buildStrategy: S.CORRIDOR_BASED,
    priorityTier: "Tier 3",
    submarkets: ["Roatán", "Tegucigalpa", "San Pedro Sula", "Copán", "La Ceiba", "Other"],
    targets: {
      demandAnchors: { firstPass: { min: 40, max: 60 }, mature: { min: 70, max: 100 } },
      travelInfrastructure: { firstPass: { min: 12, max: 20 }, mature: { min: 18, max: 30 } },
      totalRadarPoints: { firstPass: { min: 55, max: 80 }, mature: { min: 90, max: 130 } },
    },
  }),
  Nicaragua: cfg({
    region: "Central America",
    buildStrategy: S.CORRIDOR_BASED,
    priorityTier: "Tier 3",
    submarkets: ["Managua", "Granada", "San Juan del Sur", "León", "Ometepe", "Other"],
    targets: {
      demandAnchors: { firstPass: { min: 40, max: 60 }, mature: { min: 70, max: 100 } },
      travelInfrastructure: { firstPass: { min: 12, max: 20 }, mature: { min: 18, max: 30 } },
      totalRadarPoints: { firstPass: { min: 55, max: 80 }, mature: { min: 90, max: 130 } },
    },
  }),
  Belize: cfg({
    region: "Central America",
    buildStrategy: S.CORRIDOR_BASED,
    priorityTier: "Tier 3",
    submarkets: ["Belize City", "Ambergris Caye", "Placencia", "San Ignacio", "Caye Caulker", "Other"],
    targets: {
      demandAnchors: { firstPass: { min: 40, max: 60 }, mature: { min: 70, max: 100 } },
      travelInfrastructure: { firstPass: { min: 12, max: 20 }, mature: { min: 18, max: 30 } },
      totalRadarPoints: { firstPass: { min: 55, max: 80 }, mature: { min: 90, max: 130 } },
    },
  }),
  Ecuador: cfg({
    region: "South America",
    buildStrategy: S.CORRIDOR_BASED,
    priorityTier: "Tier 3",
    submarkets: ["Quito", "Guayaquil", "Galápagos", "Cuenca", "Other"],
    targets: {
      demandAnchors: { firstPass: { min: 40, max: 60 }, mature: { min: 70, max: 100 } },
      travelInfrastructure: { firstPass: { min: 12, max: 20 }, mature: { min: 18, max: 30 } },
      totalRadarPoints: { firstPass: { min: 55, max: 80 }, mature: { min: 90, max: 130 } },
    },
  }),
  Uruguay: cfg({
    region: "South America",
    buildStrategy: S.CORRIDOR_BASED,
    priorityTier: "Tier 3",
    submarkets: ["Montevideo", "Punta del Este", "Colonia", "Other"],
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 60, max: 90 } },
      travelInfrastructure: { firstPass: { min: 10, max: 18 }, mature: { min: 15, max: 25 } },
      totalRadarPoints: { firstPass: { min: 45, max: 70 }, mature: { min: 75, max: 115 } },
    },
  }),
});

/**
 * Resolve active submarkets for build plan (uses nextBuildMarket when marketSubmarkets exists).
 * @param {object} config
 */
export function resolveActiveSubmarkets(config) {
  const market = config.nextBuildMarket || "";
  const marketSubmarkets = config.marketSubmarkets || {};
  if (market && marketSubmarkets[market]) return marketSubmarkets[market];
  const partial = Object.entries(marketSubmarkets).find(([key]) => market.includes(key.split(" ")[0]));
  if (partial) return partial[1];
  return config.submarkets || config.initialMarkets || [];
}

/**
 * Resolve targets for the active market build when marketTargets + nextBuildMarket are set.
 * @param {object} config
 */
export function resolveActiveMarketTargets(config) {
  const market = config.nextBuildMarket || "";
  const marketTargets = config.marketTargets || {};
  if (!market || !marketTargets[market]) return config.targets;
  const active = marketTargets[market];
  return {
    demandAnchors: active.demandAnchors || config.targets?.demandAnchors,
    travelInfrastructure: active.travelInfrastructure || config.targets?.travelInfrastructure,
    totalRadarPoints: active.totalRadarPoints || config.targets?.totalRadarPoints,
  };
}

/**
 * @param {string} country
 */
export function getCountryConfig(country) {
  return COUNTRY_CONFIGS[country] || null;
}

/**
 * @param {{ tier?: string, strategy?: string }} [filter]
 */
export function listCountryConfigs(filter = {}) {
  return Object.entries(COUNTRY_CONFIGS)
    .map(([country, config]) => ({ country, ...config }))
    .filter((c) => {
      if (filter.tier && c.priorityTier !== filter.tier) return false;
      if (filter.strategy && c.buildStrategy !== filter.strategy) return false;
      return true;
    });
}

export const COUNTRY_CONFIG_LIST = Object.keys(COUNTRY_CONFIGS);
