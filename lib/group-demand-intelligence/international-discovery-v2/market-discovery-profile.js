/**
 * Market Discovery Profile — strategy weights, not different truth standards.
 */

import { DISCOVERY_SPINE, IDV2_VERSION } from "./constants.js";
import { CONTROLLER_QUERY_VOCAB } from "./multilingual-controller-ontology.js";

const PROFILE_VERSION = `${IDV2_VERSION}_mdp_v1`;

/** Default path weights 0–1 (relative preference only). */
const DEFAULT_WEIGHTS = Object.freeze({
  [DISCOVERY_SPINE.PARTICIPANT_FIRST]: 0.5,
  [DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST]: 0.5,
  [DISCOVERY_SPINE.ACCOUNT_FIRST]: 0.5,
  [DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST]: 0.4,
  [DISCOVERY_SPINE.HOTEL_HISTORY_FIRST]: 0.3,
});

const MARKET_PROFILES = Object.freeze({
  us_default: {
    market: "United States",
    country: "US",
    region: "NORTH_AMERICA",
    primaryLanguages: ["en"],
    secondaryLanguages: [],
    publicListAvailability: "HIGH",
    PCOPrevalence: "MEDIUM",
    DMCPrevalence: "LOW",
    centralHousingPrevalence: "MEDIUM",
    associationSecretariatPrevalence: "MEDIUM",
    publicProcurementAvailability: "HIGH",
    corporateTransparency: "HIGH",
    preferredDiscoveryPaths: [
      DISCOVERY_SPINE.PARTICIPANT_FIRST,
      DISCOVERY_SPINE.ACCOUNT_FIRST,
      DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
    ],
    pathWeights: {
      ...DEFAULT_WEIGHTS,
      [DISCOVERY_SPINE.PARTICIPANT_FIRST]: 0.95,
      [DISCOVERY_SPINE.ACCOUNT_FIRST]: 0.85,
      [DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST]: 0.55,
      [DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST]: 0.45,
      [DISCOVERY_SPINE.HOTEL_HISTORY_FIRST]: 0.7,
    },
    sourceFamilies: [
      "EXHIBITOR_LIST",
      "PASSKEY_ONPEAK",
      "PROCUREMENT_PORTAL",
      "ASSOCIATION_SITE",
      "CORPORATE_PRESSROOM",
    ],
  },
  europe_default: {
    market: "Europe",
    country: null,
    region: "EUROPE",
    primaryLanguages: ["en", "de", "es"],
    secondaryLanguages: ["fr", "it", "nl"],
    publicListAvailability: "LOW",
    PCOPrevalence: "HIGH",
    DMCPrevalence: "HIGH",
    centralHousingPrevalence: "HIGH",
    associationSecretariatPrevalence: "HIGH",
    publicProcurementAvailability: "MEDIUM",
    corporateTransparency: "MEDIUM",
    preferredDiscoveryPaths: [
      DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
      DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST,
      DISCOVERY_SPINE.ACCOUNT_FIRST,
      DISCOVERY_SPINE.PARTICIPANT_FIRST,
    ],
    pathWeights: {
      ...DEFAULT_WEIGHTS,
      [DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST]: 0.95,
      [DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST]: 0.85,
      [DISCOVERY_SPINE.ACCOUNT_FIRST]: 0.8,
      [DISCOVERY_SPINE.PARTICIPANT_FIRST]: 0.45,
      [DISCOVERY_SPINE.HOTEL_HISTORY_FIRST]: 0.75,
    },
    sourceFamilies: [
      "PCO_SITE",
      "DMC_SITE",
      "ASSOCIATION_SECRETARIAT",
      "CVB_CALENDAR",
      "UNIVERSITY_CONGRESS",
      "EU_PROCUREMENT",
      "EVENT_MANUAL_PDF",
    ],
  },
  cala_default: {
    market: "CALA",
    country: null,
    region: "CALA",
    primaryLanguages: ["es", "en"],
    secondaryLanguages: ["pt", "gl"],
    publicListAvailability: "LOW",
    PCOPrevalence: "HIGH",
    DMCPrevalence: "HIGH",
    centralHousingPrevalence: "HIGH",
    associationSecretariatPrevalence: "HIGH",
    publicProcurementAvailability: "MEDIUM",
    corporateTransparency: "MEDIUM",
    preferredDiscoveryPaths: [
      DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
      DISCOVERY_SPINE.ACCOUNT_FIRST,
      DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST,
      DISCOVERY_SPINE.PARTICIPANT_FIRST,
    ],
    pathWeights: {
      ...DEFAULT_WEIGHTS,
      [DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST]: 0.95,
      [DISCOVERY_SPINE.ACCOUNT_FIRST]: 0.85,
      [DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST]: 0.8,
      [DISCOVERY_SPINE.PARTICIPANT_FIRST]: 0.4,
      [DISCOVERY_SPINE.HOTEL_HISTORY_FIRST]: 0.8,
    },
    sourceFamilies: [
      "PCO_SITE",
      "DMC_SITE",
      "ASSOCIATION_SECRETARIAT",
      "CVB_CALENDAR",
      "CORPORATE_PRESSROOM",
      "EVENT_MANUAL_PDF",
    ],
  },
  switzerland_geneva: {
    market: "Geneva",
    country: "CH",
    region: "EUROPE",
    primaryLanguages: ["en", "fr", "de"],
    secondaryLanguages: ["it"],
    publicListAvailability: "MEDIUM",
    PCOPrevalence: "HIGH",
    DMCPrevalence: "MEDIUM",
    centralHousingPrevalence: "HIGH",
    associationSecretariatPrevalence: "HIGH",
    publicProcurementAvailability: "MEDIUM",
    corporateTransparency: "HIGH",
    preferredDiscoveryPaths: [
      DISCOVERY_SPINE.PARTICIPANT_FIRST,
      DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
      DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST,
    ],
    pathWeights: {
      ...DEFAULT_WEIGHTS,
      [DISCOVERY_SPINE.PARTICIPANT_FIRST]: 0.85,
      [DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST]: 0.8,
      [DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST]: 0.75,
      [DISCOVERY_SPINE.ACCOUNT_FIRST]: 0.7,
      [DISCOVERY_SPINE.HOTEL_HISTORY_FIRST]: 0.65,
    },
    sourceFamilies: [
      "UN_IO_CALENDAR",
      "ASSOCIATION_SITE",
      "PCO_SITE",
      "EXHIBITOR_LIST",
      "EVENT_MANUAL_PDF",
    ],
  },
  spain_coruna: {
    market: "A Coruña",
    country: "ES",
    region: "EUROPE",
    primaryLanguages: ["es", "gl"],
    secondaryLanguages: ["en"],
    publicListAvailability: "LOW",
    PCOPrevalence: "HIGH",
    DMCPrevalence: "HIGH",
    centralHousingPrevalence: "HIGH",
    associationSecretariatPrevalence: "HIGH",
    publicProcurementAvailability: "MEDIUM",
    corporateTransparency: "MEDIUM",
    preferredDiscoveryPaths: [
      DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
      DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST,
      DISCOVERY_SPINE.ACCOUNT_FIRST,
    ],
    pathWeights: {
      ...DEFAULT_WEIGHTS,
      [DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST]: 0.95,
      [DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST]: 0.9,
      [DISCOVERY_SPINE.ACCOUNT_FIRST]: 0.8,
      [DISCOVERY_SPINE.PARTICIPANT_FIRST]: 0.35,
      [DISCOVERY_SPINE.HOTEL_HISTORY_FIRST]: 0.75,
    },
    sourceFamilies: [
      "PCO_SITE",
      "ASSOCIATION_SECRETARIAT",
      "CVB_CALENDAR",
      "UNIVERSITY_CONGRESS",
      "EVENT_MANUAL_PDF",
      "ES_PROCUREMENT",
    ],
  },
  spain_mallorca_palma: {
    market: "Mallorca",
    country: "ES",
    region: "EUROPE",
    primaryLanguages: ["es"],
    secondaryLanguages: ["en"],
    selectiveLanguages: ["ca"],
    publicListAvailability: "MEDIUM",
    PCOPrevalence: "HIGH",
    DMCPrevalence: "HIGH",
    centralHousingPrevalence: "HIGH",
    associationSecretariatPrevalence: "HIGH",
    publicProcurementAvailability: "MEDIUM",
    corporateTransparency: "MEDIUM",
    preferredDiscoveryPaths: [
      DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
      DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST,
      DISCOVERY_SPINE.ACCOUNT_FIRST,
      DISCOVERY_SPINE.HOTEL_HISTORY_FIRST,
      DISCOVERY_SPINE.PARTICIPANT_FIRST,
    ],
    pathWeights: {
      ...DEFAULT_WEIGHTS,
      [DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST]: 0.95,
      [DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST]: 0.85,
      [DISCOVERY_SPINE.ACCOUNT_FIRST]: 0.85,
      [DISCOVERY_SPINE.HOTEL_HISTORY_FIRST]: 0.8,
      [DISCOVERY_SPINE.PARTICIPANT_FIRST]: 0.4,
    },
    sourceFamilies: [
      "PCO_SITE",
      "DMC_SITE",
      "ASSOCIATION_SECRETARIAT",
      "CVB_CALENDAR",
      "CONGRESS_CENTRE_CALENDAR",
      "GOLF_EVENT_CALENDAR",
      "EVENT_MANUAL_PDF",
      "ES_PROCUREMENT",
    ],
    queryLanguageNotes:
      "PRIMARY Spanish; SECONDARY English; LOCAL SUPPORT Catalan/Mallorquín where sources use it (congrés, allotjament, Illes Balears).",
    localQueryFamilies: [
      "congreso",
      "convención",
      "jornada",
      "reunión",
      "incentivo",
      "grupo",
      "evento corporativo",
      "golf",
      "torneo",
      "campeonato",
      "boda",
      "celebración",
      "asociación",
      "delegación",
      "formación",
      "viaje de empresa",
      "alojamiento",
      "hotel oficial",
      "hoteles recomendados",
      "convenio hotelero",
      "bloque de habitaciones",
      "reserva de hotel",
      "agencia receptiva",
      "DMC",
      "secretaría técnica",
      "agencia de eventos",
      "Palma",
      "Mallorca",
      "Illes Balears",
      "Islas Baleares",
      "congrés",
      "allotjament",
      "hotel oficial",
      "hotels recomanats",
      "esdeveniment",
      "associació",
    ],
  },
  germany_munich: {
    market: "Munich",
    country: "DE",
    region: "EUROPE",
    primaryLanguages: ["de", "en"],
    secondaryLanguages: [],
    publicListAvailability: "LOW",
    PCOPrevalence: "HIGH",
    DMCPrevalence: "MEDIUM",
    centralHousingPrevalence: "HIGH",
    associationSecretariatPrevalence: "HIGH",
    publicProcurementAvailability: "HIGH",
    corporateTransparency: "MEDIUM",
    preferredDiscoveryPaths: [
      DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
      DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST,
      DISCOVERY_SPINE.ACCOUNT_FIRST,
    ],
    pathWeights: {
      ...DEFAULT_WEIGHTS,
      [DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST]: 0.95,
      [DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST]: 0.85,
      [DISCOVERY_SPINE.ACCOUNT_FIRST]: 0.8,
      [DISCOVERY_SPINE.PARTICIPANT_FIRST]: 0.4,
      [DISCOVERY_SPINE.HOTEL_HISTORY_FIRST]: 0.75,
    },
    sourceFamilies: [
      "PCO_SITE",
      "MESSE_CALENDAR",
      "ASSOCIATION_SECRETARIAT",
      "CVB_CALENDAR",
      "DE_PROCUREMENT",
      "EVENT_MANUAL_PDF",
    ],
  },
  dominican_republic_sd: {
    market: "Santo Domingo",
    country: "DO",
    region: "CALA",
    primaryLanguages: ["es", "en"],
    secondaryLanguages: [],
    publicListAvailability: "LOW",
    PCOPrevalence: "HIGH",
    DMCPrevalence: "HIGH",
    centralHousingPrevalence: "HIGH",
    associationSecretariatPrevalence: "HIGH",
    publicProcurementAvailability: "LOW",
    corporateTransparency: "MEDIUM",
    preferredDiscoveryPaths: [
      DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
      DISCOVERY_SPINE.ACCOUNT_FIRST,
      DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST,
    ],
    pathWeights: {
      ...DEFAULT_WEIGHTS,
      [DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST]: 0.95,
      [DISCOVERY_SPINE.ACCOUNT_FIRST]: 0.9,
      [DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST]: 0.8,
      [DISCOVERY_SPINE.PARTICIPANT_FIRST]: 0.3,
      [DISCOVERY_SPINE.HOTEL_HISTORY_FIRST]: 0.85,
    },
    sourceFamilies: [
      "DMC_SITE",
      "PCO_SITE",
      "ASSOCIATION_SECRETARIAT",
      "CVB_CALENDAR",
      "CORPORATE_PRESSROOM",
      "EVENT_MANUAL_PDF",
    ],
  },
  maryland_bethesda: {
    market: "Bethesda",
    country: "US",
    region: "NORTH_AMERICA",
    primaryLanguages: ["en"],
    secondaryLanguages: [],
    publicListAvailability: "HIGH",
    PCOPrevalence: "MEDIUM",
    DMCPrevalence: "LOW",
    centralHousingPrevalence: "MEDIUM",
    associationSecretariatPrevalence: "MEDIUM",
    publicProcurementAvailability: "HIGH",
    corporateTransparency: "HIGH",
    preferredDiscoveryPaths: [
      DISCOVERY_SPINE.PARTICIPANT_FIRST,
      DISCOVERY_SPINE.ACCOUNT_FIRST,
      DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
    ],
    pathWeights: {
      ...DEFAULT_WEIGHTS,
      [DISCOVERY_SPINE.PARTICIPANT_FIRST]: 0.95,
      [DISCOVERY_SPINE.ACCOUNT_FIRST]: 0.9,
      [DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST]: 0.5,
      [DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST]: 0.4,
      [DISCOVERY_SPINE.HOTEL_HISTORY_FIRST]: 0.7,
    },
    sourceFamilies: [
      "EXHIBITOR_LIST",
      "PASSKEY_ONPEAK",
      "NIH_ASSOCIATION",
      "PROCUREMENT_PORTAL",
      "CORPORATE_PRESSROOM",
    ],
  },
});

const HOTEL_TO_PROFILE = Object.freeze({
  bethesda_marriott: "maryland_bethesda",
  ac_hotel_a_coruna: "spain_coruna",
  radisson_santo_domingo: "dominican_republic_sd",
  westin_grand_munchen: "germany_munich",
  yotel_geneva_lake: "switzerland_geneva",
  castillo_hotel_son_vida: "spain_mallorca_palma",
  adp_castillo_hotel_son_vida: "spain_mallorca_palma",
  rec82d9zpb8fede1i: "spain_mallorca_palma",
  sheraton_mallorca_arabella_golf: "spain_mallorca_palma",
  adp_sheraton_mallorca_arabella_golf: "spain_mallorca_palma",
  recjhqdauisyxfcqe: "spain_mallorca_palma",
});

export function resolveMarketDiscoveryProfile({ hotelId, market, country, region } = {}) {
  const hid = String(hotelId || "")
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_");
  const key = HOTEL_TO_PROFILE[hid];
  let base = key ? MARKET_PROFILES[key] : null;
  if (!base) {
    const c = String(country || "").toUpperCase();
    const r = String(region || market || "").toUpperCase();
    if (c === "US" || r.includes("UNITED STATES") || r.includes("NORTH_AMERICA")) {
      base = MARKET_PROFILES.us_default;
    } else if (
      ["DO", "PR", "MX", "CO", "PA", "CR", "GT", "HN", "NI", "SV", "CU", "JM"].includes(c) ||
      r.includes("CALA") ||
      r.includes("CARIBBEAN") ||
      r.includes("LATIN")
    ) {
      base = MARKET_PROFILES.cala_default;
    } else {
      base = MARKET_PROFILES.europe_default;
    }
  }

  const languages = [
    ...(base.primaryLanguages || []),
    ...(base.secondaryLanguages || []),
    ...(base.selectiveLanguages || []),
  ];
  const roleVocabulary = {};
  const lodgingVocabulary = {};
  for (const lang of languages) {
    if (CONTROLLER_QUERY_VOCAB[lang]) {
      roleVocabulary[lang] = CONTROLLER_QUERY_VOCAB[lang];
      lodgingVocabulary[lang] = CONTROLLER_QUERY_VOCAB[lang].filter((t) =>
        /hotel|hous|accom|aloj|allotj|unter|konting|reserva|room|lodg/i.test(t)
      );
    }
  }

  return {
    ...base,
    market: market || base.market,
    country: country || base.country,
    marketProfileVersion: PROFILE_VERSION,
    roleVocabulary,
    lodgingVocabulary,
    hotelId: hid || null,
  };
}

export function listMarketDiscoveryProfiles() {
  return Object.entries(MARKET_PROFILES).map(([id, p]) => ({
    profileId: id,
    marketProfileVersion: PROFILE_VERSION,
    ...p,
  }));
}

export { PROFILE_VERSION, MARKET_PROFILES, HOTEL_TO_PROFILE };
