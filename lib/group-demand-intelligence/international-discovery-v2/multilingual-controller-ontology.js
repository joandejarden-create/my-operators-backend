/**
 * Multilingual role / controller ontology (EN, ES, DE, GL + extensible).
 * Maps local-language cues → canonical CONTROLLER_TYPE / contact-path roles.
 */

import { CONTROLLER_TYPE } from "./constants.js";

/** Query vocabulary by language for Demand Controller discovery. */
export const CONTROLLER_QUERY_VOCAB = Object.freeze({
  en: [
    "organizer",
    "secretariat",
    "technical secretariat",
    "PCO",
    "DMC",
    "housing",
    "accommodation",
    "official hotel",
    "travel partner",
    "registration provider",
    "event agency",
    "congress organizer",
    "hotel reservation",
    "hotel allotment",
    "group accommodation",
    "preferred hotel",
    "procurement",
    "travel management",
  ],
  es: [
    "secretaría técnica",
    "organizador",
    "agencia de eventos",
    "agencia receptiva",
    "DMC",
    "alojamiento",
    "hotel oficial",
    "hoteles recomendados",
    "reserva de hotel",
    "bloque de habitaciones",
    "agencia de viajes",
    "gestión de alojamiento",
    "secretaría",
    "congreso",
  ],
  de: [
    "Kongressorganisation",
    "Tagungsorganisation",
    "Veranstaltungsagentur",
    "Eventagentur",
    "Kongressbüro",
    "Unterkunft",
    "Hotelbuchung",
    "Hotelkontingent",
    "Zimmerkontingent",
    "Partnerhotel",
    "offizielles Hotel",
    "Reiseagentur",
    "Teilnehmermanagement",
    "PCO",
    "DMC",
  ],
  gl: [
    "secretaría técnica",
    "organizador",
    "axencia de eventos",
    "aloxamento",
    "hotel oficial",
    "reserva de hotel",
    "bloque de habitacións",
    "congreso",
  ],
  ca: [
    "secretària tècnica",
    "organitzador",
    "agència d'esdeveniments",
    "agència receptiva",
    "DMC",
    "allotjament",
    "hotel oficial",
    "hotels recomanats",
    "reserva d'hotel",
    "bloc d'habitacions",
    "congrés",
    "esdeveniment",
    "associació",
    "Illes Balears",
  ],
});

/** Role phrase → CONTROLLER_TYPE */
export const ROLE_TO_CONTROLLER_TYPE = Object.freeze([
  { re: /\bpco\b|professional congress organiz|kongressorganis/i, type: CONTROLLER_TYPE.PCO },
  { re: /\bdmc\b|destination management|agencia receptiva/i, type: CONTROLLER_TYPE.DMC },
  {
    re: /housing bureau|official housing|gestión de alojamiento|hotelkontingent/i,
    type: CONTROLLER_TYPE.HOUSING_BUREAU,
  },
  {
    re: /secretar[ií]a\s+t[eé]cnica|association secretariat|sekretariat/i,
    type: CONTROLLER_TYPE.ASSOCIATION_SECRETARIAT,
  },
  { re: /host institution|universidad|universit[aä]t|faculty of/i, type: CONTROLLER_TYPE.HOST_INSTITUTION },
  { re: /corporate travel|reisenmanagement|gestión de viajes/i, type: CONTROLLER_TYPE.CORPORATE_TRAVEL },
  { re: /procurement|einkauf|contrataci[oó]n|licitaci[oó]n/i, type: CONTROLLER_TYPE.PROCUREMENT },
  {
    re: /event agency|agencia de eventos|veranstaltungsagentur|eventagentur/i,
    type: CONTROLLER_TYPE.EVENT_AGENCY,
  },
  {
    re: /\btmc\b|travel management company|reiseagentur|agencia de viajes/i,
    type: CONTROLLER_TYPE.TRAVEL_MANAGEMENT_COMPANY,
  },
  {
    re: /convention bureau|cvb|turismo|congress office|kongressbüro/i,
    type: CONTROLLER_TYPE.CONVENTION_BUREAU,
  },
  {
    re: /venue congress|messec?ongress|congress centrum/i,
    type: CONTROLLER_TYPE.VENUE_CONGRESS_OFFICE,
  },
  { re: /sports travel|sportreisen|agencia deportiva/i, type: CONTROLLER_TYPE.SPORTS_TRAVEL },
  { re: /production coordinator|location manager|unit production/i, type: CONTROLLER_TYPE.PRODUCTION_COORDINATOR },
  {
    re: /registration|inscripci[oó]n|anmeldung|cvent|passkey/i,
    type: CONTROLLER_TYPE.REGISTRATION_VENDOR,
  },
]);

export function inferControllerTypeFromText(text = "") {
  const s = String(text || "");
  for (const row of ROLE_TO_CONTROLLER_TYPE) {
    if (row.re.test(s)) return row.type;
  }
  return CONTROLLER_TYPE.UNKNOWN;
}

/**
 * Build localized discovery queries for a campaign/event name.
 */
export function buildControllerDiscoveryQueries({
  eventName = "",
  market = "",
  country = "",
  languages = ["en"],
} = {}) {
  const langs = languages.length ? languages : ["en"];
  const queries = [];
  for (const lang of langs) {
    const vocab = CONTROLLER_QUERY_VOCAB[lang] || CONTROLLER_QUERY_VOCAB.en;
    const core = vocab.slice(0, 8);
    for (const term of core) {
      queries.push(
        [eventName, term, market || country].filter(Boolean).join(" ").replace(/\s+/g, " ").trim()
      );
    }
  }
  return [...new Set(queries)].slice(0, 40);
}

/** Lodging-role path segments (multilingual) for Ready contact equivalence. */
export const MULTILINGUAL_LODGING_PATH_RE =
  /\/(housing|accommodation|hotel|hotels|lodging|rooms|room-block|passkey|onpeak|alojamiento|hoteles|reserva|unterkunft|hotelbuchung|hotelkontingent|zimmerkontingent|partnerhotel)(\/|$|\?)/i;

export const MULTILINGUAL_RELEVANT_ROLE_RE =
  /meeting|event|conference|congress|housing|travel|procurement|sourcing|planner|pco|dmc|secretariat|secretar[ií]a|alojamiento|unterkunft|kongress|tagung|einkauf|contrataci[oó]n|agencia/i;

export const SUPPORTED_ONTOLOGY_LANGUAGES = Object.freeze(["en", "es", "de", "gl"]);
