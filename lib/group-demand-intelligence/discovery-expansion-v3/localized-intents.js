/**
 * Canonical discovery intents + natural localization (not literal translate).
 */

export const CANONICAL_INTENTS = Object.freeze({
  ASSOCIATION_CONGRESS: "ASSOCIATION_CONGRESS",
  ANNUAL_MEETING: "ANNUAL_MEETING",
  BOARD_MEETING: "BOARD_MEETING",
  TRAINING_PROGRAM: "TRAINING_PROGRAM",
  DELEGATION: "DELEGATION",
  GROUP_HOUSING: "GROUP_HOUSING",
  ROOM_BLOCK: "ROOM_BLOCK",
  PROCUREMENT: "PROCUREMENT",
  LODGING_TENDER: "LODGING_TENDER",
  SPORTS_HOUSING: "SPORTS_HOUSING",
  MEDICAL_CONGRESS: "MEDICAL_CONGRESS",
  INVESTIGATOR_MEETING: "INVESTIGATOR_MEETING",
  CORPORATE_OFFSITE: "CORPORATE_OFFSITE",
  PROJECT_WORKFORCE: "PROJECT_WORKFORCE",
  UNIVERSITY_PROGRAM: "UNIVERSITY_PROGRAM",
  TOUR_SERIES: "TOUR_SERIES",
  INCENTIVE_GROUP: "INCENTIVE_GROUP",
  HIDDEN_EXHIBITOR: "HIDDEN_EXHIBITOR",
  HIDDEN_DELEGATION: "HIDDEN_DELEGATION",
});

/** Natural local phrases by language × intent (not machine literal). */
const LOCAL_PHRASES = Object.freeze({
  en: {
    ASSOCIATION_CONGRESS: "association congress hotel block",
    ANNUAL_MEETING: "annual meeting accommodations",
    BOARD_MEETING: "board meeting hotel",
    TRAINING_PROGRAM: "training program group lodging",
    DELEGATION: "delegation hotel housing",
    GROUP_HOUSING: "group housing room block",
    ROOM_BLOCK: "official room block host hotel",
    PROCUREMENT: "hotel accommodation tender RFP",
    LODGING_TENDER: "lodging services procurement",
    SPORTS_HOUSING: "sports team hotel block tournament",
    MEDICAL_CONGRESS: "medical congress housing",
    INVESTIGATOR_MEETING: "investigator meeting hotel",
    CORPORATE_OFFSITE: "corporate offsite hotel",
    PROJECT_WORKFORCE: "project workforce temporary lodging",
    UNIVERSITY_PROGRAM: "university conference accommodations",
    TOUR_SERIES: "incentive tour group hotel",
    INCENTIVE_GROUP: "incentive travel group lodging",
    HIDDEN_EXHIBITOR: "exhibitor housing preferred hotel",
    HIDDEN_DELEGATION: "national pavilion delegation hotel",
  },
  fr: {
    ASSOCIATION_CONGRESS: "congrès association hébergement hôtel",
    ANNUAL_MEETING: "assemblée annuelle hôtel",
    BOARD_MEETING: "réunion conseil administration hôtel",
    TRAINING_PROGRAM: "formation groupe hébergement",
    DELEGATION: "délégation hébergement hôtel",
    GROUP_HOUSING: "hébergement groupe bloc chambres",
    ROOM_BLOCK: "bloc de chambres hôtel officiel",
    PROCUREMENT: "appel d'offres hébergement hôtel",
    LODGING_TENDER: "marché public hébergement",
    SPORTS_HOUSING: "hébergement équipe sportive compétition",
    MEDICAL_CONGRESS: "congrès médical hébergement",
    INVESTIGATOR_MEETING: "réunion investigateurs hôtel",
    CORPORATE_OFFSITE: "séminaire entreprise hôtel",
    PROJECT_WORKFORCE: "hébergement chantier équipes projet",
    UNIVERSITY_PROGRAM: "colloque universitaire hébergement",
    TOUR_SERIES: "voyage incentive groupe hôtel",
    INCENTIVE_GROUP: "incentive voyage groupe hébergement",
    HIDDEN_EXHIBITOR: "hébergement exposants hôtel partenaire",
    HIDDEN_DELEGATION: "délégation pavillon hôtel",
  },
  es: {
    ASSOCIATION_CONGRESS: "congreso asociación alojamiento hotel",
    ANNUAL_MEETING: "asamblea anual alojamiento",
    BOARD_MEETING: "reunión junta directiva hotel",
    TRAINING_PROGRAM: "formación grupo alojamiento",
    DELEGATION: "delegación alojamiento hotel",
    GROUP_HOUSING: "alojamiento grupo bloque habitaciones",
    ROOM_BLOCK: "bloque de habitaciones hotel oficial",
    PROCUREMENT: "licitación alojamiento hotel",
    LODGING_TENDER: "contrato público alojamiento",
    SPORTS_HOUSING: "alojamiento equipo deportivo torneo",
    MEDICAL_CONGRESS: "congreso médico alojamiento",
    INVESTIGATOR_MEETING: "reunión investigadores hotel",
    CORPORATE_OFFSITE: "offsite corporativo hotel",
    PROJECT_WORKFORCE: "alojamiento temporal obra proyecto",
    UNIVERSITY_PROGRAM: "congreso universitario alojamiento",
    TOUR_SERIES: "viaje incentivo grupo hotel",
    INCENTIVE_GROUP: "viaje de incentivo alojamiento",
    HIDDEN_EXHIBITOR: "alojamiento expositores hotel",
    HIDDEN_DELEGATION: "delegación pabellón hotel",
  },
  gl: {
    ASSOCIATION_CONGRESS: "congreso asociación aloxamento hotel",
    ANNUAL_MEETING: "asemblea anual aloxamento",
    BOARD_MEETING: "reunión consello hotel",
    TRAINING_PROGRAM: "formación grupo aloxamento",
    DELEGATION: "delegación aloxamento hotel",
    GROUP_HOUSING: "aloxamento grupo",
    ROOM_BLOCK: "bloque habitacións hotel oficial",
    PROCUREMENT: "licitación aloxamento",
    LODGING_TENDER: "contrato público aloxamento",
    SPORTS_HOUSING: "aloxamento equipo deportivo",
    MEDICAL_CONGRESS: "congreso médico aloxamento",
    INVESTIGATOR_MEETING: "reunión investigadores hotel",
    CORPORATE_OFFSITE: "reunión empresa hotel",
    PROJECT_WORKFORCE: "aloxamento temporal obra",
    UNIVERSITY_PROGRAM: "congreso universitario aloxamento",
    TOUR_SERIES: "viaje incentivo grupo",
    INCENTIVE_GROUP: "incentivo grupo aloxamento",
    HIDDEN_EXHIBITOR: "aloxamento expositores",
    HIDDEN_DELEGATION: "delegación hotel",
  },
  de: {
    ASSOCIATION_CONGRESS: "Verbandskongress Hotelkontingent",
    ANNUAL_MEETING: "Jahresversammlung Hotel",
    BOARD_MEETING: "Vorstandssitzung Hotel",
    TRAINING_PROGRAM: "Schulung Gruppenunterkunft",
    DELEGATION: "Delegation Hotelunterkunft",
    GROUP_HOUSING: "Gruppenunterkunft Zimmerkontingent",
    ROOM_BLOCK: "offizielles Zimmerkontingent Host Hotel",
    PROCUREMENT: "Ausschreibung Hotelunterkunft",
    LODGING_TENDER: "Vergabe Unterkunftsleistungen",
    SPORTS_HOUSING: "Sportmannschaft Hotelkontingent",
    MEDICAL_CONGRESS: "Medizinkongress Unterkunft",
    INVESTIGATOR_MEETING: "Investigator Meeting Hotel",
    CORPORATE_OFFSITE: "Firmenklausur Hotel",
    PROJECT_WORKFORCE: "Projektmitarbeiter Unterkunft",
    UNIVERSITY_PROGRAM: "Universitätskonferenz Unterkunft",
    TOUR_SERIES: "Incentive Reise Gruppe Hotel",
    INCENTIVE_GROUP: "Incentive Gruppe Unterkunft",
    HIDDEN_EXHIBITOR: "Ausstellerunterkunft Partnerhotel",
    HIDDEN_DELEGATION: "Pavilion Delegation Hotel",
  },
  it: {
    ASSOCIATION_CONGRESS: "congresso associazione hotel",
    ANNUAL_MEETING: "assemblea annuale alloggio",
    BOARD_MEETING: "riunione consiglio hotel",
    TRAINING_PROGRAM: "formazione gruppo alloggio",
    DELEGATION: "delegazione alloggio hotel",
    GROUP_HOUSING: "alloggio gruppo blocco camere",
    ROOM_BLOCK: "blocco camere hotel ufficiale",
    PROCUREMENT: "gara alloggio hotel",
    LODGING_TENDER: "appalto servizi alberghieri",
    SPORTS_HOUSING: "alloggio squadra sportiva",
    MEDICAL_CONGRESS: "congresso medico alloggio",
    INVESTIGATOR_MEETING: "riunione investigator hotel",
    CORPORATE_OFFSITE: "offsite aziendale hotel",
    PROJECT_WORKFORCE: "alloggio temporaneo cantiere",
    UNIVERSITY_PROGRAM: "convegno universitario alloggio",
    TOUR_SERIES: "viaggio incentive gruppo hotel",
    INCENTIVE_GROUP: "incentive gruppo alloggio",
    HIDDEN_EXHIBITOR: "alloggio espositori hotel",
    HIDDEN_DELEGATION: "delegazione padiglione hotel",
  },
});

const GEO_GL = { en: "gl", fr: "fr", es: "es", gl: "es", de: "de", it: "it" };
const HL = { en: "en", fr: "fr", es: "es", gl: "gl", de: "de", it: "it" };

/**
 * Build one localized query record.
 */
export function buildLocalizedQuery({
  intent,
  language,
  marketPlaceNames = [],
  yearHints = ["2026", "2027"],
  feederMarket = null,
  queryFamily = "LOCAL_MARKET",
} = {}) {
  const lang = String(language || "en").toLowerCase();
  const phrases = LOCAL_PHRASES[lang] || LOCAL_PHRASES.en;
  const phrase = phrases[intent] || LOCAL_PHRASES.en[intent] || String(intent).replace(/_/g, " ");
  const place = (marketPlaceNames || []).filter(Boolean).slice(0, 2).join(" ");
  const feeder = feederMarket ? String(feederMarket) : "";
  const years = (yearHints || []).slice(0, 2).join(" OR ");

  let localizedQuery;
  if (queryFamily === "FEEDER_ORIGIN") {
    localizedQuery = `${phrase} ${feeder} ${place} ${years}`.replace(/\s+/g, " ").trim();
  } else {
    localizedQuery = `${phrase} ${place} ${years}`.replace(/\s+/g, " ").trim();
  }

  return {
    canonicalIntent: intent,
    queryLanguage: lang,
    localizedQuery,
    market: place || null,
    sourceLanguage: lang,
    queryFamily,
    feederMarket: feeder || null,
    serpHl: HL[lang] || "en",
    serpGl: GEO_GL[lang] || "us",
  };
}

/**
 * Expand intents × languages × places into a query matrix (caller budgets).
 */
export function buildLocalizedQueryMatrix({
  intents = [],
  languages = ["en"],
  marketPlaceNames = [],
  feederMarkets = [],
  yearHints = ["2026", "2027"],
} = {}) {
  const rows = [];
  for (const intent of intents) {
    for (const language of languages) {
      rows.push(
        buildLocalizedQuery({
          intent,
          language,
          marketPlaceNames,
          yearHints,
          queryFamily: "LOCAL_MARKET",
        })
      );
    }
  }
  // Feeder-origin queries: English + primary place + feeder (bounded)
  for (const feeder of (feederMarkets || []).slice(0, 4)) {
    for (const intent of intents.filter((i) =>
      /INCENTIVE|TOUR|CORPORATE|ASSOCIATION|ANNUAL|GROUP_HOUSING|ROOM_BLOCK/.test(i)
    ).slice(0, 3)) {
      rows.push(
        buildLocalizedQuery({
          intent,
          language: "en",
          marketPlaceNames,
          feederMarket: feeder,
          yearHints,
          queryFamily: "FEEDER_ORIGIN",
        })
      );
    }
  }
  return rows;
}
