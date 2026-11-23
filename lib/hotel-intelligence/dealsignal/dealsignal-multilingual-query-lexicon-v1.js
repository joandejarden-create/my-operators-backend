/**
 * DealSignal multilingual query lexicon (EN / ES / PT) for CALA-first discovery.
 * Expand from productive documents; translation ≠ independent corroboration.
 */
export const DEALSIGNAL_MULTILINGUAL_LEXICON_VERSION = "dealsignal-multilingual-query-lexicon-v1";

export const QUERY_CONCEPTS = Object.freeze({
  HOTEL_MANAGEMENT_AGREEMENT: Object.freeze({
    en: ["hotel management agreement", "management contract", "operator agreement"],
    es: [
      "contrato de administración hotelera",
      "contrato de operación hotelera",
      "contrato de gestión hotelera",
      "operadora hotelera",
    ],
    pt: ["contrato de gestão hoteleira", "contrato de administração hoteleira", "operadora hoteleira"],
  }),
  BRAND_CONVERSION: Object.freeze({
    en: ["brand conversion", "reflag", "soft brand conversion"],
    es: ["cambio de marca", "conversión hotelera", "reposicionamiento hotelero", "rebranding hotelero"],
    pt: ["troca de bandeira", "conversão de marca", "reposicionamento hoteleiro"],
  }),
  FRANCHISE: Object.freeze({
    en: ["hotel franchise", "franchise agreement", "franchise expiration"],
    es: ["franquicia hotelera", "contrato de franquicia", "vencimiento de franquicia"],
    pt: ["franquia hoteleira", "contrato de franquia", "vencimento de franquia"],
  }),
  CONTRACT_EXPIRATION: Object.freeze({
    en: ["contract expiration", "agreement expiry", "non-renewal notice"],
    es: ["vencimiento de contrato", "no renovación", "término de contrato"],
    pt: ["vencimento contratual", "não renovação", "término de contrato"],
  }),
  TENDER_CONCESSION: Object.freeze({
    en: ["hotel operator RFP", "hotel concession", "tourism PPP", "EOI hotel"],
    es: ["licitación hotelera", "concesión turística", "licitación operación hotelera", "PPP turismo"],
    pt: ["licitação hoteleira", "concessão turística", "edital operação hotel"],
  }),
  LEASE_MODEL: Object.freeze({
    en: ["hotel lease", "fixed rent hotel", "arrendamento"],
    es: ["arrendamiento hotelero", "renta fija hotel"],
    pt: ["arrendamento hoteleiro", "locação hoteleira"],
  }),
  INTERNALIZATION: Object.freeze({
    en: ["internalization of hotel operations", "self-operated hotels"],
    es: ["internalización de la operación", "operación interna hotelera"],
    pt: ["internalização da operação", "operação própria"],
  }),
});

/**
 * Build a multilingual OR-query fragment for a concept.
 * @param {keyof typeof QUERY_CONCEPTS} conceptKey
 * @param {Array<'en'|'es'|'pt'>} [langs]
 */
export function buildMultilingualQuery(conceptKey, langs = ["en", "es", "pt"]) {
  const concept = QUERY_CONCEPTS[conceptKey];
  if (!concept) return "";
  const terms = [];
  for (const lang of langs) {
    for (const t of concept[lang] || []) terms.push(`"${t}"`);
  }
  return terms.join(" OR ");
}

export function lexiconInventory() {
  return {
    version: DEALSIGNAL_MULTILINGUAL_LEXICON_VERSION,
    conceptCount: Object.keys(QUERY_CONCEPTS).length,
    languages: ["en", "es", "pt"],
    note: "Machine translation of a hit is not independent corroboration",
  };
}
