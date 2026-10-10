/**
 * Language-aware evidence normalization for GDI.
 * Preserves original source text; adds language + canonical class metadata.
 */

/** Multilingual buyer / housing role → canonical buyer function */
export const MULTILINGUAL_BUYER_ROLE_MAP = Object.freeze({
  // Spanish
  "secretaría técnica": "TECHNICAL_SECRETARIAT",
  "secretaria tecnica": "TECHNICAL_SECRETARIAT",
  "comité organizador": "ORGANIZING_COMMITTEE",
  "comite organizador": "ORGANIZING_COMMITTEE",
  "coordinador": "EVENT_COORDINATOR",
  "coordinadora": "EVENT_COORDINATOR",
  "responsable de eventos": "EVENT_MANAGER",
  "responsable de alojamiento": "HOUSING_CONTROLLER",
  "responsable de alojamiento hotelero": "HOUSING_CONTROLLER",
  viajes: "TRAVEL_DESK",
  "agencia de viajes": "OFFICIAL_TRAVEL_AGENCY",
  "agencia oficial de viajes": "OFFICIAL_TRAVEL_AGENCY",
  compras: "PROCUREMENT",
  contratación: "PROCUREMENT",
  contratacion: "PROCUREMENT",
  logística: "LOGISTICS",
  logistica: "LOGISTICS",
  delegaciones: "DELEGATION_MANAGER",
  delegación: "DELEGATION_MANAGER",
  expositores: "EXHIBITOR_SERVICES",
  patrocinios: "SPONSORSHIP",
  patrocinadores: "SPONSORSHIP",
  // Galician (shared cognates + gl-specific)
  aloxamento: "HOUSING_CONTROLLER",
  xornadas: "EVENT_COORDINATOR",
  // English
  "technical secretariat": "TECHNICAL_SECRETARIAT",
  "organizing committee": "ORGANIZING_COMMITTEE",
  "housing bureau": "HOUSING_CONTROLLER",
  "housing manager": "HOUSING_CONTROLLER",
  "event manager": "EVENT_MANAGER",
  "travel desk": "TRAVEL_DESK",
  procurement: "PROCUREMENT",
  logistics: "LOGISTICS",
});

/** Lodging language → evidence strength class (do not over-promote on bare "hotel") */
export const MULTILINGUAL_LODGING_TERM_MAP = Object.freeze({
  // Direct lodging evidence
  "hotel oficial": "DIRECT_LODGING_EVIDENCE",
  "hoteis oficiais": "DIRECT_LODGING_EVIDENCE",
  "hoteles oficiales": "DIRECT_LODGING_EVIDENCE",
  "hotel sede": "DIRECT_LODGING_EVIDENCE",
  "bloque de habitaciones": "DIRECT_LODGING_EVIDENCE",
  "bloque de habitacións": "DIRECT_LODGING_EVIDENCE",
  "room block": "DIRECT_LODGING_EVIDENCE",
  "hotel block": "DIRECT_LODGING_EVIDENCE",
  "tarifa preferencial": "DIRECT_LODGING_EVIDENCE",
  "preferential rate": "DIRECT_LODGING_EVIDENCE",
  "convenio hotelero": "DIRECT_LODGING_EVIDENCE",
  "agencia oficial de viajes": "DIRECT_LODGING_EVIDENCE",
  "official hotel": "DIRECT_LODGING_EVIDENCE",
  "official housing": "DIRECT_LODGING_EVIDENCE",
  // Strong hotel motion
  "hoteles recomendados": "STRONG_HOTEL_MOTION",
  "hoteis recomendados": "STRONG_HOTEL_MOTION",
  "hoteles colaboradores": "STRONG_HOTEL_MOTION",
  "recommended hotels": "STRONG_HOTEL_MOTION",
  "reserva hotelera": "STRONG_HOTEL_MOTION",
  alojamiento: "STRONG_HOTEL_MOTION",
  aloxamento: "STRONG_HOTEL_MOTION",
  hospedaje: "STRONG_HOTEL_MOTION",
  accommodation: "STRONG_HOTEL_MOTION",
  housing: "STRONG_HOTEL_MOTION",
  // Plausible only
  hotel: "PLAUSIBLE_HOTEL_MOTION",
  hoteles: "PLAUSIBLE_HOTEL_MOTION",
  hoteis: "PLAUSIBLE_HOTEL_MOTION",
});

const ROLE_KEYS = Object.keys(MULTILINGUAL_BUYER_ROLE_MAP).sort(
  (a, b) => b.length - a.length
);
const LODGE_KEYS = Object.keys(MULTILINGUAL_LODGING_TERM_MAP).sort(
  (a, b) => b.length - a.length
);

export function detectSourceLanguage(text = "") {
  const t = String(text || "");
  if (/\b(aloxamento|xornadas|encontro|asemblea|universidade|enerxía|proxecto|hoteis)\b/i.test(t)) {
    return "gl";
  }
  if (
    /\b(alojamiento|hospedaje|congreso|jornadas|secretaría|secretaria|comité|comite|patrocinadores|expositores|licitación|contratación)\b/i.test(
      t
    )
  ) {
    return "es";
  }
  if (/\b(hotel|conference|congress|housing|room block|organizing)\b/i.test(t)) {
    return "en";
  }
  return "unknown";
}

/**
 * Map free-text role mentions → canonical buyer functions.
 * Never collapses relevant roles into GENERAL_ORG_CONTACT.
 */
export function mapMultilingualBuyerRoles(text = "") {
  const lower = String(text || "").toLowerCase();
  const mapped = [];
  const recognized = [];
  for (const term of ROLE_KEYS) {
    if (lower.includes(term)) {
      recognized.push(term);
      const fn = MULTILINGUAL_BUYER_ROLE_MAP[term];
      if (!mapped.includes(fn)) mapped.push(fn);
    }
  }
  return {
    recognizedTerms: recognized,
    mappedCanonicalFunctions: mapped,
    unmappedTerms: [],
    ambiguousTerms: [],
    degradedToGeneral: false,
  };
}

/**
 * Map lodging language → DIRECT / STRONG / PLAUSIBLE / UNCONFIRMED / NONE
 * Bare "hotel" alone → PLAUSIBLE only (never DIRECT).
 */
export function mapMultilingualLodgingEvidence(text = "") {
  const lower = String(text || "").toLowerCase();
  let best = "NONE";
  const rank = {
    DIRECT_LODGING_EVIDENCE: 4,
    STRONG_HOTEL_MOTION: 3,
    PLAUSIBLE_HOTEL_MOTION: 2,
    UNCONFIRMED: 1,
    NONE: 0,
  };
  const hits = [];
  for (const term of LODGE_KEYS) {
    if (lower.includes(term)) {
      const cls = MULTILINGUAL_LODGING_TERM_MAP[term];
      hits.push({ term, class: cls });
      if (rank[cls] > rank[best]) best = cls;
    }
  }
  // Bare hotel with no stronger term stays PLAUSIBLE (already in map)
  if (best === "NONE" && /\bhotel/i.test(lower)) {
    best = "UNCONFIRMED";
  }
  return { lodgingEvidenceClass: best, termHits: hits };
}

/**
 * Normalize a discovery/evidence finding without replacing original language text.
 */
export function normalizeGdiMultilingualEvidence({
  originalText,
  sourceLanguage,
  finding,
  evidenceClass,
  epistemicStatus = "Unknown",
} = {}) {
  const text = String(originalText || finding || "");
  const lang = sourceLanguage || detectSourceLanguage(text);
  const roles = mapMultilingualBuyerRoles(text);
  const lodging = mapMultilingualLodgingEvidence(text);
  const allowed = ["Fact", "Estimate", "Inference", "Unknown"];
  const epistemic = allowed.includes(epistemicStatus) ? epistemicStatus : "Unknown";
  return {
    originalSourceText: text,
    sourceLanguage: lang,
    normalizedFinding: String(finding || text).trim(),
    canonicalEvidenceClass: evidenceClass || lodging.lodgingEvidenceClass || "UNCONFIRMED",
    epistemicStatus: epistemic,
    buyerRoleMap: roles,
    lodgingMap: lodging,
  };
}
