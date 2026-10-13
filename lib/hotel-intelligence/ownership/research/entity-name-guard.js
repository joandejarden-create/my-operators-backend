/**
 * Reject non-entity extraction fragments.
 * Hardened from ownership-eval-v1 false positives (OTA, TripAdvisor, Spanish boilerplate).
 *
 * Allow: Grupo X Hotels / Hospitality / Resorts / SA / LLC / fund / family office names.
 */

import {
  looksLikeMajorBrandName,
  looksLikeInstitutionalSponsorName,
} from "./claim-context.js";

const REJECT_EXACT = new Set(
  [
    "esta",
    "este",
    "esto",
    "particular",
    "esa exclusiva",
    "un alojamiento",
    "este alojamiento",
    "el hotel",
    "la propiedad",
    "the hotel",
    "the property",
    "o hotel",
    "a propriedade",
    "del hotel",
    "mis",
    "acá",
    "excelente",
    "negocios",
    "muy amable",
    "es español",
    "property",
    "hotel",
    "resort",
    "hoteles",
    "hoteis",
    "resorts",
    "alojamiento",
    "alojamento",
    "viajes",
    "turismo",
    "hospitality",
    "collection",
    "management",
    "propietarios de grupos diversos",
    "s de grupos diversos",
    "s de alojamientos",
    "s de coches hay una plaza de parking",
    "s de coches se les brinda el parking",
    "s do hotel",
    "s de hotel",
    "s desde hace muchos años",
    "s foi",
    "no ha indicado si el",
    "l hotel",
    "l restaurante",
    "e o",
  ].map((s) => s.toLowerCase())
);

const REJECT_CONTAINS = [
  /detector de humo/i,
  /plaza de parking/i,
  /alojamiento cuenta/i,
  /responded to this/i,
  /respondi[oó] a esta/i,
  /reviewresponded/i,
  /opini[oó]nrespondida/i,
  /estadia/i,
  /alterado as datas/i,
  /submarca/i,
  /todo incluido cerca/i,
  /s de coches/i,
  /s de alojamientos/i,
  /s do hotel/i,
  /^e o\b/i,
  /^at\s+/i,
  /^en\s+/i,
  /^del\s+/i,
  /^l\s+/i,
  /^s\s+de\s+/i,
  /mudarse del/i,
  /organización/i,
  /habitaciones/i,
  /nuevas habitaciones/i,
  /price per square/i,
  /competitive value pricing/i,
  /disfrutarás de acceso/i,
  /amabilísimo/i,
  /chica muy guapa/i,
  /señor chaparrito/i,
  /hace \d+/i,
  /hace un (año|mes)/i,
  /https?:\/\//i,
  /www\./i,
  /\b(booking\.com|tripadvisor|expedia|hotels\.com|trivago|kayak)\b/i,
  /\bcancelaci[oó]n\b/i,
  /\bnon[\s-]?refundable\b/i,
  /\bcheck[\s-]?in\b/i,
  /\bsmoke\s+detector\b/i,
  /\bparking\b/i,
  /\bguest\s+room\b/i,
  /\bsuite\b/i,
  /\bking\s+bed\b/i,
];

const GEO_LIKE_RE =
  /^(mexico|méxico|brazil|brasil|colombia|peru|perú|chile|ecuador|panama|panamá|jamaica|cuba|bahamas|cancun|cancún|cabo|vallarta|riviera\s+maya|santo\s+domingo|rio\s+de\s+janeiro|sao\s+paulo|são\s+paulo)$/i;

const LEGAL_HINT_RE =
  /\b(s\.?a\.?|s\.?\s*a\.?\s*de\s*c\.?\s*v\.?|de\s+c\.?v\.?|llc|l\.?l\.?c\.?|inc\.?|ltd\.?|corp\.?|gmbh|bv|nv|plc|llp|lp|spa|srl|s\.?r\.?l\.?|ltda|eireli|sas|s\.?a\.?s\.?|grupo|group|holding|holdings|capital|partners|investments?|inversiones|investimentos|incorporadora|hospitality|hotels?|hoteles|hoteis|resorts?|properties|propiedades|fund|funds|fideicomiso|reit|trust|family\s+office|asset\s+management|management\s+company|desarrolladora|desarrollos)\b/i;

/**
 * @param {string} name
 * @param {{ allowBrandAsEntity?: boolean }} [opts]
 * @returns {boolean}
 */
export function isPlausibleLegalEntityName(name, opts = {}) {
  const raw = String(name || "").trim();
  if (raw.length < 4 || raw.length > 90) return false;
  const lower = raw.toLowerCase();
  if (REJECT_EXACT.has(lower)) return false;
  if (GEO_LIKE_RE.test(raw)) return false;

  for (const re of REJECT_CONTAINS) {
    if (re.test(raw)) return false;
  }

  // Sentence / fragment signals
  if (/[,:;]/.test(raw) && raw.split(/[,:;]/).length > 2) return false;
  if (/\s{2,}/.test(raw)) return false;
  if (/[.!?]$/.test(raw) && raw.split(/\s+/).length > 6) return false;
  if ((raw.match(/,/g) || []).length >= 2) return false;

  // Too many lowercase function words → sentence fragment
  const tokens = lower.split(/\s+/).filter(Boolean);
  if (tokens.length === 0) return false;
  if (tokens.length === 1 && tokens[0].length < 5) return false;
  if (tokens.length > 10) return false;

  const stopwordHeavy =
    tokens.filter((t) =>
      /^(el|la|los|las|un|una|unos|unas|o|a|os|as|de|do|da|dos|das|del|the|an|esta|este|esto|esa|ese|particular|si|con|por|para|que|y|e|en|at|of|and|or|no|ha|indicado)$/i.test(
        t
      )
    ).length / tokens.length;
  if (stopwordHeavy > 0.45) return false;

  if (
    tokens.every((t) =>
      /^(el|la|los|las|un|una|unos|unas|o|a|os|as|de|do|da|dos|das|the|an|esta|este|esto|esa|ese|particular)$/i.test(
        t
      )
    )
  ) {
    return false;
  }

  if (!/[a-záéíóúñãõâêôàü]/i.test(raw)) return false;

  // Brands alone are not owner entities unless caller opts in
  if (!opts.allowBrandAsEntity && looksLikeMajorBrandName(raw)) {
    // Allow "Marriott International" style only if legal/corp suffix present
    if (!LEGAL_HINT_RE.test(raw) || tokens.length < 2) return false;
  }

  const hasLegal = LEGAL_HINT_RE.test(raw);
  const capitalTokens = raw
    .split(/\s+/)
    .filter((t) => /^[A-ZÁÉÍÓÚÑÃÕÂÊÔÀ]/.test(t)).length;

  // Require company-like signal OR multi-token proper name
  if (hasLegal) return true;
  if (capitalTokens >= 2 && tokens.length >= 2 && tokens.length <= 6) {
    // Reject if starts with preposition / review marker
    if (/^(at|en|del|de|el|la|un|una|the|a|an)\b/i.test(raw)) return false;
    return true;
  }
  // Single token: only institutional/sponsor-style or allowBrand; not generic nouns
  if (tokens.length === 1 && capitalTokens === 1 && raw.length >= 5) {
    if (opts.allowBrandAsEntity) return true;
    if (looksLikeInstitutionalSponsorName(raw)) return true;
    // Reject ALL-CAPS generic fragments (e.g. VIAJES from nav menus)
    if (raw === raw.toUpperCase() && raw.length <= 12) return false;
    return false;
  }
  return false;
}

/**
 * Soft score 0–1 for entity plausibility (used in multi-dimension confidence).
 * @param {string} name
 */
export function entityPlausibilityScore(name) {
  if (!isPlausibleLegalEntityName(name)) return 0;
  const raw = String(name || "").trim();
  let score = 0.55;
  if (LEGAL_HINT_RE.test(raw)) score += 0.25;
  if (/\b(s\.?a\.?|llc|inc|ltd|de\s+c\.?v)\b/i.test(raw)) score += 0.1;
  if (looksLikeMajorBrandName(raw) && !LEGAL_HINT_RE.test(raw)) score -= 0.4;
  return Math.max(0, Math.min(1, score));
}
