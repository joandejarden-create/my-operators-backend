/**
 * Negative / boilerplate context for ownership claim extraction.
 * Real failure modes from ownership-eval-v1 (OTA, TripAdvisor, booking terms).
 */

/** OTA / booking / policy language — reject OWNED_BY near these (EN/ES/PT). */
export const OWNERSHIP_NEGATIVE_CONTEXT_RE = [
  // EN property-features (not ownership)
  /\bproperty\s+(offers|features|has|policies|accepts|is\s+located|provides|includes|reserves\s+the\s+right)\b/i,
  /\bproperty\s+owner\s+may\s+contact\b/i,
  /\b(cancellation|booking|payment|check[\s-]?in|check[\s-]?out)\s+(policy|policies|terms|conditions)\b/i,
  /\btax(?:es)?\s+(?:and|&)\s+fees?\b/i,
  /\b(credit\s+card|deposit|non[\s-]?refundable)\b/i,
  // ES OTA / listing boilerplate (eval FPs)
  /\b(el|la|un|una)\s+alojamiento\b/i,
  /\bno\s+ha\s+indicado\s+si\s+el\b/i,
  /\bdetector\s+de\s+humo\b/i,
  /\bplaza\s+de\s+parking\b/i,
  /\brespondi[oó]\s+a\s+esta\s+(opini[oó]n|reseña)\b/i,
  /\bresponded\s+to\s+this\s+review\b/i,
  /\bpol[ií]ticas?\s+de\s+(cancelaci[oó]n|reserva|pago)\b/i,
  /\bcondiciones\s+de\s+(reserva|cancelaci[oó]n|pago)\b/i,
  /\bpropiedad\s+(ofrece|cuenta\s+con|incluye|acepta|est[aá]\s+ubicad|dispone|reserva\s+el\s+derecho)\b/i,
  /\bla\s+propiedad\s+(ofrece|cuenta|incluye|acepta|est[aá])\b/i,
  // PT booking / review
  /\b(o|um)\s+alojamento\b/i,
  /\balterar\s+as\s+datas\s+da\s+estadia\b/i,
  /\bpol[ií]ticas?\s+de\s+(cancelamento|reserva|pagamento)\b/i,
  /\bpropriedade\s+(oferece|conta\s+com|inclui|aceita|est[aá]\s+localizad)\b/i,
];

/** Fragments that indicate review / OTA response, not ownership. */
export const REVIEW_RESPONSE_FRAGMENT_RE =
  /\b(responded\s+to\s+this|respondi[oó]\s+a\s+esta|reviewresponded|opini[oó]nrespondida|hace\s+\d+\s+(a[nñ]os?|meses?|d[ií]as?)|hace\s+un\s+(a[nñ]o|mes))\b/i;

/**
 * Window around a match — reject if negative context present.
 * @param {string} fullText
 * @param {number} matchIndex
 * @param {number} matchLength
 * @param {number} [radius]
 */
export function hasNegativeOwnershipContext(
  fullText,
  matchIndex,
  matchLength,
  radius = 160
) {
  const start = Math.max(0, matchIndex - radius);
  const end = Math.min(
    fullText.length,
    matchIndex + matchLength + radius
  );
  const window = fullText.slice(start, end);
  if (REVIEW_RESPONSE_FRAGMENT_RE.test(window)) return true;
  for (const re of OWNERSHIP_NEGATIVE_CONTEXT_RE) {
    if (re.test(window)) return true;
  }
  return false;
}

/**
 * Institutional / PE sponsor names — economic control ≠ PropCo legal owner.
 */
export const SPONSOR_NAME_HINT_RE =
  /\b(blackstone|brookfield|kkr|apollo|carlyle|oaktree|starwood\s+capital|bain\s+capital|goldman\s+sachs|jp\s*morgan|morgan\s+stanley|advent|warburg|lone\s+star|cerberus|aew|prologis|bre\s+properties|host\s+hotels|park\s+hotels|rlj|apple\s+hospitality|pebbled?beach|sovereign\s+wealth|pension\s+fund|fundo\s+de\s+pens[aã]o)\b/i;

/**
 * Major hotel brands — never OWNED_BY from brand mention alone.
 */
export const MAJOR_BRAND_NAME_RE =
  /\b(marriott|hilton|hyatt|ihg|intercontinental|holiday\s+inn|crowne\s+plaza|accor|ibis|novotel|mercure|wyndham|choice\s+hotels|radisson|meli[aá]|barcel[oó]|riu|karisma|four\s+seasons|rosewood|mandarin\s+oriental|ritz[\s-]?carlton|st\.?\s*regis|westin|sheraton|jw\s+marriott|courtyard|aloft|element|kimpton|viceroy|aman|six\s+senses|one\s*&?\s*only|best\s+western|nh\s+hotels?|nhow|hard\s+rock|palladium|dreams|secrets|breathless|sunscape|zo[eë]try)\b/i;

/**
 * @param {string} name
 */
export function looksLikeMajorBrandName(name) {
  return MAJOR_BRAND_NAME_RE.test(String(name || ""));
}

/**
 * @param {string} name
 */
export function looksLikeInstitutionalSponsorName(name) {
  return SPONSOR_NAME_HINT_RE.test(String(name || ""));
}
