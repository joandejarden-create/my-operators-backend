/**
 * High-confidence hotel/resort development announcement detection.
 * Requires hospitality context AND development/create/build/project/plans/JV/partner language.
 * Generic "announced" alone is never enough (avoids travel-app / spa / tour-package noise).
 */

const HOTEL_CTX_RE =
  /\b(?:hotel|hotels|resort|resorts|hospitality|lodging|boutique\s+resort|hoteleiro|hotelero|conrad|hilton|marriott|hyatt|ihg|autograph|curio|andaz|regenta|holiday\s+inn|wyndham|accor)\b/i;

const DEV_ACTION_RE =
  /\b(?:announc(?:e|es|ed|ing)|plans?\s+to\s+develop|plans?\s+for|develop(?:s|ed|ing|ment)?|launches?\s+development|development\s+launched|unveils?|unveiled|partners?\s+with|joint\s+venture|\bjv\b|co-?develop|project|proposed|pipeline|mixed[- ]use|component|convert(?:s|ed|ing)?|conversion|adaptive[- ]reuse|expands?)\b/i;

/**
 * Explicit announcement × hotel-development combinations (high confidence).
 * Word order variants included ("resort announced" and "announced … resort").
 */
const HOTEL_DEV_ANNOUNCE_COMBOS_RE =
  /\b(?:(?:new\s+)?(?:hotel|resort|hotels|resorts)\s+announced|announc(?:e|es|ed|ing)\s+(?:a\s+)?(?:new\s+)?(?:hotel|resort|hotels|resorts)|announc(?:e|es|ed|ing)\s+plans?\s+for\s+(?:a\s+)?(?:new\s+)?(?:hotel|resort)|plans?\s+to\s+develop\s+(?:a\s+)?(?:new\s+)?(?:hotel|resort)|partners?\s+with\b[\s\S]{0,120}?\b(?:to\s+)?(?:develop|build|create)\b[\s\S]{0,80}?\b(?:hotel|resort)|joint\s+venture\b[\s\S]{0,140}?\b(?:develop|build|create)\b[\s\S]{0,80}?\b(?:hotel|resort)|(?:develop|build|create)\b[\s\S]{0,80}?\b(?:hotel|resort)[\s\S]{0,80}?\bjoint\s+venture|developer\s+unveils?\s+(?:a\s+)?(?:new\s+)?(?:hotel|resort)|(?:new\s+)?hospitality\s+component|adds?\s+(?:a\s+)?hospitality\s+component|adding\s+(?:a\s+)?hospitality\s+component|boutique\s+resort\s+within\s+(?:the\s+)?(?:development|project|mixed[- ]use)|(?:hotel|resort)\s+component\s+of\s+(?:the\s+)?(?:mixed[- ]use\s+)?development|(?:includes?|including)\s+(?:a\s+)?(?:new\s+)?(?:luxury\s+)?(?:boutique\s+)?(?:hotel|resort)\s+(?:within|inside|as\s+part\s+of))\b/i;

/**
 * @param {string} text — title + summary (+ enriched article text when available)
 * @returns {boolean}
 */
export function isHotelDevelopmentAnnouncement(text = "") {
  const t = String(text || "").trim();
  if (!t) return false;
  if (!HOTEL_CTX_RE.test(t)) return false;
  if (!DEV_ACTION_RE.test(t)) return false;
  return HOTEL_DEV_ANNOUNCE_COMBOS_RE.test(t);
}

export { HOTEL_CTX_RE, HOTEL_DEV_ANNOUNCE_COMBOS_RE };
