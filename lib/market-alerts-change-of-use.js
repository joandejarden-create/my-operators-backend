/**
 * Hotel → non-hotel change-of-use detection (V1.2.1).
 * Distinct from office/building → hotel adaptive reuse (valid Early Signal).
 */

const HOTEL_TO_NON_HOTEL_RE =
  /\b(?:(?:distressed|vacant|closed|former|underutilized|two|multiple|several)\s+(?:hotel|resort|lodging|hospitality)(?:\s+properties?|\s+property|\s+buildings?|\s+sites?)?|(?:hotel|resort|lodging|hospitality(?:\s+property|\s+properties|\s+building|\s+buildings|\s+site|\s+sites)?).{0,100}(?:convert(?:ed|ing|s)?|redevelop(?:ed|ing|s)?|transform(?:ed|ing|s)?|change(?:d|s)?|turn(?:ed|s)?|repurpose(?:d|s)?|develop(?:ed|ing|s)?).{0,50}(?:into|to|for|as)\s+(?:affordable\s+housing|apartments?|apartment\s+(?:building|tower|complex|development)|residential(?:\s+housing|\s+development|\s+units?|\s+conversion)?|housing(?:\s+development|\s+project|\s+units?)?|student\s+housing|senior\s+housing|homeless\s+housing|condos?|condominiums?|multifamily|multi-family)|(?:affordable\s+housing|apartments?|residential(?:\s+housing|\s+development)?|housing\s+development|student\s+housing|senior\s+housing).{0,80}(?:former|closed|distressed|vacant|ex[-\s]?hotel|hotel\s+properties?|hotel\s+property|hotel\s+building)|(?:buy|acquire|acquiring|purchase|purchasing|take\s+over).{0,60}(?:distressed|vacant|closed|former|two|multiple).{0,40}(?:hotel|resort|lodging).{0,80}(?:affordable\s+housing|apartments?|residential|housing)|(?:hotel|resort).{0,40}(?:demolish(?:ed|es|ing)?|demolition|razed|torn\s+down).{0,50}(?:for|to\s+make\s+way\s+for|into).{0,40}(?:apartments?|housing|residential|condos?)|(?:approved|approval|permit(?:ted)?|rezoned?).{0,40}(?:former|closed|ex[-\s]?)\s*hotel.{0,40}(?:residential|apartments?|housing))\b/i;

const OFFICE_TO_HOTEL_RE =
  /\b(?:office[-\s]?to[-\s]?hotel|adaptive\s+reuse|(?:office|warehouse|vacant|empty|commercial)\s+(?:building|tower|block|property|complex|center|centre)).{0,120}(?:to|into|become|becoming|transform(?:ing|s)?|convert(?:ing|s)?|second\s+act\s+as).{0,50}(?:hotel|resort|lodging|jw\s+marriott|marriott|hilton|hyatt)\b/i;

/**
 * @param {string} text
 * @returns {boolean}
 */
export function isHotelToNonHotelChangeOfUse(text) {
  const t = String(text || "");
  if (!t.trim()) return false;
  if (OFFICE_TO_HOTEL_RE.test(t)) return false;
  if (/\boffice.{0,80}(?:hotel|resort|jw\s+marriott)\b/i.test(t)) return false;
  return HOTEL_TO_NON_HOTEL_RE.test(t);
}

/**
 * Mixed-use with explicit future hotel component — do not treat as pure hotel exit.
 * @param {string} text
 * @returns {boolean}
 */
export function hasFutureHotelComponent(text) {
  return /\b(?:includes?|including|with|plus|alongside|featuring).{0,50}(?:\d+[-\s]?(?:room|key)\s+)?(?:hotel|resort)(?:\s+component|\s+tower|\s+rooms?|\s+rooms?\s+and|\s+and\s+residences)?\b/i.test(
    String(text || "")
  );
}

/**
 * @param {string} text
 * @returns {boolean}
 */
export function isOfficeToHotelAdaptiveReuse(text) {
  return OFFICE_TO_HOTEL_RE.test(String(text || ""));
}
