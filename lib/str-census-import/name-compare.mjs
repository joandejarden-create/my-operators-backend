import { normalizeKey } from "./normalize.mjs";

/** Aggressive hotel name normalization for STR vs census matching. */
export function normalizeHotelName(raw) {
  return normalizeKey(raw)
    .replace(/\bthe\b/g, "")
    .replace(/\bhotel\b/g, "")
    .replace(/\bby\b/g, "")
    .replace(/\binn\b/g, "")
    .replace(/\bresort\b/g, "")
    .replace(/\s*&\s*/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const NAME_STOPWORDS = new Set([
  "the",
  "a",
  "an",
  "and",
  "or",
  "of",
  "at",
  "by",
  "hotel",
  "inn",
  "resort",
  "suites",
  "suite",
  "marriott",
  "wyndham",
  "hilton",
  "hyatt",
  "ihg",
  "accor",
  "radisson",
  "trademark",
  "collection",
  "autograph",
  "inclusive",
  "all",
  "portfolio",
  "tribute",
  "curio",
  "tapestry",
  "edition",
  "luxury",
  "boutique",
]);

function significantNameTokens(raw) {
  return normalizeHotelName(raw)
    .split(" ")
    .filter((t) => t.length > 2 && !NAME_STOPWORDS.has(t));
}

export function hotelNamesEquivalent(a, b) {
  const x = normalizeHotelName(a);
  const y = normalizeHotelName(b);
  if (!x || !y) return false;
  if (x === y) return true;
  if (x.includes(y) || y.includes(x)) return true;
  return false;
}

/**
 * Looser match: city/country must match (caller checks); names share most core tokens.
 * Catches Marriott wording, punctuation, Hotel prefix, Autograph Collection, etc.
 */
export function hotelNamesLooselyEquivalent(a, b) {
  if (hotelNamesEquivalent(a, b)) return true;

  const ta = significantNameTokens(a);
  const tb = significantNameTokens(b);
  if (!ta.length || !tb.length) return false;

  const setA = new Set(ta);
  const setB = new Set(tb);
  let inter = 0;
  for (const t of setA) {
    if (setB.has(t)) inter++;
  }

  const union = new Set([...setA, ...setB]).size;
  const jaccard = union > 0 ? inter / union : 0;
  const minSize = Math.min(setA.size, setB.size);
  const coverage = minSize > 0 ? inter / minSize : 0;

  return jaccard >= 0.45 || coverage >= 0.65;
}

export function locationEquivalent(excel, census) {
  const cityOk =
    !normalizeKey(excel.city) ||
    !normalizeKey(census.city) ||
    normalizeKey(excel.city) === normalizeKey(census.city);
  const countryOk =
    !normalizeKey(excel.country) ||
    !normalizeKey(census.country) ||
    normalizeKey(excel.country) === normalizeKey(census.country);
  return cityOk && countryOk;
}
