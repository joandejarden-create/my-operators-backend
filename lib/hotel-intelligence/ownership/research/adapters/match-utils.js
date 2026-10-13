/**
 * Shared hotel↔registry name matching helpers for ownership country adapters.
 */

export function normalizeMatchText(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(
      /\b(hotel|resort|inn|suites?|by|the|el|la|los|las|de|do|da|del)\b/g,
      " "
    )
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {string} hotelName
 * @param {string} registryName
 * @param {string} [hotelCity]
 * @param {string} [registryCity]
 * @returns {number} 0–1 score
 */
export function scoreHotelRegistryMatch(
  hotelName,
  registryName,
  hotelCity = "",
  registryCity = ""
) {
  const a = normalizeMatchText(hotelName);
  const b = normalizeMatchText(registryName);
  if (!a || !b) return 0;
  if (a === b) return 1;
  const tokensA = new Set(a.split(" ").filter((t) => t.length > 2));
  const tokensB = new Set(b.split(" ").filter((t) => t.length > 2));
  if (!tokensA.size || !tokensB.size) return 0;
  let inter = 0;
  for (const t of tokensA) if (tokensB.has(t)) inter += 1;
  const union = new Set([...tokensA, ...tokensB]).size;
  let score = inter / union;
  if (a.includes(b) || b.includes(a)) score = Math.max(score, 0.85);
  const ca = normalizeMatchText(hotelCity);
  const cb = normalizeMatchText(registryCity);
  if (ca && cb && ca === cb) score = Math.min(1, score + 0.1);
  else if (ca && cb && ca !== cb) score *= 0.7;
  return score;
}

/**
 * City-gated registry match (P1.7). City mismatch never counts as a confident hit.
 * @returns {{ score: number, city_ok: boolean, eligible: boolean }}
 */
export function scoreHotelRegistryMatchCityGated(
  hotelName,
  registryName,
  hotelCity = "",
  registryCity = "",
  opts = {}
) {
  const threshold = opts.threshold ?? 0.72;
  const hotelCityKey = normalizeMatchText(hotelCity);
  const registryCityKey = normalizeMatchText(registryCity);
  const cityOk =
    Boolean(hotelCityKey) &&
    Boolean(registryCityKey) &&
    (hotelCityKey === registryCityKey ||
      hotelCityKey.includes(registryCityKey) ||
      registryCityKey.includes(hotelCityKey));
  const score = scoreHotelRegistryMatch(
    hotelName,
    registryName,
    hotelCity,
    registryCity
  );
  return {
    score,
    city_ok: cityOk,
    eligible: cityOk && score >= threshold,
  };
}
