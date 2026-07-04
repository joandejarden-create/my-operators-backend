/**
 * Canonical per-brand Match Score New lookup from a deal's matchScoresNewByBrand map.
 * Used by API and client-side helpers — do not fall back to deal-level score when a brand is requested.
 */

/** @param {unknown} name */
export function normalizeBrandLookupKey(name) {
  if (name == null || typeof name !== "string") return "";
  return String(name)
    .replace(/[\u0000-\u001f\u007f-\u009f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

/**
 * @param {Record<string, unknown>|null|undefined} scoresMap
 * @param {string|null|undefined} brand — when set, never uses dealLevelScore
 * @param {{ dealLevelScore?: number|null|string }} [options]
 * @returns {number|null}
 */
export function lookupMatchScoreNewByBrand(scoresMap, brand, options = {}) {
  const { dealLevelScore = null } = options;
  const brandTrimmed = brand != null ? String(brand).trim() : "";

  if (!scoresMap || typeof scoresMap !== "object") {
    if (!brandTrimmed && dealLevelScore != null && dealLevelScore !== "") {
      const n = Number(dealLevelScore);
      return Number.isNaN(n) ? null : n;
    }
    return null;
  }

  if (brandTrimmed) {
    if (scoresMap[brandTrimmed] != null && scoresMap[brandTrimmed] !== "") {
      const exact = Number(scoresMap[brandTrimmed]);
      if (!Number.isNaN(exact)) return exact;
    }
    const want = normalizeBrandLookupKey(brandTrimmed);
    for (const key of Object.keys(scoresMap)) {
      if (normalizeBrandLookupKey(key) !== want) continue;
      if (scoresMap[key] == null || scoresMap[key] === "") continue;
      const n = Number(scoresMap[key]);
      if (!Number.isNaN(n)) return n;
    }
    return null;
  }

  if (dealLevelScore != null && dealLevelScore !== "") {
    const n = Number(dealLevelScore);
    return Number.isNaN(n) ? null : n;
  }
  return null;
}
