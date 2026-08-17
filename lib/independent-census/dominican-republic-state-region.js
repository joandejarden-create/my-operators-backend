/**
 * Dominican Republic — High-confidence City → State / Region (province) map.
 *
 * Used for Hotel Property Census State / Region backfill only.
 * Never invents province from coordinates alone. Never writes Brand Explorer.
 *
 * Province names follow common DR administrative labels already present in
 * census city-state canonical lists (e.g. Distrito Nacional, La Altagracia).
 */

import {
  isDescriptorCity,
  normalizePlaceKey,
  canonicalCalaCity,
} from "../research-engine-v2/census-city-state-normalizer.js";

export const DR_STATE_REGION_MAP_VERSION = "dr-city-to-province-v2";

/** Canonical province / Distrito labels for DR State / Region. */
export const DR_PROVINCES = Object.freeze([
  "Distrito Nacional",
  "Santo Domingo",
  "La Altagracia",
  "Puerto Plata",
  "Samaná",
  "La Romana",
  "El Seibo",
  "Santiago",
  "La Vega",
  "Dajabón",
  "San Pedro de Macorís",
  "María Trinidad Sánchez",
  "Hato Mayor",
  "Barahona",
  "Pedernales",
  "Espaillat",
]);

/**
 * City token (normalized key) → province.
 * Only unambiguous tourism / metro cities — steward the rest.
 */
export const DR_CITY_TO_PROVINCE = Object.freeze({
  // La Altagracia
  "punta cana": "La Altagracia",
  bavaro: "La Altagracia",
  "bávaro": "La Altagracia",
  "cap cana": "La Altagracia",
  "uvero alto": "La Altagracia",
  higuey: "La Altagracia",
  "higüey": "La Altagracia",
  // Distrito Nacional (capital city)
  "santo domingo": "Distrito Nacional",
  // Santo Domingo province (east metro)
  "boca chica": "Santo Domingo",
  "juan dolio": "San Pedro de Macorís",
  // Puerto Plata
  "puerto plata": "Puerto Plata",
  sosua: "Puerto Plata",
  "sosúa": "Puerto Plata",
  cabarete: "Puerto Plata",
  maimon: "Puerto Plata",
  "maimón": "Puerto Plata",
  marapica: "Puerto Plata",
  // Samaná
  samana: "Samaná",
  "samaná": "Samaná",
  "las terrenas": "Samaná",
  "las galeras": "Samaná",
  "el valle": "Samaná",
  "el valle samana": "Samaná",
  // La Romana
  "la romana": "La Romana",
  bayahibe: "La Romana",
  "bayahibe": "La Romana",
  bahahibe: "La Romana",
  // El Seibo / east
  miches: "El Seibo",
  // Santiago / Cibao
  santiago: "Santiago",
  "santiago de los caballeros": "Santiago",
  // La Vega
  jarabacoa: "La Vega",
  constanza: "La Vega",
  // Border
  dajabon: "Dajabón",
  "dajabón": "Dajabón",
  // Southwest
  barahona: "Barahona",
});

/**
 * @param {string} cityRaw
 * @returns {{ ok: boolean, province?: string, city_canonical?: string, reason: string, confidence: 'High'|'none' }}
 */
export function resolveDominicanRepublicStateRegion(cityRaw) {
  const raw = String(cityRaw || "").trim();
  if (!raw) {
    return { ok: false, reason: "city_blank", confidence: "none" };
  }
  if (isDescriptorCity(raw) || /^unknown$/i.test(raw)) {
    return { ok: false, reason: "city_descriptor_or_unknown", confidence: "none" };
  }

  // "Bavaro - La Altagracia" / "City, Province" — if right side is already a province
  if (/[-–,|]/.test(raw) || raw.includes(",")) {
    const parts = raw.split(/\s*[-–,|]\s*/).map((p) => p.trim()).filter(Boolean);
    if (parts.length >= 2) {
      const rightKey = normalizePlaceKey(parts[parts.length - 1]);
      const rightProvince = DR_PROVINCES.find(
        (p) => normalizePlaceKey(p) === rightKey
      );
      if (rightProvince) {
        const leftCity = parts.slice(0, -1).join(" - ");
        return {
          ok: true,
          province: rightProvince,
          city_canonical: canonicalCalaCity(leftCity) || leftCity,
          reason: "city_field_contains_province",
          confidence: "High",
          suggest_city_cleanup: leftCity,
        };
      }
    }
  }

  const key = normalizePlaceKey(raw);
  const canonCity = canonicalCalaCity(raw) || raw;
  const canonKey = normalizePlaceKey(canonCity);

  // Strip trailing ", Samana" style suffixes already handled above; try full key
  let province = DR_CITY_TO_PROVINCE[key] || DR_CITY_TO_PROVINCE[canonKey];

  // "El Valle, Samana" → try last token as province hint + first as city
  if (!province && raw.includes(",")) {
    const [left, right] = raw.split(",").map((s) => s.trim());
    const leftKey = normalizePlaceKey(left);
    province = DR_CITY_TO_PROVINCE[leftKey];
    if (province) {
      return {
        ok: true,
        province,
        city_canonical: canonicalCalaCity(left) || left,
        reason: "city_comma_left_token",
        confidence: "High",
        suggest_city_cleanup: left,
      };
    }
    // right might be province name
    const rightProv = DR_PROVINCES.find(
      (p) => normalizePlaceKey(p) === normalizePlaceKey(right)
    );
    if (rightProv) {
      return {
        ok: true,
        province: rightProv,
        city_canonical: canonicalCalaCity(left) || left,
        reason: "city_comma_province_suffix",
        confidence: "High",
        suggest_city_cleanup: left,
      };
    }
  }

  if (!province) {
    return {
      ok: false,
      reason: "city_not_in_high_confidence_map",
      city_canonical: canonCity,
      confidence: "none",
    };
  }

  return {
    ok: true,
    province,
    city_canonical: canonCity,
    reason: "city_to_province_map",
    confidence: "High",
  };
}
