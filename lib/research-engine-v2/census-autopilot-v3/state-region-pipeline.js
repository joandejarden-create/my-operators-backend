/**
 * State / Region pipeline — independent deterministic derivation (no Cvent/legacy).
 */

import { resolveDominicanRepublicStateRegion } from "../../independent-census/dominican-republic-state-region.js";

/** Mexico federal entities (normalized labels). */
export const MEXICO_STATES = Object.freeze([
  "Aguascalientes",
  "Baja California",
  "Baja California Sur",
  "Campeche",
  "Chiapas",
  "Chihuahua",
  "Ciudad de México",
  "Coahuila",
  "Colima",
  "Durango",
  "Guanajuato",
  "Guerrero",
  "Hidalgo",
  "Jalisco",
  "México",
  "Michoacán",
  "Morelos",
  "Nayarit",
  "Nuevo León",
  "Oaxaca",
  "Puebla",
  "Querétaro",
  "Quintana Roo",
  "San Luis Potosí",
  "Sinaloa",
  "Sonora",
  "Tabasco",
  "Tamaulipas",
  "Tlaxcala",
  "Veracruz",
  "Yucatán",
  "Zacatecas",
]);

const MX_ALIASES = Object.freeze({
  "mexico city": "Ciudad de México",
  cdmx: "Ciudad de México",
  "distrito federal": "Ciudad de México",
  "estado de mexico": "México",
  "edo de mexico": "México",
  "nuevo leon": "Nuevo León",
  queretaro: "Querétaro",
  michoacan: "Michoacán",
  yucatan: "Yucatán",
  "quintana roo": "Quintana Roo",
  "baja california sur": "Baja California Sur",
  "san luis potosi": "San Luis Potosí",
  cancun: "Quintana Roo",
  "playa del carmen": "Quintana Roo",
  tulum: "Quintana Roo",
  "cabo san lucas": "Baja California Sur",
  "los cabos": "Baja California Sur",
  "puerto vallarta": "Jalisco",
  guadalajara: "Jalisco",
  monterrey: "Nuevo León",
  merida: "Yucatán",
});

function norm(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {{ country?: string, city?: string, address?: string, official_state?: string }} input
 */
export function resolveStateRegion(input = {}) {
  const country = String(input.country || "").trim();
  const city = String(input.city || "").trim();
  const official = String(input.official_state || "").trim();

  if (official) {
    return {
      raw_state_region: official,
      normalized_state_region: official,
      derivation: "official_structured",
      source: "official",
      confidence: "High",
      ok: true,
    };
  }

  if (country === "Dominican Republic" || country === "DR") {
    const dr = resolveDominicanRepublicStateRegion(city);
    if (dr.ok) {
      return {
        raw_state_region: city,
        normalized_state_region: dr.province,
        derivation: "dealality_dr_city_to_province",
        source: "dealality_geography",
        confidence: "High",
        ok: true,
      };
    }
  }

  if (country === "Mexico") {
    const n = norm(city);
    if (MX_ALIASES[n]) {
      return {
        raw_state_region: city,
        normalized_state_region: MX_ALIASES[n],
        derivation: "dealality_mexico_city_alias_to_state",
        source: "dealality_geography",
        confidence: "High",
        ok: true,
      };
    }
    for (const st of MEXICO_STATES) {
      if (norm(st) === n) {
        return {
          raw_state_region: city,
          normalized_state_region: st,
          derivation: "dealality_mexico_city_equals_state",
          source: "dealality_geography",
          confidence: "Medium",
          ok: true,
          note: "City label equals Mexican state/entity name — common in directory feeds",
        };
      }
    }
  }

  // Brazil / others: do not invent from postal codes
  if (/^\d/.test(city) || /\d{4,}/.test(city)) {
    return {
      raw_state_region: city,
      normalized_state_region: null,
      derivation: "unresolved_postal_or_admin_as_city",
      source: null,
      confidence: "No Match",
      ok: false,
    };
  }

  return {
    raw_state_region: city || null,
    normalized_state_region: null,
    derivation: "unresolved",
    source: null,
    confidence: "No Match",
    ok: false,
  };
}
