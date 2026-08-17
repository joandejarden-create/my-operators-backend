/**
 * Dealality commercial Market map for Hotel Property Census.
 * Thin expansion layer over census-region-market-map — never invent markets.
 */

import {
  CITY_TO_MARKET_OVERRIDE,
  RECOGNIZED_MARKET_CITIES,
  resolveMarketFromCity as resolveMarketFromCityCore,
  normGeoKey,
} from "./census-region-market-map.js";

export const COMMERCIAL_MARKET_MAP_VERSION = "census-commercial-market-map-v1";

/**
 * Additional city → Market overrides for commercial completion mission.
 * Prefer existing Dealality vocabulary (Cancún, Los Cabos, …) — not slash labels.
 */
export const COMMERCIAL_CITY_TO_MARKET = Object.freeze({
  ...CITY_TO_MARKET_OVERRIDE,
  // Riviera Maya corridor → Cancún commercial market
  "playa del carmen": "Cancún",
  tulum: "Cancún",
  akumal: "Cancún",
  "puerto morelos": "Cancún",
  cozumel: "Cancún",
  "isla mujeres": "Cancún",
  // Bajío / secondary MX metros (city-as-market)
  leon: "León",
  "león": "León",
  mexicali: "Mexicali",
  "san luis potosi": "San Luis Potosí",
  "san luis potosí": "San Luis Potosí",
  queretaro: "Querétaro",
  "querétaro": "Querétaro",
  merida: "Mérida",
  "mérida": "Mérida",
  puebla: "Puebla",
  guadalajara: "Guadalajara",
  monterrey: "Monterrey",
  veracruz: "Veracruz",
  saltillo: "Saltillo",
  "tuxtla gutierrez": "Tuxtla Gutiérrez",
  "tuxtla gutiérrez": "Tuxtla Gutiérrez",
  villahermosa: "Villahermosa",
  irapuato: "Irapuato",
  zacatecas: "Zacatecas",
  cuernavaca: "Cuernavaca",
  culiacan: "Culiacán",
  "culiacán": "Culiacán",
  tampico: "Tampico",
  "ciudad juarez": "Ciudad Juárez",
  "ciudad juárez": "Ciudad Juárez",
  "san miguel de allende": "San Miguel de Allende",
  "ciudad delicias": "Ciudad Delicias",
  delicias: "Ciudad Delicias",
  hermosillo: "Hermosillo",
  tijuana: "Tijuana",
  torreon: "Torreón",
  "torreón": "Torreón",
  mazatlan: "Mazatlán",
  "mazatlán": "Mazatlán",
  chihuahua: "Chihuahua",
  acapulco: "Acapulco",
  morelia: "Morelia",
  toluca: "Toluca",
  aguascalientes: "Aguascalientes",
  "piedras negras": "Piedras Negras",
  obregon: "Ciudad Obregón",
  "ciudad obregon": "Ciudad Obregón",
  "ciudad obregón": "Ciudad Obregón",
  monclova: "Monclova",
  cordoba: "Córdoba",
  "córdoba": "Córdoba",
  guadalupe: "Monterrey",
  "nuevo leon": "Monterrey",
  "nuevo león": "Monterrey",
  "cuajimalpa de morelos": "Mexico City",
  cuajimalpa: "Mexico City",
  chapala: "Guadalajara",
  // CO / PA / CR satellites
  barranquilla: "Barranquilla",
  cali: "Cali",
  bogota: "Bogotá",
  "bogotá": "Bogotá",
  medellin: "Medellín",
  "medellín": "Medellín",
  cartagena: "Cartagena",
  cucuta: "Cúcuta",
  "cúcuta": "Cúcuta",
  amador: "Panama City",
  panama: "Panama City",
  chame: "Panama City",
  coronado: "Panama City",
  colon: "Colón",
  "colón": "Colón",
  chitre: "Chitré",
  "chitré": "Chitré",
  "cerro punta": "Cerro Punta",
  lima: "Lima",
  "buenos aires": "Buenos Aires",
  "sao paulo": "São Paulo",
  "são paulo": "São Paulo",
  "rio de janeiro": "Rio de Janeiro",
  "santo domingo": "Santo Domingo",
});

/**
 * Resolve Market for commercial-fields mission.
 * San José requires Costa Rica country to avoid Mexico/other collisions.
 */
export function resolveCommercialMarket(input = {}) {
  const city = String(input.city || "").trim();
  const country = String(input.country || "").trim();
  const key = normGeoKey(city);
  if (!key) return { ok: false, reason: "missing_city" };

  const countryKey = normGeoKey(country);
  if (key === "san jose" || key === "san josé") {
    if (countryKey === "costa rica") {
      return {
        ok: true,
        market: "San José",
        confidence: "High",
        method: "commercial_market_san_jose_cr",
      };
    }
    return { ok: false, reason: "san_jose_country_ambiguous", steward_needed: true };
  }

  if (COMMERCIAL_CITY_TO_MARKET[key]) {
    return {
      ok: true,
      market: COMMERCIAL_CITY_TO_MARKET[key],
      confidence: "High",
      method: "commercial_city_market_map",
    };
  }
  for (const [k, market] of Object.entries(COMMERCIAL_CITY_TO_MARKET)) {
    if (normGeoKey(k) === key) {
      return {
        ok: true,
        market,
        confidence: "High",
        method: "commercial_city_market_map",
      };
    }
  }

  const core = resolveMarketFromCityCore({ city, country });
  if (core.ok) return { ...core, method: core.method || "region_market_map" };

  if (RECOGNIZED_MARKET_CITIES.has(key)) {
    return {
      ok: true,
      market: city,
      confidence: "High",
      method: "recognized_market_city",
    };
  }

  return { ok: false, reason: "market_mapping_backlog", steward_needed: true };
}

export { normGeoKey, RECOGNIZED_MARKET_CITIES };
