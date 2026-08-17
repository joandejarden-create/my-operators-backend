/**
 * Controlled Country → Continent / Sub-Continent and City → Market maps
 * for Hotel Property Census (Dealality Clean Core geography).
 *
 * Authority: founder clean-census market geography rules (not STR).
 * Never infer Continent/Sub-Continent/Market from hotel name alone.
 */

export const CENSUS_GEO_FIELDS = Object.freeze({
  continent: "Continent",
  subContinent: "Sub-Continent",
  market: "Market",
  submarket: "Submarket",
  /** Legacy combined enrichment field — do not dual-write unless explicitly requested */
  marketSubmarketCombined: "Market / Submarket",
});

/** Continent single-select options for Hotel Property Census. */
export const CONTINENT_OPTIONS = Object.freeze([
  "North America",
  "South America",
  "Europe",
  "Africa",
  "Asia",
  "Oceania",
  "Antarctica",
]);

/**
 * Sub-Continent options for Hotel Property Census Clean Core.
 * Note: Mexico uses Sub-Continent = "Mexico" (not legacy Hotel Census "North America").
 */
export const SUB_CONTINENT_OPTIONS = Object.freeze([
  "Mexico",
  "Caribbean",
  "Central America",
  "South America",
  "North America",
]);

/**
 * Country → { continent, subContinent } for CALA production census.
 * Keys are normalized via normCountry().
 */
export const COUNTRY_TO_CONTINENT_SUBCONTINENT = Object.freeze({
  mexico: { continent: "North America", subContinent: "Mexico" },
  "dominican republic": { continent: "North America", subContinent: "Caribbean" },
  "costa rica": { continent: "North America", subContinent: "Central America" },
  panama: { continent: "North America", subContinent: "Central America" },
  colombia: { continent: "South America", subContinent: "South America" },
  jamaica: { continent: "North America", subContinent: "Caribbean" },
  "puerto rico": { continent: "North America", subContinent: "Caribbean" },
  aruba: { continent: "North America", subContinent: "Caribbean" },
  curacao: { continent: "North America", subContinent: "Caribbean" },
  "curaçao": { continent: "North America", subContinent: "Caribbean" },
  bahamas: { continent: "North America", subContinent: "Caribbean" },
  "the bahamas": { continent: "North America", subContinent: "Caribbean" },
  brazil: { continent: "South America", subContinent: "South America" },
  chile: { continent: "South America", subContinent: "South America" },
  argentina: { continent: "South America", subContinent: "South America" },
  peru: { continent: "South America", subContinent: "South America" },
  ecuador: { continent: "South America", subContinent: "South America" },
  guatemala: { continent: "North America", subContinent: "Central America" },
  honduras: { continent: "North America", subContinent: "Central America" },
  "el salvador": { continent: "North America", subContinent: "Central America" },
  nicaragua: { continent: "North America", subContinent: "Central America" },
  belize: { continent: "North America", subContinent: "Central America" },
  cuba: { continent: "North America", subContinent: "Caribbean" },
  haiti: { continent: "North America", subContinent: "Caribbean" },
  barbados: { continent: "North America", subContinent: "Caribbean" },
  "trinidad and tobago": { continent: "North America", subContinent: "Caribbean" },
  "cayman islands": { continent: "North America", subContinent: "Caribbean" },
  "turks and caicos": { continent: "North America", subContinent: "Caribbean" },
  "turks and caicos islands": { continent: "North America", subContinent: "Caribbean" },
  venezuela: { continent: "South America", subContinent: "South America" },
  uruguay: { continent: "South America", subContinent: "South America" },
  paraguay: { continent: "South America", subContinent: "South America" },
  bolivia: { continent: "South America", subContinent: "South America" },
});

/**
 * City (canonical) → Market when City is a municipality inside a larger hotel market.
 * Cities not listed may Market = City when City is itself a recognized market.
 */
export const CITY_TO_MARKET_OVERRIDE = Object.freeze({
  // Mexico — Los Cabos metro
  "san jose del cabo": "Los Cabos",
  "san josé del cabo": "Los Cabos",
  "cabo san lucas": "Los Cabos",
  "los cabos": "Los Cabos",
  // DR — Punta Cana metro
  "cap cana": "Punta Cana",
  bavaro: "Punta Cana",
  "bávaro": "Punta Cana",
  "punta cana": "Punta Cana",
  // DR — other Dealality commercial markets (city → Market)
  "bayahibe": "La Romana",
  bahahibe: "La Romana",
  "juan dolio": "Santo Domingo",
  "boca chica": "Santo Domingo",
  sosua: "Puerto Plata",
  "sosúa": "Puerto Plata",
  cabarete: "Puerto Plata",
  maimon: "Puerto Plata",
  "maimón": "Puerto Plata",
  marapica: "Puerto Plata",
  "las terrenas": "Samaná",
  "las galeras": "Samaná",
  miches: "Miches",
  barahona: "Barahona",
  "puerto plata": "Puerto Plata",
  santiago: "Santiago",
  "santiago de los caballeros": "Santiago",
  higuey: "Punta Cana",
  "higüey": "Punta Cana",
  jarabacoa: "Jarabacoa",
  constanza: "Constanza",
  dajabon: "Dajabón",
  "dajabón": "Dajabón",
  // Mexico City aliases already normalize to Mexico City
  "ciudad de mexico": "Mexico City",
  "ciudad de méxico": "Mexico City",
  cdmx: "Mexico City",
  "mexico city": "Mexico City",
  // Panama
  "ciudad de panama": "Panama City",
  "ciudad de panamá": "Panama City",
  "panama city": "Panama City",
  // Mexico — Guadalajara / Monterrey metro satellites (city clean → parent market)
  zapopan: "Guadalajara",
  apodaca: "Monterrey",
  "san pedro garza garcia": "Monterrey",
  "san pedro garza garcía": "Monterrey",
  "san pedro tlaquepaque": "Guadalajara",
  tlaquepaque: "Guadalajara",
  "san nicolas de los garza": "Monterrey",
  "san nicolás de los garza": "Monterrey",
  guadalupe: "Monterrey",
  "nuevo leon": "Monterrey",
  "nuevo león": "Monterrey",
  "cuajimalpa de morelos": "Mexico City",
  cuajimalpa: "Mexico City",
  chapala: "Guadalajara",
  // Panama metro / satellite localities
  amador: "Panama City",
  panama: "Panama City",
  chame: "Panama City",
  coronado: "Panama City",
  "playa caracol": "Panama City",
  // Riviera Maya corridor label used as City → Cancún market (existing census Market vocabulary)
  "riviera maya": "Cancún",
});

/**
 * Cities that are themselves recognized hotel markets (Market may equal City).
 * Used only after City is clean — never from hotel name alone.
 */
export const RECOGNIZED_MARKET_CITIES = Object.freeze(
  new Set([
    "cancún",
    "cancun",
    "playa del carmen",
    "tulum",
    "los cabos",
    "san josé del cabo",
    "san jose del cabo",
    "cabo san lucas",
    "mexico city",
    "monterrey",
    "guadalajara",
    "punta cana",
    "santo domingo",
    "la romana",
    "puerto plata",
    "samaná",
    "samana",
    "santiago",
    "jarabacoa",
    "constanza",
    "dajabón",
    "dajabon",
    "barahona",
    "miches",
    "panama city",
    "bogotá",
    "bogota",
    "medellín",
    "medellin",
    "cartagena",
    "san josé",
    "san jose",
    "puerto vallarta",
    "mérida",
    "merida",
    "oaxaca",
    "puebla",
    "lima",
    "quito",
    "santiago",
    "buenos aires",
    "são paulo",
    "sao paulo",
    "rio de janeiro",
    "kingston",
    "montego bay",
    "san juan",
    "barranquilla",
    "cali",
    "cozumel",
    "isla mujeres",
    "mazatlán",
    "mazatlan",
    "acapulco",
    "querétaro",
    "queretaro",
    "chihuahua",
    "veracruz",
    "saltillo",
    "tuxtla gutierrez",
    "tuxtla gutiérrez",
    "villahermosa",
    "irapuato",
    "zacatecas",
    "cuernavaca",
    "culiacan",
    "culiacán",
    "tampico",
    "ciudad juarez",
    "ciudad juárez",
    "san miguel de allende",
    "ciudad delicias",
    "delicias",
    "leon",
    "león",
    "hermosillo",
    "tijuana",
    "mexicali",
    "torreon",
    "torreón",
    "colon",
    "colón",
    "chitre",
    "chitré",
    "cerro punta",
    "cucuta",
    "cúcuta",
    "liberia",
    "tamarindo",
    "cusco",
    "guayaquil",
  ])
);

/**
 * Explicit High-confidence Submarket cues (city / address / name tokens).
 * Only when Market is already set or proposed. Never vague Beach/Resort/Downtown alone.
 */
export const SUBMARKET_HIGH_RULES = Object.freeze([
  {
    market: "Cartagena",
    submarket: "Centro Histórico",
    tokens: [
      "centro historico",
      "centro histórico",
      "casco antiguo",
      "calle del cuartel",
      "getsemani",
      "getsemaní",
    ],
  },
  {
    market: "Cancún",
    submarket: "Hotel Zone",
    tokens: ["zona hotelera", "hotel zone", "blvd kukulcan", "boulevard kukulcan"],
  },
  {
    market: "Punta Cana",
    submarket: "Cap Cana",
    tokens: ["cap cana"],
  },
  {
    market: "Mexico City",
    submarket: "Polanco",
    tokens: ["polanco"],
  },
  {
    market: "Mexico City",
    submarket: "Condesa",
    tokens: ["condesa"],
  },
  {
    market: "Mexico City",
    submarket: "Roma",
    tokens: ["roma norte", "roma sur", " la roma", "colonia roma"],
  },
  {
    market: "Medellín",
    submarket: "El Poblado",
    tokens: ["el poblado", "poblado"],
  },
  {
    market: "Panama City",
    submarket: "Casco Viejo",
    tokens: ["casco viejo", "casco antiguo"],
  },
  {
    market: "Los Cabos",
    submarket: "San José del Cabo",
    tokens: ["san jose del cabo", "san josé del cabo"],
  },
  {
    market: "Los Cabos",
    submarket: "Cabo San Lucas",
    tokens: ["cabo san lucas"],
  },
  {
    market: "Cancún",
    submarket: "Airport",
    tokens: ["aeropuerto", "airport"],
  },
]);

export function normGeoKey(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

/**
 * @param {string} country
 * @returns {{ continent: string, subContinent: string }|null}
 */
export function resolveContinentSubContinentFromCountry(country) {
  const key = normGeoKey(country);
  if (!key) return null;
  // Exact then loose match
  if (COUNTRY_TO_CONTINENT_SUBCONTINENT[key]) {
    return { ...COUNTRY_TO_CONTINENT_SUBCONTINENT[key] };
  }
  for (const [name, mapped] of Object.entries(COUNTRY_TO_CONTINENT_SUBCONTINENT)) {
    if (normGeoKey(name) === key) return { ...mapped };
  }
  return null;
}

/**
 * Resolve Market from clean City (+ optional country). High only when map/recognized.
 * @param {{ city?: string, country?: string }} input
 * @returns {{ ok: boolean, market?: string, confidence?: string, reason?: string, method?: string }}
 */
export function resolveMarketFromCity(input = {}) {
  const city = String(input.city || "").trim();
  if (!city) return { ok: false, reason: "missing_city" };
  const key = normGeoKey(city);

  if (CITY_TO_MARKET_OVERRIDE[key]) {
    return {
      ok: true,
      market: CITY_TO_MARKET_OVERRIDE[key],
      confidence: "High",
      method: "tested_city_market_map",
    };
  }
  // Accent-insensitive override keys
  for (const [k, market] of Object.entries(CITY_TO_MARKET_OVERRIDE)) {
    if (normGeoKey(k) === key) {
      return {
        ok: true,
        market,
        confidence: "High",
        method: "tested_city_market_map",
      };
    }
  }

  if (RECOGNIZED_MARKET_CITIES.has(key)) {
    return {
      ok: true,
      market: city, // preserve canonical City spelling
      confidence: "High",
      method: "recognized_market_city_equals_market",
    };
  }

  return { ok: false, reason: "market_source_needed", confidence: null };
}

/**
 * High-confidence Submarket only — never Medium/Low autofill.
 * @param {{ market?: string, city?: string, address?: string, propertyName?: string }} input
 */
export function resolveSubmarketHighOnly(input = {}) {
  const market = String(input.market || "").trim();
  if (!market) return { ok: false, reason: "missing_market" };

  const hay = normGeoKey(
    [input.city, input.address, input.propertyName].filter(Boolean).join(" ")
  );
  if (!hay) return { ok: false, reason: "no_submarket_source_text" };

  const marketKey = normGeoKey(market);
  for (const rule of SUBMARKET_HIGH_RULES) {
    if (normGeoKey(rule.market) !== marketKey) continue;
    for (const token of rule.tokens) {
      if (hay.includes(normGeoKey(token))) {
        return {
          ok: true,
          submarket: rule.submarket,
          confidence: "High",
          method: "tested_submarket_token_map",
          evidence_token: token,
        };
      }
    }
  }

  // When City is a Los Cabos submarket label itself
  const cityKey = normGeoKey(input.city);
  if (marketKey === normGeoKey("Los Cabos")) {
    if (cityKey === normGeoKey("San José del Cabo")) {
      return {
        ok: true,
        submarket: "San José del Cabo",
        confidence: "High",
        method: "city_is_submarket_under_market",
      };
    }
    if (cityKey === normGeoKey("Cabo San Lucas")) {
      return {
        ok: true,
        submarket: "Cabo San Lucas",
        confidence: "High",
        method: "city_is_submarket_under_market",
      };
    }
  }
  if (marketKey === normGeoKey("Punta Cana") && cityKey === normGeoKey("Cap Cana")) {
    return {
      ok: true,
      submarket: "Cap Cana",
      confidence: "High",
      method: "city_is_submarket_under_market",
    };
  }

  return { ok: false, reason: "submarket_not_high", confidence: null };
}

/**
 * Values differ materially (case/accent-insensitive equality = not material).
 */
export function isMateriallyDifferentGeo(existing, proposed) {
  const a = normGeoKey(existing);
  const b = normGeoKey(proposed);
  if (!a || !b) return false;
  return a !== b;
}
