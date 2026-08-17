/**
 * STR / census Submarket label → Dealality corridor resolution.
 * Used by Hotel Census backfill (all registry countries, not only built radar).
 */

import { COUNTRY_CONFIGS } from "../radar-buildout/country-configs.js";
import { getSubmarketOptionsForCountry } from "../radar-submarket.js";
import { normalizeSubmarketLabel } from "../radar-submarket.js";

const OTHER = "Other";

/**
 * @param {string} value
 */
export function normStrLabel(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

/** Census country label → radar config key */
const CENSUS_COUNTRY_ALIASES = {
  "turks and caicos islands": "Turks & Caicos",
  "turks and caicos": "Turks & Caicos",
  "turks & caicos islands": "Turks & Caicos",
  "bonaire sint eustatius and saba": "Aruba",
  "bonaire, sint eustatius and saba": "Aruba",
  "sint maarten (dutch part)": "Aruba",
  "saint-martin (french part)": "Aruba",
  "us virgin islands": "Puerto Rico",
  "british virgin islands": "Puerto Rico",
};

/**
 * @param {string} country
 */
export function resolveCensusCountryKey(country) {
  const raw = String(country || "").trim();
  if (!raw) return "";
  if (COUNTRY_CONFIGS[raw]) return raw;
  const alias = CENSUS_COUNTRY_ALIASES[normStrLabel(raw)];
  if (alias && COUNTRY_CONFIGS[alias]) return alias;
  const lower = normStrLabel(raw);
  for (const key of Object.keys(COUNTRY_CONFIGS)) {
    if (normStrLabel(key) === lower) return key;
  }
  return raw;
}

/**
 * Explicit STR submarket → corridor (normalized keys).
 * @type {Record<string, Record<string, string>>}
 */
const STR_SUBMARKET_ALIASES = {
  Mexico: {
    "mexico city": "Mexico City",
    "ciudad de mexico": "Mexico City",
    "cdmx": "Mexico City",
    "puerto vallarta": "Puerto Vallarta / Riviera Nayarit",
    "riviera nayarit": "Puerto Vallarta / Riviera Nayarit",
    nayarit: "Puerto Vallarta / Riviera Nayarit",
    merida: "Mérida / Yucatán",
    "los cabos": "Los Cabos",
    cabo: "Los Cabos",
    "san jose del cabo": "Los Cabos",
    queretaro: "Querétaro",
    mazatlan: "Mazatlán",
    "playa del carmen": "Riviera Maya / Playa del Carmen",
    cancun: "Cancún Hotel Zone",
    "cancun hotel zone": "Cancún Hotel Zone",
    "mexico regional": "Other",
  },
  Brazil: {
    "sao paulo": "São Paulo",
    "são paulo": "São Paulo",
    "sao paulo state regional": "São Paulo",
    "rio de janeiro": "Rio de Janeiro",
    "brazil northeast regional": "Salvador",
    "brazil south regional": "Florianópolis",
    "brazil central west regional": "Brasília",
    "brazil north regional": "Manaus",
    "armacao dos buzios": "Rio de Janeiro",
    buzios: "Rio de Janeiro",
    "porto alegre": "Porto Alegre",
    "belo horizonte": "Belo Horizonte",
    fortaleza: "Fortaleza",
    curitiba: "Curitiba",
    salvador: "Salvador",
    recife: "Recife",
    natal: "Natal",
    manaus: "Manaus",
    brasilia: "Brasília",
    florianopolis: "Florianópolis",
  },
  Argentina: {
    cordoba: "Córdoba",
    "puerto iguazu": "Puerto Iguazú",
    "mar del plata": "Mar del Plata",
    ushuaia: "Ushuaia",
    salta: "Salta",
    rosario: "Rosario",
    mendoza: "Mendoza",
    bariloche: "Bariloche",
    "buenos aires": "Buenos Aires",
    "argentina regional": "Other",
  },
  Chile: {
    santiago: "Santiago Centro",
    "valparaiso": "Valparaíso / Viña del Mar",
    "vina del mar": "Valparaíso / Viña del Mar",
    "puerto varas": "Patagonia Lakes",
    "san pedro de atacama": "Atacama",
    "chile regional": "Other",
  },
  Peru: {
    lima: "Lima Historic Center",
    trujillo: "Lima Historic Center",
    arequipa: "Arequipa",
    cusco: "Cusco Historic Center",
    cuzco: "Cusco Historic Center",
    paracas: "Paracas",
    "peru regional": "Other",
  },
  Colombia: {
    bogota: "Bogotá",
    medellin: "Medellín",
    cartagena: "Cartagena",
    cali: "Cali",
    barranquilla: "Barranquilla",
    "santa marta": "Santa Marta",
    "san andres": "San Andrés",
    pereira: "Coffee Region / Pereira",
    "colombia regional": "Other",
  },
  "Dominican Republic": {
    "dominican republic regional": "Other",
    "punta cana": "Punta Cana / Bávaro / Cap Cana",
    santo: "Santo Domingo Metro",
    "santo domingo": "Santo Domingo Metro",
  },
  "Costa Rica": {
    "san jose": "San José Metro",
    liberia: "Guanacaste / Papagayo",
    tamarindo: "Tamarindo / North Pacific",
    "costa rica regional": "Other",
  },
  Panama: {
    "panama city": "Panama City",
    "panama regional": "Other",
  },
  Jamaica: {
    "jamaica regional": "Other",
    montego: "Montego Bay",
    negril: "Negril",
    kingston: "Kingston",
  },
  Bahamas: {
    nassau: "Nassau / New Providence",
    "grand bahama": "Grand Bahama / Freeport",
    freeport: "Grand Bahama / Freeport",
    "bahamas regional": "Other",
  },
  Barbados: {
    "barbados regional": "Bridgetown",
    bridgetown: "Bridgetown",
  },
  Belize: {
    "belize regional": "Belize City",
    "san pedro": "Ambergris Caye",
    "ambergris caye": "Ambergris Caye",
    placencia: "Placencia",
  },
  Ecuador: {
    quito: "Quito",
    guayaquil: "Guayaquil",
    galapagos: "Galápagos",
    "ecuador regional": "Other",
  },
  Uruguay: {
    montevideo: "Montevideo",
    "punta del este": "Punta del Este",
    colonia: "Colonia",
  },
  Guatemala: {
    antigua: "Antigua",
    "guatemala city": "Guatemala City",
    "lake atitlan": "Lake Atitlán",
  },
  Nicaragua: {
    managua: "Managua",
    granada: "Granada",
  },
  Honduras: {
    roatan: "Roatán",
    tegucigalpa: "Tegucigalpa",
    "san pedro sula": "San Pedro Sula",
  },
  "Puerto Rico": {
    "puerto rico regional": "San Juan Metro",
    "san juan": "San Juan Metro",
  },
  Aruba: {
    oranjestad: "Oranjestad / Cruise Port",
    "palm beach": "Palm Beach / High-Rise Hotel Area",
  },
  Curaçao: {
    willemstad: "Willemstad / Punda-Otrobanda",
  },
  "Turks & Caicos": {
    providenciales: "Providenciales",
    "grand turk": "Grand Turk",
  },
  "Cayman Islands": {
    "grand cayman": "Grand Cayman",
  },
};

/**
 * @param {string} countryKey
 * @param {string} label
 * @param {string[]} options
 */
function pickRegistryOption(countryKey, label, options) {
  const normalized = normalizeSubmarketLabel(label, countryKey);
  if (normalized && normalized !== OTHER && options.includes(normalized)) {
    return normalized;
  }

  const key = normStrLabel(label);
  if (!key) return null;

  const aliases = STR_SUBMARKET_ALIASES[countryKey] || {};
  if (aliases[key] && options.includes(aliases[key])) return aliases[key];

  for (const [aliasKey, corridor] of Object.entries(aliases)) {
    if ((key.includes(aliasKey) || aliasKey.includes(key)) && options.includes(corridor)) {
      return corridor;
    }
  }

  const config = COUNTRY_CONFIGS[countryKey];
  for (const market of config?.initialMarkets || []) {
    if (market === OTHER) continue;
    const mk = normStrLabel(market);
    if (mk === key || mk.includes(key) || key.includes(mk)) {
      if (options.includes(market)) return market;
    }
  }

  for (const opt of options) {
    if (opt === OTHER) continue;
    const ok = normStrLabel(opt);
    if (ok === key || ok.includes(key) || key.includes(ok)) return opt;
  }

  return null;
}

/**
 * @param {object} params
 * @param {string} params.country
 * @param {string} [params.strSubmarket]
 * @param {string} [params.city]
 * @param {string} [params.name]
 * @param {string} [params.market]
 */
export function resolveStrSubmarketToCorridor(params) {
  const countryKey = resolveCensusCountryKey(params.country);
  const options = getSubmarketOptionsForCountry(countryKey);
  if (!options.length || (options.length === 1 && options[0] === OTHER)) {
    return null;
  }

  const candidates = [
    params.strSubmarket,
    params.city,
    params.market,
    params.name,
  ].filter(Boolean);

  for (const label of candidates) {
    const hit = pickRegistryOption(countryKey, label, options);
    if (hit) {
      return {
        submarket: hit,
        confidence: label === params.strSubmarket ? "Medium" : "Low",
        reason:
          label === params.strSubmarket
            ? "str_submarket_alias_map"
            : label === params.city
              ? "city_to_corridor"
              : label === params.market
                ? "market_to_corridor"
                : "name_to_corridor",
        source: "census_str_submarket_alias",
      };
    }
  }

  if (/\bregional$/i.test(String(params.strSubmarket || ""))) {
    if (options.includes(OTHER)) {
      return {
        submarket: OTHER,
        confidence: "Low",
        reason: "str_regional_fallback_other",
        source: "census_str_regional_other",
      };
    }
  }

  return null;
}
