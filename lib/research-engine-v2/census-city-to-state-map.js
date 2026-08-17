/**
 * Deterministic City → State / Region maps for Hotel Property Census.
 * High only when country + city are clean and mapping is unique.
 * Never weak inference; expand via steward-approved additions.
 */

import { normalizePlaceKey, isDescriptorCity } from "./census-city-state-normalizer.js";
import { lookupResidualCityState } from "./census-city-to-state-residuals-v2.js";

export const CITY_TO_STATE_MAP_VERSION = "census-city-to-state-map-v1.2-residuals";

/**
 * Dirty State / Region values that are ISO subdivision / IATA-like codes, not display names.
 * Example Colombia: CUN=Cundinamarca, BOL=Bolívar, ATL=Atlántico (not Cancún airport).
 */
export const DIRTY_STATE_REGION_CODES = Object.freeze(
  new Set([
    // Colombia ISO 3166-2 department codes seen in Census
    "ATL", // Atlántico
    "ANT", // Antioquia
    "BOL", // Bolívar
    "CUN", // Cundinamarca
    "VAC", // Valle del Cauca
    "SAP", // San Andrés (if seen)
    "DC", // Bogotá D.C. abbrev
    // Mexico ISO 3166-2 short codes occasionally leaked from Choice JSON-LD
    "NLE",
    "ROO",
    "CMX",
    "JAL",
    "BCN",
    "BCS",
    "CHH",
    "COA",
    "GUA",
    "MEX",
    "MIC",
    "PUE",
    "QUE",
    "SIN",
    "SON",
    "TAB",
    "TAM",
    "VER",
    "YUC",
    "ZAC",
    "AGU",
    "CAM",
    "CHP",
    "DUR",
    "GRO",
    "HID",
    "MEX",
    "MOR",
    "NAY",
    "OAX",
    "SLP",
    "TLA",
  ])
);

/**
 * True when State / Region looks like a code / numeric junk, not a display region name.
 */
export function isDirtyStateRegionValue(state) {
  const s = String(state || "").trim();
  if (!s) return false;
  if (/^\d+$/.test(s)) return true; // e.g. Panama "8"
  if (/^[A-Z]{2,3}$/i.test(s) && DIRTY_STATE_REGION_CODES.has(s.toUpperCase())) {
    return true;
  }
  // Generic 3-letter uppercase codes (airport / ISO) when not a known real short name
  if (/^[A-Z]{3}$/.test(s)) return true;
  return false;
}

/**
 * country_norm → city_norm → State / Region display value
 * @type {Record<string, Record<string, string>>}
 */
export const CITY_TO_STATE_BY_COUNTRY = Object.freeze({
  mexico: Object.freeze({
    cancun: "Quintana Roo",
    "playa del carmen": "Quintana Roo",
    tulum: "Quintana Roo",
    cozumel: "Quintana Roo",
    "isla mujeres": "Quintana Roo",
    chetumal: "Quintana Roo",
    holbox: "Quintana Roo",
    "puerto morelos": "Quintana Roo",
    "puerto vallarta": "Jalisco",
    "nuevo vallarta": "Nayarit",
    guadalajara: "Jalisco",
    zapopan: "Jalisco",
    "lagos de moreno": "Jalisco",
    monterrey: "Nuevo León",
    apodaca: "Nuevo León",
    "san pedro garza garcia": "Nuevo León",
    guadalupe: "Nuevo León",
    "mexico city": "Ciudad de México",
    "ciudad de mexico": "Ciudad de México",
    cdmx: "Ciudad de México",
    "los cabos": "Baja California Sur",
    "cabo san lucas": "Baja California Sur",
    "san jose del cabo": "Baja California Sur",
    queretaro: "Querétaro",
    puebla: "Puebla",
    merida: "Yucatán",
    leon: "Guanajuato",
    silao: "Guanajuato",
    celaya: "Guanajuato",
    "san luis potosi": "San Luis Potosí",
    tijuana: "Baja California",
    rosarito: "Baja California",
    mexicali: "Baja California",
    mazatlan: "Sinaloa",
    "los mochis": "Sinaloa",
    culiacan: "Sinaloa",
    acapulco: "Guerrero",
    oaxaca: "Oaxaca",
    "puerto escondido": "Oaxaca",
    veracruz: "Veracruz",
    xalapa: "Veracruz",
    cordoba: "Veracruz",
    chihuahua: "Chihuahua",
    saltillo: "Coahuila",
    torreon: "Coahuila",
    monclova: "Coahuila",
    "piedras negras": "Coahuila",
    "ciudad juarez": "Chihuahua",
    "ciudad delicias": "Chihuahua",
    aguascalientes: "Aguascalientes",
    toluca: "Estado de México",
    naucalpan: "Estado de México",
    tlalnepantla: "Estado de México",
    villahermosa: "Tabasco",
    campeche: "Campeche",
    "ciudad del carmen": "Campeche",
    durango: "Durango",
    tepic: "Nayarit",
    "punta mita": "Nayarit",
    manzanillo: "Colima",
    zacatecas: "Zacatecas",
    reynosa: "Tamaulipas",
    matamoros: "Tamaulipas",
    "nuevo laredo": "Tamaulipas",
    "ciudad victoria": "Tamaulipas",
    tampico: "Tamaulipas",
    nogales: "Sonora",
    hermosillo: "Sonora",
    guaymas: "Sonora",
    "ciudad obregon": "Sonora",
    obregon: "Sonora",
    caborca: "Sonora",
    "tuxtla gutierrez": "Chiapas",
    tapachula: "Chiapas",
    comitan: "Chiapas",
    irapuato: "Guanajuato",
    morelia: "Michoacán",
    "lazaro cardenas": "Michoacán",
    "san miguel de allende": "Guanajuato",
    cuernavaca: "Morelos",
    "san pedro tlaquepaque": "Jalisco",
    chapala: "Jalisco",
    "cuajimalpa de morelos": "Ciudad de México",
    "mexico d.f.": "Ciudad de México",
    "mexico df": "Ciudad de México",
  }),
  colombia: Object.freeze({
    bogota: "Cundinamarca",
    medellin: "Antioquia",
    cartagena: "Bolívar",
    barranquilla: "Atlántico",
    cali: "Valle del Cauca",
    "santa marta": "Magdalena",
    cucuta: "Norte de Santander",
    chia: "Cundinamarca",
    itagui: "Antioquia",
    "san andres": "San Andrés y Providencia",
    pereira: "Risaralda",
    bucaramanga: "Santander",
    armenia: "Quindío",
  }),
  "costa rica": Object.freeze({
    "san jose": "San José",
    liberia: "Guanacaste",
    tamarindo: "Guanacaste",
    belen: "Heredia",
    ballena: "Puntarenas",
    "puerto viejo de talamanca": "Limón",
  }),
  "dominican republic": Object.freeze({
    "santo domingo": "Distrito Nacional",
    "punta cana": "La Altagracia",
    "puerto plata": "Puerto Plata",
    "la romana": "La Romana",
    santiago: "Santiago",
    bavaro: "La Altagracia",
    "cap cana": "La Altagracia",
    sosua: "Puerto Plata",
    cabarete: "Puerto Plata",
    "juan dolio": "San Pedro de Macorís",
    miches: "El Seibo",
    "las terrenas": "Samaná",
    bayahibe: "La Romana",
  }),
  panama: Object.freeze({
    "panama city": "Panamá",
    "ciudad de panama": "Panamá",
    panama: "Panamá",
    "panama ": "Panamá",
    amador: "Panamá",
    chame: "Panamá Oeste",
    coronado: "Panamá Oeste",
    "playa caracol": "Panamá Oeste",
    colon: "Colón",
    "colón": "Colón",
    "cuidad de colon": "Colón",
    "ciudad de colon": "Colón",
    chitre: "Herrera",
    "chitré": "Herrera",
    "cerro punta": "Chiriquí",
    volcan: "Chiriquí",
    "volcán": "Chiriquí",
    david: "Chiriquí",
    boquete: "Chiriquí",
    "bocas del toro": "Bocas del Toro",
  }),
  peru: Object.freeze({
    lima: "Lima",
    cusco: "Cusco",
    cuzco: "Cusco",
    miraflores: "Lima",
    urubamba: "Cusco",
    puno: "Puno",
    "aguas calientes": "Cusco",
    arequipa: "Arequipa",
  }),
  chile: Object.freeze({
    santiago: "Región Metropolitana",
    "san pedro de atacama": "Antofagasta",
    "puerto varas": "Los Lagos",
    "puerto natales": "Magallanes",
    iquique: "Tarapacá",
    antofagasta: "Antofagasta",
    "la serena": "Coquimbo",
    valparaiso: "Valparaíso",
    "vina del mar": "Valparaíso",
  }),
  argentina: Object.freeze({
    "buenos aires": "Buenos Aires",
    ushuaia: "Tierra del Fuego",
    "el calafate": "Santa Cruz",
    calafate: "Santa Cruz",
    "mar del plata": "Buenos Aires",
    iguazu: "Misiones",
    "san martin de los andes": "Neuquén",
    bariloche: "Río Negro",
    mendoza: "Mendoza",
    cordoba: "Córdoba",
  }),
  bolivia: Object.freeze({
    "la paz": "La Paz",
    "santa cruz": "Santa Cruz",
    "santa cruz de la sierra": "Santa Cruz",
    cochabamba: "Cochabamba",
  }),
  paraguay: Object.freeze({
    asuncion: "Asunción",
  }),
  venezuela: Object.freeze({
    caracas: "Distrito Capital",
  }),
  bahamas: Object.freeze({
    nassau: "New Providence",
  }),
  suriname: Object.freeze({
    paramaribo: "Paramaribo",
  }),
  grenada: Object.freeze({
    "st georges": "Saint George",
    "saint george": "Saint George",
  }),
  brazil: Object.freeze({
    "sao paulo": "São Paulo",
    "rio de janeiro": "Rio de Janeiro",
  }),
  jamaica: Object.freeze({
    kingston: "Kingston",
    "montego bay": "St. James",
    negril: "Westmoreland",
    "ocho rios": "St. Ann",
    "runaway bay": "St. Ann",
    "port antonio": "Portland",
    "rose hall": "St. James",
  }),
  "puerto rico": Object.freeze({
    "san juan": "San Juan",
    "rio grande": "Río Grande",
    fajardo: "Fajardo",
    ponce: "Ponce",
    "isla verde": "Carolina",
    carolina: "Carolina",
  }),
  cuba: Object.freeze({
    havana: "La Habana",
    habana: "La Habana",
    "la habana": "La Habana",
    vedado: "La Habana",
    miramar: "La Habana",
    "old havana": "La Habana",
    "habana vieja": "La Habana",
    varadero: "Matanzas",
    matanzas: "Matanzas",
    "cayo coco": "Ciego de Ávila",
    "jardines del rey": "Ciego de Ávila",
    "cayo guillermo": "Ciego de Ávila",
    "ciego de avila": "Ciego de Ávila",
    "cayo santa maria": "Villa Clara",
    "cayo las brujas": "Villa Clara",
    "santa clara": "Villa Clara",
    remedios: "Villa Clara",
    holguin: "Holguín",
    guardalavaca: "Holguín",
    gibara: "Holguín",
    "santiago de cuba": "Santiago de Cuba",
    trinidad: "Sancti Spíritus",
    "sancti spiritus": "Sancti Spíritus",
    "topes de collantes": "Sancti Spíritus",
    cienfuegos: "Cienfuegos",
    vinales: "Pinar del Río",
    "pinar del rio": "Pinar del Río",
    "maria la gorda": "Pinar del Río",
    camaguey: "Camagüey",
    "santa lucia": "Camagüey",
    "cayo largo": "Isla de la Juventud",
    "isla de la juventud": "Isla de la Juventud",
    baracoa: "Guantánamo",
    "peninsula de zapata": "Matanzas",
    "playa giron": "Matanzas",
    "playa larga": "Matanzas",
    bayamo: "Granma",
    "las tunas": "Las Tunas",
  }),
  ecuador: Object.freeze({
    quito: "Pichincha",
    guayaquil: "Guayas",
    cuenca: "Azuay",
    galapagos: "Galápagos",
    "puerto ayora": "Galápagos",
  }),
  uruguay: Object.freeze({
    montevideo: "Montevideo",
    punta: "Maldonado",
    "punta del este": "Maldonado",
    colonia: "Colonia",
    "colonia del sacramento": "Colonia",
  }),
  guatemala: Object.freeze({
    "guatemala city": "Guatemala",
    antigua: "Sacatepéquez",
    "antigua guatemala": "Sacatepéquez",
    flores: "Petén",
    panajachel: "Sololá",
  }),
  honduras: Object.freeze({
    tegucigalpa: "Francisco Morazán",
    "san pedro sula": "Cortés",
    roatan: "Islas de la Bahía",
    "west bay": "Islas de la Bahía",
  }),
  nicaragua: Object.freeze({
    managua: "Managua",
    granada: "Granada",
    leon: "León",
    "san juan del sur": "Rivas",
  }),
  "el salvador": Object.freeze({
    "san salvador": "San Salvador",
    "santa ana": "Santa Ana",
    "el tunco": "La Libertad",
  }),
  belize: Object.freeze({
    "belize city": "Belize",
    "san pedro": "Belize",
    placencia: "Stann Creek",
    "caye caulker": "Belize",
    belmopan: "Cayo",
    "punta gorda": "Toledo",
    dangriga: "Stann Creek",
  }),
});

/**
 * Mexico Choice URL first path segment → State / Region (official URL structure).
 * choicehotels.com/{state-slug}/{city-slug}/{brand}/{code}
 */
export const MEXICO_CHOICE_URL_STATE_SLUGS = Object.freeze({
  aguascalientes: "Aguascalientes",
  "baja-california": "Baja California",
  "baja-california-sur": "Baja California Sur",
  campeche: "Campeche",
  chiapas: "Chiapas",
  chihuahua: "Chihuahua",
  coahuila: "Coahuila",
  colima: "Colima",
  durango: "Durango",
  guanajuato: "Guanajuato",
  guerrero: "Guerrero",
  hidalgo: "Hidalgo",
  jalisco: "Jalisco",
  mexico: null, // ambiguous country-level slug — do not use as state
  michoacan: "Michoacán",
  morelos: "Morelos",
  nayarit: "Nayarit",
  "nuevo-leon": "Nuevo León",
  oaxaca: "Oaxaca",
  puebla: "Puebla",
  queretaro: "Querétaro",
  "quintana-roo": "Quintana Roo",
  "san-luis-potosi": "San Luis Potosí",
  sinaloa: "Sinaloa",
  sonora: "Sonora",
  tabasco: "Tabasco",
  tamaulipas: "Tamaulipas",
  tlaxcala: "Tlaxcala",
  veracruz: "Veracruz",
  yucatan: "Yucatán",
  zacatecas: "Zacatecas",
});

/**
 * Derive Mexico State / Region from official Choice property URL path.
 */
export function resolveStateFromChoiceOfficialUrl(url) {
  const raw = String(url || "").trim();
  if (!/choicehotels\.com/i.test(raw)) return { ok: false, reason: "not_choice_url" };
  try {
    const u = new URL(raw.includes("://") ? raw : `https://${raw}`);
    const parts = u.pathname.split("/").filter(Boolean);
    if (parts.length < 3) return { ok: false, reason: "url_path_too_short" };
    const slug = parts[0].toLowerCase();
    if (!(slug in MEXICO_CHOICE_URL_STATE_SLUGS)) {
      return { ok: false, reason: "state_slug_unknown", slug };
    }
    const state = MEXICO_CHOICE_URL_STATE_SLUGS[slug];
    if (!state) return { ok: false, reason: "state_slug_ambiguous_mexico", slug };
    return {
      ok: true,
      state,
      confidence: "High",
      method: "choice_official_url_state_slug",
    };
  } catch {
    return { ok: false, reason: "url_parse_failed" };
  }
}

/**
 * @param {{ city?: string, country?: string, state?: string }} input
 * @returns {{
 *   ok: boolean,
 *   state?: string,
 *   confidence?: string,
 *   method?: string,
 *   reason?: string,
 *   steward_needed?: boolean
 * }}
 */
export function resolveStateRegionFromCity(input = {}) {
  const city = String(input.city || "").trim();
  const country = String(input.country || "").trim();
  let existing = String(input.state || "").trim();
  const dirtyExisting = isDirtyStateRegionValue(existing);
  if (dirtyExisting) {
    // Treat ISO/IATA-like codes as blank so High city→state can overwrite
    existing = "";
  }

  if (!city || isDescriptorCity(city)) {
    return { ok: false, reason: "city_missing_or_ambiguous", steward_needed: true };
  }
  if (!country) {
    return { ok: false, reason: "country_missing", steward_needed: true };
  }

  const countryKey = normalizePlaceKey(country);
  const cityKey = normalizePlaceKey(city);
  const countryMap = CITY_TO_STATE_BY_COUNTRY[countryKey];

  let mapped = null;
  if (countryMap) {
    mapped = countryMap[cityKey] || null;
    if (!mapped) {
      for (const [k, v] of Object.entries(countryMap)) {
        if (normalizePlaceKey(k) === cityKey) {
          mapped = v;
          break;
        }
      }
    }
  }
  if (!mapped) {
    mapped = lookupResidualCityState(countryKey, cityKey);
  }

  if (!mapped && !countryMap) {
    return {
      ok: false,
      reason: "country_state_map_missing",
      steward_needed: true,
    };
  }

  if (!mapped) {
    return {
      ok: false,
      reason: "city_state_mapping_missing",
      steward_needed: true,
    };
  }

  if (existing) {
    const same = normalizePlaceKey(existing) === normalizePlaceKey(mapped);
    if (!same) {
      return {
        ok: false,
        reason: "state_conflict_with_existing",
        steward_needed: true,
        state: mapped,
      };
    }
    return {
      ok: false,
      reason: "state_already_set",
      state: existing,
      confidence: "High",
      method: "deterministic_city_state_map",
    };
  }

  return {
    ok: true,
    state: mapped,
    confidence: "High",
    method: dirtyExisting
      ? "deterministic_city_state_map_overwrite_dirty_code"
      : "deterministic_city_state_map",
    replaced_dirty_state: dirtyExisting ? String(input.state || "").trim() : null,
  };
}

/**
 * Build High patches for blank OR dirty-code State / Region from city map.
 * @param {object[]} records
 */
export function buildStateRegionMapPatches(records = []) {
  const proposals = [];
  const stewardNeeded = [];
  for (const rec of records) {
    const f = rec.fields || {};
    const city = f.City || "";
    const country = f.Country || "";
    const state = f["State / Region"] || "";
    const resolved = resolveStateRegionFromCity({ city, country, state });
    if (resolved.ok && resolved.state) {
      proposals.push({
        record_id: rec.id,
        reason: resolved.method || "deterministic_city_state_map",
        confidence: "High",
        patch: {
          "State / Region": resolved.state,
          "Last Reviewed Date": new Date().toISOString().slice(0, 10),
        },
        meta: {
          city,
          country,
          method: resolved.method,
          replaced_dirty_state: resolved.replaced_dirty_state || null,
        },
      });
    } else if (
      resolved.steward_needed &&
      (!String(state || "").trim() || isDirtyStateRegionValue(state))
    ) {
      stewardNeeded.push({
        record_id: rec.id,
        city,
        country,
        state,
        reason: resolved.reason,
      });
    }
  }
  return { proposals, stewardNeeded };
}
