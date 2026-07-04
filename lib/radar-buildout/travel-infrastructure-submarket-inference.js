/**
 * Built-country Travel Infrastructure submarket inference.
 * Confidence-based; does not guess when weak.
 */

import {
  BUILT_RADAR_COUNTRIES,
  getSubmarketOptionsForCountry,
} from "../radar-submarket-registry.js";
import { resolveCensusCountryKey } from "../hotel-census/census-str-submarket-corridors.js";
import {
  extractSubmarketFromNotes,
  normalizeSubmarketLabel,
  inferSubmarketFromCity,
} from "../radar-submarket.js";
import { MEXICO_CANCUN_TI_RECORD_RULES } from "./mexico-cancun-ti-submarket-backfill.js";

const INFERENCE_NOTE_TAG = "Submarket inferred during built-country TI submarket pass:";

const CONFIDENCE_RANK = { High: 3, Medium: 2, Low: 1, "No Match": 0 };

/**
 * @param {string} value
 */
function norm(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

/**
 * @param {object} record
 */
function buildHaystack(record) {
  return [
    record.name,
    record.city,
    record.notes,
    record.pointType,
    record.type,
    record.iataCode,
    record.airportType,
    record.infrastructureRole,
  ]
    .map(norm)
    .filter(Boolean)
    .join(" | ");
}

/**
 * @param {string} country
 * @param {string} raw
 * @returns {string}
 */
function resolveInRegistry(country, raw) {
  const label = String(raw || "").trim();
  if (!label) return "";
  const resolved = normalizeSubmarketLabel(label, country);
  const options = getSubmarketOptionsForCountry(country);
  return options.includes(resolved) ? resolved : "";
}

/**
 * @param {string} confidence
 * @param {string} minConfidence
 */
export function meetsMinConfidence(confidence, minConfidence) {
  return (CONFIDENCE_RANK[confidence] || 0) >= (CONFIDENCE_RANK[minConfidence] || 0);
}

/**
 * @typedef {object} TiInferenceResult
 * @property {string} recordId
 * @property {string} name
 * @property {string} country
 * @property {string} city
 * @property {string|null} currentSubmarket
 * @property {string|null} inferredSubmarket
 * @property {'High'|'Medium'|'Low'|'No Match'} confidence
 * @property {string} reason
 * @property {string} proposedNotesAppend
 * @property {boolean} shouldUpdate
 */

/**
 * @typedef {object} TiInferenceRule
 * @property {RegExp[]} patterns
 * @property {string} submarket
 * @property {'High'|'Medium'} confidence
 * @property {string} reason
 */

/** @type {Record<string, TiInferenceRule[]>} */
const COUNTRY_KEYWORD_RULES = {
  Mexico: [
    ...MEXICO_CANCUN_TI_RECORD_RULES.map((r) => ({
      patterns: r.patterns,
      submarket: r.submarket,
      confidence: "High",
      reason: "name_keyword_mexico_cancun_rule",
    })),
    {
      patterns: [/akumal|puerto aventuras/i],
      submarket: "Akumal / Puerto Aventuras",
      confidence: "Medium",
      reason: "name_keyword_riviera_secondary",
    },
    {
      patterns: [/mayakoba/i],
      submarket: "Mayakoba",
      confidence: "High",
      reason: "name_keyword_mayakoba",
    },
    {
      patterns: [/costa mujeres|playa mujeres/i],
      submarket: "Costa Mujeres / Playa Mujeres",
      confidence: "Medium",
      reason: "name_keyword_costa_mujeres",
    },
  ],
  Panama: [
    {
      patterns: [/tocumen|panama city airport|pty\b/i],
      submarket: "Tocumen / Airport Corridor",
      confidence: "High",
      reason: "name_keyword_tocumen",
    },
    {
      patterns: [/panama canal|miraflores locks|gatun|cristobal|cristóbal|balboa port|manzanillo|colon free zone|colón free zone|free zone logistics/i],
      submarket: "Canal / Logistics Corridor",
      confidence: "High",
      reason: "name_keyword_canal_logistics",
    },
    {
      patterns: [/amador|casco viejo|flamenco/i],
      submarket: "Casco Viejo / Waterfront",
      confidence: "Medium",
      reason: "name_keyword_casco_waterfront",
    },
    {
      patterns: [/bocas del toro|bocas town|changuinola/i],
      submarket: "Bocas del Toro",
      confidence: "High",
      reason: "name_keyword_bocas",
    },
    {
      patterns: [/boquete|david airport|enrique malek/i],
      submarket: "Boquete / Highlands",
      confidence: "Medium",
      reason: "name_keyword_highlands",
    },
    {
      patterns: [/costa del este|panama design|multiplaza/i],
      submarket: "Costa del Este",
      confidence: "Medium",
      reason: "name_keyword_costa_del_este",
    },
    {
      patterns: [/colon cruise|colon port|colón port|puerto de colon/i],
      submarket: "Canal / Logistics Corridor",
      confidence: "Medium",
      reason: "name_keyword_colon_port",
    },
  ],
  "Costa Rica": [
    {
      patterns: [/juan santamaria|juan santamaría|\bsjo\b|tob[ií]as bola[nñ]os|alajuela airport/i],
      submarket: "San José Metro",
      confidence: "High",
      reason: "name_keyword_sjo_metro",
    },
    {
      patterns: [/daniel oduber|liberia airport|\blir\b|guanacaste airport/i],
      submarket: "Guanacaste / Papagayo",
      confidence: "High",
      reason: "name_keyword_liberia",
    },
    {
      patterns: [/tamarindo|nosara airport/i],
      submarket: "Tamarindo / North Pacific",
      confidence: "High",
      reason: "name_keyword_tamarindo",
    },
    {
      patterns: [/quepos|manuel antonio|managua airport/i],
      submarket: "Manuel Antonio / Central Pacific",
      confidence: "High",
      reason: "name_keyword_manuel_antonio",
    },
    {
      patterns: [/la fortuna|arenal|fortuna access/i],
      submarket: "Arenal / La Fortuna",
      confidence: "High",
      reason: "name_keyword_arenal",
    },
    {
      patterns: [/lim[oó]n|mo[ií]n|puerto viejo|cahuita|caribbean highway|route 32/i],
      submarket: "Caribbean Coast",
      confidence: "Medium",
      reason: "name_keyword_caribbean",
    },
    {
      patterns: [/monteverde/i],
      submarket: "Monteverde",
      confidence: "High",
      reason: "name_keyword_monteverde",
    },
    {
      patterns: [/jaco|jacó|herradura|central pacific highway/i],
      submarket: "Jacó / Herradura",
      confidence: "Medium",
      reason: "name_keyword_jaco",
    },
    {
      patterns: [/puntarenas|paquera|nicoya ferry/i],
      submarket: "Guanacaste / Papagayo",
      confidence: "Medium",
      reason: "name_keyword_puntarenas_ferry",
    },
    {
      patterns: [/centro de convenciones costa rica|corferias/i],
      submarket: "San José Metro",
      confidence: "Medium",
      reason: "name_keyword_convention_sjo",
    },
  ],
  Peru: [
    {
      patterns: [/jorge ch[aá]vez|lima airport|faucett avenue airport/i],
      submarket: "Jorge Chávez Airport Corridor",
      confidence: "High",
      reason: "name_keyword_lima_airport",
    },
    {
      patterns: [/port of callao|callao maritime|callao port|terminal portuario/i],
      submarket: "Callao / Port",
      confidence: "High",
      reason: "name_keyword_callao_port",
    },
    {
      patterns: [/velasco astete|cusco airport|alejandro velasco/i],
      submarket: "Cusco Historic Center",
      confidence: "High",
      reason: "name_keyword_cusco_airport",
    },
    {
      patterns: [/perurail poroy|poroy station/i],
      submarket: "Cusco Historic Center",
      confidence: "High",
      reason: "name_keyword_poroy",
    },
    {
      patterns: [/ollantaytambo|inca rail ollantaytambo/i],
      submarket: "Ollantaytambo",
      confidence: "High",
      reason: "name_keyword_ollantaytambo",
    },
    {
      patterns: [/aguas calientes|machu picchu pueblo|machu picchu rail/i],
      submarket: "Machu Picchu Access",
      confidence: "High",
      reason: "name_keyword_machu_picchu_access",
    },
    {
      patterns: [/chinchero.*airport|chinchero international/i],
      submarket: "Sacred Valley",
      confidence: "Medium",
      reason: "name_keyword_chinchero",
    },
    {
      patterns: [/sacred valley|valle sagrado|pan-american access corridor/i],
      submarket: "Sacred Valley",
      confidence: "Medium",
      reason: "name_keyword_sacred_valley",
    },
    {
      patterns: [/urubamba/i],
      submarket: "Urubamba",
      confidence: "High",
      reason: "name_keyword_urubamba",
    },
    {
      patterns: [/lima airport corridor/i],
      submarket: "Jorge Chávez Airport Corridor",
      confidence: "High",
      reason: "name_keyword_airport_corridor",
    },
  ],
  "Dominican Republic": [
    {
      patterns: [/punta cana|\bpuj\b|bavaro|bávaro|cap cana|autopista del coral/i],
      submarket: "Punta Cana / Bávaro / Cap Cana",
      confidence: "High",
      reason: "name_keyword_punta_cana",
    },
    {
      patterns: [/las am[eé]ricas|santo domingo airport|sdq\b|isabela|ciudad colonial|port of sans souci|caribe tours/i],
      submarket: "Santo Domingo Metro",
      confidence: "High",
      reason: "name_keyword_santo_domingo",
    },
    {
      patterns: [/gregorio luper[oó]n|puerto plata|\bpop\b|sos[uú]a|cabarete/i],
      submarket: "Puerto Plata / Sosúa / Cabarete",
      confidence: "High",
      reason: "name_keyword_north_coast",
    },
    {
      patterns: [/la romana|bayahibe|casa de campo|romana industrial/i],
      submarket: "La Romana / Bayahibe",
      confidence: "High",
      reason: "name_keyword_la_romana",
    },
    {
      patterns: [/saman[aá]|las terrenas|sabana de la mar|el catey/i],
      submarket: "Samaná / Las Terrenas",
      confidence: "High",
      reason: "name_keyword_samana",
    },
    {
      patterns: [/santiago|cibao|\bsti\b/i],
      submarket: "Santiago / Cibao",
      confidence: "Medium",
      reason: "name_keyword_santiago",
    },
    {
      patterns: [/pedernales|cabo rojo|barahona|maria montez/i],
      submarket: "Barahona / Pedernales",
      confidence: "High",
      reason: "name_keyword_southwest",
    },
    {
      patterns: [/miches|costa esmeralda/i],
      submarket: "Miches / Costa Esmeralda",
      confidence: "High",
      reason: "name_keyword_miches",
    },
    {
      patterns: [/boca chica|juan dolio|caucedo/i],
      submarket: "Boca Chica / Juan Dolio",
      confidence: "Medium",
      reason: "name_keyword_boca_chica",
    },
    {
      patterns: [/jarabacoa|constanza/i],
      submarket: "Jarabacoa / Constanza",
      confidence: "Medium",
      reason: "name_keyword_cordillera",
    },
  ],
  Colombia: [
    {
      patterns: [/el dorado|bogot[aá]|vanguardia airport|enrique olaya/i],
      submarket: "Bogotá",
      confidence: "High",
      reason: "name_keyword_bogota",
    },
    {
      patterns: [/rafael n[uú][ñn]ez|cartagena.*airport|cartagena cruise|cartagena port/i],
      submarket: "Cartagena",
      confidence: "High",
      reason: "name_keyword_cartagena",
    },
    {
      patterns: [/jos[eé] mar[ií]a c[oó]rdova|rionegro|medell[ií]n.*airport|enrique olaya herrera/i],
      submarket: "Medellín",
      confidence: "High",
      reason: "name_keyword_medellin",
    },
    {
      patterns: [/barranquilla|ernesto cortissoz/i],
      submarket: "Barranquilla",
      confidence: "High",
      reason: "name_keyword_barranquilla",
    },
    {
      patterns: [/alfonso bonilla|cali airport|\bclo\b/i],
      submarket: "Cali",
      confidence: "High",
      reason: "name_keyword_cali",
    },
    {
      patterns: [/sim[oó]n bol[ií]var|santa marta/i],
      submarket: "Santa Marta",
      confidence: "High",
      reason: "name_keyword_santa_marta",
    },
    {
      patterns: [/mateca[nñ]a|pereira|coffee region/i],
      submarket: "Coffee Region / Pereira",
      confidence: "High",
      reason: "name_keyword_coffee_region",
    },
    {
      patterns: [/gustavo rojas|san andr[eé]s|\badz\b/i],
      submarket: "San Andrés",
      confidence: "High",
      reason: "name_keyword_san_andres",
    },
    {
      patterns: [/corferias/i],
      submarket: "Bogotá",
      confidence: "Medium",
      reason: "name_keyword_corferias",
    },
  ],
  "Puerto Rico": [
    {
      patterns: [/luis mu[nñ]oz mar[ií]n|\bsju\b|isla grande|fernando luis ribas|san juan airport/i],
      submarket: "San Juan Metro",
      confidence: "High",
      reason: "name_keyword_san_juan_airport",
    },
    {
      patterns: [/rafael hern[aá]ndez|\bbqn\b|aguadilla/i],
      submarket: "Northwest Air & Leisure Corridor",
      confidence: "High",
      reason: "name_keyword_aguadilla",
    },
    {
      patterns: [/mercedita|\bpse\b|ponce/i],
      submarket: "South Coast Regional City",
      confidence: "High",
      reason: "name_keyword_ponce",
    },
    {
      patterns: [/ceiba|fajardo|roosevelt roads|vieques ferry|culabra ferry/i],
      submarket: "East Coast / Island Access",
      confidence: "High",
      reason: "name_keyword_east_coast",
    },
    {
      patterns: [/vieques|culebra|antonio rivera|benjam[ií]n rivera/i],
      submarket: "Vieques / Culebra",
      confidence: "High",
      reason: "name_keyword_islands",
    },
    {
      patterns: [/mayag[uü]ez|\bmaq\b|rinc[oó]n/i],
      submarket: "West Coast / University & Surf",
      confidence: "High",
      reason: "name_keyword_west_coast",
    },
    {
      patterns: [/cabo rojo|gu[aá]nica|southwest/i],
      submarket: "Southwest Nature & Beach Corridor",
      confidence: "Medium",
      reason: "name_keyword_southwest",
    },
    {
      patterns: [/dorado|manat[ií]|north coast/i],
      submarket: "North Coast Resort Corridor",
      confidence: "Medium",
      reason: "name_keyword_north_coast",
    },
  ],
};

/** City labels that map to a submarket when exact (built countries). */
/** @type {Record<string, Record<string, { submarket: string, confidence: 'High'|'Medium' }>>} */
const CITY_EXACT_MAP = {
  Colombia: {
    bogota: { submarket: "Bogotá", confidence: "High" },
    cartagena: { submarket: "Cartagena", confidence: "High" },
    medellin: { submarket: "Medellín", confidence: "High" },
    barranquilla: { submarket: "Barranquilla", confidence: "High" },
    cali: { submarket: "Cali", confidence: "High" },
    "santa marta": { submarket: "Santa Marta", confidence: "High" },
    pereira: { submarket: "Coffee Region / Pereira", confidence: "High" },
    "san andres": { submarket: "San Andrés", confidence: "High" },
  },
  Panama: {
    "panama city": { submarket: "Panama City", confidence: "High" },
    colon: { submarket: "Canal / Logistics Corridor", confidence: "Medium" },
    "bocas del toro": { submarket: "Bocas del Toro", confidence: "High" },
    boquete: { submarket: "Boquete / Highlands", confidence: "Medium" },
    david: { submarket: "Boquete / Highlands", confidence: "Medium" },
  },
  "Costa Rica": {
    "san jose": { submarket: "San José Metro", confidence: "High" },
    alajuela: { submarket: "San José Metro", confidence: "High" },
    liberia: { submarket: "Guanacaste / Papagayo", confidence: "High" },
    tamarindo: { submarket: "Tamarindo / North Pacific", confidence: "High" },
    "quepos": { submarket: "Manuel Antonio / Central Pacific", confidence: "High" },
    "la fortuna": { submarket: "Arenal / La Fortuna", confidence: "High" },
    limon: { submarket: "Caribbean Coast", confidence: "Medium" },
    monteverde: { submarket: "Monteverde", confidence: "High" },
    jaco: { submarket: "Jacó / Herradura", confidence: "High" },
  },
  Peru: {
    cusco: { submarket: "Cusco Historic Center", confidence: "High" },
    cuzco: { submarket: "Cusco Historic Center", confidence: "High" },
    ollantaytambo: { submarket: "Ollantaytambo", confidence: "High" },
    urubamba: { submarket: "Urubamba", confidence: "High" },
    "aguas calientes": { submarket: "Machu Picchu Access", confidence: "High" },
    callao: { submarket: "Callao / Port", confidence: "High" },
    poroy: { submarket: "Cusco Historic Center", confidence: "High" },
    chinchero: { submarket: "Sacred Valley", confidence: "Medium" },
  },
  Mexico: {
    cancun: { submarket: "Cancún Hotel Zone", confidence: "Medium" },
    cozumel: { submarket: "Cozumel", confidence: "High" },
    tulum: { submarket: "Tulum", confidence: "High" },
    "playa del carmen": { submarket: "Riviera Maya / Playa del Carmen", confidence: "High" },
    "isla mujeres": { submarket: "Isla Mujeres", confidence: "High" },
  },
  "Dominican Republic": {
    "punta cana": { submarket: "Punta Cana / Bávaro / Cap Cana", confidence: "High" },
    bavaro: { submarket: "Punta Cana / Bávaro / Cap Cana", confidence: "High" },
    bávaro: { submarket: "Punta Cana / Bávaro / Cap Cana", confidence: "High" },
    higuey: { submarket: "Punta Cana / Bávaro / Cap Cana", confidence: "Medium" },
    macao: { submarket: "Punta Cana / Bávaro / Cap Cana", confidence: "Medium" },
    "cap cana": { submarket: "Punta Cana / Bávaro / Cap Cana", confidence: "High" },
    "uvero alto": { submarket: "Punta Cana / Bávaro / Cap Cana", confidence: "Medium" },
    miches: { submarket: "Miches / Costa Esmeralda", confidence: "High" },
    "la romana": { submarket: "La Romana / Bayahibe", confidence: "High" },
    bayahibe: { submarket: "La Romana / Bayahibe", confidence: "High" },
    "santo domingo": { submarket: "Santo Domingo Metro", confidence: "High" },
    "puerto plata": { submarket: "Puerto Plata / Sosúa / Cabarete", confidence: "High" },
    sosua: { submarket: "Puerto Plata / Sosúa / Cabarete", confidence: "High" },
    sousa: { submarket: "Puerto Plata / Sosúa / Cabarete", confidence: "High" },
    cabarete: { submarket: "Puerto Plata / Sosúa / Cabarete", confidence: "High" },
    samana: { submarket: "Samaná / Las Terrenas", confidence: "High" },
    samaná: { submarket: "Samaná / Las Terrenas", confidence: "High" },
    "las terrenas": { submarket: "Samaná / Las Terrenas", confidence: "High" },
    "las galeras": { submarket: "Samaná / Las Terrenas", confidence: "Medium" },
    santiago: { submarket: "Santiago / Cibao", confidence: "High" },
    "santiago de los caballeros": { submarket: "Santiago / Cibao", confidence: "High" },
    jarabacoa: { submarket: "Jarabacoa / Constanza", confidence: "High" },
    constanza: { submarket: "Jarabacoa / Constanza", confidence: "High" },
    "boca chica": { submarket: "Boca Chica / Juan Dolio", confidence: "High" },
    "juan dolio": { submarket: "Boca Chica / Juan Dolio", confidence: "High" },
    "playa juan dolio": { submarket: "Boca Chica / Juan Dolio", confidence: "High" },
    pedernales: { submarket: "Barahona / Pedernales", confidence: "High" },
    barahona: { submarket: "Barahona / Pedernales", confidence: "High" },
    bahoruco: { submarket: "Barahona / Pedernales", confidence: "Medium" },
    "san pedro de macoris": { submarket: "Santo Domingo Metro", confidence: "Low" },
    "puerto bahia": { submarket: "Santo Domingo Metro", confidence: "Low" },
  },
};

/**
 * @param {string} country
 * @param {string} haystack
 */
function matchKeywordRules(country, haystack) {
  const rules = COUNTRY_KEYWORD_RULES[country] || [];
  for (const rule of rules) {
    if (rule.patterns.some((rx) => rx.test(haystack))) {
      const resolved = resolveInRegistry(country, rule.submarket);
      if (resolved) {
        return {
          submarket: resolved,
          confidence: rule.confidence,
          reason: rule.reason,
        };
      }
    }
  }
  return null;
}

/**
 * @param {string} country
 * @param {string} city
 */
function matchExactCity(country, city) {
  const cityKey = norm(city);
  if (!cityKey) return null;

  const map = CITY_EXACT_MAP[country];
  if (map?.[cityKey]) {
    const hit = map[cityKey];
    const resolved = resolveInRegistry(country, hit.submarket);
    if (resolved) {
      return { submarket: resolved, confidence: hit.confidence, reason: "exact_city_map" };
    }
  }

  const options = getSubmarketOptionsForCountry(country).filter((o) => o !== "Other");
  for (const opt of options) {
    if (norm(opt) === cityKey) {
      return { submarket: opt, confidence: "Medium", reason: "exact_city_submarket_label" };
    }
  }

  if (country === "Puerto Rico") {
    const pr = inferSubmarketFromCity(city, country);
    if (pr) {
      return { submarket: pr, confidence: "High", reason: "pr_city_inference_map" };
    }
  }

  if (country === "Costa Rica" && (cityKey.includes("san jose") || cityKey === "heredia")) {
    return {
      submarket: resolveInRegistry(country, "San José Metro"),
      confidence: "Medium",
      reason: "costa_rica_metro_city",
    };
  }

  return null;
}

/**
 * @param {object} record
 * @param {object} [options]
 * @param {boolean} [options.force]
 * @param {string} [options.minConfidence]
 */
export function inferTravelInfrastructureSubmarket(record, options = {}) {
  const country = String(record.country || "").trim();
  const currentSubmarket = String(record.submarket || "").trim() || null;
  const city = String(record.city || "").trim();
  const name = String(record.name || "").trim();
  const notes = String(record.notes || "").trim();
  const haystack = buildHaystack(record);

  const base = {
    recordId: record.id || record.recordId || "",
    name,
    country,
    city,
    currentSubmarket,
    inferredSubmarket: null,
    confidence: "No Match",
    reason: "no_match",
    proposedNotesAppend: "",
    shouldUpdate: false,
  };

  if (!country || !BUILT_RADAR_COUNTRIES.some((c) => norm(c) === norm(country))) {
    return { ...base, reason: "country_not_in_built_list" };
  }

  const populatedNonOther = currentSubmarket && currentSubmarket !== "Other";
  if (populatedNonOther && !options.force) {
    return { ...base, reason: "already_populated_non_other" };
  }

  let hit = null;

  const notesExtracted = extractSubmarketFromNotes(notes);
  if (notesExtracted) {
    const resolved = resolveInRegistry(country, notesExtracted);
    if (resolved) {
      hit = { submarket: resolved, confidence: "High", reason: "notes_submarket_prefix" };
    }
  }

  if (!hit) {
    hit = matchKeywordRules(country, haystack);
  }

  if (!hit) {
    hit = matchExactCity(country, city);
  }

  if (!hit?.submarket) {
    return {
      ...base,
      confidence: "No Match",
      reason: hit?.reason || "no_confident_inference",
    };
  }

  const inferredSubmarket = hit.submarket;
  const confidence = hit.confidence;
  const proposedNotesAppend = `${INFERENCE_NOTE_TAG} ${inferredSubmarket} (${confidence}).`;

  const minConfidence = options.minConfidence || "High";
  const shouldUpdate =
    meetsMinConfidence(confidence, minConfidence) &&
    Boolean(inferredSubmarket) &&
    inferredSubmarket !== currentSubmarket;

  return {
    ...base,
    inferredSubmarket,
    confidence,
    reason: hit.reason,
    proposedNotesAppend,
    shouldUpdate,
  };
}

/**
 * @param {object[]} records
 * @param {object} [options]
 */
export function planBuiltCountryTiSubmarketInference(records, options = {}) {
  const countries = options.countries || BUILT_RADAR_COUNTRIES;
  const countrySet = new Set(countries.map(norm));
  const scoped = (records || []).filter((r) => countrySet.has(norm(r.country)));

  const results = scoped.map((r) => inferTravelInfrastructureSubmarket(r, options));
  const proposed = results.filter((r) => r.shouldUpdate);
  const review = results.filter(
    (r) =>
      (r.confidence === "No Match" || r.confidence === "Low") &&
      (!r.currentSubmarket || r.currentSubmarket === "Other")
  );

  const byCountry = {};
  const bySubmarket = {};
  for (const r of proposed) {
    byCountry[r.country] = (byCountry[r.country] || 0) + 1;
    bySubmarket[r.inferredSubmarket] = (bySubmarket[r.inferredSubmarket] || 0) + 1;
  }

  const skippedAlreadyPopulated = results.filter((r) => r.reason === "already_populated_non_other").length;
  const skippedLowConfidence = results.filter(
    (r) =>
      r.inferredSubmarket &&
      !r.shouldUpdate &&
      r.reason !== "already_populated_non_other" &&
      !meetsMinConfidence(r.confidence, options.minConfidence || "High")
  ).length;
  const skippedNoMatch = results.filter((r) => r.confidence === "No Match").length;

  return {
    scanned: scoped.length,
    eligible: results.filter((r) => !r.currentSubmarket || r.currentSubmarket === "Other").length,
    proposedUpdates: proposed.length,
    skippedAlreadyPopulated,
    skippedLowConfidence,
    skippedNoMatch,
    proposedByCountry: byCountry,
    proposedBySubmarket: bySubmarket,
    results,
    proposed,
    review,
  };
}

/**
 * Census backfill inference — all registry countries; STR submarket may be used as city hint.
 * @param {object} record
 * @param {object} [options]
 * @param {string} [options.minConfidence]
 */
export function inferCensusSubmarketCorridor(record, options = {}) {
  const country = resolveCensusCountryKey(String(record.country || "").trim());
  const currentSubmarket = String(record.submarket || "").trim() || null;
  const city = String(record.city || "").trim();
  const haystack = [buildHaystack(record), norm(currentSubmarket), norm(city)]
    .filter(Boolean)
    .join(" | ");

  const base = {
    inferredSubmarket: null,
    confidence: "No Match",
    reason: "no_match",
  };

  if (!country) return base;

  let hit = matchKeywordRules(country, haystack);

  if (!hit && currentSubmarket) {
    hit = matchExactCity(country, currentSubmarket);
  }

  if (!hit) {
    hit = matchExactCity(country, city);
  }

  if (!hit?.submarket) {
    return base;
  }

  const minConfidence = options.minConfidence || "Medium";
  if (!meetsMinConfidence(hit.confidence, minConfidence)) {
    return { ...base, reason: "below_min_confidence" };
  }

  return {
    inferredSubmarket: hit.submarket,
    confidence: hit.confidence,
    reason: hit.reason,
  };
}

export { INFERENCE_NOTE_TAG, BUILT_RADAR_COUNTRIES };
