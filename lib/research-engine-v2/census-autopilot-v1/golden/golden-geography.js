/**
 * Dealality geography hierarchy for Golden Census (Mexico-first, CALA-scalable).
 * Does NOT use STR Market/Submarket taxonomy.
 *
 * Hierarchy: Continent → Sub-Continent → Country → State/Region → Market → Submarket → City
 */

import { countryToSubContinent } from "../../../hotel-census/geography-enrichment-contract.js";
import { COUNTRY_CONFIGS } from "../../../radar-buildout/country-configs.js";
import { proposeCensusSubmarketCorridor } from "../../../hotel-census/census-dealality-submarket.js";

export const GOLDEN_GEO_VERSION = "dealality-geography-v1.2-mexico";

/** Dealality continent labels (global-ready). */
export const DEALALITY_CONTINENTS = Object.freeze([
  "Americas",
  "Europe",
  "Middle East & Africa",
  "Asia Pacific",
]);

/**
 * Sub-continents within Americas (Dealality operating geography).
 * Mexico is Sub-Continent = North America, while Dealality Region UI = Caribbean & Latin America (CALA).
 */
export const DEALALITY_SUB_CONTINENTS_AMERICAS = Object.freeze([
  "North America",
  "Central America",
  "Caribbean",
  "South America",
]);

/**
 * Mexico commercial markets (Dealality) — aligns with country-configs initialMarkets + expansions.
 */
export const MEXICO_DEALALITY_MARKETS = Object.freeze([
  "Cancún / Riviera Maya",
  "Mexico City",
  "Los Cabos",
  "Guadalajara",
  "Monterrey",
  "Puerto Vallarta / Riviera Nayarit",
  "Mérida / Yucatán",
  "Oaxaca",
  "Puebla",
  "Acapulco",
  "Querétaro",
  "Morelia",
  "Veracruz",
  "Mazatlán",
  "Guanajuato",
  "Tijuana / Baja California",
  "Chihuahua",
  "Hermosillo / Sonora",
  "San Luis Potosí",
  "Saltillo / Coahuila",
  "Aguascalientes",
  "Toluca / Estado de México",
  "Cuernavaca / Morelos",
  "Villahermosa / Tabasco",
  "Tuxtla Gutiérrez / Chiapas",
  "Tampico / Tamaulipas",
  "La Paz / BCS Secondary",
  "Manzanillo / Colima",
  "Huatulco",
  "Other Mexico",
]);

/** Coastal / resort markets where Beach amenity is typically applicable. */
export const MEXICO_COASTAL_MARKETS = new Set([
  "Cancún / Riviera Maya",
  "Los Cabos",
  "Puerto Vallarta / Riviera Nayarit",
  "Acapulco",
  "Mazatlán",
  "Veracruz",
  "La Paz / BCS Secondary",
  "Manzanillo / Colima",
  "Huatulco",
]);

/** Markets where All-Inclusive is commonly applicable (not universal). */
export const MEXICO_AI_APPLICABLE_MARKETS = new Set([
  "Cancún / Riviera Maya",
  "Los Cabos",
  "Puerto Vallarta / Riviera Nayarit",
  "Huatulco",
]);

function norm(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * City / locality → Dealality Market (Mexico).
 * Deterministic; ambiguous → Other Mexico + needs_review.
 */
const CITY_TO_MARKET = Object.freeze([
  { re: /cancun|cancún|isla mujeres|costa mujeres|playa mujeres|puerto cancun/i, market: "Cancún / Riviera Maya" },
  { re: /playa del carmen|riviera maya|tulum|akumal|puerto aventuras|mayakoba|cozumel|solidaridad/i, market: "Cancún / Riviera Maya" },
  { re: /mexico city|ciudad de mexico|cdmx|polanco|reforma|santa fe|condesa|roma|coyoacan|san angel|^mexico$/i, market: "Mexico City" },
  { re: /cabo san lucas|san jose del cabo|san jos[eé] del cabo|los cabos|\bcabo\b/i, market: "Los Cabos" },
  { re: /todos santos|la paz/i, market: "La Paz / BCS Secondary" },
  { re: /guadalajara|zapopan|tlaquepaque/i, market: "Guadalajara" },
  { re: /monterrey|san pedro garza|garza garcia/i, market: "Monterrey" },
  { re: /puerto vallarta|vallarta|nuevo vallarta|riviera nayarit|nayarit|punt[ao] de mita|sayulita/i, market: "Puerto Vallarta / Riviera Nayarit" },
  { re: /merida|m[eé]rida|yucatan|yucat[aá]n/i, market: "Mérida / Yucatán" },
  { re: /huatulco/i, market: "Huatulco" },
  { re: /oaxaca/i, market: "Oaxaca" },
  { re: /puebla/i, market: "Puebla" },
  { re: /acapulco|chilpancingo/i, market: "Acapulco" },
  { re: /queretaro|quer[eé]taro/i, market: "Querétaro" },
  { re: /morelia|uruapan|la piedad/i, market: "Morelia" },
  { re: /veracruz|boca del rio|xalapa|tuxpan/i, market: "Veracruz" },
  { re: /mazatlan|mazatl[aá]n|culiacan|culiacán/i, market: "Mazatlán" },
  { re: /guanajuato|leon|le[oó]n|silao|irapuato|celaya|salamanca|san miguel de allende/i, market: "Guanajuato" },
  { re: /tijuana|mexicali|ensenada|rosarito/i, market: "Tijuana / Baja California" },
  { re: /chihuahua|ciudad juarez|ju[aá]rez/i, market: "Chihuahua" },
  { re: /hermosillo|ciudad obregon|obreg[oó]n/i, market: "Hermosillo / Sonora" },
  { re: /san luis potosi|san luis potos[ií]/i, market: "San Luis Potosí" },
  { re: /saltillo|torreon|torre[oó]n|monclova|piedras negras/i, market: "Saltillo / Coahuila" },
  { re: /aguascalientes/i, market: "Aguascalientes" },
  { re: /toluca|metepec|estado de mexico/i, market: "Toluca / Estado de México" },
  { re: /cuernavaca/i, market: "Cuernavaca / Morelos" },
  { re: /villahermosa/i, market: "Villahermosa / Tabasco" },
  { re: /tuxtla/i, market: "Tuxtla Gutiérrez / Chiapas" },
  { re: /tampico|reynosa|matamoros/i, market: "Tampico / Tamaulipas" },
  { re: /manzanillo/i, market: "Manzanillo / Colima" },
]);

/** State/region hints from city/market (Mexico). */
const MARKET_TO_STATE = Object.freeze({
  "Cancún / Riviera Maya": "Quintana Roo",
  "Mexico City": "Ciudad de México",
  "Los Cabos": "Baja California Sur",
  "La Paz / BCS Secondary": "Baja California Sur",
  Guadalajara: "Jalisco",
  Monterrey: "Nuevo León",
  "Puerto Vallarta / Riviera Nayarit": "Jalisco / Nayarit",
  "Mérida / Yucatán": "Yucatán",
  Oaxaca: "Oaxaca",
  Huatulco: "Oaxaca",
  Puebla: "Puebla",
  Acapulco: "Guerrero",
  "Querétaro": "Querétaro",
  Morelia: "Michoacán",
  Veracruz: "Veracruz",
  Mazatlán: "Sinaloa",
  Guanajuato: "Guanajuato",
  "Tijuana / Baja California": "Baja California",
  Chihuahua: "Chihuahua",
  "Hermosillo / Sonora": "Sonora",
  "San Luis Potosí": "San Luis Potosí",
  "Saltillo / Coahuila": "Coahuila",
  Aguascalientes: "Aguascalientes",
  "Toluca / Estado de México": "Estado de México",
  "Cuernavaca / Morelos": "Morelos",
  "Villahermosa / Tabasco": "Tabasco",
  "Tuxtla Gutiérrez / Chiapas": "Chiapas",
  "Tampico / Tamaulipas": "Tamaulipas",
  "Manzanillo / Colima": "Colima",
});

/**
 * @param {string} city
 * @param {string} [name]
 */
export function resolveMexicoMarket(city, name = "") {
  const hay = `${city || ""} ${name || ""}`;
  for (const row of CITY_TO_MARKET) {
    if (row.re.test(hay)) {
      return {
        market: row.market,
        confidence: "High",
        source: "dealality_city_market_map",
        needs_review: false,
        derived: true,
      };
    }
  }
  return {
    market: "Other Mexico",
    confidence: "Low",
    source: "dealality_market_fallback_other",
    needs_review: true,
    derived: true,
  };
}

/**
 * Continent / Sub-Continent for a country (deterministic).
 * Mexico: Continent=Americas, Sub-Continent=North America (Dealality CALA operating region remains separate).
 */
export function resolveContinentHierarchy(country) {
  const c = String(country || "").trim();
  const sub = countryToSubContinent(c) || null;
  let continent = null;
  if (sub && DEALALITY_SUB_CONTINENTS_AMERICAS.includes(sub)) {
    continent = "Americas";
  } else if (["United States", "USA", "Canada"].includes(c)) {
    continent = "Americas";
  }
  return {
    Continent: continent,
    "Sub-Continent": sub,
    dealality_operating_region:
      c === "Mexico" || (sub && ["Central America", "Caribbean", "South America", "North America"].includes(sub))
        ? "Caribbean & Latin America"
        : null,
    source: "dealality_country_geography_map",
    derived: true,
    mexico_note:
      c === "Mexico"
        ? "Mexico Sub-Continent = North America; Dealality operating Region = Caribbean & Latin America (CALA)."
        : null,
  };
}

/**
 * Full geography assignment for a hotel record.
 * @param {{ city?: string, country?: string, name?: string, market?: string, Submarket?: string }} row
 */
export function assignDealalityGeography(row) {
  const country = String(row.country || row.Country || "Mexico").trim();
  const city = String(row.city || row.City || "").trim();
  const name = String(row.name || row["Property Name"] || "").trim();

  const hierarchy = resolveContinentHierarchy(country);
  const marketRes = country === "Mexico" ? resolveMexicoMarket(city, name) : {
    market: null,
    confidence: "Low",
    source: "unsupported_country",
    needs_review: true,
    derived: false,
  };

  const market = marketRes.market;
  const state = MARKET_TO_STATE[market] || null;

  const corridor = proposeCensusSubmarketCorridor({
    country,
    city,
    name,
    market,
    Submarket: row.Submarket || row.submarket || "",
  }, { minConfidence: "Medium" });

  let submarket = corridor.submarket;
  let sub_source = corridor.source;
  let sub_conf = corridor.confidence;
  let sub_review = corridor.confidence === "No Match" || corridor.confidence === "Low";

  // Market-level fallback when corridor inference fails
  if (!submarket && market && market !== "Other Mexico") {
    const cfg = COUNTRY_CONFIGS.Mexico;
    const marketSubs = cfg?.marketSubmarkets?.[market];
    if (marketSubs?.length) {
      submarket = "Other";
      sub_source = "dealality_market_submarket_other_fallback";
      sub_conf = "Low";
      sub_review = true;
    } else {
      submarket = market;
      sub_source = "dealality_market_as_corridor";
      sub_conf = "Medium";
      sub_review = false;
    }
  }
  if (!submarket && market === "Other Mexico") {
    submarket = "Other";
    sub_source = "dealality_other_mexico_submarket";
    sub_conf = "Low";
    sub_review = true;
  }

  return {
    Continent: hierarchy.Continent,
    "Sub-Continent": hierarchy["Sub-Continent"],
    Country: country,
    "State / Region": state,
    Market: market,
    Submarket: submarket || null,
    City: city || null,
    geography_provenance: {
      continent_source: hierarchy.source,
      market_source: marketRes.source,
      market_confidence: marketRes.confidence,
      market_needs_review: marketRes.needs_review,
      submarket_source: sub_source,
      submarket_confidence: sub_conf,
      submarket_needs_review: sub_review,
      dealality_operating_region: hierarchy.dealality_operating_region,
      mexico_note: hierarchy.mexico_note,
      str_taxonomy_used: false,
    },
  };
}

/**
 * Build taxonomy artifact payload for Mexico.
 */
export function buildMexicoMarketSubmarketTaxonomy() {
  const cfg = COUNTRY_CONFIGS.Mexico;
  return {
    version: GOLDEN_GEO_VERSION,
    country: "Mexico",
    continent: "Americas",
    sub_continent: "North America",
    dealality_operating_region: "Caribbean & Latin America",
    markets: MEXICO_DEALALITY_MARKETS,
    market_submarkets: cfg?.marketSubmarkets || {},
    submarkets_flat: cfg?.submarkets || [],
    coastal_markets: [...MEXICO_COASTAL_MARKETS],
    all_inclusive_applicable_markets: [...MEXICO_AI_APPLICABLE_MARKETS],
    rules: [
      "Market and Submarket are Dealality classifications — not STR.",
      "City maps deterministically to Market via city/locality patterns.",
      "Submarket prefers corridor inference; otherwise market-as-corridor or Other with review flag.",
      "Ambiguous properties → Other Mexico + needs_review.",
    ],
  };
}
