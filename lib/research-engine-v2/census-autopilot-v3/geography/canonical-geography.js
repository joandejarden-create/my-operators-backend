/**
 * Canonical Dealality geography object — single entry point for Census Autopilot V3.0.3+.
 */

import { COUNTRY_TO_SUB_CONTINENT } from "../../../hotel-census/geography-enrichment-contract.js";
import {
  resolveDealalityMarket,
  resolveDealalityGeography,
} from "../../census-autopilot-v2-2/geography-expansion.js";
import { proposeCensusSubmarketCorridor } from "../../../hotel-census/census-dealality-submarket.js";
import { normalizeSubmarketLabel, getSubmarketOptionsForCountry } from "../../../radar-submarket.js";
import { resolveCensusCountryKey } from "../../../hotel-census/census-str-submarket-corridors.js";
import { resolveStateRegionV3 } from "./state-region-resolver-v3.js";
import { getAdminLevel } from "./country-admin-levels.js";
import { classifySubmarketApplicability } from "./applicability-rules.js";
import { normGeoLabel } from "./admin-city-aliases.js";
import { isDescriptorCity } from "../../census-city-state-normalizer.js";
import { isSingleMarketCountry } from "./dealality-market-registry.js";

function sanitizeCity(city, address, country = null) {
  let c = String(city || "").trim();
  if (!c || /^\d/.test(c) || /^\d{4,}-\d{3}/.test(c) || isDescriptorCity(c)) {
    const parts = String(address || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    const cand = parts.find(
      (p) =>
        /[A-Za-zÁÉÍÓÚáéíóúñÑ]/.test(p) &&
        !/^\d+$/.test(p) &&
        !isDescriptorCity(p)
    );
    if (cand) c = cand.replace(/\b\d{5}\b/g, "").trim();
    else if (isDescriptorCity(c)) c = "";
  }
  if (/^\d/.test(c)) return null;
  // Country-as-city is not a City
  if (country && c && c.toLowerCase() === String(country).toLowerCase()) return null;
  if (isDescriptorCity(c)) return null;
  return c || null;
}

/** City/neighborhood → registry submarket (labels must normalize into country options). */
const CITY_TO_SUBMARKET = Object.freeze({
  Mexico: {
    cancun: "Cancún Hotel Zone",
    "playa del carmen": "Riviera Maya / Playa del Carmen",
    tulum: "Tulum",
    "cabo san lucas": "Los Cabos",
    "san jose del cabo": "Los Cabos",
    "puerto vallarta": "Puerto Vallarta / Riviera Nayarit",
  },
  "Costa Rica": {
    guanacaste: "Guanacaste / Papagayo",
    papagayo: "Guanacaste / Papagayo",
    "santa cruz": "Guanacaste / Papagayo",
    liberia: "Guanacaste / Papagayo",
    tamarindo: "Tamarindo / North Pacific",
    herradura: "Jacó / Herradura",
    "playa herradura": "Jacó / Herradura",
    "los suenos": "Jacó / Herradura",
    monteverde: "Monteverde",
    "san jose": "San José Metro",
    "manuel antonio": "Manuel Antonio / Central Pacific",
    jaco: "Jacó / Herradura",
  },
  Jamaica: {
    "montego bay": "Montego Bay",
    "rose hall": "Montego Bay",
    kingston: "Kingston",
    "port antonio": "Port Antonio",
    westmoreland: "South Coast",
    bluefields: "South Coast",
    negril: "Negril",
    "ocho rios": "Ocho Rios",
  },
  Barbados: {
    bridgetown: "Bridgetown",
    "christ church": "South Coast",
    "saint philip": "South Coast",
    "st philip": "South Coast",
  },
  Brazil: {
    pinheiros: "Faria Lima / Itaim Bibi",
    jardins: "Paulista / Jardins",
    "cidade moncoes": "Faria Lima / Itaim Bibi",
    "sao paulo": "Paulista / Jardins",
    bauru: "São Paulo",
    manaus: "Manaus",
    "rio de janeiro": "Rio de Janeiro",
  },
  Argentina: {
    "buenos aires": "Buenos Aires",
    pilar: "Buenos Aires",
    "villa la angostura": "Bariloche",
    salta: "Salta",
    "villa san lorenzo": "Salta",
    ituzaingo: "Puerto Iguazú",
    mendoza: "Mendoza",
    cordoba: "Córdoba",
    bariloche: "Bariloche",
  },
  "Dominican Republic": {
    sosua: "Puerto Plata / Sosúa / Cabarete",
    "puerto plata": "Puerto Plata / Sosúa / Cabarete",
    "costa norte": "Puerto Plata / Sosúa / Cabarete",
    "punta cana": "Punta Cana / Bávaro / Cap Cana",
    "santo domingo": "Santo Domingo Metro",
  },
});

function resolveCitySubmarket(country, city, name, address) {
  const map = CITY_TO_SUBMARKET[country];
  const blob = normGeoLabel(`${city || ""} ${name || ""} ${address || ""}`);
  if (map) {
    for (const [k, v] of Object.entries(map)) {
      if (blob.includes(k)) {
        const normalized = normalizeSubmarketLabel(v, country);
        if (normalized && normalized !== "Other") {
          return { submarket: normalized, method: "city_submarket_alias" };
        }
        const opts = getSubmarketOptionsForCountry(resolveCensusCountryKey(country));
        if (opts.includes(v)) return { submarket: v, method: "city_submarket_alias" };
      }
    }
  }
  if (country === "Costa Rica") {
    if (/guanacaste|papagayo|conchal|costa elena/i.test(blob)) {
      return { submarket: "Guanacaste / Papagayo", method: "name_corridor" };
    }
    if (/herradura|los suenos|los sueños/i.test(blob)) {
      return { submarket: "Jacó / Herradura", method: "name_corridor" };
    }
    if (/monteverde|belmar/i.test(blob)) return { submarket: "Monteverde", method: "name_corridor" };
    if (/manuel antonio/i.test(blob)) {
      return { submarket: "Manuel Antonio / Central Pacific", method: "name_corridor" };
    }
  }
  if (country === "Brazil" && /bel[eé]m|belem|ananindeua|maiorana/i.test(blob)) {
    // No Belém corridor in registry — leave unresolved rather than invent
    return null;
  }
  return null;
}

/**
 * Build one canonical geography object for a property.
 */
export function resolveCanonicalGeography(input = {}) {
  const country = String(input.country || "").trim() || null;
  const address = input.address || null;
  const cityRaw = input.city || null;
  const city = sanitizeCity(cityRaw, address, country);
  const name = input.name || null;
  const lat = input.latitude != null ? Number(input.latitude) : null;
  const lng = input.longitude != null ? Number(input.longitude) : null;
  const officialState = input.state_region || input.official_state || null;

  const adminMeta = getAdminLevel(country);
  const continent = country ? "Americas" : null;
  const subContinent = country ? COUNTRY_TO_SUB_CONTINENT[country] || null : null;

  const stateRes = resolveStateRegionV3({
    country,
    city,
    address,
    name,
    official_state: officialState,
    latitude: lat,
    longitude: lng,
    coords_production_eligible: input.coords_production_eligible !== false,
    address_production_eligible: input.address_production_eligible !== false,
  });

  const stateRegion = stateRes.ok ? stateRes.normalized_state_region : null;

  const market =
    input.market_override ||
    resolveDealalityMarket(country, city, {
      state: stateRegion || officialState,
      latitude: lat,
      longitude: lng,
    }) ||
    null;
  // Never: market = country. Unresolved stays null.

  const sub = proposeCensusSubmarketCorridor({
    country,
    city,
    name,
    market,
  });

  const geoLegacy = resolveDealalityGeography({
    country,
    city,
    name,
    address,
    state_region: stateRegion,
  });

  let submarket = sub.submarket || geoLegacy.submarket || null;
  let subConf = sub.confidence || geoLegacy.submarket_confidence || "No Match";
  let subReason = sub.reason || geoLegacy.submarket_reason || null;

  if (!submarket || subConf === "No Match") {
    const hit = resolveCitySubmarket(country, city, name, address);
    if (hit?.submarket) {
      submarket = hit.submarket;
      subConf = "High";
      subReason = hit.method;
    }
  }

  if (submarket) {
    const norm = normalizeSubmarketLabel(submarket, country);
    if (!norm || norm === "Other") {
      const opts = getSubmarketOptionsForCountry(resolveCensusCountryKey(country));
      if (!opts.includes(submarket)) {
        submarket = null;
        subConf = "No Match";
        subReason = "dropped_non_registry_label";
      }
    } else {
      submarket = norm;
    }
  }

  let applicability = classifySubmarketApplicability({
    country,
    market,
    submarket: subConf !== "No Match" ? submarket : null,
    submarketConfidence: subConf,
  });

  // Single-market countries (island taxonomy): Market may equal Country → Submarket N/A when no corridor hit.
  // Multi-market Country-as-Market is invalid and must not invent N/A.
  if (
    (!submarket || subConf === "No Match") &&
    market &&
    country &&
    String(market) === String(country) &&
    isSingleMarketCountry(country)
  ) {
    applicability = "NOT_APPLICABLE";
  }

  const opts = getSubmarketOptionsForCountry(resolveCensusCountryKey(country || "")).filter(
    (o) => o && o !== "Other"
  );
  // Costa Rica / Jamaica / Barbados / Mexico city markets remain REQUIRED when corridors exist
  if (
    applicability === "NOT_APPLICABLE" &&
    opts.length >= 4 &&
    market !== country &&
    (!submarket || subConf === "No Match")
  ) {
    applicability = "UNKNOWN";
  }

  if (applicability === "NOT_APPLICABLE" && (!submarket || subConf === "No Match")) {
    submarket = null;
    subConf = "Not Applicable";
    subReason = "market_terminal_or_no_corridor_structure";
  }

  const geography_confidence = (() => {
    let score = 0;
    if (country) score += 1;
    if (city) score += 1;
    if (stateRegion) score += 2;
    if (market) score += 1;
    if (lat != null && lng != null) score += 2;
    if (address) score += 1;
    if (applicability === "NOT_APPLICABLE" || (submarket && subConf !== "No Match")) score += 1;
    if (score >= 8) return "High";
    if (score >= 5) return "Medium";
    return "Low";
  })();

  const derived_from = [];
  if (address) {
    derived_from.push({
      claim: "Address",
      production_eligible: input.address_production_eligible !== false,
    });
  }
  if (lat != null) {
    derived_from.push({
      claim: "Latitude",
      production_eligible: input.coords_production_eligible !== false,
    });
  }
  if (lng != null) {
    derived_from.push({
      claim: "Longitude",
      production_eligible: input.coords_production_eligible !== false,
    });
  }
  if (city) derived_from.push({ claim: "City", production_eligible: true });
  if (country) derived_from.push({ claim: "Country", production_eligible: true });

  const stateProd = Boolean(stateRes.production_eligible);

  return {
    version: "canonical-geography-v3.0.3",
    country,
    continent,
    sub_continent: subContinent,
    state_region: stateRegion,
    administrative_type: adminMeta.administrative_type,
    city,
    city_raw: cityRaw,
    city_normalized: city ? normGeoLabel(city) : null,
    municipality: city,
    market,
    submarket:
      applicability === "NOT_APPLICABLE" && subConf === "Not Applicable" ? null : submarket,
    submarket_confidence: subConf,
    submarket_reason: subReason,
    submarket_applicability: applicability,
    latitude: lat,
    longitude: lng,
    address,
    postal_code: input.postal_code || null,
    geography_confidence,
    geography_method: {
      state_region: stateRes.method,
      market: "dealality_market_resolver",
      submarket: subReason || "corridor_proposer",
    },
    source_evidence: {
      state: stateRes.evidence || [],
      boundary_source: stateRes.boundary_source,
      state_confidence: stateRes.confidence,
    },
    derived_from_claims: derived_from,
    production_eligible: {
      state_region: stateProd && Boolean(stateRegion),
      market: Boolean(market),
      submarket:
        applicability === "NOT_APPLICABLE"
          ? true
          : Boolean(submarket && subConf !== "No Match" && subConf !== "Not Applicable"),
      continent: true,
      sub_continent: Boolean(subContinent),
    },
    last_verified: new Date().toISOString(),
    state_resolution: stateRes,
  };
}

export function classifySubmarketForensicV4(row, geo) {
  if (geo.submarket_applicability === "NOT_APPLICABLE") return null;
  if (
    geo.submarket &&
    geo.submarket_confidence !== "No Match" &&
    geo.submarket_confidence !== "Not Applicable"
  ) {
    return null;
  }
  const city = String(geo.city || row.city || "");
  if (/^\d/.test(city) || (!city && !geo.address)) return "H. ADDRESS/CITY QUALITY ISSUE";
  if (!geo.state_region) return "H. ADDRESS/CITY QUALITY ISSUE";
  if (geo.latitude == null) return "E. COORDINATE NEAR BOUNDARY";
  if (!geo.market) return "C. MARKET RULE MISSING";
  if (String(geo.market) === String(geo.country) && !city) return "I. TAXONOMY SHOULD END AT MARKET";
  if (String(geo.market) === String(geo.country)) return "A. CITY ALIAS";
  return "D. SUBMARKET POLYGON/RULE MISSING";
}
