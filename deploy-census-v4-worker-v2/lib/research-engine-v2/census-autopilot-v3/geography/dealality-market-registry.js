/**
 * Dealality Market registry + semantic validation (Census Autopilot V3/V4).
 *
 * Hard rule: COUNTRY ≠ MARKET unless taxonomy explicitly defines a single-market
 * country whose canonical Market name equals the Country string.
 *
 * Never: market || country
 */

import { COUNTRY_CONFIGS } from "../../../radar-buildout/country-configs.js";
import { BUILD_STRATEGY_TYPES } from "../../../radar-buildout/country-build-strategies.js";
import {
  DEALALITY_MARKET_REGISTRY_VNEXT_VERSION,
  MARKET_ALIASES_TO_CANONICAL,
  EXTRA_DEALALITY_MARKETS_VNEXT,
  CITY_TO_MARKET_VNEXT,
  STATE_TO_MARKET_EXPLICIT,
  MARKET_CENTROIDS_VNEXT,
  SINGLE_MARKET_ALLOWLIST_AUDIT,
} from "./dealality-market-registry-vnext.js";
import {
  DEALALITY_MARKET_REGISTRY_VNEXT2_VERSION,
  EXTRA_DEALALITY_MARKETS_VNEXT2,
  CITY_TO_MARKET_VNEXT2,
} from "./dealality-market-registry-vnext2.js";

export const DEALALITY_MARKET_REGISTRY_VERSION = DEALALITY_MARKET_REGISTRY_VNEXT2_VERSION;

export {
  MARKET_ALIASES_TO_CANONICAL,
  EXTRA_DEALALITY_MARKETS_VNEXT,
  CITY_TO_MARKET_VNEXT,
  STATE_TO_MARKET_EXPLICIT,
  MARKET_CENTROIDS_VNEXT,
  SINGLE_MARKET_ALLOWLIST_AUDIT,
  EXTRA_DEALALITY_MARKETS_VNEXT2,
  CITY_TO_MARKET_VNEXT2,
};

export const MARKET_CLASS = Object.freeze({
  VALID_MARKET: "VALID_MARKET",
  COUNTRY_AS_MARKET: "COUNTRY_AS_MARKET",
  STATE_AS_MARKET: "STATE_AS_MARKET",
  CITY_AS_MARKET: "CITY_AS_MARKET",
  INVALID_MARKET: "INVALID_MARKET",
  UNRESOLVED: "UNRESOLVED",
  CONFLICT: "CONFLICT",
});

/**
 * City|country → canonical Dealality Market (not STR).
 * Prefer names that exist in COUNTRY_CONFIGS.initialMarkets when present.
 */
export const CITY_TO_DEALALITY_MARKET = Object.freeze({
  // Mexico
  "cancun|mexico": "Cancún / Riviera Maya",
  "cancún|mexico": "Cancún / Riviera Maya",
  "playa del carmen|mexico": "Cancún / Riviera Maya",
  "tulum|mexico": "Cancún / Riviera Maya",
  "cozumel|mexico": "Cancún / Riviera Maya",
  "mexico city|mexico": "Mexico City",
  "ciudad de mexico|mexico": "Mexico City",
  "ciudad de méxico|mexico": "Mexico City",
  "cdmx|mexico": "Mexico City",
  "guadalajara|mexico": "Guadalajara",
  "zapopan|mexico": "Guadalajara",
  "monterrey|mexico": "Monterrey",
  "san pedro garza garcia|mexico": "Monterrey",
  "los cabos|mexico": "Los Cabos",
  "cabo san lucas|mexico": "Los Cabos",
  "san jose del cabo|mexico": "Los Cabos",
  "san josé del cabo|mexico": "Los Cabos",
  "puerto vallarta|mexico": "Puerto Vallarta / Riviera Nayarit",
  "nuevo vallarta|mexico": "Puerto Vallarta / Riviera Nayarit",
  "riviera nayarit|mexico": "Puerto Vallarta / Riviera Nayarit",
  "merida|mexico": "Mérida / Yucatán",
  "mérida|mexico": "Mérida / Yucatán",
  // Dominican Republic
  "punta cana|dominican republic": "Punta Cana / East Coast",
  "bavaro|dominican republic": "Punta Cana / East Coast",
  "bávaro|dominican republic": "Punta Cana / East Coast",
  "cap cana|dominican republic": "Punta Cana / East Coast",
  "santo domingo|dominican republic": "Greater Santo Domingo",
  "puerto plata|dominican republic": "Puerto Plata",
  "sosua|dominican republic": "Puerto Plata",
  "sosúa|dominican republic": "Puerto Plata",
  // Costa Rica
  "san jose|costa rica": "San José / Central Valley",
  "san josé|costa rica": "San José / Central Valley",
  "liberia|costa rica": "Guanacaste / Papagayo",
  "tamarindo|costa rica": "Guanacaste / Papagayo",
  "jaco|costa rica": "Central Pacific",
  "jacó|costa rica": "Central Pacific",
  // Brazil
  "sao paulo|brazil": "São Paulo",
  "são paulo|brazil": "São Paulo",
  "guarulhos|brazil": "São Paulo",
  "barueri|brazil": "São Paulo",
  "rio de janeiro|brazil": "Rio de Janeiro",
  "brasilia|brazil": "Brasília",
  "brasília|brazil": "Brasília",
  "salvador|brazil": "Salvador",
  "recife|brazil": "Recife",
  "florianopolis|brazil": "Florianópolis",
  "florianópolis|brazil": "Florianópolis",
  "curitiba|brazil": "Other",
  "belo horizonte|brazil": "Other",
  "manaus|brazil": "Other",
  "vitoria|brazil": "Other",
  "vitória|brazil": "Other",
  // Argentina
  "buenos aires|argentina": "Buenos Aires",
  "mendoza|argentina": "Mendoza",
  "cordoba|argentina": "Córdoba",
  "córdoba|argentina": "Córdoba",
  "salta|argentina": "Other",
  "bariloche|argentina": "Bariloche",
  // Jamaica / Barbados — single-market countries (Market may equal Country)
  "montego bay|jamaica": "Jamaica",
  "negril|jamaica": "Jamaica",
  "kingston|jamaica": "Jamaica",
  "ocho rios|jamaica": "Jamaica",
  "bridgetown|barbados": "Barbados",
  ...CITY_TO_MARKET_VNEXT,
  ...CITY_TO_MARKET_VNEXT2,
});

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ");
}

/**
 * Island / compact geographies where Dealality may use Country string as the
 * sole commercial Market name (explicit taxonomy allowlist).
 */
export function isSingleMarketCountry(country) {
  const c = String(country || "").trim();
  const audit = SINGLE_MARKET_ALLOWLIST_AUDIT.find((a) => a.country === c);
  if (audit) return audit.keep === true;
  const cfg = COUNTRY_CONFIGS[c];
  if (!cfg) return false;
  const markets = (cfg.initialMarkets || []).filter((m) => m && m !== "Other");
  if (markets.length >= 2) return false;
  if (cfg.buildStrategy === BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE) return true;
  const ALLOW = new Set([
    "Barbados",
    "Jamaica",
    "Aruba",
    "Curaçao",
    "Curacao",
    "Cayman Islands",
    "Trinidad and Tobago",
    "Saint Lucia",
    "Belize",
    "Puerto Rico",
  ]);
  return ALLOW.has(c) && markets.length <= 1;
}

/**
 * Build versioned Dealality Market registry from COUNTRY_CONFIGS + city map.
 */
export function buildDealalityMarketRegistry() {
  /** @type {Map<string, object>} */
  const byId = new Map();

  function upsert(marketName, country, extra = {}) {
    const name = String(marketName || "").trim();
    if (!name || name === "Other") return;
    const id = `${norm(country)}::${norm(name)}`;
    if (byId.has(id)) {
      const prev = byId.get(id);
      byId.set(id, {
        ...prev,
        ...extra,
        aliases: [...new Set([...(prev.aliases || []), ...(extra.aliases || [])])],
        cities: [...new Set([...(prev.cities || []), ...(extra.cities || [])])],
      });
      return;
    }
    byId.set(id, {
      market_id: id,
      canonical_name: name,
      country,
      state_region_coverage: extra.state_region_coverage || [],
      cities: extra.cities || [],
      aliases: extra.aliases || [],
      coordinates_or_polygon: extra.coordinates_or_polygon || null,
      priority_rules: extra.priority_rules || ["city_map", "initial_markets", "single_market_country"],
      effective_version: DEALALITY_MARKET_REGISTRY_VERSION,
      single_market_country: Boolean(extra.single_market_country),
      country_as_market_allowed: Boolean(extra.country_as_market_allowed),
    });
  }

  for (const [country, cfg] of Object.entries(COUNTRY_CONFIGS)) {
    const single = isSingleMarketCountry(country);
    for (const m of cfg.initialMarkets || []) {
      upsert(m, country, {
        single_market_country: single,
        country_as_market_allowed: single && m === country,
      });
    }
    if (single) {
      upsert(country, country, {
        single_market_country: true,
        country_as_market_allowed: true,
        priority_rules: ["single_market_country"],
      });
    }
    for (const [market, subs] of Object.entries(cfg.marketSubmarkets || {})) {
      upsert(market, country, {
        submarkets: subs,
      });
    }
  }

  for (const extra of EXTRA_DEALALITY_MARKETS_VNEXT) {
    upsert(extra.canonical_name, extra.country, {
      cities: extra.cities || [],
      rationale: extra.rationale,
      vnext: true,
    });
  }
  for (const extra of EXTRA_DEALALITY_MARKETS_VNEXT2) {
    upsert(extra.canonical_name, extra.country, {
      cities: extra.cities || [],
      rationale: extra.rationale,
      state_region_coverage: extra.state_region_coverage || [],
      vnext2: true,
      city_may_equal_market: true,
    });
  }

  for (const [key, market] of Object.entries(CITY_TO_DEALALITY_MARKET)) {
    const [city, countryRaw] = key.split("|");
    const country = Object.keys(COUNTRY_CONFIGS).find((c) => norm(c) === countryRaw) || countryRaw;
    upsert(market, country, {
      cities: [city],
      aliases: [],
    });
  }

  return {
    version: DEALALITY_MARKET_REGISTRY_VERSION,
    markets: [...byId.values()],
    by_id: Object.fromEntries(byId),
  };
}

export function lookupMarketInRegistry(marketName, country) {
  const reg = buildDealalityMarketRegistry();
  const n = norm(marketName);
  const cn = norm(country);
  const hits = reg.markets.filter(
    (m) => norm(m.canonical_name) === n && (!cn || norm(m.country) === cn)
  );
  if (hits.length === 1) return hits[0];
  if (hits.length > 1) return { conflict: true, hits };
  // alias / fuzzy exact country match on name
  const aliasHit = reg.markets.find(
    (m) =>
      (!cn || norm(m.country) === cn) &&
      (norm(m.canonical_name) === n || (m.aliases || []).some((a) => norm(a) === n))
  );
  return aliasHit || null;
}

function isWeakCityLabel(cityRaw) {
  const c = String(cityRaw || "").trim();
  if (!c) return true;
  if (/^unknown$/i.test(c)) return true;
  if (/\d{4,}/.test(c)) return true; // postal / CEP as city
  if (/^\d/.test(c)) return true;
  return false;
}

function haversineKm(lat1, lng1, lat2, lng2) {
  const toRad = (d) => (Number(d) * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/**
 * Resolve Dealality Market WITHOUT country fallback.
 * Returns null when unresolved (caller must leave blank / UNRESOLVED — never Country).
 *
 * @param {string} country
 * @param {string} [city]
 * @param {{ city?: string, state?: string, latitude?: number, longitude?: number }} [opts]
 */
export function resolveDealalityMarketStrict(country, city, opts = {}) {
  const c = String(country || "").trim();
  const cityRaw = String(city || opts.city || "").trim();
  const stateRaw = String(opts.state || "").trim();
  if (!c) return { market: null, method: null, ok: false, reason: "missing_country" };

  const cityKey = `${norm(cityRaw)}|${norm(c)}`;
  const cityKeyAccent = `${String(cityRaw).trim().toLowerCase()}|${c.toLowerCase()}`;
  const fromMap =
    CITY_TO_DEALALITY_MARKET[cityKeyAccent] ||
    CITY_TO_DEALALITY_MARKET[cityKey] ||
    CITY_TO_MARKET_VNEXT[cityKeyAccent] ||
    CITY_TO_MARKET_VNEXT[cityKey] ||
    CITY_TO_MARKET_VNEXT2[cityKeyAccent] ||
    CITY_TO_MARKET_VNEXT2[cityKey] ||
    null;
  if (fromMap && fromMap !== "Other") {
    return {
      market: fromMap,
      method: "city_to_dealality_market",
      ok: true,
      reason: null,
      confidence: "High",
      // Explicit registry: City string may equal Market when mapped to itself
      city_equals_market_via_explicit_registry: norm(fromMap) === norm(cityRaw),
    };
  }

  // Alias: production city short label via alias table
  const aliasHit = MARKET_ALIASES_TO_CANONICAL[norm(cityRaw)];
  if (aliasHit && !isWeakCityLabel(cityRaw)) {
    return {
      market: aliasHit,
      method: "market_alias_from_city",
      ok: true,
      reason: null,
      confidence: "High",
    };
  }

  // Extra vNext / vNext2 markets by city membership (exact)
  const cityN = norm(cityRaw);
  if (cityN && !isWeakCityLabel(cityRaw)) {
    for (const extra of EXTRA_DEALALITY_MARKETS_VNEXT2) {
      if (norm(extra.country) !== norm(c)) continue;
      if ((extra.cities || []).some((x) => cityN === norm(x))) {
        return {
          market: extra.canonical_name,
          method: "vnext2_explicit_city_market",
          ok: true,
          reason: null,
          confidence: "High",
          city_equals_market_via_explicit_registry: cityN === norm(extra.canonical_name),
        };
      }
    }
    for (const extra of EXTRA_DEALALITY_MARKETS_VNEXT) {
      if (norm(extra.country) !== norm(c)) continue;
      if ((extra.cities || []).some((x) => cityN === norm(x))) {
        return {
          market: extra.canonical_name,
          method: "vnext_secondary_market",
          ok: true,
          reason: null,
          confidence: "High",
        };
      }
    }
  }

  // Explicit State→Market (only when city is weak/missing)
  if (isWeakCityLabel(cityRaw) && stateRaw) {
    const sk = `${norm(stateRaw)}|${norm(c)}`;
    const stateMarket = STATE_TO_MARKET_EXPLICIT[sk];
    if (stateMarket) {
      return {
        market: stateMarket,
        method: "explicit_state_to_market_registry",
        ok: true,
        reason: null,
        confidence: "Medium",
      };
    }
  }

  // Coordinate-first Market centroid (only when city is weak/missing)
  const lat = opts.latitude != null ? Number(opts.latitude) : null;
  const lng = opts.longitude != null ? Number(opts.longitude) : null;
  if (isWeakCityLabel(cityRaw) && lat != null && lng != null && !Number.isNaN(lat) && !Number.isNaN(lng)) {
    let best = null;
    for (const cen of MARKET_CENTROIDS_VNEXT) {
      if (norm(cen.country) !== norm(c)) continue;
      const d = haversineKm(lat, lng, cen.lat, cen.lng);
      if (d <= cen.max_km && (!best || d < best.d)) {
        best = { market: cen.market, d, max_km: cen.max_km };
      }
    }
    if (best) {
      return {
        market: best.market,
        method: "coordinate_market_centroid",
        ok: true,
        reason: null,
        confidence: best.d <= best.max_km * 0.5 ? "High" : "Medium",
        distance_km: Math.round(best.d * 10) / 10,
      };
    }
  }

  if (isSingleMarketCountry(c)) {
    return {
      market: c,
      method: "single_market_country_taxonomy",
      ok: true,
      reason: null,
      country_as_market_allowed: true,
      confidence: "High",
    };
  }

  const reg = buildDealalityMarketRegistry();
  if (cityN && !isWeakCityLabel(cityRaw)) {
    const hit = reg.markets.find(
      (m) =>
        norm(m.country) === norm(c) &&
        (m.cities || []).some((x) => cityN === norm(x))
    );
    if (hit) {
      return {
        market: hit.canonical_name,
        method: "registry_city_coverage",
        ok: true,
        reason: null,
        confidence: "Medium",
      };
    }
  }

  return {
    market: null,
    method: null,
    ok: false,
    reason: "unresolved_no_country_fallback",
    confidence: null,
  };
}

/**
 * Classify a production Market value.
 */
export function classifyProductionMarket({ country, market, city, state }) {
  const m = String(market || "").trim();
  const c = String(country || "").trim();
  if (!m) return { class: MARKET_CLASS.UNRESOLVED, ok: false };

  // Normalize aliases first
  const aliased = MARKET_ALIASES_TO_CANONICAL[norm(m)] || m;

  if (c && norm(m) === norm(c)) {
    if (isSingleMarketCountry(c)) {
      return {
        class: MARKET_CLASS.VALID_MARKET,
        ok: true,
        note: "single_market_country_explicit",
        canonical: c,
      };
    }
    return {
      class: MARKET_CLASS.COUNTRY_AS_MARKET,
      ok: false,
      note: "country_used_as_market_without_taxonomy_allowlist",
    };
  }

  const st = String(state || "").trim();
  if (st && norm(m) === norm(st)) {
    // State equals Market only if State is registered as Market (rare — not auto)
    const regState = lookupMarketInRegistry(m, c);
    if (regState && !regState.conflict) {
      return { class: MARKET_CLASS.VALID_MARKET, ok: true, note: "state_registered_as_market", canonical: aliased };
    }
    return { class: MARKET_CLASS.STATE_AS_MARKET, ok: false };
  }

  const cit = String(city || "").trim();
  const reg = lookupMarketInRegistry(aliased, c);
  if (reg?.conflict) return { class: MARKET_CLASS.CONFLICT, ok: false, registry: reg };
  if (reg && !reg.conflict) {
    return { class: MARKET_CLASS.VALID_MARKET, ok: true, registry: reg, canonical: aliased };
  }

  // Extra vNext markets
  const extraHit = EXTRA_DEALALITY_MARKETS_VNEXT.find(
    (e) => norm(e.country) === norm(c) && norm(e.canonical_name) === norm(aliased)
  );
  if (extraHit) {
    return { class: MARKET_CLASS.VALID_MARKET, ok: true, note: "vnext_market", canonical: aliased };
  }

  const mapValues = new Set([
    ...Object.values(CITY_TO_DEALALITY_MARKET),
    ...Object.values(CITY_TO_MARKET_VNEXT),
    ...Object.values(MARKET_ALIASES_TO_CANONICAL),
  ]);
  if (mapValues.has(aliased) || mapValues.has(m)) {
    return { class: MARKET_CLASS.VALID_MARKET, ok: true, note: "city_map_canonical", canonical: aliased };
  }

  if (cit && norm(m) === norm(cit)) {
    // City dump: if city maps to a market, this production value is CITY_AS_MARKET aliasable
    const strict = resolveDealalityMarketStrict(c, cit);
    if (strict.ok && norm(strict.market) !== norm(m)) {
      return {
        class: MARKET_CLASS.CITY_AS_MARKET,
        ok: false,
        note: "city_label_should_map_to_canonical",
        suggested: strict.market,
      };
    }
    if (strict.ok && norm(strict.market) === norm(m)) {
      return { class: MARKET_CLASS.VALID_MARKET, ok: true, note: "city_is_canonical_market", canonical: m };
    }
    return { class: MARKET_CLASS.CITY_AS_MARKET, ok: false };
  }

  return { class: MARKET_CLASS.INVALID_MARKET, ok: false, note: "not_in_registry" };
}

/**
 * Semantic gate before Market write.
 */
export function assertMarketWriteGate({ country, market, city, state, latitude, longitude }) {
  const failures = [];
  const m = String(market || "").trim();
  if (!m) {
    return { pass: false, failures: ["market_blank"], write_allowed: false };
  }
  const cls = classifyProductionMarket({ country, market: m, city, state });
  if (cls.class === MARKET_CLASS.COUNTRY_AS_MARKET) {
    failures.push("COUNTRY_AS_MARKET_FORBIDDEN");
  }
  if (cls.class === MARKET_CLASS.STATE_AS_MARKET) {
    failures.push("STATE_AS_MARKET_FORBIDDEN");
  }
  if (cls.class === MARKET_CLASS.INVALID_MARKET) {
    failures.push("MARKET_NOT_IN_REGISTRY");
  }
  if (cls.class === MARKET_CLASS.CONFLICT) {
    failures.push("MARKET_REGISTRY_CONFLICT");
  }
  const reg = lookupMarketInRegistry(m, country);
  if (reg && !reg.conflict && country && norm(reg.country) !== norm(country)) {
    failures.push("MARKET_COUNTRY_MISMATCH");
  }
  // Coherence: if city maps to a different market, conflict
  const strict = resolveDealalityMarketStrict(country, city);
  if (strict.ok && strict.market && norm(strict.market) !== norm(m)) {
    failures.push("CITY_MARKET_INCOHERENT");
  }

  return {
    pass: failures.length === 0 && cls.ok,
    failures,
    classification: cls.class,
    write_allowed: failures.length === 0 && cls.ok,
    coords_present: latitude != null && longitude != null,
  };
}

/**
 * Submarket gate: requires valid Market first.
 */
export function assertSubmarketWriteGate({ country, market, submarket, status }) {
  const mClass = classifyProductionMarket({ country, market });
  if (!mClass.ok) {
    return {
      pass: false,
      write_allowed: false,
      forced_status: "UNRESOLVED",
      failures: ["SUBMARKET_REQUIRES_VALID_MARKET"],
    };
  }
  if (status === "NOT_APPLICABLE") {
    return {
      pass: true,
      write_allowed: false,
      status: "NOT_APPLICABLE",
      note: "do_not_manufacture_submarket",
    };
  }
  if (status === "MATCHED") {
    if (!String(submarket || "").trim()) {
      return { pass: false, write_allowed: false, failures: ["MATCHED_REQUIRES_VALUE"] };
    }
    return { pass: true, write_allowed: true, status: "MATCHED" };
  }
  return {
    pass: true,
    write_allowed: false,
    status: "UNRESOLVED",
    note: "queue_research",
  };
}
