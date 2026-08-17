/**
 * Dealality Submarket map — High token / city rules only. Never invent.
 */

import {
  SUBMARKET_HIGH_RULES,
  resolveSubmarketHighOnly as resolveSubmarketHighOnlyCore,
  normGeoKey,
} from "./census-region-market-map.js";

export const SUBMARKET_MAP_VERSION = "census-submarket-map-v1";

/** Extra High Submarket rules for commercial-fields mission. */
export const COMMERCIAL_SUBMARKET_RULES = Object.freeze([
  ...SUBMARKET_HIGH_RULES,
  {
    market: "Cancún",
    submarket: "Hotel Zone",
    tokens: ["zona hotelera", "hotel zone", "blvd kukulcan", "boulevard kukulcan", "kukulkan"],
  },
  {
    market: "Cancún",
    submarket: "Playa del Carmen Centro",
    tokens: ["playa del carmen centro", "5th avenue", "quinta avenida", "av 5"],
    requireCity: ["playa del carmen"],
  },
  {
    market: "Bogotá",
    submarket: "Chapinero",
    tokens: ["chapinero"],
  },
  {
    market: "Bogotá",
    submarket: "Zona T",
    tokens: ["zona t", "chicó", "chico", "zona rosa"],
  },
  {
    market: "Panama City",
    submarket: "Financial District",
    tokens: ["financial district", "area bancaria", "área bancaria", "costa del este", "calle 50"],
  },
  {
    market: "Santo Domingo",
    submarket: "Piantini",
    tokens: ["piantini"],
  },
  {
    market: "Santo Domingo",
    submarket: "Naco",
    tokens: ["naco"],
  },
  {
    market: "Santo Domingo",
    submarket: "Colonial Zone",
    tokens: ["zona colonial", "colonial zone", "ciudad colonial"],
  },
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
      "walled city",
      "factoria",
      "factoría",
    ],
  },
  {
    market: "Los Cabos",
    submarket: "Cabo San Lucas",
    tokens: ["cabo san lucas"],
    requireCity: ["cabo san lucas"],
  },
  {
    market: "Mexico City",
    submarket: "Pedregal",
    tokens: ["pedregal", "parques del pedregal", "cuspide", "cúspide"],
  },
  {
    market: "Panama City",
    submarket: "Amador",
    tokens: ["amador", "van hook", "causeway"],
    requireCity: ["amador", "panama city", "panama"],
  },
  {
    market: "Panama City",
    submarket: "Chame / Coronado",
    tokens: ["chame", "coronado", "playa caracol"],
  },
]);

/**
 * High-only Submarket resolution with commercial rule expansions.
 */
export function resolveCommercialSubmarket(input = {}) {
  const market = String(input.market || "").trim();
  if (!market) return { ok: false, reason: "missing_market", backlog: true };

  const cityKey = normGeoKey(input.city);
  const hay = normGeoKey(
    [input.city, input.address, input.propertyName, input.sourceText]
      .filter(Boolean)
      .join(" ")
  );
  if (!hay) return { ok: false, reason: "no_submarket_source_text", backlog: true };

  const marketKey = normGeoKey(market);
  for (const rule of COMMERCIAL_SUBMARKET_RULES) {
    if (normGeoKey(rule.market) !== marketKey) continue;
    if (rule.requireCity?.length) {
      const okCity = rule.requireCity.some((c) => cityKey === normGeoKey(c));
      if (!okCity) continue;
    }
    if (rule.tokens.some((t) => hay.includes(normGeoKey(t)))) {
      return {
        ok: true,
        submarket: rule.submarket,
        confidence: "High",
        method: "commercial_submarket_token_map",
      };
    }
  }

  const core = resolveSubmarketHighOnlyCore(input);
  if (core.ok) return core;

  return {
    ok: false,
    reason: core.reason || "submarket_mapping_backlog",
    backlog: true,
  };
}

export { SUBMARKET_HIGH_RULES, normGeoKey };
