/**
 * Submarket applicability + Golden Phone conditional rules (formal V3.0.3).
 */

import { getSubmarketOptionsForCountry } from "../../../radar-submarket.js";
import { resolveCensusCountryKey } from "../../../hotel-census/census-str-submarket-corridors.js";

/** Markets where Dealality treats Market as terminal geography (no corridor required). */
export const MARKET_TERMINAL_DEFAULTS = Object.freeze([
  "Brazil",
  "Argentina",
  "Barbados",
  "Jamaica",
  "Colombia",
  "Chile",
  "Peru",
  "Ecuador",
  "Uruguay",
  "Paraguay",
  "Bolivia",
  "Panama",
  "Guatemala",
  "Honduras",
  "Nicaragua",
  "El Salvador",
  "Belize",
]);

/**
 * Markets with meaningful corridor structure in Dealality taxonomy.
 */
export function marketHasCorridorStructure(country, market) {
  const m = String(market || "").trim();
  const c = String(country || "").trim();
  if (!m && !c) return false;

  const key = resolveCensusCountryKey(c || m);
  const opts = getSubmarketOptionsForCountry(key).filter((o) => o && o !== "Other");

  // Explicit multi-corridor commercial markets
  const CORRIDOR_MARKETS = new Set([
    "Cancún / Riviera Maya",
    "Los Cabos",
    "Puerto Vallarta",
    "Mexico City",
    "Guadalajara",
    "Monterrey",
    "Punta Cana / East Coast",
    "Greater Santo Domingo",
    "San José / Central Valley",
    "Bogotá",
    "Cartagena",
    "Lima",
    "Cusco / Sacred Valley",
    "Buenos Aires",
    "São Paulo",
    "Rio de Janeiro",
    "Santiago",
    "Panama City",
    "Costa Rica",
    "Jamaica",
    "Barbados",
    "Dominican Republic",
  ]);
  if (CORRIDOR_MARKETS.has(m)) return opts.length >= 2 || CORRIDOR_MARKETS.has(m);

  // Country-level market: corridors exist in registry → applicable (resolve via city)
  if (m === c) {
    // Large countries with metro markets should use city→market first; if still country-level,
    // treat as NOT_APPLICABLE only when no useful corridor can be selected from city.
    return opts.length >= 4;
  }

  return opts.length >= 3;
}

/**
 * @returns {"REQUIRED"|"NOT_APPLICABLE"|"UNKNOWN"}
 */
export function classifySubmarketApplicability({ country, market, submarket, submarketConfidence }) {
  const hasCorridor = marketHasCorridorStructure(country, market);
  if (!hasCorridor) return "NOT_APPLICABLE";
  if (submarket && submarketConfidence && submarketConfidence !== "No Match") return "REQUIRED";
  if (hasCorridor && (!submarket || submarketConfidence === "No Match")) return "UNKNOWN";
  return "REQUIRED";
}

/**
 * Phone applicability for Golden completeness.
 * @returns {"REQUIRED"|"NOT_APPLICABLE"|"UNKNOWN"}
 */
export function classifyPhoneApplicability({ phone, phoneType, researchedExhaustively = false }) {
  const type = String(phoneType || "");
  if (type === "PROPERTY_DIRECT" && phone) return "REQUIRED";
  if (type === "CENTRAL_RESERVATIONS" || type === "SALES") {
    return researchedExhaustively ? "NOT_APPLICABLE" : "UNKNOWN";
  }
  if (!phone && researchedExhaustively) return "NOT_APPLICABLE";
  if (!phone) return "UNKNOWN";
  return "REQUIRED";
}

export const GOLDEN_SCHEMA_VNEXT = Object.freeze({
  version: "golden-census-schema-v3.0.3",
  effective_date: "2026-08-08",
  supersedes: "golden-to-airtable-field-map-v3",
  notes:
    "Formalizes Phone CONDITIONAL REQUIRED and Submarket APPLICABILITY-BASED. Does not silently rewrite historical V3.0.2A artifacts.",
  fields: {
    Phone: {
      golden_requirement: "CONDITIONAL_REQUIRED",
      required_when: "property_direct_phone_exists_or_reasonably_expected",
      not_applicable_when: ["CENTRAL_RESERVATIONS_only", "SALES_only", "no_public_property_direct_after_research"],
      do_not_force_central_into_property_direct: true,
    },
    Submarket: {
      golden_requirement: "APPLICABILITY_BASED",
      required_when: "market_has_meaningful_dealality_corridor_structure",
      not_applicable_when: "market_is_terminal_geography",
      unknown_when: "corridor_expected_but_unresolved",
      not_applicable_excluded_from_denominator: true,
      no_artificial_submarkets: true,
    },
    "State / Region": {
      golden_requirement: "REQUIRED",
      derivation: "dealality_geography_deterministic",
      administrative_type_tracked_separately: true,
    },
    Market: {
      golden_requirement: "REQUIRED",
      derivation: "dealality_geography_deterministic",
    },
  },
});
