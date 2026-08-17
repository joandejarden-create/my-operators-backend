/**
 * MVP Brand Explorer / Brand Basics display names → census alias canonical names.
 * Used before Brand Alias Mapping lookup when the live Airtable name differs from
 * the canonical row in the alias table (common for Choice Radisson-family brands).
 */
import { AIRTABLE_NAME_TO_ARCH_KEY } from "../choice-brand-architecture-oct2025.js";

/** @type {Record<string, string>} */
const MVP_TO_CENSUS_CANONICAL = {
  ...AIRTABLE_NAME_TO_ARCH_KEY,
  "Country Inn & Suites by Choice": "Country Inn & Suites by Radisson (Choice)",
  "Radisson Individuals by Choice": "Radisson Individual (Choice)",
  "Park Plaza by Choice": "Park Plaza (Choice)",
};

/**
 * @param {string} brandName Brand Explorer / API requested name
 * @returns {string} Name to use for alias-table resolution (unchanged when no map)
 */
export function normalizeBrandNameForCensusAliasLookup(brandName) {
  const requested = String(brandName == null ? "" : brandName).trim();
  if (!requested) return requested;
  return MVP_TO_CENSUS_CANONICAL[requested] || requested;
}
