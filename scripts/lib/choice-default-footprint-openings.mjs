/**
 * Default footprint.openings — real CALA properties from census + curated cards.
 */
import { buildCalaOpeningsForProfile } from "./choice-cala-openings-from-census.mjs";

/**
 * @param {import('./choice-tier1-explorer-profiles.mjs').Tier1Profile} p
 * @returns {import('./choice-tier1-explorer-profiles.mjs').FootprintOpeningCard[]}
 */
export function defaultFootprintOpeningsForBrand(p) {
  const cala = buildCalaOpeningsForProfile(p.name);
  if (cala.length) return cala;
  if (p.footprintOpenings?.length) return p.footprintOpenings;
  return [];
}
