/**
 * Separate SOURCE-LANGUAGE extraction from CANONICAL relationship semantics.
 * Ambiguous language must not be forced into OWNED_BY.
 */

import { RELATIONSHIP_TYPES } from "../ontology.js";

export const SOURCE_LANGUAGE_MAP_VERSION = "claim-source-language-v1";

/** Source-language tokens → careful canonical candidate (or null if ambiguous). */
export const SOURCE_LANGUAGE_TO_CANDIDATE = Object.freeze({
  owner: "OWNED_BY",
  owns: "OWNED_BY",
  owned: "OWNED_BY",
  ownership: "OWNED_BY",
  propietario: "OWNED_BY",
  propietaria: "OWNED_BY",
  propiedad: "OWNED_BY",
  dueño: "OWNED_BY",
  subsidiary: "CONTROLLED_BY",
  subsidiaria: "CONTROLLED_BY",
  parent: "CONTROLLED_BY",
  control: "CONTROLLED_BY",
  controlled: "CONTROLLED_BY",
  affiliate: null, // ambiguous — observation only
  afiliada: null,
  participation: null,
  participación: null,
  interest: null,
  partner: null,
  socio: null,
  jv: "JV_WITH",
  "joint venture": "JV_WITH",
  developer: "DEVELOPED_BY",
  desarrollador: "DEVELOPED_BY",
  desarrolló: "DEVELOPED_BY",
  sponsor: "SPONSORED_BY",
  operator: "OPERATED_BY",
  operated: "OPERATED_BY",
  manager: "OPERATED_BY",
  managed: "OPERATED_BY",
  operado: "OPERATED_BY",
  administrado: "OPERATED_BY",
  lessee: "LEASED_FROM",
  leased: "LEASED_FROM",
  arrendatario: "LEASED_FROM",
  brand: "BRANDED_BY",
  branded: "BRANDED_BY",
  franchise: "BRANDED_BY",
});

/**
 * Map claim type → canonical relationship candidate type (or null).
 * @param {string} claimType
 */
export function claimTypeToRelationshipCandidate(claimType) {
  const map = {
    ENTITY_IS_OWNER_OF_PROPERTY: "OWNED_BY",
    ENTITY_CONTROLS_ENTITY: "CONTROLLED_BY",
    ENTITY_OWNS_PERCENT_OF_ENTITY: "JV_WITH",
    ENTITY_OPERATES_HOTEL: "OPERATED_BY",
    ENTITY_MANAGES_HOTEL: "OPERATED_BY",
    ENTITY_DEVELOPED_PROPERTY: "DEVELOPED_BY",
    ENTITY_SPONSORED_PROJECT: "SPONSORED_BY",
    ENTITY_PARTICIPATES_IN_JV: "JV_WITH",
    ENTITY_IS_PARENT_OF_ENTITY: "CONTROLLED_BY",
    ENTITY_IS_SUBSIDIARY_OF_ENTITY: "CONTROLLED_BY",
    ENTITY_LEASES_PROPERTY: "LEASED_FROM",
    ENTITY_BRANDS_HOTEL: "BRANDED_BY",
    HOTEL_CURRENT_BRAND: "BRANDED_BY",
    HOTEL_FORMER_BRAND: "BRANDED_BY",
    HOTEL_ANNOUNCED_BRAND: "BRANDED_BY",
    ENTITY_ACQUIRED_ENTITY_OR_ASSET: "OWNED_BY",
    ENTITY_SOLD_ENTITY_OR_ASSET: "OWNED_BY",
    ENTITY_FINANCED_PROPERTY: "FINANCED_BY",
    ENTITY_HOLDS_TITLE: "OWNED_BY",
  };
  const rel = map[claimType] || null;
  if (rel && !RELATIONSHIP_TYPES.includes(rel)) return null;
  return rel;
}

/**
 * @param {string} predicateRaw
 * @returns {{ candidate: string|null, ambiguous: boolean, reason: string }}
 */
export function mapSourceLanguageToCandidate(predicateRaw) {
  const p = String(predicateRaw || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim();
  if (!p) return { candidate: null, ambiguous: true, reason: "empty_predicate" };

  for (const [token, candidate] of Object.entries(SOURCE_LANGUAGE_TO_CANDIDATE)) {
    if (p.includes(token)) {
      if (candidate == null) {
        return {
          candidate: null,
          ambiguous: true,
          reason: `ambiguous_source_language:${token}`,
        };
      }
      return {
        candidate,
        ambiguous: false,
        reason: `mapped:${token}->${candidate}`,
      };
    }
  }
  return { candidate: null, ambiguous: true, reason: "unmapped_predicate" };
}
