/**
 * Verified Independent Hotel Census — staging data model (local artifacts only).
 * Not a production SoT until governed migration.
 */

import { CLEAN_CENSUS_RECORD_STATUSES, RESEARCH_MODES_CLEAN } from "./provenance.js";

export const VIC_ENGINE_VERSION = "verified-independent-census-v1";
export const VIC_CONFIG_VERSION = "vic-wave-config-v1";

/**
 * @param {object} partial
 */
export function createVerifiedIndependentRecord(partial = {}) {
  const now = new Date().toISOString();
  return {
    independent_record_id: partial.independent_record_id,
    // Identity
    canonical_hotel_name: partial.canonical_hotel_name || partial.fields?.name || null,
    official_property_ids: partial.official_property_ids || [],
    official_property_url: partial.official_property_url || partial.fields?.Website || null,
    brand: partial.brand || partial.fields?.Affiliation || null,
    parent: partial.parent || partial.fields?.["Parent Company"] || null,
    location_text: partial.location_text || null,
    country: partial.country || partial.fields?.country || null,
    normalized_city: partial.normalized_city || partial.fields?.city || null,
    aliases: partial.aliases || [],
    current_status: partial.current_status || partial.fields?.status || null,
    // Provenance
    discovery_source: partial.discovery_source || null,
    discovery_source_type: partial.discovery_source_type || null,
    first_independently_discovered_at: partial.first_independently_discovered_at || now,
    independent_research_timestamp: partial.independent_research_timestamp || now,
    freeze_timestamp: partial.freeze_timestamp || null,
    freeze_hash: partial.freeze_hash || null,
    field_claims: partial.field_claims || partial.claims || [],
    reconstruction_wave: partial.reconstruction_wave || null,
    engine_version: partial.engine_version || VIC_ENGINE_VERSION,
    legacy_used_as_source: false,
    // State
    reconstruction_state: partial.reconstruction_state || "Independent — Deep Research Required",
    research_mode: partial.research_mode || RESEARCH_MODES_CLEAN.CLEAN_CENSUS_RECONSTRUCTION,
    // Staging fields map (census schema names)
    fields: partial.fields || {},
    completeness: partial.completeness || null,
    page_source_state: partial.page_source_state || null,
    image_rights_status: partial.image_rights_status || "Unknown Rights",
    production_eligibility_data: partial.production_eligibility_data || null,
    production_eligibility_images: partial.production_eligibility_images || null,
    first_party_validated: Boolean(partial.first_party_validated),
  };
}

export { CLEAN_CENSUS_RECORD_STATUSES };
