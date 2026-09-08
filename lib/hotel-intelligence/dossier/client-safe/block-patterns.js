/**
 * Client-facing Full HI report — forbidden implementation-leak patterns.
 * Targets internal research machinery, not ordinary English (e.g. market "census").
 */

export const CLIENT_SAFE_BLOCK_PATTERNS = Object.freeze([
  { id: "provider_webhound", re: /\bwebhound(?:-mcp)?\b/i },
  { id: "provider_budget_dollar", re: /\$\s*5(?:\.00)?\b.*\b(?:budget|cost|provider)/i },
  { id: "provider_budget_phrase", re: /\b(?:provider|research)\s+budget\b|\bbudget\s+\$\s*\d/i },
  { id: "actual_cost", re: /\bactual\s+cost\b/i },
  { id: "provider_run", re: /\bprovider\s+run\b|\bresearch\s+session\b|\bresearch\s+cycle\b/i },
  { id: "session_uuid", re: /\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/i },
  { id: "canonical_promotion", re: /\b(?:automatically\s+promoted|canonical\s+Hotel\s+Intelligence|auto[_ -]?promote)\b/i },
  { id: "location_pending", re: /\bLocation\s+pending\b/i },
  { id: "raw_sources_meta", re: /\braw\s+sources\b|\bnormalized\s+bibliography\b|\bdeduped\/rejected\b/i },
  { id: "canonical_dhl", re: /\bdhl_[a-z0-9]+\b/i },
  { id: "airtable_rec", re: /\brec[A-Za-z0-9]{14}\b/ },
  { id: "airtable_word_impl", re: /\bAirtable(?:\/Census)?\b/ },
  { id: "census_radar_rec", re: /\bCensus\/Radar\b|\bRadar\/Census\b/i },
  // Product "Census" (capital C) is Dealality infrastructure — never customer-facing.
  { id: "census_product", re: /\bCensus\b/ },
  { id: "research_center_product", re: /\bResearch Center\b/ },
  { id: "dealality_infra_phrase", re: /\bDealality deep-research\b|\bauto[_ -]?promote\b|\bclaim_handoff\b|\bFULL_HI_REQUIRED\b|\bPacket\s+2\.\d+/i },
  { id: "known_facts", re: /\bKnown Facts\b|\buser'?s?\s+directive\b|\bfounder\s+directive\b/i },
  { id: "do_not_invent", re: /\bdo\s+not\s+invent\b|\bdo\s+NOT\s+auto-promote\b/i },
  { id: "mcp_api", re: /\bwebhound-mcp\b|\bMCP\b|\bAPI\s+payload\b|\bSIMULATION\b/ },
  { id: "debug_fixture", re: /\bgolden\s+demo\b|\bfixture\b|\bdebug\b|\bresearch\s+module\b|\braw\s+artifact\b/i },
  { id: "ontology_codes", re: /\b(?:SPONSORED_BY|OWNED_VIA_CBHL_PROBABLE|OPERATED_STATUS_CONTESTED|OPERATED_BY_PYRAMID_2023|COMMERCIAL_PARTNER_CHARLESTOWNE|OWNED_BY|OPERATED_BY|DEVELOPED_BY|OWNED_BY_ALLIANCE|OPERATED_BY_AIMBRIDGE|SIBLING_CONVERSION_PORTFOLIO)\b/ },
  { id: "raw_markdown_bold", re: /\*\*[^*]+\*\*/ },
  { id: "generic_source_n", re: /\bSource[ \t]+\d+\b(?![ \t]+(?:basis|quality|hierarchy))/ },
]);

/** Surfaces that may retain internal metadata (never rendered as customer prose). */
export const INTERNAL_ONLY_DOSSIER_KEYS = Object.freeze([
  "research_provider",
  "research_run_id",
  "research_cost_usd",
  "raw_artifact_reference",
  "claim_handoff",
  "mapping_notes",
  "source_accounting",
  "claim_accounting",
  "hotel_airtable_record_id",
  "hotel_id",
  "canonical_hotel_id",
  "internal_research_gaps",
  "finding_id",
  "source_ids",
  "claim_ids",
  "claim_ids_used",
  "sections_used",
  "theme",
  "_internal",
]);
