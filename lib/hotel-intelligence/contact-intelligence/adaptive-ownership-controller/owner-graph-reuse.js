/**
 * Owner-graph reuse after a candidate PASSES verification.
 * Hotel→owner relationship remains a separate claim from owner entity intelligence.
 */

import {
  getOwnerPortfolioProfile,
  getOwnerControlGraph,
  resolveOwnerEntityId,
  resolveOwnerForHotel,
} from "../../ownership/owner-control/portfolio-store.js";

/**
 * Lookup Dealality owner/entity intelligence for a verified candidate.
 * Does NOT prove hotel→owner; returns reusable owner-centric intelligence only.
 *
 * @param {{ hotel_id?: string, entity_name?: string, owner_entity_id?: string }} opts
 */
export function lookupOwnerGraphIntelligence(opts = {}) {
  const hotelId = opts.hotel_id || null;
  const entityName = String(opts.entity_name || "").trim();
  const explicitId = opts.owner_entity_id || null;

  let byHotel = null;
  try {
    byHotel = hotelId ? resolveOwnerForHotel(hotelId) : null;
  } catch {
    byHotel = null;
  }

  let ownerEntityId =
    explicitId ||
    byHotel?.owner_entity_id ||
    (entityName ? resolveOwnerEntityId(entityName) : null) ||
    null;

  // Avoid treating raw hotel names as entity ids
  if (ownerEntityId && ownerEntityId === entityName && !/^dle_|^ent_/.test(ownerEntityId)) {
    ownerEntityId = byHotel?.owner_entity_id || null;
  }

  let profile = null;
  let graph = null;
  if (ownerEntityId) {
    try {
      profile = getOwnerPortfolioProfile(ownerEntityId);
      graph = getOwnerControlGraph(ownerEntityId);
    } catch {
      profile = null;
      graph = null;
    }
  }

  const reused = Boolean(profile);
  return {
    owner_graph_reuse: reused,
    owner_entity_id: profile?.owner_entity_id || ownerEntityId || null,
    profile: profile
      ? {
          owner_entity_id: profile.owner_entity_id,
          display_name: profile.display_name || profile.name || null,
          aliases: profile.aliases || [],
          official_domain: profile.official_domain || profile.domain || null,
          last_verified_at: profile.last_verified_at || profile.verified_at || null,
          hotels_known: profile.hotels?.length ?? profile.hotel_ids?.length ?? null,
        }
      : null,
    graph_summary: graph
      ? {
          node_count: graph.nodes?.length ?? Object.keys(graph.nodes || {}).length ?? null,
          edge_count: graph.edges?.length ?? Object.keys(graph.edges || {}).length ?? null,
        }
      : null,
    /**
     * CRITICAL: owner entity intelligence ≠ hotel→owner relationship proof.
     */
    hotel_owner_relationship_proven_by_graph: false,
    hotel_owner_relationship_requires_independent_evidence: true,
    new_owner_research_required: !reused,
    looked_up_at: new Date().toISOString(),
  };
}

/**
 * Should we skip fresh person/contact research because owner graph already has verified contacts?
 * Contact enrichment stays OFF in this experiment — this only signals reuse opportunity.
 */
export function shouldPreferOwnerGraphBeforeFreshContactResearch(lookup) {
  if (!lookup?.owner_graph_reuse) return false;
  return true;
}
