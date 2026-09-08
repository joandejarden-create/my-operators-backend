/**
 * Packet 2.8B-2 — Portfolio factory: graph + attribution → OwnerPortfolioProfile.
 */

import { classifyPortfolioBucket, mayImportHotelsFromRelatedEntity } from "./attribution-policy.js";
import { assessPortfolioCompleteness } from "./completeness.js";
import { listRelatedEntities } from "./control-graph.js";
import { createTraversalBudget } from "./traversal-policy.js";
import { PORTFOLIO_BUCKETS } from "./vocabulary.js";

export const OWNER_PORTFOLIO_FACTORY_VERSION = "owner-portfolio-factory-v1";

/**
 * Build durable OwnerPortfolioProfile from an OwnerControlGraph + hotel relationship rows.
 * @param {{
 *   graph: object,
 *   hotel_relationships: Array<object>,
 *   people?: Array<object>,
 *   sources?: Array<object>,
 *   open_questions?: Array<object|string>,
 *   meta?: object,
 * }} input
 */
export function buildOwnerPortfolioProfile(input = {}) {
  const graph = input.graph;
  if (!graph?.owner_entity_id) {
    throw new Error("owner_graph_required");
  }
  const budget = createTraversalBudget();
  const ownerId = graph.owner_entity_id;
  const ownerNode = (graph.nodes || []).find((n) => n.entity_id === ownerId);

  const related = listRelatedEntities(graph, { purpose: "related_entities" });

  // Guard: never fan-out JV partner portfolios into hotel_relationships input
  const safeRels = [];
  for (const rel of input.hotel_relationships || []) {
    if (rel.import_from_related_entity) {
      const gate = mayImportHotelsFromRelatedEntity(rel.import_from_related_entity);
      if (!gate.ok) {
        continue;
      }
    }
    const classified = classifyPortfolioBucket(rel, { owner_entity_id: ownerId });
    const row = {
      hotel_id: rel.hotel_id || null,
      airtable_record_id: rel.airtable_record_id || null,
      hotel_name: rel.hotel_name || rel.name,
      owner_entity_id: ownerId,
      relationship_type: rel.relationship_type,
      secondary_relationship_type: rel.secondary_relationship_type || null,
      attribution_path: Array.isArray(rel.attribution_path) ? rel.attribution_path : [],
      ownership_percentage: rel.ownership_percentage ?? null,
      control_status: rel.control_status || null,
      current_historical: rel.temporal_status || rel.current_historical || "CURRENT",
      brand: rel.brand || null,
      operator: rel.operator || null,
      market: rel.market || null,
      country: rel.country || null,
      rooms: rel.rooms ?? null,
      confidence: rel.confidence || "UNKNOWN",
      source_ids: Array.isArray(rel.source_ids) ? rel.source_ids : [],
      focus_hotel: Boolean(rel.focus_hotel),
      census_match_status: rel.census_match_status || (rel.airtable_record_id ? "MATCHED" : "UNRESOLVED_HOTEL_CANDIDATE"),
      bucket: classified.bucket,
      include_as_owned: classified.include_as_owned,
      attribution_reason: classified.reason,
      note: rel.note || null,
      why_in_portfolio:
        rel.why_in_portfolio ||
        formatWhyPath(rel.attribution_path, classified.reason),
    };
    if (!row.attribution_path.length && classified.include_as_owned) {
      row.attribution_path = [
        {
          step: 1,
          relationship_type: row.relationship_type || "OWNED_BY",
          to_entity_id: ownerId,
          to_display_name: ownerNode?.display_name || ownerId,
        },
      ];
      row.why_in_portfolio = formatWhyPath(row.attribution_path, classified.reason);
    }
    safeRels.push(row);
    if (safeRels.length >= budget.max_portfolio_hotels) break;
  }

  const byBucket = Object.fromEntries(PORTFOLIO_BUCKETS.map((b) => [b, []]));
  for (const row of safeRels) {
    const b = row.bucket || "UNKNOWN_RELATIONSHIP";
    if (!byBucket[b]) byBucket[b] = [];
    byBucket[b].push(row);
  }

  const markets = [...new Set(safeRels.map((h) => h.market).filter(Boolean))].sort();
  const brands = [...new Set(safeRels.map((h) => h.brand).filter(Boolean))].sort();
  const operators = [...new Set(safeRels.map((h) => h.operator).filter(Boolean))].sort();

  const completeness = assessPortfolioCompleteness({
    owned_controlled_count: (byBucket.OWNED_CONTROLLED || []).length,
    operated_managed_count: (byBucket.OPERATED_MANAGED || []).length,
    open_question_count: (input.open_questions || []).length,
    source_count: (input.sources || []).length,
    disclosure_quality: input.meta?.disclosure_quality || "UNKNOWN",
    last_researched_at: input.meta?.last_researched_at || null,
    known_gaps: input.meta?.known_gaps || [],
    pubco: Boolean(input.meta?.pubco),
  });

  return {
    schema_version: OWNER_PORTFOLIO_FACTORY_VERSION,
    owner_entity_id: ownerId,
    owner_display_name: ownerNode?.display_name || ownerId,
    owner_legal_name: ownerNode?.legal_name || null,
    portfolio_status: completeness.completeness,
    completeness: completeness.completeness,
    customer_label: completeness.customer_label,
    completeness_detail: completeness,
    last_researched_at: input.meta?.last_researched_at || null,
    related_entities: related,
    hotel_relationships: safeRels,
    buckets: byBucket,
    markets,
    brands,
    operators,
    people: Array.isArray(input.people) ? input.people : [],
    sources: Array.isArray(input.sources) ? input.sources : [],
    open_questions: Array.isArray(input.open_questions) ? input.open_questions : [],
    control_graph: {
      node_count: (graph.nodes || []).length,
      edge_count: (graph.edges || []).length,
      version: graph.schema_version,
    },
    metrics: {
      related_entities: related.length,
      portfolio_hotels: safeRels.length,
      owned_controlled: (byBucket.OWNED_CONTROLLED || []).length,
      jv_partial: (byBucket.JV_PARTIAL || []).length,
      operated_managed: (byBucket.OPERATED_MANAGED || []).length,
      historical: (byBucket.HISTORICAL_DISPOSED || []).length,
      unresolved_census: safeRels.filter((h) => h.census_match_status === "UNRESOLVED_HOTEL_CANDIDATE")
        .length,
    },
    meta: {
      factory_version: OWNER_PORTFOLIO_FACTORY_VERSION,
      compiled_at: new Date().toISOString(),
      ...(input.meta || {}),
    },
  };
}

function formatWhyPath(path, reason) {
  if (Array.isArray(path) && path.length) {
    return path
      .map((p) => `${p.relationship_type || "?"} → ${p.to_display_name || p.to_entity_id || "?"}`)
      .join(" → ");
  }
  return reason || "unspecified";
}
