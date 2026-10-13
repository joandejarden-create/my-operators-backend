/**
 * Packet 2.8B-2 — OwnerControlGraph model + helpers.
 */

import { ASSOCIATION_LEVELS, ENTITY_TYPES, RELATIONSHIP_TYPES } from "./vocabulary.js";
import { mayTraverse, createTraversalBudget } from "./traversal-policy.js";

export const OWNER_CONTROL_GRAPH_VERSION = "owner-control-graph-v1";

/**
 * @typedef {{
 *   entity_id: string,
 *   display_name: string,
 *   legal_name?: string|null,
 *   entity_type: string,
 *   association_level?: string|null,
 *   aliases?: string[],
 *   jurisdiction?: string|null,
 *   temporal_status?: string,
 *   evidence_ids?: string[],
 *   confidence?: string,
 * }} OwnerGraphNode
 */

/**
 * @typedef {{
 *   edge_id: string,
 *   from_entity_id: string,
 *   to_entity_id: string,
 *   relationship_type: string,
 *   association_level?: string|null,
 *   temporal_status?: string,
 *   ownership_percentage?: number|null,
 *   control_percentage_class?: string|null,
 *   evidence_ids?: string[],
 *   confidence?: string,
 *   note?: string|null,
 * }} OwnerGraphEdge
 */

export function createEmptyOwnerControlGraph(ownerEntityId, meta = {}) {
  return {
    schema_version: OWNER_CONTROL_GRAPH_VERSION,
    owner_entity_id: ownerEntityId,
    nodes: [],
    edges: [],
    meta: {
      compiled_at: new Date().toISOString(),
      source: meta.source || null,
      ...meta,
    },
  };
}

export function upsertNode(graph, node) {
  if (!node?.entity_id) throw new Error("node_entity_id_required");
  const type = String(node.entity_type || "UNKNOWN").toUpperCase();
  if (!ENTITY_TYPES.includes(type) && type !== "UNKNOWN") {
    // allow unknown but normalize
  }
  const idx = graph.nodes.findIndex((n) => n.entity_id === node.entity_id);
  const normalized = {
    entity_id: node.entity_id,
    display_name: node.display_name || node.entity_id,
    legal_name: node.legal_name || null,
    entity_type: type,
    association_level: node.association_level || null,
    aliases: Array.isArray(node.aliases) ? node.aliases : [],
    jurisdiction: node.jurisdiction || null,
    temporal_status: node.temporal_status || "CURRENT",
    evidence_ids: Array.isArray(node.evidence_ids) ? node.evidence_ids : [],
    confidence: node.confidence || "UNKNOWN",
  };
  if (idx >= 0) graph.nodes[idx] = { ...graph.nodes[idx], ...normalized };
  else graph.nodes.push(normalized);
  return graph;
}

export function upsertEdge(graph, edge) {
  if (!edge?.from_entity_id || !edge?.to_entity_id) {
    throw new Error("edge_endpoints_required");
  }
  const rel = String(edge.relationship_type || "UNKNOWN").toUpperCase();
  if (!RELATIONSHIP_TYPES.includes(rel)) {
    // still allow but mark
  }
  const edgeId =
    edge.edge_id ||
    `${edge.from_entity_id}__${rel}__${edge.to_entity_id}__${edge.temporal_status || "CURRENT"}`;
  const normalized = {
    edge_id: edgeId,
    from_entity_id: edge.from_entity_id,
    to_entity_id: edge.to_entity_id,
    relationship_type: rel,
    association_level: edge.association_level || null,
    temporal_status: edge.temporal_status || "CURRENT",
    ownership_percentage: edge.ownership_percentage ?? null,
    control_percentage_class: edge.control_percentage_class || null,
    evidence_ids: Array.isArray(edge.evidence_ids) ? edge.evidence_ids : [],
    confidence: edge.confidence || "UNKNOWN",
    note: edge.note || null,
  };
  const idx = graph.edges.findIndex((e) => e.edge_id === edgeId);
  if (idx >= 0) graph.edges[idx] = { ...graph.edges[idx], ...normalized };
  else graph.edges.push(normalized);
  return graph;
}

export function listRelatedEntities(graph, { purpose = "related_entities" } = {}) {
  const budget = createTraversalBudget();
  const out = [];
  for (const edge of graph.edges || []) {
    const trav = mayTraverse(edge.relationship_type, purpose, {
      control_evidence: /control/i.test(String(edge.note || "")),
    });
    if (!trav.ok && purpose === "related_entities" && trav.flag === "NO") continue;
    if (purpose === "related_entities" && !trav.ok && trav.flag !== "CONDITIONAL") continue;

    const otherId =
      edge.from_entity_id === graph.owner_entity_id
        ? edge.to_entity_id
        : edge.to_entity_id === graph.owner_entity_id
          ? edge.from_entity_id
          : null;
    // Also include entities connected via PropCo chains (appear in either endpoint)
    const candidateIds = otherId
      ? [otherId]
      : [edge.from_entity_id, edge.to_entity_id].filter((id) => id !== graph.owner_entity_id);

    for (const id of candidateIds) {
      if (id === graph.owner_entity_id) continue;
      const node = (graph.nodes || []).find((n) => n.entity_id === id);
      if (!node) continue;
      if (out.some((r) => r.entity_id === id)) continue;
      out.push({
        ...node,
        via_edge_id: edge.edge_id,
        via_relationship_type: edge.relationship_type,
        association_level: edge.association_level || node.association_level || null,
        why_associated: edge.note || `${edge.relationship_type} evidence`,
      });
      if (out.length >= budget.max_related_entities) return out;
    }
  }
  return out;
}

export function explainEntityAssociation(graph, entityId) {
  const edges = (graph.edges || []).filter(
    (e) => e.from_entity_id === entityId || e.to_entity_id === entityId
  );
  const node = (graph.nodes || []).find((n) => n.entity_id === entityId);
  return {
    entity_id: entityId,
    node: node || null,
    edges,
    why: edges.map((e) => ({
      edge_id: e.edge_id,
      relationship_type: e.relationship_type,
      association_level: e.association_level,
      note: e.note,
      evidence_ids: e.evidence_ids,
    })),
  };
}

export { ASSOCIATION_LEVELS };
