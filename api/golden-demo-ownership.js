/**
 * Golden Demo Ownership Intelligence API (controlled Mexico / GSF cohort).
 * Read-only. No CoStar. No Census writes.
 */

import {
  buildHotelOwnershipReport,
  buildOwnershipGroupProfile,
  buildNeighborsPayload,
  listGoldenDemoCohortIndex,
  GSF_SLUG,
} from "../lib/hotel-intelligence/ownership/golden-demo/gsf-cohort.js";

function sendError(res, status, error, details = {}) {
  return res.status(status).json({ success: false, error, ...details });
}

export async function getGoldenDemoOwnershipIndex(req, res) {
  try {
    return res.json({ success: true, ...listGoldenDemoCohortIndex() });
  } catch (err) {
    console.error("[golden-demo-ownership] index", err);
    return sendError(res, 500, "golden_demo_index_failed");
  }
}

export async function getGoldenDemoHotelOwnership(req, res) {
  try {
    const recordId = String(req.params.recordId || "").trim();
    if (!recordId.startsWith("rec")) {
      return sendError(res, 400, "invalid_airtable_record_id");
    }
    const payload = buildHotelOwnershipReport(recordId);
    return res.json({ success: true, ...payload });
  } catch (err) {
    console.error("[golden-demo-ownership] hotel", err);
    return sendError(res, 500, "golden_demo_hotel_failed");
  }
}

export async function getGoldenDemoOwnershipGroup(req, res) {
  try {
    const slug = String(req.params.slug || GSF_SLUG).trim();
    const payload = buildOwnershipGroupProfile(slug);
    if (!payload.ok) return sendError(res, 404, payload.error || "group_not_found");
    return res.json({ success: true, ...payload });
  } catch (err) {
    console.error("[golden-demo-ownership] group", err);
    return sendError(res, 500, "golden_demo_group_failed");
  }
}

/** Focus Graph neighbors — Packet 2 query contract. */
export async function getGoldenDemoOwnershipNeighbors(req, res) {
  try {
    const q = req.query || {};
    const nodeId = String(q.node_id || q.nodeId || q.entity_id || "").trim();
    const nodeType = String(q.node_type || q.nodeType || "ownership_group").trim();
    const hops = Number(q.hops || q.depth || 1);
    if (!nodeId) return sendError(res, 400, "node_id_required");

    const csv = (v) =>
      String(v || "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);

    const payload = buildNeighborsPayload({
      nodeId,
      nodeType,
      hops,
      relationshipTypes: csv(q.relationship_types || q.relationshipTypes),
      relationshipCategories: csv(q.relationship_categories || q.relationshipCategories || q.category),
      currentOnly: q.current_only !== "0" && q.currentOnly !== "0" && q.current_only !== "false",
      minimumConfidence: q.minimum_confidence || q.minimumConfidence || "PROBABLE",
      includeProbable: q.include_probable !== "0" && q.includeProbable !== "0" && q.include_probable !== "false",
      nodeTypes: csv(q.node_types || q.nodeTypes),
      limit: q.limit != null ? Number(q.limit) : undefined,
      expandAll: q.expand_all === "1" || q.expandAll === "1" || q.expand_all === "true",
    });
    if (payload.ok === false) return sendError(res, 404, payload.error || "node_not_found");
    return res.json({ success: true, ...payload });
  } catch (err) {
    console.error("[golden-demo-ownership] neighbors", err);
    return sendError(res, 500, "golden_demo_neighbors_failed");
  }
}
