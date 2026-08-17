/**
 * Deterministic gap identity + deduplication (P0C).
 */

import { createHash } from "crypto";

export const GAP_ENGINE_RULE_VERSION = "ai_visibility_competitive_gap_engine_v1";

export const GAP_CLASSES = Object.freeze([
  "PEER_PRESENT_BRAND_MISSING",
  "PERSISTENT_SCENARIO_GAP",
  "VALIDATED_ASSOCIATION_GAP",
  "AI_PERCEPTION_VS_DEALALITY_FACT_GAP",
]);

/**
 * Build deterministic gap ID.
 */
export function buildGapId(parts = {}) {
  const seed = [
    parts.gapClass || "",
    parts.subjectBrandId || "",
    (parts.peerBrandIds || []).slice().sort().join(","),
    parts.scenarioId || "",
    parts.geography || "",
    parts.language || "",
    parts.attributeId || "",
    parts.peerSetId || "",
    parts.comparisonWindow || "latest",
  ].join("|");
  return `gap_${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`;
}

/**
 * Dedupe gap records by gapId, merging evidence references.
 * @param {object[]} gaps
 */
export function dedupeGaps(gaps = []) {
  const map = new Map();
  for (const g of gaps) {
    const id = g.gapId;
    if (!map.has(id)) {
      map.set(id, { ...g, evidenceIds: [...(g.evidenceIds || [])], citationIds: [...(g.citationIds || [])], promptIds: [...(g.promptIds || [])], providers: [...(g.providers || [])] });
      continue;
    }
    const existing = map.get(id);
    for (const eid of g.evidenceIds || []) {
      if (!existing.evidenceIds.includes(eid)) existing.evidenceIds.push(eid);
    }
    for (const cid of g.citationIds || []) {
      if (!existing.citationIds.includes(cid)) existing.citationIds.push(cid);
    }
    for (const pid of g.promptIds || []) {
      if (!existing.promptIds.includes(pid)) existing.promptIds.push(pid);
    }
    for (const prov of g.providers || []) {
      if (!existing.providers.includes(prov)) existing.providers.push(prov);
    }
    existing.observationCount = (existing.observationCount || 1) + (g.observationCount || 1);
    existing.questionsMissing = Math.max(existing.questionsMissing || 0, g.questionsMissing || 0);
  }
  return [...map.values()];
}
