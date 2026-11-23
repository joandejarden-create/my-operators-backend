/**
 * Cross-segment leisure ↔ GDI dedupe — same underlying event/program = one opportunity.
 * Preserves multiple segment associations; does not double-count demand/spend.
 */

import { createHash } from "node:crypto";

export const GDI_LEISURE_CROSS_SEGMENT_DEDUPE_V1 =
  "gdi-leisure-cross-segment-dedupe-v1";

function text(v) {
  return String(v == null ? "" : v)
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

/**
 * Soft demand identity for clustering (org + year + title family).
 * Compatible with Repeatable Processing V2 soft clustering approach.
 */
export function leisureDemandClusterKey(opportunity = {}) {
  const org = text(
    opportunity.organization ||
      opportunity.primaryOrganization ||
      opportunity.demandGenerator,
  );
  const title = text(
    opportunity.demandGenerator || opportunity.title || opportunity.name,
  );
  const year =
    String(opportunity.year || "").trim() ||
    extractYear(opportunity.eventOrRecurringDates || opportunity.startDate) ||
    "";
  const family = title
    .replace(/\b(20\d{2}|annual|edition|festival|concert|tournament)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 80);
  return createHash("sha256")
    .update(`${org}|${year}|${family}`)
    .digest("hex")
    .slice(0, 16);
}

function extractYear(v) {
  const s = typeof v === "object" && v ? JSON.stringify(v) : String(v || "");
  const m = s.match(/\b(20\d{2})\b/);
  return m ? m[1] : "";
}

/**
 * Merge leisure candidates into an existing inventory without duplicating IDs or soft clusters.
 */
export function dedupeLeisureAgainstInventory(
  leisureOpportunities = [],
  existingInventory = [],
  opts = {},
) {
  const existing = Array.isArray(existingInventory) ? existingInventory : [];
  const byId = new Map();
  const byCluster = new Map();

  for (const opp of existing) {
    const id = String(opp.id || opp.opportunityId || "").trim();
    if (id) byId.set(id, opp);
    const ck = leisureDemandClusterKey(opp);
    if (!byCluster.has(ck)) byCluster.set(ck, opp);
  }

  const accepted = [];
  const suppressed = [];

  for (const raw of leisureOpportunities) {
    const id = String(raw.opportunityId || raw.id || "").trim();
    const ck = leisureDemandClusterKey(raw);
    const existingHit = (id && byId.get(id)) || byCluster.get(ck);

    if (existingHit && opts.collapseDifferentPathways !== true) {
      // Same demand generator — merge segment associations; keep canonical id
      const canonicalId = existingHit.id || existingHit.opportunityId;
      const mergedSegments = unique([
        ...(existingHit.segmentAssociations || existingHit.segmentIds || []),
        ...(raw.segmentAssociations || []),
        raw.leisureModuleId,
      ]);
      suppressed.push({
        suppressedOpportunityId: id || null,
        clusterKey: ck,
        canonicalOpportunityId: canonicalId,
        reason: "CROSS_SEGMENT_SAME_DEMAND_GENERATOR",
        preservedSegmentAssociations: mergedSegments,
        leisurePathway: raw.leisureCommercialPathway || null,
      });
      // Annotate canonical in-place copy for callers
      existingHit.segmentAssociations = mergedSegments;
      existingHit.leisureModuleAssociations = unique([
        ...(existingHit.leisureModuleAssociations || []),
        raw.leisureModuleId,
      ]);
      continue;
    }

    // Separate pathway component only when independently supported AND linked
    if (
      existingHit &&
      opts.allowLinkedPathwayComponents === true &&
      raw.pathwayComponentIndependent === true &&
      raw.linkedDemandGeneratorId
    ) {
      accepted.push({
        ...raw,
        linkedCanonicalOpportunityId:
          existingHit.id || existingHit.opportunityId,
        isPathwayComponent: true,
      });
      continue;
    }

    accepted.push(raw);
    if (id) byId.set(id, raw);
    byCluster.set(ck, raw);
  }

  return {
    schema: GDI_LEISURE_CROSS_SEGMENT_DEDUPE_V1,
    accepted,
    suppressed,
    acceptedCount: accepted.length,
    suppressedCount: suppressed.length,
  };
}

function unique(arr) {
  return [...new Set(arr.map((x) => String(x || "").trim()).filter(Boolean))];
}
