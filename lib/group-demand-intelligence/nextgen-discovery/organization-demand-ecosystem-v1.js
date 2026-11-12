/**
 * Organization & Demand Ecosystem V1 — evidence-grounded relationship graph.
 *
 * Organization → Division/Affiliate → Program/Project → Demand Trigger →
 * Event/Activity → Commercial Contact → Hotel Demand hypothesis.
 *
 * A relationship or business trigger is NOT confirmed hotel demand.
 * Incremental refresh: merge by stable keys; do not rebuild the universe each week.
 */

import { createHash } from "node:crypto";
import { normalizeOrgIdentityKey } from "../workflow/reuse-prior-intelligence-v1.js";
import {
  GDI_NEXTGEN_DISCOVERY_ARCHITECTURE_V1,
  HUNTER_HYPOTHESIS_STATUS,
} from "./specialist-hunter-contract-v1.js";

export const ORGANIZATION_DEMAND_ECOSYSTEM_V1 =
  "gdi-organization-demand-ecosystem-v1";

export const ECOSYSTEM_NODE_KIND = Object.freeze({
  ORGANIZATION: "ORGANIZATION",
  DIVISION_AFFILIATE: "DIVISION_AFFILIATE",
  PROGRAM_PROJECT: "PROGRAM_PROJECT",
  DEMAND_TRIGGER: "DEMAND_TRIGGER",
  EVENT_ACTIVITY: "EVENT_ACTIVITY",
  COMMERCIAL_CONTACT: "COMMERCIAL_CONTACT",
  HOTEL_DEMAND_HYPOTHESIS: "HOTEL_DEMAND_HYPOTHESIS",
});

export const ECOSYSTEM_EDGE_KIND = Object.freeze({
  PARENT_OF: "PARENT_OF",
  AFFILIATE_OF: "AFFILIATE_OF",
  INSTITUTION_OF: "INSTITUTION_OF",
  CONTRACTOR_OF: "CONTRACTOR_OF",
  PARTICIPATES_IN: "PARTICIPATES_IN",
  PROGRAM_OF: "PROGRAM_OF",
  TRIGGERED_BY: "TRIGGERED_BY",
  ACTIVITY_OF: "ACTIVITY_OF",
  CONTACT_AT: "CONTACT_AT",
  DEMAND_FOR_HOTEL: "DEMAND_FOR_HOTEL",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function stableId(prefix, parts = []) {
  const raw = parts.map((p) => text(p).toLowerCase()).filter(Boolean).join("|");
  const hash = createHash("sha1").update(raw || "empty").digest("hex").slice(0, 12);
  return `${prefix}_${hash}`;
}

export function canonicalOrganizationId({
  name = "",
  domain = "",
  externalId = "",
} = {}) {
  if (externalId) return stableId("org", [externalId]);
  const key = normalizeOrgIdentityKey(name);
  const host = text(domain)
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .split("/")[0];
  return stableId("org", [host || key || "unknown"]);
}

export function createEcosystemGraph({ hotelId = "", generatedAt = "" } = {}) {
  return {
    schema: ORGANIZATION_DEMAND_ECOSYSTEM_V1,
    architecture: GDI_NEXTGEN_DISCOVERY_ARCHITECTURE_V1,
    hotelId: text(hotelId) || null,
    generatedAt: generatedAt || new Date().toISOString(),
    nodes: {},
    edges: [],
    stats: {
      organizations: 0,
      activities: 0,
      contacts: 0,
      hypotheses: 0,
      edges: 0,
    },
  };
}

export function upsertEcosystemNode(graph, node = {}) {
  if (!graph || typeof graph !== "object") throw new Error("graph required");
  const kind = String(node.kind || "");
  if (!Object.values(ECOSYSTEM_NODE_KIND).includes(kind)) {
    throw new Error(`invalid ecosystem node kind: ${kind}`);
  }
  const id =
    text(node.id) ||
    stableId(kind.toLowerCase().slice(0, 8), [
      node.canonicalName || node.name,
      node.hotelId,
      node.activityDate,
      node.role,
    ]);
  const prev = graph.nodes[id] || {};
  const next = {
    ...prev,
    ...node,
    id,
    kind,
    canonicalName: text(node.canonicalName || node.name || prev.canonicalName),
    evidenceUrls: uniqueUrls([
      ...(prev.evidenceUrls || []),
      ...(node.evidenceUrls || []),
    ]),
    verifiedAt: node.verifiedAt || prev.verifiedAt || null,
    confidence: clampConfidence(
      node.confidence != null ? node.confidence : prev.confidence
    ),
    hotelDemandRelevance:
      node.hotelDemandRelevance != null
        ? clampConfidence(node.hotelDemandRelevance)
        : prev.hotelDemandRelevance ?? null,
    sourceHunter: node.sourceHunter || prev.sourceHunter || null,
    updatedAt: new Date().toISOString(),
  };
  graph.nodes[id] = next;
  refreshStats(graph);
  return next;
}

export function addEcosystemEdge(graph, edge = {}) {
  const kind = String(edge.kind || "");
  if (!Object.values(ECOSYSTEM_EDGE_KIND).includes(kind)) {
    throw new Error(`invalid ecosystem edge kind: ${kind}`);
  }
  const fromId = text(edge.fromId);
  const toId = text(edge.toId);
  if (!fromId || !toId) throw new Error("fromId and toId required");
  const id =
    text(edge.id) ||
    stableId("edge", [kind, fromId, toId, edge.label]);
  const exists = graph.edges.find((e) => e.id === id);
  if (exists) {
    Object.assign(exists, {
      ...edge,
      id,
      kind,
      fromId,
      toId,
      evidenceUrls: uniqueUrls([
        ...(exists.evidenceUrls || []),
        ...(edge.evidenceUrls || []),
      ]),
      verifiedAt: edge.verifiedAt || exists.verifiedAt || null,
      confidence: clampConfidence(
        edge.confidence != null ? edge.confidence : exists.confidence
      ),
    });
    refreshStats(graph);
    return exists;
  }
  const row = {
    id,
    kind,
    fromId,
    toId,
    label: text(edge.label) || null,
    evidenceUrls: uniqueUrls(edge.evidenceUrls || []),
    verifiedAt: edge.verifiedAt || null,
    confidence: clampConfidence(edge.confidence),
    sourceHunter: edge.sourceHunter || null,
  };
  graph.edges.push(row);
  refreshStats(graph);
  return row;
}

/**
 * Incremental merge: upsert nodes/edges from a hunter delta without wiping graph.
 */
export function mergeEcosystemDelta(graph, delta = {}) {
  const nodes = Array.isArray(delta.nodes) ? delta.nodes : [];
  const edges = Array.isArray(delta.edges) ? delta.edges : [];
  const upserted = [];
  for (const n of nodes) upserted.push(upsertEcosystemNode(graph, n));
  const linked = [];
  for (const e of edges) linked.push(addEcosystemEdge(graph, e));
  return { upsertedNodeIds: upserted.map((n) => n.id), edgeIds: linked.map((e) => e.id) };
}

/**
 * Build a hotel-demand hypothesis node. Never marks Sales Ready.
 * Relationships / triggers alone stay ORGANIZATION_ONLY or DEMAND_TRIGGER_INFERRED.
 */
export function buildHotelDemandHypothesis({
  hotelId,
  organizationName,
  organizationId,
  activityTitle = "",
  activityDate = "",
  demandTrigger = "",
  territory = "",
  contactName = "",
  contactRole = "",
  evidenceUrls = [],
  sourceHunter = "",
  confidence = 0.4,
  hotelDemandRelevance = 0.5,
  pursuitThesis = "",
  hasCredibleContact = false,
  hasRealisticHotelThesis = false,
} = {}) {
  const orgId =
    organizationId ||
    canonicalOrganizationId({ name: organizationName });
  let status = HUNTER_HYPOTHESIS_STATUS.ORGANIZATION_ONLY;
  if (text(demandTrigger) || text(activityTitle)) {
    status = HUNTER_HYPOTHESIS_STATUS.DEMAND_TRIGGER_INFERRED;
  }
  if (text(activityTitle) && (evidenceUrls || []).length) {
    status = HUNTER_HYPOTHESIS_STATUS.ACTIVITY_EVIDENCED;
  }
  if (hasRealisticHotelThesis && text(pursuitThesis)) {
    status = HUNTER_HYPOTHESIS_STATUS.HOTEL_THESIS_FORMED;
  }
  if (
    status === HUNTER_HYPOTHESIS_STATUS.HOTEL_THESIS_FORMED &&
    hasCredibleContact &&
    text(contactName)
  ) {
    status = HUNTER_HYPOTHESIS_STATUS.READY_FOR_ORS;
  }

  const hypothesisId = stableId("hyp", [
    hotelId,
    orgId,
    activityTitle,
    activityDate,
    sourceHunter,
  ]);

  return {
    id: hypothesisId,
    kind: ECOSYSTEM_NODE_KIND.HOTEL_DEMAND_HYPOTHESIS,
    hotelId: text(hotelId),
    organizationId: orgId,
    organizationName: text(organizationName),
    activityTitle: text(activityTitle),
    activityDate: text(activityDate).slice(0, 10) || null,
    demandTrigger: text(demandTrigger) || null,
    territory: text(territory) || null,
    contactName: text(contactName) || null,
    contactRole: text(contactRole) || null,
    pursuitThesis: text(pursuitThesis) || null,
    evidenceUrls: uniqueUrls(evidenceUrls),
    confidence: clampConfidence(confidence),
    hotelDemandRelevance: clampConfidence(hotelDemandRelevance),
    hypothesisStatus: status,
    sourceHunter: text(sourceHunter) || null,
    commercialDispositionHint:
      status === HUNTER_HYPOTHESIS_STATUS.READY_FOR_ORS
        ? "RESEARCH_CONTINUATION_OR_ACCOUNT_DEVELOPMENT"
        : status === HUNTER_HYPOTHESIS_STATUS.HOTEL_THESIS_FORMED
          ? "RESEARCH_CONTINUATION"
          : "RESEARCH_CONTINUATION",
    notSalesReadyReason:
      "Hunter output is evidence for ORS/qualification — never auto Sales Ready",
    verifiedAt: new Date().toISOString().slice(0, 10),
  };
}

export function accountDedupKey({ organizationName = "", domain = "" } = {}) {
  return canonicalOrganizationId({ name: organizationName, domain });
}

export function opportunityDedupKey({
  organizationName = "",
  activityTitle = "",
  activityDate = "",
  hotelId = "",
} = {}) {
  return stableId("opp", [
    hotelId,
    normalizeOrgIdentityKey(organizationName),
    text(activityTitle).toLowerCase().slice(0, 80),
    text(activityDate).slice(0, 10),
  ]);
}

function uniqueUrls(list = []) {
  const out = [];
  const seen = new Set();
  for (const u of list) {
    const s = text(u);
    if (!s || !/^https?:\/\//i.test(s)) continue;
    const key = s.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(s);
  }
  return out;
}

function clampConfidence(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  return Math.max(0, Math.min(1, n));
}

function refreshStats(graph) {
  const nodes = Object.values(graph.nodes || {});
  graph.stats = {
    organizations: nodes.filter((n) => n.kind === ECOSYSTEM_NODE_KIND.ORGANIZATION)
      .length,
    activities: nodes.filter((n) => n.kind === ECOSYSTEM_NODE_KIND.EVENT_ACTIVITY)
      .length,
    contacts: nodes.filter(
      (n) => n.kind === ECOSYSTEM_NODE_KIND.COMMERCIAL_CONTACT
    ).length,
    hypotheses: nodes.filter(
      (n) => n.kind === ECOSYSTEM_NODE_KIND.HOTEL_DEMAND_HYPOTHESIS
    ).length,
    edges: (graph.edges || []).length,
  };
}

/**
 * Filter nodes that are incremental vs an existing opportunity/account key set.
 */
export function filterIncrementalHypotheses(hypotheses = [], existingKeys = new Set()) {
  const incremental = [];
  const duplicates = [];
  for (const h of hypotheses) {
    const key = opportunityDedupKey({
      organizationName: h.organizationName,
      activityTitle: h.activityTitle,
      activityDate: h.activityDate,
      hotelId: h.hotelId,
    });
    if (existingKeys.has(key) || existingKeys.has(h.id)) {
      duplicates.push({ ...h, dedupKey: key });
    } else {
      incremental.push({ ...h, dedupKey: key });
    }
  }
  return { incremental, duplicates };
}
