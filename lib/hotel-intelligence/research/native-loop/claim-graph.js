/**
 * Claim / evidence graph — Iteration 1.
 */

import crypto from "node:crypto";

export const CLAIM_GRAPH_VERSION = "native-claim-graph-v1";

export function addEvidence(state, evidence) {
  const ev = {
    evidence_id: evidence.evidence_id || `ev_${crypto.randomBytes(4).toString("hex")}`,
    source_id: evidence.source_id || null,
    url: evidence.url || null,
    source_type: evidence.source_type || "unknown",
    authority_tier: evidence.authority_tier || "supporting",
    excerpt: evidence.excerpt || evidence.text || "",
    retrieved_at: evidence.retrieved_at || new Date().toISOString(),
    entity_association: evidence.entity_association || "ASSUMED_SUBJECT",
    ...evidence,
  };
  state.evidence.push(ev);
  if (ev.url && !state.sources.some((s) => s.url === ev.url)) {
    state.sources.push({
      source_id: ev.source_id || `src_${state.sources.length + 1}`,
      url: ev.url,
      title: evidence.title || null,
      source_type: ev.source_type,
      authority_tier: ev.authority_tier,
    });
  }
  return ev;
}

export function upsertClaim(state, claim) {
  const existing = state.claims.find(
    (c) =>
      c.claim_id === claim.claim_id ||
      (c.relationship === claim.relationship &&
        norm(c.object) === norm(claim.object) &&
        norm(c.subject) === norm(claim.subject))
  );
  if (existing) {
    existing.evidence_ids = unique([...(existing.evidence_ids || []), ...(claim.evidence_ids || [])]);
    existing.contradiction_ids = unique([
      ...(existing.contradiction_ids || []),
      ...(claim.contradiction_ids || []),
    ]);
    if (rankStatus(claim.status) > rankStatus(existing.status)) {
      existing.status = claim.status;
      existing.confidence = claim.confidence;
    }
    existing.updated_at = new Date().toISOString();
    return existing;
  }
  const row = {
    claim_id: claim.claim_id || `claim_${crypto.randomBytes(4).toString("hex")}`,
    subject: claim.subject,
    relationship: claim.relationship,
    object: claim.object,
    field: claim.field || null,
    status: claim.status || "CANDIDATE",
    confidence: claim.confidence || null,
    confidence_source: claim.confidence_source || "dealality_native_loop",
    evidence_ids: claim.evidence_ids || [],
    contradiction_ids: claim.contradiction_ids || [],
    inference: claim.inference === true,
    unresolved: claim.status === "UNRESOLVED" || claim.unresolved === true,
    created_at: new Date().toISOString(),
  };
  state.claims.push(row);
  return row;
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function unique(arr) {
  return [...new Set(arr.filter(Boolean))];
}

function rankStatus(s) {
  const order = {
    VERIFIED: 5,
    HIGH: 4,
    PROBABLE: 3,
    CANDIDATE: 2,
    SIGNAL_ONLY: 1,
    UNRESOLVED: 0,
    CONTESTED: 0,
    REJECTED: -1,
  };
  return order[String(s || "").toUpperCase()] ?? 1;
}

export function claimsByRelationship(state, rel) {
  return state.claims.filter((c) => c.relationship === rel);
}
