/**
 * Normalize Parallel Task API result → Dealality evidence contract.
 * Reuses addendum-normalize patterns where possible.
 */

import crypto from "node:crypto";
import { normalizeResearchArtifact } from "../../addendum-normalize.js";
import {
  assertSubjectIdentityMatch,
  extractResolvedSubjectIdentity,
} from "./subject-identity.js";

function asArray(v) {
  return Array.isArray(v) ? v : [];
}

function extractStructuredContent(raw) {
  const output = raw?.output || raw?.result?.output || raw;
  const content = output?.content;
  if (content && typeof content === "object" && !Array.isArray(content)) return content;
  if (typeof content === "string") {
    try {
      return JSON.parse(content);
    } catch {
      return { RESEARCH_NOTES: content };
    }
  }
  return content || {};
}

function extractBasisSources(raw) {
  const output = raw?.output || raw?.result?.output || raw;
  const basis = asArray(output?.basis);
  const sources = [];
  const seen = new Set();
  for (const field of basis) {
    for (const c of asArray(field?.citations)) {
      const url = String(c?.url || "").trim();
      if (!url || seen.has(url)) continue;
      seen.add(url);
      sources.push({
        source_id: `src_${sources.length + 1}`,
        url,
        title: c?.title || null,
        publisher: null,
        source_type: null,
        date: null,
        excerpts: asArray(c?.excerpts),
        field: field?.field || null,
        reasoning: field?.reasoning || null,
        confidence: field?.confidence || null,
      });
    }
  }
  return sources;
}

function mapContacts(blocks) {
  return asArray(blocks.CONTACTS).map((c, i) => ({
    contact_id: `ct_${i + 1}`,
    subject_type: c.subject_type || "PERSON",
    subject_name: c.subject_name || c.person_name || null,
    person_name: c.person_name || null,
    organization: c.organization || null,
    role: c.role || null,
    role_currentness: c.role_currentness || "UNKNOWN",
    relationship_to_hotel: c.relationship_to_hotel || null,
    email: c.email || null,
    email_status: c.email_status || "CANDIDATE_ONLY",
    phone: c.phone || null,
    phone_type: c.phone_type || null,
    profile_url: c.profile_url || null,
    source_url: c.source_url || null,
    source_type: c.source_type || null,
    confidence: c.confidence || "CANDIDATE",
    last_seen: c.last_seen || null,
    verification_notes:
      c.verification_notes ||
      "Discovery only — not verified mailbox. Separate verification required.",
    verified_mailbox: false,
  }));
}

/**
 * @param {object} raw Parallel raw artifact (WEBHOUND-compatible shape for normalizeResearchArtifact)
 * @param {object} ctx template, hotel_id, investigation_id
 */
export function normalizeParallelArtifact(raw, ctx = {}) {
  const blocks = extractStructuredContent(raw);
  const basisSources = extractBasisSources(raw);
  const blockSources = asArray(blocks.SOURCES).map((s, i) => ({
    source_id: s.source_id || `src_b_${i + 1}`,
    url: s.url || null,
    title: s.title || null,
    publisher: s.publisher || null,
    source_type: s.source_type || null,
    date: s.date || null,
    authority_tier: s.authority_tier || null,
  }));

  const sources = [...blockSources];
  const seen = new Set(sources.map((s) => s.url).filter(Boolean));
  for (const s of basisSources) {
    if (s.url && !seen.has(s.url)) {
      sources.push(s);
      seen.add(s.url);
    }
  }

  const claims = asArray(blocks.CLAIMS).map((c, i) => ({
    claim_id: c.claim_id || `cl_${i + 1}`,
    text: c.text || c.statement || "",
    status: c.status || "CANDIDATE",
    confidence: c.confidence || null,
    sources: c.source_urls || [],
  }));

  const findings = asArray(blocks.FINDINGS).map((f, i) => ({
    finding_id: f.finding_key || `fnd_${i + 1}`,
    finding_type: "RESEARCH_FINDING",
    headline: String(f.statement || "").slice(0, 280),
    body: f.statement || "",
    status: f.status || "RESEARCH_FINDING",
    confidence: f.confidence || null,
    temporal_relevance: f.temporal_status || null,
    sources: f.source_urls || [],
  }));

  const openQuestions = asArray(blocks.OPEN_QUESTIONS).map((q, i) => ({
    open_question_id: `oq_${i + 1}`,
    title: q.question,
    question: q.question,
    why_it_matters: q.why_it_matters || null,
    what_would_resolve: q.what_would_resolve || null,
  }));

  const webhoundShaped = {
    output: {
      content_markdown: blocks.RESEARCH_NOTES || "",
      structured: blocks,
    },
    sources: { sources },
    claims: { claims },
    open_questions: openQuestions,
    evidence_pack: { documents: [] },
  };

  const base = normalizeResearchArtifact({
    raw: webhoundShaped,
    template: ctx.template || { template_id: ctx.template_id },
    request: {
      hotel_id: ctx.hotel_id,
      report_type: ctx.report_type || "RESEARCH_ADDENDUM",
    },
  });

  return {
    ...base,
    provider: "PARALLEL",
    investigation_id: ctx.investigation_id || null,
    subject_identity: blocks.SUBJECT_IDENTITY || null,
    entities: asArray(blocks.ENTITIES),
    relationships: asArray(blocks.RELATIONSHIPS),
    people: asArray(blocks.PEOPLE),
    events: asArray(blocks.EVENTS),
    property_facts: asArray(blocks.PROPERTY_FACTS),
    conflicts: asArray(blocks.CONFLICTS),
    contacts: mapContacts(blocks),
    research_notes: blocks.RESEARCH_NOTES || null,
    findings: findings.length ? findings : base.findings,
    sources: sources.length ? sources : base.sources,
    open_questions: openQuestions.length ? openQuestions : base.open_questions,
    claims,
    normalized_research_id: `norm_parallel_${crypto.randomBytes(4).toString("hex")}`,
    claim_handoff: { auto_promote: false },
    quality_gate_required: true,
  };
}

export function extractParallelUsage(raw) {
  const run = raw?.run || raw?.result?.run || raw;
  return {
    processor: run?.processor || null,
    status: run?.status || null,
    created_at: run?.created_at || null,
    modified_at: run?.modified_at || null,
    warnings: run?.warnings || null,
    metadata: run?.metadata || null,
  };
}

export function extractParallelCost(raw, compiled = {}) {
  const candidates = [
    raw?.cost_usd,
    raw?.provider_cost_usd,
    raw?.usage?.total_cost,
    raw?.run?.cost_usd,
    compiled?.estimated_cost_usd,
  ];
  for (const c of candidates) {
    const n = Number(c);
    if (Number.isFinite(n) && n >= 0) return Number(n.toFixed(4));
  }
  return null;
}

/**
 * Fail-closed identity gate — provider-agnostic comparison.
 * Downstream findings must not be scored when this fails.
 */
export function assertParallelSubjectIdentity(normalized, expected = {}) {
  const resolved = extractResolvedSubjectIdentity(normalized);
  const requested = {
    name: expected.hotel_name || expected.name || expected.requested_name,
    city: expected.city || expected.requested_city,
    country: expected.country || expected.requested_country,
    coordinates: expected.coordinates || expected.requested_coordinates,
  };

  if (!requested.name) return { ok: true, skipped: true };

  const gate = assertSubjectIdentityMatch(requested, resolved);
  return {
    ok: gate.ok,
    skipped: false,
    code: gate.failure_reason || null,
    identity_match: gate.identity_match,
    failure_reason: gate.failure_reason || null,
    checks: gate.checks,
    detail: gate.detail || null,
    expected_name: requested.name,
    expected_city: requested.city || null,
    expected_country: requested.country || null,
    returned_name: resolved.resolved_name || null,
    returned_city: resolved.resolved_city || null,
    returned_country: resolved.resolved_country || null,
    resolved,
  };
}
