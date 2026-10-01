import crypto from "crypto";
import {
  CANDIDATE_VERIFICATION_STATUS,
  OWNER_CANDIDATE_TYPE,
} from "./constants.js";
import {
  extractEntityNameFromProse,
  qualifyOwnerCandidateName,
} from "./candidate-quality.js";
import {
  buildPropertyContext,
  mergePropertyContext,
  extractLocationHintsFromProse,
  isConflictPropertyMatch,
} from "./property-context.js";
import { PROPERTY_ENTITY_MATCH } from "../ownership-research-strategy/constants.js";

export function normalizeEntityName(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Strip common legal suffixes for soft-equality merges. */
export function normalizeEntityNameCore(name) {
  return normalizeEntityName(name)
    .replace(/\b(ltda|l t d a|s a|sa|s\/a|llc|l p|inc|corp|eireli|spe|spv|mei)\b/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function digitsOnly(v) {
  const d = String(v || "").replace(/\D/g, "");
  return d.length >= 8 ? d : null;
}

function newCandidateId(hotelId, normalizedName, cnpj = null) {
  const h = crypto
    .createHash("sha256")
    .update(`${hotelId || ""}|${cnpj || ""}|${normalizedName}`)
    .digest("hex")
    .slice(0, 12);
  return `oc_${h}`;
}

/**
 * Create an owner/economic-sponsor candidate hypothesis.
 * NEVER a Dealality fact — verification_status defaults to PENDING.
 * Property context (target hotel + candidate location + identity_match) is durable.
 */
export function createOwnerCandidate({
  hotel_id,
  entity_name,
  candidate_type = OWNER_CANDIDATE_TYPE.POSSIBLE_OWNER,
  source_refs = [],
  source_class = null,
  source_date = null,
  why_generated = null,
  generating_playbook = null,
  property_identity_relationship = null,
  entity_relationship = null,
  cnpj = null,
  skip_quality_gate = false,
  hotel = null,
  property_context = null,
  candidate_city = null,
  candidate_state = null,
  candidate_country = null,
  candidate_address = null,
  property_identity_match = null,
  identity_match_reason = null,
  location_conflict = null,
  historical_or_current = null,
  source_snippet = null,
  source_url = null,
  raw_entity_text = null,
} = {}) {
  const rawText = raw_entity_text || entity_name;
  const locHints = extractLocationHintsFromProse(rawText);
  const extracted = extractEntityNameFromProse(entity_name) || String(entity_name || "").trim();
  const gate = skip_quality_gate
    ? { ok: true, cleaned_name: extracted, reason: "SKIPPED" }
    : qualifyOwnerCandidateName(extracted, {
        cnpj,
        allow_person: candidate_type === OWNER_CANDIDATE_TYPE.POSSIBLE_OWNER,
        role_evidence: why_generated,
      });
  if (!gate.ok) {
    return null;
  }
  const cleaned = gate.cleaned_name || extracted;
  const normalized = normalizeEntityName(cleaned);
  const cnpjDigits = digitsOnly(cnpj) || digitsOnly(entity_relationship);
  const now = new Date().toISOString();

  const hotelObj = hotel || { hotel_id };
  const matchSeed =
    property_identity_match ||
    property_identity_relationship ||
    property_context?.property_identity_match ||
    null;

  let ctx = buildPropertyContext({
    hotel: hotelObj,
    candidate_city: candidate_city || locHints.candidate_city || property_context?.candidate_city,
    candidate_state: candidate_state || locHints.candidate_state || property_context?.candidate_state,
    candidate_country: candidate_country || property_context?.candidate_country,
    candidate_address: candidate_address || locHints.candidate_address || property_context?.candidate_address,
    property_identity_match: matchSeed,
    identity_match_reason: identity_match_reason || property_context?.identity_match_reason,
    location_conflict: location_conflict ?? property_context?.location_conflict,
    historical_or_current: historical_or_current || property_context?.historical_or_current,
    source_snippet: source_snippet || property_context?.source_snippet,
    source_url: source_url || (Array.isArray(source_refs) ? source_refs[0] : null) || property_context?.source_url,
    evidence_refs: [
      ...(property_context?.evidence_refs || []),
      ...(Array.isArray(source_refs) ? source_refs : []),
    ],
  });
  if (property_context) {
    ctx = mergePropertyContext(property_context, ctx);
  }

  return {
    candidate_id: newCandidateId(hotel_id || hotelObj.hotel_id, normalized || String(Date.now()), cnpjDigits),
    hotel_id: hotel_id || hotelObj.hotel_id || null,
    entity_name: cleaned || null,
    normalized_entity_name: normalized || null,
    cnpj: cnpjDigits,
    candidate_type,
    source_refs: Array.isArray(source_refs) ? source_refs : [],
    source_class,
    source_date,
    why_generated,
    generating_playbook,
    property_identity_relationship:
      ctx.property_identity_match || property_identity_relationship || null,
    property_identity_match: ctx.property_identity_match || PROPERTY_ENTITY_MATCH.UNKNOWN,
    entity_relationship: entity_relationship || (cnpjDigits ? `cnpj:${cnpjDigits}` : null),
    property_context: ctx,
    verification_status: CANDIDATE_VERIFICATION_STATUS.PENDING,
    verification_evidence: [],
    rejection_reason: null,
    quality_gate: gate.reason,
    created_at: now,
    last_updated_at: now,
    is_dealality_fact: false,
    publication_authorized: false,
  };
}

function findMergeIndex(list, candidate) {
  const norm = candidate.normalized_entity_name || normalizeEntityName(candidate.entity_name);
  const core = normalizeEntityNameCore(candidate.entity_name || norm);
  const cnpj = digitsOnly(candidate.cnpj);
  return list.findIndex((c) => {
    if (c.candidate_id === candidate.candidate_id) return true;
    const theirCnpj = digitsOnly(c.cnpj);
    if (cnpj && theirCnpj && cnpj === theirCnpj) return true;
    const theirNorm = c.normalized_entity_name || normalizeEntityName(c.entity_name);
    const theirCore = normalizeEntityNameCore(c.entity_name || theirNorm);
    if (norm && theirNorm === norm) {
      if (!cnpj || !theirCnpj || cnpj === theirCnpj) return true;
    }
    // Soft merge: same core name with/without LTDA etc., when CNPJ not conflicting
    if (core && theirCore && core === theirCore && core.length >= 8) {
      if (!cnpj || !theirCnpj || cnpj === theirCnpj) return true;
    }
    return false;
  });
}

export function upsertOwnerCandidateDetailed(list = [], candidate) {
  if (!candidate) return { list: list || [], merged: false, rejected_quality: true };
  if (!candidate?.normalized_entity_name && !candidate?.entity_name) {
    return { list: list || [], merged: false, rejected_quality: false };
  }
  const out = [...(list || [])];
  const idx = findMergeIndex(out, candidate);
  if (idx >= 0) {
    const prev = out[idx];
    // Do not merge different CNPJs under same display name
    const a = digitsOnly(prev.cnpj);
    const b = digitsOnly(candidate.cnpj);
    if (a && b && a !== b) {
      out.push({ ...candidate });
      return { list: out, merged: false, rejected_quality: false };
    }
    out[idx] = {
      ...prev,
      ...candidate,
      candidate_id: prev.candidate_id,
      cnpj: a || b || prev.cnpj || candidate.cnpj || null,
      created_at: prev.created_at,
      last_updated_at: new Date().toISOString(),
      source_refs: [...new Set([...(prev.source_refs || []), ...(candidate.source_refs || [])])],
      property_context: mergePropertyContext(prev.property_context, candidate.property_context),
      property_identity_match:
        mergePropertyContext(prev.property_context, candidate.property_context)
          ?.property_identity_match ||
        prev.property_identity_match ||
        candidate.property_identity_match ||
        null,
      property_identity_relationship:
        // Never erase known conflict via soft relationship overwrite
        isConflictPropertyMatch(prev.property_identity_relationship) ||
        isConflictPropertyMatch(prev.property_identity_match)
          ? prev.property_identity_relationship || prev.property_identity_match
          : candidate.property_identity_relationship ||
            prev.property_identity_relationship ||
            null,
      is_dealality_fact: false,
      publication_authorized: false,
      // Preserve stronger verification if already evaluated
      verification_status:
        prev.verification_status && prev.verification_status !== CANDIDATE_VERIFICATION_STATUS.PENDING
          ? prev.verification_status
          : candidate.verification_status,
      // Preserve conflict rejection reasons
      rejection_reason:
        isConflictPropertyMatch(prev.property_identity_match) ||
        isConflictPropertyMatch(prev.property_context?.property_identity_match)
          ? prev.rejection_reason || candidate.rejection_reason
          : candidate.rejection_reason ?? prev.rejection_reason,
    };
    return { list: out, merged: true, rejected_quality: false };
  }
  out.push(candidate);
  return { list: out, merged: false, rejected_quality: false };
}

/** List-returning upsert (backward compatible with V2 callers). */
export function upsertOwnerCandidate(list = [], candidate) {
  return upsertOwnerCandidateDetailed(list, candidate).list;
}

export function rejectOwnerCandidate(list = [], candidateId, rejectionReason, evidence = []) {
  return (list || []).map((c) => {
    if (c.candidate_id !== candidateId) return c;
    return {
      ...c,
      verification_status: CANDIDATE_VERIFICATION_STATUS.REJECTED,
      rejection_reason: rejectionReason,
      verification_reason: rejectionReason,
      verification_evidence: [...(c.verification_evidence || []), ...evidence],
      last_updated_at: new Date().toISOString(),
      is_dealality_fact: false,
      publication_authorized: false,
    };
  });
}

export function supportOwnerCandidate(list = [], candidateId, evidence = []) {
  return (list || []).map((c) => {
    if (c.candidate_id !== candidateId) return c;
    return {
      ...c,
      verification_status: CANDIDATE_VERIFICATION_STATUS.SUPPORTED,
      rejection_reason: null,
      verification_evidence: [...(c.verification_evidence || []), ...evidence],
      last_updated_at: new Date().toISOString(),
      is_dealality_fact: false,
      publication_authorized: false,
    };
  });
}

/**
 * Mine existing research state for candidate hypotheses without inventing owners.
 * Applies quality gate + extraction + CNPJ dedup. Updates telemetry counters on adaptive if provided.
 */
export function mineCandidatesFromExistingState(state = {}, hotel = {}, playbookId = null, telemetry = null) {
  const hotelId = hotel.hotel_id || state.hotel_identity?.hotel_id;
  const prior = [...(state.owner_candidates || state.adaptive?.candidates || [])];
  let list = [];
  const t = telemetry || {};
  t.raw_hypotheses_mined = Number(t.raw_hypotheses_mined || 0);
  t.quality_gate_rejected = Number(t.quality_gate_rejected || 0);
  t.candidate_merges = Number(t.candidate_merges || 0);

  const tryAdd = (rawName, fields = {}) => {
    t.raw_hypotheses_mined += 1;
    const cand = createOwnerCandidate({
      hotel_id: hotelId,
      hotel,
      entity_name: rawName,
      raw_entity_text: rawName,
      generating_playbook: playbookId,
      ...fields,
    });
    if (!cand) {
      t.quality_gate_rejected += 1;
      return;
    }
    // Preserve prior verification dispositions + property context when re-mining
    const priorMatch = prior.find(
      (p) =>
        (p.normalized_entity_name && p.normalized_entity_name === cand.normalized_entity_name) ||
        (digitsOnly(p.cnpj) && digitsOnly(p.cnpj) === digitsOnly(cand.cnpj)) ||
        (p.entity_name &&
          normalizeEntityNameCore(p.entity_name) === normalizeEntityNameCore(cand.entity_name))
    );
    if (priorMatch) {
      cand.property_context = mergePropertyContext(priorMatch.property_context, cand.property_context);
      cand.property_identity_match =
        cand.property_context?.property_identity_match ||
        priorMatch.property_identity_match ||
        cand.property_identity_match;
      if (
        priorMatch.verification_status &&
        priorMatch.verification_status !== CANDIDATE_VERIFICATION_STATUS.PENDING
      ) {
        Object.assign(cand, {
          verification_status: priorMatch.verification_status,
          verification_reason: priorMatch.verification_reason,
          rejection_reason: priorMatch.rejection_reason,
          verified_relationship: priorMatch.verified_relationship,
          property_match: priorMatch.property_match,
          currentness: priorMatch.currentness,
          source_strength: priorMatch.source_strength,
          verification_timestamp: priorMatch.verification_timestamp,
          verification_evidence: priorMatch.verification_evidence || [],
          supported_non_owner_entity: priorMatch.supported_non_owner_entity,
          candidate_id: priorMatch.candidate_id,
        });
      }
    }
    const res = upsertOwnerCandidateDetailed(list, cand);
    list = res.list;
    if (res.merged) t.candidate_merges += 1;
  };

  // Re-qualify prior candidates through quality gate (drops V2 garbage)
  for (const p of prior) {
    tryAdd(p.entity_name, {
      candidate_type: p.candidate_type || OWNER_CANDIDATE_TYPE.POSSIBLE_OWNER,
      source_refs: p.source_refs || [],
      source_class: p.source_class || null,
      why_generated: p.why_generated || "requalified_prior_candidate",
      generating_playbook: p.generating_playbook || playbookId,
      cnpj: p.cnpj || null,
      property_identity_relationship: p.property_identity_relationship || null,
      property_identity_match: p.property_identity_match || p.property_context?.property_identity_match,
      property_context: p.property_context || null,
      entity_relationship: p.entity_relationship || null,
      candidate_city: p.property_context?.candidate_city,
      candidate_state: p.property_context?.candidate_state,
      identity_match_reason: p.property_context?.identity_match_reason,
      location_conflict: p.property_context?.location_conflict,
      source_snippet: p.property_context?.source_snippet,
    });
  }

  for (const lead of state.legal_entity_leads || []) {
    const name = lead.legal_name || lead.entity_name || lead.trade_name;
    if (!name && !lead.cnpj) continue;
    const roles = lead.roles || [];
    let type = OWNER_CANDIDATE_TYPE.UNKNOWN_RELATIONSHIP;
    if (roles.includes("PROPERTY_OWNER")) type = OWNER_CANDIDATE_TYPE.DIRECT_PROPERTY_OWNER;
    else if (roles.includes("OWNER_SPV")) type = OWNER_CANDIDATE_TYPE.OWNER_SPV;
    else if (roles.includes("REAL_ESTATE_FUND")) type = OWNER_CANDIDATE_TYPE.REIT_OR_FUND;
    else if (roles.includes("OPERATING_ENTITY")) type = OWNER_CANDIDATE_TYPE.OPERATING_ENTITY;
    else if (roles.includes("HOTEL_OPERATOR") || roles.includes("MANAGEMENT_COMPANY")) {
      type = OWNER_CANDIDATE_TYPE.HOTEL_OPERATOR;
    } else if (roles.includes("HISTORICAL_OWNER")) type = OWNER_CANDIDATE_TYPE.HISTORICAL_OWNER;
    else type = OWNER_CANDIDATE_TYPE.POSSIBLE_OWNER;

    tryAdd(name || `CNPJ ${lead.cnpj}`, {
      candidate_type: type,
      source_refs: lead.source_url ? [lead.source_url] : [],
      source_class: lead.source_class || null,
      why_generated: "mined_from_legal_entity_lead",
      cnpj: lead.cnpj || lead.cnpj_digits || null,
      property_identity_relationship: lead.identity_match || null,
      property_identity_match: lead.identity_match || lead.property_entity_match || null,
      identity_match_reason: lead.identity_match_reason || lead.match_reason || null,
      candidate_city: lead.city || lead.municipio || null,
      candidate_state: lead.state || lead.uf || null,
      candidate_address: lead.address || null,
      source_url: lead.source_url || null,
      source_snippet: lead.snippet || lead.passage || null,
      historical_or_current: lead.currentness || null,
    });
  }

  for (const party of state.candidate_parties || []) {
    const name = party.name || party.display_name || party.entity_name;
    if (!name) continue;
    tryAdd(name, {
      candidate_type: mapPartyRoleToCandidateType(party.role || party.party_role),
      source_refs: party.source_url ? [party.source_url] : [],
      why_generated: "mined_from_candidate_party",
    });
  }

  for (const claim of state.claims || []) {
    const name = claim.corrected_subject || claim.subject;
    if (!name) continue;
    const hist =
      claim.currentness === "HISTORICAL" ||
      String(claim.currentness || "").includes("HISTORICAL") ||
      claim.party_role === "HISTORICAL_OWNER";
    tryAdd(name, {
      candidate_type: hist
        ? OWNER_CANDIDATE_TYPE.HISTORICAL_OWNER
        : OWNER_CANDIDATE_TYPE.POSSIBLE_OWNER,
      source_refs: [claim.url || claim.source_url].filter(Boolean),
      source_date: claim.event_date || claim.source_date || null,
      why_generated: "mined_from_ownership_claim",
    });
  }

  t.candidates_after_normalization = list.length;
  return { list, telemetry: t };
}

function mapPartyRoleToCandidateType(role) {
  const r = String(role || "").toUpperCase();
  if (r.includes("HISTORICAL")) return OWNER_CANDIDATE_TYPE.HISTORICAL_OWNER;
  if (r.includes("OPERATOR") || r.includes("MANAGEMENT")) return OWNER_CANDIDATE_TYPE.HOTEL_OPERATOR;
  if (r.includes("OPERATING")) return OWNER_CANDIDATE_TYPE.OPERATING_ENTITY;
  if (r.includes("FUND") || r.includes("REIT") || r.includes("FII")) {
    return OWNER_CANDIDATE_TYPE.REIT_OR_FUND;
  }
  if (r.includes("SPV")) return OWNER_CANDIDATE_TYPE.OWNER_SPV;
  if (r.includes("OWNER") || r.includes("SPONSOR")) return OWNER_CANDIDATE_TYPE.POSSIBLE_OWNER;
  if (r.includes("DEVELOPER")) return OWNER_CANDIDATE_TYPE.DEVELOPER;
  return OWNER_CANDIDATE_TYPE.UNKNOWN_RELATIONSHIP;
}
