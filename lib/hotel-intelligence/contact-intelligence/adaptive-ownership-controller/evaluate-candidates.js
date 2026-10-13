/**
 * Adaptive V2.1 — evaluate actionable candidates from EXISTING research state.
 * Never publishes HPC ownership facts. Never calls providers.
 */

import {
  CANDIDATE_VERIFICATION_STATUS,
  OWNER_CANDIDATE_TYPE,
  STRUCTURED_REJECTION_REASON,
  PLAYBOOK_ID,
} from "./constants.js";
import { lookupOwnerGraphIntelligence } from "./owner-graph-reuse.js";
import {
  assessTargetPropertyRelevance,
  isConflictPropertyMatch,
} from "./property-context.js";
import {
  OWNERSHIP_CONCLUSION_STATE,
  PROPERTY_ENTITY_MATCH,
} from "../ownership-research-strategy/constants.js";

function nowIso() {
  return new Date().toISOString();
}

function isOwnerLikeType(t) {
  return [
    OWNER_CANDIDATE_TYPE.DIRECT_PROPERTY_OWNER,
    OWNER_CANDIDATE_TYPE.OWNER_SPV,
    OWNER_CANDIDATE_TYPE.ECONOMIC_SPONSOR,
    OWNER_CANDIDATE_TYPE.REIT_OR_FUND,
    OWNER_CANDIDATE_TYPE.POSSIBLE_OWNER,
  ].includes(t);
}

function isOperatorLikeType(t) {
  return [
    OWNER_CANDIDATE_TYPE.OPERATING_ENTITY,
    OWNER_CANDIDATE_TYPE.HOTEL_OPERATOR,
    OWNER_CANDIDATE_TYPE.MANAGEMENT_COMPANY,
  ].includes(t);
}

const LEGAL_SUFFIX_HINT = /\b(ltda\.?|s\.?\s*a\.?|sa|llc|eireli|spe|spv)\b/i;

/**
 * Deterministically evaluate one candidate against staged research state.
 * Consumes durable property_context (Gate 2 relevance) before owner support.
 */
export function evaluateOneCandidate(candidate, state = {}, hotel = {}) {
  const conclusion = String(state.ownership_conclusion || "");
  const leads = state.legal_entity_leads || [];
  const name = String(candidate.entity_name || "");
  const norm = String(candidate.normalized_entity_name || "").toLowerCase();
  const cnpj = candidate.cnpj || candidate.entity_relationship?.replace(/^cnpj:/, "") || null;
  const ctx = candidate.property_context || {};

  const matchingLead = leads.find((l) => {
    const ln = String(l.legal_name || l.entity_name || l.trade_name || "")
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
    if (norm && ln && (ln.includes(norm) || norm.includes(ln))) return true;
    if (cnpj && String(l.cnpj || l.cnpj_digits || "").replace(/\D/g, "") === String(cnpj).replace(/\D/g, "")) {
      return true;
    }
    return false;
  });

  // Prefer durable conflict/strong matches; do not let UNKNOWN shadow lead evidence
  const ctxMatch = ctx.property_identity_match || candidate.property_identity_match || null;
  const leadMatch = matchingLead?.identity_match || matchingLead?.property_entity_match || null;
  let identity = null;
  if (isConflictPropertyMatch(ctxMatch) || isConflictPropertyMatch(leadMatch)) {
    identity = isConflictPropertyMatch(ctxMatch) ? ctxMatch : leadMatch;
  } else if (
    ctxMatch &&
    ctxMatch !== PROPERTY_ENTITY_MATCH.UNKNOWN &&
    ctxMatch !== "UNKNOWN"
  ) {
    identity = ctxMatch;
  } else {
    identity = leadMatch || ctxMatch || candidate.property_identity_relationship || null;
  }

  const base = {
    verification_timestamp: nowIso(),
    property_match: identity || PROPERTY_ENTITY_MATCH.UNKNOWN,
    currentness: ctx.historical_or_current || matchingLead?.currentness || null,
    source_strength: matchingLead?.source_class || candidate.source_class || null,
    verified_relationship: null,
    verification_evidence: [],
  };

  // Gate 2 — target-property relevance (legal shape alone is insufficient)
  const relevance = assessTargetPropertyRelevance(candidate, hotel);
  if (relevance.gate === "FAIL_LOCATION" || isConflictPropertyMatch(identity)) {
    const match =
      relevance.property_identity_match ||
      identity ||
      PROPERTY_ENTITY_MATCH.CONFLICTING_LOCATION;
    const rejected = match === PROPERTY_ENTITY_MATCH.WRONG_PROPERTY || match === "WRONG_PROPERTY";
    return {
      ...base,
      verification_status: rejected
        ? CANDIDATE_VERIFICATION_STATUS.REJECTED
        : CANDIDATE_VERIFICATION_STATUS.REJECTED,
      verification_reason: rejected
        ? STRUCTURED_REJECTION_REASON.ENTITY_LOCATION_CONFLICT
        : STRUCTURED_REJECTION_REASON.ENTITY_LOCATION_CONFLICT,
      property_match: match,
      verified_relationship: null,
      next_playbook: PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP,
      trigger_owner_graph: false,
      target_property_relevant: false,
      verification_evidence: [
        { note: "gate2_location_or_conflict", relevance, identity },
      ],
    };
  }
  if (relevance.gate === "FAIL_UNRELATED") {
    return {
      ...base,
      verification_status: CANDIDATE_VERIFICATION_STATUS.REJECTED,
      verification_reason: STRUCTURED_REJECTION_REASON.ENTITY_LOCATION_CONFLICT,
      property_match: PROPERTY_ENTITY_MATCH.WRONG_PROPERTY,
      verified_relationship: null,
      next_playbook: PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP,
      trigger_owner_graph: false,
      target_property_relevant: false,
      verification_evidence: [{ note: "gate2_unrelated_legal_entity", relevance }],
    };
  }

  // Name-embedded wrong-city cue (fallback if context lost earlier)
  const hotelCity = String(hotel.city || ctx.target_city || hotel.market || "").toLowerCase();
  const nameLoc = name.match(/localizad[oa]\s+em\s+([^,;]+)/i);
  if (nameLoc && hotelCity) {
    const mentioned = nameLoc[1].toLowerCase();
    if (
      mentioned.length > 3 &&
      !mentioned.includes(hotelCity.slice(0, 5)) &&
      !hotelCity.includes(mentioned.slice(0, 5))
    ) {
      return {
        ...base,
        verification_status: CANDIDATE_VERIFICATION_STATUS.REJECTED,
        verification_reason: STRUCTURED_REJECTION_REASON.ENTITY_LOCATION_CONFLICT,
        property_match: PROPERTY_ENTITY_MATCH.CONFLICTING_LOCATION,
        verified_relationship: null,
        next_playbook: PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP,
        trigger_owner_graph: false,
        target_property_relevant: false,
        verification_evidence: [{ note: "name_embedded_location_conflict", mentioned }],
      };
    }
  }

  // Historical
  if (
    candidate.candidate_type === OWNER_CANDIDATE_TYPE.HISTORICAL_OWNER ||
    /HISTORICAL/.test(conclusion) ||
    matchingLead?.roles?.includes?.("HISTORICAL_OWNER")
  ) {
    return {
      ...base,
      verification_status: CANDIDATE_VERIFICATION_STATUS.INSUFFICIENT,
      verification_reason: STRUCTURED_REJECTION_REASON.HISTORICAL_OWNER,
      verified_relationship: "HISTORICAL_OWNER",
      next_playbook: PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER,
      trigger_owner_graph: false,
      verification_evidence: [{ conclusion, note: "historical_entity" }],
    };
  }

  // Operator / management — supported relationship, not property owner
  if (
    isOperatorLikeType(candidate.candidate_type) ||
    /administradora/i.test(name) ||
    (candidate.candidate_type === OWNER_CANDIDATE_TYPE.UNKNOWN_RELATIONSHIP &&
      conclusion === OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_OPERATING_ENTITY &&
      LEGAL_SUFFIX_HINT.test(name))
  ) {
    if (
      isOperatorLikeType(candidate.candidate_type) ||
      /administradora/i.test(name) ||
      conclusion === OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_OPERATING_ENTITY
    ) {
      return {
        ...base,
        verification_status: CANDIDATE_VERIFICATION_STATUS.INSUFFICIENT,
        verification_reason: STRUCTURED_REJECTION_REASON.OPERATING_ENTITY_NOT_OWNER,
        verified_relationship: "OPERATING_ENTITY",
        next_playbook: PLAYBOOK_ID.FINANCING_PUBLIC_RECORD,
        trigger_owner_graph: false,
        verification_evidence: [
          {
            note: "supported_operating_relationship_not_property_owner",
            conclusion,
          },
        ],
        supported_non_owner_entity: true,
      };
    }
  }

  // Fund / FII lead
  if (
    candidate.candidate_type === OWNER_CANDIDATE_TYPE.REIT_OR_FUND ||
    conclusion === OWNERSHIP_CONCLUSION_STATE.SUPPORTED_REAL_ESTATE_FUND_LEAD ||
    /\bFII\b|fundo imobili/i.test(name)
  ) {
    const fundConfirmed =
      conclusion === OWNERSHIP_CONCLUSION_STATE.SUPPORTED_REAL_ESTATE_FUND ||
      conclusion === OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_OWNER_SPV;
    if (fundConfirmed) {
      return {
        ...base,
        verification_status: CANDIDATE_VERIFICATION_STATUS.SUPPORTED,
        verification_reason: "SUPPORTED_REAL_ESTATE_FUND",
        verified_relationship: "REAL_ESTATE_FUND",
        next_playbook: null,
        trigger_owner_graph: true,
        verification_evidence: [{ conclusion }],
      };
    }
    return {
      ...base,
      verification_status: CANDIDATE_VERIFICATION_STATUS.INSUFFICIENT,
      verification_reason: STRUCTURED_REJECTION_REASON.FUND_RELATIONSHIP_UNCONFIRMED,
      verified_relationship: "FUND_LEAD",
      next_playbook: PLAYBOOK_ID.FINANCING_PUBLIC_RECORD,
      trigger_owner_graph: false,
      verification_evidence: [{ conclusion, note: "fii_lead_without_titled_owner" }],
      supported_non_owner_entity: Boolean(
        conclusion === OWNERSHIP_CONCLUSION_STATE.SUPPORTED_REAL_ESTATE_FUND_LEAD
      ),
    };
  }

  // Legal entity identified, owner unconfirmed
  if (
    conclusion === OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_IDENTIFIED_OWNER_UNCONFIRMED ||
    (matchingLead && !matchingLead.property_owner_resolved)
  ) {
    if (isOwnerLikeType(candidate.candidate_type) || matchingLead) {
      return {
        ...base,
        verification_status: CANDIDATE_VERIFICATION_STATUS.INSUFFICIENT,
        verification_reason: STRUCTURED_REJECTION_REASON.CURRENT_OWNER_UNCONFIRMED,
        verified_relationship: matchingLead ? "LEGAL_ENTITY" : null,
        next_playbook: PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER,
        trigger_owner_graph: false,
        verification_evidence: [{ conclusion, lead: Boolean(matchingLead) }],
        supported_non_owner_entity: Boolean(matchingLead?.legal_name || matchingLead?.cnpj),
      };
    }
  }

  // Ambiguous / uncertain match
  if (
    conclusion === OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS ||
    identity === PROPERTY_ENTITY_MATCH.PARTIAL_MATCH ||
    identity === PROPERTY_ENTITY_MATCH.NAME_ONLY_MATCH
  ) {
    return {
      ...base,
      verification_status: CANDIDATE_VERIFICATION_STATUS.INSUFFICIENT,
      verification_reason: STRUCTURED_REJECTION_REASON.ENTITY_MATCH_UNCERTAIN,
      verified_relationship: null,
      next_playbook: PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP,
      trigger_owner_graph: false,
      verification_evidence: [{ conclusion, identity }],
    };
  }

  // Strong owner support from conclusion + owner-like type
  if (
    (conclusion === OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_PROPERTY_OWNER ||
      conclusion === OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_OWNER_SPV) &&
    isOwnerLikeType(candidate.candidate_type)
  ) {
    return {
      ...base,
      verification_status: CANDIDATE_VERIFICATION_STATUS.SUPPORTED,
      verification_reason: "SUPPORTED_CURRENT_OWNER",
      verified_relationship:
        conclusion === OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_OWNER_SPV
          ? "OWNER_SPV"
          : "PROPERTY_OWNER",
      next_playbook: null,
      trigger_owner_graph: true,
      verification_evidence: [{ conclusion }],
    };
  }

  // Default: actionable but evidence insufficient for owner claim
  return {
    ...base,
    verification_status: CANDIDATE_VERIFICATION_STATUS.INSUFFICIENT,
    verification_reason: STRUCTURED_REJECTION_REASON.OWNER_EVIDENCE_INSUFFICIENT,
    verified_relationship: null,
    next_playbook: PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER,
    trigger_owner_graph: false,
    verification_evidence: [{ conclusion: conclusion || "UNRESOLVED", note: "no_owner_support" }],
  };
}

/**
 * Evaluate all PENDING actionable candidates; update adaptive state + telemetry.
 * Triggers owner-graph only for supported owner/sponsor dispositions.
 */
export function evaluateAllActionableCandidates(adaptive, state = {}, hotel = {}) {
  const a = {
    ...(adaptive || {}),
    candidates: [...(adaptive?.candidates || [])],
    owner_graph_lookups: [...(adaptive?.owner_graph_lookups || [])],
    telemetry: { ...(adaptive?.telemetry || {}) },
    verification_runs: [...(adaptive?.verification_runs || [])],
  };

  const t = a.telemetry;
  t.verification_attempts = Number(t.verification_attempts || 0);
  t.candidates_supported = Number(t.candidates_supported || 0);
  t.candidates_rejected = Number(t.candidates_rejected || 0);
  t.candidates_insufficient = Number(t.candidates_insufficient || 0);
  t.supported_non_owner_entities = Number(t.supported_non_owner_entities || 0);
  t.owner_graph_reuse_events = Number(t.owner_graph_reuse_events || 0);

  let routingReason = null;
  let nextPlaybook = null;

  for (let i = 0; i < a.candidates.length; i++) {
    const c = a.candidates[i];
    const ctxConflict =
      c.property_context?.location_conflict === true ||
      isConflictPropertyMatch(c.property_context?.property_identity_match) ||
      isConflictPropertyMatch(c.property_identity_match);
    const needsReeval =
      !c.verification_status ||
      c.verification_status === CANDIDATE_VERIFICATION_STATUS.PENDING ||
      (ctxConflict &&
        c.verification_reason !== STRUCTURED_REJECTION_REASON.ENTITY_LOCATION_CONFLICT);
    if (!needsReeval) {
      continue;
    }
    t.verification_attempts += 1;
    const result = evaluateOneCandidate(c, state, hotel);
    const nextCtx = {
      ...(c.property_context || {}),
      property_identity_match:
        result.property_match || c.property_context?.property_identity_match || null,
      location_conflict:
        isConflictPropertyMatch(result.property_match) ||
        c.property_context?.location_conflict === true,
    };
    const updated = {
      ...c,
      verification_status: result.verification_status,
      verification_reason: result.verification_reason,
      rejection_reason:
        result.verification_status === CANDIDATE_VERIFICATION_STATUS.REJECTED
          ? result.verification_reason
          : result.verification_status === CANDIDATE_VERIFICATION_STATUS.INSUFFICIENT
            ? result.verification_reason
            : null,
      verified_relationship: result.verified_relationship,
      property_match: result.property_match,
      property_identity_match: result.property_match,
      property_context: nextCtx,
      currentness: result.currentness,
      source_strength: result.source_strength,
      verification_timestamp: result.verification_timestamp,
      verification_evidence: [
        ...(c.verification_evidence || []),
        ...(result.verification_evidence || []),
      ],
      supported_non_owner_entity: result.supported_non_owner_entity === true,
      target_property_relevant: result.target_property_relevant !== false,
      last_updated_at: nowIso(),
      is_dealality_fact: false,
      publication_authorized: false,
    };
    a.candidates[i] = updated;
    a.verification_runs.push({
      candidate_id: c.candidate_id,
      ...result,
    });

    if (result.verification_status === CANDIDATE_VERIFICATION_STATUS.SUPPORTED) {
      t.candidates_supported += 1;
    } else if (result.verification_status === CANDIDATE_VERIFICATION_STATUS.REJECTED) {
      t.candidates_rejected += 1;
    } else if (result.verification_status === CANDIDATE_VERIFICATION_STATUS.INSUFFICIENT) {
      t.candidates_insufficient += 1;
      if (result.supported_non_owner_entity) t.supported_non_owner_entities += 1;
    }

    if (result.trigger_owner_graph === true) {
      const lookup = lookupOwnerGraphIntelligence({
        hotel_id: hotel.hotel_id || c.hotel_id,
        entity_name: c.entity_name,
      });
      a.owner_graph_lookups.push({
        ...lookup,
        candidate_id: c.candidate_id,
        allowed_because: result.verification_reason,
      });
      if (lookup.owner_graph_reuse) t.owner_graph_reuse_events += 1;
    }

    if (result.next_playbook && !nextPlaybook) {
      nextPlaybook = result.next_playbook;
      routingReason = result.verification_reason;
    }
  }

  // Reconcile pending count
  t.candidates_pending = a.candidates.filter(
    (c) => c.verification_status === CANDIDATE_VERIFICATION_STATUS.PENDING
  ).length;
  t.candidates_after_verification = a.candidates.length;

  return {
    adaptive: a,
    routing: {
      from_verification: Boolean(routingReason),
      rejection_reason: routingReason,
      next_playbook: nextPlaybook,
    },
  };
}

/**
 * Prefer candidate verification outcomes for playbook routing.
 */
export function selectPlaybookFromCandidateVerification(adaptive) {
  const pendingRoute = (adaptive?.verification_runs || []).find((r) => r.next_playbook);
  if (pendingRoute?.next_playbook) {
    return {
      playbook_id: pendingRoute.next_playbook,
      reason: `CANDIDATE_VERIFICATION_${pendingRoute.verification_reason}`,
      exhausted: false,
    };
  }
  const insuff = (adaptive?.candidates || []).find(
    (c) =>
      c.verification_status === CANDIDATE_VERIFICATION_STATUS.INSUFFICIENT &&
      c.verification_reason
  );
  if (insuff?.verification_reason === STRUCTURED_REJECTION_REASON.OPERATING_ENTITY_NOT_OWNER) {
    return {
      playbook_id: PLAYBOOK_ID.FINANCING_PUBLIC_RECORD,
      reason: "CANDIDATE_OPERATING_ENTITY_NOT_OWNER",
      exhausted: false,
    };
  }
  if (insuff?.verification_reason === STRUCTURED_REJECTION_REASON.HISTORICAL_OWNER) {
    return {
      playbook_id: PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER,
      reason: "CANDIDATE_HISTORICAL_OWNER",
      exhausted: false,
    };
  }
  if (insuff?.verification_reason === STRUCTURED_REJECTION_REASON.ENTITY_LOCATION_CONFLICT) {
    return {
      playbook_id: PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP,
      reason: "CANDIDATE_ENTITY_LOCATION_CONFLICT",
      exhausted: false,
    };
  }
  return null;
}
