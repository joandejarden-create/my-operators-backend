import {
  ALL_PLAYBOOK_IDS,
  PLAYBOOK_ID,
  STRUCTURED_REJECTION_REASON,
} from "./constants.js";

/**
 * Deterministic playbook selector.
 * Does not blindly repeat attempted strategies.
 *
 * @param {{ rejection_reason?: string, attempted_playbooks?: string[], evidence_signals?: object }} ctx
 * @returns {{ playbook_id: string|null, reason: string, exhausted: boolean }}
 */
export function selectNextPlaybook(ctx = {}) {
  const attempted = new Set(ctx.attempted_playbooks || []);
  const reason = String(ctx.rejection_reason || STRUCTURED_REJECTION_REASON.NO_OWNER_CANDIDATE);
  const signals = ctx.evidence_signals || {};

  /** @type {string[]} */
  let preferred = [];

  switch (reason) {
    case STRUCTURED_REJECTION_REASON.NO_OWNER_CANDIDATE:
      preferred = [PLAYBOOK_ID.GENERAL_OWNERSHIP_DISCOVERY];
      break;
    case STRUCTURED_REJECTION_REASON.HISTORICAL_OWNER:
    case STRUCTURED_REJECTION_REASON.OWNERSHIP_DATE_UNCLEAR:
    case STRUCTURED_REJECTION_REASON.CURRENT_OWNER_UNCONFIRMED:
      preferred = [PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER];
      break;
    case STRUCTURED_REJECTION_REASON.OWNER_OPERATOR_CONFUSION:
    case STRUCTURED_REJECTION_REASON.OPERATING_ENTITY_NOT_OWNER:
      preferred = [
        PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP,
        PLAYBOOK_ID.FINANCING_PUBLIC_RECORD,
      ];
      break;
    case STRUCTURED_REJECTION_REASON.ENTITY_MATCH_UNCERTAIN:
    case STRUCTURED_REJECTION_REASON.ENTITY_LOCATION_CONFLICT:
    case STRUCTURED_REJECTION_REASON.ENTITY_CONFLICT:
    case STRUCTURED_REJECTION_REASON.JV_STRUCTURE_SUSPECTED:
      preferred = [PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP];
      break;
    case STRUCTURED_REJECTION_REASON.FUND_RELATIONSHIP_UNCONFIRMED:
      preferred = [PLAYBOOK_ID.FINANCING_PUBLIC_RECORD];
      break;
    case STRUCTURED_REJECTION_REASON.PROPERTY_RELATIONSHIP_UNCONFIRMED:
      preferred = signals.fii_or_fund
        ? [PLAYBOOK_ID.FINANCING_PUBLIC_RECORD, PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER]
        : [PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER, PLAYBOOK_ID.FINANCING_PUBLIC_RECORD];
      break;
    case STRUCTURED_REJECTION_REASON.SOURCE_TOO_WEAK:
      preferred = [
        PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP,
        PLAYBOOK_ID.FINANCING_PUBLIC_RECORD,
        PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER,
      ];
      break;
    case STRUCTURED_REJECTION_REASON.OWNER_EVIDENCE_INSUFFICIENT:
    case STRUCTURED_REJECTION_REASON.NO_USEFUL_NEW_EVIDENCE:
      preferred = [
        PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER,
        PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP,
        PLAYBOOK_ID.FINANCING_PUBLIC_RECORD,
        PLAYBOOK_ID.GENERAL_OWNERSHIP_DISCOVERY,
      ];
      break;
    default:
      preferred = [
        PLAYBOOK_ID.GENERAL_OWNERSHIP_DISCOVERY,
        PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER,
        PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP,
        PLAYBOOK_ID.FINANCING_PUBLIC_RECORD,
      ];
  }

  // Evidence-driven boosts
  if (signals.historical_owner && !preferred.includes(PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER)) {
    preferred = [PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER, ...preferred];
  }
  if (signals.fii_or_fund && !preferred.includes(PLAYBOOK_ID.FINANCING_PUBLIC_RECORD)) {
    preferred = [PLAYBOOK_ID.FINANCING_PUBLIC_RECORD, ...preferred];
  }
  if (signals.operating_entity_only && !preferred.includes(PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP)) {
    preferred = [PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP, ...preferred];
  }

  for (const id of preferred) {
    if (!attempted.has(id)) {
      return { playbook_id: id, reason: `ROUTE_${reason}_TO_${id}`, exhausted: false };
    }
  }

  for (const id of ALL_PLAYBOOK_IDS) {
    if (!attempted.has(id)) {
      return {
        playbook_id: id,
        reason: `FALLBACK_UNTRIED_${id}`,
        exhausted: false,
      };
    }
  }

  return {
    playbook_id: null,
    reason: STRUCTURED_REJECTION_REASON.RESEARCH_PATH_EXHAUSTED,
    exhausted: true,
  };
}

/**
 * Infer structured rejection / routing reason from research state.
 */
export function inferStructuredRejectionFromState(state = {}) {
  const conclusion = String(state.ownership_conclusion || "");
  const candidates = state.adaptive?.candidates || state.owner_candidates || [];
  const pending = candidates.filter((c) => c.verification_status === "PENDING");
  const rejected = candidates.filter((c) => c.verification_status === "REJECTED");

  if (/HISTORICAL/.test(conclusion)) return STRUCTURED_REJECTION_REASON.HISTORICAL_OWNER;
  if (conclusion === "LEGAL_ENTITY_IDENTIFIED_OWNER_UNCONFIRMED") {
    return STRUCTURED_REJECTION_REASON.CURRENT_OWNER_UNCONFIRMED;
  }
  if (conclusion === "SUPPORTED_CURRENT_OPERATING_ENTITY") {
    return STRUCTURED_REJECTION_REASON.OPERATING_ENTITY_NOT_OWNER;
  }
  if (conclusion === "SUPPORTED_REAL_ESTATE_FUND_LEAD") {
    return STRUCTURED_REJECTION_REASON.FUND_RELATIONSHIP_UNCONFIRMED;
  }
  if (conclusion === "LEGAL_ENTITY_AMBIGUOUS") {
    return STRUCTURED_REJECTION_REASON.ENTITY_MATCH_UNCERTAIN;
  }

  for (const c of rejected) {
    if (c.rejection_reason) return c.rejection_reason;
  }

  if (!candidates.length && !pending.length) {
    return STRUCTURED_REJECTION_REASON.NO_OWNER_CANDIDATE;
  }

  if ((state.stop_reasons || []).includes("NO_USEFUL_NEW_EVIDENCE")) {
    return STRUCTURED_REJECTION_REASON.NO_USEFUL_NEW_EVIDENCE;
  }

  return STRUCTURED_REJECTION_REASON.OWNER_EVIDENCE_INSUFFICIENT;
}

export function collectEvidenceSignals(state = {}) {
  const blob = JSON.stringify({
    conclusion: state.ownership_conclusion,
    leads: state.legal_entity_leads,
    candidates: state.adaptive?.candidates || state.owner_candidates,
    claims: (state.claims || []).slice(0, 20),
  }).slice(0, 8000);
  return {
    historical_owner: /HISTORICAL|historical owner|adquiriu|aquisição/i.test(blob),
    fii_or_fund: /FII|fundo imobili|REIT|CVM|BTHI/i.test(blob),
    operating_entity_only: /OPERATING_ENTITY|administradora hoteleira|SUPPORTED_CURRENT_OPERATING/i.test(
      blob
    ),
    location_conflict: /WRONG_PROPERTY|CONFLICTING_LOCATION|ENTITY_LOCATION/i.test(blob),
  };
}
