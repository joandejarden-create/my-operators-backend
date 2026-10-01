/**
 * Intermediate research progress metrics — avoid owner YES/NO as sole score.
 */
import {
  OWNERSHIP_CONCLUSION_STATE,
  RESEARCH_PROGRESS_KEYS,
  ENTITY_CANDIDATE_ROLE,
  PROPERTY_LINEAGE_STATE,
} from "./constants.js";
import { NON_OWNER_WITHOUT_INDEPENDENT_EVIDENCE } from "./constants.js";

/**
 * @param {{
 *   lineage?: object,
 *   legal_entities?: object[],
 *   principals?: object[],
 *   conclusion?: string,
 *   owner_domain?: string|null,
 *   decision_maker?: object|null,
 *   contact?: object|null,
 * }} state
 */
export function computeOwnershipResearchProgress(state = {}) {
  const lineage = state.lineage || {};
  const entities = state.legal_entities || [];
  const principals = state.principals || [];
  const conclusion = state.conclusion || OWNERSHIP_CONCLUSION_STATE.UNRESOLVED;

  const hasLegal = entities.some(
    (e) =>
      (e.primary_cnpj || e.legal_name || e.cnpj_digits) &&
      e.legal_entity_resolved_for_target !== false &&
      e.identity_match !== "CONFLICTING_LOCATION" &&
      e.identity_match !== "WRONG_PROPERTY" &&
      e.staging_conclusion !== OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS
  );
  const hasOperating = entities.some((e) =>
    (e.roles || []).some((r) =>
      [
        ENTITY_CANDIDATE_ROLE.OPERATING_ENTITY,
        ENTITY_CANDIDATE_ROLE.HOTEL_OPERATOR,
        ENTITY_CANDIDATE_ROLE.MANAGEMENT_COMPANY,
      ].includes(r)
    ) &&
      e.identity_match !== "CONFLICTING_LOCATION" &&
      e.staging_conclusion !== OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS
  );
  // FII / fund lead is material advancement but NOT titled property ownership
  const hasFundLead = entities.some((e) =>
    (e.roles || []).includes(ENTITY_CANDIDATE_ROLE.REAL_ESTATE_FUND) || e.fii_lead === true
  );
  const hasOwner = entities.some((e) =>
    (e.roles || []).includes(ENTITY_CANDIDATE_ROLE.PROPERTY_OWNER) ||
    (e.roles || []).includes(ENTITY_CANDIDATE_ROLE.OWNER_SPV)
  );
  const identityOk = [
    PROPERTY_LINEAGE_STATE.CURRENT_IDENTITY,
    PROPERTY_LINEAGE_STATE.REBRANDED_PROPERTY,
    PROPERTY_LINEAGE_STATE.RENAMED_PROPERTY,
  ].includes(lineage.lineage_state);
  const historicalOk =
    Array.isArray(lineage.historical_names) && lineage.historical_names.length > 0;

  const progress = {
    current_property_identity_resolved: identityOk,
    historical_lineage_resolved: historicalOk || lineage.lineage_state === PROPERTY_LINEAGE_STATE.REBRANDED_PROPERTY,
    legal_entity_resolved: hasLegal,
    operating_entity_resolved: hasOperating,
    property_owner_resolved: hasOwner ||
      [
        OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_PROPERTY_OWNER,
        OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_OWNER_SPV,
      ].includes(conclusion),
    principal_identified: principals.length > 0,
    owner_domain_identified: Boolean(state.owner_domain),
    decision_maker_identified: Boolean(state.decision_maker),
    contact_identified: Boolean(state.contact),
  };

  const resolvedCount = RESEARCH_PROGRESS_KEYS.filter((k) => progress[k]).length;
  const material_advancement =
    progress.legal_entity_resolved ||
    progress.operating_entity_resolved ||
    progress.principal_identified ||
    progress.property_owner_resolved ||
    progress.historical_lineage_resolved ||
    hasFundLead ||
    conclusion === OWNERSHIP_CONCLUSION_STATE.SUPPORTED_REAL_ESTATE_FUND_LEAD;

  return {
    version: "ownership-research-progress-v1",
    keys: RESEARCH_PROGRESS_KEYS,
    progress,
    resolved_count: resolvedCount,
    total_keys: RESEARCH_PROGRESS_KEYS.length,
    material_advancement,
    titled_ownership_proven: progress.property_owner_resolved,
    note: material_advancement && !progress.property_owner_resolved
      ? "Legal/operating entity or principals identified — material advancement without titled property ownership proof"
      : null,
  };
}

/**
 * Map entity bundle + lineage into a staged conclusion state.
 * Never auto-promotes operator/brand to property owner.
 */
export function concludeFromEntityEvidence({
  lineage = null,
  entities = [],
  conflicting = false,
} = {}) {
  if (conflicting) return OWNERSHIP_CONCLUSION_STATE.CONFLICTING_EVIDENCE;
  if (lineage?.hpc_current_name_review || lineage?.lineage_state === PROPERTY_LINEAGE_STATE.REBRANDED_PROPERTY) {
    if (!entities.length) return OWNERSHIP_CONCLUSION_STATE.PROPERTY_LINEAGE_REQUIRES_REVIEW;
  }

  const active = entities.filter((e) => !e.inactive);
  const inactive = entities.filter((e) => e.inactive);

  const hasFund = active.some((e) => (e.roles || []).includes(ENTITY_CANDIDATE_ROLE.REAL_ESTATE_FUND));
  if (hasFund) return OWNERSHIP_CONCLUSION_STATE.SUPPORTED_REAL_ESTATE_FUND_LEAD;

  const hasOwner = active.some((e) =>
    (e.roles || []).some((r) =>
      [ENTITY_CANDIDATE_ROLE.PROPERTY_OWNER, ENTITY_CANDIDATE_ROLE.OWNER_SPV].includes(r)
    )
  );
  if (hasOwner) {
    return OWNERSHIP_CONCLUSION_STATE.STRONG_PROPERTY_OWNER_CANDIDATE_NEEDS_CONFIRMATION;
  }

  const hasOp = active.some((e) =>
    (e.roles || []).some((r) => NON_OWNER_WITHOUT_INDEPENDENT_EVIDENCE.includes(r))
  );
  if (hasOp) {
    return OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_OPERATING_ENTITY;
  }

  if (active.some((e) => e.primary_cnpj || e.legal_name)) {
    return OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_IDENTIFIED_OWNER_UNCONFIRMED;
  }

  if (inactive.length && !active.length) {
    const histOp = inactive.some((e) =>
      (e.roles || []).some((r) => NON_OWNER_WITHOUT_INDEPENDENT_EVIDENCE.includes(r))
    );
    return histOp
      ? OWNERSHIP_CONCLUSION_STATE.SUPPORTED_HISTORICAL_OPERATING_ENTITY
      : OWNERSHIP_CONCLUSION_STATE.SUPPORTED_HISTORICAL_OWNER;
  }

  if (lineage?.hpc_current_name_review) {
    return OWNERSHIP_CONCLUSION_STATE.PROPERTY_LINEAGE_REQUIRES_REVIEW;
  }

  return OWNERSHIP_CONCLUSION_STATE.UNRESOLVED;
}
