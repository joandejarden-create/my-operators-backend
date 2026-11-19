/**
 * Standardized staged output envelopes for ownership / org / contact / evidence.
 */

import {
  orgDedupeKey,
  personDedupeKey,
  relationshipDedupeKey,
  channelDedupeKey,
} from "./entity-keys.js";
import { ENGINE_CONTRACT_ID, ENGINE_CONTRACT_VERSION } from "./contract.js";

export function emptyStagedPackage({ hpcId, caseId = null } = {}) {
  return {
    contract: ENGINE_CONTRACT_ID,
    contract_version: ENGINE_CONTRACT_VERSION,
    hpc_id: hpcId || null,
    case_id: caseId,
    research_complete: false,
    ownership: {
      owner_entity: null,
      economic_sponsor: null,
      operator_entity: null,
      owner_vs_operator: "UNRESOLVED",
      claims: [],
    },
    organizations: [],
    relationships: [],
    persons: [],
    channels: [],
    evidence: [],
    unsupported_claims: [],
    validation: { ok: null, outcomes: [] },
    provider_usage: [],
    cost: { usd: null, usd_status: "UNKNOWN", credits: null },
  };
}

export function normalizeOrganization(input = {}) {
  const legalName = input.legal_name || input.name || null;
  const dedupe_key =
    input.dedupe_key ||
    orgDedupeKey({
      legalName,
      jurisdiction: input.jurisdiction,
      registryId: input.registry_id,
    });
  return {
    org_id: input.org_id || null,
    legal_name: legalName,
    jurisdiction: input.jurisdiction || null,
    registry_id: input.registry_id || null,
    parent_org_id: input.parent_org_id || null,
    dedupe_key,
    evidence_refs: input.evidence_refs || [],
    confidence: input.confidence || null,
  };
}

export function normalizeRelationship(input = {}) {
  const dedupe_key =
    input.dedupe_key ||
    relationshipDedupeKey({
      hpcId: input.hpc_id,
      relationshipType: input.relationship_type,
      orgId: input.org_id,
      orgDedupe: input.org_dedupe_key,
    });
  return {
    hpc_id: input.hpc_id || null,
    org_id: input.org_id || null,
    org_dedupe_key: input.org_dedupe_key || null,
    relationship_type: input.relationship_type || null,
    current: input.current !== false,
    effective_from: input.effective_from || null,
    effective_to: input.effective_to || null,
    dedupe_key,
    evidence_refs: input.evidence_refs || [],
    confidence: input.confidence || null,
  };
}

export function normalizePerson(input = {}) {
  const displayName = input.display_name || input.name || null;
  const dedupe_key =
    input.dedupe_key ||
    personDedupeKey({
      displayName,
      orgId: input.org_id,
      orgDedupe: input.org_dedupe_key,
    });
  return {
    person_id: input.person_id || null,
    display_name: displayName,
    title: input.title || null,
    org_id: input.org_id || null,
    org_dedupe_key: input.org_dedupe_key || null,
    why_relevant: input.why_relevant || null,
    role_currency: input.role_currency || null,
    dedupe_key,
    evidence_refs: input.evidence_refs || [],
    confidence: input.confidence || null,
  };
}

export function normalizeChannel(input = {}) {
  const kind = input.kind || input.channel_kind || null;
  const value = input.value || input.normalized_value || null;
  const dedupe_key =
    input.dedupe_key ||
    channelDedupeKey({
      subjectId: input.subject_id,
      kind,
      normalizedValue: value,
    });
  return {
    subject_id: input.subject_id || null,
    subject_type: input.subject_type || null,
    kind,
    value,
    attribution: input.attribution || null,
    verification_status: input.verification_status || "PUBLIC_ONLY",
    usage_rights: input.usage_rights || null,
    dedupe_key,
    evidence_refs: input.evidence_refs || [],
    confidence: input.confidence || null,
  };
}

export function normalizeEvidence(input = {}) {
  return {
    evidence_id: input.evidence_id || null,
    url: input.url || null,
    publisher_class: input.publisher_class || null,
    publication_date: input.publication_date || null,
    event_date: input.event_date || null,
    passage: input.passage || null,
    claim_ids: input.claim_ids || [],
    run_id: input.run_id || null,
    code_version: input.code_version || ENGINE_CONTRACT_VERSION,
  };
}
