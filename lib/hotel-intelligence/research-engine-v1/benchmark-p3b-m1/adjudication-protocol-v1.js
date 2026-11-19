/**
 * Independent evidence adjudication protocol for the live ownership experiment.
 * Search hits / URLs / AI assertions / provider confidence ≠ verified ownership.
 */

import { independentlyAcceptOwnershipClaim } from "./discovery-candidates.js";
import { isSafeHttpUrl } from "./evidence.js";
import {
  OWNERSHIP_RESEARCH_OBJECTIVE_ID,
  OWNERSHIP_RESEARCH_OBJECTIVE_VERSION,
} from "../ownership-research-objective.js";

export const ADJUDICATION_PROTOCOL_VERSION = "he-intel-p3b-m1-adjudication-v1";

export const EVIDENCE_CLASS = Object.freeze({
  INDEPENDENTLY_CORROBORATED: "INDEPENDENTLY_CORROBORATED",
  SINGLE_SOURCE: "SINGLE_SOURCE",
  UNRESOLVED: "UNRESOLVED",
  REJECTED: "REJECTED",
  DISCOVERY_ONLY: "DISCOVERY_ONLY",
});

/**
 * Required fields for each material finding record.
 */
export const MATERIAL_FINDING_FIELDS = Object.freeze([
  "hotel_hpc_id",
  "discovered_entity_or_relationship",
  "claim_type",
  "source_url",
  "publisher",
  "supporting_excerpt",
  "evidence_date",
  "verification_method",
  "acceptance_or_rejection",
  "confidence_level",
  "contradictory_findings",
  "ownership_relationship_relevance",
  "public_contact_evidence",
  "research_method",
  "cost_attribution",
]);

export function buildAdjudicationProtocol() {
  return {
    version: ADJUDICATION_PROTOCOL_VERSION,
    objective_id: OWNERSHIP_RESEARCH_OBJECTIVE_ID,
    objective_version: OWNERSHIP_RESEARCH_OBJECTIVE_VERSION,
    rules: [
      "Search results, URLs, AI assertions, and provider confidence are not independently verified ownership facts",
      "Valid URL alone ≠ ownership verification",
      "FULL_ACTIONABLE requires evidence-grounded owner linked to HPC + relevant person + contact pathway",
      "Do not award FULL_ACTIONABLE from names, contact arrays, URLs, or provider labels alone",
      "Blind adjudication preferred: hide provider identity from reviewer where feasible",
      "Distinguish independently corroborated / single-source / unresolved / rejected",
      "Never invent ownership percentages, personal names, roles, emails, or phones",
      "Legal titleholder alone = partial success, not full",
    ],
    material_finding_fields: MATERIAL_FINDING_FIELDS,
    evidence_classes: EVIDENCE_CLASS,
    acceptance_gate: "independentlyAcceptOwnershipClaim + classifyOwnershipResearchSuccess",
  };
}

/**
 * Adjudicate one discovery candidate into an evidence class.
 */
export function adjudicateFinding(finding = {}, checks = {}) {
  const urlOk = isSafeHttpUrl(finding.url || finding.source_url || finding.evidenceUrl);
  if (!urlOk) {
    return {
      evidence_class: EVIDENCE_CLASS.REJECTED,
      reason: "MISSING_OR_INVALID_URL",
      accepted: false,
    };
  }
  if (finding.kind === "DISCOVERY_CANDIDATE" || finding.verified !== true) {
    const gate = independentlyAcceptOwnershipClaim(finding, checks);
    if (!gate.ok) {
      return {
        evidence_class: EVIDENCE_CLASS.DISCOVERY_ONLY,
        reason: gate.reason,
        accepted: false,
        url_alone_not_verification: true,
      };
    }
  }
  if (checks.contradicted === true) {
    return {
      evidence_class: EVIDENCE_CLASS.REJECTED,
      reason: "CONTRADICTED",
      accepted: false,
    };
  }
  if (checks.secondSourceOrPrimaryDoc === true || checks.primaryDocument === true) {
    return {
      evidence_class: EVIDENCE_CLASS.INDEPENDENTLY_CORROBORATED,
      accepted: true,
    };
  }
  if (finding.verified === true && checks.linksToHotel === true) {
    return {
      evidence_class: EVIDENCE_CLASS.SINGLE_SOURCE,
      accepted: true,
      note: "Single-source — not FULL_ACTIONABLE by itself",
    };
  }
  return {
    evidence_class: EVIDENCE_CLASS.UNRESOLVED,
    accepted: false,
  };
}

export function buildMaterialFindingTemplate(partial = {}) {
  const row = {};
  for (const f of MATERIAL_FINDING_FIELDS) {
    row[f] = partial[f] ?? null;
  }
  return row;
}
