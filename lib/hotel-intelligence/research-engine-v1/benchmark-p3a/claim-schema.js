/**
 * Separated claim layers + research question coverage for P3A.
 * Provider self-reported confidence is never acceptance proof.
 */

import { CLAIM_ACCEPTANCE, EVIDENCE_LAYER } from "./contract.js";

export const REQUIRED_RESEARCH_QUESTIONS = Object.freeze([
  "PROPERTY_IDENTITY",
  "ORGANIZATION_IDENTITY",
  "ULTIMATE_OWNERSHIP",
  "OWNERSHIP_STRUCTURE_CHAIN",
  "BRAND_OPERATOR_OWNER_SEPARATION",
  "PARENT_AND_PORTFOLIO_RELATIONSHIPS",
  "NAMED_OWNERSHIP_DECISION_MAKERS",
  "PUBLIC_CONTACT_CHANNELS",
  "SOURCE_CREDIBILITY_FRESHNESS_TRACEABILITY",
  // retained alias for older fixtures/tests
  "LEGAL_OR_ECONOMIC_OWNERSHIP",
]);

export const CLAIM_TYPES = Object.freeze({
  OWNER_ENTITY: "OWNER_ENTITY",
  LEGAL_TITLEHOLDER: "LEGAL_TITLEHOLDER",
  ULTIMATE_OWNER: "ULTIMATE_OWNER",
  CONTROLLING_INVESTOR: "CONTROLLING_INVESTOR",
  ECONOMIC_SPONSOR: "ECONOMIC_SPONSOR",
  OWNERSHIP_REPRESENTATIVE: "OWNERSHIP_REPRESENTATIVE",
  OWNERSHIP_STRUCTURE_EDGE: "OWNERSHIP_STRUCTURE_EDGE",
  OWNERSHIP_DECISION_MAKER: "OWNERSHIP_DECISION_MAKER",
  ALTERNATIVE_CONTACT_PATHWAY: "ALTERNATIVE_CONTACT_PATHWAY",
  OPERATOR_ENTITY: "OPERATOR_ENTITY",
  OWNER_VS_OPERATOR: "OWNER_VS_OPERATOR",
  ORG_RELATIONSHIP: "ORG_RELATIONSHIP",
  DECISION_MAKER: "DECISION_MAKER",
  PUBLIC_EMAIL: "PUBLIC_EMAIL",
  PUBLIC_PHONE: "PUBLIC_PHONE",
  PROPERTY_IDENTITY: "PROPERTY_IDENTITY",
  UNSUPPORTED: "UNSUPPORTED",
});

/**
 * Normalize a claim into mandatory separated layers.
 */
export function buildClaimEnvelope(input = {}) {
  const evidenceSufficient = Boolean(
    input.supportingSource?.url ||
      (Array.isArray(input.supportingSources) && input.supportingSources.length)
  );
  const independentOk = Boolean(input.independentVerification?.ok);
  const providerConfidence = input.providerConfidence ?? null;

  let acceptance = input.acceptanceDecision || null;
  if (!acceptance) {
    if (!evidenceSufficient) acceptance = CLAIM_ACCEPTANCE.UNKNOWN;
    else if (input.inferredContact === true) acceptance = CLAIM_ACCEPTANCE.REJECTED;
    else if (!independentOk && input.requireIndependent !== false) {
      acceptance = CLAIM_ACCEPTANCE.HELD_FOR_REVIEW;
    } else acceptance = CLAIM_ACCEPTANCE.ACCEPTED;
  }

  if (!evidenceSufficient && acceptance === CLAIM_ACCEPTANCE.ACCEPTED) {
    acceptance = CLAIM_ACCEPTANCE.UNKNOWN;
  }

  return {
    [EVIDENCE_LAYER.DISCOVERED_CLAIM]: {
      claimType: input.claimType || CLAIM_TYPES.UNSUPPORTED,
      value: input.value ?? null,
      text: input.text || null,
    },
    [EVIDENCE_LAYER.SUPPORTING_SOURCE]: input.supportingSource || {
      url: null,
      publisherClass: null,
      publishedAt: null,
      excerpt: null,
    },
    [EVIDENCE_LAYER.INDEPENDENT_VERIFICATION]: input.independentVerification || {
      ok: false,
      method: null,
      note: "missing",
    },
    [EVIDENCE_LAYER.CONFIDENCE]: {
      provider_self_reported: providerConfidence,
      provider_confidence_is_not_proof: true,
      adjudicator_score: input.adjudicatorScore ?? null,
    },
    [EVIDENCE_LAYER.ACCEPTANCE_DECISION]: acceptance,
    [EVIDENCE_LAYER.CANONICAL_GRAPH_MAPPING]: input.graphMapping || {
      targetTable: null,
      stagedOnly: true,
      productionWriteForbidden: true,
    },
    [EVIDENCE_LAYER.PUBLICATION_ELIGIBILITY]: input.publicationEligibility || "BLOCKED",
    inferredContact: Boolean(input.inferredContact),
    neverInventEmail: true,
  };
}

export function rejectInferredContact(claim) {
  const value = String(claim?.value || claim?.text || "");
  const looksEmail = /@/.test(value);
  if (claim?.inferredContact === true || (looksEmail && !claim?.supportingSource?.url)) {
    return buildClaimEnvelope({
      ...claim,
      inferredContact: true,
      acceptanceDecision: CLAIM_ACCEPTANCE.REJECTED,
      claimType: CLAIM_TYPES.PUBLIC_EMAIL,
    });
  }
  return buildClaimEnvelope(claim);
}

export function coverageReport(claims = []) {
  const acceptedTypes = new Set(
    claims
      .filter((c) => c[EVIDENCE_LAYER.ACCEPTANCE_DECISION] === CLAIM_ACCEPTANCE.ACCEPTED)
      .map((c) => c[EVIDENCE_LAYER.DISCOVERED_CLAIM]?.claimType)
  );
  const byQuestion = {};
  for (const q of REQUIRED_RESEARCH_QUESTIONS) {
    byQuestion[q] = {
      covered: questionCovered(q, acceptedTypes),
      status: questionCovered(q, acceptedTypes) ? "COVERED" : "UNKNOWN",
    };
  }
  return {
    required: REQUIRED_RESEARCH_QUESTIONS,
    byQuestion,
    coveredCount: Object.values(byQuestion).filter((x) => x.covered).length,
    totalRequired: REQUIRED_RESEARCH_QUESTIONS.length,
  };
}

function questionCovered(q, types) {
  switch (q) {
    case "PROPERTY_IDENTITY":
      return types.has(CLAIM_TYPES.PROPERTY_IDENTITY);
    case "ORGANIZATION_IDENTITY":
      return (
        types.has(CLAIM_TYPES.ULTIMATE_OWNER) ||
        types.has(CLAIM_TYPES.OWNER_ENTITY) ||
        types.has(CLAIM_TYPES.OPERATOR_ENTITY) ||
        types.has(CLAIM_TYPES.ORG_RELATIONSHIP)
      );
    case "ULTIMATE_OWNERSHIP":
      return (
        types.has(CLAIM_TYPES.ULTIMATE_OWNER) ||
        types.has(CLAIM_TYPES.CONTROLLING_INVESTOR) ||
        types.has(CLAIM_TYPES.ECONOMIC_SPONSOR)
      );
    case "OWNERSHIP_STRUCTURE_CHAIN":
      return (
        types.has(CLAIM_TYPES.OWNERSHIP_STRUCTURE_EDGE) ||
        types.has(CLAIM_TYPES.ORG_RELATIONSHIP)
      );
    case "LEGAL_OR_ECONOMIC_OWNERSHIP":
      // Alias: prefer ultimate; legal titleholder alone is incomplete coverage
      return (
        types.has(CLAIM_TYPES.ULTIMATE_OWNER) ||
        types.has(CLAIM_TYPES.CONTROLLING_INVESTOR) ||
        types.has(CLAIM_TYPES.ECONOMIC_SPONSOR) ||
        types.has(CLAIM_TYPES.OWNER_ENTITY) ||
        types.has(CLAIM_TYPES.LEGAL_TITLEHOLDER)
      );
    case "BRAND_OPERATOR_OWNER_SEPARATION":
      return types.has(CLAIM_TYPES.OWNER_VS_OPERATOR);
    case "PARENT_AND_PORTFOLIO_RELATIONSHIPS":
      return types.has(CLAIM_TYPES.ORG_RELATIONSHIP);
    case "NAMED_OWNERSHIP_DECISION_MAKERS":
    case "NAMED_DECISION_MAKERS":
      return (
        types.has(CLAIM_TYPES.OWNERSHIP_DECISION_MAKER) ||
        types.has(CLAIM_TYPES.DECISION_MAKER)
      );
    case "PUBLIC_CONTACT_CHANNELS":
      return (
        types.has(CLAIM_TYPES.PUBLIC_EMAIL) ||
        types.has(CLAIM_TYPES.PUBLIC_PHONE) ||
        types.has(CLAIM_TYPES.ALTERNATIVE_CONTACT_PATHWAY)
      );
    case "SOURCE_CREDIBILITY_FRESHNESS_TRACEABILITY":
      return true; // scored separately in rubric via source fields
    default:
      return false;
  }
}
