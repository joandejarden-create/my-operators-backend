/**
 * Constrained adjudicator prompt + structured I/O contract.
 */

import {
  GOVERNED_RECOMMENDATION_ROLES,
  TAXONOMY_DEFINITIONS,
  TAXONOMY_DECISION_RULES,
} from "./taxonomy.js";

export const ADJUDICATOR_PROMPT_VERSION =
  "ai_visibility_recommendation_adjudicator_prompt_v1";

export function buildAdjudicatorSystemInstructions() {
  const defs = GOVERNED_RECOMMENDATION_ROLES.map(
    (r) => `- ${r}: ${TAXONOMY_DEFINITIONS[r]}`
  ).join("\n");
  const rules = TAXONOMY_DECISION_RULES.map((r) => `- ${r}`).join("\n");

  return [
    "You are classifying the role of one hotel brand/operator/entity within one AI response.",
    "Choose exactly one label from Dealality's governed taxonomy.",
    "Base the decision only on the supplied evidence.",
    "Do not infer market truth or preference beyond the response.",
    "Do not invent evidence. Do not return freeform user advice.",
    "",
    "GOVERNED TAXONOMY (exactly one):",
    defs,
    "",
    "DECISION RULES:",
    rules,
    "",
    "OUTPUT: Return ONLY a single JSON object with keys:",
    'selectedRole (string enum), evidenceRefs (string[]), taxonomyRule (string), ambiguityResolved (string).',
    "evidenceRefs MUST use only these ids when present: entity_local_span, section_heading, section_intro, structural_evidence, cue_facts.",
    "No markdown fences. No extra keys. No confidence score.",
  ].join("\n");
}

/**
 * Bound adjudicator payload — no full monitoring dump, no leading GT hint.
 */
export function buildAdjudicatorUserPayload(input) {
  const {
    entityName,
    entityLocalEvidence,
    sectionHeading,
    sectionIntro,
    structuralEvidence,
    cueFacts,
    plausibleRoles,
    ambiguityReasons,
  } = input;

  return {
    contractVersion: ADJUDICATOR_PROMPT_VERSION,
    entityName: String(entityName || ""),
    entityLocalEvidence: String(entityLocalEvidence || "").slice(0, 1200),
    sectionHeading: String(sectionHeading || "").slice(0, 240),
    sectionIntro: String(sectionIntro || "").slice(0, 400),
    structuralEvidence: structuralEvidence || {},
    cueFacts: cueFacts || {},
    plausibleRoles: Array.isArray(plausibleRoles) ? plausibleRoles : [],
    ambiguityReasons: Array.isArray(ambiguityReasons) ? ambiguityReasons : [],
    allowedRoles: [...GOVERNED_RECOMMENDATION_ROLES],
    note: "Deterministic classifier prediction is intentionally omitted to avoid anchoring.",
  };
}

export function buildAdjudicatorPromptText(payload) {
  return [
    buildAdjudicatorSystemInstructions(),
    "",
    "Classify the entity role using ONLY this evidence package:",
    JSON.stringify(payload, null, 2),
    "",
    "Respond with ONLY one JSON object. No markdown. No headings. No prose before or after.",
    'Example shape: {"selectedRole":"associated_option","evidenceRefs":["entity_local_span","cue_facts"],"taxonomyRule":"RULE2","ambiguityResolved":"consideration membership without lead"}',
  ].join("\n");
}
