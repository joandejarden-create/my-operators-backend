/**
 * Blinded Recommended binary semantic adjudicator (feasibility study).
 * Locked definition: ai_signal_recommended_definition_lock_v1
 * Does not change deterministic classifier rules.
 */

export const RECOMMENDED_SEMANTIC_ADJUDICATOR_VERSION =
  "ai_visibility_recommended_semantic_adjudicator_v1";

export const REASON_CODES = Object.freeze([
  "EXPLICIT_RECOMMENDATION",
  "CONSIDERATION_SET",
  "SHORTLIST",
  "LIST_SCOPE",
  "TABLE_SCOPE",
  "QUALIFIED_AFFIRMATIVE",
  "DESCRIPTIVE_ONLY",
  "COMPARATOR_ONLY",
  "NEGATIVE_EXCLUSION",
  "UNRELATED_CONTEXT",
  "INSUFFICIENT_EVIDENCE",
]);

export function buildRecommendedAdjudicatorSystemInstructions() {
  return [
    "You adjudicate ONE binary signal for Dealality AI Visibility.",
    "Question: Does the supplied evidence affirmatively place THIS specific canonical entity into the actionable decision/consideration set for the user's stated hotel decision?",
    "",
    "Return RECOMMENDED only when evidence shows affirmative decision-set placement:",
    "- explicit recommendation",
    "- shortlist / brands to consider / options include",
    "- ranked or bulleted recommendation list clearly answering the prompt",
    "- table under a recommendation framing",
    "- qualified affirmative (could be a strong option if...)",
    "",
    "Return NOT_RECOMMENDED when the entity is only:",
    "- descriptive / market context",
    "- comparator / competitor reference",
    "- parent/sibling portfolio listing without decision-set placement",
    "- historical example",
    "- negative exclusion",
    "- unrelated passing mention",
    "- insufficient evidence in the supplied package",
    "",
    "Rules:",
    "- Use ONLY the supplied evidence package. Do not invent facts.",
    "- Do not identify a different entity. The canonical entity is given.",
    "- evidenceText MUST be copied verbatim from the supplied evidence (or empty if insufficient).",
    "- If evidenceText would be empty for RECOMMENDED, you MUST return NOT_RECOMMENDED with reasonCode INSUFFICIENT_EVIDENCE.",
    "- No confidence score. No probabilities. No free-form advice.",
    "",
    "OUTPUT: Return ONLY one JSON object with keys:",
    'decision ("RECOMMENDED" | "NOT_RECOMMENDED"),',
    "evidenceText (string),",
    `reasonCode (one of: ${REASON_CODES.join(", ")}).`,
    "No markdown fences. No extra keys.",
  ].join("\n");
}

/**
 * Blinded evidence package — no GT, no classifier prediction, no error category.
 */
export function buildRecommendedAdjudicatorPayload(input = {}) {
  return {
    contractVersion: RECOMMENDED_SEMANTIC_ADJUDICATOR_VERSION,
    PROMPT: String(input.prompt || "").slice(0, 600),
    PROMPT_INTENT: String(input.promptIntent || "").slice(0, 120),
    CANONICAL_ENTITY: String(input.canonicalEntity || ""),
    ENTITY_LOCAL_TEXT: String(input.entityLocalText || "").slice(0, 1400),
    SECTION_HEADING: String(input.sectionHeading || "").slice(0, 240),
    SECTION_INTRO: String(input.sectionIntro || "").slice(0, 400),
    LIST_OR_TABLE_CONTEXT: String(input.listOrTableContext || "").slice(0, 800),
    NEARBY_SENTENCES: String(input.nearbySentences || "").slice(0, 1000),
    DETERMINISTIC_FLAGS: {
      explicitRecommendationCue: Boolean(input.flags?.explicitRecommendationCue),
      considerationCue: Boolean(input.flags?.considerationCue),
      comparatorCue: Boolean(input.flags?.comparatorCue),
      negativeCue: Boolean(input.flags?.negativeCue),
      listContext: Boolean(input.flags?.listContext),
      tableContext: Boolean(input.flags?.tableContext),
      parentContext: Boolean(input.flags?.parentContext),
    },
  };
}

export function buildRecommendedAdjudicatorPromptText(payload) {
  return [
    buildRecommendedAdjudicatorSystemInstructions(),
    "",
    "Adjudicate using ONLY this evidence package:",
    JSON.stringify(payload, null, 2),
    "",
    "Respond with ONLY one JSON object.",
    'Example: {"decision":"NOT_RECOMMENDED","evidenceText":"Autograph Collection is part of Marriott.","reasonCode":"DESCRIPTIVE_ONLY"}',
  ].join("\n");
}

export function parseRecommendedAdjudicatorOutput(text) {
  const raw = String(text || "").trim();
  let jsonText = raw;
  const fence = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fence) jsonText = fence[1].trim();
  const start = jsonText.indexOf("{");
  const end = jsonText.lastIndexOf("}");
  if (start >= 0 && end > start) jsonText = jsonText.slice(start, end + 1);
  let obj;
  try {
    obj = JSON.parse(jsonText);
  } catch {
    return {
      ok: false,
      decision: "NOT_RECOMMENDED",
      evidenceText: "",
      reasonCode: "INSUFFICIENT_EVIDENCE",
      error: "JSON_PARSE_FAILED",
    };
  }
  const decision =
    String(obj.decision || "").toUpperCase() === "RECOMMENDED"
      ? "RECOMMENDED"
      : "NOT_RECOMMENDED";
  const reasonCode = REASON_CODES.includes(String(obj.reasonCode || ""))
    ? String(obj.reasonCode)
    : "INSUFFICIENT_EVIDENCE";
  let evidenceText = String(obj.evidenceText || "").trim();
  if (decision === "RECOMMENDED" && !evidenceText) {
    return {
      ok: true,
      decision: "NOT_RECOMMENDED",
      evidenceText: "",
      reasonCode: "INSUFFICIENT_EVIDENCE",
      failClosed: "EMPTY_EVIDENCE_ON_TRUE",
    };
  }
  return {
    ok: true,
    decision,
    evidenceText: evidenceText.slice(0, 800),
    reasonCode,
  };
}
