/**
 * Golden Set Review Packet — deterministic copyable context for human review.
 * External ChatGPT assistance is NOT ground truth. No provider calls.
 */

import {
  RECOMMENDATION_STATUS_TAXONOMY,
  QUESTION_STATUS_TAXONOMY,
  CITATION_ASSOCIATION_TAXONOMY,
  loadCandidateDocument,
  loadCaseEvidence,
} from "./golden-set-human-review.js";

export const REVIEW_PACKET_VERSION = "ai_intelligence_golden_set_review_packet_v1";
export const COPY_NEXT_MAX = 5;

/** Plain-language help for governed enums (no new statuses). */
export const TAXONOMY_HELP = Object.freeze({
  entityPresent: {
    YES: "The evaluated brand/entity actually appears in the response.",
    NO: "The evaluated entity does not appear in the response (MISSING).",
  },
  recommendationStatus: {
    first_recommendation:
      "Entity is the first/leading positive recommendation in the governed interpretation of the response.",
    ranked_recommendation:
      "Entity appears as a positively ranked option (e.g. 2nd/3rd in an explicit list), not necessarily first.",
    explicit_recommendation:
      "Entity is positively recommended in explicit recommendation language (not only described).",
    associated_option:
      "Entity is presented as an associated/example option rather than a primary recommendation for the decision.",
    comparator: "Entity is used mainly as a comparison reference, not as the recommended choice.",
    discussed:
      "Entity is mentioned or described but not positively recommended as an option to select.",
    passing_mention: "Brief/incidental mention without recommendation weight.",
    negative_or_qualified:
      "Entity is discouraged, excluded, rejected, or negatively/qualified positioned.",
    source_only: "Entity appears only via source/citation framing, not as a recommendation.",
    no_mention: "Entity is not present in the response.",
  },
  firstRecommendation: {
    YES: "Entity is the first/leading recommendation under governed ranking rules.",
    NO: "Entity is not the first recommendation.",
    NOT_APPLICABLE: "First-recommendation judgment does not apply (e.g. entity absent).",
  },
  questionStatus: {
    PRESENT: "Entity appears in the answer (presence without requiring recommendation).",
    MISSING: "Entity does not appear in the answer.",
    RECOMMENDED: "Entity is positively recommended.",
    FIRST_RECOMMENDED: "Entity is the first/leading recommendation.",
    DISCUSSION_ONLY: "Entity is mentioned/discussed but not positively recommended.",
    NEGATIVE_OR_NOT_RECOMMENDED: "Entity is discouraged or explicitly not recommended.",
    NOT_APPLICABLE: "Question-status label does not apply for this evaluation.",
  },
  citationAssociation: {
    ASSOCIATED:
      "A citation/evidence source is associated with this entity/recommendation under governed citation rules.",
    NOT_ASSOCIATED: "Citations may exist, but none are associated with this entity under the rules.",
    UNKNOWN: "Stored evidence does not support a determination.",
    NOT_APPLICABLE: "Citation association is not applicable for this case.",
  },
  howToLabel: [
    {
      title: "ENTITY PRESENT",
      text: "The evaluated brand/entity actually appears in the response.",
    },
    {
      title: "RECOMMENDED (positive roles)",
      text: "The response positively suggests the entity as an option (first_recommendation, ranked_recommendation, or explicit_recommendation).",
    },
    {
      title: "FIRST RECOMMENDED",
      text: "The entity is the first/leading recommendation according to the governed response interpretation.",
    },
    {
      title: "DISCUSSION ONLY",
      text: "The entity is mentioned or described but not positively recommended (discussed / related non-positive roles).",
    },
    {
      title: "NEGATIVE / NOT RECOMMENDED",
      text: "The response explicitly discourages, excludes, rejects, or negatively positions the entity.",
    },
    {
      title: "MISSING",
      text: "The evaluated entity does not appear in the response.",
    },
    {
      title: "CITATION ASSOCIATED",
      text: "A citation/evidence source is actually associated with the entity/recommendation under governed citation rules.",
    },
    {
      title: "UNKNOWN / NOT APPLICABLE",
      text: "The stored evidence does not support a determination, or the field does not apply.",
    },
  ],
});

export function normalizeCitationSuggestion(raw) {
  if (!raw) return null;
  if (raw === "citations_present_unreviewed") return "UNKNOWN";
  if (CITATION_ASSOCIATION_TAXONOMY.includes(raw)) return raw;
  return "UNKNOWN";
}

/**
 * Map SYSTEM_SUGGESTION into comparable human-label shape.
 */
export function systemSuggestionAsLabels(candidate) {
  const sug = candidate?.systemSuggestion || {};
  const entityName = candidate?.candidateEntity || null;
  return {
    entityPresent: entityName ? true : null,
    canonicalEntityId: candidate?.canonicalEntityId || null,
    canonicalEntityName: entityName,
    recommendationStatus: sug.expectedRecommendationClass || null,
    firstRecommendation:
      sug.expectedFirstRecommendation === true
        ? true
        : sug.expectedFirstRecommendation === false
          ? false
          : null,
    questionStatus: sug.expectedQuestionStatus || null,
    citationAssociation: normalizeCitationSuggestion(sug.expectedCitationAssociation),
    parentVsBrandNote: null,
  };
}

export function diffHumanVsSystem(humanLabels, systemLabels) {
  const fields = [
    "entityPresent",
    "recommendationStatus",
    "firstRecommendation",
    "questionStatus",
    "citationAssociation",
    "canonicalEntityId",
    "canonicalEntityName",
  ];
  const differences = [];
  for (const f of fields) {
    const h = humanLabels?.[f];
    const s = systemLabels?.[f];
    const hn = h === undefined ? null : h;
    const sn = s === undefined ? null : s;
    if (String(hn) !== String(sn)) {
      differences.push({ field: f, system: sn, human: hn });
    }
  }
  return {
    matches: differences.length === 0,
    suggestedAction: differences.length === 0 ? "CONFIRM" : "CORRECT",
    differences,
    fieldsChanged: differences.map((d) => d.field),
  };
}

function fmt(v) {
  if (v === true) return "YES / true";
  if (v === false) return "NO / false";
  if (v == null || v === "") return "—";
  return String(v);
}

function listBlock(title, values, helpMap) {
  const lines = [`${title}:`];
  for (const v of values) {
    const help = helpMap?.[v];
    lines.push(help ? `  - ${v}: ${help}` : `  - ${v}`);
  }
  return lines.join("\n");
}

/**
 * Build standardized Review Packet text for one case.
 */
export async function buildReviewPacket(caseId, options = {}) {
  const doc = loadCandidateDocument();
  const candidate = (doc.cases || []).find((c) => c.caseId === caseId);
  if (!candidate) {
    const err = new Error("CASE_NOT_FOUND");
    err.code = "CASE_NOT_FOUND";
    throw err;
  }
  let promptText = candidate.promptText || "";
  let responseText = candidate.rawResponseExcerpt || "";
  let fullAvailable = false;
  if (options.expand !== false) {
    try {
      const detail = await loadCaseEvidence(caseId, options);
      promptText = detail.promptText || promptText;
      responseText = detail.rawResponseText || responseText;
      fullAvailable = !!detail.evidenceRef?.fullAvailable;
    } catch {
      // keep excerpt
    }
  }
  const sug = candidate.systemSuggestion || {};
  const entity = candidate.candidateEntity || "UNKNOWN_ENTITY";

  const packet = `--------------------------------------------------
DEALALITY GOLDEN SET REVIEW CASE

CASE ID:
${candidate.caseId}

SUBJECT ENTITY:
${entity}

CANONICAL ENTITY ID:
${candidate.canonicalEntityId || "—"}

PROVIDER:
${candidate.provider || "—"}

MODEL:
${candidate.model || "—"}

GEOGRAPHY:
${candidate.geography || "—"}

LANGUAGE:
${candidate.language || "—"}

PROMPT FAMILY:
${candidate.promptFamily || candidate.promptIntentTerritory || "—"}

PROMPT:
${promptText || "—"}

STORED AI RESPONSE:
${responseText || "—"}
${fullAvailable ? "" : "(Note: excerpt or truncated stored response — expand in Dealality Review UI if needed.)"}

ENTITY BEING EVALUATED:
${entity}

SYSTEM SUGGESTION — NOT GROUND TRUTH:
Entity Present: ${entity ? "YES (entity nominated for evaluation)" : "—"}
Recommendation Status: ${fmt(sug.expectedRecommendationClass)}
First Recommendation: ${fmt(sug.expectedFirstRecommendation)}
Question Status: ${fmt(sug.expectedQuestionStatus)}
Citation Association: ${fmt(normalizeCitationSuggestion(sug.expectedCitationAssociation))}
Parent/Brand Classification: —

AVAILABLE HUMAN LABEL OPTIONS:

Entity Present:
  - YES: ${TAXONOMY_HELP.entityPresent.YES}
  - NO: ${TAXONOMY_HELP.entityPresent.NO}

${listBlock("Recommendation Status", RECOMMENDATION_STATUS_TAXONOMY, TAXONOMY_HELP.recommendationStatus)}

${listBlock("First Recommendation", ["YES", "NO", "NOT_APPLICABLE"], TAXONOMY_HELP.firstRecommendation)}

${listBlock("Question Status", QUESTION_STATUS_TAXONOMY, TAXONOMY_HELP.questionStatus)}

${listBlock("Citation Association", CITATION_ASSOCIATION_TAXONOMY, TAXONOMY_HELP.citationAssociation)}

Parent vs Brand:
  - Free-text note when parent company vs brand identity is ambiguous
  - Leave blank when not relevant

REVIEW QUESTION:

Based ONLY on what the stored AI response actually says about
${entity}, what should the human labels be?

Please return:

Entity Present:
Canonical Entity:
Recommendation Status:
First Recommendation:
Question Status:
Citation Association:
Parent vs Brand Note:
Reason:

IMPORTANT:
External review assistance is NOT ground truth.
The human reviewer must confirm/correct labels in Dealality.
Do not invent labels beyond the available options above.
--------------------------------------------------
`;

  return {
    version: REVIEW_PACKET_VERSION,
    caseId: candidate.caseId,
    entity,
    packetText: packet,
    responseOnlyText: responseText || "",
    promptText: promptText || "",
    systemSuggestion: sug,
    systemAsLabels: systemSuggestionAsLabels(candidate),
    enums: {
      entityPresent: ["YES", "NO"],
      recommendationStatus: [...RECOMMENDATION_STATUS_TAXONOMY],
      firstRecommendation: ["YES", "NO", "NOT_APPLICABLE"],
      questionStatus: [...QUESTION_STATUS_TAXONOMY],
      citationAssociation: [...CITATION_ASSOCIATION_TAXONOMY],
    },
    taxonomyHelp: TAXONOMY_HELP,
    fullContextAvailable: fullAvailable,
  };
}

/**
 * Build up to COPY_NEXT_MAX packets for unreviewed queue head.
 */
export async function buildNextReviewPackets(caseIds, options = {}) {
  const ids = (caseIds || []).slice(0, COPY_NEXT_MAX);
  const packets = [];
  for (const id of ids) {
    packets.push(await buildReviewPacket(id, options));
  }
  const combined =
    packets.map((p) => p.packetText).join("\n\n") +
    `\n\n--- BATCH INSTRUCTIONS ---\nFor each case above, return a table with:\nCase ID\nEntity Present\nRecommendation Status\nFirst Recommendation\nQuestion Status\nCitation Association\nParent/Brand Note\nReason\n\nAssisted answers are NOT ground truth. Labels must be entered in Dealality by the human reviewer.\n`;
  return {
    version: REVIEW_PACKET_VERSION,
    max: COPY_NEXT_MAX,
    count: packets.length,
    caseIds: ids,
    packets,
    combinedText: combined,
  };
}

export function buildResponseOnlyPacket(caseId, responseText) {
  return {
    caseId,
    text: `DEALALITY GOLDEN SET — RESPONSE ONLY\nCASE ID: ${caseId}\n\n${responseText || ""}`,
  };
}
