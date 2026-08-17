/**
 * AI_SIGNAL_RECOMMENDED binary classifier v1
 *
 * Aligned to ai_signal_recommended_definition_lock_v1.
 * Uses evidence extraction from recommendation-evidence-v4_1, then applies a
 * binary decision (not the old 10-class production gate).
 *
 * Presence fail-closed: if entityPresent=false → Recommended FALSE.
 */

import {
  extractEntityLocalEvidence,
  buildTypedSections,
  RECOMMENDATION_EVIDENCE_VERSION,
} from "./recommendation-evidence-v4_1.js";
import {
  decideRecommendationRoleFromEvidence,
} from "./recommendation-classifier-v4_1.js";
import {
  RECOMMENDED_DEFINITION_LOCK_VERSION,
  isBrandDecisionPromptFamily,
} from "./signal-architecture/recommended-signal-definition.js";
import { POSITIVE_RECOMMENDATION_ROLES } from "./metrics.js";

export const RECOMMENDED_BINARY_CLASSIFIER_VERSION =
  "ai_visibility_recommended_binary_v1";

export const RECOMMENDED_BINARY_RULE_VERSION =
  "ai_visibility_recommended_binary_rules_v1_3";

export const RECOMMENDED_REGRESSION_SUITE_VERSION =
  "ai_visibility_recommended_binary_regression_v1";

const POSITIVE_ROLE_SET = new Set(POSITIVE_RECOMMENDATION_ROLES);

const AFFIRMATIVE_CUE_RE =
  /\b(recommend(?:ed|ing)?|we\s+recommend|i\s+(?:would\s+)?recommend|recomiendo|recomendad[oa]s?|should\s+consider|may\s+(?:also\s+)?consider|worth\s+considering|commonly\s+considered|most\s+(?:commonly\s+)?considered|brands?\s+commonly\s+considered|shortlist(?:ed)?|lista\s+corta|best\s+options?|top\s+options?|strong\s+(?:options?|fit|candidate|choice|choices)|relevant\s+options?|good\s+(?:fit|choice|option)|suitable\s+(?:option|brand|brands?)|strong\s+fit|good\s+fit|prioritize|primary\s+(?:option|candidate)|leading\s+candidate|could\s+be\s+a\s+strong\s+option|could\s+work\s+well|would\s+be\s+a\s+good\s+option|opci[oó]n\s+(?:fuerte|s[oó]lida|preferida|adecuada)|marcas?\s+a\s+considerar|opciones\s+(?:recomendadas|incluyen|fuertes)|conviene\s+considerar|issue\s+an\s+rfp\s+to|solicit\s+proposals?|brands?\s+to\s+consider|options?\s+to\s+consider|options?\s+include|top\s+pick)\b/i;

const QUALIFIED_AFFIRMATIVE_RE =
  /\b(could\s+be\s+a\s+strong\s+option|good\s+option\s+if|could\s+work\s+(?:well\s+)?(?:if|where)|consider\s+.{0,40}\s+if|strong\s+option\s+if|worth\s+considering\s+for|may\s+work\s+well|if\s+.{0,60}(priority|independence|flexibility|design))\b/i;

const NEGATIVE_EXCLUSION_RE =
  /\b(not\s+recommend(?:ed)?|not\s+suitable|would\s+not\s+(?:be\s+)?suitable|poor\s+fit|would\s+not\s+fit|less\s+suitable|weaker\s+fit|avoid|too\s+restrictive|not\s+appropriate|should\s+not\s+be\s+considered|not\s+ideal|no\s+recomend|poco\s+adecuado|evitar|menos\s+adecuado)\b/i;

const COMPARATOR_ONLY_RE =
  /\b(competes?\s+with|compared\s+(?:to|with)|versus|vs\.?|alternative\s+to|unlike|similar\s+to|stronger\s+than|weaker\s+than|frente\s+a|comparad[oa]\s+con)\b/i;

const DESCRIPTIVE_ONLY_RE =
  /\b(is\s+part\s+of|operates?\s+(?:several|many|hotels)|has\s+(?:a\s+)?presence|expanded\s+in|launched|portfolio\s+includes|family\s+of\s+brands?|brand\s+overview|market\s+participants?)\b/i;

const DECISION_SECTION_TYPES = new Set([
  "LEAD_RECOMMENDATION_SECTION",
  "RANKED_RECOMMENDATION_SECTION",
  "RECOMMENDATION_SET_SECTION",
  "CONSIDERATION_SET_SECTION",
]);

const SCOPE_HEADING_RE =
  /\b(recommended\s+brands?|brands?\s+to\s+consider|top\s+options?|best\s+options?|shortlist|most\s+relevant\s+brands?|recommended\s+shortlist|suitable\s+brands?|strongest\s+options?|opciones\s+recomendadas|marcas?\s+a\s+considerar|lista\s+corta|relevant\s+options?|conversion[\s-]?friendly\s+brands?|key\s+brands?)\b/i;

/**
 * Detect coordinated multi-entity recommendation in a sentence.
 * Affirmative cue must appear before (or tightly around) the entity cluster.
 */
export function hasCoordinatedRecommendationScope(sentence, entityStartInSentence) {
  const s = String(sentence || "");
  const before = s.slice(0, Math.max(0, entityStartInSentence));
  const after = s.slice(Math.max(0, entityStartInSentence));
  if (!AFFIRMATIVE_CUE_RE.test(before) && !AFFIRMATIVE_CUE_RE.test(s.slice(0, entityStartInSentence + 40))) {
    // Also allow cue immediately after opener like "The best options are X, Y, and Z"
    if (!/\b(are|include|:)\s*$/i.test(before.trim()) || !AFFIRMATIVE_CUE_RE.test(before)) {
      if (!AFFIRMATIVE_CUE_RE.test(before)) return false;
    }
  }
  // Multi-entity structure: commas / and / slash near entity
  const window = s.slice(
    Math.max(0, entityStartInSentence - 80),
    Math.min(s.length, entityStartInSentence + 120)
  );
  return /,\s+|\s+\/\s+|\band\b|\by\b/i.test(window);
}

function lineContaining(text, start) {
  const source = String(text || "");
  const lineStart = source.lastIndexOf("\n", start - 1) + 1;
  const lineEndIdx = source.indexOf("\n", start);
  const lineEnd = lineEndIdx === -1 ? source.length : lineEndIdx;
  return {
    lineStart,
    lineEnd,
    line: source.slice(lineStart, lineEnd),
  };
}

function sentenceContaining(text, start, end) {
  const source = String(text || "");
  const before = source.slice(0, start);
  const after = source.slice(end);
  const sentStartRel = Math.max(
    before.lastIndexOf(". "),
    before.lastIndexOf(".\n"),
    before.lastIndexOf("! "),
    before.lastIndexOf("? "),
    before.lastIndexOf("\n\n")
  );
  const sentStart = sentStartRel === -1 ? Math.max(0, start - 200) : sentStartRel + 2;
  let sentEndRel = after.search(/[.!?](?:\s|$)/);
  if (sentEndRel === -1) sentEndRel = Math.min(after.length, 200);
  else sentEndRel += 1;
  const sentenceStart = Math.max(0, Math.min(sentStart, start));
  const sentenceEnd = Math.min(source.length, end + sentEndRel);
  return {
    sentenceStart,
    sentenceEnd,
    sentence: source.slice(sentenceStart, sentenceEnd),
  };
}

/**
 * Walk backward for recommendation-scope heading inheritance.
 */
export function findInheritedRecommendationScope(text, position, typedSections) {
  const source = String(text || "");
  const pos = Number(position) || 0;

  // Typed sections first
  const sections = typedSections || [];
  for (const sec of sections) {
    const s0 = Number(sec.start ?? sec.sectionStart ?? 0);
    const s1 = Number(sec.end ?? sec.sectionEnd ?? source.length);
    if (pos < s0 || pos >= s1) continue;
    if (DECISION_SECTION_TYPES.has(sec.type || sec.sectionType)) {
      return {
        inherited: true,
        source: "typed_section",
        sectionType: sec.type || sec.sectionType,
      };
    }
  }

  // Heading scan: previous markdown/heading lines
  const before = source.slice(0, pos);
  const chunks = before.split(/\n/);
  for (let i = chunks.length - 1; i >= 0; i -= 1) {
    const raw = chunks[i].replace(/^#{1,6}\s*/, "").replace(/^\*\*|^\*/, "").trim();
    if (!raw) continue;
    // Termination: new peer heading that is NOT recommendation-scoped
    if (/^#{1,6}\s+\S|^[A-Z][A-Za-z0-9 ,/&-]{3,80}$/.test(chunks[i].trim()) || /^\*\*[^*].+\*\*$/.test(chunks[i].trim())) {
      if (SCOPE_HEADING_RE.test(raw)) {
        return { inherited: true, source: "heading", heading: raw };
      }
      // Peer heading without scope → stop
      if (
        /^(sources?|references?|citations?|appendix|conclusi[oó]n|summary|overview|background|what\s+is|methodology)\b/i.test(
          raw
        )
      ) {
        break;
      }
      // Continue scanning past non-heading body lines; stop on clear new H2/H3 without scope
      if (/^#{1,3}\s+/.test(chunks[i]) && !SCOPE_HEADING_RE.test(raw)) {
        break;
      }
    }
    if (SCOPE_HEADING_RE.test(raw)) {
      return { inherited: true, source: "heading", heading: raw };
    }
    // Limit lookback
    if (chunks.length - i > 40) break;
  }

  return { inherited: false };
}

/**
 * Table inheritance: recommendation column / title near entity line.
 */
export function hasTableRecommendationScope(line, nearbyText) {
  const ctx = `${nearbyText || ""}\n${line || ""}`;
  if (/\|\s*recommended\s+brand/i.test(ctx) || /\|\s*brand\s+to\s+consider/i.test(ctx)) {
    return true;
  }
  if (/\brecommended\s+brand|\bbrands?\s+to\s+consider/i.test(ctx) && /\|/.test(line || "")) {
    return true;
  }
  // Generic brand | parent company tables do NOT count
  if (/\|\s*parent\s+company/i.test(ctx) && !/\brecommended|\bconsider|\bshortlist/i.test(ctx)) {
    return false;
  }
  return false;
}

/**
 * Classify AI_SIGNAL_RECOMMENDED for one entity mention span.
 *
 * @param {{
 *  text: string,
 *  start?: number,
 *  end?: number,
 *  mentionPosition?: number,
 *  rawMention?: string,
 *  canonicalEntityName?: string,
 *  canonicalEntityId?: string,
 *  entityPresent?: boolean,
 *  promptFamily?: string|null,
 *  typedSections?: object[],
 * }} args
 */
export function classifyRecommendedBinary(args = {}) {
  const entityPresent = args.entityPresent !== false;
  if (!entityPresent) {
    return {
      value: false,
      reason: "presence_false",
      classifierVersion: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
      ruleVersion: RECOMMENDED_BINARY_RULE_VERSION,
      definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
      evidenceVersion: RECOMMENDATION_EVIDENCE_VERSION,
    };
  }

  const text = String(args.text || "");
  const start = Number(args.start ?? args.mentionPosition ?? 0);
  const end = Number(
    args.end ?? start + String(args.rawMention || args.canonicalEntityName || "").length
  );
  const typedSections = args.typedSections || buildTypedSections(text);
  const promptFamily = args.promptFamily || null;
  const decisionPrompt = isBrandDecisionPromptFamily(promptFamily);

  const evidence = extractEntityLocalEvidence({
    text,
    start,
    end,
    rawMention: args.rawMention,
    canonicalEntityId: args.canonicalEntityId,
    canonicalEntityName: args.canonicalEntityName,
    typedSections,
  });

  const ev = evidence.recommendationEvidence || {};
  const st = evidence.structure || {};
  const { line } = lineContaining(text, start);
  const { sentence, sentenceStart } = sentenceContaining(text, start, end);
  const entityStartInSentence = Math.max(0, start - sentenceStart);
  const local = `${line}\n${sentence}`;

  // --- Overrides (FALSE) ---
  if (ev.directNegativeCue || NEGATIVE_EXCLUSION_RE.test(local)) {
    return {
      value: false,
      reason: "negative_exclusion_override",
      classifierVersion: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
      ruleVersion: RECOMMENDED_BINARY_RULE_VERSION,
      definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
      evidence,
    };
  }

  // Precision-safe floor: retain v4.1 positive recommendation roles as Recommended TRUE
  const v41 = decideRecommendationRoleFromEvidence(evidence, { entityPresent: true });
  if (POSITIVE_ROLE_SET.has(v41.role)) {
    return {
      value: true,
      reason: `v41_positive_role:${v41.role}`,
      classifierVersion: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
      ruleVersion: RECOMMENDED_BINARY_RULE_VERSION,
      definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
      evidence,
      internalRole: v41.role,
    };
  }

  const comparatorHit =
    ev.comparatorCue || COMPARATOR_ONLY_RE.test(local);
  const affirmativeLocal =
    ev.directPositiveCue ||
    ev.sectionPositiveCue ||
    ev.leadCue ||
    AFFIRMATIVE_CUE_RE.test(local) ||
    QUALIFIED_AFFIRMATIVE_RE.test(local);

  if (comparatorHit && !affirmativeLocal) {
    return {
      value: false,
      reason: "comparator_only_override",
      classifierVersion: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
      ruleVersion: RECOMMENDED_BINARY_RULE_VERSION,
      definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
      evidence,
    };
  }

  // --- TRUE paths (precision-first extensions beyond v4.1 positives) ---
  if (ev.leadCue || ev.directPositiveCue || ev.sectionPositiveCue) {
    return {
      value: true,
      reason: ev.leadCue
        ? "explicit_lead_cue"
        : ev.sectionPositiveCue
          ? "section_recommendation_set"
          : "explicit_recommendation_language",
      classifierVersion: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
      ruleVersion: RECOMMENDED_BINARY_RULE_VERSION,
      definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
      evidence,
    };
  }

  if (
    (st.confirmedRankStructure || evidence.confirmedRankStructure) &&
    (st.orderedPosition != null || st.tableRank != null || evidence.rankPosition != null)
  ) {
    return {
      value: true,
      reason: "ranked_or_ordered_recommendation_structure",
      classifierVersion: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
      ruleVersion: RECOMMENDED_BINARY_RULE_VERSION,
      definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
      evidence,
    };
  }

  if (QUALIFIED_AFFIRMATIVE_RE.test(local) || QUALIFIED_AFFIRMATIVE_RE.test(sentence)) {
    return {
      value: true,
      reason: "qualified_affirmative",
      classifierVersion: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
      ruleVersion: RECOMMENDED_BINARY_RULE_VERSION,
      definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
      evidence,
    };
  }

  // Entity-local affirmative: cue must appear on the same line, or before the
  // entity in the same sentence (prevents distant "consider" false positives).
  const sentenceBeforeEntity = sentence.slice(0, entityStartInSentence + 1);
  const sentenceNearEntity = sentence.slice(
    Math.max(0, entityStartInSentence - 60),
    Math.min(sentence.length, entityStartInSentence + String(args.rawMention || "").length + 40)
  );
  if (
    AFFIRMATIVE_CUE_RE.test(line) ||
    AFFIRMATIVE_CUE_RE.test(sentenceBeforeEntity) ||
    QUALIFIED_AFFIRMATIVE_RE.test(sentenceNearEntity)
  ) {
    return {
      value: true,
      reason: "entity_local_affirmative_cue",
      classifierVersion: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
      ruleVersion: RECOMMENDED_BINARY_RULE_VERSION,
      definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
      evidence,
    };
  }

  const bulletOrRow =
    /^\s*[-*•]\s+/.test(line) || /\|/.test(line) || /^\s*\d+[\).]\s+/.test(line);
  const priorClose = text.slice(Math.max(0, start - 220), start);
  const hasCloseScopeHeading = SCOPE_HEADING_RE.test(priorClose);

  // List/table inheritance only under an explicit recommendation-scope heading nearby
  if (bulletOrRow && hasCloseScopeHeading && !DESCRIPTIVE_ONLY_RE.test(line)) {
    return {
      value: true,
      reason: "scoped_list_inheritance",
      classifierVersion: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
      ruleVersion: RECOMMENDED_BINARY_RULE_VERSION,
      definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
      evidence,
    };
  }

  const nearby = text.slice(Math.max(0, start - 300), Math.min(text.length, end + 80));
  if (hasTableRecommendationScope(line, nearby)) {
    return {
      value: true,
      reason: "table_recommendation_inheritance",
      classifierVersion: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
      ruleVersion: RECOMMENDED_BINARY_RULE_VERSION,
      definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
      evidence,
    };
  }

  // Bare ranked list answering a decision prompt (1./2./3.) — require decision prompt
  // and a close affirmative frame or scope heading (not mere parent-company bullets)
  if (
    decisionPrompt === true &&
    /^\s*\d+[\).]\s+/.test(line) &&
    (hasCloseScopeHeading || AFFIRMATIVE_CUE_RE.test(priorClose)) &&
    !DESCRIPTIVE_ONLY_RE.test(sentence)
  ) {
    return {
      value: true,
      reason: "decision_prompt_ranked_list",
      classifierVersion: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
      ruleVersion: RECOMMENDED_BINARY_RULE_VERSION,
      definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
      evidence,
    };
  }

  // Coordinated multi-entity with affirmative cue before the cluster
  if (hasCoordinatedRecommendationScope(sentence, entityStartInSentence)) {
    return {
      value: true,
      reason: "coordinated_multi_entity_recommendation",
      classifierVersion: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
      ruleVersion: RECOMMENDED_BINARY_RULE_VERSION,
      definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
      evidence,
    };
  }

  if (DESCRIPTIVE_ONLY_RE.test(local) || DESCRIPTIVE_ONLY_RE.test(sentence)) {
    return {
      value: false,
      reason: "descriptive_or_market_context_only",
      classifierVersion: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
      ruleVersion: RECOMMENDED_BINARY_RULE_VERSION,
      definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
      evidence,
    };
  }

  return {
    value: false,
    reason: "no_affirmative_decision_set_evidence",
    classifierVersion: RECOMMENDED_BINARY_CLASSIFIER_VERSION,
    ruleVersion: RECOMMENDED_BINARY_RULE_VERSION,
    definitionVersion: RECOMMENDED_DEFINITION_LOCK_VERSION,
    evidence,
  };
}

/**
 * Convenience: boolean only.
 */
export function isRecommendedBinary(args) {
  return classifyRecommendedBinary(args).value === true;
}
