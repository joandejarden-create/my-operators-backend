/**
 * Deterministic mention extraction for AI Visibility.
 * Uses entity resolver v2 + recommendation classifier v4.1
 * (entity×response evidence v4.1 → mutually exclusive decision tree).
 */

import { randomUUID } from "crypto";
import { RESOLVER_VERSION, findEntitySpans } from "./normalize-entities.js";
import {
  RECOMMENDATION_CLASSIFIER_VERSION,
  classifyEntityFromMentionSpans,
  assignFirstRecommendationAcrossMentionsV4_1,
  classifyMentionRoleV4_1,
} from "./recommendation-classifier-v4_1.js";
import { buildTypedSections } from "./recommendation-evidence-v4_1.js";

function newMentionId() {
  return `men_${randomUUID().replace(/-/g, "").slice(0, 16)}`;
}

function snippetAround(text, start, end, radius = 80) {
  const from = Math.max(0, start - radius);
  const to = Math.min(text.length, end + radius);
  let s = text.slice(from, to).replace(/\s+/g, " ").trim();
  if (from > 0) s = "…" + s;
  if (to < text.length) s = s + "…";
  return s;
}

/**
 * @param {{
 *   responseId: string,
 *   text: string,
 *   entityIndex: ReturnType<import('./normalize-entities.js').buildEntityAliasIndex>,
 *   promptIntentTerritory?: string,
 * }} args
 */
export function extractMentions(args) {
  const { responseId, text, entityIndex } = args;
  const source = String(text || "");
  const typedSections = buildTypedSections(source);
  const spans = findEntitySpans(source, entityIndex);

  const byEntity = new Map();
  for (const span of spans) {
    const id = span.entity.id;
    if (!byEntity.has(id)) byEntity.set(id, []);
    byEntity.get(id).push(span);
  }

  const entityDecision = new Map();
  for (const [entityId, entitySpans] of byEntity) {
    entityDecision.set(
      entityId,
      classifyEntityFromMentionSpans({
        text: source,
        spans: entitySpans.map((s) => ({
          start: s.start,
          end: s.end,
          rawMention: s.rawMention,
        })),
        canonicalEntityId: entityId,
        canonicalEntityName: entitySpans[0].entity.name,
        typedSections,
      })
    );
  }

  const seenEntityIds = new Set();
  const draft = [];

  for (const span of spans) {
    const decided =
      entityDecision.get(span.entity.id) ||
      classifyMentionRoleV4_1({
        text: source,
        start: span.start,
        end: span.end,
        rawMention: span.rawMention,
        canonicalEntityId: span.entity.id,
        canonicalEntityName: span.entity.name,
        typedSections,
      });
    const contextSnippet = snippetAround(source, span.start, span.end);
    const firstMention = !seenEntityIds.has(span.entity.id);
    if (firstMention) seenEntityIds.add(span.entity.id);

    draft.push({
      mentionId: newMentionId(),
      responseId,
      entityType: span.entity.entityType,
      canonicalEntityId: span.entity.id,
      canonicalEntityName: span.entity.name,
      rawMention: span.rawMention,
      mentionPosition: span.start,
      recommendationPosition: decided.recommendationPosition,
      firstMention,
      explicitRecommendation: decided.explicitRecommendation,
      role: decided.role,
      sectionRole: decided.sectionRole || decided.evidence?.sectionType || null,
      classificationReason: decided.reason,
      contextSnippet,
      sentimentOrContext:
        decided.role === "negative_or_qualified"
          ? "negative_or_qualified"
          : decided.role === "associated_option"
            ? "associated"
            : decided.explicitRecommendation
              ? "recommended"
              : "unclear",
      extractionMethod: "deterministic_alias_span_v4_1_evidence",
      resolverVersion: RESOLVER_VERSION,
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
    });
  }

  return assignFirstRecommendationAcrossMentionsV4_1(draft, source);
}

export function unresolvedMention(responseId, rawMention, position = 0) {
  return {
    mentionId: newMentionId(),
    responseId,
    entityType: "unresolved",
    canonicalEntityId: null,
    canonicalEntityName: null,
    rawMention: String(rawMention || ""),
    mentionPosition: position,
    recommendationPosition: null,
    firstMention: true,
    explicitRecommendation: false,
    role: "discussed",
    sectionRole: null,
    contextSnippet: String(rawMention || ""),
    sentimentOrContext: "unclear",
    extractionMethod: "unresolved_v1",
    resolverVersion: RESOLVER_VERSION,
    classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
  };
}
