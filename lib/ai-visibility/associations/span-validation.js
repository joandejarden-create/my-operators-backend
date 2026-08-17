/**
 * Supporting-span validation + failure-mode audit (P0B.1).
 * Spans must originate in exact stored response text.
 */

import { getAssociationAttribute } from "./attribute-taxonomy.js";
import { splitSentencesWithOffsets, validateEntityBinding } from "./entity-binding.js";

export const SPAN_FAILURE_MODES = Object.freeze([
  "SPAN_TOO_WIDE",
  "SPAN_TOO_NARROW",
  "ENTITY_OUTSIDE_SPAN",
  "ATTRIBUTE_OUTSIDE_SPAN",
  "CROSS_SENTENCE_BINDING",
  "TABLE_STRUCTURE_ERROR",
  "PARENTHETICAL_BINDING",
  "PRONOUN_AMBIGUITY",
  "SIBLING_LEAKAGE",
  "PARENT_INHERITANCE",
  "NORMALIZATION_OFFSET_ERROR",
  "SPAN_NOT_IN_RESPONSE",
  "OTHER",
]);

/** Markdown/table row heuristic */
export function isTableRowSpan(text) {
  const t = String(text || "");
  const pipeCount = (t.match(/\|/g) || []).length;
  return pipeCount >= 2 || /\|\s*\*\*/.test(t);
}

/** List-item boundary: span starts mid-bullet or crosses list items */
export function isListItemLeakage(fullText, spanStart, spanEnd) {
  const slice = String(fullText || "").slice(spanStart, spanEnd);
  const bulletStarts = (slice.match(/(?:^|\n)\s*[-*•]\s+/g) || []).length;
  return bulletStarts > 1;
}

/**
 * Validate supporting span against raw response.
 * @param {string} rawResponseText
 * @param {object} supportingSpan { start, end, text, exactText? }
 * @param {object} [context] { entity, attributeId, mentions }
 */
export function validateSupportingSpan(rawResponseText, supportingSpan, context = {}) {
  const raw = String(rawResponseText || "");
  if (!supportingSpan || !raw.trim()) {
    return { valid: false, failureMode: "SPAN_NOT_IN_RESPONSE", details: "missing span or response" };
  }

  const exact =
    supportingSpan.exactText != null
      ? String(supportingSpan.exactText)
      : String(supportingSpan.text || "").replace(/^…+/, "").replace(/…+$/, "");

  let offsetValid = false;
  if (
    Number.isInteger(supportingSpan.start) &&
    Number.isInteger(supportingSpan.end) &&
    supportingSpan.start >= 0 &&
    supportingSpan.end <= raw.length &&
    supportingSpan.end > supportingSpan.start
  ) {
    const slice = raw.slice(supportingSpan.start, supportingSpan.end);
    offsetValid = slice.trim() === exact.trim() || raw.slice(supportingSpan.start, supportingSpan.end).includes(exact.slice(0, 20));
  }

  const substringValid = exact.length >= 8 && raw.includes(exact);
  if (!offsetValid && !substringValid) {
    return {
      valid: false,
      failureMode: "NORMALIZATION_OFFSET_ERROR",
      details: "span text not found in raw response",
    };
  }

  const spanStart = offsetValid
    ? supportingSpan.start
    : raw.indexOf(exact.slice(0, Math.min(24, exact.length)));
  const spanEnd = offsetValid ? supportingSpan.end : spanStart + exact.length;

  if (isTableRowSpan(exact)) {
    return { valid: false, failureMode: "TABLE_STRUCTURE_ERROR", details: "table-row span" };
  }

  if (isListItemLeakage(raw, spanStart, spanEnd)) {
    return { valid: false, failureMode: "CROSS_SENTENCE_BINDING", details: "cross-list-item span" };
  }

  const sentences = splitSentencesWithOffsets(raw);
  const spanMid = Math.floor((spanStart + spanEnd) / 2);
  const sentenceCount = sentences.filter(
    (s) => !(s.end <= spanStart || s.start >= spanEnd)
  ).length;
  if (sentenceCount > 2) {
    return { valid: false, failureMode: "SPAN_TOO_WIDE", details: "span crosses too many sentences" };
  }

  if (context.entity && context.attributeId) {
    const def = getAssociationAttribute(context.attributeId);
    const attrHit = (def?.inScopeLanguage || []).some((phrase) =>
      exact.toLowerCase().includes(String(phrase).toLowerCase())
    );
    if (!attrHit) {
      return { valid: false, failureMode: "ATTRIBUTE_OUTSIDE_SPAN", details: context.attributeId };
    }

    const binding = validateEntityBinding({
      text: raw,
      spanStart,
      spanEnd,
      entity: context.entity,
      mentions: context.mentions || [],
    });
    if (!binding.ok) {
      const mode = binding.parentOnlyLeak ? "PARENT_INHERITANCE" : "ENTITY_OUTSIDE_SPAN";
      return { valid: false, failureMode: mode, details: binding.reason };
    }
  }

  if (exact.length < 12) {
    return { valid: false, failureMode: "SPAN_TOO_NARROW", details: "span too short" };
  }

  return {
    valid: true,
    failureMode: null,
    spanStart,
    spanEnd,
    exactText: offsetValid ? raw.slice(spanStart, spanEnd).trim() : exact.trim(),
  };
}

/**
 * Audit span failures for a set of predictions.
 * @param {object[]} predictions
 * @param {Map<string, object>} evidenceById
 */
export function auditSpanFailures(predictions = [], evidenceById = new Map()) {
  const counts = Object.fromEntries(SPAN_FAILURE_MODES.map((m) => [m, 0]));
  const samples = [];

  for (const pred of predictions) {
    const ev = evidenceById.get(pred.evidenceId);
    if (!ev) continue;
    const raw = ev.payload?.rawResponseText || "";
    const entity = {
      id: pred.entityId,
      name: pred.entityName,
      parentCompany: pred.parentCompany || null,
    };
    const result = validateSupportingSpan(raw, pred.supportingSpan, {
      entity,
      attributeId: pred.attributeId,
      mentions: ev.payload?.mentions || [],
    });
    if (!result.valid) {
      const mode = result.failureMode || "OTHER";
      counts[mode] = (counts[mode] || 0) + 1;
      if (samples.length < 20) {
        samples.push({
          associationEvidenceId: pred.associationEvidenceId,
          evidenceId: pred.evidenceId,
          attributeId: pred.attributeId,
          failureMode: mode,
          details: result.details,
          spanPreview: String(pred.supportingSpan?.text || "").slice(0, 100),
        });
      }
    }
  }

  return { counts, samples, totalFailures: Object.values(counts).reduce((a, b) => a + b, 0) };
}

/**
 * Build minimal sentence-bounded span containing entity + attribute match.
 * @param {string} text full response
 * @param {object} sentence { start, end, text }
 * @param {object} hit { start, end }
 * @param {object} entity
 */
export function buildSentenceBoundedSpan(text, sentence, hit, entity) {
  const sentText = sentence.text;
  const sentStart = sentence.start;

  const entityNames = [
    entity?.name,
    entity?.canonicalEntityName,
    ...(entity?.aliases || []),
  ].filter(Boolean);

  let entityRelStart = sentText.length;
  for (const name of entityNames) {
    const idx = sentText.toLowerCase().indexOf(String(name).toLowerCase());
    if (idx >= 0 && idx < entityRelStart) entityRelStart = idx;
  }

  const hitRelStart = hit.start - sentStart;
  const hitRelEnd = hit.end - sentStart;

  const fromRel = Math.min(entityRelStart, hitRelStart);
  const toRel = Math.max(hitRelEnd, entityRelStart + (entityNames[0]?.length || 0));

  const bounded = sentText.slice(Math.max(0, fromRel), Math.min(sentText.length, toRel)).trim();
  const useFullSentence =
    bounded.length < 20 || isTableRowSpan(sentText) || isTableRowSpan(bounded);

  const exactText = (useFullSentence ? sentText : bounded).trim();
  const absStart = useFullSentence ? sentence.start : sentence.start + fromRel;
  const absEnd = useFullSentence ? sentence.end : sentence.start + fromRel + exactText.length;

  return {
    start: absStart,
    end: absEnd,
    text: exactText,
    exactText,
  };
}
