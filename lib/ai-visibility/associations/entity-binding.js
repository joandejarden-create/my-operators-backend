/**
 * Entity-scope binding for AI Brand Association extraction (P0B).
 * Prevents parent/sibling inheritance without explicit brand binding.
 */

import { normalizeMatchKey } from "../normalize-entities.js";

const PARENT_LABELS = new Set(
  [
    "marriott",
    "marriott international",
    "hilton",
    "hilton worldwide",
    "choice hotels",
    "ihg",
    "ihg hotels",
    "accor",
    "hyatt",
  ].map((s) => normalizeMatchKey(s))
);

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Split into sentence-like spans with start offsets. */
export function splitSentencesWithOffsets(text) {
  const source = String(text || "");
  const sentences = [];
  const re = /[^.!?\n]+[.!?]?/g;
  let m;
  while ((m = re.exec(source)) !== null) {
    const chunk = m[0];
    if (!chunk.trim()) continue;
    sentences.push({
      text: chunk,
      start: m.index,
      end: m.index + chunk.length,
    });
  }
  if (!sentences.length && source.trim()) {
    sentences.push({ text: source, start: 0, end: source.length });
  }
  return sentences;
}

function mentionNames(entity) {
  const names = new Set();
  if (!entity) return names;
  if (entity.name) names.add(entity.name);
  if (entity.canonicalEntityName) names.add(entity.canonicalEntityName);
  for (const a of entity.aliases || []) names.add(a);
  if (entity.rawMention) names.add(entity.rawMention);
  return [...names].filter(Boolean);
}

function sentenceContainsName(sentenceText, name) {
  if (!name) return false;
  const re = new RegExp(`\\b${escapeRegExp(name)}\\b`, "i");
  if (re.test(sentenceText)) return true;
  // Partial peer match: first token of multi-word brand names (e.g. "Curio" in sentence)
  const firstToken = String(name).trim().split(/\s+/)[0];
  if (firstToken && firstToken.length >= 4) {
    const partial = new RegExp(`\\b${escapeRegExp(firstToken)}\\b`, "i");
    return partial.test(sentenceText);
  }
  return false;
}

function sentenceContainsParentOnly(sentenceText, parentCompany) {
  const parentKey = normalizeMatchKey(parentCompany);
  if (!parentKey || !PARENT_LABELS.has(parentKey) && parentCompany) {
    // still check explicit parent string
  }
  const parentNames = [];
  if (parentCompany) parentNames.push(parentCompany);
  for (const p of PARENT_LABELS) {
    if (parentKey && normalizeMatchKey(p) === parentKey) parentNames.push(p);
  }
  return parentNames.some((n) => sentenceContainsName(sentenceText, n));
}

/**
 * @param {object} args
 * @param {string} args.text full response text
 * @param {number} args.spanStart
 * @param {number} args.spanEnd
 * @param {object} args.entity target brand entity { id, name, parentCompany, aliases }
 * @param {object[]} [args.mentions] mention rows from evidence
 */
export function validateEntityBinding(args = {}) {
  const { text, spanStart, spanEnd, entity, mentions = [] } = args;
  const sentences = splitSentencesWithOffsets(text);
  const spanMid = Math.floor(((spanStart ?? 0) + (spanEnd ?? 0)) / 2);
  const assocSentence = sentences.find((s) => spanMid >= s.start && spanMid <= s.end) || null;
  if (!assocSentence) {
    return {
      ok: false,
      reason: "span_not_in_response",
      entityBound: false,
      parentOnlyLeak: false,
    };
  }

  const names = mentionNames(entity);
  const entityInSentence = names.some((n) => sentenceContainsName(assocSentence.text, n));
  const mentionInSentence = (mentions || []).some((m) => {
    const mid = m.mentionPosition ?? 0;
    if (mid < assocSentence.start || mid > assocSentence.end) return false;
    const midId = m.canonicalEntityId || m.entityId || m.resolvedEntityId;
    if (midId !== entity.id) return false;
    const mentionName = m.canonicalEntityName || m.entityName || m.rawMention || m.name;
    return mentionName && sentenceContainsName(assocSentence.text, mentionName);
  });

  const parentInSentence = sentenceContainsParentOnly(
    assocSentence.text,
    entity.parentCompany
  );
  const parentOnlyLeak = parentInSentence && !entityInSentence && !mentionInSentence;

  const entityBound = entityInSentence || mentionInSentence;

  return {
    ok: entityBound && !parentOnlyLeak,
    reason: parentOnlyLeak
      ? "parent_only_inheritance_blocked"
      : entityBound
        ? "entity_bound"
        : "entity_not_bound_in_span_sentence",
    entityBound,
    parentOnlyLeak,
    sentenceText: assocSentence.text.trim(),
  };
}

/**
 * Build entity lookup from mention rows + optional entity index.
 * @param {object[]} mentions
 */
export function indexMentionsByEntityId(mentions = []) {
  const map = new Map();
  for (const m of mentions) {
    const id = m.canonicalEntityId || m.entityId || m.resolvedEntityId;
    if (!id) continue;
    if (!map.has(id)) {
      map.set(id, {
        id,
        name: m.canonicalEntityName || m.entityName || m.name,
        parentCompany: m.parentCompany || null,
        aliases: [],
        mentions: [],
      });
    }
    const row = map.get(id);
    row.mentions.push(m);
    if (m.rawMention && !row.aliases.includes(m.rawMention)) {
      row.aliases.push(m.rawMention);
    }
  }
  return map;
}

/**
 * Detect sibling-brand collision: span sentence names another peer brand prominently
 * without naming target entity.
 */
export function detectSiblingCollision(args = {}) {
  const { sentenceText, entity, peerNames = [] } = args;
  if (!sentenceText || !entity?.name) return false;
  const targetNames = mentionNames(entity);
  const hasTarget = targetNames.some((n) => sentenceContainsName(sentenceText, n));
  if (hasTarget) return false;
  return peerNames.some(
    (peer) =>
      peer &&
      normalizeMatchKey(peer) !== normalizeMatchKey(entity.name) &&
      sentenceContainsName(sentenceText, peer)
  );
}
