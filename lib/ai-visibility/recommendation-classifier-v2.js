/**
 * AI Visibility recommendation classifier v2 (deterministic).
 *
 * Roles:
 *   first_recommendation | ranked_recommendation | explicit_recommendation |
 *   comparator | passing_mention | negative_or_qualified | discussed | source_only
 *
 * First recommendation priority (structure over raw first mention):
 *   1) Explicit rank markers (#1 / 1. / Priority 1) under recommendation context
 *   2) Explicit "first" / "first call" language attached to the entity
 *   3) First item in a detected shortlist / solicitation list
 *   4) Fallback: earliest explicit recommendation by recommendationPosition
 */

export const RECOMMENDATION_CLASSIFIER_VERSION =
  "ai_visibility_recommendation_classifier_v2";

const NEGATIVE_CUES =
  /\b(not\s+recommend(?:ed)?|less\s+suitable|weaker\s+fit|may\s+be\s+difficult|not\s+ideal|unlikely|avoid|less\s+attractive|only\s+if|depends\s+heavily\s+on|poor\s+fit|weak\s+fit|would\s+not\s+consider|do\s+not\s+consider)\b/i;

const COMPARATOR_CUES =
  /\b(alternative\s+to|compared\s+(?:to|with)|versus|vs\.?|unlike|rather\s+than|similar\s+to|competitor\s+to|alongside)\b/i;

const PASSING_CUES =
  /\b(for\s+example|such\s+as|including|among\s+others|also\s+mentioned|e\.g\.|i\.e\.)\b/i;

const POSITIVE_REC_CUES =
  /\b(recommend(?:ed)?|should\s+consider|may\s+also\s+consider|also\s+consider|may\s+consider|consider|shortlist|strong\s+fit|strong\s+candidate|strong\s+alternative|first\s+call|best\s+fit|best\s+value(?:\s+engineering)?(?:\s+option)?|good\s+option|preferred|particularly\s+suitable|likely\s+fit|solicit\s+proposals?\s+from|i\s+would\s+consider|i\s+would\s+shortlist|top\s+options?|primary\s+option|leading\s+candidate)\b/i;

const FIRST_CALL_CUES =
  /\b(first\s+call|first\s+choice|first\s+option|shortlist\s+first|consider\s+first|begin\s+with|start\s+with|my\s+first)\b/i;

const SOURCE_ONLY_CUES =
  /\b(according\s+to|cited\s+(?:in|by)|source:|see\s+also)\b/i;

/**
 * Detect numbered rank immediately before an entity mention on its line.
 * @returns {number|null}
 */
export function detectRankMarker(text, start) {
  const source = String(text || "");
  const lineStart = source.lastIndexOf("\n", start - 1) + 1;
  const prefix = source.slice(lineStart, start);

  // Markdown table row: | **1** | **Brand**
  const tableRank = prefix.match(/\|\s*\*?\*?(\d+)\*?\*?\s*\|\s*\*?\*?$/);
  if (tableRank) return parseInt(tableRank[1], 10);

  // "1. Brand" / "1) Brand" / "#1 Brand"
  const numbered = prefix.match(/(?:^|[\s|])(?:#)?(\d+)[.)]\s*\*?\*?$/);
  if (numbered) return parseInt(numbered[1], 10);

  // "Priority 1 |" style
  const priority = prefix.match(/\bpriority\s+(\d+)\s*[:|]?\s*\*?\*?$/i);
  if (priority) return parseInt(priority[1], 10);

  // Standalone "#1" just before
  const hash = source.slice(Math.max(0, start - 12), start).match(/#\s*(\d+)\s*$/);
  if (hash) return parseInt(hash[1], 10);

  return null;
}

/**
 * Detect if mention sits inside a recommendation-oriented list/shortlist block.
 */
export function detectRecommendationBlock(text, start) {
  const source = String(text || "");
  const lookBehind = source.slice(Math.max(0, start - 400), start);
  return /\b(recommended\s+shortlist|shortlist|top\s+options?|i\s+would\s+(?:consider|shortlist|solicit)|solicit\s+proposals?\s+from|begin\s+with|start\s+with|primary\s+candidates?)\b/i.test(
    lookBehind
  );
}

/**
 * Classify a single mention given full response text + span.
 * @param {{
 *   text: string,
 *   start: number,
 *   end: number,
 *   contextSnippet?: string,
 *   explicitRecommendation?: boolean,
 *   recommendationPosition?: number|null,
 * }} mention
 */
export function classifyMentionRoleV2(mention) {
  const text = String(mention?.text || "");
  const start = Number(mention?.start ?? mention?.mentionPosition ?? 0);
  const end = Number(mention?.end ?? start + String(mention?.rawMention || "").length);
  const snip =
    mention?.contextSnippet ||
    text.slice(Math.max(0, start - 100), Math.min(text.length, end + 100));
  const window = text.slice(Math.max(0, start - 160), Math.min(text.length, end + 120));

  if (NEGATIVE_CUES.test(snip) || NEGATIVE_CUES.test(window)) {
    return {
      role: "negative_or_qualified",
      explicitRecommendation: false,
      recommendationPosition: null,
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      reason: "negative_or_qualified_cue",
    };
  }

  // Comparator object: "... alternative to ENTITY" / "... compared with ENTITY"
  const before = text.slice(Math.max(0, start - 80), start);
  const isComparatorObject = /(?:alternative\s+to|compared\s+(?:to|with)|versus|vs\.?|unlike|similar\s+to|competitor\s+to)\s+[^\n.]{0,40}$/i.test(
    before
  );

  if (isComparatorObject) {
    return {
      role: "comparator",
      explicitRecommendation: false,
      recommendationPosition: null,
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      reason: "comparator_object",
    };
  }

  const rank = detectRankMarker(text, start);
  const inRecBlock = detectRecommendationBlock(text, start);
  const positive = POSITIVE_REC_CUES.test(snip) || POSITIVE_REC_CUES.test(window);
  const firstCall = FIRST_CALL_CUES.test(snip) || FIRST_CALL_CUES.test(window);

  if (rank === 1 || firstCall) {
    return {
      role: "first_recommendation",
      explicitRecommendation: true,
      recommendationPosition: 1,
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      reason: rank === 1 ? "rank_marker_1" : "first_call_language",
    };
  }

  if (rank != null && rank > 1) {
    return {
      role: "ranked_recommendation",
      explicitRecommendation: true,
      recommendationPosition: rank,
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      reason: "rank_marker_gt_1",
    };
  }

  if (positive || inRecBlock) {
    // Inside solicitation list without explicit numbers — treat as explicit;
    // position filled later by document-order assignment.
    return {
      role: "explicit_recommendation",
      explicitRecommendation: true,
      recommendationPosition: mention?.recommendationPosition ?? null,
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      reason: positive ? "positive_recommendation_cue" : "recommendation_block",
    };
  }

  if (PASSING_CUES.test(snip) || PASSING_CUES.test(window)) {
    return {
      role: "passing_mention",
      explicitRecommendation: false,
      recommendationPosition: null,
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      reason: "passing_cue",
    };
  }

  if (SOURCE_ONLY_CUES.test(snip)) {
    return {
      role: "source_only",
      explicitRecommendation: false,
      recommendationPosition: null,
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      reason: "source_only_cue",
    };
  }

  return {
    role: "discussed",
    explicitRecommendation: false,
    recommendationPosition: null,
    classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
    reason: "default_discussed",
  };
}

/**
 * Post-pass: assign first recommendation + positions across all mentions in one response.
 * Structure beats raw first text mention.
 * @param {object[]} classifiedMentions mentions already enriched with role/explicit flags
 * @param {string} text
 */
export function assignFirstRecommendationAcrossMentions(classifiedMentions, text) {
  const mentions = (classifiedMentions || []).map((m) => ({ ...m }));
  const byEntity = new Map();

  for (const m of mentions) {
    if (!m.canonicalEntityId || !m.explicitRecommendation) continue;
    const prev = byEntity.get(m.canonicalEntityId);
    if (!prev) {
      byEntity.set(m.canonicalEntityId, m);
      continue;
    }
    // Prefer lower recommendationPosition, then earlier mention
    const prevPos = prev.recommendationPosition ?? Number.MAX_SAFE_INTEGER;
    const curPos = m.recommendationPosition ?? Number.MAX_SAFE_INTEGER;
    if (curPos < prevPos || (curPos === prevPos && m.mentionPosition < prev.mentionPosition)) {
      byEntity.set(m.canonicalEntityId, m);
    }
  }

  // Build ordered unique recommendation list
  const ordered = [...byEntity.values()].sort((a, b) => {
    const ap = a.recommendationPosition ?? Number.MAX_SAFE_INTEGER;
    const bp = b.recommendationPosition ?? Number.MAX_SAFE_INTEGER;
    if (ap !== bp) return ap - bp;
    return a.mentionPosition - b.mentionPosition;
  });

  // If no rank-1 but we have ordered recs, promote first structural/list item
  if (ordered.length && !ordered.some((m) => m.role === "first_recommendation")) {
    const first = ordered[0];
    first.role = "first_recommendation";
    first.recommendationPosition = 1;
    first.explicitRecommendation = true;
  }

  // Sync roles on all mentions for the first entity
  const firstEntityId = ordered.find((m) => m.role === "first_recommendation")?.canonicalEntityId;
  if (firstEntityId) {
    for (const m of mentions) {
      if (m.canonicalEntityId !== firstEntityId) continue;
      if (m.explicitRecommendation) {
        m.role = "first_recommendation";
        m.recommendationPosition = m.recommendationPosition ?? 1;
      }
    }
  }

  // Fill missing positions for other explicit recs in order
  let next = 2;
  const seen = new Set(firstEntityId ? [firstEntityId] : []);
  for (const m of ordered) {
    if (seen.has(m.canonicalEntityId)) continue;
    seen.add(m.canonicalEntityId);
    if (m.recommendationPosition == null) {
      m.recommendationPosition = next;
    }
    if (m.role === "explicit_recommendation" && m.recommendationPosition > 1) {
      m.role = "ranked_recommendation";
    }
    next = Math.max(next, (m.recommendationPosition || next) + 1);
  }

  // Propagate positions onto duplicate mentions of same entity
  const posByEntity = new Map();
  for (const m of mentions) {
    if (m.canonicalEntityId && m.recommendationPosition != null) {
      const prev = posByEntity.get(m.canonicalEntityId);
      if (prev == null || m.recommendationPosition < prev) {
        posByEntity.set(m.canonicalEntityId, m.recommendationPosition);
      }
    }
  }
  for (const m of mentions) {
    if (!m.canonicalEntityId || !m.explicitRecommendation) continue;
    if (m.recommendationPosition == null && posByEntity.has(m.canonicalEntityId)) {
      m.recommendationPosition = posByEntity.get(m.canonicalEntityId);
    }
  }

  return mentions;
}

/** Back-compat wrapper used by Phase 2A helpers. */
export function classifyMentionRole(mention) {
  return classifyMentionRoleV2({
    ...mention,
    text: mention.text || mention.contextSnippet || "",
    start: mention.mentionPosition ?? 0,
  }).role;
}
