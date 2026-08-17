/**
 * Phase 2A recommendation-classification helpers + unresolved harvest.
 * Phase 2B: roles via recommendation-classifier-v2; unresolved noise filtered.
 */

import {
  classifyMentionRole as classifyMentionRoleV2Compat,
  classifyMentionRoleV2,
  RECOMMENDATION_CLASSIFIER_VERSION,
} from "./recommendation-classifier-v2.js";
import { filterUnresolvedCandidates } from "./unresolved-candidate-filter.js";

export { classifyMentionRoleV2, RECOMMENDATION_CLASSIFIER_VERSION };

/**
 * @param {{ contextSnippet?: string, explicitRecommendation?: boolean, recommendationPosition?: number|null, text?: string, mentionPosition?: number, rawMention?: string, role?: string }} mention
 */
export function classifyMentionRole(mention) {
  if (mention?.role) return mention.role;
  if (mention?.explicitRecommendation) {
    if (mention.recommendationPosition === 1) return "first_recommendation";
    return "explicit_recommendation";
  }
  const snip = String(mention?.contextSnippet || "");
  if (/\b(not\s+recommend(?:ed)?|less\s+suitable|avoid|poor\s+fit)\b/i.test(snip)) {
    return "negative_or_qualified";
  }
  if (/\b(compared\s+to|compared\s+with|versus|vs\.?|unlike|alternative\s+to|alongside)\b/i.test(snip)) {
    return "comparator";
  }
  if (/\b(for\s+example|such\s+as|including|among\s+others|also\s+mentioned)\b/i.test(snip)) {
    return "passing_mention";
  }
  return classifyMentionRoleV2Compat(mention);
}

/**
 * Heuristic unresolved token harvest — for review only, not entity creation.
 * Captures Proper-Case phrases on a single line (no cross-newline garbage).
 * Phase 2B applies deterministic noise filter before return.
 */
export function harvestUnresolvedProperPhrases(text, knownKeys, opts = {}) {
  const source = String(text || "");
  const re = /\b([A-Z][A-Za-z0-9&'.-]*(?:\s+[A-Z][A-Za-z0-9&'.-]*){0,5})\b/g;
  const out = [];
  const seen = new Set();
  let m;
  while ((m = re.exec(source)) !== null) {
    const raw = m[1].trim();
    if (raw.length < 4) continue;
    if (/[\r\n]/.test(raw)) continue;
    if (/^(The|A|An|For|In|On|Of|And|Or|With|Which|When|This|That|These|Those)$/i.test(raw)) {
      continue;
    }
    const key = raw.toLowerCase();
    if (knownKeys.has(key)) continue;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ rawMention: raw, position: m.index });
  }

  if (opts.applyFilter === false) {
    return out;
  }
  return filterUnresolvedCandidates(out).kept;
}

/**
 * Harvest + full filter stats (for Phase 2B reporting).
 */
export function harvestUnresolvedWithFilterStats(text, knownKeys) {
  const source = String(text || "");
  const re = /\b([A-Z][A-Za-z0-9&'.-]*(?:\s+[A-Z][A-Za-z0-9&'.-]*){0,5})\b/g;
  const raw = [];
  const seen = new Set();
  let m;
  while ((m = re.exec(source)) !== null) {
    const phrase = m[1].trim();
    if (phrase.length < 4) continue;
    if (/[\r\n]/.test(phrase)) continue;
    if (/^(The|A|An|For|In|On|Of|And|Or|With|Which|When|This|That|These|Those)$/i.test(phrase)) {
      continue;
    }
    const key = phrase.toLowerCase();
    if (knownKeys.has(key)) continue;
    if (seen.has(key)) continue;
    seen.add(key);
    raw.push({ rawMention: phrase, position: m.index });
  }
  return filterUnresolvedCandidates(raw);
}
