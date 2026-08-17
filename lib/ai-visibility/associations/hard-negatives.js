/**
 * Hard-negative pattern library for association holdout (P0B.1).
 * Permanent regression fixtures — no provider calls.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getAssociationAttribute } from "./attribute-taxonomy.js";
import {
  indexMentionsByEntityId,
  validateEntityBinding,
  detectSiblingCollision,
  splitSentencesWithOffsets,
} from "./entity-binding.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(__dirname, "..", "..", "..");

export const HARD_NEGATIVE_CATEGORIES = Object.freeze([
  "PROMPT_ATTRIBUTE_ONLY",
  "PEER_BRAND_ATTRIBUTE",
  "PARENT_COMPANY_ONLY",
  "SIBLING_BRAND",
  "MARKET_NOT_BRAND",
  "LIST_MENTION_NO_ATTRIBUTE",
  "CITATION_TITLE_ONLY",
  "PRONOUN_AMBIGUOUS",
  "TABLE_ROW_LEAKAGE",
  "NEARBY_SENTENCE_LEAKAGE",
  "NEGATIVE_COMPARISON_PEER",
  "MIXED_QUALIFICATION",
  "GENERIC_PARENT_LOYALTY",
  "BRAND_ABSENT",
]);

export const DEFAULT_HARD_NEGATIVES_PATH = path.join(
  REPO_ROOT,
  "fixtures",
  "ai-visibility",
  "association-hard-negatives-v1.json"
);

export function loadHardNegativeFixtures(filePath = DEFAULT_HARD_NEGATIVES_PATH) {
  if (!fs.existsSync(filePath)) return { version: "v1", cases: [] };
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function phraseInText(text, phrases = []) {
  const lower = String(text || "").toLowerCase();
  return phrases.some((p) => lower.includes(String(p).toLowerCase()));
}

/**
 * Classify hard-negative category for a candidate case.
 * @returns {{ category: string|null, humanLabel: string, humanLabelled: boolean, notes: string }|null}
 */
export function classifyHardNegative(ev, entityId, attributeId, peerNames = []) {
  const text = String(ev.payload?.rawResponseText || "");
  const promptText = String(ev.promptText || "");
  const def = getAssociationAttribute(attributeId);
  if (!def) return null;

  const mentions = ev.payload?.mentions || [];
  const entityMap = indexMentionsByEntityId(mentions);
  const entity = entityMap.get(entityId);

  // Brand absent
  if (!entity) {
    return {
      category: "BRAND_ABSENT",
      humanLabel: "NO_ASSOCIATION",
      humanLabelled: true,
      hardNegative: true,
      notes: "Canonical entity not mentioned in response.",
    };
  }

  const inResponse = phraseInText(text, def.inScopeLanguage);
  const inPrompt = phraseInText(promptText, def.inScopeLanguage);

  if (inPrompt && !inResponse) {
    return {
      category: "PROMPT_ATTRIBUTE_ONLY",
      humanLabel: "NO_ASSOCIATION",
      humanLabelled: true,
      hardNegative: true,
      notes: "Attribute language appears in prompt only.",
    };
  }

  if (!inResponse) {
    return null;
  }

  const sentences = splitSentencesWithOffsets(text);
  for (const sentence of sentences) {
    if (!phraseInText(sentence.text, def.inScopeLanguage)) continue;

    const binding = validateEntityBinding({
      text,
      spanStart: sentence.start + 1,
      spanEnd: sentence.end - 1,
      entity,
      mentions,
    });

    if (binding.parentOnlyLeak) {
      return {
        category: "PARENT_COMPANY_ONLY",
        humanLabel: "NO_ASSOCIATION",
        humanLabelled: true,
        hardNegative: true,
        notes: "Attribute describes parent company without explicit brand binding.",
      };
    }

    if (
      detectSiblingCollision({
        sentenceText: sentence.text,
        entity,
        peerNames,
      })
    ) {
      return {
        category: "SIBLING_BRAND",
        humanLabel: "NO_ASSOCIATION",
        humanLabelled: true,
        hardNegative: true,
        notes: "Attribute bound to peer/sibling brand, not target entity.",
      };
    }
  }

  // Entity mentioned but attribute not in same binding context
  const entityMentioned = mentions.some(
    (m) => (m.canonicalEntityId || m.entityId) === entityId
  );
  if (entityMentioned && !inResponse) {
    return {
      category: "LIST_MENTION_NO_ATTRIBUTE",
      humanLabel: "NO_ASSOCIATION",
      humanLabelled: true,
      hardNegative: true,
      notes: "Brand listed without attribute attachment.",
    };
  }

  return null;
}

export { phraseInText };
