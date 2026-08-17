/**
 * Bilingual semantic-pair foundation (Phase 3A.6).
 * Pairs share owner decision identity; language and natural wording differ.
 * No automatic translation. No production Spanish prompts in this phase.
 */

import {
  AI_VISIBILITY_LANGUAGES,
  normalizeLanguage,
  requireSupportedLanguage,
} from "./language-dimension.js";

export const SEMANTIC_PAIR_VERSION = "ai_visibility_semantic_pair_v1";

/**
 * @typedef {object} SemanticPairMember
 * @property {string} promptId
 * @property {string|number} version
 * @property {string} language
 * @property {string} [semanticPairId]
 * @property {string} [intentTerritory]
 * @property {string} [geographyScope]
 * @property {string|null} [commercialRegion]
 * @property {string|null} [country]
 * @property {string} [promptFamily]
 * @property {string} [entityScope]
 * @property {string[]} [stakeholderRelevance]
 * @property {string} [peerSetId]
 * @property {string} [developmentType]
 * @property {string} [chainScale]
 */

function normStr(v) {
  return v == null || v === "" ? null : String(v).trim();
}

function sameIgnoreCase(a, b) {
  return String(a || "").toLowerCase() === String(b || "").toLowerCase();
}

/**
 * Validate that EN + ES members form a legitimate semantic pair.
 * Text need not be literal translations.
 *
 * @param {SemanticPairMember} a
 * @param {SemanticPairMember} b
 */
export function validateSemanticPairMembers(a, b) {
  const errors = [];
  if (!a || !b) {
    return { ok: false, errors: ["both_members_required"], TEXT_MUST_BE_LITERAL_TRANSLATION: false };
  }

  const langA = requireSupportedLanguage(a.language);
  const langB = requireSupportedLanguage(b.language);
  if (!langA.ok) errors.push(`member_a_${langA.reasonCode}`);
  if (!langB.ok) errors.push(`member_b_${langB.reasonCode}`);
  if (langA.ok && langB.ok && langA.language === langB.language) {
    errors.push("languages_must_differ");
  }
  if (langA.ok && langB.ok) {
    const set = new Set([langA.language, langB.language]);
    if (!(set.has("en") && set.has("es"))) {
      errors.push("pair_must_be_en_and_es");
    }
  }

  const pairA = normStr(a.semanticPairId);
  const pairB = normStr(b.semanticPairId);
  if (!pairA || !pairB) errors.push("semanticPairId_required");
  else if (pairA !== pairB) errors.push("semanticPairId_mismatch");

  if (!sameIgnoreCase(a.intentTerritory, b.intentTerritory)) {
    errors.push("intentTerritory_mismatch");
  }
  if (!sameIgnoreCase(a.geographyScope, b.geographyScope)) {
    errors.push("geographyScope_mismatch");
  }
  if (!sameIgnoreCase(a.commercialRegion, b.commercialRegion)) {
    errors.push("commercialRegion_mismatch");
  }
  if (!sameIgnoreCase(a.country, b.country)) {
    errors.push("country_mismatch");
  }
  if (!sameIgnoreCase(a.promptFamily, b.promptFamily)) {
    errors.push("promptFamily_mismatch");
  }
  if (!sameIgnoreCase(a.entityScope, b.entityScope)) {
    errors.push("entityScope_mismatch");
  }
  if (!sameIgnoreCase(a.peerSetId, b.peerSetId)) {
    errors.push("peerSetId_mismatch");
  }
  if (
    normStr(a.developmentType) &&
    normStr(b.developmentType) &&
    !sameIgnoreCase(a.developmentType, b.developmentType)
  ) {
    errors.push("developmentType_mismatch");
  }
  if (
    normStr(a.chainScale) &&
    normStr(b.chainScale) &&
    !sameIgnoreCase(a.chainScale, b.chainScale)
  ) {
    errors.push("chainScale_mismatch");
  }

  const stakeA = new Set((a.stakeholderRelevance || []).map((s) => String(s).toLowerCase()));
  const stakeB = new Set((b.stakeholderRelevance || []).map((s) => String(s).toLowerCase()));
  if (stakeA.size || stakeB.size) {
    const same =
      stakeA.size === stakeB.size && [...stakeA].every((s) => stakeB.has(s));
    if (!same) errors.push("stakeholderRelevance_mismatch");
  }

  return {
    ok: errors.length === 0,
    errors,
    semanticPairId: pairA || pairB,
    languages: [langA.ok ? langA.language : null, langB.ok ? langB.language : null].filter(
      Boolean
    ),
    TEXT_MUST_BE_LITERAL_TRANSLATION: false,
    PAIR_VALIDATION_RULES: [
      "same semanticPairId",
      "languages en + es",
      "same intentTerritory",
      "same geographyScope / commercialRegion / country",
      "same promptFamily",
      "same entityScope / peerSetId",
      "compatible stakeholderRelevance",
      "prompt text may differ naturally",
    ],
    version: SEMANTIC_PAIR_VERSION,
    supportedLanguages: [...AI_VISIBILITY_LANGUAGES],
  };
}

/**
 * Build a stable semantic pair id suggestion (governance may override).
 * @param {{ promptFamily: string, intentTerritory: string, geographyKey: string }} args
 */
export function suggestSemanticPairId(args = {}) {
  const family = String(args.promptFamily || "family")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "");
  const intent = String(args.intentTerritory || "intent")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "");
  const geo = String(args.geographyKey || "geo")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "");
  return `sp_${family}_${intent}_${geo}_v1`;
}

/**
 * Prompt execution identity dimensions (must differ by language).
 */
export function promptExecutionIdentity(promptLike = {}) {
  return {
    promptId: promptLike.promptId || null,
    version: promptLike.version != null ? String(promptLike.version) : null,
    language: normalizeLanguage(promptLike.language),
    semanticPairId: normStr(promptLike.semanticPairId),
    intentTerritory: normStr(promptLike.intentTerritory),
    geographyScope: normStr(promptLike.geographyScope),
  };
}
