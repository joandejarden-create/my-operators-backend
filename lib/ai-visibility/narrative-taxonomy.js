/**
 * Narrative & Source Intelligence V1 taxonomy.
 * Evidence-first positioning concepts for hotel-owner decision contexts.
 * Does not replace certified Presence / Association / Truth layers.
 */

import { associationAttributePublicationTier } from "./gaps/association-eligibility.js";

export const NARRATIVE_TAXONOMY_VERSION = "ai_visibility_narrative_taxonomy_v1";

export const NARRATIVE_FAMILIES = Object.freeze([
  "BRAND_POSITIONING",
  "CONVERSION_SUITABILITY",
  "OWNER_FLEXIBILITY_CONTROL",
  "DISTRIBUTION_LOYALTY",
  "FEES_ECONOMICS",
  "SOFT_BRAND_INDIVIDUALITY",
  "CHAIN_SCALE_POSITIONING",
  "DESIGN_LOCAL_CHARACTER",
  "BRANDED_RESIDENCES",
  "DEVELOPMENT_SUPPORT",
  "MARKET_RELEVANCE",
  "OPERATING_MODEL",
  "OTHER",
]);

export const BRAND_RELATIONSHIP_TYPES = Object.freeze([
  "EXPLICIT_BRAND_ASSOCIATION",
  "IMPLICIT_BRAND_ASSOCIATION",
  "COMPETITOR_ASSOCIATION",
  "GENERAL_CONTEXT",
  "AMBIGUOUS",
]);

export const ALLOWED_SOURCE_RELATIONSHIP = Object.freeze([
  "DIRECTLY_CITED",
  "RECURRING_CITED_SOURCE",
  "DIRECTLY_CITED_WITH_NARRATIVE",
  "RECURRING_CITED_ALONGSIDE_NARRATIVE",
  "FREQUENTLY_CO_OCCURRING",
  "ASSOCIATED_SOURCE",
  "ASSOCIATED_NOT_CAUSAL",
  "UNCONFIRMED_RELATIONSHIP",
]);

export const FORBIDDEN_SOURCE_LANGUAGE = Object.freeze([
  "INFLUENCED",
  "CAUSED",
  "TRAINED_ON",
  "MODEL_LEARNED_FROM",
  "MODEL_BELIEVES_THIS_BECAUSE",
  "SOURCE_DRIVES_AI_VISIBILITY",
  "LIKELY_CAUSAL_SOURCE",
]);

export const SOURCE_CATEGORIES = Object.freeze([
  "BRAND_OWNED",
  "PARENT_COMPANY_OWNED",
  "DEVELOPMENT_SITE",
  "RESIDENCES_SITE",
  "INVESTOR_RELATIONS",
  "GOVERNMENT_REGULATORY",
  "TRADE_MEDIA",
  "INDUSTRY_DATA",
  "CONSULTING_ADVISORY",
  "OTA_TRAVEL",
  "GENERAL_MEDIA",
  "OTHER",
]);

/** Deterministic attribute → narrative family map (reuse association extractor). */
export const ATTRIBUTE_TO_NARRATIVE_FAMILY = Object.freeze({
  INDEPENDENT_IDENTITY: "SOFT_BRAND_INDIVIDUALITY",
  DESIGN_INDIVIDUALITY: "DESIGN_LOCAL_CHARACTER",
  DISTRIBUTION: "DISTRIBUTION_LOYALTY",
  LOYALTY: "DISTRIBUTION_LOYALTY",
  OWNER_FLEXIBILITY: "OWNER_FLEXIBILITY_CONTROL",
  OWNER_CONTROL: "OWNER_FLEXIBILITY_CONTROL",
  CONVERSION_SUITABILITY: "CONVERSION_SUITABILITY",
  ECONOMICS: "FEES_ECONOMICS",
  GEOGRAPHIC_PRESENCE: "MARKET_RELEVANCE",
  LIFESTYLE_POSITIONING: "BRAND_POSITIONING",
  LUXURY_POSITIONING: "BRAND_POSITIONING",
  BRANDED_RESIDENCES: "BRANDED_RESIDENCES",
  DEVELOPMENT_SUPPORT: "DEVELOPMENT_SUPPORT",
  OPERATING_MODEL: "OPERATING_MODEL",
  MARKET_FIT: "MARKET_RELEVANCE",
});

export const NARRATIVE_FAMILY_LABELS = Object.freeze({
  BRAND_POSITIONING: "Brand positioning",
  CONVERSION_SUITABILITY: "Conversion suitability",
  OWNER_FLEXIBILITY_CONTROL: "Owner flexibility / control",
  DISTRIBUTION_LOYALTY: "Distribution / loyalty strength",
  FEES_ECONOMICS: "Fees / economics",
  SOFT_BRAND_INDIVIDUALITY: "Soft-brand individuality",
  CHAIN_SCALE_POSITIONING: "Chain scale positioning",
  DESIGN_LOCAL_CHARACTER: "Design / local character",
  BRANDED_RESIDENCES: "Branded residences capability",
  DEVELOPMENT_SUPPORT: "Development support",
  MARKET_RELEVANCE: "Market relevance",
  OPERATING_MODEL: "Operating model",
  OTHER: "Other",
});

/**
 * @param {string} attributeId
 * @returns {string}
 */
export function mapAttributeToNarrativeFamily(attributeId) {
  return ATTRIBUTE_TO_NARRATIVE_FAMILY[String(attributeId || "")] || "OTHER";
}

/**
 * Conservative V1 production gating by narrative family.
 * @param {string} family
 * @param {{ validationPrecision?: number|null, hasEvidence?: boolean }} [ctx]
 */
export function classifyNarrativeFamilyProductionState(family, ctx = {}) {
  const id = String(family || "OTHER");
  const holdoutPrecision = ctx.holdoutPrecision ?? ctx.validationPrecision;
  const hasEvidence = ctx.hasEvidence !== false;
  const holdoutCases = ctx.holdoutCases ?? 0;
  const execFp = ctx.executiveFalsePositives ?? 0;
  const brandErr = ctx.brandAttributionErrors ?? 0;
  const collisionRisk = ctx.collisionRisk ?? "LOW";

  if (!hasEvidence) return "BLOCKED";
  if (id === "FEES_ECONOMICS" || id === "DEVELOPMENT_SUPPORT" || id === "OTHER") {
    return id === "OTHER" ? "RESEARCH_ONLY" : "RESEARCH_ONLY";
  }
  if (id === "CHAIN_SCALE_POSITIONING") return "BLOCKED";

  if (holdoutCases < 5) return holdoutPrecision != null && holdoutPrecision >= 0.85 ? "DETAIL_ONLY" : "RESEARCH_ONLY";

  if (
    holdoutPrecision != null &&
    holdoutPrecision >= 0.95 &&
    execFp === 0 &&
    brandErr === 0 &&
    collisionRisk !== "HIGH"
  ) {
    return "PRODUCTION_ELIGIBLE";
  }
  if (holdoutPrecision != null && holdoutPrecision >= 0.85) return "DETAIL_ONLY";
  return "RESEARCH_ONLY";
}

/**
 * Summarize taxonomy gating buckets after portfolio run.
 * @param {object[]} qualifiedNarratives
 * @param {Record<string, number|null>} [familyPrecision]
 */
export function summarizeTaxonomyGating(qualifiedNarratives = [], familyPrecision = {}) {
  const familiesWithEvidence = new Set(qualifiedNarratives.map((n) => n.narrativeFamily));
  const buckets = {
    PRODUCTION_ELIGIBLE: [],
    DETAIL_ONLY: [],
    RESEARCH_ONLY: [],
    BLOCKED: [],
  };

  for (const family of NARRATIVE_FAMILIES) {
    const state = classifyNarrativeFamilyProductionState(family, {
      hasEvidence: familiesWithEvidence.has(family),
      validationPrecision: familyPrecision[family] ?? null,
    });
    buckets[state].push(family);
  }
  for (const family of NARRATIVE_FAMILIES) {
    if (!familiesWithEvidence.has(family) && !buckets.BLOCKED.includes(family)) {
      buckets.BLOCKED.push(family);
    }
  }
  return buckets;
}

/**
 * @param {object} observation from narrative extractor
 */
export function classifyBrandRelationship(observation) {
  if (!observation?.entityBinding || observation.entityBinding !== "entity_bound") {
    return "AMBIGUOUS";
  }
  if (observation.explicitness === "EXPLICIT") return "EXPLICIT_BRAND_ASSOCIATION";
  return "IMPLICIT_BRAND_ASSOCIATION";
}

export function isExecutiveSafeBrandRelationship(relationship) {
  return (
    relationship === "EXPLICIT_BRAND_ASSOCIATION" ||
    relationship === "IMPLICIT_BRAND_ASSOCIATION"
  );
}

/**
 * Heuristic domain → source category (conservative; not an authority score).
 * @param {string} domain
 * @param {{ ownedExternal?: string|null }} [ctx]
 */
export function classifySourceCategory(domain, ctx = {}) {
  const d = String(domain || "").toLowerCase();
  if (!d) return "OTHER";
  if (ctx.ownedExternal === "OWNED") return "BRAND_OWNED";
  if (ctx.ownedExternal === "PARENT_OWNED") return "PARENT_COMPANY_OWNED";
  if (/\.gov$|ftc\.|sec\.|europa\.eu/.test(d)) return "GOVERNMENT_REGULATORY";
  if (/costar|str\.|hvs|cbre|jll|pwc|deloitte|mckinsey|bcg/.test(d)) {
    return "INDUSTRY_DATA";
  }
  if (/hotelnews|hospitalitynet|hotels\.com|skift|lodging|hotelmanagement/.test(d)) {
    return "TRADE_MEDIA";
  }
  if (/booking\.|expedia|tripadvisor|kayak|trivago/.test(d)) return "OTA_TRAVEL";
  if (/development|franchise|owners|investor|ir\./.test(d)) return "DEVELOPMENT_SITE";
  if (/residen|residences/.test(d)) return "RESIDENCES_SITE";
  if (/marriott|hilton|ihg|hyatt|accor|choicehotels/.test(d)) return "PARENT_COMPANY_OWNED";
  if (/nytimes|wsj|reuters|bloomberg|forbes|cnn/.test(d)) return "GENERAL_MEDIA";
  return "OTHER";
}

export function containsForbiddenSourceLanguage(text) {
  const upper = String(text || "").toUpperCase();
  return FORBIDDEN_SOURCE_LANGUAGE.some((token) => upper.includes(token));
}

export const NARRATIVE_METHODOLOGY_COPY =
  "Narrative Intelligence identifies recurring positioning themes in AI responses across hotel-owner decision prompts. Source Intelligence shows which owned and external sources are cited alongside those themes. Recurrence and citation indicate observed association, not causation.";
