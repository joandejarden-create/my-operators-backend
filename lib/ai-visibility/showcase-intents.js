/**
 * Phase 3A.9 — Active Brand AI Visibility showcase decision territories.
 * Eligibility terminology only (not Suitability / fit scoring).
 */

export const SHOWCASE_INTENT_GOVERNANCE_VERSION = "ai_visibility_showcase_intents_v1";

/** Active Wave-1 showcase intents (exactly six). */
export const ACTIVE_SHOWCASE_INTENTS = Object.freeze([
  "Conversion",
  "Collection / Soft Brand",
  "Lifestyle Positioning",
  "Upper-Upscale Positioning",
  "Branded Residences",
  "Soft-Brand Affiliation Flexibility",
]);

/** Deferred from first showcase prompt wave (may remain in eligibility config). */
export const DEFERRED_SHOWCASE_INTENTS = Object.freeze([
  "New Build",
  "Mixed Use",
  "Owner Economics / Flexibility",
  "Branded Residences / Mixed Use",
]);

/**
 * @typedef {object} ShowcaseIntentDefinition
 * @property {string} intentId
 * @property {string} displayName
 * @property {string} definition
 * @property {string} ownerDecision
 * @property {string[]} eligibleBrandArchetypes
 * @property {string[]} notEligibleBrandArchetypes
 * @property {string} unknownHandling
 * @property {string} geographicRelevance
 * @property {string} whyItBelongsInShowcase
 * @property {string} eligibilityTerritoryKey — brand_decision_eligibility territory name
 */

/** @type {Record<string, ShowcaseIntentDefinition>} */
export const SHOWCASE_INTENT_DEFINITIONS = Object.freeze({
  Conversion: {
    intentId: "conversion",
    displayName: "Conversion",
    definition:
      "Owner/developer consideration of Brands for repositioning or converting an existing hotel asset.",
    ownerDecision:
      "Which Brands belong in the addressable conversion / reflag consideration set for an existing hotel.",
    eligibleBrandArchetypes: ["Active development brands in the governed peer cohort"],
    notEligibleBrandArchetypes: [],
    unknownHandling: "UNKNOWN remains observable; never coerced to NOT_ELIGIBLE.",
    geographicRelevance: "Global, CALA, Europe, North America, Mexico (country)",
    whyItBelongsInShowcase: "Core owner decision; full ELIGIBLE density in cohort.",
    eligibilityTerritoryKey: "Conversion",
  },
  "Collection / Soft Brand": {
    intentId: "collection_soft_brand",
    displayName: "Collection / Soft Brand",
    definition:
      "Owner consideration of collection / soft-brand affiliation options where the hotel seeks Brand affiliation while preserving meaningful individuality.",
    ownerDecision:
      "Which collection / soft-brand affiliation options are addressable for an individuality-preserving affiliation decision.",
    eligibleBrandArchetypes: ["Collection / soft-brand Brand Model"],
    notEligibleBrandArchetypes: ["Hard lifestyle / full-service brands without Collection model"],
    unknownHandling: "UNKNOWN remains UNKNOWN; do not infer Collection from Lifestyle.",
    geographicRelevance: "Global, CALA, Europe, North America, Mexico",
    whyItBelongsInShowcase: "Primary Marriott/Hilton/Choice soft-brand competitive territory.",
    eligibilityTerritoryKey: "Collection / Soft Brand",
  },
  "Lifestyle Positioning": {
    intentId: "lifestyle_positioning",
    displayName: "Lifestyle Positioning",
    definition:
      "Owner/developer consideration of Brands positioned around differentiated lifestyle identity, experience, design, or local character.",
    ownerDecision:
      "Which Brands are addressable when pursuing a lifestyle positioning strategy for a hotel asset.",
    eligibleBrandArchetypes: ["Governed lifestyle / design positioning"],
    notEligibleBrandArchetypes: ["Explicitly non-lifestyle hard brands"],
    unknownHandling:
      "Collection brands without governed lifestyle positioning stay UNKNOWN — Collection ≠ Lifestyle.",
    geographicRelevance: "Global, CALA, Europe, North America, Mexico",
    whyItBelongsInShowcase: "Owner positioning decision distinct from Collection affiliation.",
    eligibilityTerritoryKey: "Lifestyle Positioning",
  },
  "Upper-Upscale Positioning": {
    intentId: "upper_upscale_positioning",
    displayName: "Upper-Upscale Positioning",
    definition:
      "Owner/developer consideration of Brand options appropriate to an upper-upscale hotel positioning strategy.",
    ownerDecision:
      "Which Brands are addressable for an upper-upscale hotel positioning / chain-scale strategy.",
    eligibleBrandArchetypes: ["Hotel Chain Scale = Upper Upscale"],
    notEligibleBrandArchetypes: ["Upscale / other chain scales outside Upper Upscale"],
    unknownHandling: "UNKNOWN if Chain Scale missing; never invent Upper Upscale.",
    geographicRelevance: "Global, CALA, Europe, North America, Mexico",
    whyItBelongsInShowcase: "Chain-scale positioning is a primary owner shortlist filter.",
    eligibilityTerritoryKey: "Upper-Upscale Positioning",
  },
  "Branded Residences": {
    intentId: "branded_residences",
    displayName: "Branded Residences",
    definition:
      "Owner/developer consideration of hotel Brands with governed branded-residences capability or case-by-case residences support.",
    ownerDecision:
      "Which Brands are addressable for a branded-residences hotel development decision.",
    eligibleBrandArchetypes: ["Branded Residences Status = Yes or Case-by-Case"],
    notEligibleBrandArchetypes: ["Branded Residences Status = No"],
    unknownHandling: "Not Confirmed / blank → UNKNOWN.",
    geographicRelevance: "Global, CALA, Europe, North America, Mexico",
    whyItBelongsInShowcase: "Governed Brand Basics field; Mixed Use deliberately excluded.",
    eligibilityTerritoryKey: "Branded Residences",
  },
  "Soft-Brand Affiliation Flexibility": {
    intentId: "soft_brand_affiliation_flexibility",
    displayName: "Soft-Brand Affiliation Flexibility",
    definition:
      "Owner consideration of collection / soft-brand affiliation where maintaining greater hotel individuality and affiliation flexibility is relevant. Not fee economics or franchise underwriting.",
    ownerDecision:
      "Which Brands are addressable when the owner prioritizes affiliation flexibility and individuality under a soft/collection model.",
    eligibleBrandArchetypes: ["Collection / soft-brand Brand Model"],
    notEligibleBrandArchetypes: [],
    unknownHandling: "Non-Collection brands → UNKNOWN (not fee-judged NOT_ELIGIBLE).",
    geographicRelevance: "Global, CALA, Europe, North America, Mexico",
    whyItBelongsInShowcase:
      "Narrowed from broad Owner Economics; grounded in Brand Model only.",
    eligibilityTerritoryKey: "Soft-Brand Affiliation Flexibility",
  },
});

export function isActiveShowcaseIntent(name) {
  return ACTIVE_SHOWCASE_INTENTS.includes(String(name || "").trim());
}

export function getShowcaseIntentDefinition(displayName) {
  return SHOWCASE_INTENT_DEFINITIONS[displayName] || null;
}

/**
 * Map Airtable / prompt Intent Territory to eligibility territory key.
 * @param {string} intentTerritory
 */
export function resolveEligibilityTerritoryKey(intentTerritory) {
  const name = String(intentTerritory || "").trim();
  if (SHOWCASE_INTENT_DEFINITIONS[name]) {
    return SHOWCASE_INTENT_DEFINITIONS[name].eligibilityTerritoryKey;
  }
  // Legacy aliases
  if (name === "Owner Flexibility" || name === "Owner Economics") {
    return "Soft-Brand Affiliation Flexibility";
  }
  if (name === "Branded Residences / Mixed Use") return "Branded Residences";
  if (name === "Chain Scale / Positioning") return "Upper-Upscale Positioning";
  if (name === "Brand Selection") return null;
  return name || null;
}
