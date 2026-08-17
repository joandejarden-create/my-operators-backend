/**
 * Factual brand archetypes derived from Brand Basics Brand Model + Chain Scale.
 * Multiple archetypes may apply. No invented values.
 */

export const BRAND_ARCHETYPE_VERSION = "ai_visibility_brand_archetype_v1";

export const ARCHETYPE = Object.freeze({
  COLLECTION: "COLLECTION",
  SOFT_BRAND: "SOFT_BRAND",
  HARD_BRAND: "HARD_BRAND",
  LIFESTYLE: "LIFESTYLE",
  UPPER_UPSCALE: "UPPER_UPSCALE",
  LUXURY: "LUXURY",
  OTHER: "OTHER",
});

/**
 * @param {{ brandModel?: string|null, chainScale?: string|null }} brand
 */
export function deriveBrandArchetypes(brand = {}) {
  const model = String(brand.brandModel || "").trim();
  const scale = String(brand.chainScale || "").trim();
  const archetypes = [];
  const sources = [];

  if (model === "Collection Brand") {
    archetypes.push(ARCHETYPE.COLLECTION, ARCHETYPE.SOFT_BRAND);
    sources.push("Brand Basics Brand Model=Collection Brand");
  } else if (model === "Hard Brand") {
    archetypes.push(ARCHETYPE.HARD_BRAND);
    sources.push("Brand Basics Brand Model=Hard Brand");
  } else if (model === "Lifestyle Brand") {
    archetypes.push(ARCHETYPE.LIFESTYLE);
    sources.push("Brand Basics Brand Model=Lifestyle Brand");
  } else if (model) {
    archetypes.push(ARCHETYPE.OTHER);
    sources.push(`Brand Basics Brand Model=${model}`);
  }

  if (scale === "Upper Upscale") {
    archetypes.push(ARCHETYPE.UPPER_UPSCALE);
    sources.push("Brand Basics Hotel Chain Scale=Upper Upscale");
  } else if (scale === "Luxury") {
    archetypes.push(ARCHETYPE.LUXURY);
    sources.push("Brand Basics Hotel Chain Scale=Luxury");
  }

  return {
    archetypes: [...new Set(archetypes)],
    source: sources.join("; ") || "Brand Basics",
    quality: model && scale ? "HIGH" : model || scale ? "MEDIUM" : "LOW",
    version: BRAND_ARCHETYPE_VERSION,
  };
}
