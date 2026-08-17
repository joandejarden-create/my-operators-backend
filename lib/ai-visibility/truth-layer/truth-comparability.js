/**
 * Truth Layer dimension comparability + semantic classification (P0D-A.1).
 * Prevents cross-dimension false conflicts and contrastive-context misreads.
 */

import { normalizeMatchKey } from "../normalize-entities.js";

export const TRUTH_RULE_VERSION_SEMANTIC = "ai_visibility_truth_layer_v1_1_semantic";

/** Smallest defensible P0D-A taxonomy mapped to governed Brand Basics fields. */
export const P0DA_TRUTH_TAXONOMY = Object.freeze({
  PARENT_COMPANY: {
    governedField: "Parent Company",
    source: "Brand Basics",
    status: "PRODUCTION_VALIDATED",
  },
  CHAIN_SCALE: {
    governedField: "Hotel Chain Scale",
    source: "Brand Basics",
    status: "CONDITIONAL",
  },
  BRAND_ARCHITECTURE: {
    governedField: "Brand Model",
    source: "Brand Basics",
    note: "Brand Model field conflates architecture (Collection/Hard/Soft) with operating posture (Lifestyle/Conversion). P0D-A compares architecture claims only against architecture values.",
    status: "PRODUCTION_VALIDATED",
  },
  POSITIONING: {
    governedField: null,
    source: "NOT_STRUCTURED",
    status: "DEFERRED",
    note: "Lifestyle/luxury/upscale marketing language — not comparable to Brand Model architecture.",
  },
  OPERATING_MODEL: {
    governedField: null,
    source: "NOT_STRUCTURED",
    status: "DEFERRED",
    note: "Conversion/franchise/managed posture — not in Brand Model field as distinct dimension.",
  },
  SOFT_BRAND_COLLECTION_STATUS: {
    governedField: "Brand Architecture (inferred from Brand Model)",
    source: "Brand Basics",
    status: "PRODUCTION_VALIDATED",
  },
  BRAND_FAMILY: {
    governedField: "Parent Company",
    source: "Brand Basics",
    status: "PRODUCTION_VALIDATED",
  },
});

/** Semantic dimension inferred from AI claim value + context. */
export function inferAiSemanticDimension(claimType, claimValue, spanText = "") {
  const val = normalizeMatchKey(claimValue);
  const span = String(spanText || "").toLowerCase();

  if (claimType === "PARENT_COMPANY" || claimType === "BRAND_FAMILY") {
    return "PARENT_COMPANY";
  }
  if (claimType === "CHAIN_SCALE") {
    if (val.includes("lifestyle") || val.includes("boutique")) return "POSITIONING";
    return "CHAIN_SCALE";
  }
  if (claimType === "SOFT_BRAND_COLLECTION") {
    return "SOFT_BRAND_COLLECTION_STATUS";
  }
  if (claimType === "BRAND_MODEL") {
    if (val.includes("lifestyle")) return "POSITIONING";
    if (val.includes("conversion")) return "OPERATING_MODEL";
    if (val.includes("hard") || val.includes("soft") || val.includes("collection")) {
      return "BRAND_ARCHITECTURE";
    }
    return "BRAND_ARCHITECTURE";
  }
  return claimType;
}

/** Dealality fact semantic dimension from governed field. */
export function inferDealalitySemanticDimension(factType, factValue) {
  const val = normalizeMatchKey(factValue);
  if (factType === "PARENT_COMPANY" || factType === "BRAND_FAMILY") return "PARENT_COMPANY";
  if (factType === "CHAIN_SCALE") return "CHAIN_SCALE";
  if (factType === "SOFT_BRAND_COLLECTION") return "SOFT_BRAND_COLLECTION_STATUS";
  if (factType === "BRAND_MODEL") {
    if (val.includes("lifestyle")) return "POSITIONING";
    if (val.includes("collection") || val.includes("soft") || val.includes("hard")) {
      return "BRAND_ARCHITECTURE";
    }
    return "BRAND_ARCHITECTURE";
  }
  return factType;
}

/** Pairwise comparability matrix. */
export const DIMENSION_COMPARABILITY = Object.freeze({
  "PARENT_COMPANY|PARENT_COMPANY": "YES",
  "BRAND_ARCHITECTURE|BRAND_ARCHITECTURE": "YES",
  "CHAIN_SCALE|CHAIN_SCALE": "YES",
  "SOFT_BRAND_COLLECTION_STATUS|SOFT_BRAND_COLLECTION_STATUS": "YES",
  "SOFT_BRAND_COLLECTION_STATUS|BRAND_ARCHITECTURE": "CONDITIONAL",
  "BRAND_ARCHITECTURE|SOFT_BRAND_COLLECTION_STATUS": "CONDITIONAL",
  "BRAND_FAMILY|PARENT_COMPANY": "YES",
  "PARENT_COMPANY|BRAND_FAMILY": "YES",
  "POSITIONING|BRAND_ARCHITECTURE": "NO",
  "BRAND_ARCHITECTURE|POSITIONING": "NO",
  "POSITIONING|CHAIN_SCALE": "NO",
  "CHAIN_SCALE|POSITIONING": "NO",
  "OPERATING_MODEL|BRAND_ARCHITECTURE": "NO",
  "BRAND_ARCHITECTURE|OPERATING_MODEL": "NO",
  "OPERATING_MODEL|CHAIN_SCALE": "NO",
  "CHAIN_SCALE|OPERATING_MODEL": "NO",
  "POSITIONING|POSITIONING": "NO",
});

/**
 * @param {string} aiDim
 * @param {string} dealDim
 */
export function areDimensionsComparable(aiDim, dealDim) {
  if (aiDim === dealDim) return "YES";
  const key = `${aiDim}|${dealDim}`;
  return DIMENSION_COMPARABILITY[key] || "NO";
}

/** Mutually exclusive normalized architecture values in Dealality semantics. */
export const ARCHITECTURE_MUTUAL_EXCLUSIVITY = Object.freeze({
  COLLECTION: ["hard brand", "hard"],
  HARD_BRAND: ["collection brand", "collection", "soft brand", "soft"],
  SOFT_BRAND: ["hard brand", "hard"],
});

/** Normalize architecture bucket. */
export function normalizeArchitectureBucket(value) {
  const v = normalizeMatchKey(value);
  if (v.includes("collection")) return "COLLECTION";
  if (v.includes("soft")) return "SOFT_BRAND";
  if (v.includes("hard")) return "HARD_BRAND";
  if (v.includes("lifestyle")) return "LIFESTYLE_POSITIONING";
  if (v.includes("conversion")) return "CONVERSION_OPERATING";
  return v.toUpperCase().replace(/\s+/g, "_");
}

/** Contrastive context — AI mentions dimension term about OTHER brands, not subject attribute. */
export function isContrastiveMention(spanText, claimValue, subjectBrandName) {
  const span = String(spanText || "");
  const lower = span.toLowerCase();
  const claim = normalizeMatchKey(claimValue);

  const contrastPatterns = [
    /\b(?:than|unlike|compared to|versus|vs\.?|rather than|instead of|compared with)\s+(?:a\s+)?(?:traditional\s+)?(?:strict\s+)?(?:hard|soft)\s+brand/i,
    /\b(?:hard|soft)\s+brand(?:s)?\s+(?:would|will|could|might|may)\b/i,
    /\b["']?(?:hard|soft)["']?\s+(?:brand(?:s)?|flags?)\s+(?:like|such as|including)\b/i,
    /\b(?:hard|soft)\s+flags?\s+like\b/i,
    /\bcompared to (?:strict )?(?:hard|soft) brands?\b/i,
    /\bmenores restricciones.*(?:hard brand|marca dura)/i,
    /\bhard brand would dilute\b/i,
    /\bsoft brands?\s*\([^)]+\).*compared to.*(?:hard|strict)/i,
    /\b(?:hard|soft)\s+brands?\s*\([^)]*(?:Westin|Marriott|Hilton|Hyatt|Grand Hyatt)[^)]*\)/i,
    /\b(?:Autograph|Curio|Tribute).*hard brand would/i,
    /\bhard brand would dilute the concept/i,
    /\bfewer fixed standards than hard brands?\b/i,
    /\bmore flexibility than (?:a )?(?:traditional )?hard brand/i,
    /\bsoft brands?\s*\([^)]+\).*(?:hard brands?|hard flags?)\s*\(/i,
    /\bhard flags?\s+like\b/i,
    /\bmarca dura como\b/i,
  ];

  if (contrastPatterns.some((re) => re.test(span))) return true;

  if (claim.includes("hard") && /\bhard brand would\b/i.test(lower)) return true;
  if (claim.includes("soft") && /\b["']?hard["']?\s+flags?\s+like\b/i.test(lower)) return true;
  if (claim.includes("hard") && /\bsoft brands?\s*\(/i.test(lower)) return true;

  return false;
}

/** Chain scale claim extracted from peer-list / brand-name noise (e.g. "Luxury Collection"). */
export function isChainScaleListNoise(spanText, claimValue, subjectBrandName) {
  const span = String(spanText || "");
  const claim = normalizeMatchKey(claimValue);
  if (claim.includes("luxury") && /\bluxury collection\b/i.test(span)) return true;
  if (/\b(?:Autograph|Tribute|Design Hotels|AC Hotels)[^.\n]{0,120}(?:,|\|)/i.test(span) && span.split(",").length >= 3) {
    if (!new RegExp(`${subjectBrandName?.split(/\s+/)[0] || "____"}[^\\n]{0,40}\\b${claimValue}`, "i").test(span)) {
      return true;
    }
  }
  return false;
}

/** Portfolio range table — not a subject-specific chain scale claim. */
export function isPortfolioRangeChainScale(spanText) {
  const span = String(spanText || "");
  return (
    /\bmidscale[\s–-]+luxury\b/i.test(span) ||
    /\bmidscale to upper/i.test(span) ||
    /\b(?:economy|midscale|upscale|luxury)[\s–-]+(?:economy|midscale|upscale|luxury|luxury)\b/i.test(span) ||
    (/\|/.test(span) && /midscale/i.test(span) && /luxury/i.test(span))
  );
}

/** List enumeration — subject mentioned in peer list with different attribute term. */
export function isListEnumerationClaim(spanText, claimValue, subjectBrandName) {
  const span = String(spanText || "");
  const name = String(subjectBrandName || "");
  if (!name) return false;

  const lifestyleList =
    normalizeMatchKey(claimValue).includes("lifestyle") &&
    /(?:such as|including|consider|brands like|commonly cited)/i.test(span) &&
    span.split(",").length >= 3;

  const conversionList =
    normalizeMatchKey(claimValue).includes("conversion") &&
    /conversion brands/i.test(span) &&
    span.split(",").length >= 3;

  const portfolioGrouping =
    normalizeMatchKey(claimValue).includes("collection") &&
    /(?:alongside|within its|premium full-service|collection brands)/i.test(span) &&
    /\b(?:Westin|Sheraton|Marriott Hotels)\b/i.test(span);

  return lifestyleList || conversionList || portfolioGrouping;
}

/** Parent claim that is positioning language, not corporate parent. */
export function isNonParentParentClaim(spanText, claimValue) {
  const span = String(spanText || "").toLowerCase();
  const val = String(claimValue || "").toLowerCase();
  if (/softer entry point into/i.test(span)) return true;
  if (/entry point into/i.test(val)) return true;
  if (!/\b(?:part of|owned by|operated by|portfolio of|subsidiary)\b/i.test(span)) {
    if (/the marriott/i.test(val) && /portfolio/i.test(span)) return false;
    if (val.length > 40) return true;
  }
  return false;
}

/** Classify gap for audit reporting. */
export function classifySemanticAudit(comparison, subjectBrandName = "") {
  const span = comparison.aiSupportingSpan || "";
  const aiDim = inferAiSemanticDimension(comparison.aiClaimType, comparison.aiClaimValue, span);
  const dealDim = inferDealalitySemanticDimension(comparison.dealalityFactType, comparison.dealalityFactValue);
  const comparability = areDimensionsComparable(aiDim, dealDim);

  if (comparability === "NO") return "CROSS_DIMENSION_NON_CONFLICT";
  if (isContrastiveMention(span, comparison.aiClaimValue, subjectBrandName)) return "AMBIGUOUS_NOT_COMPARABLE";
  if (isListEnumerationClaim(span, comparison.aiClaimValue, subjectBrandName)) return "AMBIGUOUS_NOT_COMPARABLE";
  if (isPortfolioRangeChainScale(span)) return "AMBIGUOUS_NOT_COMPARABLE";
  if (comparison.aiClaimType === "PARENT_COMPANY" && isNonParentParentClaim(span, comparison.aiClaimValue)) {
    return "AMBIGUOUS_NOT_COMPARABLE";
  }
  if (comparison.comparisonReason === "parent_company_match" || comparison.comparisonStatus === "ALIGNED") {
    return "TERMINOLOGY_VARIATION";
  }

  const aiBucket = normalizeArchitectureBucket(comparison.aiClaimValue);
  const dealBucket = normalizeArchitectureBucket(comparison.dealalityFactValue);
  if (aiBucket === dealBucket) return "TERMINOLOGY_VARIATION";

  if (comparison.comparisonStatus === "POTENTIAL_PERCEPTION_GAP") {
    return "TRUE_SAME_DIMENSION_CONFLICT";
  }
  return "AMBIGUOUS_NOT_COMPARABLE";
}
