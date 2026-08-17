/**
 * Lifestyle / affiliation copy governance modes v35B.
 */
import { GENERIC_BOILERPLATE_PATTERNS } from "./brand-explorer-active-profile-copy-governance-config.js";

export const LIFESTYLE_COPY_GOVERNANCE_VERSION = "v35B";

export const AFFILIATION_FRANCHISE_LANGUAGE_PATTERNS = Object.freeze([
  /\bfranchise flag\b/i,
  /\bfranchise conversion\b/i,
  /\bstandard prototype\b/i,
  /\bfranchise disclosure\b/i,
  /\bitem\s*19\b/i,
  /\bfdd\b/i,
  /\broyalty on gross\b/i,
  /\bfranchise fee schedule\b/i,
  /\bconfirm fees in (the )?fdd\b/i,
]);

export const AFFILIATION_COPY_MODES = Object.freeze({
  affiliation_curation_platform: {
    mode: "affiliation_curation_platform",
    label: "Affiliation / curation platform",
    positioningPillars: [
      "independent identity preservation",
      "design-led and culturally distinctive hotel fit",
      "architecture, storytelling, and local identity",
      "distribution and recognition value",
      "owner control and curation expectations",
    ],
    blockedLanguage: [...AFFILIATION_FRANCHISE_LANGUAGE_PATTERNS],
    founderNotes: [
      "Never use franchise-flag, FDD, Item 19, or prototype conversion language.",
      "Frame as curation, affiliation, collection membership, and distribution — not franchise.",
      "Preserve property-level design story and independent character.",
    ],
    genericBoilerplateBlocked: true,
  },
  independent_luxury_consortium: {
    mode: "independent_luxury_consortium",
    label: "Independent luxury consortium",
    positioningPillars: [
      "independent luxury and boutique hotel fit",
      "quality and guest-experience expectations",
      "affiliation and distribution value",
      "owner control",
      "luxury credibility without chain flag",
    ],
    blockedLanguage: [...AFFILIATION_FRANCHISE_LANGUAGE_PATTERNS, /\bparent brand\b/i, /\bchain flag\b/i],
    founderNotes: [
      "Never use parent-brand or franchise language.",
      "Frame as consortium membership, quality standards, and distribution reach.",
      "Legal/consortium sensitivity — source-backed claims only.",
    ],
    genericBoilerplateBlocked: true,
  },
  soft_brand_collection: {
    mode: "soft_brand_collection",
    label: "Soft-brand collection",
    positioningPillars: [
      "parent platform context",
      "soft-brand lifestyle positioning",
      "conversion and repositioning fit",
      "loyalty and distribution context",
      "standards and owner obligations with flexibility",
    ],
    blockedLanguage: AFFILIATION_FRANCHISE_LANGUAGE_PATTERNS.filter((p) => !/fdd/i.test(p.source)),
    founderNotes: [
      "Use soft-collection framing — not rigid flag conversion language.",
      "Emphasize owner flexibility, local character, and parent platform benefits.",
      "No FDD / Item 19 / ADR / performance claims in owner-facing copy.",
    ],
    genericBoilerplateBlocked: true,
  },
  lifestyle_full_brand: {
    mode: "lifestyle_full_brand",
    label: "Lifestyle full brand",
    positioningPillars: [
      "neighborhood and local discovery positioning",
      "lifestyle hotel brand within parent system",
      "IHG / parent distribution and loyalty context",
      "conversion and new-build fit where source-supported",
      "design narrative and guest experience authenticity",
    ],
    blockedLanguage: [
      ...AFFILIATION_FRANCHISE_LANGUAGE_PATTERNS,
      /\bintercontinental hotels?\b(?!.*group)/i,
      /\bluxury soft[- ]brand\b/i,
      /\bgeneric boutique hotel\b/i,
      /\bunsupported loyalty\b/i,
    ],
    founderNotes: [
      "Use lifestyle hotel brand framing — neighborhood/local discovery only when source-supported.",
      "Distinguish IHG parent (InterContinental Hotels Group) from InterContinental luxury brand.",
      "No franchise fee/FDD copy unless source-supported and owner-safe.",
      "No unsupported loyalty or performance claims.",
    ],
    genericBoilerplateBlocked: true,
  },
});

function affiliationSlotRewrites(brandName, parentPlatform) {
  return {
    "overview.featured_application": {
      title: "Owner Fit",
      body: `${brandName} fits independent, design-led hotels where owners want curated global recognition, local identity preservation, and ${parentPlatform} distribution without a standardized franchise prototype. Owners should diligence curation standards, design narrative, and affiliation value during evaluation.`,
      sourceRefs: ["consumerUrl", "developmentUrl"],
    },
    "overview.portfolio_context": {
      title: "Collection Context",
      body: `${brandName} operates as a design-led affiliation and curation platform—connecting distinctive independent hotels to ${parentPlatform} distribution and guest recognition while preserving property-level identity and storytelling.`,
      sourceRefs: ["consumerUrl", "developmentUrl"],
    },
    portfolio_context: {
      title: "Affiliation Context",
      body: `Owners evaluating ${brandName} compare curation fit, design credibility, owner control, and distribution reach—not franchise prototype compliance or flag conversion economics.`,
      sourceRefs: ["consumerUrl"],
    },
  };
}

function slhSlotRewrites() {
  return {
    "overview.featured_application": {
      title: "Independent Luxury Fit",
      body:
        "Small Luxury Hotels of the World fits owner-operated luxury boutique properties where independent character, guest experience quality, and consortium credibility matter. Owners should diligence SLH participation standards, quality expectations, and affiliation value—not chain-flag conversion.",
      sourceRefs: ["consumerUrl", "developmentUrl"],
    },
    "overview.portfolio_context": {
      title: "Consortium Context",
      body:
        "SLH connects independent luxury hotels to global consortium distribution and recognition—owners retain brand identity while accessing affiliation benefits, quality benchmarks, and guest-experience expectations suited to boutique luxury.",
      sourceRefs: ["consumerUrl"],
    },
    portfolio_context: {
      title: "Affiliation Value",
      body:
        "Owners evaluating SLH compare luxury credibility, owner control, consortium distribution reach, and participation standards during diligence—without franchise-flag or parent-brand conversion framing.",
      sourceRefs: ["consumerUrl"],
    },
  };
}

function lifestyleFullBrandSlotRewrites(brandName, parentPlatform) {
  return {
    "overview.featured_application": {
      title: "Lifestyle Owner Fit",
      body: `${brandName} suits urban and neighborhood lifestyle hotels where local discovery, design narrative, and guest experience authenticity matter—within ${parentPlatform} distribution and loyalty participation. Owners should evaluate conversion scope, IHG systems fit, and neighborhood positioning during diligence.`,
      sourceRefs: ["consumerUrl", "developmentUrl"],
    },
    "overview.portfolio_context": {
      title: "IHG Lifestyle Context",
      body: `${brandName} sits within ${parentPlatform}'s lifestyle portfolio—relevant when owners want a full lifestyle brand with IHG One Rewards and distribution reach without InterContinental luxury-brand positioning.`,
      sourceRefs: ["consumerUrl", "developmentUrl"],
    },
    portfolio_context: {
      title: "Neighborhood Lifestyle Positioning",
      body: `Owners comparing ${brandName} evaluate neighborhood boutique fit, conversion/new-build implications, and IHG platform benefits—using source-backed claims only for loyalty and performance context.`,
      sourceRefs: ["consumerUrl"],
    },
  };
}

function softBrandSlotRewrites(brandName, parentPlatform) {
  return {
    "overview.featured_application": {
      title: "Soft-Collection Owner Fit",
      body: `${brandName} suits lifestyle and independent-character hotels seeking ${parentPlatform} distribution, loyalty participation, and commercial systems while retaining local identity. Owners should evaluate conversion scope, standards intensity, and owner flexibility during diligence.`,
      sourceRefs: ["consumerUrl", "developmentUrl"],
    },
    "overview.portfolio_context": {
      title: "Parent Platform Context",
      body: `${brandName} sits within ${parentPlatform}'s soft-collection portfolio—relevant when owners want affiliation benefits and distribution reach without a rigid full-service flag conversion.`,
      sourceRefs: ["consumerUrl"],
    },
    portfolio_context: {
      title: "Collection Positioning",
      body: `Owners comparing ${brandName} evaluate lifestyle positioning, repositioning fit, loyalty context, and standards obligations alongside owner flexibility within the ${parentPlatform} platform.`,
      sourceRefs: ["consumerUrl"],
    },
  };
}

export function buildLifestyleCopyGovernanceConfig(brandConfig) {
  const mode = AFFILIATION_COPY_MODES[brandConfig.copyGovernanceMode];
  if (!mode) return null;

  const parentPlatform = brandConfig.parentCompany || "parent platform";
  let slotRewrites = {};
  if (brandConfig.copyGovernanceMode === "affiliation_curation_platform") {
    slotRewrites = affiliationSlotRewrites(brandConfig.name, parentPlatform);
  } else if (brandConfig.copyGovernanceMode === "independent_luxury_consortium") {
    slotRewrites = slhSlotRewrites();
  } else if (brandConfig.copyGovernanceMode === "lifestyle_full_brand") {
    slotRewrites = lifestyleFullBrandSlotRewrites(brandConfig.name, parentPlatform);
  } else {
    slotRewrites = softBrandSlotRewrites(brandConfig.name, parentPlatform);
  }

  return {
    brandName: brandConfig.name,
    parentPlatform,
    segment: mode.label,
    copyGovernanceMode: mode.mode,
    positioningPillars: mode.positioningPillars,
    founderNotes: mode.founderNotes,
    blockedLanguagePatterns: mode.blockedLanguage,
    genericBoilerplatePatterns: mode.genericBoilerplateBlocked ? GENERIC_BOILERPLATE_PATTERNS : [],
    consumerUrl: brandConfig.consumerUrl,
    developmentUrl: brandConfig.developmentUrl || null,
    slotRewrites,
    copyRepairTargets: [],
    franchiseLanguageBlocked: brandConfig.franchiseLanguageBlocked === true,
  };
}

export const LIFESTYLE_COPY_GOVERNANCE_BY_BRAND = Object.freeze({
  "design-hotels": null,
  "small-luxury-hotels-of-the-world": null,
  "autograph-collection": null,
  "tribute-portfolio": null,
  "vignette-collection": null,
  "mgallery-collection": null,
  "hotel-indigo": null,
  "handwritten-collection": null,
});

export function getLifestyleCopyGovernanceForSlug(brandSlug, brandConfig) {
  if (!brandConfig) return null;
  return buildLifestyleCopyGovernanceConfig(brandConfig);
}
