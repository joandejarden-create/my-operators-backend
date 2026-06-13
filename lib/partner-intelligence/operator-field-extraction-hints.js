/**
 * Extraction hints for Operator Explorer registry fields (rule + keyword pass).
 * Expand over time; meta rollup fields are skipped in extraction.
 */

/** @type {Record<string, { patterns?: RegExp[], keywords?: string[], capability?: boolean }>} */
export const OPERATOR_FIELD_EXTRACTION_HINTS = {
  "op.snapshot.companyName": {
    patterns: [/Arbor Lodging(?:\s+Partners|\s+\(CALA\))?/i],
    keywords: ["company name", "arbor lodging"],
  },
  "op.snapshot.companyDescription": {
    keywords: ["about us", "who we are", "company overview", "hotel management", "vertically integrated"],
  },
  "op.snapshot.parentCompany": {
    keywords: ["parent company", "subsidiary", "arbor lodging partners", "corporate structure"],
  },
  "op.snapshot.primaryServiceModel": {
    patterns: [/third[- ]party (?:hotel )?management/i, /hotel management/i, /asset management/i],
    keywords: ["service model", "management company", "operator"],
  },
  "op.snapshot.totalProperties": {
    patterns: [/(\d{1,4})\s+hotels?/i, /portfolio of\s+(\d{1,4})/i],
    keywords: ["properties", "hotels", "portfolio"],
  },
  "op.snapshot.totalRooms": {
    patterns: [/([\d,]+)\s+rooms?/i, /keys under management/i],
    keywords: ["rooms", "keys", "room count"],
  },
  "op.snapshot.differentiators": {
    keywords: ["differentiator", "what sets us apart", "why owners", "competitive advantage"],
  },
  "op.markets.activeCountries": {
    keywords: ["mexico", "united states", "caribbean", "latin america", "cala", "countries"],
  },
  "op.markets.activeMarkets": {
    keywords: ["mexico city", "cabo", "los cabos", "riviera maya", "markets", "cities"],
  },
  "op.markets.geographicPriorities": {
    keywords: ["priority markets", "growth markets", "expansion", "geographic focus"],
  },
  "op.brand.familiesOperated": {
    keywords: ["marriott", "hilton", "hyatt", "ihg", "choice", "brand families", "brand relationships"],
  },
  "op.brand.brandedVsIndependentMix": {
    keywords: ["independent", "soft brand", "branded", "lifestyle", "collection"],
  },
  "op.platform.preOpeningSupport": {
    capability: true,
    keywords: ["pre-opening", "preopening", "opening support", "ramp-up", "ramp up"],
  },
  "op.platform.conversionExperience": {
    capability: true,
    keywords: ["conversion", "repositioning", "reflag", "rebrand", "transition"],
  },
  "op.platform.revenueManagement": {
    capability: true,
    keywords: ["revenue management", "yield", "pricing", "revpar", "commercial"],
  },
  "op.engagement.ownerReportingLevel": {
    keywords: ["owner reporting", "reporting package", "monthly reporting", "owner communication"],
  },
  "op.dealFit.bestFitOwnerTypes": {
    keywords: ["owner type", "institutional", "private equity", "family office", "best fit"],
  },
  "op.dealFit.minPropertySize": {
    patterns: [/minimum\s+(\d{2,4})\s+rooms?/i, /at least\s+(\d{2,4})\s+rooms?/i],
    keywords: ["minimum", "room count", "property size"],
  },
  "op.dealFit.maxPropertySize": {
    patterns: [/maximum\s+(\d{2,4})\s+rooms?/i, /up to\s+(\d{2,4})\s+rooms?/i],
    keywords: ["maximum", "ideal size", "large format"],
  },
  "op.materials.galleryOverview": {
    keywords: ["materials", "presentation", "brochure", "deck", "case study"],
  },
};

export function isMetaExtractionField(fieldKey) {
  return fieldKey.startsWith("op.meta.");
}
