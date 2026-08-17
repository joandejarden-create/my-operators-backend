/**
 * Controlled AI Brand Association attribute taxonomy (P0B research).
 * Not client-facing. Not LLM-generated. Governed definitions only.
 */

export const ASSOCIATION_TAXONOMY_VERSION = "ai_visibility_association_taxonomy_v1";

export const ASSOCIATION_POLARITIES = Object.freeze([
  "POSITIVE",
  "NEGATIVE",
  "MIXED",
  "NEUTRAL_DESCRIPTIVE",
]);

export const ASSOCIATION_EXPLICITNESS = Object.freeze(["EXPLICIT", "IMPLICIT"]);

export const GOLDEN_HUMAN_LABELS = Object.freeze([
  "POSITIVE",
  "NEGATIVE",
  "MIXED",
  "NEUTRAL_DESCRIPTIVE",
  "NO_ASSOCIATION",
  "AMBIGUOUS",
]);

/** Deferred from P0 production eligibility — research may tag but not ship. */
export const DEFERRED_ATTRIBUTE_IDS = Object.freeze(["ECONOMICS", "DEVELOPMENT_SUPPORT"]);

/** Stronger validation threshold required before client publication. */
export const HIGH_RISK_ATTRIBUTE_IDS = Object.freeze([
  "OWNER_FLEXIBILITY",
  "OWNER_CONTROL",
  "ECONOMICS",
  "DEVELOPMENT_SUPPORT",
  "OPERATING_MODEL",
  "MARKET_FIT",
  "CONVERSION_SUITABILITY",
]);

/**
 * @typedef {object} AssociationAttributeDef
 * @property {string} attributeId
 * @property {string} clientLabel
 * @property {string} definition
 * @property {string[]} inScopeLanguage
 * @property {string[]} outOfScopeLanguage
 * @property {string[]} positiveExamples
 * @property {string[]} negativeExamples
 * @property {string[]} ambiguousExamples
 * @property {string[]} commonFalsePositives
 * @property {boolean} highRisk
 * @property {boolean} deferred
 * @property {boolean} requiresCitation
 */

/** @type {AssociationAttributeDef[]} */
export const ASSOCIATION_ATTRIBUTES = Object.freeze([
  {
    attributeId: "INDEPENDENT_IDENTITY",
    clientLabel: "Independent Identity",
    definition:
      "Brand allows or emphasizes independent hotel identity, non-chain character, or owner uniqueness versus standardized chain sameness.",
    inScopeLanguage: [
      "independent identity",
      "preserve individuality",
      "unique identity",
      "distinct character",
      "non-standardized",
    ],
    outOfScopeLanguage: ["independent hotel owner", "independent operator", "independent review"],
    positiveExamples: [
      "Autograph allows owners to preserve a distinctive independent identity.",
    ],
    negativeExamples: ["Autograph feels like a standardized chain product."],
    ambiguousExamples: ["Autograph offers a unique guest experience."],
    commonFalsePositives: ["independent hotel", "independently owned"],
    highRisk: false,
    deferred: false,
    requiresCitation: false,
  },
  {
    attributeId: "DESIGN_INDIVIDUALITY",
    clientLabel: "Design Individuality",
    definition:
      "Explicit link between brand and design freedom, local character, bespoke design, or soft-brand individuality.",
    inScopeLanguage: [
      "design individuality",
      "design freedom",
      "local character",
      "bespoke design",
      "unique design",
      "individual design",
    ],
    outOfScopeLanguage: ["flexible meeting space", "flexible floor plan"],
    positiveExamples: ["Curio emphasizes design individuality and local character."],
    negativeExamples: ["The brand requires rigid design standards."],
    ambiguousExamples: ["The hotel has a distinctive design."],
    commonFalsePositives: ["interior design firm", "design awards"],
    highRisk: false,
    deferred: false,
    requiresCitation: false,
  },
  {
    attributeId: "DISTRIBUTION",
    clientLabel: "Distribution Strength",
    definition:
      "Explicit association between the canonical brand and reservations/distribution/channel reach — not parent-only unless brand is explicitly tied.",
    inScopeLanguage: [
      "distribution",
      "distribution network",
      "reservation system",
      "booking reach",
      "global distribution",
      "GDS",
      "distribution platform",
    ],
    outOfScopeLanguage: ["distribution center", "food distribution"],
    positiveExamples: ["Autograph benefits from Marriott's distribution when explicitly tied to Autograph."],
    negativeExamples: ["Marriott has strong global distribution."],
    ambiguousExamples: ["Strong commercial platform."],
    commonFalsePositives: ["parent company distribution only", "market distribution of hotels"],
    highRisk: false,
    deferred: false,
    requiresCitation: false,
  },
  {
    attributeId: "LOYALTY",
    clientLabel: "Loyalty Program Strength",
    definition:
      "Explicit link between brand affiliation and loyalty program benefits accessible to that brand's hotels.",
    inScopeLanguage: [
      "loyalty program",
      "Bonvoy",
      "Hilton Honors",
      "loyalty benefits",
      "member rewards",
    ],
    outOfScopeLanguage: ["customer loyalty to the market", "brand loyalty in abstract"],
    positiveExamples: ["Autograph hotels participate in Marriott Bonvoy when explicitly stated for Autograph."],
    negativeExamples: ["Marriott Bonvoy is industry-leading."],
    ambiguousExamples: ["Strong loyalty ecosystem."],
    commonFalsePositives: ["parent loyalty named without brand binding"],
    highRisk: false,
    deferred: false,
    requiresCitation: false,
  },
  {
    attributeId: "OWNER_FLEXIBILITY",
    clientLabel: "Owner Flexibility",
    definition:
      "Explicit statements about owner latitude in standards, approvals, operating requirements, affiliation terms, or conversion implementation.",
    inScopeLanguage: [
      "owner flexibility",
      "owners flexibility",
      "owners greater flexibility",
      "flexible brand standards",
      "operating flexibility",
      "approval flexibility",
      "affiliation flexibility",
      "conversion flexibility",
      "owner latitude",
      "flexibility in brand standards",
      "flexibility in operating",
    ],
    outOfScopeLanguage: [
      "flexible location",
      "flexible meeting space",
      "flexible booking",
      "flexible dates",
    ],
    positiveExamples: ["Curio offers owners greater flexibility in brand standards."],
    negativeExamples: ["Autograph may provide less owner flexibility than Curio."],
    ambiguousExamples: ["Flexible approach to affiliation."],
    commonFalsePositives: ["flexible cancellation policy", "flexible event space"],
    highRisk: true,
    deferred: false,
    requiresCitation: false,
  },
  {
    attributeId: "OWNER_CONTROL",
    clientLabel: "Owner Control",
    definition:
      "Explicit association with owner control over operations, approvals, capex, staffing, or key decisions.",
    inScopeLanguage: [
      "owner control",
      "operating control",
      "control over operations",
      "owner approval",
      "owner oversight",
    ],
    outOfScopeLanguage: ["quality control", "damage control", "crowd control"],
    positiveExamples: ["Owners retain meaningful control under the affiliation."],
    negativeExamples: ["The brand limits owner control over daily operations."],
    ambiguousExamples: ["Balanced control between owner and brand."],
    commonFalsePositives: ["internal control systems"],
    highRisk: true,
    deferred: false,
    requiresCitation: false,
  },
  {
    attributeId: "CONVERSION_SUITABILITY",
    clientLabel: "Conversion Suitability",
    definition:
      "Explicit suitability for conversion, reflag, repositioning, or affiliation of an existing asset.",
    inScopeLanguage: [
      "conversion",
      "reflag",
      "reposition",
      "affiliation of an existing",
      "suitable for conversion",
      "conversion candidate",
    ],
    outOfScopeLanguage: ["currency conversion", "unit conversion"],
    positiveExamples: ["Autograph is a strong option for converting an independent upper-upscale hotel."],
    negativeExamples: ["Not ideal for conversion projects."],
    ambiguousExamples: ["Works for existing hotels."],
    commonFalsePositives: ["new build only"],
    highRisk: true,
    deferred: false,
    requiresCitation: false,
  },
  {
    attributeId: "ECONOMICS",
    clientLabel: "Fees / Economics",
    definition:
      "Explicit fee, cost, incentive, royalty, or economic structure attributed to the brand.",
    inScopeLanguage: [
      "fees",
      "royalty",
      "incentive fee",
      "economic terms",
      "cost structure",
      "franchise fees",
    ],
    outOfScopeLanguage: ["macro economics", "market economics", "economic outlook"],
    positiveExamples: ["Competitive fee structure for owners."],
    negativeExamples: ["Higher fees than peers."],
    ambiguousExamples: ["Economics depend on deal structure."],
    commonFalsePositives: ["market economic conditions"],
    highRisk: true,
    deferred: true,
    requiresCitation: true,
  },
  {
    attributeId: "GEOGRAPHIC_PRESENCE",
    clientLabel: "Geographic Presence",
    definition:
      "Explicit claim about where the brand operates, its footprint, or presence in a geography.",
    inScopeLanguage: [
      "presence in",
      "footprint in",
      "operates in",
      "hotels in",
      "properties in",
      "limited presence",
      "strong presence",
    ],
    outOfScopeLanguage: ["geographic market trends", "presence of competitors"],
    positiveExamples: ["Autograph has growing presence in CALA."],
    negativeExamples: ["Limited brand presence in Mexico."],
    ambiguousExamples: ["Known in major markets."],
    commonFalsePositives: ["parent company global footprint without brand tie"],
    highRisk: false,
    deferred: false,
    requiresCitation: true,
  },
  {
    attributeId: "LIFESTYLE_POSITIONING",
    clientLabel: "Lifestyle Positioning",
    definition:
      "Explicit lifestyle brand positioning, experiential positioning, or lifestyle segment characterization.",
    inScopeLanguage: [
      "lifestyle brand",
      "lifestyle positioning",
      "lifestyle hotel",
      "experiential positioning",
      "boutique lifestyle",
    ],
    outOfScopeLanguage: ["luxury lifestyle magazine", "work-life balance"],
    positiveExamples: ["Autograph is positioned as a lifestyle collection brand."],
    negativeExamples: ["Not a lifestyle brand."],
    ambiguousExamples: ["Experiential guest focus."],
    commonFalsePositives: ["local lifestyle amenities"],
    highRisk: false,
    deferred: false,
    requiresCitation: false,
  },
  {
    attributeId: "LUXURY_POSITIONING",
    clientLabel: "Luxury Positioning",
    definition: "Explicit luxury segment positioning attributed to the brand.",
    inScopeLanguage: [
      "luxury brand",
      "luxury positioning",
      "luxury segment",
      "ultra-luxury",
      "upper luxury",
    ],
    outOfScopeLanguage: ["luxury market in the city", "luxury travel demand"],
    positiveExamples: ["Positioned in the luxury segment."],
    negativeExamples: ["Not positioned as luxury."],
    ambiguousExamples: ["Premium guest experience."],
    commonFalsePositives: ["luxury amenities list"],
    highRisk: false,
    deferred: false,
    requiresCitation: false,
  },
  {
    attributeId: "BRANDED_RESIDENCES",
    clientLabel: "Branded Residences Capability",
    definition:
      "Explicit branded residences, residential component, or mixed-use residences capability tied to the brand.",
    inScopeLanguage: [
      "branded residences",
      "residence program",
      "residential component",
      "branded residential",
    ],
    outOfScopeLanguage: ["guest residence length of stay", "local residences nearby"],
    positiveExamples: ["Brand offers branded residences in mixed-use projects."],
    negativeExamples: ["No branded residences program."],
    ambiguousExamples: ["Residential opportunities may exist."],
    commonFalsePositives: ["long-stay residential guests"],
    highRisk: false,
    deferred: false,
    requiresCitation: false,
  },
  {
    attributeId: "DEVELOPMENT_SUPPORT",
    clientLabel: "Development Support",
    definition:
      "Explicit development services, pipeline support, pre-opening support, or owner development assistance from the brand.",
    inScopeLanguage: [
      "development support",
      "pre-opening support",
      "development services",
      "pipeline support",
      "owner development assistance",
    ],
    outOfScopeLanguage: ["market development", "software development"],
    positiveExamples: ["Strong development support for new projects."],
    negativeExamples: ["Limited development support."],
    ambiguousExamples: ["Support during opening."],
    commonFalsePositives: ["community development"],
    highRisk: true,
    deferred: true,
    requiresCitation: true,
  },
  {
    attributeId: "OPERATING_MODEL",
    clientLabel: "Operating Model",
    definition:
      "Explicit franchise vs managed vs HMA/operator model characterization tied to the brand.",
    inScopeLanguage: [
      "franchise model",
      "managed model",
      "HMA",
      "management contract",
      "operator model",
      "franchise affiliation",
    ],
    outOfScopeLanguage: ["business operating model in general", "operating model of the owner"],
    positiveExamples: ["Typically offered under a franchise model."],
    negativeExamples: ["Requires a managed structure."],
    ambiguousExamples: ["Flexible operating structures available."],
    commonFalsePositives: ["operator named without brand model tie"],
    highRisk: true,
    deferred: false,
    requiresCitation: false,
  },
  {
    attributeId: "MARKET_FIT",
    clientLabel: "Market Fit",
    definition:
      "Explicit suitability or fit for a market, submarket, or owner project context.",
    inScopeLanguage: [
      "market fit",
      "fit for this market",
      "well-suited for",
      "strong fit in",
      "appropriate for the market",
    ],
    outOfScopeLanguage: ["product-market fit for startups", "market fitting room"],
    positiveExamples: ["A strong fit for urban lifestyle markets in CALA."],
    negativeExamples: ["Not a good fit for resort markets."],
    ambiguousExamples: ["Could work depending on the asset."],
    commonFalsePositives: ["market size statements without brand tie"],
    highRisk: true,
    deferred: false,
    requiresCitation: false,
  },
]);

const BY_ID = new Map(ASSOCIATION_ATTRIBUTES.map((a) => [a.attributeId, a]));

export function getAssociationAttribute(attributeId) {
  return BY_ID.get(attributeId) || null;
}

export function listProductionEligibleAttributes() {
  return ASSOCIATION_ATTRIBUTES.filter((a) => !a.deferred);
}

export const TAXONOMY_SUMMARY = Object.freeze({
  retained: ASSOCIATION_ATTRIBUTES.map((a) => a.attributeId),
  merged: [],
  deferred: [...DEFERRED_ATTRIBUTE_IDS],
  highRisk: [...HIGH_RISK_ATTRIBUTE_IDS],
});
