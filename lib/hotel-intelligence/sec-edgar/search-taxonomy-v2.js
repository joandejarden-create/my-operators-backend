/**
 * Centrally managed, versioned SEC hotel-intelligence search taxonomy.
 * Keywords aid discovery; structured extractors prove contractual facts.
 */

export const SEARCH_TAXONOMY_VERSION = "sec-edgar-search-taxonomy-v2.1.0";

/** @typedef {{ id:string, label:string, patterns:string[], abbreviations?:string[], notes?:string }} SearchFamily */

/** @type {ReadonlyArray<SearchFamily>} */
export const SEARCH_FAMILIES = Object.freeze([
  {
    id: "hotel_identity_aliases",
    label: "Hotel identity and aliases",
    patterns: [
      "hotel",
      "inn",
      "resort",
      "suites",
      "lodge",
      "property",
      "formerly known as",
      "f/k/a",
      "also known as",
      "a/k/a",
      "rebranded as",
      "now known as",
    ],
    abbreviations: ["f/k/a", "a/k/a", "n/k/a"],
    notes: "Pair with brand-family detector and schedule extractors; do not treat proximity as identity proof.",
  },
  {
    id: "ownership_legal_entities",
    label: "Ownership and legal entities",
    patterns: [
      "owner",
      "owned by",
      "ownership interest",
      "fee simple",
      "ground lessor",
      "landlord",
      "property owner",
      "hotel owner",
      "LLC",
      "L.P.",
      "limited partnership",
      "joint venture",
      "JV",
    ],
    abbreviations: ["LLC", "LP", "LLP", "JV"],
  },
  {
    id: "parent_subsidiary",
    label: "Parent/subsidiary relationships",
    patterns: [
      "subsidiary",
      "wholly owned",
      "indirect subsidiary",
      "parent company",
      "holding company",
      "affiliates",
      "controlled by",
      "consolidated subsidiary",
    ],
  },
  {
    id: "management_operating",
    label: "Management and operating companies",
    patterns: [
      "management agreement",
      "hotel management agreement",
      "HMA",
      "managed by",
      "operator",
      "operating company",
      "third-party manager",
      "management company",
      "asset manager",
    ],
    abbreviations: ["HMA"],
  },
  {
    id: "franchise_licensing",
    label: "Franchise and licensing agreements",
    patterns: [
      "franchise agreement",
      "franchisee",
      "franchisor",
      "license agreement",
      "brand license",
      "affiliation agreement",
      "soft brand",
      "collection affiliation",
      "master franchise",
    ],
  },
  {
    id: "signing_effectiveness",
    label: "Agreement signing and effectiveness",
    patterns: [
      "dated as of",
      "executed",
      "entered into",
      "signed",
      "effective as of",
      "effective date",
      "commencement date",
      "commenced",
      "initial term",
    ],
  },
  {
    id: "expiration_termination",
    label: "Expiration and termination",
    patterns: [
      "expiration date",
      "expires",
      "expire on",
      "term expires",
      "termination",
      "terminated",
      "terminate for convenience",
      "early termination",
      "notice of termination",
    ],
  },
  {
    id: "renewal_extensions",
    label: "Renewal options and extensions",
    patterns: [
      "renewal option",
      "option to renew",
      "automatic renewal",
      "renewal notice",
      "notice period",
      "extended through",
      "extension of the term",
      "amended and restated",
    ],
  },
  {
    id: "acquisitions_dispositions",
    label: "Acquisitions and dispositions",
    patterns: [
      "acquired",
      "acquisition of",
      "purchase and sale",
      "disposed",
      "disposition",
      "sold the hotel",
      "divested",
      "closing of the acquisition",
      "PSA",
    ],
    abbreviations: ["PSA"],
  },
  {
    id: "brand_conversions",
    label: "Brand changes and conversions",
    patterns: [
      "conversion",
      "converted to",
      "reflag",
      "reflagged",
      "rebrand",
      "rebranded",
      "brand change",
      "change of brand",
      "left the system",
      "exited the brand",
    ],
  },
  {
    id: "capital_renovations",
    label: "Capital investment and renovations",
    patterns: [
      "renovation",
      "PIP",
      "property improvement plan",
      "capital expenditure",
      "capex",
      "remodel",
      "refurbishment",
      "FF&E",
    ],
    abbreviations: ["PIP", "FF&E", "capex"],
  },
  {
    id: "hotel_financial_performance",
    label: "Hotel-level financial performance",
    patterns: [
      "RevPAR",
      "ADR",
      "occupancy",
      "hotel operating income",
      "property-level",
      "comparable hotels",
      "same-store",
    ],
    abbreviations: ["RevPAR", "ADR"],
    notes: "Contextual only — not contractual proof.",
  },
  {
    id: "restructuring_distress",
    label: "Corporate restructuring and distress",
    patterns: [
      "chapter 11",
      "bankruptcy",
      "restructuring",
      "receivership",
      "foreclosure",
      "special servicer",
      "default",
      "going concern",
    ],
  },
  {
    id: "portfolio_membership",
    label: "Portfolio membership and historical changes",
    patterns: [
      "hotel portfolio",
      "portfolio of hotels",
      "owned hotels",
      "our hotels",
      "property schedule",
      "schedule of properties",
      "franchise expiration schedule",
      "management agreement schedule",
    ],
  },
]);

export function listSearchFamilies() {
  return SEARCH_FAMILIES.map((f) => ({
    id: f.id,
    label: f.label,
    patternCount: f.patterns.length,
    abbreviations: f.abbreviations || [],
  }));
}

export function allTaxonomyPatterns() {
  const out = [];
  for (const f of SEARCH_FAMILIES) {
    for (const p of f.patterns) {
      out.push({ familyId: f.id, pattern: p });
    }
  }
  return out;
}

/**
 * Count taxonomy family hits in text (discovery aid only — not verification).
 */
export function scoreTaxonomyHits(text) {
  const lower = String(text || "").toLowerCase();
  const byFamily = {};
  let total = 0;
  for (const f of SEARCH_FAMILIES) {
    let n = 0;
    for (const p of f.patterns) {
      const re = new RegExp(escapeRe(p), "gi");
      const m = lower.match(re);
      if (m) n += m.length;
    }
    byFamily[f.id] = n;
    total += n;
  }
  return { version: SEARCH_TAXONOMY_VERSION, total, byFamily };
}

/**
 * Contract-keyword patterns used by the deterministic extractor (subset of taxonomy).
 * Kept explicit so audits can compare "8 legacy keywords" vs full taxonomy.
 */
export function contractExtractorKeywordPatterns() {
  return [
    /management agreement/gi,
    /franchise agreement/gi,
    /hotel management agreement/gi,
    /\bhma\b/gi,
    /expiration (?:date|of the|of our)/gi,
    /renewal option/gi,
    /terminat(?:e|ed|ion|es)/gi,
    /amended and restated/gi,
  ];
}

export function taxonomyForExtractor() {
  // Expanded patterns for contract extractor enrichment (still require hotel attribution + verification).
  return [
    ...contractExtractorKeywordPatterns(),
    /license agreement/gi,
    /affiliation agreement/gi,
    /commencement date/gi,
    /effective as of/gi,
    /dated as of/gi,
    /option to renew/gi,
    /extended through/gi,
    /property improvement plan|\bPIP\b/gi,
    /rebrand(?:ed|ing)?|reflag(?:ged|ging)?|converted to/gi,
  ];
}

function escapeRe(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
