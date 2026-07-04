/**
 * Official hotel development portals + search patterns for reference material collection.
 * Used by CLI tools and future capture UI — not automated scraping.
 */

/** @typedef {{ key: string, parentCompany: string, referenceFolder: string, developmentPortal: string, downloadsPage?: string, fddNotes?: string, regionalNotes?: string, searchPatterns: string[] }} DevelopmentPortal */

/** @type {DevelopmentPortal[]} */
export const DEVELOPMENT_PORTALS = [
  {
    key: "marriott",
    parentCompany: "Marriott International",
    referenceFolder: "Marriott International",
    developmentPortal: "https://hotel-development.marriott.com/",
    downloadsPage: "https://hotel-development.marriott.com/",
    fddNotes: "FDD PDFs often indexed on development site (e.g. AC Hotels, Residence Inn).",
    searchPatterns: [
      "site:hotel-development.marriott.com ResourceFiles [brand] pdf",
      "site:hotel-development.marriott.com [brand] one pager pdf",
      "site:hotel-development.marriott.com [brand] FDD pdf",
    ],
  },
  {
    key: "hilton",
    parentCompany: "Hilton",
    referenceFolder: "Hilton",
    developmentPortal: "https://www.hilton.com/en/corporate/development/",
    searchPatterns: [
      "site:development.hilton.com [brand] pdf",
      "site:hilton.com [brand] development brochure pdf",
      '"[brand]" "development" site:hilton.com filetype:pdf',
    ],
  },
  {
    key: "ihg",
    parentCompany: "IHG Hotels & Resorts",
    referenceFolder: "IHG Hotels & Resorts",
    developmentPortal: "https://development.ihg.com/",
    downloadsPage: "https://development.ihg.com/resources",
    regionalNotes: "Regional brochures in development resource library; some materials gated — contact local developer.",
    searchPatterns: [
      "site:development.ihg.com [brand] development brochure pdf",
      "site:development.ihg.com [brand] pdf",
    ],
  },
  {
    key: "hyatt",
    parentCompany: "Hyatt",
    referenceFolder: "Hyatt",
    developmentPortal: "https://www.hyatt.com/development",
    searchPatterns: [
      "site:hyatt.com [brand] development pdf",
      '"[brand]" Hyatt development brochure pdf',
    ],
  },
  {
    key: "choice",
    parentCompany: "Choice Hotels International",
    referenceFolder: "Choice Hotels International",
    developmentPortal: "https://www.choicehotels.com/development",
    fddNotes: "Local FDD folder + fixtures/choice-fdd-text; see docs/choice-fdd-inventory.md",
    regionalNotes: "Choice Australia, Choice Mexico franchise pages may have regional brochures.",
    searchPatterns: [
      "site:choicehotels.com [brand] development brochure pdf",
      "site:choicehotels.com/development [brand] pdf",
    ],
  },
  {
    key: "wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    referenceFolder: "Wyndham Hotels & Resorts",
    developmentPortal: "https://development.wyndhamhotels.com/",
    downloadsPage: "https://development.wyndhamhotels.com/development-downloads",
    regionalNotes: "EMEA brochures on regional development subsites.",
    searchPatterns: [
      "site:development.wyndhamhotels.com [brand] one sheet pdf",
      "site:development.wyndhamhotels.com [brand] pdf",
    ],
  },
  {
    key: "accor",
    parentCompany: "Accor",
    referenceFolder: "Accor",
    developmentPortal: "https://group.accor.com/en/hotel-development",
    downloadsPage: "https://group.accor.com/en/publications",
    searchPatterns: [
      "site:assets.group.accor.com [brand] Development Brochure pdf",
      "site:group.accor.com [brand] development pdf",
    ],
  },
  {
    key: "bwh",
    parentCompany: "BWH Hotels",
    referenceFolder: "BWH Hotels",
    developmentPortal: "https://www.bwhhotels.com/development",
    searchPatterns: ['site:bwhhotels.com [brand] development pdf'],
  },
  {
    key: "radisson",
    parentCompany: "Radisson Hotel Group",
    referenceFolder: "Radisson Hotel Group",
    developmentPortal: "https://www.radissonhotels.com/en-us/brand-partnership/hotel-development",
    searchPatterns: [
      "site:media.radissonhotels.net [brand] development pdf",
      '"[brand]" Radisson development brochure pdf',
    ],
  },
];

/** Generic patterns when portal search fails */
export const GENERIC_BRAND_SEARCH_PATTERNS = [
  '"[brand]" "development brochure" hotel pdf',
  '"[brand]" "owner" "franchise" PDF',
  '"[brand]" "one sheet" "development"',
  '"[brand]" "FDD" PDF',
  '"[brand]" "prototype" hotel development',
  '"[brand]" "conversion" franchise hotel PDF',
];

export const GENERIC_OPERATOR_SEARCH_PATTERNS = [
  '"[operator]" "hotel management" brochure PDF',
  '"[operator]" "owner presentation" hotel management',
  '"[operator]" "capabilities deck" hotel management',
  '"[operator]" CALA hotel management pdf',
  'site:[domain] filetype:pdf portfolio',
  'site:[domain] case studies filetype:pdf',
];

export const HELENA_OUTREACH_TEMPLATE = `We are building a structured brand intelligence profile for hotel owners and developers evaluating brand fit. Could you please share the latest owner/developer-facing materials for [Brand], including development brochure, prototype overview, conversion guidance, support model, regional development priorities, and current FDD where applicable?`;

/**
 * @param {string} brandName
 * @param {string} [parentKey]
 */
export function buildGoogleSearchUrls(brandName, parentKey) {
  const portal = parentKey
    ? DEVELOPMENT_PORTALS.find((p) => p.key === parentKey)
    : null;
  const patterns = portal
    ? portal.searchPatterns.map((p) => p.replace(/\[brand\]/gi, brandName))
    : GENERIC_BRAND_SEARCH_PATTERNS.map((p) => p.replace(/\[brand name\]/gi, brandName).replace(/\[brand\]/gi, brandName));

  return patterns.map((q) => ({
    query: q,
    url: `https://www.google.com/search?q=${encodeURIComponent(q)}`,
  }));
}

export function getPortalByFolder(referenceFolder) {
  return DEVELOPMENT_PORTALS.find(
    (p) => p.referenceFolder.toLowerCase() === String(referenceFolder || "").toLowerCase()
  );
}

export function listPortals() {
  return DEVELOPMENT_PORTALS.map((p) => ({
    key: p.key,
    parentCompany: p.parentCompany,
    referenceFolder: p.referenceFolder,
    developmentPortal: p.developmentPortal,
    downloadsPage: p.downloadsPage || null,
    fddNotes: p.fddNotes || null,
    regionalNotes: p.regionalNotes || null,
    patternCount: p.searchPatterns.length,
  }));
}
