/**
 * Brand Setup - Deal Terms profiles for all Brand Basics rows.
 * Resolve order: exact brand override → Choice FDD alias → parent-company template.
 */
import {
  CHOICE_DEAL_PIP_CONVERSION_USD,
  CHOICE_DEAL_TERMS_FDD_FILE,
} from "./choice-deal-terms-profiles.mjs";
import { FDD_FIELD_DISCLAIMER } from "../../lib/external-owner-copy.mjs";

const D = FDD_FIELD_DISCLAIMER;
const DIR =
  "Directionally accurate brand-typical estimate for matching—not a property-specific quote. Confirm against current brand documents.";

/**
 * @typedef {object} DealTermsProfile
 * @property {'fdd'|'directional'} sourceTier
 * @property {string} [cohort]
 * @property {string} [fddFile]
 * @property {number|null} [initialYears]
 * @property {boolean} [noRenewal]
 * @property {number|null} [renewalYears]
 * @property {number|null} [renewalOptionQty]
 * @property {number|null} [noticeMonths]
 * @property {string} renewalStructure
 * @property {string} renewalNoticeResponsibility
 * @property {string} renewalConditions
 * @property {string|null} performanceTestRequirement
 * @property {string} curePeriodText
 * @property {string} curePeriodDuration
 * @property {string} qa
 * @property {string} pipAtRenewal
 * @property {string} pipForConversions
 * @property {number|null} pipUsdPerRoom
 * @property {number|null} conversionMaxMonths
 * @property {number|null} renewalMaxMonths
 * @property {string} terminationFeeStructure
 * @property {string} terminationFeeNotes
 * @property {string} performanceTerminationRights
 */

export function baseFranchise(overrides = {}) {
  return {
    sourceTier: "directional",
    cohort: "generic-franchise",
    initialYears: 20,
    noRenewal: false,
    renewalYears: 10,
    renewalOptionQty: 1,
    noticeMonths: 12,
    renewalStructure: "Renewal by Mutual Agreement Only",
    renewalNoticeResponsibility: "Mutual",
    renewalConditions: `Renewal typically requires good standing, fee currency, brand standards compliance, and any required PIP. ${DIR}`,
    performanceTestRequirement: "Yes",
    curePeriodText:
      "Typical cure windows for material defaults often fall in a 10–30 day range; confirm with counsel.",
    curePeriodDuration: "Month(s)",
    qa: "Yes",
    pipAtRenewal: "Yes",
    pipForConversions: "Yes",
    pipUsdPerRoom: 10000,
    conversionMaxMonths: 24,
    renewalMaxMonths: 24,
    terminationFeeStructure: "Allowed With X Months Fees",
    terminationFeeNotes: `Early exit often triggers liquidated damages or a multiple of ongoing fees. ${DIR}`,
    performanceTerminationRights: "Mutual",
    ...overrides,
  };
}

export function softBrandCollection(overrides = {}) {
  return baseFranchise({
    cohort: "soft-brand",
    initialYears: 20,
    renewalYears: 10,
    renewalOptionQty: 1,
    noticeMonths: 12,
    renewalStructure: "Renewal by Mutual Agreement Only",
    renewalConditions: `Soft-brand / collection license: initial term and renewal are deal-specific; continued affiliation typically requires standards compliance and mutual agreement to renew or re-license. ${DIR}`,
    pipUsdPerRoom: 15000,
    terminationFeeStructure: "Case-by-Case",
    terminationFeeNotes: `Exit economics vary widely by soft brand and market; often negotiated. ${DIR}`,
    performanceTerminationRights: "Rarely Exercised / Case-by-Case",
    ...overrides,
  });
}

export function membershipNetwork(overrides = {}) {
  return baseFranchise({
    cohort: "membership",
    sourceTier: "directional",
    initialYears: 5,
    noRenewal: false,
    renewalYears: 5,
    renewalOptionQty: 1,
    noticeMonths: 6,
    renewalStructure: "Renewal by Mutual Agreement Only",
    renewalNoticeResponsibility: "Mutual",
    renewalConditions: `Membership / referral network (not a classic long-term franchise agreement). Term and renewal are typically shorter and governed by membership rules; confirm current membership agreement. ${DIR}`,
    performanceTestRequirement: null,
    curePeriodText:
      "Membership defaults and cure periods follow network rules—confirm current agreement. Formal franchise-style performance tests are uncommon.",
    curePeriodDuration: "Month(s)",
    qa: "Yes",
    pipAtRenewal: "No",
    pipForConversions: "No",
    pipUsdPerRoom: null,
    conversionMaxMonths: null,
    renewalMaxMonths: null,
    terminationFeeStructure: "Typically None",
    terminationFeeNotes: `Membership exit fees (if any) are network-specific; often limited vs franchise liquidated damages. ${DIR}`,
    performanceTerminationRights: "Rarely Exercised / Case-by-Case",
    ...overrides,
  });
}

function choiceFranchise(fddKey, brandLabel) {
  const fddFile = CHOICE_DEAL_TERMS_FDD_FILE[fddKey];
  const pip = CHOICE_DEAL_PIP_CONVERSION_USD[fddKey];
  return baseFranchise({
    sourceTier: "fdd",
    cohort: "choice",
    fddFile: fddFile || undefined,
    noRenewal: true,
    renewalOptionQty: 0,
    renewalYears: null,
    renewalStructure: "Renewal by Mutual Agreement Only",
    pipUsdPerRoom: pip ?? 6500,
    curePeriodText:
      "See FDD Item 17(g): 10 days (fee/report defaults); 30 days (other material defaults).",
    renewalConditions: `No contractual renewal right after the initial term; continued operation may require a new franchise agreement on then-current terms and fees (${brandLabel}). ${D}`,
  });
}

/**
 * Exact Brand Basics name → Choice FDD key (when names differ from FDD inventory keys).
 */
export const BRAND_TO_CHOICE_FDD_KEY = Object.freeze({
  "Ascend Hotel Collection": "Ascend Hotel Collection",
  "Cambria Hotels": "Cambria Hotels",
  Clarion: "Clarion",
  "Clarion Pointe": "Clarion Pointe",
  "Comfort Inn & Suites": "Comfort Inn & Suites",
  "MainStay Suites": "MainStay Suites",
  "Quality Inn": "Quality Inn",
  "Sleep Inn": "Sleep Inn",
  "Econo Lodge": "Econo Lodge",
  "Rodeway Inn": "Rodeway Inn",
  "Suburban Studios": "Suburban Studios",
  "WoodSpring Suites": "WoodSpring Suites",
  "Everhome Suites": "Everhome Suites",
  "Radisson Inn & Suites": "Radisson Inn & Suites",
  // Active/Live naming
  "Country Inn & Suites by Choice": "Country Inn & Suites by Radisson (Choice)",
  "Radisson Blu by Choice": "Radisson Blu (Choice)",
  "Radisson by Choice": "Radisson (Choice)",
  "Radisson Individuals by Choice": "Radisson Individual (Choice)",
  "Radisson RED by Choice": "Radisson RED  (Choice)",
  // Draft / alternate naming in Brand Basics
  "Country Inn & Suites by Radisson": "Country Inn & Suites by Radisson (Choice)",
  "Country Inn & Suites by Radisson (Choice)": "Country Inn & Suites by Radisson (Choice)",
  "Radisson (Choice)": "Radisson (Choice)",
  "Radisson RED  (Choice)": "Radisson RED  (Choice)",
  "Radisson Collection  (Choice)": "Radisson Collection  (Choice)",
  "Radisson Collection by Choice": "Radisson Collection  (Choice)",
  "Park Plaza (Choice)": "Park Plaza (Choice)",
  "Park Plaza by Choice": "Park Plaza (Choice)",
  "Park Inn by Radisson (Choice)": "Park Inn by Radisson (Choice)",
  "Park Inn by Choice": "Park Inn by Radisson (Choice)",
  "Radisson Blu (Choice)": "Radisson Blu (Choice)",
  "Radisson Individual (Choice)": "Radisson Individual (Choice)",
});

/** Brand-specific overrides (exact Brand Name). */
export const BRAND_DEAL_TERMS_OVERRIDES = Object.freeze({
  "Kimpton Hotels": baseFranchise({
    sourceTier: "fdd",
    cohort: "ihg",
    initialYears: 20,
    noRenewal: true,
    renewalYears: null,
    renewalOptionQty: 0,
    // Meta already includes this option; prefer it over form-only Mutual Agreement.
    renewalStructure: "No automatic renewal — re-licensing may be offered",
    renewalConditions: `Initial license term is 20 years for new development and 10 years for conversions; change-of-ownership/re-licensing typically 10 years. License does not provide automatic renewal—re-licensing may carry materially different terms. ${D}`,
    curePeriodText:
      "Cure periods apply to QA, fee, and reporting defaults—confirm with counsel.",
    pipUsdPerRoom: 25000,
  }),
  "Hotel Indigo": baseFranchise({
    sourceTier: "directional",
    cohort: "ihg",
    initialYears: 20,
    noRenewal: true,
    renewalOptionQty: 0,
    renewalYears: null,
    renewalConditions: `IHG lifestyle franchise (Hotel Indigo): directional—often ~20-year new-build / shorter conversion terms with no automatic renewal; re-license on then-current terms. ${DIR}`,
    pipUsdPerRoom: 20000,
  }),
  "Vignette Collection": softBrandCollection({
    cohort: "ihg",
    initialYears: 20,
    noRenewal: true,
    renewalOptionQty: 0,
    renewalYears: null,
    renewalConditions: `IHG Vignette Collection soft brand: directional collection license; renewals typically mutual / re-license. ${DIR}`,
    pipUsdPerRoom: 22000,
  }),
  "Handwritten Collection": softBrandCollection({
    cohort: "ihg",
    initialYears: 15,
    noRenewal: true,
    renewalOptionQty: 0,
    renewalYears: null,
    pipUsdPerRoom: 18000,
  }),
  "Curio Collection by Hilton": softBrandCollection({
    sourceTier: "fdd",
    cohort: "hilton",
    initialYears: 23,
    noRenewal: true,
    renewalOptionQty: 0,
    renewalYears: null,
    renewalStructure: "Renewal by Mutual Agreement Only",
    renewalConditions: `Hilton Curio Collection: FDD Item 17 typically ~23 years new development and ~10–20 years conversion; no contractual automatic renewal—continued affiliation via re-license / mutual agreement. ${D}`,
    pipUsdPerRoom: 20000,
    noticeMonths: 12,
  }),
  "Tapestry Collection by Hilton": softBrandCollection({
    cohort: "hilton",
    initialYears: 20,
    noRenewal: true,
    renewalOptionQty: 0,
    renewalYears: null,
    renewalStructure: "Renewal by Mutual Agreement Only",
    renewalConditions: `Hilton Tapestry Collection soft brand: directional—collection license; renewals typically mutual / re-license rather than automatic. ${DIR}`,
    pipUsdPerRoom: 18000,
    noticeMonths: 12,
  }),
  "Autograph Collection": softBrandCollection({
    cohort: "marriott",
    pipUsdPerRoom: 18000,
    renewalConditions: `Marriott Autograph Collection soft brand: directional—long initial terms common; renewal typically mutual. ${DIR}`,
  }),
  "Tribute Portfolio": softBrandCollection({
    cohort: "marriott",
    pipUsdPerRoom: 16000,
  }),
  "Design Hotels": softBrandCollection({
    cohort: "marriott",
    initialYears: 15,
    renewalYears: 5,
    pipUsdPerRoom: 12000,
    pipAtRenewal: "No",
    renewalConditions: `Design Hotels (Marriott-affiliated membership/collection hybrid): directional—terms often shorter/membership-like vs hard franchise. ${DIR}`,
  }),
  "BW Premier Collection": softBrandCollection({
    cohort: "best-western",
    initialYears: 15,
    renewalYears: 5,
    pipUsdPerRoom: 8000,
  }),
  "BW Signature Collection": softBrandCollection({
    cohort: "best-western",
    initialYears: 15,
    renewalYears: 5,
    pipUsdPerRoom: 7000,
  }),
  "MGallery Collection": softBrandCollection({
    cohort: "accor",
    pipUsdPerRoom: 15000,
  }),
  "Preferred Hotels & Resorts": membershipNetwork({
    initialYears: 5,
    renewalYears: 5,
    renewalConditions: `Preferred Hotels & Resorts: membership/referral network—not a classic 20-year franchise. ${DIR}`,
  }),
  "Small Luxury Hotels of the World": membershipNetwork({
    initialYears: 3,
    renewalYears: 3,
    noticeMonths: 6,
    renewalConditions: `SLH: membership/referral network—not a classic franchise agreement. ${DIR}`,
  }),
  "The Leading Hotels of the World": membershipNetwork({
    initialYears: 5,
    renewalYears: 5,
    noticeMonths: 8,
    pipAtRenewal: "No",
    pipForConversions: "No",
    pipUsdPerRoom: null,
    renewalConditions: `Leading Hotels of the World: membership/referral network—flexible soft-brand affiliation; not a classic franchise FA. ${DIR}`,
  }),
  "Mr & Mrs Smith": membershipNetwork({
    initialYears: 3,
    renewalYears: 3,
  }),
});

/**
 * Parent Company → default Deal Terms template.
 * Keys must match Airtable Parent Company values exactly.
 */
export const PARENT_DEAL_TERMS_TEMPLATES = Object.freeze({
  "Choice Hotels International": choiceFranchise("Quality Inn", "Choice Hotels brand"),
  "Hilton Worldwide": baseFranchise({
    cohort: "hilton",
    initialYears: 20,
    renewalOptionQty: 2,
    renewalYears: 10,
    noticeMonths: 12,
    renewalStructure: "Renewal by Mutual Agreement Only",
    pipUsdPerRoom: 15000,
    renewalConditions: `Hilton franchise / license: directional—long initial terms common; renewal and PIP requirements are brand- and deal-specific. ${DIR}`,
  }),
  "Marriott International, Inc.": baseFranchise({
    cohort: "marriott",
    initialYears: 20,
    renewalOptionQty: 2,
    renewalYears: 10,
    noticeMonths: 12,
    pipUsdPerRoom: 18000,
    renewalConditions: `Marriott franchise / license: directional—long initial terms common; renewal typically mutual with standards/PIP gates. ${DIR}`,
  }),
  "InterContinental Hotels Group": baseFranchise({
    cohort: "ihg",
    initialYears: 20,
    noRenewal: true,
    renewalOptionQty: 0,
    renewalYears: null,
    pipUsdPerRoom: 15000,
    renewalConditions: `IHG franchise / license: directional—often no automatic renewal; re-licensing on then-current terms is common. ${DIR}`,
  }),
  "Hyatt Hotels Corporation": baseFranchise({
    cohort: "hyatt",
    initialYears: 20,
    renewalOptionQty: 2,
    renewalYears: 10,
    noticeMonths: 12,
    pipUsdPerRoom: 18000,
    renewalConditions: `Hyatt franchise / management/license hybrid: directional—long terms common; renewals mutual and standards-driven. ${DIR}`,
  }),
  "Hyatt Vacation Ownership": baseFranchise({
    cohort: "hyatt",
    initialYears: 20,
    renewalOptionQty: 1,
    renewalYears: 10,
    pipUsdPerRoom: 20000,
  }),
  "Wyndham Hotels & Resorts": baseFranchise({
    cohort: "wyndham",
    initialYears: 20,
    renewalOptionQty: 2,
    renewalYears: 10,
    pipUsdPerRoom: 10000,
    renewalConditions: `Wyndham franchise: directional—~20-year terms common; renewal subject to standards and PIP. ${DIR}`,
  }),
  AccorHotels: baseFranchise({
    cohort: "accor",
    initialYears: 20,
    renewalOptionQty: 2,
    renewalYears: 10,
    pipUsdPerRoom: 12000,
    renewalConditions: `Accor franchise / management: directional—renewal typically mutual with brand standards compliance. ${DIR}`,
  }),
  "BWH Hotels": softBrandCollection({
    cohort: "best-western",
    initialYears: 15,
    renewalYears: 5,
    renewalOptionQty: 1,
    pipUsdPerRoom: 6000,
    renewalConditions: `Best Western / BWH affiliation: directional—often membership/affiliation style vs hard franchise; renewals mutual with standards review. ${DIR}`,
  }),
  "Sonesta International Hotels Corporation": baseFranchise({
    cohort: "sonesta",
    initialYears: 15,
    renewalOptionQty: 1,
    renewalYears: 5,
    pipUsdPerRoom: 10000,
  }),
  "Radisson Hotel Group": baseFranchise({
    cohort: "radisson-legacy",
    initialYears: 20,
    noRenewal: true,
    renewalOptionQty: 0,
    pipUsdPerRoom: 9000,
    renewalConditions: `Radisson family (non-Choice row or legacy naming): directional—confirm whether under Choice America FDD or other agreement. ${DIR}`,
  }),
  "Red Roof Franchise, UK": baseFranchise({
    cohort: "red-roof",
    initialYears: 20,
    renewalOptionQty: 1,
    renewalYears: 10,
    pipUsdPerRoom: 5000,
  }),
  "Minor Hotel Group Limited": baseFranchise({
    cohort: "minor",
    initialYears: 20,
    renewalOptionQty: 1,
    renewalYears: 10,
    pipUsdPerRoom: 12000,
  }),
  "Dovetail + Co": softBrandCollection({
    cohort: "dovetail",
    initialYears: 10,
    renewalYears: 5,
    pipUsdPerRoom: 8000,
  }),
  "Staycity Ltd": baseFranchise({
    cohort: "staycity",
    initialYears: 15,
    renewalOptionQty: 1,
    renewalYears: 5,
    pipUsdPerRoom: 8000,
  }),
  "Banyan Tree Hotels & Resorts": softBrandCollection({
    cohort: "banyan",
    initialYears: 20,
    pipUsdPerRoom: 25000,
  }),
  "Iberostar Hotels & Resorts": baseFranchise({
    cohort: "iberostar",
    initialYears: 20,
    renewalOptionQty: 1,
    renewalYears: 10,
    pipUsdPerRoom: 20000,
  }),
  "Prem Group": softBrandCollection({
    cohort: "prem",
    initialYears: 20,
    renewalOptionQty: 2,
    renewalYears: 5,
    noticeMonths: 18,
    pipUsdPerRoom: 25000,
  }),
  "Four Seasons Hotels and Resorts": softBrandCollection({
    cohort: "luxury-mgmt",
    initialYears: 30,
    renewalOptionQty: 1,
    renewalYears: 10,
    pipUsdPerRoom: 40000,
    renewalConditions: `Four Seasons: typically management agreement economics (not a classic franchise). Directional term length for matching only. ${DIR}`,
  }),
  "Rosewood Hotel Group": softBrandCollection({
    cohort: "luxury-mgmt",
    initialYears: 25,
    pipUsdPerRoom: 35000,
  }),
  "Shangri-La Hotels and Resorts": softBrandCollection({
    cohort: "luxury-mgmt",
    initialYears: 25,
    pipUsdPerRoom: 30000,
  }),
  "Mandarin Oriental Hotel Group": softBrandCollection({
    cohort: "luxury-mgmt",
    initialYears: 25,
    pipUsdPerRoom: 35000,
  }),
  "The Peninsula Hotels": softBrandCollection({
    cohort: "luxury-mgmt",
    initialYears: 25,
    pipUsdPerRoom: 40000,
  }),
  "Oetker Hotels": softBrandCollection({
    cohort: "luxury-mgmt",
    initialYears: 20,
    pipUsdPerRoom: 30000,
  }),
  "Aman Group": softBrandCollection({
    cohort: "luxury-mgmt",
    initialYears: 25,
    pipUsdPerRoom: 50000,
  }),
  "Leading Hotels of the World": membershipNetwork({
    initialYears: 5,
    renewalYears: 5,
    noticeMonths: 8,
  }),
  "Preferred Hotels & Resorts": membershipNetwork({
    initialYears: 5,
    renewalYears: 5,
  }),
  "Small Luxury Hotels of the World": membershipNetwork({
    initialYears: 3,
    renewalYears: 3,
  }),
  "AmeriVu Inn and Suites": baseFranchise({
    cohort: "independent-franchise",
    initialYears: 15,
    pipUsdPerRoom: 4000,
  }),
  "Northland Properties": baseFranchise({
    cohort: "independent-franchise",
    initialYears: 15,
    pipUsdPerRoom: 6000,
  }),
  "Coast Hotels Limited": softBrandCollection({
    cohort: "coast",
    initialYears: 15,
    pipUsdPerRoom: 8000,
  }),
  "Edyn Limited": softBrandCollection({
    cohort: "edyn",
    initialYears: 10,
    pipUsdPerRoom: 10000,
  }),
});

const DEFAULT_PARENT = baseFranchise({
  cohort: "unknown-parent",
  renewalConditions: `Brand-typical deal terms (parent company unknown in Brand Basics)—directional estimate only. ${DIR}`,
});

/**
 * @param {string} brandName
 * @param {string} [parentCompany]
 * @returns {{ profile: DealTermsProfile, resolveSource: string }}
 */
export function getDealTermsProfile(brandName, parentCompany = "") {
  const name = String(brandName || "").trim();
  const parent = String(parentCompany || "").trim();

  if (BRAND_DEAL_TERMS_OVERRIDES[name]) {
    return { profile: { ...BRAND_DEAL_TERMS_OVERRIDES[name] }, resolveSource: "brand-override" };
  }

  const fddKey = BRAND_TO_CHOICE_FDD_KEY[name];
  if (fddKey) {
    return {
      profile: choiceFranchise(fddKey, name),
      resolveSource: `choice-fdd:${fddKey}`,
    };
  }

  if (parent && PARENT_DEAL_TERMS_TEMPLATES[parent]) {
    return {
      profile: { ...PARENT_DEAL_TERMS_TEMPLATES[parent] },
      resolveSource: `parent:${parent}`,
    };
  }

  return { profile: { ...DEFAULT_PARENT }, resolveSource: "default" };
}

/** @deprecated Use getDealTermsProfile */
export function getActiveLiveDealTermsProfile(brandName) {
  return getDealTermsProfile(brandName).profile;
}

export const ACTIVE_LIVE_DEAL_TERMS_PROFILES = BRAND_DEAL_TERMS_OVERRIDES;
