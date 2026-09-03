/**
 * Five-property affiliation → Brand & Portfolio lens mapping.
 * PRIMARY (first release): LOYALTY_ECOSYSTEM for branded hotels; INDEPENDENT for independents.
 * Collection / hard-brand lenses are OPTIONAL_FUTURE secondary drill-downs.
 * Source of truth: governed property profile fixtures (not inferred).
 */

import { PORTFOLIO_TYPES, PORTFOLIO_LENS_STATUS } from "./brand-portfolio-position-contract-v1.js";

/**
 * Customer-safe lens label recommendation (founder decision).
 * UI shows loyalty program name; prompts may use parent-brand wording with governed equivalence.
 */
export const LENS_LABEL_RECOMMENDATION_V1 = Object.freeze({
  hilton: {
    recommendedCustomerLabel: "Hilton Honors",
    alternativesConsidered: ["Hilton Portfolio", "Hilton Honors Portfolio"],
    rationale:
      "Owners recognize Hilton Honors; 'Portfolio' is more internal. Do not use Curio as the primary lens label.",
    promptDefaultConstraint: "Hilton",
    promptLoyaltyConstraint: "Hilton Honors",
    equivalence: Object.freeze(["Hilton", "Hilton Honors", "Hilton hotels", "Hilton Honors hotels"]),
  },
  marriott: {
    recommendedCustomerLabel: "Marriott Bonvoy",
    alternativesConsidered: ["Marriott Portfolio", "Marriott Bonvoy Portfolio"],
    rationale:
      "Bonvoy is the traveler/owner-recognized loyalty frame; 'Portfolio' is more internal. Do not use Renaissance as the primary lens label.",
    promptDefaultConstraint: "Marriott",
    promptLoyaltyConstraint: "Marriott Bonvoy",
    equivalence: Object.freeze(["Marriott", "Marriott Bonvoy", "Marriott hotels", "Marriott Bonvoy hotels"]),
  },
  independent: {
    recommendedCustomerLabel: "Independent Positioning",
    promptDefaultConstraint: "independent",
  },
});

export const FIVE_PROPERTY_PORTFOLIO_MAPPING_V1 = Object.freeze({
  adp_waterstone_boca_raton: Object.freeze({
    propertyId: "adp_waterstone_boca_raton",
    name: "Waterstone Resort & Marina",
    profileEvidence: Object.freeze({
      brand: "Curio Collection",
      affiliation: "Curio Collection by Hilton",
      parentCompany: "Hilton",
      officialBrandDomain: "hilton.com",
    }),
    defaultLensId: "hilton_honors",
    lenses: Object.freeze([
      Object.freeze({
        lensId: "hilton_honors",
        portfolioType: PORTFOLIO_TYPES.LOYALTY_ECOSYSTEM,
        label: "Hilton Honors",
        status: PORTFOLIO_LENS_STATUS.DEFAULT,
        constraintPhrase: "Hilton",
        loyaltyPhrase: "Hilton Honors",
        ecosystemId: "hilton_honors",
        notes: "PRIMARY — Hilton loyalty/parent portfolio within governed market",
      }),
      Object.freeze({
        lensId: "curio_collection",
        portfolioType: PORTFOLIO_TYPES.COLLECTION_PORTFOLIO,
        label: "Curio Collection",
        status: PORTFOLIO_LENS_STATUS.OPTIONAL_FUTURE,
        constraintPhrase: "Curio Collection",
        notes: "SECONDARY future drill-down — not first-release ranking universe",
      }),
    ]),
    sectionMode: "BRAND_PORTFOLIO_POSITION",
  }),

  adp_renaissance_times_square: Object.freeze({
    propertyId: "adp_renaissance_times_square",
    name: "Renaissance New York Times Square Hotel",
    profileEvidence: Object.freeze({
      brand: "Renaissance Hotels",
      affiliation: "Renaissance Hotels (Marriott)",
      parentCompany: "Marriott International",
      officialBrandDomain: "marriott.com",
    }),
    defaultLensId: "marriott_bonvoy",
    lenses: Object.freeze([
      Object.freeze({
        lensId: "marriott_bonvoy",
        portfolioType: PORTFOLIO_TYPES.LOYALTY_ECOSYSTEM,
        label: "Marriott Bonvoy",
        status: PORTFOLIO_LENS_STATUS.DEFAULT,
        constraintPhrase: "Marriott",
        loyaltyPhrase: "Marriott Bonvoy",
        ecosystemId: "marriott_bonvoy",
        notes: "PRIMARY — Marriott Bonvoy / Marriott portfolio within governed NYC market",
      }),
      Object.freeze({
        lensId: "renaissance",
        portfolioType: PORTFOLIO_TYPES.HARD_BRAND_PORTFOLIO,
        label: "Renaissance",
        status: PORTFOLIO_LENS_STATUS.OPTIONAL_FUTURE,
        constraintPhrase: "Renaissance",
        notes: "SECONDARY future drill-down",
      }),
    ]),
    sectionMode: "BRAND_PORTFOLIO_POSITION",
  }),

  adp_cambridge_beaches_bermuda: Object.freeze({
    propertyId: "adp_cambridge_beaches_bermuda",
    name: "Cambridge Beaches Resort & Spa",
    profileEvidence: Object.freeze({
      brand: "Independent",
      affiliation: "Independent",
      parentCompany: null,
      officialBrandDomain: null,
    }),
    defaultLensId: "independent_positioning",
    lenses: Object.freeze([
      Object.freeze({
        lensId: "independent_positioning",
        portfolioType: PORTFOLIO_TYPES.INDEPENDENT_POSITIONING,
        label: "Independent Positioning",
        status: PORTFOLIO_LENS_STATUS.DEFAULT,
        constraintPhrase: "independent",
        notes: "No loyalty ecosystem — Independent Positioning only",
      }),
    ]),
    sectionMode: "INDEPENDENT_POSITIONING",
  }),

  adp_now_now_noho: Object.freeze({
    propertyId: "adp_now_now_noho",
    name: "NOW NOW NOHO",
    profileEvidence: Object.freeze({
      brand: "Independent",
      affiliation: "Independent",
      parentCompany: null,
      operatorCompany: "Dovetail + Co",
      officialBrandDomain: null,
    }),
    defaultLensId: "independent_positioning",
    lenses: Object.freeze([
      Object.freeze({
        lensId: "independent_positioning",
        portfolioType: PORTFOLIO_TYPES.INDEPENDENT_POSITIONING,
        label: "Independent Positioning",
        status: PORTFOLIO_LENS_STATUS.DEFAULT,
        constraintPhrase: "independent",
        notes: "No Hyatt portfolio/loyalty lens ever.",
      }),
    ]),
    forbiddenLenses: Object.freeze(["hyatt", "world_of_hyatt", "HARD_BRAND_HYATT"]),
    sectionMode: "INDEPENDENT_POSITIONING",
  }),

  adp_hotel_phillips_kansas_city: Object.freeze({
    propertyId: "adp_hotel_phillips_kansas_city",
    name: "Hotel Phillips Kansas City, Curio Collection by Hilton",
    profileEvidence: Object.freeze({
      brand: "Curio Collection",
      affiliation: "Curio Collection by Hilton",
      parentCompany: "Hilton",
      officialBrandDomain: "hilton.com",
    }),
    defaultLensId: "hilton_honors",
    lenses: Object.freeze([
      Object.freeze({
        lensId: "hilton_honors",
        portfolioType: PORTFOLIO_TYPES.LOYALTY_ECOSYSTEM,
        label: "Hilton Honors",
        status: PORTFOLIO_LENS_STATUS.DEFAULT,
        constraintPhrase: "Hilton",
        loyaltyPhrase: "Hilton Honors",
        ecosystemId: "hilton_honors",
        notes: "PRIMARY — Hilton loyalty/parent portfolio in downtown Kansas City",
      }),
      Object.freeze({
        lensId: "curio_collection",
        portfolioType: PORTFOLIO_TYPES.COLLECTION_PORTFOLIO,
        label: "Curio Collection",
        status: PORTFOLIO_LENS_STATUS.OPTIONAL_FUTURE,
        constraintPhrase: "Curio Collection",
        notes: "SECONDARY future drill-down",
      }),
    ]),
    sectionMode: "BRAND_PORTFOLIO_POSITION",
  }),
});

export function getPortfolioMapping(propertyId) {
  return FIVE_PROPERTY_PORTFOLIO_MAPPING_V1[propertyId] || null;
}
