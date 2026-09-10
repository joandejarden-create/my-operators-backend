/**
 * Governed property affiliation → Brand & Portfolio lens mapping.
 * PRIMARY: LOYALTY_ECOSYSTEM for branded hotels; INDEPENDENT for independents.
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
  choice: {
    recommendedCustomerLabel: "Choice Privileges",
    alternativesConsidered: ["Choice Portfolio", "Radisson Hotels"],
    rationale: "Choice Privileges is the loyalty frame for Choice / Radisson / Faranda affiliations.",
    promptDefaultConstraint: "Choice",
    promptLoyaltyConstraint: "Choice Privileges",
    equivalence: Object.freeze(["Choice", "Choice Privileges", "Choice hotels", "Radisson"]),
  },
  independent: {
    recommendedCustomerLabel: "Independent Positioning",
    promptDefaultConstraint: "independent",
  },
});

export const ADP_PROPERTY_PORTFOLIO_MAPPING_V1 = Object.freeze({
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

  adp_bethesda_marriott: Object.freeze({
    propertyId: "adp_bethesda_marriott",
    name: "Bethesda Marriott",
    profileEvidence: Object.freeze({
      brand: "Marriott Hotels",
      affiliation: "Marriott Hotels",
      parentCompany: "Marriott International",
      officialBrandDomain: "marriott.com",
      marriottHotelCode: "WASBT",
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
        notes:
          "PRIMARY — Marriott Bonvoy loyalty ecosystem within Bethesda / Montgomery County market; peer set deferred to certified expansion hierarchy",
      }),
      Object.freeze({
        lensId: "marriott_hotels",
        portfolioType: PORTFOLIO_TYPES.HARD_BRAND_PORTFOLIO,
        label: "Marriott Hotels",
        status: PORTFOLIO_LENS_STATUS.OPTIONAL_FUTURE,
        constraintPhrase: "Marriott Hotels",
        notes: "SECONDARY future hard-brand drill-down",
      }),
    ]),
    sectionMode: "BRAND_PORTFOLIO_POSITION",
  }),

  adp_jw_marriott_santo_domingo: Object.freeze({
    propertyId: "adp_jw_marriott_santo_domingo",
    name: "JW Marriott Santo Domingo",
    profileEvidence: Object.freeze({
      brand: "JW Marriott",
      affiliation: "JW Marriott",
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
        notes: "PRIMARY — Marriott Bonvoy within Greater Santo Domingo",
      }),
    ]),
    sectionMode: "BRAND_PORTFOLIO_POSITION",
  }),

  adp_jw_marriott_monterrey_valle: Object.freeze({
    propertyId: "adp_jw_marriott_monterrey_valle",
    name: "JW Marriott Monterrey Valle",
    profileEvidence: Object.freeze({
      brand: "JW Marriott",
      affiliation: "JW Marriott",
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
        notes: "PRIMARY — Marriott Bonvoy within Monterrey Valle",
      }),
    ]),
    sectionMode: "BRAND_PORTFOLIO_POSITION",
  }),

  adp_westin_monterrey_valle: Object.freeze({
    propertyId: "adp_westin_monterrey_valle",
    name: "The Westin Monterrey Valle",
    profileEvidence: Object.freeze({
      brand: "Westin",
      affiliation: "Westin",
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
        notes: "PRIMARY — Marriott Bonvoy within Monterrey Valle",
      }),
    ]),
    sectionMode: "BRAND_PORTFOLIO_POSITION",
  }),

  adp_st_regis_mexico_city: Object.freeze({
    propertyId: "adp_st_regis_mexico_city",
    name: "The St. Regis Mexico City",
    profileEvidence: Object.freeze({
      brand: "St. Regis",
      affiliation: "St. Regis",
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
        notes: "PRIMARY — Marriott Bonvoy within Mexico City",
      }),
    ]),
    sectionMode: "BRAND_PORTFOLIO_POSITION",
  }),

  adp_st_regis_cap_cana: Object.freeze({
    propertyId: "adp_st_regis_cap_cana",
    name: "The St. Regis Cap Cana Resort",
    profileEvidence: Object.freeze({
      brand: "St. Regis",
      affiliation: "St. Regis",
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
        notes: "PRIMARY — Marriott Bonvoy within Cap Cana / Punta Cana",
      }),
    ]),
    sectionMode: "BRAND_PORTFOLIO_POSITION",
  }),

  adp_radisson_santo_domingo: Object.freeze({
    propertyId: "adp_radisson_santo_domingo",
    name: "Radisson Hotel Santo Domingo",
    profileEvidence: Object.freeze({
      brand: "Radisson",
      affiliation: "Radisson",
      parentCompany: "Choice Hotels",
      officialBrandDomain: "radissonhotels.com",
    }),
    defaultLensId: "choice_privileges",
    lenses: Object.freeze([
      Object.freeze({
        lensId: "choice_privileges",
        portfolioType: PORTFOLIO_TYPES.LOYALTY_ECOSYSTEM,
        label: "Choice Privileges",
        status: PORTFOLIO_LENS_STATUS.DEFAULT,
        constraintPhrase: "Choice",
        loyaltyPhrase: "Choice Privileges",
        ecosystemId: "choice_privileges",
        notes: "PRIMARY — Choice Privileges / Radisson within Greater Santo Domingo",
      }),
    ]),
    sectionMode: "BRAND_PORTFOLIO_POSITION",
  }),

  adp_faranda_collection_bogota: Object.freeze({
    propertyId: "adp_faranda_collection_bogota",
    name: "Faranda Collection Bogotá",
    profileEvidence: Object.freeze({
      brand: "Faranda Collection",
      affiliation: "Faranda Collection",
      parentCompany: "Choice Hotels",
      officialBrandDomain: null,
    }),
    defaultLensId: "choice_privileges",
    lenses: Object.freeze([
      Object.freeze({
        lensId: "choice_privileges",
        portfolioType: PORTFOLIO_TYPES.LOYALTY_ECOSYSTEM,
        label: "Choice Privileges",
        status: PORTFOLIO_LENS_STATUS.DEFAULT,
        constraintPhrase: "Choice",
        loyaltyPhrase: "Choice Privileges",
        ecosystemId: "choice_privileges",
        notes: "PRIMARY — Choice Privileges within Bogotá",
      }),
    ]),
    sectionMode: "BRAND_PORTFOLIO_POSITION",
  }),

  adp_hotel_caribe_faranda_grand: Object.freeze({
    propertyId: "adp_hotel_caribe_faranda_grand",
    name: "Hotel Caribe Faranda Grand",
    profileEvidence: Object.freeze({
      brand: "Faranda Grand",
      affiliation: "Faranda Grand",
      parentCompany: "Choice Hotels",
      officialBrandDomain: null,
    }),
    defaultLensId: "choice_privileges",
    lenses: Object.freeze([
      Object.freeze({
        lensId: "choice_privileges",
        portfolioType: PORTFOLIO_TYPES.LOYALTY_ECOSYSTEM,
        label: "Choice Privileges",
        status: PORTFOLIO_LENS_STATUS.DEFAULT,
        constraintPhrase: "Choice",
        loyaltyPhrase: "Choice Privileges",
        ecosystemId: "choice_privileges",
        notes: "PRIMARY — Choice Privileges within Cartagena",
      }),
    ]),
    sectionMode: "BRAND_PORTFOLIO_POSITION",
  }),
});

/** @deprecated name retained for compatibility — mapping now covers full ADP universe. */
export const FIVE_PROPERTY_PORTFOLIO_MAPPING_V1 = ADP_PROPERTY_PORTFOLIO_MAPPING_V1;

export function getPortfolioMapping(propertyId) {
  return ADP_PROPERTY_PORTFOLIO_MAPPING_V1[propertyId] || null;
}
