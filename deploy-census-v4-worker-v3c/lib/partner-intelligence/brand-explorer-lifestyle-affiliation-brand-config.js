/**
 * Lifestyle / affiliation active-profile brand configs v35B.
 *
 * Imported into brand-explorer-active-profile-brand-config.js.
 */
import { ACTIVE_PROFILE_GALLERY_MINIMUM } from "./brand-explorer-brand-asset-image-governance.js";
import {
  DESIGN_HOTELS_PROPERTY_CATALOG,
  SLH_PROPERTY_CATALOG,
  HOTEL_INDIGO_PROPERTY_CATALOG,
  MGALLERY_PROPERTY_CATALOG,
  selectPropertyExampleCatalog,
} from "./brand-explorer-lifestyle-affiliation-property-catalog.js";
import {
  LANE2_DAZZLER_PROPERTY_CATALOG,
  LANE2_TRADEMARK_PROPERTY_CATALOG,
} from "./brand-explorer-lane2-property-catalog.js";
import {
  CALA_SECTION_LABEL_DEFAULT,
  selectPropertyExamplesWithGeographicFallback,
} from "./brand-explorer-cala-property-example-rules.js";

const BASE_DISALLOWED_COPY_TERMS = Object.freeze([
  "fdd",
  "item 19",
  "franchise disclosure",
  "confirm fees",
  "adr",
  "net contribution",
  "rooms from loyalty",
  "performance representation",
  "consumer site",
  "active property page",
  "source capture",
  "metadata",
]);

export const LIFESTYLE_BRAND_CONFIG_VERSION = "v35B";

export const LIFESTYLE_GALLERY_SLOT_TITLES = Object.freeze([
  "Exterior / Arrival",
  "Guest Room / Suite",
  "Public Space",
  "F&B or Local Experience",
  "Design Detail",
  "Property Setting",
]);

export const AFFILIATION_DISALLOWED_COPY_TERMS = Object.freeze([
  ...BASE_DISALLOWED_COPY_TERMS,
  "franchise flag",
  "franchise conversion",
  "standard prototype",
  "item 19",
  "royalty on gross",
  "franchise fee schedule",
]);

export const TRIBUTE_PROPERTY_CATALOG = Object.freeze([
  {
    propertyKey: "sjutx",
    propertyName: "Hotel Rumbao",
    marketCity: "San Juan",
    stateRegion: "Puerto Rico",
    sourcePageUrl: "https://www.marriott.com/en-us/hotels/sjutx-hotel-rumbao-a-tribute-portfolio-hotel/overview/",
    meta: "CALA Urban Property Example · Old San Juan lifestyle",
    chips: "Urban, Puerto Rico, CALA, Old San Juan",
    scenario: "PROPERTY EXAMPLE / URBAN LIFESTYLE",
    teaser:
      "Old San Juan lifestyle hotel under Tribute—relevant for urban conversion deals where owners want local character with Marriott systems and Bonvoy participation.",
  },
  {
    propertyKey: "limtx",
    propertyName: "Humano Lima",
    marketCity: "Lima",
    stateRegion: "Peru",
    sourcePageUrl: "https://www.marriott.com/en-us/hotels/limtx-humano-lima-a-tribute-portfolio-hotel/overview/",
    meta: "South America Urban Property Example · Waterfront lifestyle",
    chips: "Urban, Peru, South America, Waterfront",
    scenario: "PROPERTY EXAMPLE / SOUTH AMERICA URBAN",
    teaser:
      "Malecón waterfront hotel in Lima showing Tribute's South America urban footprint for lifestyle urban affiliation comparisons.",
  },
  {
    propertyKey: "mdetx",
    propertyName: "Loma Medellín",
    marketCity: "Medellín",
    stateRegion: "Colombia",
    sourcePageUrl: "https://www.marriott.com/en-us/hotels/mdetx-loma-medellin-a-tribute-portfolio-hotel/overview/",
    meta: "Andean Urban Property Example · Secondary-city lifestyle",
    chips: "Urban, Colombia, South America",
    scenario: "PROPERTY EXAMPLE / ANDEAN URBAN",
    teaser:
      "Medellín urban hotel under Tribute—reference for Andean secondary-city lifestyle positioning with independent design sensibility inside Marriott's commercial stack.",
  },
]);

export {
  DESIGN_HOTELS_PROPERTY_CATALOG,
  SLH_PROPERTY_CATALOG,
  HOTEL_INDIGO_PROPERTY_CATALOG,
  MGALLERY_PROPERTY_CATALOG,
};

function baseLifestyleConfig({
  slug,
  recordId,
  name,
  parentCompany,
  brandModelType,
  brandFamily,
  consumerUrl,
  developmentUrl = null,
  officialSourceDomains,
  allowedSiblingMentions,
  propertyCatalog = [],
  propertyExampleCatalog = null,
  galleryPoolFixture = null,
  galleryPoolStrategy = null,
  propertyExampleStrategy = "labeled_property_examples_from_official_sources",
  geographicFallbackRule = "global_property_examples_from_official_directory",
  propertyExampleSectionLabel = null,
  overviewScenarioCopy = null,
  momentumSourceUrls = [],
  copyGovernanceMode,
  franchiseLanguageBlocked = false,
}) {
  return {
    slug,
    recordId,
    name,
    parentCompany,
    brandModelType,
    brandFamily,
    consumerUrl,
    developmentUrl,
    copyGovernanceMode,
    franchiseLanguageBlocked,
    companyValidatedProtection: true,
    propertyImagesRequired: true,
    allowedSiblingMentions,
    geographicFallbackRule,
    propertyExampleSectionLabel,
    propertyExampleStrategy,
    galleryPoolStrategy,
    officialSourceDomains,
    imageSourcePatterns: ["marriott.com", "designhotels.com", "slh.com", "ihg.com", "accor.com"],
    galleryPoolFixture,
    propertyCatalog,
    propertyExampleCatalog:
      propertyExampleCatalog || selectPropertyExampleCatalog(propertyCatalog, 3),
    gallerySlotTitles: LIFESTYLE_GALLERY_SLOT_TITLES,
    disallowedCopyTerms: franchiseLanguageBlocked ? AFFILIATION_DISALLOWED_COPY_TERMS : BASE_DISALLOWED_COPY_TERMS,
    galleryMinimum: ACTIVE_PROFILE_GALLERY_MINIMUM,
    scenarioMinimum: 3,
    propertyExampleMinimum: 3,
    standardDetailGovernanceRequired: true,
    protectedBrandSlugs: [],
    overviewScenarioCopy,
    momentumSourceUrls,
  };
}

export const LIFESTYLE_ACTIVE_PROFILE_BRAND_CONFIGS = Object.freeze({
  "design-hotels": baseLifestyleConfig({
    slug: "design-hotels",
    recordId: "rec02zPClpWUTCyXM",
    name: "Design Hotels",
    parentCompany: "Marriott International, Inc.",
    brandModelType: "affiliation_curation_platform",
    brandFamily: "marriott-affiliate-collection",
    consumerUrl: "https://www.designhotels.com/",
    developmentUrl: "https://www.marriott.com/marriott-brands/design-hotels.mi",
    officialSourceDomains: ["designhotels.com", "marriott.com", "news.marriott.com"],
    allowedSiblingMentions: ["design hotels", "marriott bonvoy", "marriott international"],
    propertyCatalog: DESIGN_HOTELS_PROPERTY_CATALOG,
    propertyExampleCatalog: selectPropertyExamplesWithGeographicFallback(
      DESIGN_HOTELS_PROPERTY_CATALOG,
      { minimum: 3 }
    ).selected,
    geographicFallbackRule: "cala_first_no_us_when_three_cala_examples",
    propertyExampleSectionLabel: CALA_SECTION_LABEL_DEFAULT,
    usFallbackBlockedWhenCalaCount: 3,
    galleryPoolStrategy: "lifestyle_property_page_probe",
    propertyExampleStrategy: "lifestyle_property_page_probe",
    copyGovernanceMode: "affiliation_curation_platform",
    franchiseLanguageBlocked: true,
    momentumSourceUrls: [
      "https://www.designhotels.com/",
      "https://www.marriott.com/marriott-brands/design-hotels.mi",
    ],
    overviewScenarioCopy: {
      "overview.scenario.1": {
        title: "Design-Led Independent Hotel",
        body:
          "Culturally distinctive, design-forward independent hotels seeking curated global recognition—Design Hotels fits when owners want to preserve local identity, architecture, and storytelling while accessing Marriott Bonvoy distribution and collection credibility.",
      },
      "overview.scenario.2": {
        title: "Conversion With Design Integrity",
        body:
          "Repositioned boutique or lifestyle assets where design narrative and guest experience authenticity are central—owners evaluate curation standards, owner control, and affiliation value without a standardized franchise prototype.",
      },
      "overview.scenario.3": {
        title: "Urban Cultural Destination",
        body:
          "Urban hotels anchored in neighborhood culture, art, and local programming—Design Hotels suits owners who prioritize independent character and design credibility over flag-standardization.",
      },
    },
  }),
  "small-luxury-hotels-of-the-world": baseLifestyleConfig({
    slug: "small-luxury-hotels-of-the-world",
    recordId: "recjjSnY2opb8P4DG",
    name: "Small Luxury Hotels of the World",
    parentCompany: "Small Luxury Hotels of the World",
    brandModelType: "independent_luxury_consortium",
    brandFamily: "slh-consortium",
    consumerUrl: "https://www.slh.com/",
    developmentUrl: "https://www.slh.com/about-slh",
    officialSourceDomains: ["slh.com"],
    allowedSiblingMentions: ["small luxury hotels", "slh", "slh club"],
    propertyCatalog: SLH_PROPERTY_CATALOG,
    propertyExampleCatalog: selectPropertyExampleCatalog(SLH_PROPERTY_CATALOG, 3),
    galleryPoolStrategy: "lifestyle_property_page_probe",
    propertyExampleStrategy: "lifestyle_property_page_probe",
    copyGovernanceMode: "independent_luxury_consortium",
    franchiseLanguageBlocked: true,
    momentumSourceUrls: ["https://www.slh.com/", "https://www.slh.com/about-slh"],
    overviewScenarioCopy: {
      "overview.scenario.1": {
        title: "Independent Luxury Boutique",
        body:
          "Owner-operated luxury boutique hotels seeking global consortium credibility—SLH fits when property quality, guest experience, and independent character meet consortium participation standards.",
      },
      "overview.scenario.2": {
        title: "Affiliation Without Chain Flag",
        body:
          "Luxury independents evaluating distribution and recognition value without converting to a chain flag—owners assess SLH quality expectations, owner control, and affiliation benefits during diligence.",
      },
      "overview.scenario.3": {
        title: "Destination Luxury Retreat",
        body:
          "Resort or destination luxury properties where place-making and service distinctiveness drive guest loyalty—SLH suits owners prioritizing luxury credibility and consortium reach over standardized brand prototypes.",
      },
    },
  }),
  "autograph-collection": baseLifestyleConfig({
    slug: "autograph-collection",
    recordId: "recEJCTDj1zrsjPM6",
    name: "Autograph Collection",
    parentCompany: "Marriott International, Inc.",
    brandModelType: "soft_brand_collection",
    brandFamily: "marriott-soft-collection",
    consumerUrl: "https://autograph-hotels.marriott.com/",
    developmentUrl: "https://development.marriott.com/our-brands/",
    officialSourceDomains: ["marriott.com", "autograph-hotels.marriott.com", "development.marriott.com"],
    allowedSiblingMentions: ["autograph collection", "marriott bonvoy", "marriott international", "tribute portfolio", "design hotels"],
    copyGovernanceMode: "soft_brand_collection",
    momentumSourceUrls: ["https://autograph-hotels.marriott.com/"],
  }),
  "tribute-portfolio": baseLifestyleConfig({
    slug: "tribute-portfolio",
    recordId: "recCvV0PuZOi8c3hC",
    name: "Tribute Portfolio",
    parentCompany: "Marriott International, Inc.",
    brandModelType: "lifestyle_conversion_brand",
    brandFamily: "marriott-soft-collection",
    consumerUrl: "https://tribute-portfolio.marriott.com/",
    developmentUrl: "https://development.marriott.com/our-brands/",
    officialSourceDomains: ["marriott.com", "tribute-portfolio.marriott.com", "development.marriott.com", "news.marriott.com"],
    allowedSiblingMentions: ["tribute portfolio", "marriott bonvoy", "autograph collection", "design hotels"],
    propertyCatalog: TRIBUTE_PROPERTY_CATALOG,
    copyGovernanceMode: "soft_brand_collection",
    momentumSourceUrls: [
      "https://tribute-portfolio.marriott.com/",
      "https://news.marriott.com/brands/tribute-portfolio",
    ],
    overviewScenarioCopy: {
      "overview.scenario.1": {
        title: "Urban Lifestyle Conversion",
        body:
          "Independent-character urban hotels seeking Marriott Bonvoy distribution and commercial systems—Tribute fits when local style, programming, and design narrative should remain visible post-affiliation.",
      },
      "overview.scenario.2": {
        title: "Resort / Leisure Repositioning",
        body:
          "Leisure or resort assets where owners want collection affiliation without a rigid full-service flag conversion—evaluate standards intensity, owner flexibility, and Bonvoy participation.",
      },
      "overview.scenario.3": {
        title: "CALA Lifestyle Affiliation",
        body:
          "Caribbean or Latin America lifestyle hotels comparing Marriott soft-collection paths—Tribute competes when owners want independent character with Marriott scale in the region.",
      },
    },
  }),
  "hotel-indigo": baseLifestyleConfig({
    slug: "hotel-indigo",
    recordId: "recegXrqaPiSLGCIe",
    name: "Hotel Indigo",
    parentCompany: "InterContinental Hotels Group",
    brandModelType: "lifestyle_full_brand",
    brandFamily: "ihg-lifestyle",
    consumerUrl: "https://www.hotelindigo.com/",
    developmentUrl: "https://development.ihg.com/brand/hotel-indigo",
    officialSourceDomains: ["hotelindigo.com", "ihg.com", "development.ihg.com", "ihgplc.com"],
    allowedSiblingMentions: ["hotel indigo", "ihg", "ihg one rewards", "vignette collection", "crowne plaza"],
    propertyCatalog: HOTEL_INDIGO_PROPERTY_CATALOG,
    propertyExampleCatalog: selectPropertyExamplesWithGeographicFallback(HOTEL_INDIGO_PROPERTY_CATALOG, {
      minimum: 3,
    }).selected,
    geographicFallbackRule: "cala_first_then_us_then_global",
    propertyExampleSectionLabel: CALA_SECTION_LABEL_DEFAULT,
    galleryPoolStrategy: "lifestyle_property_page_probe",
    propertyExampleStrategy: "lifestyle_property_page_probe",
    copyGovernanceMode: "lifestyle_full_brand",
    franchiseLanguageBlocked: true,
    momentumSourceUrls: [
      "https://www.hotelindigo.com/",
      "https://www.ihg.com/hotelindigo/hotels/us/en/reservation",
      "https://development.ihg.com/brand/hotel-indigo",
    ],
    overviewScenarioCopy: {
      "overview.scenario.1": {
        title: "Neighborhood Boutique Lifestyle",
        body:
          "Urban neighborhood hotels where local discovery, design narrative, and guest experience authenticity matter—Hotel Indigo fits when owners want IHG distribution and IHG One Rewards within a lifestyle full-brand operating model.",
      },
      "overview.scenario.2": {
        title: "Conversion / Repositioning",
        body:
          "Independent or soft-product hotels evaluating IHG affiliation where design-led repositioning and neighborhood storytelling are central—owners diligence PIP scope, IHG systems, and operating model fit.",
      },
      "overview.scenario.3": {
        title: "Urban Leisure–Business Mix",
        body:
          "Gateway and secondary-city assets blending corporate transient and leisure demand—Hotel Indigo suits owners comparing lifestyle positioning against voco, Kimpton, and Crowne Plaza tiers within IHG.",
      },
    },
  }),
  "vignette-collection": baseLifestyleConfig({
    slug: "vignette-collection",
    recordId: "recDwzv86TWnz2gGB",
    name: "Vignette Collection",
    parentCompany: "InterContinental Hotels Group",
    brandModelType: "soft_brand_collection",
    brandFamily: "ihg-soft-collection",
    consumerUrl: "https://www.ihg.com/vignettecollection/hotels/us/en/reservation",
    developmentUrl: "https://development.ihg.com/brand/vignette-collection",
    officialSourceDomains: ["ihg.com", "development.ihg.com"],
    allowedSiblingMentions: ["vignette collection", "ihg", "ihg one rewards"],
    copyGovernanceMode: "soft_brand_collection",
    momentumSourceUrls: ["https://www.ihg.com/vignettecollection/hotels/us/en/reservation"],
  }),
  "mgallery-collection": baseLifestyleConfig({
    slug: "mgallery-collection",
    recordId: "recrWCD1LMqu864oU",
    name: "MGallery Collection",
    parentCompany: "Accor",
    brandModelType: "soft_brand_collection",
    brandFamily: "accor-soft-collection",
    consumerUrl: "https://mgallery.accor.com/",
    developmentUrl: "https://group.accor.com/en/brands-and-experiences/mgallery",
    officialSourceDomains: ["accor.com", "mgallery.accor.com", "group.accor.com", "all.accor.com"],
    allowedSiblingMentions: ["mgallery", "accor", "all loyalty"],
    propertyCatalog: MGALLERY_PROPERTY_CATALOG,
    propertyExampleCatalog: selectPropertyExamplesWithGeographicFallback(MGALLERY_PROPERTY_CATALOG, {
      minimum: 3,
    }).selected,
    geographicFallbackRule: "cala_first_then_us_then_global",
    propertyExampleSectionLabel: CALA_SECTION_LABEL_DEFAULT,
    galleryPoolStrategy: "lifestyle_property_page_probe",
    propertyExampleStrategy: "lifestyle_property_page_probe",
    copyGovernanceMode: "soft_brand_collection",
    momentumSourceUrls: ["https://mgallery.accor.com/"],
    overviewScenarioCopy: {
      "overview.scenario.1": {
        title: "Distinctive Local Character",
        body:
          "Memorable hotels with strong local narrative and design identity—MGallery fits when owners want Accor collection affiliation while preserving property character and storytelling.",
      },
      "overview.scenario.2": {
        title: "Soft-Collection Conversion",
        body:
          "Independent or boutique assets evaluating Accor soft-collection path—owners diligence collection standards, conversion PIP, and ALL participation without generic Accor boilerplate.",
      },
      "overview.scenario.3": {
        title: "CALA Collection Reference",
        body:
          "Caribbean and Latin America distinctive hotels comparing Accor collection options—MGallery competes when local character and curated experience matter in the region.",
      },
    },
  }),
  "handwritten-collection": baseLifestyleConfig({
    slug: "handwritten-collection",
    recordId: "rec7hTXwMRC81EPqz",
    name: "Handwritten Collection",
    parentCompany: "Accor",
    brandModelType: "soft_brand_collection",
    brandFamily: "accor-soft-collection",
    consumerUrl: "https://all.accor.com/a/en/brands/handwritten-collection.html",
    developmentUrl: "https://group.accor.com/en/hotel-development",
    officialSourceDomains: ["accor.com", "all.accor.com", "group.accor.com", "handwritten-collection.com"],
    allowedSiblingMentions: ["handwritten collection", "accor", "all loyalty", "mgallery"],
    copyGovernanceMode: "soft_brand_collection",
    momentumSourceUrls: ["https://all.accor.com/a/en/brands/handwritten-collection.html"],
  }),
  "radisson-collection": baseLifestyleConfig({
    slug: "radisson-collection",
    recordId: "rec2DDyPu38C6zDBC",
    name: "Radisson Collection",
    parentCompany: "Choice Hotels International",
    brandModelType: "soft_brand_collection",
    brandFamily: "radisson-collection",
    consumerUrl: "https://www.radissonhotels.com/en-us/hotels/radisson-collection",
    developmentUrl: "https://www.choicehotelsdevelopment.com/",
    officialSourceDomains: ["radissonhotels.com", "choicehotels.com", "choicehotelsdevelopment.com"],
    allowedSiblingMentions: [
      "radisson collection",
      "radisson",
      "radisson blu",
      "radisson red",
      "radisson individuals",
      "choice privileges",
    ],
    copyGovernanceMode: "soft_brand_collection",
    momentumSourceUrls: ["https://www.radissonhotels.com/en-us/hotels/radisson-collection"],
  }),
  "tapestry-collection-by-hilton": baseLifestyleConfig({
    slug: "tapestry-collection-by-hilton",
    recordId: "reccXxMHEh7NNRhIE",
    name: "Tapestry Collection by Hilton",
    parentCompany: "Hilton",
    brandModelType: "soft_brand_collection",
    brandFamily: "hilton-soft-collection",
    consumerUrl: "https://www.hilton.com/en/tapestry/",
    developmentUrl: "https://www.hilton.com/en/development/",
    officialSourceDomains: ["hilton.com", "tapestrycollection.com"],
    allowedSiblingMentions: ["tapestry collection", "hilton", "hilton honors", "curio"],
    copyGovernanceMode: "soft_brand_collection",
    momentumSourceUrls: ["https://www.hilton.com/en/tapestry/"],
  }),
  "dazzler-by-wyndham": baseLifestyleConfig({
    slug: "dazzler-by-wyndham",
    recordId: "rec5CNMM4ZUD7ZHlM",
    name: "Dazzler by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    brandModelType: "lifestyle_brand",
    brandFamily: "wyndham-lifestyle",
    consumerUrl: "https://www.wyndhamhotels.com/dazzler",
    developmentUrl: "https://www.wyndhamhotels.com/dazzler",
    officialSourceDomains: ["wyndhamhotels.com", "wyndham.com"],
    allowedSiblingMentions: ["dazzler", "wyndham", "wyndham rewards"],
    copyGovernanceMode: "soft_brand_collection",
    propertyCatalog: [...LANE2_DAZZLER_PROPERTY_CATALOG],
    galleryPoolFixture: "lane2-dazzler-by-wyndham-gallery-pool.json",
    momentumSourceUrls: ["https://www.wyndhamhotels.com/dazzler"],
  }),
  "trademark-collection-by-wyndham": baseLifestyleConfig({
    slug: "trademark-collection-by-wyndham",
    recordId: "recob7tgHRryRSbeO",
    name: "Trademark Collection by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    brandModelType: "soft_brand_collection",
    brandFamily: "wyndham-soft-collection",
    consumerUrl: "https://www.wyndhamhotels.com/trademark",
    developmentUrl: "https://www.wyndhamhotels.com/trademark",
    officialSourceDomains: ["wyndhamhotels.com", "wyndham.com"],
    allowedSiblingMentions: ["trademark collection", "trademark", "wyndham", "wyndham rewards", "dazzler"],
    copyGovernanceMode: "soft_brand_collection",
    propertyCatalog: [...LANE2_TRADEMARK_PROPERTY_CATALOG],
    galleryPoolFixture: "lane2-trademark-collection-by-wyndham-gallery-pool.json",
    momentumSourceUrls: ["https://www.wyndhamhotels.com/trademark"],
  }),
});
