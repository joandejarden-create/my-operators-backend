/**
 * Brand Explorer Active Profile brand config model v35B.
 *
 * Single source of truth for generic factory builds — brand-specific data only.
 */
import { WOODSPRING_PROPERTY_CATALOG } from "./brand-explorer-woodspring-real-property-examples-writer.js";
import { ACTIVE_PROFILE_GALLERY_MINIMUM } from "./brand-explorer-brand-asset-image-governance.js";
import { LIFESTYLE_ACTIVE_PROFILE_BRAND_CONFIGS } from "./brand-explorer-lifestyle-affiliation-brand-config.js";

export const BRAND_CONFIG_VERSION = "v35B";

export const DEFAULT_DISALLOWED_COPY_TERMS = Object.freeze([
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

export const GALLERY_SLOT_TITLES = Object.freeze([
  "Exterior / Prototype",
  "Guest Room",
  "Kitchen-Equipped Suite",
  "Suite Work Area",
  "In-Room Kitchen Detail",
  "Extended-Stay Room Detail",
]);

/** U.S. Suburban property examples — official Choice property URLs (sitemap extract). */
export const SUBURBAN_PROPERTY_CATALOG = Object.freeze([
  {
    propertyKey: "fl894",
    propertyName: "Suburban Studios Orlando",
    marketCity: "Orlando",
    stateRegion: "Florida",
    sourcePageUrl: "https://www.choicehotels.com/florida/orlando/suburban-hotels/fl894",
    meta: "U.S. Property Example · Extended-stay studio positioning",
    chips: "Extended-Stay, Florida, Weekly-Stay Studio",
    scenario: "PROPERTY EXAMPLE / MARKET FIT",
    teaser:
      "A Suburban Studios property example for owners evaluating weekly-stay studio demand, kitchenette positioning, and Choice platform fit.",
  },
  {
    propertyKey: "oh914",
    propertyName: "Suburban Studios Columbus",
    marketCity: "Columbus",
    stateRegion: "Ohio",
    sourcePageUrl: "https://www.choicehotels.com/ohio/columbus/suburban-hotels/oh914",
    meta: "U.S. Property Example · Economy extended-stay reference",
    chips: "Extended-Stay, Ohio, Weekly Corporate Demand",
    scenario: "PROPERTY EXAMPLE / COMPETITIVE CONTEXT",
    teaser:
      "A U.S. Suburban example for owners comparing economy extended-stay positioning and operating simplicity within the Choice portfolio.",
  },
    {
    propertyKey: "ga556",
    propertyName: "Suburban Studios Kennesaw",
    marketCity: "Kennesaw",
    stateRegion: "Georgia",
    sourcePageUrl: "https://www.choicehotels.com/georgia/kennesaw/suburban-hotels/ga556",
    meta: "U.S. Property Example · Studio conversion reference",
    chips: "Extended-Stay, Georgia, Kitchenette Studio",
    scenario: "PROPERTY EXAMPLE / SUITE MODEL",
    teaser:
      "A property-level reference for owners assessing Suburban's studio model and competitive extended-stay supply in suburban corridors.",
  },
  {
    propertyKey: "fld21",
    propertyName: "Suburban Studios Ocoee",
    marketCity: "Ocoee",
    stateRegion: "Florida",
    sourcePageUrl: "https://www.choicehotels.com/florida/ocoee/suburban-hotels/fld21",
    meta: "U.S. Property Example · Orlando-metro weekly stay",
    chips: "Extended-Stay, Florida, Employment Corridor",
    scenario: "PROPERTY EXAMPLE / WEEKLY MIX",
    teaser:
      "Orlando-metro Suburban Studios listing for owners underwriting weekly studio demand, kitchenette residuals, and Choice platform fit near employment nodes.",
  },
]);

export const ACTIVE_PROFILE_BRAND_CONFIGS = Object.freeze({
  "suburban-studios": {
    slug: "suburban-studios",
    recordId: "reclcjg5Foa9Vs5TC",
    name: "Suburban Studios",
    parentCompany: "Choice Hotels International",
    brandFamily: "choice-extended-stay",
    consumerUrl: "https://www.choicehotels.com/suburban-studios",
    allowedSiblingMentions: ["suburban studios", "choice privileges", "choice hotels", "woodspring", "everhome"],
    geographicFallbackRule: "us_property_examples_when_cala_unavailable",
    propertyExampleStrategy: "us_labeled_property_examples",
    officialSourceDomains: ["choicehotels.com", "suburbanstudios.com", "choicehotelsdevelopment.com", "media.choicehotels.com"],
    imageSourcePatterns: ["hoteldam", "choicehotels.com/hoteldam"],
    galleryPoolFixture: "fixtures/choice-suburban-gallery-hoteldam-pool.json",
    propertyCatalog: SUBURBAN_PROPERTY_CATALOG,
    gallerySlotTitles: GALLERY_SLOT_TITLES,
    disallowedCopyTerms: DEFAULT_DISALLOWED_COPY_TERMS,
    galleryMinimum: ACTIVE_PROFILE_GALLERY_MINIMUM,
    scenarioMinimum: 3,
    propertyExampleMinimum: 3,
    standardDetailGovernanceRequired: true,
    protectedBrandSlugs: ["woodspring-suites", "everhome-suites"],
    overviewScenarioCopy: {
      "overview.scenario.1": {
        title: "Extended-Stay Studio Conversion",
        body:
          "Economy extended-stay studios for contractors, project crews, and temporary housing—Suburban fits when owners need weekly-rate positioning, in-room kitchenettes, and Choice Privileges distribution without full-service operating load.",
      },
      "overview.scenario.2": {
        title: "Weekly Corporate Demand Corridor",
        body:
          "Weekly-stay corridors near employment centers, hospitals, or training campuses—Suburban works when demand is project-driven and owners can align housekeeping and kitchenette FF&E to Suburban prototype bands.",
      },
      "overview.scenario.3": {
        title: "Kitchenette Conversion From Select-Service",
        body:
          "Kitchenette conversions from older select-service or independent extended-stay formats—Suburban competes when room modules support cooking facilities and owners want Choice scale without upscale public-space requirements.",
      },
    },
    momentumSourceUrls: [
      "https://www.choicehotels.com/suburban-studios",
      "https://www.choicehotelsdevelopment.com/our-brands/extended-stay/suburban-studios",
    ],
  },
  "woodspring-suites": {
    slug: "woodspring-suites",
    recordId: "recsOd51NzRPYsMko",
    name: "WoodSpring Suites",
    parentCompany: "Choice Hotels International",
    brandFamily: "choice-extended-stay",
    consumerUrl: "https://www.woodspring.com/",
    allowedSiblingMentions: ["woodspring", "choice hotels", "choice privileges"],
    geographicFallbackRule: "us_property_examples_when_cala_unavailable",
    propertyExampleStrategy: "us_labeled_property_examples",
    officialSourceDomains: ["woodspring.com", "choicehotels.com", "choicehotelsdevelopment.com"],
    imageSourcePatterns: ["hoteldam"],
    galleryPoolFixture: "fixtures/choice-woodspring-gallery-hoteldam-pool.json",
    propertyCatalog: WOODSPRING_PROPERTY_CATALOG.map((c) => ({
      propertyKey: c.sourcePageUrl.split("/").pop(),
      propertyName: c.propertyName,
      marketCity: c.marketCity,
      stateRegion: c.stateRegion,
      sourcePageUrl: c.sourcePageUrl,
      meta: c.meta,
      chips: c.chips,
      scenario: c.scenario,
      teaser: c.teaser,
      presentationRecordId: c.presentationRecordId,
    })),
    gallerySlotTitles: GALLERY_SLOT_TITLES,
    disallowedCopyTerms: DEFAULT_DISALLOWED_COPY_TERMS,
    galleryMinimum: ACTIVE_PROFILE_GALLERY_MINIMUM,
    scenarioMinimum: 3,
    propertyExampleMinimum: 3,
    standardDetailGovernanceRequired: true,
    protectedBrandSlugs: ["everhome-suites", "suburban-studios"],
    overviewScenarioCopy: null,
    momentumSourceUrls: [
      "https://www.woodspring.com/",
      "https://www.choicehotelsdevelopment.com/our-brands/extended-stay/woodspring-suites",
    ],
  },
  "everhome-suites": {
    slug: "everhome-suites",
    recordId: "recqkkrsevi4r9ibj",
    name: "Everhome Suites",
    parentCompany: "Choice Hotels International",
    brandFamily: "choice-extended-stay",
    consumerUrl: "https://www.choicehotels.com/everhome-suites",
    allowedSiblingMentions: ["everhome", "choice hotels", "woodspring", "suburban"],
    geographicFallbackRule: "us_property_examples_when_cala_unavailable",
    propertyExampleStrategy: "us_labeled_property_examples",
    officialSourceDomains: ["choicehotels.com", "choicehotelsdevelopment.com"],
    imageSourcePatterns: ["hoteldam"],
    galleryPoolFixture: null,
    propertyCatalog: [],
    gallerySlotTitles: GALLERY_SLOT_TITLES,
    disallowedCopyTerms: DEFAULT_DISALLOWED_COPY_TERMS,
    galleryMinimum: ACTIVE_PROFILE_GALLERY_MINIMUM,
    scenarioMinimum: 3,
    propertyExampleMinimum: 3,
    standardDetailGovernanceRequired: true,
    protectedBrandSlugs: ["woodspring-suites", "suburban-studios"],
    overviewScenarioCopy: null,
    momentumSourceUrls: ["https://www.choicehotels.com/everhome-suites"],
  },
  ...LIFESTYLE_ACTIVE_PROFILE_BRAND_CONFIGS,
});

export function getActiveProfileBrandConfig(slug) {
  return ACTIVE_PROFILE_BRAND_CONFIGS[String(slug || "").toLowerCase()] || null;
}

export function listActiveProfileBrandSlugs() {
  return Object.keys(ACTIVE_PROFILE_BRAND_CONFIGS);
}
