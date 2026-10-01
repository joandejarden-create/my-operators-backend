/**
 * Fairfield by Marriott — corrected footprint.openings fixture rows (Stage 2C bar).
 */
import {
  buildOpeningsPropertyCardBody,
  buildOpeningsPropertyCardTitle,
} from "./brand-explorer-openings-property-card-contract.js";

const CANCUN_URL =
  "https://www.marriott.com/en-us/hotels/cunfi-fairfield-inn-and-suites-cancun-airport/overview/";
const NYCMW_URL =
  "https://www.marriott.com/en-us/hotels/nycmw-fairfield-inn-and-suites-new-york-manhattan-times-square-south/overview/";
const NYCFT_URL =
  "https://www.marriott.com/en-us/hotels/nycft-fairfield-inn-and-suites-new-york-manhattan-central-park/overview/";

function openingRow({
  propertyName,
  marketCity,
  geographyLabel,
  chips,
  locationLine,
  metaLine,
  scenarioLine,
  teaser,
  imageUrl,
  sort,
}) {
  const brandName = "Fairfield by Marriott";
  return {
    slotKey: "footprint.openings",
    title: buildOpeningsPropertyCardTitle({ propertyName, brandName, marketCity }),
    body: buildOpeningsPropertyCardBody({
      chips,
      locationLine,
      metaLine,
      scenarioLine,
      teaser,
      sourceUrl: "",
    }),
    sort,
    imageUrl,
    caseSummaryOverview: teaser,
    caseSummaryTags: chips.join(", "),
    caseSummaryBrandRelevance:
      geographyLabel === "CALA"
        ? "Official CALA property photography used as a Brand Explorer property example for this brand."
        : "Official International Reference property photography for Brand Explorer openings — not a CALA operating claim.",
    caseSummaryOwnerObjective:
      "Use as a directional property reference when underwriting product fit, capital scope, and platform participation.",
    caseSummaryInterpretation:
      "Confirm live affiliation criteria and property-specific scope with the brand before underwriting.",
  };
}

export function buildFairfieldOpeningsFixtureRows() {
  return [
    openingRow({
      propertyName: "Fairfield Inn & Suites Cancun Airport",
      marketCity: "Cancún",
      geographyLabel: "CALA",
      chips: ["CALA", "Cancún", "Property example"],
      locationLine: "Cancún, Mexico (CALA)",
      metaLine: "CALA · Mexico",
      scenarioLine: "CALA / CANCÚN / PROPERTY EXAMPLE",
      teaser:
        "Fairfield Inn & Suites Cancun Airport illustrates Fairfield’s lean select-service rooms product near Cancún International Airport—Bonvoy distribution with limited F&B and airport-transient demand.",
      imageUrl:
        "https://cache.marriott.com/content/dam/marriott-renditions/CUNFI/cunfi-exterior-4003-hor-wide.jpg",
      sort: 491,
    }),
    openingRow({
      propertyName: "Fairfield Inn & Suites New York Manhattan/Times Square South",
      marketCity: "New York",
      geographyLabel: "International Reference",
      chips: ["International Reference", "New York", "Property example"],
      locationLine: "New York, USA (International Reference)",
      metaLine: "International Reference · United States",
      scenarioLine: "INTERNATIONAL REFERENCE / NEW YORK / PROPERTY EXAMPLE",
      teaser:
        "Times Square South shows Fairfield’s urban select-service box in a high-transient Manhattan node—rooms-first intensity without Courtyard-level meetings or F&B capital.",
      imageUrl:
        "https://cache.marriott.com/content/dam/marriott-renditions/NYCMW/nycmw-exterior-3552-hor-wide.jpg",
      sort: 492,
    }),
    openingRow({
      propertyName: "Fairfield Inn & Suites New York Manhattan/Central Park",
      marketCity: "New York",
      geographyLabel: "International Reference",
      chips: ["International Reference", "New York", "Property example"],
      locationLine: "New York, USA (International Reference)",
      metaLine: "International Reference · United States",
      scenarioLine: "INTERNATIONAL REFERENCE / NEW YORK / PROPERTY EXAMPLE",
      teaser:
        "Central Park positions Fairfield as a practical Manhattan rooms product for business and leisure transient demand—compare operating scope against Courtyard and SpringHill before underwriting.",
      imageUrl:
        "https://cache.marriott.com/content/dam/marriott-renditions/NYCFT/nycft-exterior-0001-hor-wide.jpg",
      sort: 493,
    }),
  ];
}

export const FAIRFIELD_OPENINGS_SOURCE_URLS = Object.freeze({
  cancun: CANCUN_URL,
  nycmw: NYCMW_URL,
  nycft: NYCFT_URL,
});
