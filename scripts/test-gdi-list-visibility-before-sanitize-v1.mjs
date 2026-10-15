/**
 * Regression: customer list visibility must not widen when presentation
 * sanitize fills summaryWhat from summaryWhyMatters / thesis prose.
 */
import assert from "node:assert/strict";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { sanitizeOpportunityForCustomerPresentation } from "../lib/group-demand-intelligence/customer-presentation-sanitize-v1.js";
import { evaluateProspectSuitability } from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";
import { isReviewedProspectCustomerVisible } from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";

const HOTEL = "recnPB3Q02NO8Ma1o";

const peMissingTravelEvidence = {
  id: "gdi_pe_test_visibility_order_sparks",
  hotelId: HOTEL,
  title: "Sparks Gallery — Preferred Lodging Partnership",
  organizationName: "Sparks Gallery",
  opportunityType: "VENUE_PARTNERSHIP",
  customerVisible: true,
  prospectHotelFacing: true,
  prospectPublicationBasis: "REVIEWED_PACKAGE_V1",
  summaryWhat: null,
  // Presentation fallback target — must NOT count as travel evidence for gate
  summaryWhyMatters:
    "It is a non-hotel wedding venue within the configured Gaslamp catchment, creating a complementary event-space/accommodation partnership prospect.",
  hotelOpportunityThesis:
    "It is a non-hotel wedding venue within the configured Gaslamp catchment, creating a complementary event-space/accommodation partnership prospect.",
  hotelFitScore: 68,
  fitExplanation:
    "It is a non-hotel wedding venue within the configured Gaslamp catchment.",
  sources: [{ url: "https://sparksgallery.com/", label: "Official" }],
  primaryContact: {
    name: "Levi",
    email: "levi@sparksgallery.com",
  },
  contactEmail: "levi@sparksgallery.com",
};

assert.equal(
  evaluateProspectSuitability(peMissingTravelEvidence).ok,
  false,
  "canonical row lacks travel evidence"
);
assert.equal(
  isReviewedProspectCustomerVisible(peMissingTravelEvidence, { hotelId: HOTEL }),
  false,
  "reviewed gate excludes enrichment-queue PE"
);

const sanitized = sanitizeOpportunityForCustomerPresentation(peMissingTravelEvidence);
assert.ok(
  sanitized.summaryWhat,
  "sanitize fills summaryWhat from summaryWhyMatters for presentation"
);
assert.equal(
  evaluateProspectSuitability(sanitized).ok,
  true,
  "sanitize accidentally satisfies travel evidence"
);
assert.equal(
  isReviewedProspectCustomerVisible(sanitized, { hotelId: HOTEL }),
  true,
  "sanitize-then-gate would wrongly admit the row"
);

const afterSanitizeThenFilter = filterCustomerFacingOpportunities(
  [peMissingTravelEvidence].map((o) =>
    sanitizeOpportunityForCustomerPresentation(o)
  ),
  { hotelId: HOTEL }
);
assert.equal(
  afterSanitizeThenFilter.length,
  1,
  "broken order admits PE row"
);

const afterFilterThenSanitize = filterCustomerFacingOpportunities(
  [peMissingTravelEvidence],
  { hotelId: HOTEL }
).map((o) => sanitizeOpportunityForCustomerPresentation(o));
assert.equal(
  afterFilterThenSanitize.length,
  0,
  "correct order keeps PE row off the customer list"
);

console.log("PASS test-gdi-list-visibility-before-sanitize-v1");
