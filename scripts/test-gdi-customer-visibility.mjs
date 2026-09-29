/**
 * Quick unit checks for GDI customer visibility filter (readiness convergence V1).
 */
import assert from "node:assert/strict";
import {
  isGdiTestOrFixtureOpportunity,
  filterCustomerFacingOpportunities,
  isCustomerFacingOpportunity,
  isActiveCustomerOpportunity,
  markAsTestOpportunity,
} from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  stampLegacyVisibilityPreserved,
  LEGACY_VISIBILITY_COHORT_V1,
} from "../lib/group-demand-intelligence/legacy-visibility-compatibility-v1.js";

assert.equal(
  isGdiTestOrFixtureOpportunity({ id: "gdi_pe_test_abc", title: "x" }),
  true
);
assert.equal(
  isGdiTestOrFixtureOpportunity({ id: "x", title: "PE link test" }),
  true
);
assert.equal(
  isGdiTestOrFixtureOpportunity({
    id: "x",
    title: "Venue partnership: Bethesda GARDEN 11",
  }),
  true
);
assert.equal(
  isGdiTestOrFixtureOpportunity({
    id: "gdi_pe_781f12393f8117e7",
    title: "Woman's Club of Bethesda — Preferred Lodging Partnership",
    isTestData: false,
  }),
  false
);
assert.equal(
  isGdiTestOrFixtureOpportunity({
    id: "real",
    title: "Real corp",
    isTestData: true,
  }),
  true
);

const keepOpp = {
  id: "keep",
  title: "AMWA 112th Annual Meeting 2027 — Washington, DC area",
  organizationName: "American Medical Women's Association (AMWA)",
  eventStartDate: "2027-03-11",
  eventEndDate: "2027-03-14",
  opportunityType: "OVERFLOW_HOUSING",
  summaryWhyHotel: "NIH adjacency; hotel TBA",
  hotelFitScore: 70,
  venueStatus: "NOT_ANNOUNCED",
  lodgingEvidence: { roomBlockMentioned: true, overflowMentioned: true, status: "CONFIRMED" },
  lodging: { overflowMentioned: true },
  priority: "HIGH_PRIORITY",
  officialSource: "https://www.amwa-doc.org/events/",
  whyNow: "2027 meeting dates confirmed; housing still open.",
  recommendedAction: "Contact organizer housing desk for overflow.",
  summaryWhat:
    "American Medical Women's Association (AMWA) is associated with AMWA 112th Annual Meeting 2027 (2027-03-11 – 2027-03-14) in Washington, DC. Housing or host-hotel details are incomplete or not yet publicly locked.",
  summaryQuality: "ADEQUATE",
  contactResearchState: "RESEARCHED",
  primaryContact: { name: "Housing Desk", role: "Meetings" },
  entityClass: "NAMED_ORGANIZATION_EVENT",
};

const thinSurface = {
  id: "thin_surface",
  title: "Some Conference 2027",
  organizationName: "Some Org Association",
  eventStartDate: "2027-06-01",
  opportunityType: "OVERFLOW_HOUSING",
  lodgingEvidence: { roomBlockMentioned: true, status: "CONFIRMED" },
  priority: "MEDIUM_PRIORITY",
  officialSource: "https://example.org/event",
  summaryWhat: "Some Conference 2027",
  entityClass: "NAMED_ORGANIZATION_EVENT",
};

const filtered = filterCustomerFacingOpportunities(
  [
    keepOpp,
    thinSurface,
    markAsTestOpportunity({ id: "drop", title: "Drop me" }),
    { id: "x", title: "PE link test" },
    {
      id: "past",
      title: "Past Meeting",
      organizationName: "Past Org Association",
      eventStartDate: "2026-01-01",
      eventEndDate: "2026-01-02",
      priority: "WATCHLIST",
    },
  ],
  { nowDate: "2026-09-28" }
);
assert.equal(filtered.length, 1, "only strict-ready keepOpp");
assert.equal(filtered[0].id, "keep");

// Surface-only escape hatch still sees thin if surface-eligible
const surfaceOnly = filterCustomerFacingOpportunities([keepOpp, thinSurface], {
  nowDate: "2026-09-28",
  surfaceOnly: true,
});
assert.ok(surfaceOnly.length >= 1);

// Legacy stamp allows thin to remain visible temporarily
const legacyThin = stampLegacyVisibilityPreserved(thinSurface, {
  reason: "test",
  blockers: ["SUMMARY_THIN"],
});
assert.equal(legacyThin.legacyVisibilityCohort, LEGACY_VISIBILITY_COHORT_V1);
assert.equal(
  isCustomerFacingOpportunity(legacyThin, { nowDate: "2026-09-28" }),
  true
);

// New opportunity cannot bypass without stamp or readiness
assert.equal(
  isCustomerFacingOpportunity(thinSurface, { nowDate: "2026-09-28" }),
  false
);

// Hilton-style DQ surface remains hidden even with bogus legacy stamp
const hiltonDq = stampLegacyVisibilityPreserved(
  {
    id: "hilton_dq",
    title: "How To Manage The Paper — EXHIBITOR BLOCK",
    organizationName: "Noise",
    priority: "WATCHLIST",
    customerFacingState: "FUTURE_WATCH",
  },
  { reason: "should_not_help" }
);
assert.equal(
  isActiveCustomerOpportunity(hiltonDq, { nowDate: "2026-09-28" }),
  false
);
assert.equal(
  isCustomerFacingOpportunity(hiltonDq, { nowDate: "2026-09-28" }),
  false
);

console.log("test:gdi-customer-visibility OK");
