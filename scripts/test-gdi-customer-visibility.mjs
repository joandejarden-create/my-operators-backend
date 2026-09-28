/**
 * Quick unit checks for GDI customer visibility filter.
 */
import assert from "node:assert/strict";
import {
  isGdiTestOrFixtureOpportunity,
  filterCustomerFacingOpportunities,
  markAsTestOpportunity,
} from "../lib/group-demand-intelligence/customer-visibility.js";

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
  venueStatus: "NOT_ANNOUNCED",
  lodging: { overflowMentioned: true },
  priority: "HIGH_PRIORITY",
};

const filtered = filterCustomerFacingOpportunities(
  [
    keepOpp,
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
assert.equal(filtered.length, 1);
assert.equal(filtered[0].id, "keep");

console.log("test:gdi-customer-visibility OK");
