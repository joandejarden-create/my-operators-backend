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

const filtered = filterCustomerFacingOpportunities([
  { id: "keep", title: "Keep me" },
  markAsTestOpportunity({ id: "drop", title: "Drop me" }),
  { id: "x", title: "PE link test" },
]);
assert.equal(filtered.length, 1);
assert.equal(filtered[0].id, "keep");

console.log("test:gdi-customer-visibility OK");
