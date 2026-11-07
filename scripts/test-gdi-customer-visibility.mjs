/**
 * Quick unit checks for GDI customer visibility filter (readiness convergence V1).
 *
 * Commercial readiness (preserved):
 *   verified named commercially relevant buyer OR verified named commercial entry
 *   point + credible hotel-specific demand thesis.
 * Not required: proven booking authority, confirmed room nights, immediate RFP,
 * or two named contacts.
 * Not allowed: unrelated / unverified named contacts; unsupported demand.
 */
import assert from "node:assert/strict";
import {
  isGdiTestOrFixtureOpportunity,
  filterCustomerFacingOpportunities,
  isCustomerFacingOpportunity,
  isActiveCustomerOpportunity,
  markAsTestOpportunity,
} from "../lib/group-demand-intelligence/customer-visibility.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
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

const NOW = "2026-09-28";

/** Shared hotel-specific demand thesis + future event surface. */
const demandThesisBase = {
  title: "AMWA 112th Annual Meeting 2027 — Washington, DC area",
  organizationName: "American Medical Women's Association (AMWA)",
  eventStartDate: "2027-03-11",
  eventEndDate: "2027-03-14",
  opportunityType: "OVERFLOW_HOUSING",
  summaryWhyHotel:
    "NIH / Bethesda corridor adjacency; AMWA meeting housing overflow is a credible fit for the hotel's group product.",
  hotelFitScore: 70,
  venueStatus: "NOT_ANNOUNCED",
  lodgingEvidence: {
    roomBlockMentioned: true,
    overflowMentioned: true,
    status: "CONFIRMED",
  },
  lodging: { overflowMentioned: true },
  priority: "HIGH_PRIORITY",
  officialSource: "https://www.amwa-doc.org/events/",
  whyNow: "2027 meeting dates confirmed; housing still open.",
  recommendedAction: "Contact AMWA meetings lead for overflow lodging.",
  summaryWhat:
    "American Medical Women's Association (AMWA) is associated with AMWA 112th Annual Meeting 2027 (2027-03-11 – 2027-03-14) in Washington, DC. Housing or host-hotel details are incomplete or not yet publicly locked.",
  summaryQuality: "ADEQUATE",
  contactResearchState: "RESEARCHED",
  entityClass: "NAMED_ORGANIZATION_EVENT",
};

/** Positive: verified named commercially relevant buyer + demand thesis. */
const keepOpp = {
  ...demandThesisBase,
  id: "keep",
  primaryContact: {
    name: "Jordan Blake",
    role: "Director of Meetings",
    email: "meetings@amwa-doc.org",
    sourceUrl: "https://www.amwa-doc.org/contact/",
  },
  publicContactPath: "https://www.amwa-doc.org/contact/",
  buyerRole: "Director of Meetings",
};

/** Positive: credible commercial entry point (functional housing URL) without a named person. */
const entryPointOpp = {
  ...demandThesisBase,
  id: "entry_point",
  primaryContact: { name: "Meetings Desk", role: "Meetings" },
  publicContactPath: "https://www.amwa-doc.org/events/housing/",
  organizationContactUrl: "https://www.amwa-doc.org/events/housing/",
  buyerFunction: "meetings_housing",
};

/** Negative: role label / desk stamp without usable commercial route (legacy keepOpp shape). */
const deskWithoutRoute = {
  ...demandThesisBase,
  id: "desk_no_route",
  primaryContact: { name: "Housing Desk", role: "Meetings" },
};

/** Negative: unrelated named contact (press) must not qualify. */
const unrelatedContact = {
  ...demandThesisBase,
  id: "unrelated_press",
  primaryContact: {
    name: "Sam Courier",
    role: "Staff Writer",
    email: "press@amwa-doc.org",
  },
  publicContactPath: "https://www.amwa-doc.org/news/",
  buyerRole: "Staff Writer",
};

/** Negative: unsupported demand (no hotel thesis / why-now / action). */
const unsupportedDemand = {
  ...demandThesisBase,
  id: "unsupported_demand",
  summaryWhyHotel: "",
  hotelFitScore: null,
  whyNow: "",
  recommendedAction: "",
  lodgingEvidence: { status: "UNKNOWN" },
  lodging: {},
  primaryContact: {
    name: "Jordan Blake",
    role: "Director of Meetings",
    email: "meetings@amwa-doc.org",
  },
  publicContactPath: "https://www.amwa-doc.org/contact/",
  buyerRole: "Director of Meetings",
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

{
  const named = isGdiCustomerOpportunityReady(keepOpp, { nowDate: NOW });
  assert.equal(named.ok, true, `named buyer must be Ready: ${named.failed}`);
  assert.equal(named.buyerContactPathClass, "NAMED_BUYER_PERSON");
}

{
  const entry = isGdiCustomerOpportunityReady(entryPointOpp, { nowDate: NOW });
  assert.equal(entry.ok, true, `entry point must be Ready: ${entry.failed}`);
  assert.equal(entry.buyerContactPathClass, "RELEVANT_FUNCTION_CONTACT");
}

{
  const desk = isGdiCustomerOpportunityReady(deskWithoutRoute, { nowDate: NOW });
  assert.equal(desk.ok, false);
  assert.ok(desk.failed.includes("buyer_contact_path_insufficient"));
  assert.equal(desk.state, "HELD_FOR_WHO_RESEARCH");
}

{
  const press = isGdiCustomerOpportunityReady(unrelatedContact, { nowDate: NOW });
  assert.equal(press.ok, false, "unrelated press contact must not qualify");
  assert.ok(press.failed.includes("buyer_contact_path_insufficient"));
  assert.equal(press.buyerContactPathReason, "named_contact_commercially_irrelevant");
}

{
  const unsupported = isGdiCustomerOpportunityReady(unsupportedDemand, {
    nowDate: NOW,
  });
  assert.equal(unsupported.ok, false, "unsupported demand must not qualify");
  assert.ok(
    unsupported.failed.includes("hotel_fit") ||
      unsupported.failed.includes("why_now") ||
      unsupported.failed.includes("recommended_action")
  );
}

const filtered = filterCustomerFacingOpportunities(
  [
    keepOpp,
    entryPointOpp,
    deskWithoutRoute,
    unrelatedContact,
    unsupportedDemand,
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
  { nowDate: NOW }
);
assert.equal(filtered.length, 2, "only Ready named-buyer + entry-point");
assert.deepEqual(
  filtered.map((o) => o.id).sort(),
  ["entry_point", "keep"]
);

// Surface-only escape hatch still sees thin if surface-eligible
const surfaceOnly = filterCustomerFacingOpportunities([keepOpp, thinSurface], {
  nowDate: NOW,
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
  isCustomerFacingOpportunity(legacyThin, { nowDate: NOW }),
  true
);

// New opportunity cannot bypass without stamp or readiness
assert.equal(
  isCustomerFacingOpportunity(thinSurface, { nowDate: NOW }),
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
  isActiveCustomerOpportunity(hiltonDq, { nowDate: NOW }),
  false
);
assert.equal(
  isCustomerFacingOpportunity(hiltonDq, { nowDate: NOW }),
  false
);

console.log("test:gdi-customer-visibility OK");
