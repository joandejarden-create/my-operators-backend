/**
 * Unit tests: entity truth + customer surface revalidation (V5A).
 */
import assert from "node:assert/strict";
import { classifyEntityTruth, ENTITY_CLASS } from "../lib/group-demand-intelligence/entity-truth-gate-v1.js";
import {
  classifyCustomerSurfaceOpportunity,
  CUSTOMER_SURFACE_DISPOSITION,
} from "../lib/group-demand-intelligence/customer-surface-revalidation-v1.js";
import {
  filterCustomerFacingOpportunities,
} from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  applyCommercialCardContract,
  scoreCardCompleteness,
} from "../lib/group-demand-intelligence/commercial-card-contract-v1.js";

const NOW = { nowDate: "2026-09-28" };

// UI chrome
assert.equal(
  classifyEntityTruth({ organizationName: "Interested in Exhibiting", title: "Interested in Exhibiting — EXHIBITOR BLOCK" }).entityClass,
  ENTITY_CLASS.UI_CHROME
);
assert.equal(
  classifyEntityTruth({ organizationName: "Powered by A2Z Events", title: "Powered by A2Z Events — EXHIBITOR BLOCK" }).entityClass,
  ENTITY_CLASS.PLATFORM_ATTRIBUTION
);

// Hotel promotion / directory
assert.equal(
  classifyEntityTruth({ organizationName: "City of New York", title: "NYC Hotel Week" }).entityClass,
  ENTITY_CLASS.HOTEL_PROMOTION
);
assert.ok(
  [ENTITY_CLASS.GENERIC_MARKET_PAGE, ENTITY_CLASS.PLATFORM_ATTRIBUTION, ENTITY_CLASS.DIRECTORY_TITLE].includes(
    classifyEntityTruth({
      organizationName: "Easy RFP",
      title: "Tech Conference Venues in New York 2026",
    }).entityClass
  )
);

// Session title
assert.equal(
  classifyEntityTruth({
    organizationName: "How To Manage The Paper...And The Computer",
    title: "How To Manage The Paper...And The Computer — EXHIBITOR BLOCK",
  }).entityClass,
  ENTITY_CLASS.SESSION_TITLE
);

// Valid company
assert.equal(
  classifyEntityTruth({ organizationName: "Vanguard Industrial Corp", title: "Vanguard Industrial Corp — EXHIBITOR BLOCK" }).validEntity,
  true
);

// Past closed → not customer facing
{
  const cls = classifyCustomerSurfaceOpportunity(
    {
      title: "CDA Annual Meeting 2026",
      organizationName: "Colonial Dames of America",
      eventStartDate: "2026-05-02",
      eventEndDate: "2026-05-04",
    },
    NOW
  );
  assert.equal(cls.disposition, CUSTOMER_SURFACE_DISPOSITION.PAST_CLOSED);
}

// Valid exhibitor without team → insufficient team
{
  const cls = classifyCustomerSurfaceOpportunity(
    {
      title: "FRANCHISE Solutions Group — EXHIBITOR BLOCK",
      organizationName: "FRANCHISE Solutions Group",
      eventStartDate: "2027-06-01",
      opportunityType: "FUTURE_WATCH",
    },
    NOW
  );
  assert.equal(cls.disposition, CUSTOMER_SURFACE_DISPOSITION.INSUFFICIENT_TEAM_PROOF);
}

// Filter excludes past + chrome
{
  const filtered = filterCustomerFacingOpportunities(
    [
      {
        id: "past",
        title: "CDA Annual Meeting 2026",
        organizationName: "Colonial Dames of America",
        eventStartDate: "2026-05-02",
        eventEndDate: "2026-05-04",
        priority: "WATCHLIST",
      },
      {
        id: "chrome",
        title: "Interested in Exhibiting — EXHIBITOR BLOCK",
        organizationName: "Interested in Exhibiting",
        eventStartDate: "2027-06-01",
        priority: "WATCHLIST",
      },
      {
        id: "keep",
        title: "AMWA 112th Annual Meeting 2027 — Washington, DC area",
        organizationName: "American Medical Women's Association (AMWA)",
        eventStartDate: "2027-03-11",
        eventEndDate: "2027-03-14",
        opportunityType: "OVERFLOW_HOUSING",
        summaryWhyHotel: "NIH adjacency; hotel TBA for DC-area medical meeting",
        venueStatus: "NOT_ANNOUNCED",
        lodging: { overflowMentioned: true, status: "UNKNOWN_AFTER_RESEARCH" },
        priority: "HIGH_PRIORITY",
        primaryContact: { name: "AMWA meetings staff", role: "Meetings", email: "associatedirector@amwa-doc.org" },
        whyNow: "CONTACT NOW: 2027 dates public; hotel not named",
        summaryWhat: "AMWA annual meeting announced for DC area; host hotel not named.",
        opportunityQualificationLabel: "Verified Open",
        bookingWindowStatus: "CONTACT_NOW",
        segment: "Medical",
      },
    ],
    NOW
  );
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].id, "keep");
}

// Card contract: no duplicated title as summary
{
  const card = applyCommercialCardContract({
    title: "Aidoc Open",
    organizationName: "Aidoc",
    summaryWhat: "Aidoc Open",
    eventStartDate: "2026-10-20",
    eventEndDate: "2026-10-21",
    lodging: { overflowMentioned: true },
    summaryWhyHotel: "Strong fit for Midtown medical demand.",
    whyNow: "Qualify now — lodging not yet public",
    primaryContact: { name: "Jane Smith", role: "Director of Events", email: "jane@aidoc.com" },
    bookingWindowStatus: "QUALIFY_NOW",
    opportunityQualificationLabel: "Moderate",
    segment: "Medical",
    opportunityType: "OVERFLOW_HOUSING",
  });
  assert.notEqual(card.commercialSummary, "Aidoc Open");
  assert.ok(card.commercialSummary.includes("Aidoc"));
  assert.equal(card.cardContact.pathClass, "NAMED_DIRECT");
  const score = scoreCardCompleteness(card);
  assert.ok(score.score >= 8, `expected high card completeness, got ${score.score}: ${score.missing}`);
}

console.log("test:gdi-customer-surface-revalidation OK");
