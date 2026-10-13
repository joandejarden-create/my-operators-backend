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
        officialSource: "https://www.amwa-doc.org/events/annual-meeting",
        recommendedAction: "Contact AMWA meetings staff about host hotel / overflow for 2027 DC cycle.",
        hotelFitScore: 72,
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

// AidEx regression: boilerplate summaryWhyMatters must NOT discard overflow thesis /
// accommodation housing source (surface eligibility bug fix 2026-10-03).
{
  const aidex = {
    id: "gdi_opp_aidex_geneva_11",
    title: "AidEx Geneva",
    organizationName: "AidEx / Clarion Events",
    opportunityType: "FUTURE_CYCLE",
    eventStartDate: "2026-10-21",
    eventEndDate: "2026-10-22",
    venueStatus: "Unknown",
    hotelOpportunityThesis:
      "AidEx sits at Geneva Airport (Palexpo). YOTEL Geneva Lake serves the airport / La Côte corridor and can pursue overflow or preferred listing beyond onsite Ibis/Hilton inventory.",
    summaryWhyMatters:
      "Planning for hotel accommodations is essential as the event date approaches.",
    whyNow: "Planning for hotel accommodations is essential as the event date approaches.",
    summaryWhyHotel:
      "YOTEL Geneva Lake: La Côte / Nyon / Geneva Airport corridor market access; 237 guestrooms.",
    officialSource: "https://aid-expo.com/when-where",
    discoverySource: "https://aid-expo.com/accommodation",
    hotelFitScore: 57,
    priority: "WATCHLIST",
  };
  const cls = classifyCustomerSurfaceOpportunity(aidex, { nowDate: "2026-10-03" });
  assert.equal(
    cls.disposition,
    CUSTOMER_SURFACE_DISPOSITION.KEEP_ACTIVE,
    `AidEx must KEEP_ACTIVE after surface bugfix; got ${cls.disposition} (${(cls.reasons || []).join(",")})`
  );
  assert.equal(cls.keepActive, true);
}

// Boilerplate-only (no thesis motion, no housing URL) still downgrades
{
  const bare = {
    title: "Generic Industry Summit 2027",
    organizationName: "Generic Industry Association",
    opportunityType: "FUTURE_CYCLE",
    eventStartDate: "2027-06-01",
    eventEndDate: "2027-06-03",
    venueStatus: "Unknown",
    hotelOpportunityThesis: "The hotel has sufficient capacity for attendees.",
    summaryWhyMatters:
      "Planning for hotel accommodations is essential as the event date approaches.",
    whyNow: "Planning for hotel accommodations is essential as the event date approaches.",
    officialSource: "https://example-association.org/events/2027",
    priority: "WATCHLIST",
  };
  const cls = classifyCustomerSurfaceOpportunity(bare, { nowDate: "2026-10-03" });
  assert.notEqual(cls.disposition, CUSTOMER_SURFACE_DISPOSITION.KEEP_ACTIVE);
}

console.log("test:gdi-customer-surface-revalidation OK");
