#!/usr/bin/env node
/**
 * Focused regression cases for Priority + Action Window contract v1.
 */
import assert from "node:assert/strict";
import {
  assignPriorityAndActionWindowV1,
  applyPriorityActionAssignment,
  PRIORITY_ACTION_ASSIGNMENT_VERSION,
} from "../lib/group-demand-intelligence/priority-action-window/assignment-contract-v1.js";
import { PRIORITY, BOOKING_WINDOW } from "../lib/group-demand-intelligence/claim-types.js";

function base(overrides = {}) {
  return {
    id: "gdi_opp_test",
    title: "Test occasion 2027",
    organizationName: "Test Org",
    hotelOpportunityThesis:
      "Hotel sits in the same corridor as the venue and can capture overnight demand for traveling attendees.",
    hotelRelevanceDisposition: "READY_FOR_CARD",
    venueSourcingStatus: "UNKNOWN",
    contactEmail: "events@example.org",
    primaryContact: { email: "events@example.org", name: "Events Desk" },
    prospectReviewSection: "PRIORITY",
    openBuyingInference: false,
    confirmedBuying: false,
    ...overrides,
  };
}

// 1) Strong fit + sourcing unconfirmed + usable contact → Medium + Contact
{
  const a = assignPriorityAndActionWindowV1(
    base({ hotelFitScore: 70, evidenceConfidence: 60 })
  );
  assert.equal(a.after.priority, PRIORITY.MEDIUM);
  assert.equal(a.after.bookingWindowStatus, BOOKING_WINDOW.QUALIFY_NOW);
  assert.match(a.bookingWindowReason, /unconfirmed|Contact/i);
}

// 2) Known placed host with unconfirmed overflow → Contact, not Pursue Now
{
  const a = assignPriorityAndActionWindowV1(
    base({
      opportunityType: "OVERFLOW_HOUSING",
      venueSourcingStatus: "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE",
      hotelFitScore: 68,
    })
  );
  assert.equal(a.after.bookingWindowStatus, BOOKING_WINDOW.QUALIFY_NOW);
  assert.notEqual(a.after.bookingWindowStatus, BOOKING_WINDOW.CONTACT_NOW);
}

// 3) Ongoing venue partnership → Contact / relationship development
{
  const a = assignPriorityAndActionWindowV1(
    base({
      opportunityType: "VENUE_PARTNERSHIP",
      peVenueId: "pev_test",
      demandFamily: "PRIVATE_EVENTS",
      contactUrl: "https://example.org/plan-an-event/evening-events",
      contactEmail: null,
      primaryContact: null,
    })
  );
  assert.equal(a.after.bookingWindowStatus, BOOKING_WINDOW.QUALIFY_NOW);
  assert.match(a.bookingWindowReason, /partnership|relationship/i);
}

// 4) Future occasion with unknown planning window → Too Early when far
{
  const a = assignPriorityAndActionWindowV1(
    base({
      opportunityType: "FUTURE_CYCLE",
      eventStartDate: "2028-09-01",
      planningWindowStatus: "UNKNOWN",
      hotelFitScore: 60,
    })
  );
  assert.equal(a.after.bookingWindowStatus, BOOKING_WINDOW.TOO_EARLY);
  assert.equal(a.after.priority, PRIORITY.WATCHLIST);
}

// 5) Internal enrichment candidate (no usable contact)
{
  const a = assignPriorityAndActionWindowV1(
    base({
      contactEmail: null,
      primaryContact: null,
      contactUrl: "https://example.org/",
      contactMethod: "homepage",
    })
  );
  assert.equal(a.enrichmentNeeded, true);
  assert.equal(a.after.bookingWindowStatus, BOOKING_WINDOW.WATCH);
  assert.match(a.bookingWindowReason, /enrichment/i);
  // Priority must not disguise contact gap as weak fit Watch-only packaging
  assert.equal(a.after.priority, PRIORITY.MEDIUM);
}

// 6) Same occasion, different hotel suitability
{
  const strong = assignPriorityAndActionWindowV1(
    base({ hotelFitScore: 80, evidenceConfidence: 70 })
  );
  const weak = assignPriorityAndActionWindowV1(
    base({
      hotelFitScore: 20,
      evidenceConfidence: 20,
      hotelOpportunityThesis: null,
      hotelRelevanceDisposition: null,
      summaryWhyHotel: null,
    })
  );
  assert.equal(strong.after.priority, PRIORITY.MEDIUM);
  assert.equal(weak.after.priority, PRIORITY.WATCHLIST);
}

// 7) Import defaults must not overwrite evidence-backed HIGH when onlyIfMissing
{
  const existing = base({
    priority: PRIORITY.HIGH,
    bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
    priorityReason: "prior evidence-backed High",
    hotelFitScore: 82,
    evidenceConfidence: 70,
    confirmedBuying: true,
    opportunityType: "PRIMARY_PURSUIT",
    venueSourcingStatus: "RFP_ACTIVE_SOURCING",
  });
  const applied = applyPriorityActionAssignment(existing, {
    onlyIfMissing: true,
  });
  assert.equal(applied.opportunity.priority, PRIORITY.HIGH);
  assert.equal(applied.opportunity.bookingWindowStatus, BOOKING_WINDOW.CONTACT_NOW);
}

// 8) Watch defaults ARE overwritten when evidence supports Contact
{
  const stamped = base({
    priority: PRIORITY.WATCHLIST,
    bookingWindowStatus: BOOKING_WINDOW.WATCH,
  });
  const applied = applyPriorityActionAssignment(stamped, {
    reclassifyWatchDefaults: true,
  });
  assert.equal(applied.opportunity.priority, PRIORITY.MEDIUM);
  assert.equal(applied.opportunity.bookingWindowStatus, BOOKING_WINDOW.QUALIFY_NOW);
  assert.equal(
    applied.opportunity.priorityActionAssignmentVersion,
    PRIORITY_ACTION_ASSIGNMENT_VERSION
  );
}

// 9) Confirmed open buying → Pursue Now
{
  const a = assignPriorityAndActionWindowV1(
    base({
      confirmedBuying: true,
      opportunityType: "PRIMARY_PURSUIT",
      hotelFitScore: 78,
      evidenceConfidence: 60,
      venueSourcingStatus: "RFP_ACTIVE_SOURCING",
    })
  );
  assert.equal(a.after.bookingWindowStatus, BOOKING_WINDOW.CONTACT_NOW);
  assert.equal(a.after.priority, PRIORITY.HIGH);
}

// 10) Commercial class stays independent of priority filter
{
  const a = assignPriorityAndActionWindowV1(
    base({ prospectReviewSection: "EXPLORATORY" })
  );
  assert.equal(a.commercialClass, "EXPLORATORY");
  assert.equal(a.commercialClassLabel, "Exploratory account");
  assert.equal(a.after.priority, PRIORITY.MEDIUM);
}

console.log("test-gdi-priority-action-window-assignment-v1: PASS");
