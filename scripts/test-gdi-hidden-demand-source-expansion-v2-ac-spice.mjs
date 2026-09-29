#!/usr/bin/env node
/**
 * Tests — GDI Hidden Demand Source Expansion V2 (AC / Spice)
 */
import assert from "node:assert/strict";
import {
  SOURCE_FAMILY,
  buildMotionQueries,
  classifyMotionFromText,
  classifyLodgingEvidence,
  classifyTiming,
  scoreFamilyVerdict,
  TIMING_CLASS,
  LODGING_EVIDENCE,
  ADDRESSABLE_MOTION,
  PLAYBOOK_VERDICT,
  PRIOR_FAMILY_STATUS,
} from "../lib/group-demand-intelligence/hidden-demand/source-family-catalog-v2.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { buildGdiOpportunitySummary } from "../lib/group-demand-intelligence/opportunity-summary-v1.js";

const AC = "rec2PVBDavppGpenm";
const SPICE = "recKRJjcPnb4tVDDS";
const BETHESDA = "recLuxvwwxID7U2B8";

// Source family classification
assert.ok(SOURCE_FAMILY.MARINE_YACHT);
assert.ok(buildMotionQueries(AC, SOURCE_FAMILY.MARINE_YACHT, { city: "A Coruña" }).length >= 2);
assert.ok(buildMotionQueries(SPICE, SOURCE_FAMILY.DMC_INCENTIVE, { city: "St George's" }).length >= 2);
assert.equal(buildMotionQueries(AC, "EVENT_CALENDAR", {}).length, 0);

// Motion classification
assert.equal(
  classifyMotionFromText("contractor team deployment A Coruña infrastructure project"),
  ADDRESSABLE_MOTION.PROJECT_TEAM
);
assert.equal(
  classifyMotionFromText("incentive travel program Grenada DMC", SOURCE_FAMILY.DMC_INCENTIVE),
  ADDRESSABLE_MOTION.INCENTIVE_GROUP
);

// Lodging evidence
assert.equal(
  classifyLodgingEvidence("official hotel block accommodation for visiting team"),
  LODGING_EVIDENCE.DIRECT
);
assert.equal(classifyLodgingEvidence("company listed in directory"), LODGING_EVIDENCE.NONE);

// Timing — historical blocked
assert.equal(classifyTiming("conference held in 2019 with 500 attendees"), TIMING_CLASS.HISTORICAL_ONLY);

// Scorecard verdict
assert.equal(
  scoreFamilyVerdict({ fetches: 5, validMotions: 2, lodgingSupported: 1, ready: 1 }),
  PLAYBOOK_VERDICT.PROMOTE_TO_STANDARD_PLAYBOOK
);

// Readiness gate — calendar-only should not pass
const calendarOpp = buildGdiOpportunitySummary({
  title: "Citywide Festival 2027",
  organizationName: "Tourism Board",
  officialSource: "https://example.com/events",
  contactResearchAttempted: true,
  contactResearchState: "ATTEMPTED",
  publicContactCeilingReason: "PUBLIC_DATA_CEILING",
  hotelFitScore: 0.2,
  summaryWhyHotel: "Generic city event.",
  whyNow: "Listed on calendar.",
  recommendedAction: "Monitor.",
});
const calReady = isGdiCustomerOpportunityReady(calendarOpp);
assert.equal(calReady.ok, false, "calendar-only should not be customer-ready");

// Bethesda not in prior inventory mutation
assert.ok(!PRIOR_FAMILY_STATUS[BETHESDA]);

// No Surfe auto env
assert.notEqual(process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED, "1");

console.log("test-gdi-hidden-demand-source-expansion-v2-ac-spice: PASS");
