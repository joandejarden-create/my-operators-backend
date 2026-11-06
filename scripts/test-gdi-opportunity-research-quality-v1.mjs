#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  DEMAND_THESIS_CLASS,
  QUALIFICATION_LANE,
  RESEARCH_COMPLETION,
  evaluateOpportunityResearchQuality,
  evaluateHotelOpportunityInventory,
} from "../lib/group-demand-intelligence/research/gdi-opportunity-research-quality-v1.js";

const results = [];
function test(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}

test("inferred_demand_can_qualify_without_rfp", () => {
  const r = evaluateOpportunityResearchQuality({
    id: "opp1",
    title: "Regional Medical Association Annual Meeting",
    hotelDemandThesis:
      "This association historically books 120–180 guestrooms and meeting space at full-service hotels near Bethesda for its mid-Atlantic annual meeting. Prior cycles used Marriott inventory; reliable inferred lodging demand for Bethesda Marriott without claiming an open RFP.",
    sources: [{ url: "https://example.org/annual-meeting" }],
    primaryContact: { name: "Alex Rivera", title: "Meeting Planner", email: "a@example.org" },
    contactPathClass: "NAMED_CONTACT",
    contactVerified: true,
    localConnection: "Bethesda / DC metro medical association circuit",
  });
  assert.equal(r.demandClass, DEMAND_THESIS_CLASS.RELIABLE_INFERRED_DEMAND);
  assert.equal(r.qualificationLane, QUALIFICATION_LANE.SALES_READY);
  assert.equal(r.thesis.requiresConfirmedRfp, false);
});

test("generic_company_growth_alone_is_not_sales_ready", () => {
  const r = evaluateOpportunityResearchQuality({
    id: "opp2",
    title: "FastCo Inc hiring spree",
    hotelDemandThesis: "Company is growing.",
    organizationName: "FastCo Inc",
  });
  assert.ok(
    r.qualificationLane === QUALIFICATION_LANE.NEEDS_MORE_RESEARCH ||
      r.qualificationLane === QUALIFICATION_LANE.EXCLUDED
  );
  assert.notEqual(r.demandClass, DEMAND_THESIS_CLASS.CONFIRMED_DEMAND);
});

test("planner_intermediary_preserved_when_tied_to_demand", () => {
  const r = evaluateOpportunityResearchQuality({
    id: "opp3",
    title: "Incentive program via DMC",
    hotelDemandThesis:
      "DMC plans Caribbean incentive waves that need beachfront resort room blocks and F&B at Spice Island Beach Resort scale properties over the next 18 months.",
    primaryContact: { name: "Sam Lee", title: "Incentive Planner", email: "s@dmc.example" },
    buyerType: "intermediary",
    sources: [{ url: "https://dmc.example/incentives" }],
    hotelFitRationale: "Beachfront luxury fit for incentive gala + rooms",
  });
  assert.equal(r.contact.meaningfulPlannerOrIntermediary, true);
  assert.ok(
    r.qualificationLane === QUALIFICATION_LANE.ACCOUNT_DEVELOPMENT ||
      r.qualificationLane === QUALIFICATION_LANE.SALES_READY
  );
});

test("incomplete_research_not_marked_commercially_sufficient", () => {
  const r = evaluateOpportunityResearchQuality({
    id: "opp4",
    title: "Untitled org",
  });
  assert.equal(r.researchCompletion, RESEARCH_COMPLETION.INCOMPLETE);
});

test("inventory_summary_counts_lanes", () => {
  const inv = evaluateHotelOpportunityInventory(
    [
      {
        id: "a",
        title: "A",
        hotelDemandThesis:
          "Confirmed association room block historically at this hotel with planner contact and source evidence for next cycle lodging demand.",
        sources: [{ url: "https://example.org/a" }],
        primaryContact: { name: "Pat", title: "Planner", email: "p@x.org" },
        contactPathClass: "NAMED_CONTACT",
        contactVerified: true,
        localConnection: "local",
      },
      { id: "b", title: "Noise housing tab" },
    ],
    { hotelId: "recTest" }
  );
  assert.equal(inv.summary.total, 2);
  assert.ok(inv.summary.EXCLUDED >= 1 || inv.summary.NEEDS_MORE_RESEARCH >= 1);
});

const ok = results.every((r) => r.ok);
console.log(JSON.stringify({ ok, passed: results.filter((r) => r.ok).length, failed: results.filter((r) => !r.ok).length, results }, null, 2));
process.exit(ok ? 0 : 1);
