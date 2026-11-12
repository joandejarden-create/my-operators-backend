#!/usr/bin/env node
/**
 * Offline: PLP/PEP/LCM discovery targeting + two-stage paid-deep dedupe.
 * No paid provider calls.
 */
import assert from "node:assert/strict";
import { buildPreferredLodgingDiscoveryPrompt } from "../lib/group-demand-intelligence/preferred-lodging-partnership/exa-preferred-lodging-contracts-v1.js";
import { buildProfessionalEventPlannerDiscoveryPrompt } from "../lib/group-demand-intelligence/professional-event-planners/exa-professional-event-planner-contracts-v1.js";
import { getSegment912ExaContracts } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-segments-9-12-contracts-v1.js";
import { buildSegmentDiscoveryQueries } from "../lib/group-demand-intelligence/research/segment-discovery-adapters-v1.js";
import {
  evaluatePrePaidDeepDedupe,
  reconcilePostDeepIdentity,
  orgDeepKey,
  buildInventoryDedupeContextForHotels,
} from "../lib/group-demand-intelligence/research/gdi-paid-deep-two-stage-dedupe-v1.js";
import { collectRetainedCandidatesFromDiscovery } from "../lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js";
import { buildLocalMeetingsCateringResearchQueries } from "../lib/group-demand-intelligence/workflow/local-meetings-catering-research-v1.js";

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`PASS  ${name}`);
}

const hotel = {
  hotelId: "recLuxvwwxID7U2B8",
  displayName: "Bethesda Marriott",
  name: "Bethesda Marriott",
  city: "Bethesda",
  market: "Bethesda",
  address: "5151 Pooks Hill Rd, Bethesda, MD",
};

check("plp_does_not_reject_meeting_catering_venues", () => {
  const p = buildPreferredLodgingDiscoveryPrompt({ hotel });
  const blob = JSON.stringify(p);
  assert.match(blob, /Do NOT reject a venue merely because it offers meeting space/i);
  assert.match(blob, /complementary/i);
});

check("pep_targets_planner_firms_not_conference_brands", () => {
  const p = buildProfessionalEventPlannerDiscoveryPrompt({ hotel });
  const blob = JSON.stringify(p);
  assert.match(blob, /planning companies, DMCs, meeting planners/i);
  assert.match(blob, /Do not substitute the conference brand/i);
});

check("lcm_contract_excludes_destination_conferences", () => {
  const c = getSegment912ExaContracts("local_catering_day_meetings");
  assert.match(c.discoveryObjective, /NOT predominantly large destination conferences/i);
  assert.match(c.deepResearchObjective, /Reject or re-route large multi-day destination conferences/i);
});

check("adapters_serialize_plp_pep_prompts_not_object_object", () => {
  const plp = buildSegmentDiscoveryQueries("preferred_lodging_partnerships", hotel);
  const pep = buildSegmentDiscoveryQueries("professional_event_planners", hotel);
  assert.ok(plp.queries?.length >= 2);
  assert.ok(pep.queries?.length >= 2);
  for (const q of [...plp.queries, ...pep.queries]) {
    assert.ok(!/\[object Object\]/.test(q.query), q.query);
    assert.ok(q.query.length > 40);
  }
  assert.ok(
    pep.queries.some((q) => /planner|DMC|event management/i.test(q.query))
  );
});

check("lcm_queries_exclude_annual_conference_bias", () => {
  const rows = buildLocalMeetingsCateringResearchQueries(hotel);
  const blob = JSON.stringify(rows);
  assert.match(blob, /exclude_destination_conferences_from_lcm/i);
  assert.match(blob, /local corporate functions, chapter meetings/i);
  // When subsegments apply, queries should also de-bias annual conferences.
  const queryRows = rows.filter((r) => r.query);
  if (queryRows.length) {
    assert.ok(queryRows.some((r) => /-"annual conference"|chapter OR local/i.test(r.query)));
  }
});

check("lcm_retention_rejects_destination_conference_candidates", () => {
  const retained = collectRetainedCandidatesFromDiscovery(
    {
      cells: [
        {
          hotelId: hotel.hotelId,
          segmentId: "local_catering_day_meetings",
          candidates: [
            {
              organizationName: "Association for Financial Counseling and Planning Education",
              activityTitle: "AFCPE Annual Conference 2027",
              evidenceUrls: ["https://example.org/afcpe"],
              hasSourceEvidence: true,
            },
            {
              organizationName: "Montgomery County SHRM Chapter",
              activityTitle: "Monthly chapter luncheon",
              evidenceUrls: ["https://example.org/shrm"],
              hasSourceEvidence: true,
            },
          ],
        },
      ],
    },
    { hotelNameById: { [hotel.hotelId]: hotel.displayName } }
  );
  assert.equal(retained.length, 1);
  assert.match(retained[0].organizationName, /SHRM/i);
});

check("pre_paid_deep_skips_existing_inventory_org", () => {
  const inventoryOrgKeys = new Set([orgDeepKey({ organizationName: "AFCEA Bethesda" })]);
  const hit = evaluatePrePaidDeepDedupe(
    { id: "disc_new", organizationName: "AFCEA Bethesda" },
    { inventoryOrgKeys, paidDeepOrgKeys: new Set(), completedDeepResearchIds: new Set() }
  );
  assert.equal(hit.skipPaidDeep, true);
  assert.equal(hit.reasonCode, "EXISTING_HOTEL_INVENTORY_ORG");

  const future = evaluatePrePaidDeepDedupe(
    {
      id: "disc_future",
      organizationName: "AFCEA Bethesda",
      futureCycleSupported: true,
      hotelOpportunityThesis: "Future cycle overflow for Bethesda Marriott",
    },
    { inventoryOrgKeys, paidDeepOrgKeys: new Set(), completedDeepResearchIds: new Set() }
  );
  assert.equal(future.skipPaidDeep, false);
});

check("post_deep_preserves_distinct_annual_editions", () => {
  const a = {
    id: "a",
    organizationName: "NADO",
    title: "WashCon",
    eventStartDate: "2027-03-01",
  };
  const b = {
    id: "b",
    organizationName: "NADO",
    title: "WashCon",
    eventStartDate: "2028-03-01",
  };
  const r = reconcilePostDeepIdentity(a, [b]);
  assert.equal(r.collapseProhibited, true);
  assert.ok(r.collisions.some((c) => c.kind === "SAME_ORG_DISTINCT_EDITION"));
});

check("bethesda_inventory_loader_sees_org_keys", () => {
  const ctx = buildInventoryDedupeContextForHotels(["recLuxvwwxID7U2B8"]);
  assert.ok(ctx.totalOrgKeys >= 50, `expected Bethesda inventory orgs, got ${ctx.totalOrgKeys}`);
  // opportunities.json surface may be 59 commercial; ORS union should reach ~88+
  assert.ok(
    ctx.totalOpportunityIds >= 59,
    `expected Bethesda inventory ids, got ${ctx.totalOpportunityIds}`
  );
});

console.log(`\nOK  ${passed} PLP/PEP/LCM targeting + dedupe checks passed (offline, $0)`);
