#!/usr/bin/env node
/**
 * Permanent regression guards for objective GDI semantic-audit P1 clusters.
 * Does not replace semantic judgment reviews — only objectively testable defects.
 */
import assert from "node:assert/strict";
import {
  applySemanticCommercialRemediation,
  __test,
} from "../lib/group-demand-intelligence/commercial-consistency/gdi-semantic-commercial-remediation-v1.js";
import { sanitizeCommercialPresentation } from "../lib/group-demand-intelligence/commercial-consistency/commercial-presentation-sanitizer-v1.js";
import { resolveCustomerSalesAction } from "../lib/group-demand-intelligence/customer-sales-action-resolver-v1.js";
import { scrubInternalResearchLanguage } from "../lib/group-demand-intelligence/scaling-companies-sales-presentation-v1.js";
import { customerFacingOpportunityTitle } from "../lib/group-demand-intelligence/customer-facing-opportunity-title-contract-v1.js";
import { inferDemandSegmentIdFromResearchEvidence } from "../lib/group-demand-intelligence/legacy-demand-segment-alias-map-v1.js";
import { classifyOpportunityTypeFilter } from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

check("dedupe_evidence_urls", () => {
  const next = applySemanticCommercialRemediation({
    evidenceSources: [
      { url: "https://example.com/a", label: "A" },
      { url: "https://example.com/a", label: "A dup" },
      { url: "https://example.com/b", label: "B" },
    ],
  });
  assert.equal(next.evidenceSources.length, 2);
});

check("why_act_now_not_copy_of_potential_business", () => {
  const blob =
    "This programme can generate multi-night room-block demand for corporate attendees travelling into the destination for the annual meeting and adjacent committee sessions.";
  const next = applySemanticCommercialRemediation({
    potentialBusiness: blob,
    whyActNow: blob,
  });
  assert.notEqual(next.whyActNow, blob);
  assert.match(next.whyActNow, /watch-only|Monitor stance|confirmed lodging/i);
});

check("tournament_housing_leak_scrubbed_for_non_sports", () => {
  const next = applySemanticCommercialRemediation({
    segment: "association",
    recommendedAction: "Develop organizer relationship — tournament housing lead",
    title: "BioCultura A Coruña 2027",
  });
  assert.doesNotMatch(next.recommendedAction, /tournament housing/i);
  assert.match(next.recommendedAction, /housing/i);
});

check("tournament_housing_preserved_for_sports", () => {
  const next = applySemanticCommercialRemediation({
    segment: "sports_entertainment",
    recommendedAction: "Develop organizer relationship — tournament housing lead",
    title: "Super Copa de España Cadete",
  });
  assert.match(next.recommendedAction, /tournament housing/i);
});

check("pipeline_slug_and_role_paren_stripped_from_title", () => {
  assert.equal(
    customerFacingOpportunityTitle("NADO Conference 2027 (SPONSOR)"),
    "NADO Conference 2027"
  );
  const next = applySemanticCommercialRemediation({
    title: "YCAMP 2026 — Regional Leadership Summit",
  });
  assert.equal(next.title, "Regional Leadership Summit");
});

check("monitor_action_clears_urgency_now_copy", () => {
  const next = applySemanticCommercialRemediation({
    recommendedAction: "Monitor — host lodging already contracted",
    whyActNow: "Act now to pursue the open room block this week.",
  });
  assert.match(next.whyActNow, /Monitor stance/i);
  assert.doesNotMatch(next.whyActNow, /Act now/i);
});

check("closed_sourcing_scrubs_win_room_block", () => {
  const next = applySemanticCommercialRemediation({
    hotelSourcingStatusLabel: "Host hotel contracted — Competitor Marriott",
    recommendedAction: "Win Room Block",
  });
  assert.doesNotMatch(next.recommendedAction, /Win Room Block/i);
  assert.match(next.recommendedAction, /Monitor/i);
});

check("sales_action_resolver_closed_monitor_not_win_room_block", () => {
  const resolved = resolveCustomerSalesAction({
    hotelSourcingStatusLabel: "Competitor venue confirmed — overflow sourcing unconfirmed",
    recommendedAction: "Win Room Block",
    customerFacingState: "WATCH",
  });
  const label = String(resolved?.label || resolved?.action || resolved || "");
  assert.doesNotMatch(label, /Win Room Block/i);
});

check("internal_ids_and_demand_enums_scrubbed", () => {
  const scrubbed = scrubInternalResearchLanguage(
    "Contact gdi_hotel_bethesda_demo and recABCDEFGHIJKLMN for ASSOCIATION_CONFERENCE_GROUP demand"
  );
  assert.doesNotMatch(scrubbed, /gdi_hotel_/i);
  assert.doesNotMatch(scrubbed, /\brec[A-Z]/i);
  assert.doesNotMatch(scrubbed, /ASSOCIATION_CONFERENCE_GROUP/);
});

check("host_contracted_malformed_glue_scrubbed_in_whyHotelDemand", () => {
  const next = sanitizeCommercialPresentation({
    whyHotelDemand:
      "The event is dated in Host hotel contracted — Crystal Gateway Marriott with hotel sourcing unconfirmed for Bethesda Marriott.",
    recommendedAction: "Monitor",
  });
  assert.doesNotMatch(next.whyHotelDemand || "", /dated in Host hotel contracted/i);
  assert.match(next.whyHotelDemand || "", /competitor host/i);
});

check("room_demand_enum_not_leaked_in_overview", () => {
  const next = sanitizeCommercialPresentation({
    roomDemandLive: "LODGING_DEMAND_LIKELY",
    title: "Test Conference 2027",
  });
  assert.doesNotMatch(String(next.roomDemandLive || ""), /^LODGING_DEMAND_/);
  assert.match(String(next.roomDemandLive || ""), /Lodging demand likely/i);
});

check("classification_recovery_from_research_title_contracts", () => {
  assert.equal(
    classifyOpportunityTypeFilter({
      title: "102 Congreso SEO Oftalmología Palma 2026",
      opportunityType: "FUTURE_CYCLE",
      researchSegment: "Discovered",
    }),
    "ASSOCIATION_CONFERENCE_GROUP"
  );
  assert.equal(
    classifyOpportunityTypeFilter({
      title: "Super Copa de España Cadete",
      opportunityType: "FUTURE_CYCLE",
      researchSegment: "Discovered",
    }),
    "SPORTS_ENTERTAINMENT"
  );
  assert.equal(
    classifyOpportunityTypeFilter({
      title: "EDULEARN 2026 Palma",
      opportunityType: "FUTURE_CYCLE",
      researchSegment: "Discovered",
    }),
    "EDUCATION_SCIENTIFIC"
  );
  const unresolved = inferDemandSegmentIdFromResearchEvidence({
    title: "DIVCULT Spring Webinar",
    opportunityType: "FUTURE_WATCH",
    researchSegment: "Discovered",
  });
  assert.equal(unresolved.demandSegmentId, null);
  assert.equal(
    classifyOpportunityTypeFilter({
      title: "DIVCULT Spring Webinar",
      opportunityType: "FUTURE_WATCH",
      researchSegment: "Discovered",
    }),
    "OTHER_UNCLASSIFIED"
  );
});

check("pipeline_slug_helper_unit", () => {
  assert.equal(
    __test.scrubPipelineSlugTitle("PR Teams — Annual Board Retreat (EXHIBITOR)"),
    "Annual Board Retreat"
  );
});

if (failed) {
  console.error(`\n${failed} failing check(s)`);
  process.exit(1);
}
console.log("\nAll semantic commercial remediation regression checks passed.");
