#!/usr/bin/env node
/**
 * Permanent gates for GDI commercial demand type (Guest Rooms / Local Catering / Both).
 */
import assert from "node:assert/strict";
import {
  COMMERCIAL_DEMAND_TYPE,
  demandTypeToRevenueStreamClass,
  revenueStreamClassToDemandType,
  matchesCommercialDemandTypeFilter,
  evaluateCommercialDemandTypeQuality,
  HOTEL_REVENUE_STREAM_CLASS,
} from "../lib/group-demand-intelligence/commercial-demand-type-v1.js";
import {
  classifyOpportunityCommercialDemandType,
} from "../lib/group-demand-intelligence/classify-opportunity-commercial-demand-type-v1.js";
import {
  resolveOpportunityRevenueStreamIcons,
  runOpportunityRevenueStreamIconsGate,
  REVENUE_STREAM_ICON_LABEL,
} from "../lib/group-demand-intelligence/opportunity-revenue-stream-icons-v1.js";
import { buildDemandTypeClassificationManifest } from "../lib/group-demand-intelligence/research/bethesda-88-commercial-demand-type-remediation-v1.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}:`, err.message || err);
  }
}

check("founder_labels_map_1to1", () => {
  assert.equal(
    demandTypeToRevenueStreamClass(COMMERCIAL_DEMAND_TYPE.GUEST_ROOMS),
    HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY,
  );
  assert.equal(
    demandTypeToRevenueStreamClass(COMMERCIAL_DEMAND_TYPE.LOCAL_CATERING),
    HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY,
  );
  assert.equal(
    demandTypeToRevenueStreamClass(COMMERCIAL_DEMAND_TYPE.BOTH),
    HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB,
  );
  assert.equal(
    revenueStreamClassToDemandType(HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY),
    COMMERCIAL_DEMAND_TYPE.GUEST_ROOMS,
  );
});

check("icon_a11y_labels", () => {
  assert.equal(REVENUE_STREAM_ICON_LABEL.guestrooms, "Guest Rooms");
  assert.equal(REVENUE_STREAM_ICON_LABEL.eventFnb, "Local Catering");
});

check("filter_inclusion_rules", () => {
  const rooms = { revenueStreamClass: "GUESTROOMS_ONLY" };
  const catering = { revenueStreamClass: "EVENT_FNB_ONLY" };
  const both = { revenueStreamClass: "COMBINED_GUESTROOMS_EVENT_FNB" };
  assert.equal(matchesCommercialDemandTypeFilter(rooms, "GUEST_ROOMS"), true);
  assert.equal(matchesCommercialDemandTypeFilter(both, "GUEST_ROOMS"), true);
  assert.equal(matchesCommercialDemandTypeFilter(catering, "GUEST_ROOMS"), false);
  assert.equal(matchesCommercialDemandTypeFilter(catering, "LOCAL_CATERING"), true);
  assert.equal(matchesCommercialDemandTypeFilter(both, "LOCAL_CATERING"), true);
  assert.equal(matchesCommercialDemandTypeFilter(rooms, "LOCAL_CATERING"), false);
  assert.equal(matchesCommercialDemandTypeFilter(both, "BOTH"), true);
  assert.equal(matchesCommercialDemandTypeFilter(rooms, "BOTH"), false);
});

check("overflow_is_rooms_not_both", () => {
  const r = classifyOpportunityCommercialDemandType({
    id: "test_overflow",
    priority: "MEDIUM_PRIORITY",
    category: "OVERFLOW_HOUSING",
    title: "Conference overflow near competitor host",
    hotelOpportunityThesis:
      "Host hotel is already Crystal Gateway Marriott; Bethesda competes for overflow room nights only.",
    hotelFitHonesty: "OVERFLOW_WATCH_COMPETITOR_HOST",
  });
  assert.equal(r.commercialDemandType, COMMERCIAL_DEMAND_TYPE.GUEST_ROOMS);
  assert.ok(r.guestroomThesis);
  assert.equal(r.eventFnbThesis, null);
});

check("disqualified_is_not_applicable", () => {
  const r = classifyOpportunityCommercialDemandType({
    id: "test_dq",
    priority: "DISQUALIFIED",
    category: "FUTURE_CYCLE",
    title: "Out of market conference",
    hotelFitHonesty: "DESTINATION_OUTSIDE_CHICAGO",
    hotelOpportunityThesis: "Outside Bethesda demand territory — do not pursue.",
  });
  assert.equal(r.commercialDemandType, COMMERCIAL_DEMAND_TYPE.NOT_APPLICABLE);
  assert.equal(
    r.revenueStreamClass,
    HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED,
  );
});

check("local_meetings_are_catering", () => {
  const r = classifyOpportunityCommercialDemandType({
    id: "test_local",
    priority: "WATCHLIST",
    category: "CORPORATE_ACCOUNT_RELATIONSHIP",
    title: "Chapter dinner meetings",
    hotelOpportunityThesis:
      "Monthly chapter dinner meetings and holiday programs create local catering demand.",
    whatIsHappening: "Local chapter dinner meetings and receptions.",
  });
  assert.equal(r.commercialDemandType, COMMERCIAL_DEMAND_TYPE.LOCAL_CATERING);
  assert.ok(r.eventFnbThesis);
});

check("planner_partnership_is_both", () => {
  const r = classifyOpportunityCommercialDemandType({
    id: "test_pep",
    priority: "WATCHLIST",
    category: "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP",
    title: "Event planner partnership",
    hotelOpportunityThesis: "Planner can place programs at the hotel.",
  });
  assert.equal(r.commercialDemandType, COMMERCIAL_DEMAND_TYPE.BOTH);
  assert.ok(r.guestroomThesis && r.eventFnbThesis);
});

check("both_rejected_without_both_theses", () => {
  const q = evaluateCommercialDemandTypeQuality({
    priority: "HIGH_PRIORITY",
    revenueStreamClass: "COMBINED_GUESTROOMS_EVENT_FNB",
    guestroomThesis: "Room block for attendees",
    eventFnbThesis: null,
  });
  assert.equal(q.ok, false);
  assert.ok(q.blockers.includes("BOTH_MISSING_CATERING_EVIDENCE"));
});

check("icons_gate_still_passes", () => {
  const gate = runOpportunityRevenueStreamIconsGate();
  assert.equal(gate.pass, true, JSON.stringify(gate.failures));
});

check("persisted_class_drives_icons", () => {
  const got = resolveOpportunityRevenueStreamIcons({
    revenueStreamClass: "GUESTROOMS_ONLY",
    guestroomThesis: "Overnight lodging for nonlocal attendees",
  });
  assert.equal(got.revenueStreamClass, "GUESTROOMS_ONLY");
  assert.equal(got.icons.guestrooms, "active");
  assert.equal(got.icons.eventFnb, "muted");
  assert.match(got.aria.guestrooms, /Guest Rooms/);
});

check("bethesda_88_manifest_covers_all", () => {
  const manifest = buildDemandTypeClassificationManifest();
  assert.equal(manifest.unionCount, 88);
  const sum = Object.values(manifest.counts).reduce((a, b) => a + b, 0);
  assert.equal(sum, 88);
  assert.ok(manifest.counts.GUEST_ROOMS > 0);
  assert.ok(
    (manifest.counts.NOT_APPLICABLE || 0) + (manifest.counts.UNDETERMINED || 0) > 0,
  );
  // Actionable rows must not leave BOTH without both theses
  for (const row of manifest.rows) {
    if (row.commercialDemandType === "BOTH") {
      assert.ok(row.guestroomThesis, row.id);
      assert.ok(row.eventFnbThesis, row.id);
    }
    if (
      row.priority === "HIGH_PRIORITY" ||
      row.priority === "MEDIUM_PRIORITY"
    ) {
      assert.notEqual(
        row.commercialDemandType,
        "UNDETERMINED",
        `actionable ${row.id} undetermined`,
      );
    }
  }
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nAll commercial demand-type gates passed.");
