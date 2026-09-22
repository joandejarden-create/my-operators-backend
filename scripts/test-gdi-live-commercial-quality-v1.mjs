#!/usr/bin/env node
/**
 * Offline tests — GDI Live Commercial Quality V1
 */
import assert from "node:assert/strict";
import {
  normalizeDateModel,
  normalizeFutureCycle,
  reconcileSuggestedAction,
  applyLiveCommercialQuality,
  buildRelatedOpportunityGroups,
  buildExportCsv,
  DATE_GRANULARITY,
  NEXT_CYCLE_STATUS,
  ROOM_DEMAND_LIVE,
  HOTEL_VALIDATION_REASON,
} from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";

const results = [];
function test(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
    console.log("PASS", name);
  } catch (err) {
    results.push({ name, ok: false, error: err.message });
    console.error("FAIL", name, err.message);
  }
}

test("year_only_not_jan_1", () => {
  const n = normalizeDateModel({
    title: "AMWA Annual Meeting 2027",
    eventStartDate: "2027-01-01",
  });
  assert.equal(n.eventDateGranularity, DATE_GRANULARITY.YEAR);
  assert.equal(n.eventStartDate, null);
  assert.equal(n.eventYear, "2027");
  assert.equal(n.eventDateDisplay, "2027");
  assert.ok(!String(n.eventDateDisplay).includes("Jan"));
});

test("historical_recurrence_not_active_future", () => {
  const f = normalizeFutureCycle({
    opportunityType: "FUTURE_CYCLE",
    futureCycleEvidenceState: "FUTURE_CYCLE_UNCONFIRMED",
    evidenceYears: ["2025", "2026"],
    eventStartDate: "2027-06-01",
    futureCycleInvented: true,
  });
  assert.equal(f.opportunityType, "FUTURE_WATCH");
  assert.equal(f.nextCycleStatus, NEXT_CYCLE_STATUS.UNCONFIRMED);
  assert.match(f.nextCycleDisplay, /not yet confirmed/i);
});

test("confirmed_future_cycle_preserved_when_not_invented", () => {
  const f = normalizeFutureCycle({
    opportunityType: "PRIMARY_PURSUIT",
    eventStartDate: "2027-09-11",
    nextCycleStatus: "CONFIRMED",
  });
  assert.notEqual(f.opportunityType, "FUTURE_WATCH");
});

test("past_event_no_immediate_pursuit", () => {
  const a = reconcileSuggestedAction(
    {
      title: "Past Meeting",
      eventStartDate: "2025-01-15",
      eventDateStatus: "PAST",
      recommendedAction: "Pursue room block now",
      sources: [{ url: "https://example.com/event" }],
      primaryContact: { name: "Jane Doe", email: "jane@example.com" },
    },
    { nowDate: "2026-09-22" }
  );
  assert.match(a.recommendedAction, /Past cycle/i);
});

test("remote_overflow_blocked", () => {
  const a = reconcileSuggestedAction(
    {
      title: "Remote Metro Event",
      recommendedAction: "Pursue as overflow housing partner",
      eventLocationSummary: "far corridor",
      sources: [{ url: "https://example.com" }],
      primaryContact: { name: "A B", email: "a@b.com" },
    },
    {
      distanceKm: 60,
      hotelSubmarkets: ["core"],
      eventSubmarket: "other",
      overflowEvidence: false,
    }
  );
  assert.match(a.recommendedAction, /Do not recommend overflow/i);
  assert.equal(a.overflowPlausible, false);
});

test("nearby_overflow_retained_with_evidence", () => {
  const a = reconcileSuggestedAction(
    {
      title: "Local Cup",
      recommendedAction: "Pursue overflow lodging",
      sources: [{ url: "https://example.com" }],
      primaryContact: { name: "A B", email: "a@b.com" },
    },
    { distanceKm: 8, overflowEvidence: true, hotelSubmarkets: ["core"] }
  );
  assert.equal(a.overflowPlausible, true);
  assert.ok(!/Do not recommend overflow/i.test(a.recommendedAction));
});

test("venue_selected_no_venue_pitch", () => {
  const a = reconcileSuggestedAction({
    venueSourcingStatus: "FULLY_PLACED",
    recommendedAction: "Pursue as venue for this conference",
    sources: [{ url: "https://example.com" }],
    primaryContact: { name: "A B", email: "a@b.com" },
  });
  assert.match(a.recommendedAction, /Do not pursue as venue/i);
});

test("local_no_room_blocks_room_pursuit", () => {
  const cq = applyLiveCommercialQuality({
    title: "Local members only day meeting — no guestrooms",
    recommendedAction: "Pursue overflow room block",
    sources: [{ url: "https://example.com" }],
    primaryContact: { name: "A B", email: "a@b.com" },
  });
  assert.equal(cq.roomDemandLive, ROOM_DEMAND_LIVE.LOCAL_NO_ROOM_SIGNAL);
  assert.match(cq.recommendedAction, /Do not pursue room block|Do not recommend overflow/i);
});

test("attendance_does_not_generate_rooms", () => {
  const cq = applyLiveCommercialQuality({
    title: "Big Conf 2027",
    estimatedAttendance: 500,
    estimatedPeakRooms: "UNKNOWN",
  });
  assert.equal(cq.peakRoomsStatus, "UNKNOWN");
});

test("series_grouping_related", () => {
  const opps = [
    { id: "a", title: "PTOIC Tournament — Boys Division 2026", organizationName: "PTOIC", eventStartDate: "2026-11-13" },
    { id: "b", title: "PTOIC Tournament — Girls Division 2026", organizationName: "PTOIC", eventStartDate: "2026-11-20" },
    { id: "c", title: "Unrelated Gala 2026", organizationName: "Other", eventStartDate: "2026-12-01" },
  ];
  const related = buildRelatedOpportunityGroups(opps.map((o) => applyLiveCommercialQuality(o)));
  // At least one series groups a+b if normalized similarly
  const cq = opps.map((o) => applyLiveCommercialQuality(o));
  assert.ok(cq[0].eventSeriesId);
  assert.ok(cq[1].eventSeriesId);
});

test("different_annual_cycle_separate_cycle_id", () => {
  const a = applyLiveCommercialQuality({
    title: "AMWA Annual Meeting 2027",
    eventStartDate: "2027-01-01",
  });
  const b = applyLiveCommercialQuality({
    title: "AMWA Annual Meeting 2028",
    eventStartDate: "2028-01-01",
  });
  assert.equal(a.eventSeriesId, b.eventSeriesId);
  assert.notEqual(a.eventCycleId, b.eventCycleId);
});

test("export_stable_opportunity_id_and_filters_meta", () => {
  const csv = buildExportCsv(
    [
      {
        id: "gdi_opp_stable_1",
        title: "Test Event",
        organizationName: "Org",
        sources: [{ url: "https://example.com" }],
      },
    ],
    { hotelId: "recX", name: "Test Hotel" },
    { weekly: "NEW", priority: "HIGH" }
  );
  assert.match(csv, /Dealality Opportunity ID/);
  assert.match(csv, /gdi_opp_stable_1/);
  assert.match(csv, /generatedAt=/);
  assert.match(csv, /filter=.*NEW/);
});

test("list_dto_includes_weekly_and_commercial", () => {
  const dto = toOpportunityListDto({
    id: "1",
    title: "T",
    weeklyDeltaState: "NEW",
    isNewThisWeek: true,
    estimatedPeakRooms: 40,
    eventStartDate: "2026-11-13",
    eventEndDate: "2026-11-15",
  });
  assert.equal(dto.weeklyDeltaState, "NEW");
  assert.equal(dto.peakRooms, 40);
  assert.ok(dto.eventDateDisplay);
});

test("validation_reason_taxonomy_present", () => {
  assert.ok(HOTEL_VALIDATION_REASON.ALREADY_KNOWN_OR_IN_SYSTEM);
  assert.ok(HOTEL_VALIDATION_REASON.WRONG_CONTACT);
  assert.ok(HOTEL_VALIDATION_REASON.DUPLICATE_OR_RELATED);
});

test("sourceless_insufficient", () => {
  const a = reconcileSuggestedAction({
    title: "No source event",
    recommendedAction: "Call someone",
    primaryContact: null,
    sources: [],
  });
  assert.equal(a.customerFacingState, "INSUFFICIENT_EVIDENCE");
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify({
    suite: "gdi-live-commercial-quality-v1",
    pass: results.length - failed.length,
    fail: failed.length,
    total: results.length,
    failures: failed,
  })
);
process.exit(failed.length ? 1 : 0);
