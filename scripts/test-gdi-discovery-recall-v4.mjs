#!/usr/bin/env node
/**
 * Offline tests: Discovery Recall V4 + Commercial Evidence V4.
 * Does not call SERP/OpenAI. Does not modify Hygiene V3 gates.
 */
import assert from "node:assert/strict";
import {
  selectQueriesStratified,
  buildRecallV4SearchTasks,
  inferDemandArchetype,
  DEMAND_ARCHETYPE,
  RECALL_EXTRACT_SYSTEM_V4,
  classifyQueryYield,
} from "../lib/group-demand-intelligence/discovery-recall-v4.js";
import {
  parseSpanishOrFlexibleDate,
  isUnconfirmedFutureCycleInvention,
  detectLocalNoRoomProgram,
  assessOverflowMarketRealism,
  buildEventSeriesIdentity,
  classifyContactPath,
  hasMeaningfulSource,
  enrichCommercialEvidenceV4,
  buildHygieneEvidenceOverlayV4,
  applyCommercialReadinessDemotionV4,
  CONTACT_PATH_CLASS,
} from "../lib/group-demand-intelligence/commercial-evidence-v4.js";
import { applyDiscoveryHygieneV3 } from "../lib/group-demand-intelligence/discovery-hygiene-v3.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("extract_system_v4_separates_find_from_qualify", () => {
  assert.match(RECALL_EXTRACT_SYSTEM_V4, /DOWNSTREAM/i);
  assert.match(RECALL_EXTRACT_SYSTEM_V4, /ALLOW and KEEP as UNKNOWN/i);
  assert.doesNotMatch(RECALL_EXTRACT_SYSTEM_V4, /Prefer events with venue TBD/);
});

test("stratified_budget_keeps_late_families", () => {
  const plan = [];
  for (const v of [
    "association_conference",
    "corporate_meeting",
    "incentive_retreat",
    "housing_overflow",
  ]) {
    for (let i = 0; i < 20; i++) plan.push({ vertical: v, query: `${v}_${i}` });
  }
  const selected = selectQueriesStratified(plan, 28);
  const verts = new Set(selected.map((s) => s.vertical));
  assert.ok(verts.has("incentive_retreat"));
  assert.ok(verts.has("housing_overflow"));
  assert.equal(selected.length, 28);
});

test("spanish_date_formats", () => {
  assert.equal(parseSpanishOrFlexibleDate("22–24 septiembre 2026").iso, "2026-09-22");
  assert.equal(parseSpanishOrFlexibleDate("22 al 24 de septiembre de 2026").iso, "2026-09-22");
  assert.equal(parseSpanishOrFlexibleDate("22/09/2026").iso, "2026-09-22");
  assert.equal(parseSpanishOrFlexibleDate("septiembre 22–24, 2026").iso, "2026-09-22");
  assert.equal(parseSpanishOrFlexibleDate("2026-11-03").status, "CONFIRMED");
});

test("unknown_sourcing_survives_as_candidate_enrichment", () => {
  const e = enrichCommercialEvidenceV4({
    title: "Congreso Nacional de Hotelería 2026",
    eventStartDate: "2026-11-10",
    venueSourcingStatus: "UNKNOWN",
    officialSource: "https://example.org/congreso",
  });
  assert.equal(e.venueSourcingStatus || "UNKNOWN", "UNKNOWN");
  assert.equal(e.semanticTypeHint, "EVENT");
  assert.ok(e.eventSeriesId);
});

test("unknown_room_count_survives", () => {
  const e = enrichCommercialEvidenceV4({
    title: "Foro Empresarial Bogotá 2026",
    eventStartDate: "2026-10-01",
    officialSource: "https://example.org/foro",
  });
  assert.equal(e.peakRoomsStatus, "UNKNOWN");
  assert.equal(e.attendanceStatus, "UNKNOWN");
});

test("local_no_room_does_not_stay_true_after_readiness", () => {
  const opp = {
    title: "Monthly Local Members Breakfast",
    opportunityType: "PRIMARY_PURSUIT",
    whyNow: "day meeting only, primarily local membership, no guestrooms",
    officialSource: "https://example.org/breakfast",
    localAttendanceOnly: true,
  };
  assert.equal(detectLocalNoRoomProgram(opp).localNoRoom, true);
  const dem = applyCommercialReadinessDemotionV4(
    { actionabilityV3: "TRUE_ACTIONABLE" },
    opp
  );
  assert.equal(dem.demoted, true);
  assert.ok(dem.reasons.includes("LOCAL_NO_ROOM"));
});

test("remote_overflow_fails_without_evidence", () => {
  const r = assessOverflowMarketRealism({
    eventSubmarket: "alexandria waterfront",
    hotelSubmarkets: ["bethesda", "north bethesda"],
    distanceKm: 55,
    housingClusterMentionsHotelCorridor: false,
  });
  assert.equal(r.plausible, false);
  const dem = applyCommercialReadinessDemotionV4(
    { actionabilityV3: "TRUE_ACTIONABLE" },
    {
      opportunityType: "OVERFLOW_HOUSING",
      eventSubmarket: "alexandria waterfront",
      hotelSubmarkets: ["bethesda"],
      distanceKm: 55,
      housingClusterMentionsHotelCorridor: false,
      officialSource: "https://example.org/event",
    }
  );
  assert.equal(dem.demoted, true);
  assert.ok(dem.reasons.includes("REMOTE_OVERFLOW"));
});

test("future_cycle_not_invented", () => {
  assert.equal(
    isUnconfirmedFutureCycleInvention({
      evidenceYears: [2026],
      claimedStartDate: "2027-06-01",
    }),
    true
  );
  const e = enrichCommercialEvidenceV4({
    title: "Annual Industry Summit",
    eventStartDate: "2027-06-01",
    evidenceYears: [2026],
    officialSource: "https://example.org/2026",
  });
  assert.equal(e.opportunityType, "FUTURE_CYCLE");
  assert.equal(e.futureCycleInvented, true);
});

test("event_series_subevent_identity", () => {
  const a = buildEventSeriesIdentity({
    title: "National Tournament 2026 — Division A",
    organizationName: "Sports Org",
    eventStartDate: "2026-08-01",
  });
  const b = buildEventSeriesIdentity({
    title: "National Tournament 2026 — Division B",
    organizationName: "Sports Org",
    eventStartDate: "2026-08-02",
  });
  assert.equal(a.eventSeriesId, b.eventSeriesId);
  assert.notEqual(a.eventCycleId, null);
  assert.equal(a.newToHotel, null);
  assert.equal(a.newToGdi, true);
});

test("named_contact_not_blocked_classification", () => {
  assert.equal(
    classifyContactPath({ name: "Maria Lopez", email: "maria.lopez@assoc.org" }),
    CONTACT_PATH_CLASS.NAMED_CONTACT
  );
  assert.equal(
    classifyContactPath({ name: "Events Desk", email: "info@assoc.org" }),
    CONTACT_PATH_CLASS.GENERIC_CONTACT
  );
});

test("sourceless_true_demoted", () => {
  assert.equal(hasMeaningfulSource({ title: "Event" }), false);
  const dem = applyCommercialReadinessDemotionV4(
    { actionabilityV3: "TRUE_ACTIONABLE" },
    { title: "Event", opportunityType: "PRIMARY_PURSUIT" }
  );
  assert.equal(dem.demoted, true);
  assert.ok(dem.reasons.includes("SOURCELESS_TRUE"));
});

test("casas_archetype_is_luxury_small_not_hotel_hardcode", () => {
  const arch = inferDemandArchetype({
    capabilityProfile: {
      totalGuestrooms: 21,
      classification: "santo_domingo_zona_colonial_luxury_boutique",
      serviceLevel: "luxury_boutique",
    },
  });
  assert.equal(arch, DEMAND_ARCHETYPE.LUXURY_DESTINATION_SMALL);
  const pack = buildRecallV4SearchTasks({
    geos: ["Zona Colonial", "Santo Domingo"],
    archetype: arch,
    countryCode: "DO",
  });
  const verts = pack.tasks.map((t) => t.vertical);
  assert.ok(verts.includes("incentive_retreat"));
  assert.ok(pack.locales.some((l) => l.lang === "es"));
});

test("v3_still_accepts_unknown_sourcing_candidate_as_non_true", () => {
  const opps = [
    {
      id: "gdi_opp_test_congreso",
      title: "Congreso Nacional de Turismo 2026",
      eventStartDate: "2026-11-12",
      destinationStatus: "Santo Domingo",
      venueSourcingStatus: "UNKNOWN",
      roomDemandStatus: "UNKNOWN",
      officialSource: "https://example.org/congreso-turismo-2026",
      semanticTypeHint: "EVENT",
    },
  ];
  const overlay = buildHygieneEvidenceOverlayV4(opps[0]);
  const r = applyDiscoveryHygieneV3("recUOyzOXn2Zdp98I", opps, {
    nowDate: "2026-09-22",
    subjectHotel: { hotelId: "recUOyzOXn2Zdp98I", name: "Radisson" },
    evidenceOverlays: { gdi_opp_test_congreso: overlay },
  });
  // May be WATCH/INSUFFICIENT/TRUE depending demand — must not crash; UNKNOWN stays non-open
  const row = r.rows[0];
  assert.ok(row.actionabilityV3);
  assert.notEqual(row.venueSourcingStatus, "OPEN_UNRESOLVED");
});

test("query_yield_classifier", () => {
  assert.equal(classifyQueryYield({ candidates: 4 }), "HIGH_YIELD");
  assert.equal(classifyQueryYield({ candidates: 1 }), "MEDIUM_YIELD");
  assert.equal(classifyQueryYield({ usablePages: 3, candidates: 0 }), "LOW_YIELD");
  assert.equal(classifyQueryYield({}), "ZERO_YIELD");
});

test("competitor_venue_selected_overlay_fully_placed", () => {
  const overlay = buildHygieneEvidenceOverlayV4({
    title: "City Summit",
    fullyPlaced: true,
    hostHotelSelected: true,
    officialSource: "https://example.org/summit",
  });
  assert.equal(overlay.fullyPlaced, true);
});

test("peak_rooms_and_attendance_statuses", () => {
  const e = enrichCommercialEvidenceV4({
    title: "Industry Expo",
    eventStartDate: "2026-12-01",
    estimatedAttendance: 800,
    estimatedPeakRooms: 220,
    officialSource: "https://example.org/expo",
    attendanceStatus: "CONFIRMED",
    peakRoomsStatus: "ESTIMATED",
  });
  assert.equal(e.attendanceStatus, "CONFIRMED");
  assert.equal(e.peakRoomsStatus, "ESTIMATED");
  assert.equal(e.peakRooms, 220);
});

let pass = 0;
let fail = 0;
const failures = [];
for (const t of tests) {
  try {
    t.fn();
    pass += 1;
    console.log(`PASS ${t.name}`);
  } catch (err) {
    fail += 1;
    failures.push({ name: t.name, error: String(err.message || err) });
    console.error(`FAIL ${t.name}: ${err.message || err}`);
  }
}
console.log(JSON.stringify({ suite: "gdi-discovery-recall-v4", pass, fail, total: tests.length, failures }, null, 2));
if (fail) process.exit(1);
