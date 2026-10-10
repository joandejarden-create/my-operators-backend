#!/usr/bin/env node
/**
 * Regression: isValidFutureWatch — hotel-agnostic Future Watch law.
 */

import assert from "node:assert/strict";
import {
  isValidFutureWatch,
  countValidFutureWatches,
  WATCH_VALIDATION_CLASS,
  WATCH_TRIGGER_TYPE,
} from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";

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

check("valid_watch_with_entity_cycle_fit_trigger_evidence", () => {
  const v = isValidFutureWatch({
    id: "gdi_opp_synth_alpha_2027",
    title: "Regional Marine Congress 2027",
    organizationName: "Caribbean Marine Association",
    hotelOpportunityThesis:
      "Overflow housing for congress delegates when the primary island host hotel fills; property fit is leisure-luxury peak rooms.",
    whyMonitor: "Next cycle housing page not yet published; monitor for room block.",
    futureCycleEvidenceState: "FUTURE_CYCLE_UNCONFIRMED",
    eventStartDate: "2027-06-15",
    officialSource: "https://example.org/marine-congress-2027",
    roomDemandStatus: "ESTIMATED_ROOM_DEMAND",
    hotelFitScore: 62,
    priority: "WATCHLIST",
  });
  assert.equal(v.ok, true);
  assert.equal(v.class, WATCH_VALIDATION_CLASS.VALID_FUTURE_WATCH);
  assert.equal(v.trigger.type, WATCH_TRIGGER_TYPE.DATE_WINDOW);
  assert.ok(v.latestEvidenceDate || v.trigger.researchDate);
});

check("generic_leadership_retreat_is_not_watch", () => {
  const v = isValidFutureWatch({
    id: "gdi_opp_leadership_retreat_x",
    title: "Leadership Retreat",
    organizationName: "Various Corporate Entities",
    hotelOpportunityThesis: "May require accommodation.",
    whyMonitor: "Recurring pattern",
    futureCycleEvidenceState: "FUTURE_CYCLE_UNCONFIRMED",
    officialSource: "https://example.org/generic",
    priority: "WATCHLIST",
  });
  assert.equal(v.ok, false);
  assert.equal(v.class, WATCH_VALIDATION_CLASS.ENTITY_INVALID);
});

check("insufficient_evidence_alone_is_not_watch", () => {
  const v = isValidFutureWatch({
    id: "gdi_opp_no_source",
    title: "Named Industry Summit 2027",
    organizationName: "Named Industry Council",
    hotelOpportunityThesis:
      "Named summit historically needs 80–120 peak rooms near the convention district.",
    whyMonitor: "Await next housing release",
    futureCycleEvidenceState: "FUTURE_CYCLE_UNCONFIRMED",
    eventStartDate: "2027-09-01",
    priority: "WATCHLIST",
  });
  assert.equal(v.ok, false);
  assert.equal(v.class, WATCH_VALIDATION_CLASS.INSUFFICIENT_EVIDENCE_NOT_WATCH);
});

check("duplicate_series_second_row_rejected", () => {
  const seen = new Set();
  const a = isValidFutureWatch(
    {
      id: "a1",
      title: "NABHOOD Annual Conference 2026",
      organizationName: "NABHOOD",
      eventSeriesKey: "series:nabhood_annual_conference",
      eventStartDate: "2026-11-01",
      hotelOpportunityThesis:
        "Association conference with prior island host pattern; monitor housing.",
      whyMonitor: "Housing not open",
      futureCycleEvidenceState: "CURRENT_FUTURE_CYCLE_CONFIRMED",
      officialSource: "https://example.org/nabhood",
      hotelFitScore: 70,
      priority: "WATCHLIST",
    },
    { seenKeys: seen }
  );
  const b = isValidFutureWatch(
    {
      id: "a2",
      title: "NABHOOD Annual Conference (dup)",
      organizationName: "NABHOOD",
      eventSeriesKey: "series:nabhood_annual_conference",
      eventStartDate: "2026-11-01",
      hotelOpportunityThesis:
        "Association conference with prior island host pattern; monitor housing.",
      whyMonitor: "Housing not open",
      futureCycleEvidenceState: "CURRENT_FUTURE_CYCLE_CONFIRMED",
      officialSource: "https://example.org/nabhood",
      hotelFitScore: 70,
      priority: "WATCHLIST",
    },
    { seenKeys: seen }
  );
  assert.equal(a.ok, true);
  assert.equal(b.ok, false);
  assert.equal(b.class, WATCH_VALIDATION_CLASS.DUPLICATE);
});

check("stale_past_event_not_watch", () => {
  const v = isValidFutureWatch({
    id: "past1",
    title: "ADAPTATUR 2025",
    organizationName: "Universidad Complutense de Madrid",
    eventStartDate: "2025-06-12",
    eventEndDate: "2025-06-14",
    hotelOpportunityThesis: "Attendees may need rooms near campus.",
    whyMonitor: "Monitor",
    futureCycleEvidenceState: "FUTURE_CYCLE_UNCONFIRMED",
    officialSource: "https://example.org/adaptatur",
    hotelFitScore: 55,
    priority: "WATCHLIST",
  });
  assert.equal(v.ok, false);
  assert.equal(v.class, WATCH_VALIDATION_CLASS.STALE);
});

check("placed_package_lodging_not_watch", () => {
  const v = isValidFutureWatch({
    id: "placed1",
    title: "Changemakers Retreat",
    organizationName: "Initiatives of Change",
    eventStartDate: "2026-11-05",
    hotelOpportunityThesis: "Retreat near Lake Geneva for changemakers.",
    whyMonitor: "Monitor overflow",
    futureCycleEvidenceState: "CURRENT_FUTURE_CYCLE_CONFIRMED",
    officialSource: "https://www.iofc.ch/changemakers",
    hotelOpportunityThesisExtra: "x",
    venueSourcingStatus: "FULLY_PLACED",
    whyNow: "Package includes overnight stays at the Caux Palace",
    hotelFitScore: 50,
    priority: "WATCHLIST",
  });
  assert.equal(v.ok, false);
  assert.equal(v.class, WATCH_VALIDATION_CLASS.PLACED_NO_OVERFLOW);
});

check("count_reconciles_without_double_count", () => {
  const ops = [
    {
      id: "1",
      title: "Named Summit 2027",
      organizationName: "Named Org",
      eventStartDate: "2027-03-01",
      hotelOpportunityThesis: "Named summit needs overflow rooms near airport corridor hotels.",
      whyMonitor: "Housing platform not listing property yet",
      futureCycleEvidenceState: "FUTURE_CYCLE_UNCONFIRMED",
      officialSource: "https://example.org/s",
      hotelFitScore: 60,
      priority: "WATCHLIST",
    },
    {
      id: "2",
      title: "Leadership Retreat",
      organizationName: "Various",
      whyMonitor: "x",
      futureCycleEvidenceState: "FUTURE_CYCLE_UNCONFIRMED",
      officialSource: "https://example.org/y",
      priority: "WATCHLIST",
    },
  ];
  const c = countValidFutureWatches(ops);
  assert.equal(c.total, 2);
  assert.equal(c.valid, 1);
  assert.equal(
    Object.values(c.byClass).reduce((a, b) => a + b, 0),
    2
  );
});

check("template_monitor_rationale_is_backlog_not_watch", () => {
  const v = isValidFutureWatch({
    id: "tmpl1",
    title: "Named Island Congress 2027",
    organizationName: "Island Congress Authority",
    eventStartDate: "2027-05-01",
    hotelOpportunityThesis:
      "Congress historically needs 100+ peak rooms on Grand Anse beach corridor.",
    whyMonitor: "Recurring / future-cycle pattern — monitor for next published cycle; not open current sourcing",
    futureCycleEvidenceState: "FUTURE_CYCLE_UNCONFIRMED",
    officialSource: "https://example.org/congress",
    hotelFitScore: 70,
    priority: "WATCHLIST",
  });
  assert.equal(v.ok, false);
  assert.equal(v.class, WATCH_VALIDATION_CLASS.RESEARCH_BACKLOG_NOT_WATCH);
});

if (failed) {
  console.error(`\n${failed} failing`);
  process.exit(1);
}
console.log("\nAll isValidFutureWatch tests passed.");
