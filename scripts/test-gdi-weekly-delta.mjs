#!/usr/bin/env node
/**
 * GDI weekly delta + UI surface tests.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  WEEKLY_DELTA_STATE,
  snapshotBaselineOpportunity,
  scoreCanonicalMatch,
  classifyWeeklyDelta,
  computeWeeklyDelta,
  applyWeeklyDeltaToOpportunities,
  deriveIsNewThisWeek,
} from "../lib/group-demand-intelligence/weekly-delta.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const RUN = "gdi_weekly_test_20260921";

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

const prior = {
  id: "gdi_opp_demo_annual_2026",
  title: "Demo Association Annual Meeting 2026",
  organizationName: "Demo Association (DA)",
  eventStartDate: "2026-06-10",
  eventEndDate: "2026-06-12",
  destinationStatus: "Bethesda, MD",
  venueSourcingStatus: "UNKNOWN",
  opportunityType: "PRIMARY_PURSUIT",
  priority: "WATCHLIST",
  roomDemandStatus: "UNKNOWN",
  firstSeenAt: "2026-09-01T00:00:00.000Z",
  firstSeenRunId: "gdi_weekly_old",
  primaryContact: { name: "Alex Planner", email: null, phone: null },
  sources: [{ url: "https://demo.org/annual-2026" }],
  customerValidation: { status: "VALIDATED" },
  customerAction: { status: "CONTACTED" },
  customerOutcome: { status: "OPEN" },
};

const baseline = [snapshotBaselineOpportunity(prior)];

check("same_event_rediscovered_unchanged", () => {
  const cur = { ...prior, sources: [{ url: "https://demo.org/annual-2026" }] };
  const c = classifyWeeklyDelta(baseline, cur, { runId: RUN, currentIsTrueActionable: true });
  assert.equal(c.weeklyDeltaState, WEEKLY_DELTA_STATE.UNCHANGED);
  assert.equal(c.firstSeenAt, prior.firstSeenAt);
});

check("same_event_new_source_not_new", () => {
  const cur = {
    ...prior,
    id: "cand_alt_source",
    sources: [{ url: "https://demo.org/meetings/2026-annual.pdf" }],
  };
  const c = classifyWeeklyDelta(baseline, cur, { runId: RUN, currentIsTrueActionable: true });
  assert.notEqual(c.weeklyDeltaState, WEEKLY_DELTA_STATE.NEW);
  assert.equal(c.match.matched, true);
});

check("same_event_new_who_updated", () => {
  const cur = {
    ...prior,
    primaryContact: { name: "Jordan Chair", email: null, phone: null },
  };
  const c = classifyWeeklyDelta(baseline, cur, { runId: RUN, currentIsTrueActionable: true });
  assert.equal(c.weeklyDeltaState, WEEKLY_DELTA_STATE.UPDATED);
  assert.ok(c.changedFields.some((f) => f.field === "primaryContactName"));
});

check("same_event_new_email_updated", () => {
  const cur = {
    ...prior,
    primaryContact: { name: "Alex Planner", email: "alex@demo.org", phone: null },
  };
  const c = classifyWeeklyDelta(baseline, cur, { runId: RUN, currentIsTrueActionable: true });
  assert.equal(c.weeklyDeltaState, WEEKLY_DELTA_STATE.UPDATED);
  assert.ok(c.changedFields.some((f) => f.field === "primaryContactEmail"));
});

check("same_event_changed_venue_updated", () => {
  const cur = { ...prior, venue: "Bethesda Marriott", venueStatus: "ANNOUNCED" };
  const c = classifyWeeklyDelta(baseline, cur, { runId: RUN, currentIsTrueActionable: true });
  assert.equal(c.weeklyDeltaState, WEEKLY_DELTA_STATE.UPDATED);
});

check("sourcing_becomes_open_reactivated", () => {
  const cur = {
    ...prior,
    venueSourcingStatus: "HOTEL_VENUE_TBD",
    priority: "MEDIUM_PRIORITY",
  };
  const c = classifyWeeklyDelta(baseline, cur, { runId: RUN, currentIsTrueActionable: true });
  assert.equal(c.weeklyDeltaState, WEEKLY_DELTA_STATE.REACTIVATED);
});

check("new_annual_cycle_is_new", () => {
  const cur = {
    id: "gdi_opp_demo_annual_2027",
    title: "Demo Association Annual Meeting 2027",
    organizationName: "Demo Association (DA)",
    eventStartDate: "2027-06-08",
    destinationStatus: "Bethesda, MD",
    venueSourcingStatus: "HOTEL_VENUE_TBD",
    opportunityType: "PRIMARY_PURSUIT",
    priority: "MEDIUM_PRIORITY",
    sources: [{ url: "https://demo.org/annual-2027" }],
  };
  const c = classifyWeeklyDelta(baseline, cur, { runId: RUN, currentIsTrueActionable: true });
  assert.equal(c.weeklyDeltaState, WEEKLY_DELTA_STATE.NEW);
});

check("brand_new_event_is_new", () => {
  const cur = {
    id: "gdi_opp_new_summit_2026",
    title: "Montgomery Health Innovation Summit 2026",
    organizationName: "Montgomery Health Alliance",
    eventStartDate: "2026-10-02",
    destinationStatus: "Rockville, MD",
    venueSourcingStatus: "HOTEL_VENUE_TBD",
    priority: "HIGH_PRIORITY",
    sources: [{ url: "https://moha.org/summit-2026" }],
  };
  const c = classifyWeeklyDelta(baseline, cur, { runId: RUN, currentIsTrueActionable: true });
  assert.equal(c.weeklyDeltaState, WEEKLY_DELTA_STATE.NEW);
});

check("alias_title_not_new", () => {
  const cur = {
    id: "cand_alias",
    title: "DA Annual Meeting — Bethesda 2026",
    organizationName: "Demo Association",
    eventStartDate: "2026-06-10",
    destinationStatus: "Bethesda, MD",
    sources: [{ url: "https://demo.org/annual-2026" }],
  };
  const m = scoreCanonicalMatch(baseline[0], cur);
  assert.equal(m.matched, true);
  const c = classifyWeeklyDelta(baseline, cur, { runId: RUN, currentIsTrueActionable: true });
  assert.notEqual(c.weeklyDeltaState, WEEKLY_DELTA_STATE.NEW);
});

check("different_url_same_event_not_new", () => {
  const cur = {
    ...prior,
    id: "cand_url2",
    sources: [{ url: "https://events.demo.org/2026" }],
  };
  // Without shared domain, still match via title/org/date
  const c = classifyWeeklyDelta(baseline, cur, { runId: RUN, currentIsTrueActionable: true });
  assert.notEqual(c.weeklyDeltaState, WEEKLY_DELTA_STATE.NEW);
});

check("firstSeenAt_does_not_reset", () => {
  const cur = { ...prior, venueSourcingStatus: "HOTEL_VENUE_TBD", priority: "MEDIUM_PRIORITY" };
  const c = classifyWeeklyDelta(baseline, cur, { runId: RUN, currentIsTrueActionable: true });
  assert.equal(c.firstSeenAt, prior.firstSeenAt);
  assert.ok(c.lastSeenAt);
});

check("customer_lifecycle_survives_apply", () => {
  const delta = computeWeeklyDelta({
    baselineOpportunities: baseline,
    currentCandidates: [
      {
        ...prior,
        venueSourcingStatus: "HOTEL_VENUE_TBD",
        priority: "MEDIUM_PRIORITY",
        actionabilityV3: "TRUE_ACTIONABLE",
      },
    ],
    runId: RUN,
  });
  const next = applyWeeklyDeltaToOpportunities({
    existingOpportunities: [prior],
    delta,
    runId: RUN,
    latestCompletedWeeklyRunId: RUN,
  });
  const row = next.find((o) => o.id === prior.id);
  assert.equal(row.customerValidation.status, "VALIDATED");
  assert.equal(row.customerAction.status, "CONTACTED");
  assert.equal(row.customerOutcome.status, "OPEN");
  assert.equal(row.firstSeenAt, prior.firstSeenAt);
});

check("zero_duplicate_on_rediscovery", () => {
  const delta = computeWeeklyDelta({
    baselineOpportunities: baseline,
    currentCandidates: [
      { ...prior, id: "alt_id_same_event", actionabilityV3: "TRUE_ACTIONABLE" },
    ],
    runId: RUN,
  });
  const next = applyWeeklyDeltaToOpportunities({
    existingOpportunities: [prior],
    delta,
    runId: RUN,
    latestCompletedWeeklyRunId: RUN,
    newOpportunityBuilders: {
      alt_id_same_event: { id: "alt_id_same_event", title: prior.title },
    },
  });
  assert.equal(next.filter((o) => /demo_annual_2026|alt_id/.test(o.id)).length, 1);
});

check("isNewThisWeek_derives_from_run_id", () => {
  assert.equal(deriveIsNewThisWeek({ firstSeenRunId: RUN }, RUN), true);
  assert.equal(deriveIsNewThisWeek({ firstSeenRunId: "old" }, RUN), false);
});

check("ui_exports_weekly_helpers", () => {
  const uiPath = path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js");
  const cssPath = path.join(ROOT, "public/css/group-demand-intelligence.css");
  const ui = fs.readFileSync(uiPath, "utf8");
  const css = fs.readFileSync(cssPath, "utf8");
  assert.match(ui, /weeklyDeltaPillHtml/);
  assert.match(ui, /data-gdi-weekly/);
  assert.match(ui, /New This Week/);
  assert.match(ui, /weeklyDeltaHeaderSummaryHtml/);
  assert.match(css, /gdi-pill-delta-new/);
  assert.match(css, /gdi-weekly-summary/);
  const app = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
    "utf8"
  );
  const share = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/share-app.js"),
    "utf8"
  );
  assert.match(app, /data-gdi-weekly/);
  assert.match(share, /data-gdi-weekly/);
  assert.match(app, /weeklyHeaderCounts/);
  assert.match(share, /weeklyHeaderCounts/);
});

if (failed) {
  console.error(`\n${failed} failing`);
  process.exit(1);
}
console.log("\nAll weekly-delta checks passed");
