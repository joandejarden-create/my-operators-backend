#!/usr/bin/env node
/**
 * Weekly refresh contract + two-cycle deterministic proof (fixtures + test clock).
 * Separates: live execution vs deterministic replay vs simulated future scheduling.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import {
  CHANGE_CLASS,
  PUBLICATION_POLICY,
  QUEUE_KIND,
  classifyRefreshChange,
  isCheckDue,
  mayAutoPublish,
  feedbackToResearchTask,
  resolveCadenceDays,
} from "../lib/group-demand-intelligence/workflow/weekly-refresh-contract-v1.js";
import {
  detectRefreshChanges,
  buildWeeklyWorkQueues,
  runWeeklyRefreshCycle,
} from "../lib/group-demand-intelligence/workflow/weekly-refresh-orchestrator-v1.js";
import {
  acquireWeeklyRefreshLock,
  releaseWeeklyRefreshLock,
  loadWeeklyRefreshState,
  saveWeeklyRefreshState,
} from "../lib/group-demand-intelligence/workflow/weekly-refresh-state-v1.js";

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-weekly-refresh-"));
process.env.GDI_DATA_ROOT = tmpRoot;
process.env.GDI_WEEKLY_REFRESH_PERSIST_STATE_ON_DRY_RUN = "1";

const hotelId = "recTESTWEEKLY001";
fs.mkdirSync(path.join(tmpRoot, "hotels", hotelId), { recursive: true });
// Minimal hotel config for segment plan
fs.mkdirSync(
  path.join(
    path.resolve("config/group-demand-intelligence/hotels")
  ),
  { recursive: true }
);

// --- Contract unit checks ---
assert.equal(
  classifyRefreshChange(null, { organizationName: "Acme" }).changeClass,
  CHANGE_CLASS.NEW_PROSPECT
);
assert.equal(
  classifyRefreshChange(
    { organizationName: "Acme", eventStartDate: "2026-01-01" },
    { organizationName: "Acme", eventStartDate: "2026-01-01" }
  ).changeClass,
  CHANGE_CLASS.NO_MATERIAL_CHANGE
);
assert.equal(
  classifyRefreshChange(
    { organizationName: "Acme", contactEmail: "a@x.com" },
    { organizationName: "Acme", contactEmail: "b@x.com" }
  ).changeClass,
  CHANGE_CLASS.CONTACT_ENRICHED
);
assert.equal(
  classifyRefreshChange(
    { organizationName: "Acme", venueSourcingStatus: "OPEN_UNRESOLVED" },
    { organizationName: "Acme", venueSourcingStatus: "RFP_ACTIVE_SOURCING" }
  ).changeClass,
  CHANGE_CLASS.MATERIAL_UPDATE
);
assert.equal(
  classifyRefreshChange(null, {}, { previouslyAssessedInternal: true })
    .changeClass,
  CHANGE_CLASS.NEEDS_REVIEW
);
assert.equal(
  classifyRefreshChange(
    {},
    {},
    { researchBlocked: true, blockReasons: ["404"] }
  ).changeClass,
  CHANGE_CLASS.RESEARCH_BLOCKED
);

const pub = mayAutoPublish(
  { publicationPolicy: PUBLICATION_POLICY.QUALIFIED_NEW },
  { qualificationPassed: true }
);
assert.equal(pub.allowed, false);
assert.equal(pub.reason, "auto_publish_disabled");

const fbWrong = feedbackToResearchTask({ action: "wrong contact bounced" });
assert.equal(fbWrong.queueKind, QUEUE_KIND.CONTACT_ENRICHMENT);
const fbReject = feedbackToResearchTask({ outcome: "not relevant" });
assert.equal(fbReject.requireMaterialChangeToResurface, true);

assert.equal(
  isCheckDue({ lastSuccessfulCheckAt: null, now: new Date("2026-10-15") }),
  true
);
assert.equal(
  isCheckDue({
    lastSuccessfulCheckAt: "2026-10-14T00:00:00.000Z",
    cadenceDays: 7,
    now: new Date("2026-10-15T00:00:00.000Z"),
  }),
  false
);
assert.equal(
  isCheckDue({
    lastSuccessfulCheckAt: "2026-10-01T00:00:00.000Z",
    cadenceDays: 7,
    now: new Date("2026-10-15T00:00:00.000Z"),
  }),
  true
);

// Cross-segment same account → one standing match, not duplicate NEW
const baseline = [
  {
    id: "gdi_sc_1",
    hotelId,
    organizationName: "ExpandCo",
    standingOpportunity: true,
    researchSegment: "scaling_companies",
    contactEmail: "ops@expand.co",
  },
];
const candidates = [
  {
    hotelId,
    organizationName: "ExpandCo",
    standingOpportunity: true,
    researchSegment: "local_meetings_catering",
    contactEmail: "ops@expand.co",
  },
  {
    hotelId,
    organizationName: "ExpandCo",
    eventStartDate: "2026-11-01",
    title: "ExpandCo Customer Summit 2026",
  },
];
const detected = detectRefreshChanges({ baselineOpportunities: baseline, candidates });
assert.equal(detected.results[0].changeClass, CHANGE_CLASS.NO_MATERIAL_CHANGE);
assert.equal(detected.results[1].changeClass, CHANGE_CLASS.NEW_PROSPECT);

// Lock overlap prevention
const lock1 = acquireWeeklyRefreshLock(hotelId, { runId: "run_a" });
assert.equal(lock1.ok, true);
const lock2 = acquireWeeklyRefreshLock(hotelId, { runId: "run_b" });
assert.equal(lock2.ok, false);
releaseWeeklyRefreshLock(hotelId, lock1.claimToken);

// --- Two-cycle deterministic proof with test clock ---
const opps = [
  {
    id: "gdi_pe_demo",
    hotelId,
    organizationName: "Demo Venue",
    opportunityType: "VENUE_PARTNERSHIP",
    peVenueId: "pev_demo",
    hotelOpportunityThesis: "Core catchment",
    contactUrl: "https://example.com/events",
    prospectHotelFacing: true,
  },
  {
    id: "gdi_sc_demo",
    hotelId,
    organizationName: "Demo Corp",
    opportunityType: "CORPORATE_ACCOUNT_RELATIONSHIP",
    researchSegment: "scaling_companies",
    standingOpportunity: true,
    contactEmail: "hello@democorp.example",
  },
];

// Write a stub hotel config into GDI data won't load segment plan from config/
// buildWeeklyWorkQueues uses loadHotelConfig from repo config — for test hotel
// use a known hotel id for queue building instead
const liveHotel = "rec0eTEnCqlsGJJt9";
const liveRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-weekly-live-"));
process.env.GDI_DATA_ROOT = liveRoot;

const t0 = new Date("2026-10-15T12:00:00.000Z");
const cycle1 = await runWeeklyRefreshCycle({
  hotelId: liveHotel,
  opportunities: opps.map((o) => ({ ...o, hotelId: liveHotel })),
  trigger: "manual",
  now: t0,
  dryRun: true,
  clockLabel: "t0_live_fixture",
});
assert.equal(cycle1.ok, true);
assert.ok(cycle1.summary.segmentCoverageGaps.length >= 1, "coverage gaps remain");

const cycle2 = await runWeeklyRefreshCycle({
  hotelId: liveHotel,
  opportunities: opps.map((o) => ({ ...o, hotelId: liveHotel })),
  candidates: opps.map((o) => ({ ...o, hotelId: liveHotel })),
  trigger: "retry",
  now: new Date("2026-10-15T12:00:05.000Z"),
  dryRun: true,
  clockLabel: "t0_second_cycle",
});
assert.equal(cycle2.ok, true);
assert.equal(cycle2.changes.summary.newProspect, 0);
assert.ok(cycle2.changes.summary.noMaterialChange >= 1);

// Simulated future (+2 weeks): more refresh dues without claiming coverage complete
const tFuture = new Date("2026-10-29T12:00:00.000Z");
const cycleFuture = await runWeeklyRefreshCycle({
  hotelId: liveHotel,
  opportunities: opps.map((o) => ({ ...o, hotelId: liveHotel })),
  trigger: "simulated",
  now: tFuture,
  dryRun: true,
  clockLabel: "simulated_plus_2_weeks",
});
assert.equal(cycleFuture.ok, true);
assert.ok(
  (cycleFuture.summary.segmentCoverageGaps || []).length >= 1,
  "future refresh must not mark initial coverage complete"
);

// Feedback rejection then material change → resurface as MATERIAL_UPDATE
const afterReject = classifyRefreshChange(
  {
    organizationName: "RejectCo",
    venueSourcingStatus: "OPEN_UNRESOLVED",
  },
  {
    organizationName: "RejectCo",
    venueSourcingStatus: "RFP_ACTIVE_SOURCING",
    eventStartDate: "2027-03-01",
  }
);
assert.equal(afterReject.changeClass, CHANGE_CLASS.MATERIAL_UPDATE);

// Local catering zero guestrooms remains distinct identity
const catering = classifyRefreshChange(null, {
  organizationName: "Chapter Co",
  guestroomStatus: "NOT_REQUIRED",
  researchSegment: "local_meetings_catering",
});
assert.equal(catering.changeClass, CHANGE_CLASS.NEW_PROSPECT);

console.log("PASS test-gdi-weekly-refresh-v1");
console.log(
  JSON.stringify(
    {
      deterministic: {
        cycle1Gaps: cycle1.summary.segmentCoverageGaps,
        cycle2New: cycle2.changes.summary.newProspect,
        cycle2Unchanged: cycle2.changes.summary.noMaterialChange,
        futureGaps: cycleFuture.summary.segmentCoverageGaps,
      },
      tmpRoots: { unit: tmpRoot, live: liveRoot },
    },
    null,
    2
  )
);
