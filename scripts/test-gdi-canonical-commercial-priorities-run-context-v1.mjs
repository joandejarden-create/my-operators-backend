#!/usr/bin/env node
/**
 * $0 — Commercial Priorities run-context → canonical Full E2E.
 * Soft guidance only; never removes segments or auto-rejects.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCanonicalCommercialPrioritiesRunContext,
  scoreCommercialAlignmentForOpportunity,
  applyCommercialPrioritiesToOpportunityRows,
  classifyCommercialPriorityChangeImpact,
  CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID,
  HILTON_NY_TIMES_SQUARE_HOTEL_ID,
  WYNDHAM_HOTEL_ID_RECONCILIATION,
  GDI_CANONICAL_COMMERCIAL_PRIORITIES_RUN_CONTEXT_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-canonical-commercial-priorities-run-context-v1.js";
import {
  mapSavedProfileToCommercialPriorities,
  peakRoomsPresetToCoreTargetBand,
} from "../lib/group-demand-intelligence/need-date-demand/hotel-saved-commercial-profile-research-bridge-v1.js";
import { emptyCommercialProfile } from "../lib/group-demand-intelligence/need-date-demand/hotel-commercial-priorities-contract-v1.js";
import { runCanonicalFullE2eResearch } from "../lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js";
import {
  saveNeedPeriodChange,
  clearIsolatedNeedPeriodStore,
  NEED_DATES_PORTAL_ISOLATION_NAMESPACE,
} from "../lib/group-demand-intelligence/need-date-demand/need-period-hotel-store-v1.js";
import { NEED_PERIOD_STATUS } from "../lib/group-demand-intelligence/need-date-demand/need-date-demand-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/gdi-canonical-commercial-priorities-run-context-v1",
);
fs.mkdirSync(REPORT, { recursive: true });

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}
async function checkAsync(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}

const HOTEL_A = "recLuxvwwxID7U2B8"; // Bethesda
const HOTEL_B = CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID;
const ISOLATION = NEED_DATES_PORTAL_ISOLATION_NAMESPACE;

check("wyndham_hilton_ids_separate", () => {
  assert.equal(CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID, "recnPB3Q02NO8Ma1o");
  assert.equal(HILTON_NY_TIMES_SQUARE_HOTEL_ID, "rec35fExUxCClpOP6");
  assert.equal(
    WYNDHAM_HOTEL_ID_RECONCILIATION.relationship,
    "SEPARATE_HOTELS_NOT_ALIAS",
  );
});

check("peak_rooms_preset_maps_to_fit_band", () => {
  assert.deepEqual(peakRoomsPresetToCoreTargetBand("26_50"), {
    coreTargetPeakRoomsMin: 26,
    coreTargetPeakRoomsMax: 50,
  });
  assert.deepEqual(peakRoomsPresetToCoreTargetBand("FLEXIBLE"), {
    coreTargetPeakRoomsMin: null,
    coreTargetPeakRoomsMax: null,
  });
});

await checkAsync("1_no_stated_preferences_is_neutral", async () => {
  const ctx = await buildCanonicalCommercialPrioritiesRunContext(HOTEL_A, {
    skipLoad: true,
    commercialProfile: emptyCommercialProfile(HOTEL_A),
  });
  assert.equal(ctx.ok, true);
  assert.equal(ctx.savedProfileApplied, false);
  assert.equal(ctx.snapshot.missingFieldsAreNeutral, true);
  assert.equal(ctx.snapshot.doesNotLimitDiscovery, true);
  assert.equal(ctx.snapshot.zeroActiveNeedPeriodsMeansNeutralTiming, true);
  const scored = scoreCommercialAlignmentForOpportunity(
    { title: "Any Association Conference 2027", segmentId: "associations_conferences_events" },
    ctx,
  );
  assert.equal(scored.commercialAlignment, "NEUTRAL");
  assert.equal(scored.doesNotRejectForPreferenceMiss, true);
});

await checkAsync("2_group_rooms_and_need_months", async () => {
  clearIsolatedNeedPeriodStore(HOTEL_A);
  saveNeedPeriodChange({
    hotelId: HOTEL_A,
    isolation: ISOLATION,
    changeType: "ADD",
    actorUser: { id: "cp_test" },
    patch: {
      periodLabel: "Mar–Apr need",
      status: NEED_PERIOD_STATUS.ACTIVE,
      startDate: "2027-03-01",
      endDate: "2027-04-30",
      revenueNeed: ["GROUP_GUESTROOMS"],
    },
  });
  const profile = {
    ...emptyCommercialProfile(HOTEL_A),
    profileId: "cp_group_rooms_v1",
    currentVersion: 3,
    revenuePriorities: ["GROUP_ROOM_NIGHTS"],
    peakRoomsPreset: "51_100",
    bookingWindow: "3_6_MONTHS",
    prioritySegments: ["associations_conferences_events"],
    lastConfirmedAt: "2026-11-01T00:00:00.000Z",
    provenance: "HOTEL_CONFIRMED",
  };
  const ctx = await buildCanonicalCommercialPrioritiesRunContext(HOTEL_A, {
    commercialProfile: profile,
    needPeriodIsolation: ISOLATION,
  });
  assert.equal(ctx.savedProfileApplied, true);
  assert.equal(ctx.snapshot.preferredRoomBlockSize.min, 51);
  assert.equal(ctx.snapshot.preferredRoomBlockSize.max, 100);
  assert.ok(ctx.activeNeedPeriods.length >= 1);
  assert.equal(ctx.snapshot.currentVersion, 3);
  const mapped = mapSavedProfileToCommercialPriorities(profile);
  assert.equal(mapped.coreTargetPeakRoomsMin, 51);
  assert.equal(mapped.doesNotLimitDiscovery, true);
});

await checkAsync("3_catering_and_social_events", async () => {
  const profile = {
    ...emptyCommercialProfile(HOTEL_A),
    profileId: "cp_catering",
    currentVersion: 1,
    revenuePriorities: ["LOCAL_CATERING"],
    meetingSpaceNeed: "MEETINGS_CATERING_ONLY",
    prioritySegments: ["social_special_events", "corporate_celebrations_private_events"],
    lastConfirmedAt: "2026-11-01T00:00:00.000Z",
  };
  const ctx = await buildCanonicalCommercialPrioritiesRunContext(HOTEL_A, {
    commercialProfile: profile,
  });
  const scored = scoreCommercialAlignmentForOpportunity(
    {
      title: "Wedding banquet catering block",
      summary: "Banquet and F&B catering for social event",
      segmentId: "social_special_events",
    },
    ctx,
  );
  assert.equal(scored.commercialAlignment, "SUPPORTS_STATED_PRIORITIES");
  assert.ok(scored.hotelPreferenceMatchExplanations.length >= 1);
});

await checkAsync("4_verified_business_restrictions", async () => {
  const profile = {
    ...emptyCommercialProfile(HOTEL_A),
    profileId: "cp_restrict",
    currentVersion: 1,
    businessToAvoid: ["Spring Break"],
    hotelConsiderations: ["Noise curfew"],
    lastConfirmedAt: "2026-11-01T00:00:00.000Z",
  };
  const ctx = await buildCanonicalCommercialPrioritiesRunContext(HOTEL_A, {
    commercialProfile: profile,
  });
  const scored = scoreCommercialAlignmentForOpportunity(
    { title: "Spring Break student housing block" },
    ctx,
  );
  assert.equal(scored.commercialAlignment, "CONFLICTS_WITH_RESTRICTION");
  assert.ok(scored.restrictionConflicts.length >= 1);
  // Still not rejected — demand preserved
  assert.equal(scored.doesNotRejectForPreferenceMiss, true);
});

await checkAsync("5_selected_demand_segments_do_not_remove_others", async () => {
  const profile = {
    ...emptyCommercialProfile(HOTEL_A),
    profileId: "cp_segments",
    currentVersion: 1,
    prioritySegments: ["education_scientific"],
    excludedSegments: ["sports_entertainment"],
    lastConfirmedAt: "2026-11-01T00:00:00.000Z",
  };
  const ctx = await buildCanonicalCommercialPrioritiesRunContext(HOTEL_A, {
    commercialProfile: profile,
  });
  assert.deepEqual(ctx.snapshot.targetSegments, ["education_scientific"]);
  assert.equal(ctx.snapshot.doesNotLimitDiscovery, true);
  // Soft: sports still scores without rejection
  const sports = scoreCommercialAlignmentForOpportunity(
    { title: "Local tournament housing", segmentId: "sports_entertainment" },
    ctx,
  );
  assert.notEqual(sports.commercialAlignment, "REJECTED");
  assert.equal(sports.doesNotRejectForPreferenceMiss, true);
});

await checkAsync("6_priority_update_between_weekly_runs", async () => {
  const prior = {
    profileId: "cp_v1",
    currentVersion: 1,
    revenuePriorities: ["GROUP_ROOM_NIGHTS"],
    activeNeedPeriods: [],
  };
  const current = {
    profileId: "cp_v1",
    currentVersion: 2,
    revenuePriorities: ["LOCAL_CATERING"],
    activeNeedPeriods: [],
  };
  const impact = classifyCommercialPriorityChangeImpact({
    priorSnapshot: prior,
    currentSnapshot: current,
  });
  assert.equal(impact.priorityChangeAloneIsNotNewDiscovery, true);
  assert.equal(impact.versionChanged, true);
  assert.equal(impact.revenueChanged, true);
  assert.ok(impact.weeklyChangeLabels.includes("CHANGED_HOTEL_PRIORITY"));
  assert.equal(impact.reEvaluateQualifiedInventoryWithoutPaidDiscovery, true);
});

await checkAsync("7_historical_replaced_need_periods_excluded", async () => {
  clearIsolatedNeedPeriodStore(HOTEL_A);
  saveNeedPeriodChange({
    hotelId: HOTEL_A,
    isolation: ISOLATION,
    changeType: "ADD",
    actorUser: { id: "cp_test" },
    patch: {
      periodLabel: "Replaced period",
      status: NEED_PERIOD_STATUS.REPLACED,
      startDate: "2026-01-01",
      endDate: "2026-01-31",
      revenueNeed: ["GROUP_GUESTROOMS"],
    },
  });
  saveNeedPeriodChange({
    hotelId: HOTEL_A,
    isolation: ISOLATION,
    changeType: "ADD",
    actorUser: { id: "cp_test" },
    patch: {
      periodLabel: "Active period",
      status: NEED_PERIOD_STATUS.ACTIVE,
      startDate: "2027-05-01",
      endDate: "2027-05-31",
      revenueNeed: ["GROUP_GUESTROOMS"],
    },
  });
  const ctx = await buildCanonicalCommercialPrioritiesRunContext(HOTEL_A, {
    commercialProfile: {
      ...emptyCommercialProfile(HOTEL_A),
      profileId: "cp_periods",
      currentVersion: 1,
      lastConfirmedAt: "2026-11-01T00:00:00.000Z",
    },
    needPeriodIsolation: ISOLATION,
  });
  assert.ok(ctx.activeNeedPeriods.every((p) => p.status !== "REPLACED"));
  assert.ok(ctx.historicalOrReplacedNeedPeriods.length >= 1);
  assert.ok(
    ctx.activeNeedPeriods.some((p) => /Active/i.test(p.periodLabel || "")),
  );
});

await checkAsync("8_multi_hotel_same_opp_different_preferences", async () => {
  const sharedOpp = {
    id: "shared_opp_1",
    title: "National Association Annual Meeting 2027",
    organizationName: "Example Society",
    segmentId: "associations_conferences_events",
    summary: "Association conference with group room block",
  };
  const beth = await buildCanonicalCommercialPrioritiesRunContext(HOTEL_A, {
    commercialProfile: {
      ...emptyCommercialProfile(HOTEL_A),
      profileId: "beth_cp",
      currentVersion: 1,
      revenuePriorities: ["ASSOCIATION_BUSINESS"],
      lastConfirmedAt: "2026-11-01T00:00:00.000Z",
    },
  });
  const wyn = await buildCanonicalCommercialPrioritiesRunContext(HOTEL_B, {
    commercialProfile: {
      ...emptyCommercialProfile(HOTEL_B),
      profileId: "wyn_cp",
      currentVersion: 1,
      revenuePriorities: ["LOCAL_CATERING"],
      lastConfirmedAt: "2026-11-01T00:00:00.000Z",
    },
  });
  assert.notEqual(beth.snapshot.hotelId, wyn.snapshot.hotelId);
  const bethRows = applyCommercialPrioritiesToOpportunityRows([sharedOpp], beth);
  const wynRows = applyCommercialPrioritiesToOpportunityRows([sharedOpp], wyn);
  assert.equal(bethRows[0].id, wynRows[0].id);
  // Different preference explanations / alignment — same opportunity identity preserved
  assert.ok(bethRows[0].commercialPrioritiesAlignment);
  assert.ok(wynRows[0].commercialPrioritiesAlignment);
});

await checkAsync("canonical_entry_writes_run_context_snapshot", async () => {
  const outDir = path.join(REPORT, "canonical-entry-cp");
  const r = await runCanonicalFullE2eResearch({
    hotelId: "recaokE8psIbuVeuH",
    mode: "fixture",
    skipHpc: true,
    skipHotelWideFinalization: true,
    allowIsolatedTestSkipHotelWideFinalization: true,
    isolatedTestHarness: true,
    disableLeisureStandardStage: true,
    disableCadStage: true,
    commercialProfile: {
      ...emptyCommercialProfile("recaokE8psIbuVeuH"),
      profileId: "laluna_cp_test",
      currentVersion: 1,
      revenuePriorities: ["LEISURE_GROUPS"],
      peakRoomsPreset: "26_50",
      lastConfirmedAt: "2026-11-01T00:00:00.000Z",
    },
    outDir,
  });
  assert.equal(r.commercialPrioritiesRunContext?.ok, true);
  assert.equal(r.commercialPrioritiesRunContext?.doesNotLimitDiscovery, true);
  assert.ok(
    fs.existsSync(path.join(outDir, "commercial-priorities-run-context.json")),
  );
  const snap = JSON.parse(
    fs.readFileSync(
      path.join(outDir, "commercial-priorities-run-context.json"),
      "utf8",
    ),
  );
  assert.equal(snap.contract, GDI_CANONICAL_COMMERCIAL_PRIORITIES_RUN_CONTEXT_V1);
  assert.equal(snap.savedProfileApplied, true);
  assert.equal(snap.snapshot.preferredRoomBlockSize.min, 26);
});

clearIsolatedNeedPeriodStore(HOTEL_A);

const failed = results.filter((r) => !r.ok);
const out = {
  contract: GDI_CANONICAL_COMMERCIAL_PRIORITIES_RUN_CONTEXT_V1,
  ok: failed.length === 0,
  passed: results.filter((r) => r.ok).length,
  failed: failed.length,
  results,
  paidLive: false,
};
fs.writeFileSync(
  path.join(REPORT, "TEST_RESULTS.json"),
  `${JSON.stringify(out, null, 2)}\n`,
);
console.log(JSON.stringify(out, null, 2));
if (failed.length) process.exit(1);
