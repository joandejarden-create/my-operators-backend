#!/usr/bin/env node
/**
 * GDI New Opportunities V1 — offline fixtures + newness + CRM-guard + lane tests.
 * No provider calls. Does not lower TRUE_ACTIONABLE bar.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ACTIONABILITY_V3,
  qualifyOpportunityV3,
} from "../lib/group-demand-intelligence/discovery-hygiene-v3.js";
import {
  DEMAND_SIGNAL_TYPE,
  NEWNESS_STATUS,
  classifyDemandSignalType,
  demandSignalTypeLabel,
  evaluateCommercialFactors,
  enrichOpportunityDemandSignal,
  assertNoCrmInference,
  preferCommercialFit,
  buildDemandLaneSearchTasks,
  computePotentialRoomNights,
  resolveNewnessStatus,
  FORBIDDEN_PUBLIC_CRM_LABELS,
  NEW_OPPORTUNITIES_V1,
  classifyPublicTrigger,
  PUBLIC_TRIGGER_CLASS,
  createOrgResearchReuseCache,
} from "../lib/group-demand-intelligence/demand-signal-types.js";
import {
  WEEKLY_DELTA_STATE,
  snapshotBaselineOpportunity,
  classifyWeeklyDelta,
  computeWeeklyDelta,
} from "../lib/group-demand-intelligence/weekly-delta.js";
import {
  buildRecallV4SearchTasks,
  inferDemandArchetype,
} from "../lib/group-demand-intelligence/discovery-recall-v4.js";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const AS_OF = "2026-03-01";
const SUBJECT = {
  hotelId: "recLuxvwwxID7U2B8",
  name: "Bethesda Marriott",
};

let failed = 0;
let passed = 0;
function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

const fix = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "data/group-demand-intelligence/evals/gdi-new-opportunities-v1-offline-fixtures.json"
    ),
    "utf8"
  )
);

check("fixture_matrix_hygiene", () => {
  assert.equal(fix.cases.length >= 12, true);
  for (const c of fix.cases) {
    const q = qualifyOpportunityV3(SUBJECT.hotelId, c.opp, {
      nowDate: AS_OF,
      subjectHotel: SUBJECT,
    });
    assert.equal(
      q.actionability,
      c.expect,
      `${c.id}: expected ${c.expect} got ${q.actionability} (${q.failureClass}) notes=${(q.notes || []).join("|")}`
    );
    if (c.failureClass) {
      assert.equal(q.failureClass, c.failureClass, `${c.id} failureClass`);
    }
    if (c.demandSignalType && q.actionability !== ACTIONABILITY_V3.INVALID) {
      assert.equal(
        q.demandSignalType || classifyDemandSignalType(c.opp),
        c.demandSignalType,
        `${c.id} demandSignalType`
      );
    }
  }
});

check("small_recurring_preferred_over_large_poor_fit", () => {
  const byId = Object.fromEntries(fix.cases.map((c) => [c.opp.id, c.opp]));
  const pair = fix.commercialPreferPair;
  const pref = preferCommercialFit(byId[pair.a], byId[pair.b]);
  assert.equal(pref.preferred, pair.expectPreferred, JSON.stringify(pref));
  assert.ok(pref.scores.a > pref.scores.b);
});

check("potential_room_nights_estimated", () => {
  const opp = fix.cases.find((c) => c.id === "recurring_25_room_training").opp;
  const rn = computePotentialRoomNights(opp);
  assert.equal(rn.potentialRoomNights, 75);
  assert.equal(rn.potentialRoomNightsStatus, "ESTIMATED");
});

check("newness_new_to_gdi_only", () => {
  const prior = {
    id: "gdi_opp_existing_train",
    title: "Annual Leadership Certification Training Program",
    organizationName: "Leadership Institute East",
    eventStartDate: "2026-08-10",
    destinationStatus: "Bethesda, MD",
    venueSourcingStatus: "HOTEL_VENUE_TBD",
    opportunityType: "PRIMARY_PURSUIT",
    priority: "MEDIUM_PRIORITY",
    firstSeenAt: "2026-09-01T00:00:00.000Z",
    firstSeenRunId: "gdi_weekly_old",
    sources: [{ url: "https://example.org/leadership-training-annual" }],
  };
  const baseline = [snapshotBaselineOpportunity(prior)];
  const runId = "gdi_weekly_new_opp_v1";

  const same = classifyWeeklyDelta(baseline, { ...prior }, {
    runId,
    currentIsTrueActionable: true,
  });
  assert.notEqual(same.weeklyDeltaState, WEEKLY_DELTA_STATE.NEW);
  assert.equal(same.newnessStatus, NEWNESS_STATUS.EXISTING_IN_GDI);

  const neu = classifyWeeklyDelta(
    baseline,
    {
      id: "cand_brand_new_program",
      title: "Cybersecurity Field Training Academy 2027",
      organizationName: "Defense Training Network",
      eventStartDate: "2027-03-01",
      destinationStatus: "Bethesda, MD",
      venueSourcingStatus: "HOTEL_VENUE_TBD",
      opportunityType: "PRIMARY_PURSUIT",
      priority: "HIGH_PRIORITY",
      sources: [{ url: "https://example.org/cyber-academy-2027" }],
    },
    { runId, currentIsTrueActionable: true }
  );
  assert.equal(neu.weeklyDeltaState, WEEKLY_DELTA_STATE.NEW);
  assert.equal(neu.newnessStatus, NEWNESS_STATUS.NEW_TO_GDI);
  assert.equal(neu.isNewThisWeek, true);

  const watchPrior = {
    ...prior,
    id: "gdi_opp_watch",
    priority: "WATCHLIST",
    opportunityType: "FUTURE_CYCLE",
    venueSourcingStatus: "UNKNOWN",
  };
  const reactivated = classifyWeeklyDelta(
    [snapshotBaselineOpportunity(watchPrior)],
    {
      ...watchPrior,
      venueSourcingStatus: "HOTEL_VENUE_TBD",
      opportunityType: "PRIMARY_PURSUIT",
      priority: "HIGH_PRIORITY",
    },
    { runId, currentIsTrueActionable: true }
  );
  assert.equal(reactivated.weeklyDeltaState, WEEKLY_DELTA_STATE.REACTIVATED);
  assert.equal(reactivated.isNewThisWeek, false);
  assert.equal(reactivated.newnessStatus, NEWNESS_STATUS.EXISTING_IN_GDI);
});

check("no_crm_inference_labels", () => {
  for (const c of fix.cases) {
    const enriched = enrichOpportunityDemandSignal({
      ...c.opp,
      isNewThisWeek: true,
      weeklyDeltaState: "NEW",
    });
    assert.equal(enriched.newnessStatus, NEWNESS_STATUS.NEW_TO_GDI);
    const guard = assertNoCrmInference(enriched);
    assert.equal(guard.ok, true, JSON.stringify(guard.hits));
  }
  // Hard reject if someone tries to set NEW_TO_HOTEL — remap to UNKNOWN (not NEW_TO_GDI)
  const poisoned = enrichOpportunityDemandSignal({
    title: "Test",
    newnessStatus: "NEW_TO_HOTEL",
  });
  assert.equal(poisoned.newnessStatus, NEWNESS_STATUS.UNKNOWN);
  for (const lab of FORBIDDEN_PUBLIC_CRM_LABELS) {
    assert.notEqual(poisoned.newnessStatus, lab);
  }
});

check("public_trigger_is_not_opportunity", () => {
  const t = classifyPublicTrigger(
    "Agency announces $12M contract award for IT support"
  );
  assert.equal(t.triggerClass, PUBLIC_TRIGGER_CLASS.CONTRACT_AWARD);
  assert.equal(t.isOpportunity, false);
  assert.equal(t.requiresLodgingThesis, true);
});

check("org_research_reuse_cache", () => {
  const cache = createOrgResearchReuseCache();
  cache.set("Public Sector Training Consortium", {
    officialDomain: "example.org",
  });
  cache.set("Public Sector Training Consortium", { incrementReuse: true });
  const hit = cache.get("public sector training consortium");
  assert.equal(hit.officialDomain, "example.org");
  assert.equal(cache.stats().reusedHits >= 1, true);
});

check("project_team_lane_present", () => {
  const plan = buildDemandLaneSearchTasks({
    geos: ["Bethesda", "Washington DC"],
    archetype: "URBAN_BUSINESS_MEETINGS",
    maxLanes: 14,
    maxQueriesPerLane: 3,
  });
  const lanes = plan.tasks.map((t) => t.lane);
  assert.equal(lanes.includes("PROJECT_TEAM"), true);
  assert.equal(lanes.includes("RELOCATION"), true);
  assert.equal(plan.queryBudget > 0, true);
  assert.equal(plan.queryBudget <= 14 * 3, true);
});

check("demand_signal_labels", () => {
  assert.equal(
    demandSignalTypeLabel(DEMAND_SIGNAL_TYPE.TRAINING_PROGRAM),
    "Training Program"
  );
  assert.equal(
    classifyDemandSignalType({
      title: "Corporate office relocation temporary housing",
    }),
    DEMAND_SIGNAL_TYPE.CORPORATE_RELOCATION
  );
});

check("lane_budgets_bounded", () => {
  const plan = buildDemandLaneSearchTasks({
    geos: ["Bethesda", "Rockville", "Washington DC"],
    archetype: "URBAN_BUSINESS_MEETINGS",
    maxLanes: 10,
    maxQueriesPerLane: 3,
  });
  assert.equal(plan.version, NEW_OPPORTUNITIES_V1);
  assert.ok(plan.queryBudget <= 30, `budget ${plan.queryBudget}`);
  assert.ok(plan.tasks.some((t) => t.lane === "TRAINING"));
  assert.ok(plan.tasks.some((t) => t.lane === "GOVERNMENT_CONTRACTOR"));
});

check("archetype_lane_mix_differs", () => {
  const urban = buildDemandLaneSearchTasks({
    geos: ["New York"],
    archetype: "URBAN_BUSINESS_MEETINGS",
  });
  const luxury = buildDemandLaneSearchTasks({
    geos: ["Bermuda"],
    archetype: "LUXURY_DESTINATION_SMALL",
  });
  assert.equal(urban.tasks[0].lane, "EVENT_ASSOCIATION");
  assert.equal(luxury.tasks[0].lane, "INCENTIVE_RETREAT");
});

check("recall_v4_includes_demand_lanes", () => {
  const tasks = buildRecallV4SearchTasks({
    geos: ["Bethesda", "Rockville"],
    archetype: "URBAN_BUSINESS_MEETINGS",
    countryCode: "US",
    includeDemandLanes: true,
    maxDemandLaneQueries: 24,
  });
  assert.equal(tasks.newOpportunitiesVersion, NEW_OPPORTUNITIES_V1);
  assert.ok(tasks.tasks.some((t) => String(t.note || "").startsWith("demand_lane:")));
});

check("list_dto_exposes_demand_signal", () => {
  const dto = toOpportunityListDto(
    enrichOpportunityDemandSignal({
      id: "x",
      title: "Training",
      demandSignalType: "TRAINING_PROGRAM",
      isNewThisWeek: true,
      weeklyDeltaState: "NEW",
      peakRooms: 25,
      eventStartDate: "2026-04-14",
      eventEndDate: "2026-04-17",
    })
  );
  assert.equal(dto.demandSignalType, "TRAINING_PROGRAM");
  assert.ok(dto.demandSignalTypeLabel);
  assert.equal(dto.newnessStatus, NEWNESS_STATUS.NEW_TO_GDI);
});

check("hotel_profile_archetypes_no_hardcode", () => {
  for (const hotelId of [
    "recLuxvwwxID7U2B8",
    "recG66DQJKP2c0UNh",
    "recIwaP1etgx2g9nA",
  ]) {
    const cfg = loadHotelDemandConfig(hotelId);
    assert.ok(cfg, hotelId);
    const arch = inferDemandArchetype(cfg, {});
    assert.ok(arch, hotelId);
  }
});

check("commercial_factors_structured_not_score", () => {
  const factors = evaluateCommercialFactors(fix.cases[0].opp, { nowDate: AS_OF });
  assert.ok(factors.demandValue);
  assert.ok(factors.recurrence);
  assert.ok(factors.buyerAccessibility);
  assert.ok(factors.timing);
  assert.equal(factors.compositeScore, undefined);
});

check("resolve_newness_helper", () => {
  assert.equal(
    resolveNewnessStatus({ weeklyDeltaState: "NEW", isNewThisWeek: true }),
    NEWNESS_STATUS.NEW_TO_GDI
  );
  assert.equal(
    resolveNewnessStatus({ weeklyDeltaState: "UPDATED", isNewThisWeek: false }),
    NEWNESS_STATUS.EXISTING_IN_GDI
  );
});

check("weekly_delta_new_cycle_vs_existing", () => {
  const prior = {
    id: "gdi_opp_series_2026",
    title: "Regional Industry Conference 2026",
    organizationName: "Industry Association",
    eventStartDate: "2026-05-12",
    destinationStatus: "Bethesda, MD",
    venueSourcingStatus: "HOTEL_VENUE_TBD",
    opportunityType: "PRIMARY_PURSUIT",
    priority: "HIGH_PRIORITY",
    firstSeenRunId: "old",
    sources: [{ url: "https://example.org/conference-2026" }],
  };
  const delta = computeWeeklyDelta({
    baselineOpportunities: [prior],
    currentCandidates: [
      { ...prior, actionabilityV3: "TRUE_ACTIONABLE" },
      {
        id: "cand_2027_cycle",
        title: "Regional Industry Conference 2027",
        organizationName: "Industry Association",
        eventStartDate: "2027-05-10",
        destinationStatus: "Bethesda, MD",
        venueSourcingStatus: "HOTEL_VENUE_TBD",
        opportunityType: "PRIMARY_PURSUIT",
        priority: "HIGH_PRIORITY",
        actionabilityV3: "TRUE_ACTIONABLE",
        sources: [{ url: "https://example.org/conference-2027" }],
      },
    ],
    runId: "gdi_weekly_cycle_test",
    trueActionableIds: ["gdi_opp_series_2026", "cand_2027_cycle"],
  });
  const states = Object.fromEntries(
    delta.rows.map((r) => [r.candidateId || r.opportunityId, r.weeklyDeltaState])
  );
  assert.notEqual(states.gdi_opp_series_2026, WEEKLY_DELTA_STATE.NEW);
  assert.equal(states.cand_2027_cycle, WEEKLY_DELTA_STATE.NEW);
});

console.log(
  JSON.stringify(
    {
      version: NEW_OPPORTUNITIES_V1,
      passed,
      failed,
      total: passed + failed,
    },
    null,
    2
  )
);
process.exit(failed ? 1 : 0);
