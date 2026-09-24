/**
 * GDI Yield Recovery V1.1 — regression tests (fixture / unit; no Airtable required).
 */
import assert from "node:assert/strict";
import {
  TARGET_TYPE,
  TARGET_STATUS,
  TARGET_PRIORITY,
  RESEARCH_CADENCE,
  EXECUTION_STATUS,
  RESULT_TYPE,
  buildResearchTarget,
  buildTargetRun,
  classifyTargetAgainstEvidence,
  executeCoverageCycle,
  applyTargetRunOutcome,
  buildCustomerResearchSummary,
  buildTargetsFromExistingIntelligence,
} from "../lib/group-demand-intelligence/research-coverage/index.js";
import {
  routeTargetToPlaybook,
  PLAYBOOK,
  executeBoundedTargetResearch,
} from "../lib/group-demand-intelligence/weekly-discovery-orchestrator.js";
import {
  findCanonicalDuplicate,
  buildPromotionProvenance,
  validateQualifiedCandidate,
  NEWNESS_SEMANTICS,
} from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";

const HOTEL = "recLuxvwwxID7U2B8";
let failed = 0;

function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(err && err.stack ? err.stack : err);
  }
}

async function checkAsync(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(err && err.stack ? err.stack : err);
  }
}

function makeTarget(overrides = {}) {
  return buildResearchTarget({
    hotelId: HOTEL,
    hotelName: "Bethesda Marriott",
    targetType: TARGET_TYPE.DEMAND_GENERATOR,
    entityKey: overrides.entityKey || "dg_yr",
    entityId: overrides.entityId || "dg_yr",
    demandGeneratorId: overrides.demandGeneratorId || "dg_yr",
    canonicalName: overrides.canonicalName || "Yield Recovery Target",
    priority: TARGET_PRIORITY.HIGH,
    status: TARGET_STATUS.ACTIVE,
    researchCadence: RESEARCH_CADENCE.WEEKLY,
    nextResearchAt: overrides.nextResearchAt || new Date(Date.now() - 86400000).toISOString(),
    lastResearchedAt: overrides.lastResearchedAt ?? null,
    primarySourceUrl: overrides.primarySourceUrl || "https://example.org/housing",
    ...overrides,
  });
}

check("backfill_baseline_not_weekly_new", () => {
  const pack = buildTargetsFromExistingIntelligence({
    hotelId: HOTEL,
    hotelName: "Bethesda Marriott",
    generators: Array.from({ length: 50 }, (_, i) => ({
      demandGeneratorId: `dg_base_${i}`,
      organizationName: `Org ${i}`,
      firstSeenAt: "2026-01-01T00:00:00.000Z",
    })),
    fits: Array.from({ length: 50 }, (_, i) => ({
      hotelId: HOTEL,
      demandGeneratorId: `dg_base_${i}`,
      generatorPriority: "HIGH",
    })),
    programs: [],
    venues: [],
    venueFits: [],
  });
  assert.equal(pack.totals.total, 50);

  const cycle = executeCoverageCycle({
    hotelId: HOTEL,
    targets: pack.targets,
    evidence: {
      signals: pack.targets.map((t) => ({
        demandGeneratorId: t.demandGeneratorId,
        firstSeenAt: "2026-01-01T00:00:00.000Z",
        lastSeenAt: "2026-01-01T00:00:00.000Z",
      })),
    },
  });
  assert.equal(cycle.run.newSignals, 0);
  assert.equal(cycle.researchedTargetRuns.length, 0);
  for (const tr of cycle.targetRuns.filter((x) => x.executionStatus !== "NOT_DUE")) {
    assert.notEqual(tr.resultType, RESULT_TYPE.NEW_SIGNAL);
    assert.equal(tr.executionStatus, EXECUTION_STATUS.SKIPPED);
    assert.match(String(tr.lastResultSummary || ""), /BASELINE_EXISTING|NO_MATERIAL/);
  }
});

check("first_true_future_signal_after_baseline_is_new", () => {
  const t = makeTarget({
    lastResearchedAt: "2026-09-01T00:00:00.000Z",
    demandGeneratorId: "dg_post_base",
    entityKey: "dg_post_base",
    entityId: "dg_post_base",
  });
  const classification = classifyTargetAgainstEvidence(
    t,
    {
      signals: [
        {
          demandGeneratorId: "dg_post_base",
          firstSeenAt: "2026-09-20T00:00:00.000Z",
          lastSeenAt: "2026-09-20T00:00:00.000Z",
        },
      ],
    },
    { runId: "gdir_post" }
  );
  assert.equal(classification.resultType, RESULT_TYPE.NEW_SIGNAL);
  assert.equal(classification.newSignalCount, 1);
  assert.equal(classification.researchExecuted, false);
  assert.equal(classification.executionStatus, EXECUTION_STATUS.SKIPPED);
});

check("coverage_row_without_research_not_researched", () => {
  const t = makeTarget({ lastResearchedAt: null });
  const cycle = executeCoverageCycle({
    hotelId: HOTEL,
    targets: [t],
    evidence: { signals: [], opportunities: [] },
  });
  assert.equal(cycle.researchedTargetRuns.length, 0);
  assert.equal(cycle.targetRuns[0].executionStatus, EXECUTION_STATUS.SKIPPED);
  assert.equal(cycle.targetRuns[0].researchExecuted, false);
  const applied = applyTargetRunOutcome(t, cycle.targetRuns[0]);
  assert.equal(applied.learning.researched, false);
  assert.equal(applied.target.lastResearchedAt, null);
});

check("target_routed_to_specific_playbook", () => {
  assert.equal(
    routeTargetToPlaybook({ targetType: TARGET_TYPE.DEMAND_GENERATOR }),
    PLAYBOOK.DEMAND_GENERATOR
  );
  assert.equal(
    routeTargetToPlaybook({ targetType: TARGET_TYPE.EVENT_SERIES }),
    PLAYBOOK.EVENT_FUTURE_CYCLE
  );
  assert.equal(
    routeTargetToPlaybook({ targetType: TARGET_TYPE.PRIVATE_EVENT_VENUE }),
    PLAYBOOK.VENUE_PARTNERSHIP
  );
  assert.equal(
    routeTargetToPlaybook({ targetType: TARGET_TYPE.SPORTS_SERIES }),
    PLAYBOOK.SPORTS_HOUSING
  );
  assert.equal(
    routeTargetToPlaybook({ targetType: TARGET_TYPE.TRAINING_PROGRAM }),
    PLAYBOOK.TRAINING_PROGRAM
  );
  assert.equal(
    routeTargetToPlaybook({ targetType: TARGET_TYPE.HOUSING_PAGE }),
    PLAYBOOK.LODGING_HOUSING
  );
  assert.notEqual(
    routeTargetToPlaybook({ targetType: TARGET_TYPE.EVENT_SERIES }),
    PLAYBOOK.GENERAL_FOLLOWUP
  );
});

check("duplicate_true_updates_existing", () => {
  const existing = [
    {
      id: "gdi_opp_bethesda_premier_cup_2026",
      title: "Bethesda Premier Cup",
      organizationName: "Bethesda Soccer Club",
      eventStartDate: "2026-07-10",
    },
  ];
  const dup = findCanonicalDuplicate(
    {
      id: "gdi_opp_other",
      title: "Bethesda Premier Cup 2026",
      organizationName: "Bethesda Soccer",
      eventStartDate: "2026-07-10",
    },
    existing
  );
  assert.ok(dup);
  assert.equal(dup.match.id, "gdi_opp_bethesda_premier_cup_2026");
});

check("historical_discovery_provenance_preserved", () => {
  const prov = buildPromotionProvenance({
    promotionRunId: "gdi_yield_recovery_v1_1_20260924",
    discoveryRunId: "gdi_new_opps_v1_2_live_bethesda_20260923",
    discoveryAt: "2026-09-23T23:36:33.455Z",
    method: "new_opportunities_v1_2_controlled_promote",
    playbook: "TRAINING_PROGRAM",
    newnessSemantics: NEWNESS_SEMANTICS.NEW_TO_GDI_ON_PROMOTION,
    at: "2026-09-24T17:56:12.072Z",
  });
  assert.equal(prov.firstDiscoveredRunId, "gdi_new_opps_v1_2_live_bethesda_20260923");
  assert.equal(prov.firstDiscoveredAt, "2026-09-23T23:36:33.455Z");
  assert.equal(prov.firstSeenRunId, "gdi_yield_recovery_v1_1_20260924");
  assert.notEqual(prov.firstDiscoveredAt.slice(0, 10), "2026-09-24");
  assert.equal(prov.newnessSemantics, NEWNESS_SEMANTICS.NEW_TO_GDI_ON_PROMOTION);
});

check("true_candidate_validates_for_promotion", () => {
  const v = validateQualifiedCandidate({
    id: "gdi_opp_regulatory_information_conference_20260930",
    hotelId: HOTEL,
    title: "Regulatory Information Conference",
    organizationName: "Nuclear Regulatory Commission",
    opportunityQualification: "STRONG",
    eventStartDate: "2026-09-30",
    officialSource:
      "https://www.nrc.gov/public-meetings-hearings/conferences-symposia/regulatory-information-conference/hotel-information",
    lodgingEvidence: "Official housing page",
  });
  assert.equal(v.ok, true);
  assert.equal(v.failed.length, 0);
});

check("customer_summary_null_without_researched", () => {
  const t = makeTarget();
  const cycle = executeCoverageCycle({
    hotelId: HOTEL,
    targets: [t],
    evidence: {},
  });
  const summary = buildCustomerResearchSummary({
    run: cycle.run,
    researchedTargetRuns: cycle.researchedTargetRuns,
    activeTargetCount: 1,
  });
  assert.equal(summary, null);
});

check("researched_target_run_advances_cadence", () => {
  const t = makeTarget({ lastResearchedAt: null });
  const tr = buildTargetRun({
    hotelId: HOTEL,
    targetId: t.targetId,
    runId: "gdir_real",
    executionStatus: EXECUTION_STATUS.RESEARCHED,
    researchExecuted: true,
    resultType: RESULT_TYPE.NO_MATERIAL_CHANGE,
  });
  const applied = applyTargetRunOutcome(t, tr);
  assert.equal(applied.learning.researched, true);
  assert.ok(applied.target.lastResearchedAt);
});

await checkAsync("bounded_research_marks_researched_with_fetch", async () => {
  const t = makeTarget({
    primarySourceUrl: "https://example.com/",
    targetType: TARGET_TYPE.HOUSING_PAGE,
  });
  const research = await executeBoundedTargetResearch(t, { runId: "gdir_fetch" });
  assert.equal(research.executionStatus, EXECUTION_STATUS.RESEARCHED);
  assert.equal(research.researchExecuted, true);
  assert.ok(research.fetchesUsed >= 1);
  assert.equal(research.playbook, PLAYBOOK.LODGING_HOUSING);
});

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nAll GDI Yield Recovery V1.1 checks passed.");
