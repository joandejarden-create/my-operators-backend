#!/usr/bin/env node
/**
 * V1 Final Client Readiness — commercial interpretation tests.
 */
import assert from "node:assert/strict";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import { runCompetitiveGapEngineFromStore } from "../lib/ai-visibility/gaps/competitive-gap-engine.js";
import {
  interpretProductionGap,
  auditGapInterpretations,
  resolveBrandScenarioEligibility,
  SCENARIO_ELIGIBILITY,
  ACTION_DISPOSITION,
  GAP_COMMERCIAL_MEANING,
  SCENARIO_DECISION_TERRITORY,
} from "../lib/ai-visibility/gap-commercial-interpretation.js";
import { buildExecutiveFindings } from "../lib/ai-visibility/executive-finding-engine.js";
import { peerSetBrandNamesById, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import { isAssociationAttributeProductionEligible } from "../lib/ai-visibility/gaps/association-eligibility.js";
import {
  loadCachedTruthComparisons,
  filterTruthComparisonsForCohort,
} from "../lib/ai-visibility/truth-layer/truth-comparisons-loader.js";

const AC = "rec9aZp7GHtzUEg0c";
const WESTIN = "recIPuBC50fv13zRR";
const AUTOGRAPH = "recEJCTDj1zrsjPM6";
const MARRIOTT = [AUTOGRAPH, "recCvV0PuZOi8c3hC", AC, "rec02zPClpWUTCyXM", WESTIN];

let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

function mockGap(overrides = {}) {
  return {
    gapId: overrides.gapId || "gap_test",
    subjectBrandId: overrides.subjectBrandId || AC,
    scenarioId: overrides.scenarioId || "scenario_soft_brand_collection_affiliation_v1",
    classification: overrides.classification || "HIGH_PRIORITY",
    persistence: "STRONGLY_REPEATED",
    observationCount: 10,
    providers: ["openai"],
    lifecycleStatus: "ACTIVE",
    peerBrandIds: [AUTOGRAPH],
    ...overrides,
  };
}

async function main() {
  console.log("\nV1 Final Client Readiness tests\n");

  await test("hard brand excluded from soft-brand scenario", async () => {
    const r = interpretProductionGap(
      mockGap({ subjectBrandId: WESTIN, scenarioId: "scenario_soft_brand_collection_affiliation_v1" }),
      { brandNamesById: { [WESTIN]: "Westin" } }
    );
    assert.equal(r.eligibilityStatus, SCENARIO_ELIGIBILITY.OUT_OF_SCOPE);
    assert.equal(r.actionDisposition, ACTION_DISPOSITION.NO_ACTION_EXPECTED_POSITIONING);
    assert.equal(r.executiveEligible, false);
  });

  await test("collection brand eligible for collection scenario", async () => {
    const r = interpretProductionGap(
      mockGap({ subjectBrandId: AUTOGRAPH, scenarioId: "scenario_soft_brand_collection_affiliation_v1" }),
      { brandNamesById: { [AUTOGRAPH]: "Autograph Collection" } }
    );
    assert.equal(r.eligibilityStatus, SCENARIO_ELIGIBILITY.ELIGIBLE);
    assert.equal(r.executiveEligible, true);
  });

  await test("AC lifestyle brand out of scope for soft-brand", async () => {
    const r = resolveBrandScenarioEligibility(AC, "scenario_soft_brand_collection_affiliation_v1");
    assert.equal(r.eligibilityStatus, SCENARIO_ELIGIBILITY.OUT_OF_SCOPE);
  });

  await test("AC eligible for new build when territory mapped", async () => {
    const r = resolveBrandScenarioEligibility(AC, "scenario_newbuild_uu_brand_selection_v1");
    assert.equal(r.decisionTerritory, "New Build");
    assert.equal(r.eligibilityStatus, SCENARIO_ELIGIBILITY.ELIGIBLE);
  });

  await test("out-of-scope gap blocked from executive findings", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);
    const pf = await buildExecutiveFindings({
      store,
      brandIds: MARRIOTT,
      brandNamesById,
      geographyKey: "CALA",
      language: "en",
      scope: "portfolio",
      peerSetId: PEER_SET_ID_V2,
    });
    const bad = pf.findings.find(
      (f) =>
        f.brandId === WESTIN &&
        f.findingType === "LARGEST_COMPETITIVE_GAP" &&
        f.scenarioId === "scenario_soft_brand_collection_affiliation_v1"
    );
    assert.equal(bad, undefined);
  });

  await test("out-of-scope gap preserved in raw P0C audit counts", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);
    const engine = await runCompetitiveGapEngineFromStore(store, {
      geography: "CALA",
      language: "en",
      brandIds: MARRIOTT,
      brandNamesById,
      peerSetId: PEER_SET_ID_V2,
    });
    const audit = auditGapInterpretations(
      engine.gaps.filter((g) => g.classification),
      { brandNamesById }
    );
    assert.ok(audit.counts.RAW_P0C_PRODUCTION_GAPS > audit.counts.EXECUTIVE_GAPS);
    assert.ok(audit.counts.OUT_OF_SCOPE_GAPS > 0);
  });

  await test("NO_ACTION_EXPECTED_POSITIONING for Westin soft-brand", async () => {
    const r = interpretProductionGap(
      mockGap({ subjectBrandId: WESTIN }),
      { brandNamesById: { [WESTIN]: "Westin" } }
    );
    assert.equal(r.actionDisposition, ACTION_DISPOSITION.NO_ACTION_EXPECTED_POSITIONING);
    assert.equal(r.commercialMeaning, GAP_COMMERCIAL_MEANING.EXPECTED_POSITIONING_DIFFERENCE);
  });

  await test("ACTION_REQUIRED for eligible HIGH_PRIORITY conversion gap", async () => {
    const r = interpretProductionGap(
      mockGap({
        subjectBrandId: AC,
        scenarioId: "scenario_independent_uu_conversion_v1",
      }),
      { brandNamesById: { [AC]: "AC Hotels" } }
    );
    assert.equal(r.eligibilityStatus, SCENARIO_ELIGIBILITY.ELIGIBLE);
    assert.equal(r.actionDisposition, ACTION_DISPOSITION.ACTION_REQUIRED);
    assert.equal(r.executiveEligible, true);
  });

  await test("Truth gap independent of scenario competitive eligibility", async () => {
    const { comparisons } = loadCachedTruthComparisons();
    const execTruth = filterTruthComparisonsForCohort(comparisons, {
      language: "en",
      brandIds: [WESTIN],
    }).filter((c) => c.executiveEligible === true);
    const store = createBrandAiVisibilityReadStore({});
    const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);
    const pf = await buildExecutiveFindings({
      store,
      brandIds: MARRIOTT,
      brandNamesById,
      geographyKey: "CALA",
      language: "en",
      scope: "portfolio",
      peerSetId: PEER_SET_ID_V2,
    });
    if (execTruth.length) {
      assert.ok(
        pf.findings.some((f) => f.findingType === "POTENTIAL_AI_PERCEPTION_GAP" && f.brandId === WESTIN)
      );
    }
  });

  await test("research association blocked", async () => {
    assert.equal(isAssociationAttributeProductionEligible("OWNER_FLEXIBILITY"), false);
  });

  await test("MONITOR gap disposition", async () => {
    const r = interpretProductionGap(mockGap({ classification: "MONITOR" }), {});
    assert.equal(r.actionDisposition, ACTION_DISPOSITION.MONITOR_ONLY);
    assert.equal(r.executiveEligible, false);
  });

  await test("no-action does not outrank actionable finding in exec output", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);
    const pf = await buildExecutiveFindings({
      store,
      brandIds: MARRIOTT,
      brandNamesById,
      geographyKey: "CALA",
      language: "en",
      scope: "portfolio",
      peerSetId: PEER_SET_ID_V2,
    });
    const actionable = pf.findings.findIndex(
      (f) => f.actionDisposition === ACTION_DISPOSITION.ACTION_REQUIRED
    );
    const noAction = pf.findings.findIndex(
      (f) => f.actionDisposition === ACTION_DISPOSITION.NO_ACTION_EXPECTED_POSITIONING
    );
    if (actionable >= 0 && noAction >= 0) {
      assert.ok(actionable < noAction);
    }
    assert.ok(!pf.findings.some((f) => f.actionDisposition === ACTION_DISPOSITION.NO_ACTION_EXPECTED_POSITIONING));
  });

  await test("scenario territory map covers pilot scenarios", async () => {
    assert.ok(SCENARIO_DECISION_TERRITORY.scenario_soft_brand_collection_affiliation_v1);
    assert.ok(SCENARIO_DECISION_TERRITORY.scenario_independent_uu_conversion_v1);
  });

  console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}\n`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
