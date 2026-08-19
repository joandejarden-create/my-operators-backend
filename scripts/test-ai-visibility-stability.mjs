#!/usr/bin/env node
/**
 * Repeated-testing / stability contract tests.
 * No provider calls. No DataForSEO. No scheduler.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  RECURRENCE_STATES,
  STABILITY_STATES,
  CROSS_PROVIDER_STATES,
  MIN_OBSERVATIONS,
  STABILITY_METHODOLOGY_COPY,
  FORBIDDEN_STABILITY_COPY,
  aggregateStabilitySeries,
  classifyCrossProviderAlignment,
  attachStabilityMetadataToGap,
  classifyStageBTimeWindow,
} from "../lib/ai-visibility/stability-aggregation.js";
import { lookupValidationCohortHistory } from "../lib/ai-visibility/stability-historical-audit.js";
import { verifyValidationPromptConfigs } from "../lib/ai-visibility/stability-stage-b-orchestrator.js";
import { classifyExecutiveEvidenceSupportLabel } from "../lib/ai-visibility/stability-client.js";
import {
  VALIDATION_COHORT,
  estimateValidationCost,
  resolveSamplingPriority,
  HISTORIC_PROVIDER_COST,
  VALIDATION_COST_CAPS,
  STAGE_B_AUTHORITATIVE_REPORT_REL_PATH,
  STAGE_B_AUTHORITATIVE_WAVE_ID,
  STAGE_B_NON_AUTHORITATIVE_WAVE_IDS,
} from "../lib/ai-visibility/stability-policy.js";
import {
  formatExecutiveEvidenceLanguage,
  formatFindingSupportDescriptor,
  enrichRowWithObservationSummary,
} from "../lib/ai-visibility/stability-client.js";
import { computeAiPresenceRate, computeQuestionsMissing } from "../lib/ai-visibility/metrics.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const BRAND_JS = path.join(root, "public", "js", "ai-visibility", "ai-visibility-brand.js");

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nHotel Brand AI Intelligence — Repeated Testing / Stability\n");

test("enums + min observations", () => {
  assert.ok(RECURRENCE_STATES.includes("ONE_OFF"));
  assert.ok(STABILITY_STATES.includes("CONSISTENTLY_PRESENT"));
  assert.ok(CROSS_PROVIDER_STATES.includes("HIGH_VARIABILITY"));
  assert.equal(MIN_OBSERVATIONS.STABILITY_ELIGIBLE, 3);
});

test("N=1 is one-off / insufficient stability", () => {
  const s = aggregateStabilitySeries(
    [{ promptId: "p1", provider: "openai", presentEntityIds: ["b1"], timestamp: "2026-08-01" }],
    { brandId: "b1", provider: "openai" }
  );
  assert.equal(s.recurrenceState, "ONE_OFF");
  assert.equal(s.stabilityState, "INSUFFICIENT_OBSERVATIONS");
  assert.equal(s.presenceLabel, "Present in 1 of 1 observed runs");
  assert.equal(s.NUMERIC_CONFIDENCE, false);
});

test("N=2 early repeated evidence", () => {
  const s = aggregateStabilitySeries(
    [
      { presentEntityIds: ["b1"], timestamp: "2026-08-01" },
      { presentEntityIds: ["b1"], timestamp: "2026-08-08" },
    ],
    { brandId: "b1" }
  );
  assert.equal(s.recurrenceState, "EARLY_REPEATED_EVIDENCE");
  assert.equal(s.stabilityState, "INSUFFICIENT_OBSERVATIONS");
});

test("N=3 consistent present", () => {
  const s = aggregateStabilitySeries(
    [
      { presentEntityIds: ["b1"], timestamp: "2026-06-01" },
      { presentEntityIds: ["b1"], timestamp: "2026-07-01" },
      { presentEntityIds: ["b1"], timestamp: "2026-08-01" },
    ],
    { brandId: "b1" }
  );
  assert.equal(s.stabilityState, "CONSISTENTLY_PRESENT");
  assert.equal(s.recurrenceState, "RECURRENT");
  assert.equal(s.timeWindow, "LONGITUDINAL");
});

test("N=3 mixed same-run is MIXED not trend", () => {
  const s = aggregateStabilitySeries(
    [
      { presentEntityIds: ["b1"], timestamp: "2026-08-01T10:00:00Z" },
      { presentEntityIds: [], timestamp: "2026-08-01T10:20:00Z" },
      { presentEntityIds: ["b1"], timestamp: "2026-08-01T10:40:00Z" },
    ],
    { brandId: "b1" }
  );
  assert.equal(s.stabilityState, "MIXED");
  assert.equal(s.timeWindow, "SAME_RUN_REPETITION");
});

test("exact and variant denominators stay separate", () => {
  const exact = aggregateStabilitySeries([{ presentEntityIds: ["b1"], timestamp: "2026-08-01" }], {
    brandId: "b1",
    repeatType: "EXACT_REPEAT",
  });
  const variant = aggregateStabilitySeries([{ presentEntityIds: ["b1"], timestamp: "2026-08-01" }], {
    brandId: "b1",
    repeatType: "CONTROLLED_VARIANT",
  });
  assert.equal(exact.exactRepeatCount, 1);
  assert.equal(exact.variantCount, 0);
  assert.equal(variant.variantCount, 1);
  assert.equal(variant.exactRepeatCount, 0);
});

test("cross-provider does not pool 4/6", () => {
  const openai = aggregateStabilitySeries(
    [
      { presentEntityIds: ["b1"], timestamp: "2026-06-01" },
      { presentEntityIds: ["b1"], timestamp: "2026-07-01" },
      { presentEntityIds: ["b1"], timestamp: "2026-08-01" },
    ],
    { brandId: "b1", provider: "openai" }
  );
  const pplx = aggregateStabilitySeries(
    [
      { presentEntityIds: ["b1"], timestamp: "2026-06-01" },
      { presentEntityIds: [], timestamp: "2026-07-01" },
      { presentEntityIds: [], timestamp: "2026-08-01" },
    ],
    { brandId: "b1", provider: "perplexity" }
  );
  const align = classifyCrossProviderAlignment([openai, pplx]);
  assert.equal(align.crossProviderAlignment, "HIGH_VARIABILITY");
  assert.notEqual(openai.presenceCount + pplx.presenceCount, null);
  assert.equal(openai.presenceLabel.includes("3 of 3"), true);
  assert.equal(pplx.presenceLabel.includes("1 of 3"), true);
});

test("no forbidden confidence copy", () => {
  const blob = [
    formatExecutiveEvidenceLanguage({
      observationCount: 3,
      presenceCount: 3,
      recurrenceState: "RECURRENT",
      stabilityState: "CONSISTENTLY_PRESENT",
    }),
    formatFindingSupportDescriptor({ observationCount: 3, providerCount: 4 }),
  ]
    .join(" ")
    .toLowerCase();
  for (const phrase of FORBIDDEN_STABILITY_COPY) {
    assert.equal(blob.includes(phrase), false, phrase);
  }
  assert.match(STABILITY_METHODOLOGY_COPY, /artificial confidence scores/);
});

test("sampling rules are transparent", () => {
  assert.equal(resolveSamplingPriority({ commercialPriority: "CRITICAL" }), "CRITICAL");
  assert.equal(resolveSamplingPriority({ promptOrigin: "DERIVED" }), "EXPLORATORY");
  assert.equal(
    resolveSamplingPriority({ demandTier: "HIGH", coreOwnerDecision: true }),
    "HIGH"
  );
  assert.equal(resolveSamplingPriority({ priorInstability: true }), "CRITICAL");
});

test("validation cohort mix and cost caps", () => {
  const cost = estimateValidationCost();
  assert.ok(cost.promptCount >= 12 && cost.promptCount <= 20);
  assert.ok(cost.observed >= 1);
  assert.ok(cost.scenario >= 1);
  assert.ok(cost.derived >= 1);
  assert.equal(cost.FULL_133_PROMPT_RUN, 0);
  assert.equal(cost.withinTarget, true);
  assert.equal(cost.STOP, false);
  assert.ok(cost.expectedHistoricCost <= VALIDATION_COST_CAPS.TARGET_HISTORIC_USD);
  assert.ok(cost.conservativeCost <= VALIDATION_COST_CAPS.HARD_CAP_USD);
  assert.equal(cost.STAGE_B, "READY_UNDER_HARD_CAP");
  assert.equal(VALIDATION_COST_CAPS.HARD_CAP_USD, 30);
  assert.equal(cost.hardCapUsd, 30);
  assert.equal(cost.totalValidationCalls, 31);
  assert.equal(cost.callsByProvider.openai, 16);
  assert.equal(cost.callsByProvider.gemini, 3);
  assert.equal(cost.callsByProvider.perplexity, 9);
  assert.equal(cost.callsByProvider.claude, 3);
  assert.ok(cost.PROJECTED_TOTAL_COST < 30);
  assert.ok(cost.conservativeCost <= VALIDATION_COST_CAPS.HARD_CAP_USD);
  assert.ok(VALIDATION_COHORT.some((r) => r.language === "es"));
  assert.ok(VALIDATION_COHORT.some((r) => r.origin === "DERIVED"));
});

test("per-provider historic rates have sample sizes", () => {
  for (const id of ["openai", "gemini", "perplexity", "claude"]) {
    assert.equal(HISTORIC_PROVIDER_COST[id].sampleSize, 84);
    assert.ok(HISTORIC_PROVIDER_COST[id].historicUsdPerCall > 0);
  }
});

test("gap attach does not change raw existence", () => {
  const gap = { gapId: "g1", exists: true, classification: "HIGH_PRIORITY" };
  const out = attachStabilityMetadataToGap(gap, { recurrenceState: "ONE_OFF" });
  assert.equal(out.gapId, "g1");
  assert.equal(out.exists, true);
  assert.equal(out.classification, "HIGH_PRIORITY");
  assert.equal(out.stability.recurrenceState, "ONE_OFF");
});

test("certified presence/QM inputs unchanged by stability fields", () => {
  const obs = [
    { promptId: "p1", success: true, presentEntityIds: ["b1"] },
    { promptId: "p2", success: true, presentEntityIds: [] },
  ];
  const a = computeAiPresenceRate(obs, "b1");
  const b = computeAiPresenceRate(
    obs.map((o) => ({ ...o, recurrenceState: "ONE_OFF" })),
    "b1"
  );
  assert.equal(a.value, b.value);
  assert.equal(computeQuestionsMissing(obs, "b1").value, computeQuestionsMissing(
    obs.map((o) => ({ ...o, stabilityState: "INSUFFICIENT_OBSERVATIONS" })),
    "b1"
  ).value);
});

test("UI compact evidence language, no confidence", () => {
  const js = fs.readFileSync(BRAND_JS, "utf8");
  assert.ok(js.includes("observationSupport"));
  assert.ok(js.includes("aiv-insight-stability"));
  assert.ok(!js.includes("high confidence"));
  assert.ok(!js.includes("Stability Score"));
});

test("row enrich stays one-off without a series", () => {
  const row = enrichRowWithObservationSummary({ presenceObserved: true, batchDate: "2026-08-01" });
  assert.equal(row.observationSummary.recurrence, "ONE_OFF");
  assert.equal(row.brandStatus, undefined);
});

test("Stage B time window never labels longitudinal as long-term stable", () => {
  assert.equal(classifyStageBTimeWindow("2026-08-01", "2026-08-20"), "SHORT_TERM");
  assert.equal(
    classifyStageBTimeWindow("2026-08-14T10:00:00Z", "2026-08-14T18:00:00Z"),
    "SAME_RUN_REPETITION"
  );
  assert.equal(classifyStageBTimeWindow("2026-08-14", "2026-08-17"), "SHORT_TERM");
});

test("all 16 validation prompt IDs resolve from fixtures", () => {
  const check = verifyValidationPromptConfigs();
  assert.equal(check.ok, true);
  assert.deepEqual(check.INVALID_PROMPT_IDS, []);
  assert.equal(check.configs.length, 16);
  assert.equal(check.configs.filter((c) => c.monitoringEligible === false).length, 6);
  assert.equal(check.monitoringEligibleToggled, false);
});

test("executive support labels stay non-confidence", () => {
  assert.equal(classifyExecutiveEvidenceSupportLabel({ observationCount: 1 }), "EARLY_SIGNAL");
  assert.equal(
    classifyExecutiveEvidenceSupportLabel({
      observationCount: 3,
      recurrenceState: "RECURRENT",
      stabilityState: "CONSISTENTLY_PRESENT",
    }),
    "RECURRENT"
  );
  assert.equal(
    classifyExecutiveEvidenceSupportLabel({
      observationCount: 4,
      providerCount: 4,
      crossProviderAlignment: "HIGH_VARIABILITY",
    }),
    "PROVIDER_VARIABLE"
  );
});

{
  const name = "full cohort lookup is not limited to the first 40 grains";
  try {
    const filler = [];
    for (let i = 0; i < 45; i += 1) {
      filler.push({
        promptId: `p_filler_${String(i).padStart(3, "0")}`,
        provider: "openai",
        language: "en",
        geographyKey: "CALA",
        timestamp: "2026-08-14T12:00:00.000Z",
        payload: { mentions: [{ canonicalEntityId: "b1" }], citations: [] },
      });
    }
    const targetId = VALIDATION_COHORT[0].promptId;
    filler.push({
      promptId: targetId,
      provider: "openai",
      language: "en",
      geographyKey: "CALA",
      timestamp: "2026-08-14T12:00:00.000Z",
      payload: { mentions: [{ canonicalEntityId: "b1" }], citations: [] },
    });
    const store = {
      async listEvidence() {
        return filler;
      },
    };
    const lookup = await lookupValidationCohortHistory({ store });
    const row = lookup.rows.find((r) => r.PROMPT_ID === targetId);
    assert.ok(row);
    assert.equal(row.EXACT_REPEAT_COUNT, 1);
    assert.notEqual(row.HISTORICAL_OBSERVATIONS_BY_PROVIDER, "ZERO");
    const observed = lookup.rows.filter((r) => r.origin === "OBSERVED" || r.origin === "DERIVED");
    assert.ok(observed.every((r) => r.EXACT_REPEAT_COUNT === 0));
    assert.equal(observed[0].HISTORICAL_OBSERVATIONS_BY_PROVIDER, "ZERO");
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

{
  const name = "authoritative Stage B report binds final wave only";
  try {
    const reportPath = path.join(root, STAGE_B_AUTHORITATIVE_REPORT_REL_PATH);
    assert.ok(fs.existsSync(reportPath), `missing ${STAGE_B_AUTHORITATIVE_REPORT_REL_PATH}`);
    const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
    assert.equal(report.waveId, STAGE_B_AUTHORITATIVE_WAVE_ID);
    assert.ok(report.excludedWaveIds.includes(STAGE_B_NON_AUTHORITATIVE_WAVE_IDS[0]));
    assert.equal(report.stageBEvidenceCount, 31);
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log(`\nRepeated testing / stability tests: ${passed} passed, ${failed} failed\n`);
if (failed) process.exit(1);
