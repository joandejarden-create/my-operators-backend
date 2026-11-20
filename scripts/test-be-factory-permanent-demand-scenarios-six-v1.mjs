#!/usr/bin/env node
/**
 * Permanent Demand Scenario Fit exact-six — positive/negative fixtures.
 * Lesson: BE-QL-DEMAND-SCENARIOS-SIX-001
 */
import assert from "assert";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BE_FACTORY_PERMANENT_DEMAND_SCENARIOS_SIX_V1,
  DEMAND_FIT_LABELS,
  DEMAND_SCENARIO_REQUIRED_COUNT,
  decideDemandScenarioCandidateApply,
  enforceDemandScenariosExactSixOnBlocks,
  normalizeDemandFit,
  selectBestSixDemandScenarios,
  validateDemandScenariosExactSix,
} from "../lib/brand-explorer/be-factory-permanent-demand-scenarios-six-v1.js";
import { evaluateFactoryPermanentStandardsGate } from "../lib/brand-explorer/be-factory-permanent-loyalty-region-standards-v1.js";
import { assembleCumulativeProposal } from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";
import { loadFounderQualityLessonsRegistry } from "../lib/brand-explorer/be-founder-architecture-corrections-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const fixDir = path.join(root, "fixtures/brand-explorer-factory-permanent-standards");

assert.equal(
  BE_FACTORY_PERMANENT_DEMAND_SCENARIOS_SIX_V1,
  "BE_FACTORY_PERMANENT_DEMAND_SCENARIOS_SIX_V1"
);
assert.equal(DEMAND_SCENARIO_REQUIRED_COUNT, 6);

assert.equal(normalizeDemandFit("Strong").label, DEMAND_FIT_LABELS.STRONG_FIT);
assert.equal(normalizeDemandFit("Not a fit").label, DEMAND_FIT_LABELS.NOT_A_FIT);
assert.equal(normalizeDemandFit("Moderate Fit").label, DEMAND_FIT_LABELS.MODERATE_FIT);
assert.equal(normalizeDemandFit("Very strong").ok, false, "Very strong rejected");
assert.equal(normalizeDemandFit("Moderate–strong").ok, false, "Moderate–strong rejected for new gen");
assert.equal(normalizeDemandFit("Medium-high").ok, false);
assert.equal(
  normalizeDemandFit("Moderate–strong", { allowLegacyCompounds: true }).label,
  DEMAND_FIT_LABELS.MODERATE_FIT,
  "legacy CURRENT can map Moderate–strong → Moderate Fit"
);

const five = JSON.parse(
  fs.readFileSync(path.join(fixDir, "failing-demand-scenarios-five.json"), "utf8")
);
const fiveV = validateDemandScenariosExactSix(five.blocks);
assert.equal(fiveV.ok, false);
assert.ok(fiveV.reasons.includes(five.expectReason));

const seven = JSON.parse(
  fs.readFileSync(path.join(fixDir, "failing-demand-scenarios-seven.json"), "utf8")
);
const sevenV = validateDemandScenariosExactSix(seven.blocks);
assert.equal(sevenV.ok, false);
assert.ok(sevenV.reasons.includes(seven.expectReason));

const selected = selectBestSixDemandScenarios(seven.blocks);
assert.equal(selected.selectedCount, 6, "scored selection yields six");
assert.equal(selected.canCertify, true);
assert.ok(selected.excluded.length >= 1, "seventh card recorded as excluded");
assert.equal(validateDemandScenariosExactSix(selected.selected).ok, true);

const dup = JSON.parse(
  fs.readFileSync(path.join(fixDir, "failing-demand-scenarios-duplicate.json"), "utf8")
);
const dupV = validateDemandScenariosExactSix(dup.blocks);
assert.equal(dupV.ok, false);
assert.ok(dupV.reasons.includes(dup.expectReason));

const badFit = JSON.parse(
  fs.readFileSync(path.join(fixDir, "failing-demand-scenarios-invalid-fit.json"), "utf8")
);
const badFitV = validateDemandScenariosExactSix(badFit.blocks);
assert.equal(badFitV.ok, false);
assert.ok(badFitV.reasons.includes(badFit.expectReason));

const pass = JSON.parse(
  fs.readFileSync(path.join(fixDir, "passing-demand-scenarios-six.json"), "utf8")
);
assert.equal(validateDemandScenariosExactSix(pass.blocks).ok, true);

// Assembler: incomplete five must not replace CURRENT six
const decideFive = decideDemandScenarioCandidateApply(pass.blocks, five.blocks);
assert.equal(decideFive.applyCandidate, false);
const assembledFive = assembleCumulativeProposal(pass.blocks, five.blocks);
assert.equal(
  assembledFive.blocks.filter((b) => b.slotKey === "commercial.demand").length,
  6,
  "CURRENT six retained when candidate has five"
);

// Assembler: seven candidate auto-normalized via selection → apply
const decideSeven = decideDemandScenarioCandidateApply([], seven.blocks);
assert.equal(decideSeven.applyCandidate, true, "scored seven→six may apply");
assert.equal(decideSeven.normalizedBlocks.length, 6);

const enforced = enforceDemandScenariosExactSixOnBlocks(seven.blocks);
assert.equal(enforced.needsRemediation, false);
assert.equal(enforced.validation.ok, true);
assert.equal(enforced.audit.excluded.length, 1);

const gate = evaluateFactoryPermanentStandardsGate({
  blocks: pass.blocks,
  loyaltyParticipates: false,
});
assert.equal(gate.ok, true);
assert.ok(gate.lessons.includes("BE-QL-DEMAND-SCENARIOS-SIX-001"));

const gateSeven = evaluateFactoryPermanentStandardsGate({
  blocks: seven.blocks,
  loyaltyParticipates: false,
});
assert.equal(gateSeven.ok, false, "raw seven blocks fail permanent gate");

const registry = loadFounderQualityLessonsRegistry({ reload: true });
const lessons = registry?.lessons || [];
const found = lessons.find((l) => l.lessonId === "BE-QL-DEMAND-SCENARIOS-SIX-001");
assert.ok(found, "BE-QL-DEMAND-SCENARIOS-SIX-001 registered");

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: BE_FACTORY_PERMANENT_DEMAND_SCENARIOS_SIX_V1,
      lessonId: "BE-QL-DEMAND-SCENARIOS-SIX-001",
      negativeFixturesBlocked: true,
      positiveFixturePass: true,
      sevenScoredToSix: true,
      assemblerRetainsSix: true,
    },
    null,
    2
  )
);
