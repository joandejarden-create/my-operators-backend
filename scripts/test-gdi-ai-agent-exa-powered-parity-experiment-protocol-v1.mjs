#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const doc = path.join(
  ROOT,
  "docs/data-intelligence/gdi-ai-agent-exa-powered-parity-experiment-v1.md",
);
const summary = path.join(
  ROOT,
  "reports/gdi-ai-agent-exa-powered-parity-experiment-v1/PROTOCOL_SUMMARY.json",
);
const focusPath = path.join(
  ROOT,
  "fixtures/gdi-experiments/gdi-ai-agent-incremental-value-experiment-v1/focus-eight-cases.json",
);
const e1Results = path.join(
  ROOT,
  "reports/gdi-ai-agent-exa-powered-parity-experiment-v1/E1_HARNESS_RESULTS.json",
);
const e1Eval = path.join(
  ROOT,
  "reports/gdi-ai-agent-exa-powered-parity-experiment-v1/E1_INDEPENDENT_EVALUATION.json",
);

assert.ok(fs.existsSync(doc), "Exa parity protocol doc missing");
assert.ok(fs.existsSync(summary), "Exa parity protocol summary missing");
assert.ok(fs.existsSync(focusPath), "focus-eight lock missing");

const j = JSON.parse(fs.readFileSync(summary, "utf8"));
assert.equal(j.schema, "gdi_ai_agent_exa_powered_parity_experiment_v1");
assert.ok(
  j.status === "PROTOCOL_DESIGN_LOCKED_SPEND_DENIED" ||
    j.status === "E0_HARNESS_READY_SPEND_DENIED" ||
    j.status === "E1_COMPLETE_REJECT",
  `unexpected status ${j.status}`,
);
assert.equal(j.clarification.wave1OfficialPageIterateDoesNotTestExaPoweredBaseline, true);
assert.equal(j.clarification.doNotConcludeAgentsHaveNoValueFromOfficialPageOnly, true);
assert.equal(j.fairnessContract.identicalExaCapabilitiesBothArms, true);
assert.equal(j.fairnessContract.armAIsCurrentExaPoweredGdiNotOfficialPageOnly, true);
assert.equal(j.fairnessContract.budgetAsymmetryForbidden, true);
assert.equal(j.fairnessContract.evidenceGroundedMaterialLiftRequired, true);
assert.equal(j.waves.E1_paid_exa_parity.hardCeilingUsdTotal, 20);
assert.equal(j.writes.airtable, false);
assert.equal(j.cohort.focusSubsetCases, 8);
assert.equal(j.conclusionScope.primary, "A_incremental_research_on_existing_opportunities");

if (j.e0) {
  assert.equal(j.e0.actualPaidUsd, 0);
  assert.equal(j.e0.e1Readiness, "E1_READY");
  assert.equal(j.e0.commercialVerdict, "NOT_APPLICABLE_E0_MOCK");
}

if (j.status === "E1_COMPLETE_REJECT") {
  assert.equal(j.paidResearchAuthorized, true);
  assert.equal(j.paidResearchExecuted, true);
  assert.ok(j.e1, "e1 block required");
  assert.equal(j.e1.independentVerdict, "REJECT");
  assert.equal(j.e1.commercialVerdict, "REJECT");
  assert.equal(j.e1.materialLiftCases, 0);
  assert.ok(Number(j.e1.actualPaidUsd) > 0);
  assert.ok(Number(j.e1.actualPaidUsd) <= 20);
  assert.equal(j.recommendation.executePaidResearchNow, false);
  assert.equal(j.recommendation.integrateAdaptiveAgentIntoProductionGdi, false);
  assert.equal(j.recommendation.authorizeE2, false);
  assert.ok(fs.existsSync(e1Results), "E1 results missing");
  assert.ok(fs.existsSync(e1Eval), "E1 independent eval missing");
  const results = JSON.parse(fs.readFileSync(e1Results, "utf8"));
  assert.equal(results.status, "E1_COMPLETE");
  assert.equal(results.airtableWrites, false);
  assert.equal(results.customerPublication, false);
  assert.ok(Number(results.actualPaidUsd) <= 20);
  const indep = JSON.parse(fs.readFileSync(e1Eval, "utf8"));
  assert.equal(indep.independentChallengerVerdict.decision, "REJECT");
} else {
  assert.equal(j.paidResearchAuthorized, false);
  assert.equal(j.paidResearchExecuted, false);
  assert.equal(j.recommendation.executePaidResearchNow, false);
  assert.equal(j.recommendation.conductBoundedPaidExperimentAfterSeparateAuth, true);
}

const focus = JSON.parse(fs.readFileSync(focusPath, "utf8"));
assert.equal(focus.cases.length, 8);

const md = fs.readFileSync(doc, "utf8");
assert.match(md, /Do not conclude that agents offer no commercial value/i);
assert.match(md, /Exa Agent API/);
assert.match(md, /Dealality adaptive research agent/);
assert.match(md, /\$20/);
assert.match(md, /Wave E1/);
assert.match(md, /Conclusion B/);
if (j.status === "E1_COMPLETE_REJECT") {
  assert.match(md, /independent verdict \*\*REJECT\*\*/i);
  assert.match(md, /\$0\.88/);
} else {
  assert.match(md, /Paid research executed by this PR:\*\* none/i);
  assert.doesNotMatch(md, /Authorize paid Exa spend now/i);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      status: j.status,
      paidResearchAuthorized: j.paidResearchAuthorized,
      paidResearchExecuted: j.paidResearchExecuted,
      focusCases: 8,
      e1ActualPaidUsd: j.e1?.actualPaidUsd ?? null,
      e1Verdict: j.e1?.independentVerdict ?? null,
      proposedCeilingUsd: j.waves.E1_paid_exa_parity.hardCeilingUsdTotal,
    },
    null,
    2,
  ),
);
