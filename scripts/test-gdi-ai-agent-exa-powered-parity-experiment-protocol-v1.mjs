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

assert.ok(fs.existsSync(doc), "Exa parity protocol doc missing");
assert.ok(fs.existsSync(summary), "Exa parity protocol summary missing");
assert.ok(fs.existsSync(focusPath), "focus-eight lock missing");

const j = JSON.parse(fs.readFileSync(summary, "utf8"));
assert.equal(j.schema, "gdi_ai_agent_exa_powered_parity_experiment_v1");
assert.equal(j.status, "PROTOCOL_DESIGN_LOCKED_SPEND_DENIED");
assert.equal(j.paidResearchAuthorized, false);
assert.equal(j.paidResearchExecuted, false);
assert.equal(j.clarification.wave1OfficialPageIterateDoesNotTestExaPoweredBaseline, true);
assert.equal(j.clarification.doNotConcludeAgentsHaveNoValueFromOfficialPageOnly, true);
assert.equal(j.fairnessContract.identicalExaCapabilitiesBothArms, true);
assert.equal(j.fairnessContract.armAIsCurrentExaPoweredGdiNotOfficialPageOnly, true);
assert.equal(j.fairnessContract.budgetAsymmetryForbidden, true);
assert.equal(j.fairnessContract.evidenceGroundedMaterialLiftRequired, true);
assert.equal(j.waves.E1_paid_exa_parity.defaultAuthorized, false);
assert.equal(j.waves.E1_paid_exa_parity.hardCeilingUsdTotal, 20);
assert.equal(j.recommendation.executePaidResearchNow, false);
assert.equal(j.recommendation.conductBoundedPaidExperimentAfterSeparateAuth, true);
assert.equal(j.writes.airtable, false);
assert.equal(j.cohort.focusSubsetCases, 8);
assert.equal(j.conclusionScope.primary, "A_incremental_research_on_existing_opportunities");

const focus = JSON.parse(fs.readFileSync(focusPath, "utf8"));
assert.equal(focus.cases.length, 8);

const md = fs.readFileSync(doc, "utf8");
assert.match(md, /Do not conclude that agents offer no commercial value/i);
assert.match(md, /Exa Agent API/);
assert.match(md, /Dealality adaptive research agent/);
assert.match(md, /\$20/);
assert.match(md, /Wave E1/);
assert.match(md, /Conclusion B/);
assert.match(md, /Paid research executed by this PR:\*\* none/i);
assert.doesNotMatch(md, /Authorize paid Exa spend now/i);

console.log(
  JSON.stringify(
    {
      ok: true,
      status: j.status,
      paidResearchAuthorized: false,
      focusCases: 8,
      proposedCeilingUsd: j.waves.E1_paid_exa_parity.hardCeilingUsdTotal,
      recommendPaidAfterAuth: true,
      executeNow: false,
    },
    null,
    2,
  ),
);
