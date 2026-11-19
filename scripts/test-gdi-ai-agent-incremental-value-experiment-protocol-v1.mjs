#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const doc = path.join(
  ROOT,
  "docs/data-intelligence/gdi-ai-agent-incremental-value-experiment-v1.md",
);
const summary = path.join(
  ROOT,
  "reports/gdi-ai-agent-incremental-value-experiment-v1/PROTOCOL_SUMMARY.json",
);

assert.ok(fs.existsSync(doc), "protocol doc missing");
assert.ok(fs.existsSync(summary), "protocol summary missing");

const j = JSON.parse(fs.readFileSync(summary, "utf8"));
assert.equal(j.schema, "gdi_ai_agent_incremental_value_experiment_v1");
assert.equal(j.status, "PROTOCOL");
assert.equal(j.pr414Role, "limitations_memo_only_not_product_authorization");
assert.equal(j.budgetParity.wave0_offline.paidUsd, 0);
assert.equal(j.budgetParity.wave1_live_public.paidUsd, 0);
assert.equal(j.budgetParity.wave2_paid.defaultAuthorized, false);
assert.equal(j.decisionFramework.wave0CannotAdopt, true);
assert.equal(j.writes.airtable, false);
assert.ok(j.deferredProductWork.includes("demand_mechanism_mission_packs"));
assert.ok(j.reusedHarnesses.includes("gdi-20opp-adaptive-investigation-experiment-v1"));
assert.ok(j.primaryMetrics.includes("newly_verified_commercially_relevant_named_contacts"));
assert.ok(j.nonRewards.includes("rubric_total_alone"));
assert.ok(
  j.cohort.excludeDeepenLoops.includes("gdi_lmc_00562bf81b1e1957"),
);
assert.ok(
  j.cohort.excludeDeepenLoops.includes("gdi_opp_2027_ddaa_annual_conference_8"),
);

const md = fs.readFileSync(doc, "utf8");
assert.match(md, /Primary question/);
assert.match(md, /ADOPT/);
assert.match(md, /ITERATE/);
assert.match(md, /REJECT/);
assert.match(md, /not\*\* authorization to build|not authorization to build/i);
assert.match(md, /limitations memo/i);
assert.doesNotMatch(md, /Authorize broad discovery architecture/i);

console.log(
  JSON.stringify(
    {
      ok: true,
      status: j.status,
      focusSubsetCases: j.cohort.focusSubsetCases,
      wave0CannotAdopt: j.decisionFramework.wave0CannotAdopt,
    },
    null,
    2,
  ),
);
