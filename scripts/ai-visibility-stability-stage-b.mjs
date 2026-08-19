#!/usr/bin/env node
/**
 * Controlled Stage B repeated-testing runner.
 * Default: --preflight only (no provider calls).
 * Paid execution requires --execute after preflight PASS.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildStageBPreflight,
  executeStageB,
  reaggregateStageBReport,
  STAGE_B_HARD_CAP_USD,
  STAGE_B_MAX_CALLS,
} from "../lib/ai-visibility/stability-stage-b-orchestrator.js";
import {
  STAGE_B_AUTHORITATIVE_REPORT_REL_PATH,
  STAGE_B_AUTHORITATIVE_WAVE_ID,
} from "../lib/ai-visibility/stability-policy.js";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const outDir = path.join(root, "reports", "ai-visibility");
const preflightPath = path.join(outDir, "repeated-testing-stage-b-preflight.json");

const args = process.argv.slice(2);
const execute = args.includes("--execute");
const reaggregate = args.includes("--reaggregate-wave");
const waveArgIdx = args.indexOf("--reaggregate-wave");
const waveIdArg =
  waveArgIdx >= 0 && args[waveArgIdx + 1] && !args[waveArgIdx + 1].startsWith("-")
    ? args[waveArgIdx + 1]
    : STAGE_B_AUTHORITATIVE_WAVE_ID;
const preflightOnly = args.includes("--preflight") || (!execute && !reaggregate);

fs.mkdirSync(outDir, { recursive: true });

process.env.AI_VISIBILITY_ENABLED = process.env.AI_VISIBILITY_ENABLED || "true";
if (execute) {
  process.env.AI_VISIBILITY_LIVE_TEST = "true";
  process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS =
    process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || "300000";
}

const preflight = await buildStageBPreflight();
fs.writeFileSync(preflightPath, JSON.stringify(preflight, null, 2));

const lookupRows = (preflight.lookup?.rows || []).map((r) => ({
  PROMPT_ID: r.PROMPT_ID,
  HISTORICAL_OBSERVATIONS_BY_PROVIDER: r.HISTORICAL_OBSERVATIONS_BY_PROVIDER,
  EXACT_REPEAT_COUNT: r.EXACT_REPEAT_COUNT,
  FIRST_OBSERVED_AT: r.FIRST_OBSERVED_AT,
  LAST_OBSERVED_AT: r.LAST_OBSERVED_AT,
  CURRENT_RECURRENCE_STATE: r.CURRENT_RECURRENCE_STATE,
  CURRENT_STABILITY_STATE: r.CURRENT_STABILITY_STATE,
}));

console.log(
  JSON.stringify(
    {
      mode: execute ? "execute" : "preflight",
      FULL_COHORT_LOOKUP: preflight.FULL_COHORT_LOOKUP,
      PROMPTS_RESOLVED: preflight.PROMPTS_RESOLVED,
      INVALID_PROMPT_IDS: preflight.INVALID_PROMPT_IDS,
      failReasons: preflight.failReasons,
      PROJECTED_TOTAL_COST: preflight.cost?.PROJECTED_TOTAL_COST,
      conservativeCost: preflight.cost?.conservativeCost,
      HARD_CAP: STAGE_B_HARD_CAP_USD,
      PLANNED_CALLS: STAGE_B_MAX_CALLS,
      PROVIDER_CALLS: 0,
      preflightPath,
      lookupRows,
    },
    null,
    2
  )
);

if (preflightOnly && !execute && !reaggregate) {
  if (preflight.FULL_COHORT_LOOKUP === "FAIL" || preflight.blocked) process.exit(2);
  process.exit(0);
}

if (reaggregate) {
  const report = await reaggregateStageBReport({ waveId: waveIdArg, preflight });
  const reportPath = path.join(outDir, path.basename(STAGE_B_AUTHORITATIVE_REPORT_REL_PATH));
  console.log(
    JSON.stringify(
      {
        mode: "reaggregate-final-wave",
        waveId: report.waveId,
        excludedWaveIds: report.excludedWaveIds,
        status: report.status,
        execution: report.execution,
        observedVsDerived: report.observedVsDerived,
        observedResults: report.observedResults,
        reportPath,
      },
      null,
      2
    )
  );
  process.exit(report.status === "PASS" ? 0 : 1);
}

if (preflight.FULL_COHORT_LOOKUP === "FAIL" || !preflight.promptCheck.ok) {
  console.error("STOP: preflight FAIL — no provider calls.");
  process.exit(2);
}
if (preflight.budgetBlocked) {
  console.error("REPEATED_TESTING_BUDGET_BLOCKED");
  process.exit(3);
}

const report = await executeStageB({ preflight });
const reportPath = path.join(outDir, "repeated-testing-stage-b-report.json");
fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
console.log(
  JSON.stringify(
    {
      status: report.status,
      waveId: report.waveId,
      execution: report.execution,
      cost: report.cost,
      reportPath,
    },
    null,
    2
  )
);
if (report.status === "REPEATED_TESTING_BUDGET_BLOCKED") process.exit(3);
if (report.status !== "PASS") process.exit(1);
