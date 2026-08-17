#!/usr/bin/env node
/**
 * Presence product integration + Stage 1 OpenAI Query-Origin Regionalization.
 *
 *   node scripts/ai-intelligence-presence-product-and-query-origin-stage1.mjs --plan-only
 *   node scripts/ai-intelligence-presence-product-and-query-origin-stage1.mjs --execute
 *
 * Requires AI_VISIBILITY_LIVE_TEST=true for --execute.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { persistPresenceProductIntegrationReport } from "../lib/ai-visibility/presence-product-certification.js";
import {
  buildStage1OpenAiPlan,
  executeStage1OpenAiExperiment,
  analyzeStage1OpenAiResults,
  writeStage1Artifacts,
  COST_CAP_USD,
} from "../lib/ai-visibility/validation/query-origin-regionalization-stage1-openai.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PLAN_ONLY = process.argv.includes("--plan-only");
const EXECUTE = process.argv.includes("--execute");

function resolveOpenAiKeyForStage1() {
  const openaiKey = String(process.env.OPENAI_API_KEY || "").trim();
  const fddKey = String(process.env.FDD_INTELLIGENCE_MODEL_API_KEY || "").trim();
  if (!openaiKey && fddKey) {
    process.env.OPENAI_API_KEY = fddKey;
  }
  return Boolean(String(process.env.OPENAI_API_KEY || "").trim());
}

async function main() {
  // Part A — product integration (no provider calls)
  const { report: productReport, path: productPath } =
    persistPresenceProductIntegrationReport();
  console.log("PRESENCE_PRODUCT_INTEGRATION_COMPLETE");
  console.log(`CERTIFICATION_STATUS: ${productReport.CERTIFICATION_STATUS}`);
  console.log(`ENABLED: ${(productReport.ENABLED || []).join(", ")}`);
  console.log(`BLOCKED: ${(productReport.BLOCKED || []).join(", ")}`);
  console.log(`wrote ${path.relative(ROOT, productPath)}`);
  console.log("");

  // Update regionalization plan status marker
  const planPath = path.join(
    ROOT,
    "data/ai-visibility/validation/query-origin-regionalization-experiment-v1-plan.json"
  );
  if (fs.existsSync(planPath)) {
    const doc = JSON.parse(fs.readFileSync(planPath, "utf8"));
    doc.status = "READY_FOR_STAGE_1_OPENAI_REGIONALIZATION_EXPERIMENT";
    doc.executionAllowed = true;
    doc.blocksOn = null;
    doc.note =
      "Presence Holdout v3 PRODUCTION_VALIDATED. Stage 1 OpenAI execution authorized when plan READY_TO_RUN=YES.";
    doc.updatedAt = new Date().toISOString();
    fs.writeFileSync(planPath, JSON.stringify(doc, null, 2) + "\n", "utf8");
  }

  const plan = buildStage1OpenAiPlan();
  const stage1PlanOut = path.join(
    ROOT,
    "data/ai-visibility/validation/query-origin-regionalization-stage1-openai-plan.json"
  );
  const planForDisk = { ...plan };
  delete planForDisk.slots; // keep slots in memory only for execute; slim plan on disk
  planForDisk.slotCount = plan.slots.length;
  fs.writeFileSync(stage1PlanOut, JSON.stringify(planForDisk, null, 2) + "\n", "utf8");

  console.log("QUERY_ORIGIN_STAGE1_OPENAI_PLAN_READY");
  console.log(`ASSET_GEOGRAPHY: ${plan.ASSET_GEOGRAPHY}`);
  console.log(`ORIGINS: ${plan.ORIGINS.join(", ")}`);
  console.log(`PROMPT_FAMILIES: ${plan.PROMPT_FAMILIES.join(" | ")}`);
  console.log(`LANGUAGES: ${plan.LANGUAGES.join(", ")}`);
  console.log(`REPEAT_COUNT: ${plan.REPEAT_COUNT}`);
  console.log(`PLANNED_CALLS: ${plan.PLANNED_CALLS}`);
  console.log(`ESTIMATED_COST: $${plan.ESTIMATED_COST}`);
  console.log(`COST_CAP: $${plan.COST_CAP}`);
  console.log(`MODEL: ${plan.MODEL}`);
  console.log(`EXECUTION_SEED: ${plan.EXECUTION_SEED}`);
  console.log(`READY_TO_RUN: ${plan.READY_TO_RUN}`);
  if (plan.stopReasons?.length) console.log(`stopReasons: ${plan.stopReasons.join(", ")}`);
  console.log(`wrote ${path.relative(ROOT, stage1PlanOut)}`);

  if (PLAN_ONLY || !EXECUTE) {
    if (plan.READY_TO_RUN === "YES" && !EXECUTE) {
      console.log("\nPlan ready within cap. Re-run with --execute to run provider calls.");
    }
    return;
  }

  if (plan.READY_TO_RUN !== "YES") {
    console.error("NOT READY — aborting execute");
    process.exit(2);
  }
  if (plan.ESTIMATED_COST > COST_CAP_USD) {
    console.error("COST CAP EXCEEDED — aborting");
    process.exit(2);
  }
  if (!resolveOpenAiKeyForStage1()) {
    console.error(
      "OPENAI_API_KEY_MISSING — aborting before provider calls (also checked FDD_INTELLIGENCE_MODEL_API_KEY)"
    );
    process.exit(2);
  }
  process.env.AI_VISIBILITY_ENABLED = process.env.AI_VISIBILITY_ENABLED || "true";
  process.env.AI_VISIBILITY_LIVE_TEST = "true";
  process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS =
    process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || "180000";
  console.log("OPENAI_API_KEY_RESOLVED: PRESENT");

  console.log("\nExecuting Stage 1 OpenAI regionalization...");
  const exec = await executeStage1OpenAiExperiment({ plan, forceLive: false });
  if (!exec.ok) {
    console.error(JSON.stringify(exec, null, 2));
    process.exit(2);
  }

  const analysis = analyzeStage1OpenAiResults(exec.observations, {
    plannedCalls: plan.PLANNED_CALLS,
  });
  const { resultsPath, summaryPath, results } = writeStage1Artifacts({
    plan,
    runManifest: exec.runManifest,
    observations: exec.observations,
    analysis,
  });

  console.log("\nQUERY_ORIGIN_STAGE1_OPENAI_COMPLETE");
  console.log(`OBSERVATIONS: ${results.OBSERVATIONS}`);
  console.log(`SUCCESSFUL_CALLS: ${results.SUCCESSFUL_CALLS}`);
  console.log(`FAILED_CALLS: ${results.FAILED_CALLS}`);
  console.log(`ACTUAL_COST: $${results.ACTUAL_COST}`);
  console.log(`MODEL: ${results.MODEL}`);
  console.log(`DECISION: ${analysis.decision}`);
  console.log(`NEXT: ${analysis.nextStep}`);
  console.log(`wrote ${path.relative(ROOT, resultsPath)}`);
  console.log(`wrote ${path.relative(ROOT, summaryPath)}`);
  if (analysis.inconclusive) {
    console.error(
      "\nSTAGE_1_INCONCLUSIVE — do not treat as productization evidence; fix provider failures and re-run."
    );
    process.exit(3);
  }
  console.log("\nPRESENCE_PRODUCT_INTEGRATION_AND_QUERY_ORIGIN_STAGE1_COMPLETE");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
