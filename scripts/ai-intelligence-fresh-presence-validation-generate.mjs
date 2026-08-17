#!/usr/bin/env node
/**
 * Fresh Presence validation candidate generation.
 * --plan-only : write/print plan, no provider calls
 * --execute   : live provider calls (requires AI_VISIBILITY_LIVE_TEST=true)
 * --build-candidates-only : rebuild candidates from saved responses
 * --openai-batch : OpenAI-only batch (presence_validation_openai_batch_v1)
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  selectPresenceValidationPrompts,
  detectProviderAvailability,
  buildExecutionSlots,
  buildOpenAiPresenceValidationPlan,
  generatePresenceValidationResponses,
  generateOpenAiPresenceValidationResponses,
  buildPresenceValidationCandidates,
  presenceValidationPaths,
  OPENAI_BATCH_ID,
} from "../lib/ai-visibility/validation/presence-validation-candidates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const PLAN_ONLY = process.argv.includes("--plan-only");
const EXECUTE = process.argv.includes("--execute");
const BUILD_ONLY = process.argv.includes("--build-candidates-only");
const OPENAI_BATCH = process.argv.includes("--openai-batch");

async function main() {
  if (OPENAI_BATCH) {
    return runOpenAiBatch();
  }

  const availability = detectProviderAvailability();
  const prompts = selectPresenceValidationPrompts();
  const slots = buildExecutionSlots(prompts, availability);
  const EST = 0.25;
  const CAP = 30;
  const estimated = slots.length * EST;

  const plan = {
    phase: "AI_INTELLIGENCE_FRESH_PRESENCE_VALIDATION_PLAN_READY",
    batchId: "presence_validation_candidate_batch_v1",
    PLANNED_RESPONSES: slots.length,
    EXPECTED_CANDIDATES: "130–150",
    BY_PROVIDER: {
      openai: slots.filter((s) => s.provider === "openai").length,
      gemini: slots.filter((s) => s.provider === "gemini").length,
      perplexity: slots.filter((s) => s.provider === "perplexity").length,
      claude: slots.filter((s) => s.provider === "claude").length,
    },
    BY_LANGUAGE: {
      en: slots.filter((s) => s.prompt.language === "en").length,
      es: slots.filter((s) => s.prompt.language === "es").length,
    },
    BY_GEOGRAPHY: slots.reduce((acc, s) => {
      const g = s.prompt.geography || "Global";
      acc[g] = (acc[g] || 0) + 1;
      return acc;
    }, {}),
    ESTIMATED_COST: estimated,
    COST_CAP: CAP,
    providerAvailability: availability,
    openaiUnavailable: !availability.openai,
    LEAKAGE_CHECK_PLAN: [
      "text hash vs golden sets",
      "responseId uniqueness",
      "caseId not in prior fixtures",
    ],
    READY_TO_RUN: estimated <= CAP && (availability.gemini || availability.perplexity || availability.claude) ? "YES" : "NO",
    promptIds: prompts.map((p) => p.promptId),
  };

  const planPath = presenceValidationPaths().plan;
  fs.mkdirSync(path.dirname(planPath), { recursive: true });
  fs.writeFileSync(planPath, JSON.stringify(plan, null, 2) + "\n");
  console.log(JSON.stringify(plan, null, 2));

  if (PLAN_ONLY || (!EXECUTE && !BUILD_ONLY)) {
    console.log("\nPlan written. Pass --execute to run live generation.");
    return;
  }

  if (BUILD_ONLY) {
    const dir = presenceValidationPaths().responsesDir;
    const files = fs.existsSync(dir) ? fs.readdirSync(dir).filter((f) => f.endsWith(".json")) : [];
    const okResponses = files
      .map((f) => JSON.parse(fs.readFileSync(path.join(dir, f), "utf8")))
      .filter((r) => r.status === "ok");
    const cand = buildPresenceValidationCandidates(okResponses);
    writeReadyReport(cand, { COMPLETED_OK: okResponses.length });
    return;
  }

  if (plan.READY_TO_RUN !== "YES") {
    console.error("Not ready to run — see plan");
    process.exit(2);
  }

  process.env.AI_VISIBILITY_LIVE_TEST = process.env.AI_VISIBILITY_LIVE_TEST || "true";

  const gen = await generatePresenceValidationResponses({ resume: true, requireLiveFlag: true });
  if (!gen.ok) {
    console.error(JSON.stringify(gen, null, 2));
    process.exit(2);
  }

  const cand = buildPresenceValidationCandidates(gen.okResponses);
  writeReadyReport(cand, gen.manifest);
}

async function runOpenAiBatch() {
  const plan = buildOpenAiPresenceValidationPlan();
  const planPath = presenceValidationPaths().openaiPlan;
  fs.mkdirSync(path.dirname(planPath), { recursive: true });
  const planOut = { ...plan };
  delete planOut.slots;
  fs.writeFileSync(planPath, JSON.stringify(planOut, null, 2) + "\n");
  console.log(JSON.stringify(planOut, null, 2));

  if (PLAN_ONLY || (!EXECUTE && !BUILD_ONLY)) {
    console.log("\nOpenAI plan written. Pass --openai-batch --execute to run.");
    return;
  }

  if (BUILD_ONLY) {
    const dir = presenceValidationPaths().responsesDir;
    const files = fs.existsSync(dir)
      ? fs.readdirSync(dir).filter((f) => f.startsWith(`${OPENAI_BATCH_ID}__`) && f.endsWith(".json"))
      : [];
    const okResponses = files
      .map((f) => JSON.parse(fs.readFileSync(path.join(dir, f), "utf8")))
      .filter((r) => r.status === "ok" && r.provider === "openai");
    const cand = buildPresenceValidationCandidates(okResponses, {
      mergeWithExisting: true,
      replaceBatchId: OPENAI_BATCH_ID,
      primaryQueueTarget: Math.max(48, okResponses.length * 2),
    });
    writeOpenAiReadyReport(cand, { COMPLETED_OK: okResponses.length }, okResponses);
    return;
  }

  if (plan.READY_TO_RUN !== "YES") {
    console.error("OpenAI batch not ready to run — see plan");
    process.exit(2);
  }

  process.env.AI_VISIBILITY_LIVE_TEST = process.env.AI_VISIBILITY_LIVE_TEST || "true";

  const gen = await generateOpenAiPresenceValidationResponses({
    resume: true,
    requireLiveFlag: true,
  });
  if (!gen.ok) {
    console.error(JSON.stringify(gen, null, 2));
    process.exit(2);
  }

  const cand = buildPresenceValidationCandidates(gen.okResponses, {
    mergeWithExisting: true,
    replaceBatchId: OPENAI_BATCH_ID,
    primaryQueueTarget: Math.max(48, gen.okResponses.length * 2),
  });
  writeOpenAiReadyReport(cand, gen.manifest, gen.okResponses);
}

function writeReadyReport(cand, manifest) {
  const report = {
    phase: "AI_INTELLIGENCE_FRESH_PRESENCE_CANDIDATES_READY",
    NEW_RESPONSES: manifest?.COMPLETED_OK ?? null,
    TOTAL_CANDIDATES: cand.TOTAL_CANDIDATES,
    PRIMARY_REVIEW_QUEUE: cand.PRIMARY_REVIEW_QUEUE,
    POTENTIAL_TRUE: cand.POTENTIAL_TRUE,
    POTENTIAL_FALSE: cand.POTENTIAL_FALSE,
    PROVIDER_COVERAGE: countBy(cand.cases, "provider"),
    LANGUAGE_COVERAGE: countBy(cand.cases, "language"),
    GEOGRAPHY_COVERAGE: countBy(cand.cases, "geography"),
    DUPLICATES_REJECTED: cand.DUPLICATES_REJECTED,
    LEAKAGE_REJECTED: cand.LEAKAGE_REJECTED,
    HUMAN_REVIEW_ROUTE: "/ai-intelligence-presence-validation-review",
    HUMAN_REVIEW_REQUIRED: "YES",
    HOLDOUT_V2_SCORED: false,
    SYSTEM_SUGGESTION_IS_NOT_GROUND_TRUTH: true,
    generationManifest: manifest || null,
  };
  const out = presenceValidationPaths().afterGenerationReport;
  fs.writeFileSync(out, JSON.stringify(report, null, 2) + "\n");
  console.log(JSON.stringify(report, null, 2));
  console.log(`\nWrote ${out}`);
  console.log("STOP for human review.");
}

function writeOpenAiReadyReport(cand, manifest, okResponses) {
  const openaiCases = (cand.cases || []).filter(
    (c) => c.batchId === OPENAI_BATCH_ID || c.provider === "openai"
  );
  const primaryOpenAi = openaiCases.filter((c) => c.primaryReviewQueue);
  const report = {
    phase: "OPENAI_FRESH_PRESENCE_CANDIDATES_READY",
    status: "OPENAI_FRESH_PRESENCE_VALIDATION_BATCH_READY",
    batchId: OPENAI_BATCH_ID,
    NEW_OPENAI_RESPONSES: manifest?.COMPLETED_OK ?? okResponses?.length ?? null,
    TOTAL_OPENAI_CANDIDATES: openaiCases.length,
    PRIMARY_REVIEW_QUEUE: primaryOpenAi.length,
    POTENTIAL_PRESENT: openaiCases.filter((c) => c.candidateType === "PRESENCE_TRUE").length,
    POTENTIAL_NOT_PRESENT: openaiCases.filter((c) => c.candidateType === "PRESENCE_FALSE")
      .length,
    LANGUAGE_COVERAGE: countBy(openaiCases, "language"),
    GEOGRAPHY_COVERAGE: countBy(openaiCases, "geography"),
    DUPLICATES_REJECTED: cand.DUPLICATES_REJECTED,
    LEAKAGE_REJECTED: cand.LEAKAGE_REJECTED,
    ACTUAL_COST: manifest?.ESTIMATED_SPENT_USD ?? null,
    ERRORS: manifest?.ERRORS ?? 0,
    REJECTED: manifest?.REJECTED ?? 0,
    HUMAN_REVIEW_ROUTE: "/ai-intelligence-presence-validation-review",
    EXPORT_READY: openaiCases.length > 0 ? "YES" : "NO",
    HUMAN_REVIEW_REQUIRED: "YES",
    HOLDOUT_V2_FINAL_FREEZE_ALLOWED: "NO",
    NEXT_ACTION: "EXPORT_OPENAI_PRESENCE_REVIEW_BATCH",
    POOL_TOTAL_CANDIDATES: cand.TOTAL_CANDIDATES,
    POOL_UNIQUE_RESPONSES: cand.UNIQUE_RESPONSE_N,
    generationManifest: manifest || null,
  };
  const out = presenceValidationPaths().openaiAfterGenerationReport;
  fs.writeFileSync(out, JSON.stringify(report, null, 2) + "\n");
  // Update plan status
  const planPath = presenceValidationPaths().openaiPlan;
  if (fs.existsSync(planPath)) {
    const prev = JSON.parse(fs.readFileSync(planPath, "utf8"));
    fs.writeFileSync(
      planPath,
      JSON.stringify(
        {
          ...prev,
          status: "COMPLETED",
          READY_TO_RUN: "DONE",
          OPENAI_API_KEY_AVAILABLE: true,
          completedAt: new Date().toISOString(),
          afterGenerationReport: path.relative(ROOT, out).replace(/\\/g, "/"),
        },
        null,
        2
      ) + "\n"
    );
  }
  console.log(JSON.stringify(report, null, 2));
  console.log(`\nWrote ${out}`);
  console.log("STOP for export / ChatGPT-assisted review.");
}

function countBy(rows, key) {
  const out = {};
  for (const r of rows || []) {
    const k = r[key] || "unspecified";
    out[k] = (out[k] || 0) + 1;
  }
  return out;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
