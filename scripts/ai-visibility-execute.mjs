#!/usr/bin/env node
/**
 * AI Visibility governed cohort execution CLI (Phase 2E).
 *
 * Dry-run (default):
 *   node scripts/ai-visibility-execute.mjs --stakeholder brand --scope region --region CALA --dry-run
 *
 * Live (gated):
 *   node scripts/ai-visibility-phase2a-live-env.mjs scripts/ai-visibility-execute.mjs \
 *     --stakeholder brand --scope region --region CALA --execute
 *
 * Gates for --execute:
 *   AI_VISIBILITY_ENABLED=true
 *   AI_VISIBILITY_LIVE_TEST=true
 *   OPENAI_API_KEY present
 *   AI_VISIBILITY_MODEL=gpt-5.6
 */
import "../load-env.js";
import path from "path";
import { fileURLToPath } from "url";
import { executeAiVisibilityCohort } from "../lib/ai-visibility/execute-cohort.js";
import { createAiVisibilityStore } from "../lib/ai-visibility/storage/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function parseArgs(argv) {
  const out = {
    dryRun: true,
    execute: false,
    forceNewBatch: false,
    stakeholder: "brand",
    entityScope: null,
    scope: "region",
    region: null,
    country: null,
    intent: null,
    language: null,
    wave1Showcase: false,
    provider: "openai",
    peerSet: null,
    resume: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--dry-run") out.dryRun = true;
    else if (a === "--execute") {
      out.execute = true;
      out.dryRun = false;
    } else if (a === "--force-new-batch") out.forceNewBatch = true;
    else if (a === "--stakeholder") out.stakeholder = argv[++i];
    else if (a === "--entity-scope") out.entityScope = argv[++i];
    else if (a === "--scope") out.scope = argv[++i];
    else if (a === "--region") out.region = argv[++i];
    else if (a === "--country") out.country = argv[++i];
    else if (a === "--intent") out.intent = argv[++i];
    else if (a === "--language") out.language = argv[++i];
    else if (a === "--wave1-showcase") out.wave1Showcase = true;
    else if (a === "--provider") out.provider = argv[++i];
    else if (a === "--peer-set") out.peerSet = argv[++i];
    else if (a === "--resume") out.resume = true;
    else if (a === "--help" || a === "-h") out.help = true;
  }
  if (!out.entityScope) out.entityScope = out.stakeholder;
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log(`Usage:
  npm run ai-visibility:execute -- --stakeholder brand --scope region --region CALA --dry-run
  npm run ai-visibility:execute -- --wave1-showcase --dry-run
  npm run ai-visibility:execute -- --stakeholder brand --scope region --region CALA --execute

Flags:
  --stakeholder brand|operator
  --entity-scope brand|operator|both
  --scope global|region|country
  --region CALA|Europe|North America
  --country Mexico
  --language en|es
  --intent <Intent Territory>
  --wave1-showcase   Wave-1 84-prompt plan dry-run (no provider calls)
  --provider openai
  --peer-set <id>
  --dry-run | --execute
  --force-new-batch`);
    process.exit(0);
  }

  if (args.wave1Showcase) {
    if (args.execute) {
      // Phase 3A.11 — live Wave-1 orchestrator (use dedicated live-env bootstrap)
      const { executeWave1Showcase, preflightWave1LiveEnv } = await import(
        "../lib/ai-visibility/wave1-showcase-orchestrator.js"
      );
      const { buildWave1PostWaveAudit } = await import(
        "../lib/ai-visibility/wave1-post-wave-audit.js"
      );
      const preflight = preflightWave1LiveEnv();
      if (!preflight.LIVE_ENV_READY) {
        console.error(
          JSON.stringify(
            {
              ok: false,
              BUILD_STATUS: "BRAND_AI_VISIBILITY_PHASE_3A11_LIVE_OPENAI_SHOWCASE_WAVE_BLOCKED",
              reason: "preflight_failed",
              preflight,
              LIVE_PROVIDER_LOGICAL_CALLS: 0,
            },
            null,
            2
          )
        );
        process.exit(2);
      }
      const result = await executeWave1Showcase({ execute: true, resume: args.resume });
      if (result.mode === "blocked") {
        console.error(JSON.stringify(result, null, 2));
        process.exit(2);
      }
      const audit = await buildWave1PostWaveAudit({
        wave1Id: result.wave1Id,
        checkpoint: result.checkpoint,
        summary: result.summary,
      });
      console.log(
        JSON.stringify(
          {
            mode: "wave1-showcase-execute",
            WAVE1_ID: result.wave1Id,
            STATUS: result.summary.status,
            ACTIVATION_GATE: result.summary.activationGate?.RESULT || null,
            LOGICAL: result.summary.logical,
            COST: result.summary.cost,
            DATASET_STATUS: audit.DATASET_STATUS,
            BUILD_STATUS: audit.BUILD_STATUS,
            AIRTABLE_WRITES: 0,
          },
          null,
          2
        )
      );
      process.exit(
        String(audit.BUILD_STATUS || "").includes("BLOCKED")
          ? 2
          : String(audit.BUILD_STATUS || "").includes("PARTIAL")
            ? 0
            : 0
      );
    }
    const { buildWave1ShowcaseDryRunPlan } = await import(
      "../lib/ai-visibility/wave1-showcase-plan.js"
    );
    const { buildOpenAiVisibilityRequest } = await import(
      "../lib/ai-visibility/providers/openai.js"
    );
    const { WAVE1_ROOT } = await import("../lib/ai-visibility/storage/resolve-store-root.js");
    const plan = buildWave1ShowcaseDryRunPlan();
    let buildable = 0;
    for (const exec of plan.EXECUTIONS) {
      const built = buildOpenAiVisibilityRequest({
        prompt: { text: exec.promptText, promptId: exec.promptId },
        model: process.env.AI_VISIBILITY_MODEL || "gpt-5.6",
      });
      if (built.ok) buildable += 1;
    }
    console.log(
      JSON.stringify(
        {
          mode: "wave1-showcase-dry-run",
          DRY_RUN_PROVIDER_CALLS: 0,
          LIVE_PROVIDER_CALLS: 0,
          ok: plan.ok && buildable === 84,
          PROMPT_COUNT: plan.PROMPT_LIBRARY.LOADED,
          MATRIX: plan.MATRIX,
          PEER: plan.PEER,
          OPENAI_REQUESTS_BUILDABLE: buildable,
          FINGERPRINT_UNIQUE: plan.FINGERPRINTS.UNIQUE,
          COLLISIONS: plan.FINGERPRINTS.COLLISIONS,
          WAVE1_STORE_ROOT: WAVE1_ROOT,
          RECOMMENDED_HARD_CAP_USD: plan.COST.RECOMMENDED_HARD_CAP_USD,
          errors: plan.errors,
        },
        null,
        2
      )
    );
    process.exit(plan.ok && buildable === 84 ? 0 : 2);
  }

  // Default model for Phase 2E
  if (!process.env.AI_VISIBILITY_MODEL) {
    process.env.AI_VISIBILITY_MODEL = "gpt-5.6";
  }
  if (!process.env.AI_VISIBILITY_MAX_BATCH_COST_USD) {
    process.env.AI_VISIBILITY_MAX_BATCH_COST_USD = "5";
  }
  if (!process.env.AI_VISIBILITY_MAX_TEST_RUNS) {
    process.env.AI_VISIBILITY_MAX_TEST_RUNS = "20";
  }

  const store = createAiVisibilityStore();
  const storeRoot = store.rootDir;

  const result = await executeAiVisibilityCohort({
    stakeholder: args.stakeholder,
    entityScope: args.entityScope,
    geographyScope: args.scope,
    region: args.region,
    country: args.country,
    intentTerritories: args.intent || null,
    language: args.language || "en",
    dryRun: args.dryRun,
    execute: args.execute,
    forceNewBatch: args.forceNewBatch,
    store,
    promptMode: "airtable",
    provider: args.provider,
    peerSetId: args.peerSet || undefined,
  });

  if (result.mode === "dry-run") {
    console.log(
      JSON.stringify(
        {
          mode: "dry-run",
          DRY_RUN_PROVIDER_CALLS: 0,
          AIRTABLE_EXECUTION_WRITES: 0,
          PROMPT_COUNT: result.cohort.count,
          COHORT_FINGERPRINT: result.cohort.fingerprint,
          GEOGRAPHY_SCOPE: result.geographyScope,
          REGION: result.commercialRegion,
          COUNTRY: result.country,
          MONITORING_ELIGIBLE_ONLY: true,
          MODEL: result.model,
          PLANNED_RUNS: result.plannedRuns,
          ESTIMATED_MAX_COST_USD: result.estimatedMaxCostUsd,
          MAX_BATCH_COST_USD: result.maxBatchCostUsd,
          PREREQUISITES_OK: result.prerequisitesOk,
          PREREQUISITES: result.prerequisites,
          PROMPT_IDS: result.cohort.members.map((m) => `${m.promptId}@${m.version}`),
        },
        null,
        2
      )
    );
    process.exit(result.prerequisitesOk ? 0 : 2);
  }

  console.log(
    JSON.stringify(
      {
        mode: "execute",
        BATCH_ID: result.batch.batchId,
        STATUS: result.batch.status,
        HEALTH: result.batch.health,
        PLANNED_RUNS: result.summary.execution.planned,
        SUCCESSFUL_RUNS: result.summary.execution.successful,
        FAILED_RUNS: result.summary.execution.failed,
        RETRIES: result.summary.execution.retries,
        PROVIDER: result.summary.provider.name,
        MODEL: result.summary.provider.model,
        INPUT_TOKENS: result.summary.usage.inputTokens,
        OUTPUT_TOKENS: result.summary.usage.outputTokens,
        TOTAL_TOKENS: result.summary.usage.totalTokens,
        ESTIMATED_COST: result.summary.usage.estimatedCost,
        ELAPSED_MS: result.summary.elapsedMs,
        AIRTABLE_EXECUTION_WRITES: 0,
        PEER_SET_VALID: result.summary.peerSet.canonicalValid,
        SUMMARY_PATH: path.join(storeRoot, "summaries", `${result.batch.batchId}.json`),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(
    JSON.stringify(
      {
        error: err.message,
        code: err.code || null,
        health: err.health || null,
        batchId: err.batchId || null,
      },
      null,
      2
    )
  );
  process.exit(1);
});
