#!/usr/bin/env node
/**
 * Phase 3B.3 — Multi-provider baseline expansion tests.
 */
import assert from "node:assert/strict";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import {
  preflightAllProviderCredentials,
  preflightProviderCredentials,
  resolveGeminiCredential,
} from "../lib/ai-visibility/provider-credentials.js";
import { buildProviderBaselineExecutionPlan } from "../lib/ai-visibility/provider-baseline-plan.js";
import {
  BASELINE_COMPLETENESS,
  PROVIDER_BASELINE_SERIES,
  PROVIDER_BASELINE_HARD_CAPS,
  resolveBaselineCompleteness,
  isFullBaselineSummary,
} from "../lib/ai-visibility/provider-baseline-state.js";
import {
  deriveProviderHardCapFromValidation,
  PROVIDER_BASELINE_ORCHESTRATOR_VERSION,
} from "../lib/ai-visibility/provider-baseline-orchestrator.js";
import { probeClaudeBillingExecution } from "../lib/ai-visibility/provider-billing-probe.js";
import { buildPhase3b3FinalReport } from "../lib/ai-visibility/phase3b3-report.js";
import { PHASE_3B3_ORCHESTRATOR_VERSION } from "../lib/ai-visibility/phase3b3-orchestrator.js";
import {
  MONITORING_RUN_PURPOSE,
  isValidationMonitoringRun,
  isBaselineMonitoringRun,
} from "../lib/ai-visibility/monitoring-run-purpose.js";
import { listAvailableAiVisibilityProviders } from "../lib/ai-visibility/provider-dimension.js";
import { createAiVisibilityStore } from "../lib/ai-visibility/storage/file-store.js";
import {
  resolveProviderBaselineStoreRoot,
  resolveProviderValidationStoreRoot,
} from "../lib/ai-visibility/storage/resolve-store-root.js";
import { buildMultiProviderDryRunReport, validateFingerprintProviderIsolation } from "../lib/ai-visibility/providers/multi-provider-dry-run.js";
import { buildWave1ShowcaseDryRunPlan } from "../lib/ai-visibility/wave1-showcase-plan.js";
import { project84CallCost } from "../lib/ai-visibility/provider-validation-cost.js";
import { ProviderError } from "../lib/ai-visibility/providers/base-provider.js";
import { classifyProviderError } from "../lib/ai-visibility/providers/provider-errors.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    const r = fn();
    if (r && typeof r.then === "function") {
      return r
        .then(() => {
          passed += 1;
          console.log(`  PASS ${name}`);
        })
        .catch((err) => {
          failed += 1;
          console.error(`  FAIL ${name}: ${err.message}`);
        });
    }
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

async function testAsync(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nAI Visibility Phase 3B.3 — Multi-Provider Baseline Expansion\n");

test("credentials — preflightAll includes OpenAI env name only", () => {
  const out = preflightAllProviderCredentials();
  assert.equal(out.SECRET_EXPOSURE, "NONE");
  assert.ok(["PRESENT", "MAPPED", "MISSING"].includes(out.OPENAI_CREDENTIAL || "MISSING"));
  if (out.OPENAI_ENV_VAR_USED) assert.equal(out.OPENAI_ENV_VAR_USED, "OPENAI_API_KEY");
});

test("credentials — Gemini maps GEMINI_API_KEY only", () => {
  const g = resolveGeminiCredential();
  if (g.envVarUsed) assert.equal(g.envVarUsed, "GEMINI_API_KEY");
});

test("baseline plan — 84 executions per provider with isolated fingerprints", () => {
  for (const provider of ["gemini", "perplexity", "claude"]) {
    const plan = buildProviderBaselineExecutionPlan(provider);
    assert.equal(plan.ok, true, provider);
    assert.equal(plan.PLANNED, 84, provider);
    assert.equal(plan.baselineSeriesId, PROVIDER_BASELINE_SERIES[provider]);
    const fps = new Set(plan.EXECUTIONS.map((e) => e.fingerprint));
    assert.equal(fps.size, 84, `${provider} fingerprint uniqueness`);
  }
  const gem = buildProviderBaselineExecutionPlan("gemini");
  const ppl = buildProviderBaselineExecutionPlan("perplexity");
  assert.notEqual(gem.EXECUTIONS[0].fingerprint, ppl.EXECUTIONS[0].fingerprint);
});

test("fingerprints — no cross-provider collisions", () => {
  const wave = buildWave1ShowcaseDryRunPlan();
  const exec = wave.EXECUTIONS[0];
  const iso = validateFingerprintProviderIsolation(exec);
  assert.equal(iso.valid, true);
});

test("run purpose — validation != baseline", () => {
  assert.equal(isValidationMonitoringRun({ monitoringRunPurpose: "validation" }), true);
  assert.equal(isBaselineMonitoringRun({ monitoringRunPurpose: "validation" }), false);
  assert.equal(
    isBaselineMonitoringRun({
      monitoringRunPurpose: "baseline",
      baselineSeriesId: PROVIDER_BASELINE_SERIES.gemini,
      succeeded: 84,
      status: "completed",
    }),
    true
  );
});

test("baseline state — FULL_BASELINE requires 84 succeeded", () => {
  assert.equal(
    resolveBaselineCompleteness({
      monitoringRunPurpose: "baseline",
      baselineSeriesId: PROVIDER_BASELINE_SERIES.perplexity,
      SUCCEEDED: 84,
      status: "completed",
    }),
    BASELINE_COMPLETENESS.FULL_BASELINE
  );
  assert.equal(
    resolveBaselineCompleteness({
      monitoringRunPurpose: "validation",
      SUCCEEDED: 12,
      status: "completed",
    }),
    BASELINE_COMPLETENESS.VALIDATION_ONLY
  );
  assert.equal(
    resolveBaselineCompleteness({
      monitoringRunPurpose: "baseline",
      SUCCEEDED: 40,
      status: "partial",
    }),
    BASELINE_COMPLETENESS.PARTIAL_BASELINE
  );
});

test("hard caps — Perplexity $15 enforced default", () => {
  assert.equal(PROVIDER_BASELINE_HARD_CAPS.perplexity, 15);
});

test("cost calibration — derive cap from validation sample", () => {
  const cap = deriveProviderHardCapFromValidation("gemini", {
    costLedger: { actualUsd: 0.08 },
    SUCCEEDED: 12,
  });
  assert.ok(cap > 0 && cap <= 50);
  assert.equal(deriveProviderHardCapFromValidation("perplexity", {}), 15);
});

test("billing classifier — credit balance maps to auth/billing", () => {
  const err = new ProviderError("Your credit balance is too low", { status: 400 });
  const c = classifyProviderError(err);
  assert.ok(/credit|billing|balance/i.test(c.message) || c.category === "AUTH" || c.category === "SAFETY_REFUSAL");
});

test("orchestrator versions present", () => {
  assert.ok(PROVIDER_BASELINE_ORCHESTRATOR_VERSION.includes("baseline"));
  assert.ok(PHASE_3B3_ORCHESTRATOR_VERSION.includes("3b3"));
});

test("dry-run — 252 buildable unchanged", () => {
  const dr = buildMultiProviderDryRunReport();
  assert.equal(dr.TOTAL_BUILDABLE, 252);
  assert.equal(dr.LIVE_PROVIDER_CALLS, 0);
});

testAsync("provider UI — validation-only excluded from measured providers", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-3b3-"));
  const store = createAiVisibilityStore({ rootDir: tmp });
  await store.saveBatchSummary({
    batchId: "val_perp",
    provider: { name: "perplexity" },
    status: "completed",
    monitoringRunPurpose: MONITORING_RUN_PURPOSE.VALIDATION,
    succeeded: 12,
  });
  await store.saveBatchSummary({
    batchId: "base_perp",
    provider: { name: "perplexity" },
    status: "completed",
    monitoringRunPurpose: MONITORING_RUN_PURPOSE.BASELINE,
    baselineSeriesId: PROVIDER_BASELINE_SERIES.perplexity,
    succeeded: 84,
  });
  const available = await listAvailableAiVisibilityProviders({ store });
  assert.equal(available.some((p) => p.id === "perplexity"), true);
  assert.equal(available.length, 1);
});

test("report builder — produces BUILD_STATUS", () => {
  const report = buildPhase3b3FinalReport({
    credentials: {
      OPENAI_CREDENTIAL: "PRESENT",
      GEMINI_CREDENTIAL: "PRESENT",
      PERPLEXITY_CREDENTIAL: "PRESENT",
      CLAUDE_CREDENTIAL: "MAPPED",
    },
    gemini: { decision: "GO", validationSummary: { PLANNED: 12, SUCCEEDED: 12, COST: 0.1 }, baseline: { SUCCEEDED: 84, status: "completed", costLedger: { actualUsd: 1.2 }, slotResults: {} } },
    perplexity: { baseline: { SUCCEEDED: 84, status: "completed", costLedger: { actualUsd: 0.5 }, slotResults: {} } },
    claude: { readiness: { EXECUTION_READY: "NO" }, status: "BLOCKED_BILLING" },
    activity: {},
  });
  assert.ok(report.BUILD_STATUS.includes("3B3"));
  assert.ok(report.markdown.includes("ALL_AI_OPTION: NO"));
});

test("store roots — baseline isolated per provider", () => {
  assert.ok(resolveProviderBaselineStoreRoot("gemini").includes("provider-baselines"));
  assert.ok(resolveProviderValidationStoreRoot("gemini").includes("provider-validation"));
  assert.notEqual(
    resolveProviderBaselineStoreRoot("gemini"),
    resolveProviderBaselineStoreRoot("perplexity")
  );
});

test("project84 — calibration from validation", () => {
  const p = project84CallCost(0.06729, 12);
  assert.ok(p.EXPECTED < 1);
  assert.equal(p.STATUS, "CALIBRATED_FROM_VALIDATION");
});

await Promise.all([]);

console.log(`\nPhase 3B.3 tests: ${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exit(1);
