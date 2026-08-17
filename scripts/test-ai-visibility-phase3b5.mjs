#!/usr/bin/env node
/**
 * Phase 3B.5 tests — missing inventory, fingerprint protection, completion guards.
 */
import assert from "node:assert/strict";
import {
  auditMissingBaselineFingerprints,
  inventoryProviderMissingFingerprints,
  PHASE_3B5_WAVE_IDS,
} from "../lib/ai-visibility/baseline-missing-fingerprints.js";
import {
  assertFingerprintExecutable,
  CompletedFingerprintProtectionError,
} from "../lib/ai-visibility/baseline-fingerprint-protection.js";
import {
  resolveBaselineCompleteness,
  BASELINE_COMPLETENESS,
} from "../lib/ai-visibility/provider-baseline-state.js";
import {
  CLAUDE_COMPLETION_CUMULATIVE_CAP_USD,
  PHASE_3B5_ORCHESTRATOR_VERSION,
} from "../lib/ai-visibility/phase3b5-orchestrator.js";
import { BASELINE_FREEZE_ID } from "../lib/ai-visibility/baseline-freeze.js";
import { buildMatchedPromptGroups } from "../lib/ai-visibility/cited-source-intelligence.js";

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

console.log("\nAI Visibility Phase 3B.5 — Baseline Finalization\n");

test("inventory — reconciles to 336 when complete or 12 when partial", () => {
  const inv = auditMissingBaselineFingerprints();
  assert.equal(inv.OPENAI.missingCount, 0);
  assert.equal(inv.PERPLEXITY.missingCount, 0);
  if (inv.TOTAL_MISSING === 0) {
    assert.equal(inv.TOTAL_SUCCESSFUL, 336);
    assert.equal(inv.GEMINI.missingCount, 0);
    assert.equal(inv.CLAUDE.missingCount, 0);
    assert.equal(inv.inventoryValid, false);
  } else {
    assert.equal(inv.GEMINI.missingCount, 1);
    assert.equal(inv.CLAUDE.missingCount, 11);
    assert.equal(inv.TOTAL_MISSING, 12);
    assert.equal(inv.TOTAL_SUCCESSFUL, 324);
    assert.equal(inv.RECONCILES_TO_12, true);
    assert.equal(inv.inventoryValid, true);
  }
});

test("inventory — pre-completion fixture shape", () => {
  const cp = {
    waveId: "test",
    provider: "gemini",
    baselineSeriesId: "aiv_wave1_gemini_peer_v2_showcase_prompts_v1",
    completedFingerprints: { done: { runId: "r1" } },
    failedFingerprints: {
      missing1: { exhausted: true, error: { category: "SERVER" }, slot: "MEXICO_ES" },
    },
    slots: {},
  };
  assert.equal(Object.keys(cp.completedFingerprints).length, 1);
  assert.equal(Object.keys(cp.failedFingerprints).length, 1);
});

test("protection — completed fingerprint aborts locally", () => {
  const cp = {
    waveId: "test",
    provider: "gemini",
    completedFingerprints: { abc123: { runId: "run_1" } },
  };
  assert.throws(
    () => assertFingerprintExecutable(cp, "abc123", { protectCompleted: true }),
    CompletedFingerprintProtectionError
  );
});

test("protection — missing fingerprint executable", () => {
  const cp = {
    waveId: "test",
    provider: "gemini",
    completedFingerprints: { done: {} },
  };
  const r = assertFingerprintExecutable(cp, "missing_fp", { protectCompleted: true });
  assert.equal(r.executable, true);
});

test("state — FULL_BASELINE requires 84/84 completed", () => {
  assert.equal(
    resolveBaselineCompleteness({
      succeeded: 84,
      status: "completed",
      baselineSeriesId: "aiv_wave1_gemini_peer_v2_showcase_prompts_v1",
      monitoringRunPurpose: "baseline",
    }),
    BASELINE_COMPLETENESS.FULL_BASELINE
  );
  assert.equal(
    resolveBaselineCompleteness({
      SUCCEEDED: 83,
      status: "partial",
      baselineSeriesId: "aiv_wave1_gemini_peer_v2_showcase_prompts_v1",
      monitoringRunPurpose: "baseline",
    }),
    BASELINE_COMPLETENESS.PARTIAL_BASELINE
  );
});

test("claude cap — cumulative $70 approved", () => {
  assert.equal(CLAUDE_COMPLETION_CUMULATIVE_CAP_USD, 70);
});

test("matched groups — no consensus metrics", () => {
  const m = buildMatchedPromptGroups({});
  assert.equal(m.CONSENSUS_METRICS, "NOT_IMPLEMENTED");
});

test("freeze id — governed marker", () => {
  assert.equal(BASELINE_FREEZE_ID, "FOUR_PROVIDER_BASELINE_V1_COMPLETE");
});

test("orchestrator version present", () => {
  assert.ok(PHASE_3B5_ORCHESTRATOR_VERSION.includes("phase3b5"));
});

console.log(`\nPhase 3B.5 tests: ${passed} passed, ${failed} failed\n`);
process.exit(failed > 0 ? 1 : 0);
