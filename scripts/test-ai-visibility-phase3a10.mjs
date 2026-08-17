#!/usr/bin/env node
/**
 * Phase 3A.10 — Showcase monitoring dry-run + multi-provider portability tests.
 * No provider calls.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildWave1ShowcaseDryRunPlan,
  buildWave1ExecutionFingerprint,
  WAVE1_PEER_SET_ID,
  WAVE1_RETRY_POLICY,
  WAVE1_COST_EVIDENCE,
  WAVE1_BASELINE_SERIES_ID,
} from "../lib/ai-visibility/wave1-showcase-plan.js";
import { buildOpenAiVisibilityRequest } from "../lib/ai-visibility/providers/openai.js";
import {
  normalizeVisibilityProviderResponse,
  listNormalizedProviderContractFields,
} from "../lib/ai-visibility/providers/normalized-response.js";
import {
  resolveAiVisibilityStoreRoot,
  WAVE1_ROOT,
  PHASE2E_ROOT,
} from "../lib/ai-visibility/storage/resolve-store-root.js";
import {
  isRetryableProviderError,
  isAuthProviderError,
} from "../lib/ai-visibility/execution-batch.js";
import { ProviderError } from "../lib/ai-visibility/providers/base-provider.js";
import { PEER_SET_ID_V1, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import { METRIC_VERSION } from "../lib/ai-visibility/config.js";
import { ACTIVE_SHOWCASE_INTENTS } from "../lib/ai-visibility/showcase-intents.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

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

console.log("\nAI Visibility Phase 3A.10 — Showcase Monitoring Dry Run\n");

const plan = buildWave1ShowcaseDryRunPlan();

test("prompt matrix — 84 / 60 EN / 24 ES / 12 per slot / 14 per intent", () => {
  assert.equal(plan.ok, true, plan.errors.join("; "));
  assert.equal(plan.PROMPT_LIBRARY.LOADED, 84);
  assert.equal(plan.PROMPT_LIBRARY.EN, 60);
  assert.equal(plan.PROMPT_LIBRARY.ES, 24);
  for (const k of [
    "GLOBAL_EN",
    "CALA_EN",
    "CALA_ES",
    "EUROPE_EN",
    "NORTH_AMERICA_EN",
    "MEXICO_EN",
    "MEXICO_ES",
  ]) {
    assert.equal(plan.MATRIX[k], 12, k);
  }
  for (const intent of ACTIVE_SHOWCASE_INTENTS) {
    assert.equal(plan.INTENT_DENSITY[intent], 14, intent);
  }
});

test("semantic pairs — 24 valid bilingual pairs", () => {
  assert.equal(plan.SEMANTIC_PAIRS.TOTAL, 24);
  assert.equal(plan.SEMANTIC_PAIRS.CALA, 12);
  assert.equal(plan.SEMANTIC_PAIRS.MEXICO, 12);
  assert.equal(plan.SEMANTIC_PAIRS.INVALID.length, 0);
});

test("peer v2 exact — 15 brands; no mutation; v1 preserved", () => {
  assert.equal(plan.PEER.ID, PEER_SET_ID_V2);
  assert.equal(WAVE1_PEER_SET_ID, PEER_SET_ID_V2);
  assert.equal(plan.PEER.COUNT, 15);
  assert.equal(plan.PEER.VALID, true);
  assert.ok(plan.PEER.FINGERPRINT);
  const batch = JSON.parse(
    fs.readFileSync(
      path.join(root, "data/ai-visibility/runtime/phase2e/batches/aiv_batch_20260813_d14b3e80.json"),
      "utf8"
    )
  );
  assert.equal(batch.peerSetId, PEER_SET_ID_V1);
});

test("companies — four showcase portfolios; no duplicate runs", () => {
  for (const key of ["marriott", "hilton", "choice", "ihg"]) {
    assert.ok(plan.COMPANIES[key], key);
    assert.equal(plan.COMPANIES[key].DUPLICATE_PROVIDER_RUN_REQUIRED, false);
    assert.equal(plan.COMPANIES[key].PEER_DATASET_REUSED, true);
  }
  assert.equal(plan.COMPANIES.ihg.PORTFOLIO_COUNT, 5);
});

test("84 unique fingerprints; no collisions; schema includes language/peer/metric", () => {
  assert.equal(plan.FINGERPRINTS.UNIQUE, 84);
  assert.equal(plan.FINGERPRINTS.COLLISIONS, 0);
  assert.ok(plan.FINGERPRINTS.SCHEMA.includes("language"));
  assert.ok(plan.FINGERPRINTS.SCHEMA.includes("peerSetVersion"));
  const a = buildWave1ExecutionFingerprint({
    promptId: "a",
    promptVersion: "1",
    geographyKey: "CALA",
    language: "en",
    intent: "Conversion",
    promptFamily: "f1",
  });
  const b = buildWave1ExecutionFingerprint({
    promptId: "a",
    promptVersion: "1",
    geographyKey: "CALA",
    language: "es",
    intent: "Conversion",
    promptFamily: "f1",
  });
  assert.notEqual(a.fingerprint, b.fingerprint);
});

test("OpenAI requests buildable for all 84; no live calls", () => {
  let n = 0;
  for (const exec of plan.EXECUTIONS) {
    const built = buildOpenAiVisibilityRequest({
      prompt: { text: exec.promptText, promptId: exec.promptId },
      model: "gpt-5.6",
    });
    assert.equal(built.ok, true);
    assert.equal(built.LIVE_PROVIDER_CALL, false);
    assert.equal(built.body.model, "gpt-5.6");
    assert.ok(built.body.tools?.some((t) => t.type === "web_search"));
    n += 1;
  }
  assert.equal(n, 84);
  assert.equal(plan.LIVE_PROVIDER_CALLS, 0);
});

test("normalized provider contract; metrics not OpenAI-field-dependent", () => {
  const fields = listNormalizedProviderContractFields();
  assert.ok(fields.includes("rawText"));
  assert.ok(fields.includes("rawArtifactUri"));
  const norm = normalizeVisibilityProviderResponse(
    { provider: "openai", model: "x", text: "hi", citations: [], raw: { id: "1" } },
    { promptId: "p", language: "en", geography: "GLOBAL" }
  );
  assert.equal(norm.status, "completed");
  assert.equal(norm.raw.id, "1");
  const metricsSrc = fs.readFileSync(path.join(root, "lib/ai-visibility/metrics.js"), "utf8");
  assert.ok(!/openai\.com|url_citation|responses\.create/.test(metricsSrc));
});

test("retry / cost budgets bounded; provider failure ≠ brand absence", () => {
  assert.equal(WAVE1_RETRY_POLICY.plannedCalls, 84);
  assert.equal(WAVE1_RETRY_POLICY.maxAttemptsPerCall, 2);
  assert.equal(WAVE1_RETRY_POLICY.maxTotalAttempts, 168);
  assert.ok(WAVE1_COST_EVIDENCE.RECOMMENDED_HARD_CAP_USD >= WAVE1_COST_EVIDENCE.HIGH);
  const e503 = new ProviderError("x", { type: "upstream_error", status: 503, retryable: true });
  assert.equal(isRetryableProviderError(e503), true);
  const auth = new ProviderError("x", { type: "auth_error", status: 401, retryable: false });
  assert.equal(isAuthProviderError(auth), true);
});

test("Wave-1 storage namespace isolated from Phase 2E", () => {
  const w1 = resolveAiVisibilityStoreRoot({ wave1: true });
  assert.equal(w1.rootDir, WAVE1_ROOT);
  assert.equal(w1.wave1Namespace, true);
  assert.notEqual(WAVE1_ROOT, PHASE2E_ROOT);
  assert.ok(WAVE1_BASELINE_SERIES_ID.includes("wave1"));
  assert.equal(METRIC_VERSION, "ai_visibility_metrics_v1");
});

test("no All Languages; no silent language fallback in fingerprints", () => {
  for (const e of plan.EXECUTIONS) {
    assert.ok(e.language === "en" || e.language === "es");
  }
  assert.equal(
    plan.EXECUTIONS.filter((e) => e.language === "es" && e.slot === "GLOBAL_EN").length,
    0
  );
});

console.log(`\nPhase 3A.10 results: ${passed} passed, ${failed} failed`);
console.log("LIVE_PROVIDER_CALLS: 0");
if (failed) process.exit(1);
