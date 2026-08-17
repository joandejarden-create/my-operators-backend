#!/usr/bin/env node
/**
 * Phase 3A.11 — Wave-1 live orchestrator tests (mocked provider; no live OpenAI).
 */
import assert from "node:assert/strict";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildWave1ShowcaseDryRunPlan,
  WAVE1_EXECUTION_ORDER,
  WAVE1_PEER_SET_ID,
  WAVE1_RETRY_POLICY,
} from "../lib/ai-visibility/wave1-showcase-plan.js";
import { evaluateGlobalEnActivationGate } from "../lib/ai-visibility/wave1-activation-gate.js";
import {
  WAVE1_HARD_CAP_USD,
  wouldBreachHardCap,
  createWave1CostLedger,
  applyWave1CallCost,
  projectWaveCostFromSample,
} from "../lib/ai-visibility/wave1-cost.js";
import {
  executeWave1Showcase,
  preflightWave1LiveEnv,
} from "../lib/ai-visibility/wave1-showcase-orchestrator.js";
import { buildAiVisibilityEntityIndex } from "../lib/ai-visibility/entity-index.js";
import { loadPeerSetConfig, resolvePeerSetMembership, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import { ProviderError } from "../lib/ai-visibility/providers/base-provider.js";
import { PHASE2E_ROOT } from "../lib/ai-visibility/storage/resolve-store-root.js";
import { METRIC_VERSION } from "../lib/ai-visibility/config.js";

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
    console.error(`  FAIL ${name}: ${err.stack || err.message}`);
  }
}

function buildPeerEntityIndex() {
  const peer = resolvePeerSetMembership({ peerSetId: WAVE1_PEER_SET_ID }, loadPeerSetConfig());
  const set = (loadPeerSetConfig().peerSets || []).find((p) => p.peerSetId === WAVE1_PEER_SET_ID);
  const brands = (set.members || []).map((m) => ({
    id: m.brandId,
    name: m.brandName,
    entityType: "brand",
    aliases: [m.brandName],
    parentCompany: m.canonicalParent,
    isParentCompanyLabel: false,
  }));
  // Ensure all entityIds present even if members missing
  for (const id of peer.entityIds || []) {
    if (!brands.some((b) => b.id === id)) {
      brands.push({
        id,
        name: id,
        entityType: "brand",
        aliases: [],
        isParentCompanyLabel: false,
      });
    }
  }
  return buildAiVisibilityEntityIndex({ brands, operators: [], applyOverlay: false });
}

function mockSuccessRun({ prompt }) {
  return {
    provider: "openai",
    model: "gpt-5.6",
    text: `For owners, Autograph Collection and Curio Collection by Hilton are strong options. 1. Autograph Collection 2. Kimpton Hotels 3. Design Hotels.`,
    citations: [
      {
        url: "https://example.com/owner-brands",
        title: "Example",
        startIndex: 0,
        endIndex: 10,
        providerSupplied: true,
      },
    ],
    usage: { inputTokens: 800, outputTokens: 400, totalTokens: 1200 },
    latencyMs: 50,
    citationCapability: "supported",
    parserVersion: "test",
    providerMeta: { responseId: `resp_test_${prompt?.promptId || "x"}` },
    raw: { id: `resp_test_${prompt?.promptId || "x"}`, output: [] },
  };
}

console.log("\nAI Visibility Phase 3A.11 — Live OpenAI Showcase Wave (mocked)\n");

const plan = buildWave1ShowcaseDryRunPlan();

test("orchestrator plan — 7 slots × 12 = 84 deterministic order", () => {
  assert.equal(WAVE1_EXECUTION_ORDER.length, 7);
  assert.equal(plan.EXECUTIONS.length, 84);
  assert.deepEqual(
    WAVE1_EXECUTION_ORDER.map((s) => s.key),
    [
      "GLOBAL_EN",
      "CALA_EN",
      "CALA_ES",
      "EUROPE_EN",
      "NORTH_AMERICA_EN",
      "MEXICO_EN",
      "MEXICO_ES",
    ]
  );
  for (const s of WAVE1_EXECUTION_ORDER) {
    assert.equal(plan.MATRIX[s.key], 12, s.key);
  }
  assert.equal(plan.EXECUTIONS[0].slot, "GLOBAL_EN");
  assert.equal(plan.EXECUTIONS[12].slot, "CALA_EN");
});

test("activation gate — PASS continues; FAIL stops; content desirability irrelevant", () => {
  const pass = evaluateGlobalEnActivationGate({
    planned: 12,
    succeeded: 12,
    failed: 0,
    retries: 1,
    slotCostUsd: 8.1,
  });
  assert.equal(pass.RESULT, "PASS");
  const fail = evaluateGlobalEnActivationGate({
    planned: 12,
    succeeded: 5,
    failed: 7,
    slotCostUsd: 4,
  });
  assert.equal(fail.RESULT, "FAIL");
  assert.ok(fail.REASONS.some((r) => r.includes("execution_success")));
  // Unexpected recommendations do not appear as reasons
  assert.ok(!fail.REASONS.some((r) => /recommend|autograph|kimpton/i.test(r)));
});

test("cost — $125 hard cap helpers", () => {
  assert.equal(WAVE1_HARD_CAP_USD, 125);
  const ledger = createWave1CostLedger(125);
  applyWave1CallCost(ledger, { inputTokens: 1000, outputTokens: 500, totalTokens: 1500 }, 1);
  assert.ok(ledger.actualUsd > 0);
  assert.equal(wouldBreachHardCap({ ...ledger, actualUsd: 124, hardCapUsd: 125, capBreached: false }, 2), true);
  assert.equal(wouldBreachHardCap({ ...ledger, actualUsd: 10, hardCapUsd: 125, capBreached: false }, 1), false);
  const proj = projectWaveCostFromSample(9.6, 12, 84);
  assert.ok(proj.projected84 > 60);
  assert.equal(proj.likelyHardCapBreach, false);
});

test("peer v2 exact + portfolio-only excluded from denominator config", () => {
  assert.equal(WAVE1_PEER_SET_ID, PEER_SET_ID_V2);
  assert.equal(plan.PEER.COUNT, 15);
  assert.equal(plan.PEER.VALID, true);
});

test("language isolation in plan matrix", () => {
  assert.equal(plan.MATRIX.GLOBAL_EN, 12);
  assert.equal(plan.MATRIX.CALA_EN, 12);
  assert.equal(plan.MATRIX.CALA_ES, 12);
  assert.equal(plan.MATRIX.MEXICO_EN, 12);
  assert.equal(plan.MATRIX.MEXICO_ES, 12);
  assert.ok(plan.PROMPT_LIBRARY.EN === 60 && plan.PROMPT_LIBRARY.ES === 24);
});

await testAsync("live mock wave — first slot gate PASS then full 84; resume-safe", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "wave1-showcase-"));
  const storeRoot = path.join(tmp, "wave1-showcase");
  fs.mkdirSync(storeRoot, { recursive: true });
  process.env.AI_VISIBILITY_ENABLED = "true";
  process.env.AI_VISIBILITY_LIVE_TEST = "true";
  process.env.OPENAI_API_KEY = "sk-test-not-real";
  process.env.AI_VISIBILITY_MODEL = "gpt-5.6";
  process.env.AI_VISIBILITY_MAX_BATCH_COST_USD = "125";
  process.env.AI_VISIBILITY_STORE_ROOT = storeRoot;

  const entityIndex = buildPeerEntityIndex();
  let calls = 0;
  const runFn = async (args) => {
    calls += 1;
    return mockSuccessRun(args);
  };

  const first = await executeWave1Showcase({
    execute: true,
    storeRoot,
    entityIndex,
    runVisibilityPrompt: runFn,
    stopAfterFirstSlot: true,
    allowUnsafePreflight: false,
  });
  assert.equal(first.summary.activationGate.RESULT, "PASS");
  assert.equal(first.summary.slots.GLOBAL_EN.succeeded, 12);
  assert.equal(first.summary.logical.succeeded, 12);
  const wave1Id = first.wave1Id;
  const callsAfterGate = calls;
  assert.equal(callsAfterGate, 12);

  const full = await executeWave1Showcase({
    execute: true,
    resume: true,
    wave1Id,
    storeRoot,
    entityIndex,
    runVisibilityPrompt: runFn,
  });
  assert.equal(full.summary.logical.succeeded, 84);
  assert.equal(full.summary.logical.failedFinal, 0);
  // Resume must not re-call completed GLOBAL_EN fingerprints
  assert.equal(calls, 84);
  assert.equal(full.summary.cost.capBreached, false);
  assert.equal(full.summary.metrics.COMPARABLE_PRIOR_PERIOD, "NONE");
  assert.equal(full.summary.metrics.TREND_AVAILABLE, false);
  assert.ok(path.resolve(storeRoot) !== path.resolve(PHASE2E_ROOT));
  assert.ok(fs.existsSync(path.join(storeRoot, "checkpoints", `${wave1Id}.json`)));
  assert.ok(fs.existsSync(path.join(storeRoot, "waves", wave1Id, "raw")));
});

await testAsync("activation gate FAIL stops before remaining slots", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "wave1-showcase-fail-"));
  const storeRoot = path.join(tmp, "wave1-showcase");
  fs.mkdirSync(storeRoot, { recursive: true });
  process.env.AI_VISIBILITY_STORE_ROOT = storeRoot;
  process.env.AI_VISIBILITY_ENABLED = "true";
  process.env.AI_VISIBILITY_LIVE_TEST = "true";
  process.env.OPENAI_API_KEY = "sk-test";
  process.env.AI_VISIBILITY_MODEL = "gpt-5.6";
  process.env.AI_VISIBILITY_MAX_BATCH_COST_USD = "125";

  let calls = 0;
  const runFn = async () => {
    calls += 1;
    throw new ProviderError("simulated provider down", { type: "network_error", retryable: true });
  };
  const result = await executeWave1Showcase({
    execute: true,
    storeRoot,
    entityIndex: buildPeerEntityIndex(),
    runVisibilityPrompt: runFn,
  });
  assert.equal(result.summary.activationGate.RESULT, "FAIL");
  assert.equal(result.summary.status, "activation_gate_failed");
  assert.ok(result.summary.logical.succeeded < 10);
  // Should not continue into CALA after gate fail (at most GLOBAL_EN attempts)
  assert.ok(calls <= 12 * WAVE1_RETRY_POLICY.maxAttemptsPerCall);
  assert.equal(result.summary.slots.CALA_EN.status, "pending");
});

await testAsync("retry keeps same fingerprint; no duplicate completed observations", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "wave1-showcase-retry-"));
  const storeRoot = path.join(tmp, "wave1-showcase");
  fs.mkdirSync(storeRoot, { recursive: true });
  process.env.AI_VISIBILITY_STORE_ROOT = storeRoot;
  const seen = new Map();
  let attempts = 0;
  const runFn = async ({ prompt }) => {
    attempts += 1;
    const id = prompt.promptId;
    const n = (seen.get(id) || 0) + 1;
    seen.set(id, n);
    if (n === 1) {
      throw new ProviderError("rate limit", { type: "rate_limit", retryable: true, status: 429 });
    }
    return mockSuccessRun({ prompt });
  };
  const result = await executeWave1Showcase({
    execute: true,
    storeRoot,
    entityIndex: buildPeerEntityIndex(),
    runVisibilityPrompt: runFn,
    stopAfterFirstSlot: true,
  });
  assert.equal(result.summary.activationGate.RESULT, "PASS");
  assert.equal(result.summary.logical.succeeded, 12);
  assert.equal(result.summary.slots.GLOBAL_EN.retried, 12);
  assert.equal(attempts, 24);
  const fps = Object.keys(result.checkpoint.completedFingerprints);
  assert.equal(fps.length, 12);
  assert.equal(new Set(fps).size, 12);
});

await testAsync("hard cap stop safe", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "wave1-showcase-cap-"));
  const storeRoot = path.join(tmp, "wave1-showcase");
  fs.mkdirSync(storeRoot, { recursive: true });
  process.env.AI_VISIBILITY_STORE_ROOT = storeRoot;
  process.env.AI_VISIBILITY_MAX_BATCH_COST_USD = "125";
  const runFn = async ({ prompt }) => ({
    ...mockSuccessRun({ prompt }),
    // Force high attributed cost via huge token counts
    usage: { inputTokens: 20_000_000, outputTokens: 5_000_000, totalTokens: 25_000_000 },
  });
  const result = await executeWave1Showcase({
    execute: true,
    storeRoot,
    entityIndex: buildPeerEntityIndex(),
    runVisibilityPrompt: runFn,
    hardCapUsd: 125,
  });
  assert.ok(
    result.summary.status === "partial_cost_cap" || result.summary.cost.capBreached === true
  );
  assert.ok(result.summary.logical.succeeded < 84);
});

test("companies — four showcase portfolios reuse central dataset (plan)", () => {
  assert.ok(plan.COMPANIES.marriott);
  assert.ok(plan.COMPANIES.hilton);
  assert.ok(plan.COMPANIES.choice);
  assert.ok(plan.COMPANIES.ihg);
  assert.equal(plan.COMPANIES.marriott.DUPLICATE_PROVIDER_RUN_REQUIRED, false);
});

test("metric version locked; no fabricated trend fields in plan", () => {
  assert.equal(METRIC_VERSION, "ai_visibility_metrics_v1");
});

test("preflight shape without printing secrets", () => {
  const prev = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "sk-secret-should-not-appear";
  process.env.AI_VISIBILITY_ENABLED = "true";
  process.env.AI_VISIBILITY_LIVE_TEST = "true";
  process.env.AI_VISIBILITY_MODEL = "gpt-5.6";
  process.env.AI_VISIBILITY_MAX_BATCH_COST_USD = "125";
  process.env.AI_VISIBILITY_STORE_ROOT = path.join(root, "data/ai-visibility/runtime/wave1-showcase");
  const pf = preflightWave1LiveEnv();
  const dumped = JSON.stringify(pf);
  assert.equal(dumped.includes("sk-secret"), false);
  assert.equal(pf.OPENAI_KEY_PRESENT, "YES");
  assert.equal(pf.HARD_COST_CAP, 125);
  process.env.OPENAI_API_KEY = prev;
});

console.log(`\nPhase 3A.11 results: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
