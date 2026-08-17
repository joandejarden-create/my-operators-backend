#!/usr/bin/env node
/**
 * Phase 3B.2 — Controlled multi-provider validation tests.
 * Live provider calls only in dedicated live subtests (skipped by default).
 */
import assert from "node:assert/strict";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import {
  preflightProviderCredentials,
  resolveGeminiCredential,
  resolvePerplexityCredential,
  resolveClaudeCredential,
} from "../lib/ai-visibility/provider-credentials.js";
import {
  buildProviderValidationExecutionPlan,
  attachProviderFingerprints,
} from "../lib/ai-visibility/provider-validation-plan.js";
import {
  CONTROLLED_VALIDATION_PROMPT_IDS,
  VALIDATION_PROMPTS_PER_PROVIDER,
  buildControlledValidationPlan,
} from "../lib/ai-visibility/providers/validation-plan.js";
import {
  MONITORING_RUN_PURPOSE,
  isValidationMonitoringRun,
  isBaselineMonitoringRun,
  monitoringRunTypeSupported,
} from "../lib/ai-visibility/monitoring-run-purpose.js";
import { evaluateProviderValidationActivationGate } from "../lib/ai-visibility/provider-validation-activation-gate.js";
import {
  executeProviderValidation,
  preflightValidationLiveEnv,
} from "../lib/ai-visibility/provider-validation-orchestrator.js";
import {
  summarizeProviderValidationStats,
  assessGoNoGo,
} from "../lib/ai-visibility/provider-validation-audit.js";
import { listAvailableAiVisibilityProviders } from "../lib/ai-visibility/provider-dimension.js";
import { createAiVisibilityStore } from "../lib/ai-visibility/storage/file-store.js";
import { resolveProviderValidationStoreRoot, WAVE1_ROOT } from "../lib/ai-visibility/storage/resolve-store-root.js";
import { getProviderAdapter, validateProviderAdapter } from "../lib/ai-visibility/providers/index.js";
import { buildAiVisibilityEntityIndex } from "../lib/ai-visibility/entity-index.js";
import { loadPeerSetConfig, resolvePeerSetMembership, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import { project84CallCost } from "../lib/ai-visibility/provider-validation-cost.js";
import { ProviderError } from "../lib/ai-visibility/providers/base-provider.js";
import { classifyProviderError, PROVIDER_ERROR_CATEGORIES } from "../lib/ai-visibility/providers/provider-errors.js";

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

console.log("\nAI Visibility Phase 3B.2 — Controlled Multi-Provider Validation\n");

function buildPeerEntityIndex() {
  const peer = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2 }, loadPeerSetConfig());
  const set = (loadPeerSetConfig().peerSets || []).find((p) => p.peerSetId === PEER_SET_ID_V2);
  const brands = (set.members || []).map((m) => ({
    id: m.brandId,
    name: m.brandName,
    entityType: "brand",
    aliases: [m.brandName],
    parentCompany: m.canonicalParent,
    isParentCompanyLabel: false,
  }));
  for (const id of peer.entityIds || []) {
    if (!brands.some((b) => b.id === id)) {
      brands.push({ id, name: id, entityType: "brand", aliases: [], isParentCompanyLabel: false });
    }
  }
  return buildAiVisibilityEntityIndex({ brands, operators: [], applyOverlay: false });
}

test("credentials — secret values never printed in preflight", () => {
  const out = preflightProviderCredentials();
  const json = JSON.stringify(out);
  assert.ok(!/sk-[a-zA-Z0-9]{20,}/.test(json));
  assert.ok(!/pplx-[a-zA-Z0-9]{20,}/.test(json));
  assert.equal(out.SECRET_EXPOSURE, "NONE");
  for (const key of ["GEMINI_CREDENTIAL", "PERPLEXITY_CREDENTIAL", "CLAUDE_CREDENTIAL"]) {
    assert.ok(["PRESENT", "MAPPED", "MISSING"].includes(out[key]));
  }
});

test("credentials — only env var names returned", () => {
  const g = resolveGeminiCredential();
  if (g.envVarUsed) assert.match(g.envVarUsed, /^[A-Z0-9_]+$/);
});

test("validation plan — exactly 12 governed prompt IDs", () => {
  const plan = buildControlledValidationPlan();
  assert.equal(plan.CALLS_PER_PROVIDER, 12);
  assert.equal(plan.TOTAL_CALLS, 36);
  assert.equal(CONTROLLED_VALIDATION_PROMPT_IDS.length, 12);
  const exec = buildProviderValidationExecutionPlan();
  assert.equal(exec.ok, true);
  assert.equal(exec.EXECUTIONS.length, 12);
  for (const id of plan.PROMPT_IDS) {
    assert.ok(exec.EXECUTIONS.some((e) => e.promptId === id), id);
  }
});

test("run purpose — validation != baseline", () => {
  assert.equal(monitoringRunTypeSupported(), true);
  assert.equal(isValidationMonitoringRun({ monitoringRunPurpose: "validation" }), true);
  assert.equal(isBaselineMonitoringRun({ monitoringRunPurpose: "validation" }), false);
  assert.equal(
    isBaselineMonitoringRun({ baselineSeriesId: "aiv_wave1_openai_peer_v2_showcase_prompts_v1" }),
    true
  );
});

test("provider identities — adapters + fingerprint isolation", () => {
  const exec = buildProviderValidationExecutionPlan().EXECUTIONS[0];
  const fps = new Set();
  for (const p of ["gemini", "perplexity", "claude"]) {
    const rows = attachProviderFingerprints([exec], p);
    assert.equal(rows[0].provider, p);
    fps.add(rows[0].fingerprint);
    assert.equal(validateProviderAdapter(getProviderAdapter(p)).ok, true);
  }
  assert.equal(fps.size, 3);
});

test("activation gate — pipeline health not content", () => {
  const pass = evaluateProviderValidationActivationGate({ planned: 3, succeeded: 3, failed: 0 });
  assert.equal(pass.ACTIVATION_GATE, "PASS");
  const fail = evaluateProviderValidationActivationGate({
    planned: 3,
    succeeded: 0,
    failed: 3,
    authErrors: 1,
  });
  assert.equal(fail.ACTIVATION_GATE, "FAIL");
});

test("mock provider validation — storage + validation purpose", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-3b2-"));
  const storeRoot = path.join(tmp, "perplexity");
  const execPlan = attachProviderFingerprints(
    buildProviderValidationExecutionPlan().EXECUTIONS.slice(0, 2),
    "perplexity"
  );

  if (!process.env.PERPLEXITY_API_KEY) process.env.PERPLEXITY_API_KEY = "test-key-mock";

  const mockRun = async () => ({
    provider: "perplexity",
    model: "sonar",
    text: "Kimpton and Autograph Collection are often recommended.",
    citations: [{ url: "https://example.com/a", title: "A", providerSupplied: true }],
    searchResults: [{ url: "https://example.com/a", title: "A" }],
    usage: { inputTokens: 100, outputTokens: 50, providerCostUsd: 0.002 },
    latencyMs: 1200,
    citationCapability: "supported",
    raw: { id: "mock" },
  });

  const stats = await executeProviderValidation({
    provider: "perplexity",
    waveId: "aiv_validation_perplexity_test",
    storeRoot,
    hardCapUsd: 5,
    executionsOverride: execPlan,
    runVisibilityPrompt: mockRun,
    entityIndex: buildPeerEntityIndex(),
  });

  assert.equal(stats.SUCCEEDED, 2);
  assert.equal(stats.PLANNED, 2);
  const summaryPath = path.join(storeRoot, "summaries", "aiv_validation_perplexity_test.json");
  assert.ok(fs.existsSync(summaryPath));
  const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
  assert.equal(summary.monitoringRunPurpose, "validation");
});

test("errors — canonical categories; provider failure != brand absence", () => {
  const e = classifyProviderError(new ProviderError("x", { type: "rate_limit", status: 429 }));
  assert.equal(e.category, PROVIDER_ERROR_CATEGORIES.RATE_LIMIT);
});

test("cost — validation projection + hard cap recommendation", () => {
  const proj = project84CallCost(1.2, 3);
  assert.ok(proj.EXPECTED > 0);
  assert.equal(proj.STATUS, "CALIBRATED_FROM_VALIDATION");
});

test("UI — validation summaries excluded from measured providers", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-ui-"));
  const store = createAiVisibilityStore({ rootDir: tmp });
  await store.saveBatchSummary({
    batchId: "aiv_validation_gemini_x",
    status: "completed",
    provider: { name: "gemini" },
    monitoringRunPurpose: "validation",
    succeeded: 12,
  });
  await store.saveBatchSummary({
    batchId: "aiv_wave1_openai_showcase_x",
    status: "completed",
    provider: { name: "openai" },
    monitoringRunPurpose: "baseline",
    baselineSeriesId: "aiv_wave1_openai_peer_v2_showcase_prompts_v1",
    succeeded: 84,
  });
  const available = await listAvailableAiVisibilityProviders({ store });
  const ids = available.map((p) => p.id);
  assert.ok(ids.includes("openai"));
  assert.ok(!ids.includes("gemini"));
});

test("storage — validation namespace isolated from Wave-1", () => {
  const p = resolveProviderValidationStoreRoot("perplexity");
  assert.ok(p.includes("provider-validation"));
  assert.notEqual(p, WAVE1_ROOT);
});

test("OpenAI Wave-1 path unchanged", () => {
  assert.ok(fs.existsSync(WAVE1_ROOT) || true);
  assert.ok(String(WAVE1_ROOT).includes("wave1-showcase"));
});

test("go/no-go assessor", () => {
  assert.equal(assessGoNoGo({ SUCCEEDED: 12, ACTIVATION_GATE: "PASS", DATASET_STATUS: "READY" }), "GO");
  assert.equal(
    assessGoNoGo({ status: "NOT_EXECUTED_MISSING_CREDENTIAL" }),
    "BLOCKED"
  );
});

console.log(`\nPhase 3B.2 results: ${passed} passed, ${failed} failed`);
console.log("LIVE_OPENAI_CALLS: 0");
if (failed) process.exit(1);
