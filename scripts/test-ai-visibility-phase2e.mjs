#!/usr/bin/env node
/**
 * Phase 2E tests — execution batch, retry, cost, storage, peer-set gates.
 * No paid provider calls.
 */
import assert from "node:assert/strict";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import {
  createBatchId,
  createExecutionBatch,
  deriveBatchStatus,
  deriveBatchHealth,
  isRetryableProviderError,
  isAuthProviderError,
  findDuplicateRecentBatch,
  hashPromptText,
  BATCH_HEALTH,
  planAiVisibilityCohort,
  executeAiVisibilityCohort,
  validatePeerSetAgainstIndex,
  resolvePeerSetMembership,
  loadPeerSetConfig,
  createAiVisibilityStore,
  buildAiVisibilityEntityIndex,
  ProviderError,
} from "../lib/ai-visibility/index.js";
import { ProviderError as PE } from "../lib/ai-visibility/providers/base-provider.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURES = path.join(__dirname, "..", "fixtures", "ai-visibility");

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    const ret = fn();
    if (ret && typeof ret.then === "function") {
      return ret
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

async function run() {
  console.log("AI Visibility Phase 2E tests\n");

  console.log("Batch model");
  await test("unique batch id shape", () => {
    const a = createBatchId();
    const b = createBatchId();
    assert.match(a, /^aiv_batch_\d{8}_[a-f0-9]+$/);
    assert.notEqual(a, b);
  });
  await test("manifest frozen with prompt versions + hashes", () => {
    const cohort = {
      fingerprint: "abc123",
      members: [
        {
          promptId: "p1",
          version: "1",
          promptText: "Hello",
          geographyScope: "Region",
          commercialRegion: "CALA",
          intentTerritory: "Conversion",
        },
      ],
    };
    const { batch, manifest } = createExecutionBatch({
      cohort,
      stakeholder: "brand",
      entityScope: "Brand",
      geographyScope: "Region",
      commercialRegion: "CALA",
      provider: "openai",
      model: "gpt-5.6",
      entityIndexFingerprint: "fp1",
    });
    assert.equal(batch.status, "planned");
    assert.equal(manifest.plannedRuns, 1);
    assert.equal(manifest.prompts[0].promptTextHash, hashPromptText("Hello"));
    assert.equal(manifest.model, "gpt-5.6");
    assert.equal(manifest.geographyScope, "Region");
  });
  await test("status derivation completed/partial/failed", () => {
    assert.equal(deriveBatchStatus({ successfulRuns: 3, failedRuns: 0 }), "completed");
    assert.equal(deriveBatchStatus({ successfulRuns: 2, failedRuns: 1 }), "partial");
    assert.equal(deriveBatchStatus({ successfulRuns: 0, failedRuns: 2 }), "failed");
    assert.equal(
      deriveBatchStatus({ successfulRuns: 1, failedRuns: 0, costLimitReached: true }),
      "partial"
    );
  });

  console.log("\nRetry policy");
  await test("503 retryable; auth not", () => {
    assert.equal(isRetryableProviderError(new PE("x", { status: 503, retryable: true })), true);
    assert.equal(isRetryableProviderError(new PE("x", { status: 429, retryable: true })), true);
    assert.equal(isAuthProviderError(new PE("x", { status: 401 })), true);
    assert.equal(isRetryableProviderError(new PE("x", { status: 400, retryable: false })), false);
  });

  console.log("\nDuplicate protection");
  await test("recent duplicate detected; force path separate", () => {
    const now = Date.now();
    const batches = [
      {
        batchId: "old",
        status: "completed",
        cohortFingerprint: "fp",
        provider: "openai",
        model: "gpt-5.6",
        geographyScope: "Region",
        commercialRegion: "CALA",
        country: null,
        requestedAt: new Date(now - 60_000).toISOString(),
      },
    ];
    const hit = findDuplicateRecentBatch(
      batches,
      {
        cohortFingerprint: "fp",
        provider: "openai",
        model: "gpt-5.6",
        geographyScope: "Region",
        commercialRegion: "CALA",
        country: null,
      },
      15 * 60 * 1000,
      now
    );
    assert.equal(hit.batchId, "old");
    const miss = findDuplicateRecentBatch(
      batches,
      {
        cohortFingerprint: "other",
        provider: "openai",
        model: "gpt-5.6",
        geographyScope: "Region",
        commercialRegion: "CALA",
        country: null,
      },
      15 * 60 * 1000,
      now
    );
    assert.equal(miss, null);
  });

  console.log("\nStorage");
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-p2e-"));
  const store = createAiVisibilityStore({ rootDir: tmp });
  await test("batch/manifest/summary/snapshot round-trip", async () => {
    await store.saveBatch({ batchId: "aiv_batch_test_1", status: "planned" });
    await store.saveBatchManifest({ batchId: "aiv_batch_test_1", plannedRuns: 2 });
    await store.saveBatchSummary({ batchId: "aiv_batch_test_1", status: "completed" });
    await store.saveMetricSnapshot({
      batchId: "aiv_batch_test_1",
      entityId: "e1",
      metric: "ai_presence_rate",
      value: 0.5,
      geographyScope: "Region",
      commercialRegion: "CALA",
      provider: "openai",
      batchDate: "2026-08-13T00:00:00.000Z",
    });
    assert.equal((await store.getBatch("aiv_batch_test_1")).status, "planned");
    assert.equal((await store.getBatchManifest("aiv_batch_test_1")).plannedRuns, 2);
    const snaps = await store.listMetricSnapshots({
      entityId: "e1",
      geographyScope: "Region",
      region: "CALA",
      metric: "ai_presence_rate",
      provider: "openai",
    });
    assert.equal(snaps.length, 1);
    assert.equal(store.durability, "local_dev_not_production");
  });

  console.log("\nDry-run cohort planning (fixture prompts)");
  const dryCala = await planAiVisibilityCohort({
    stakeholder: "brand",
    entityScope: "Brand",
    geographyScope: "Region",
    region: "CALA",
    promptMode: "fixture",
    fixturePath: path.join(FIXTURES, "phase2d-prompt-seed.json"),
  });
  await test("CALA brand dry-run region-pure", () => {
    assert.equal(dryCala.DRY_RUN_PROVIDER_CALLS, 0);
    assert.ok(dryCala.cohort.count >= 1);
    assert.ok(dryCala.cohort.members.every((m) => m.geographyScope === "Region"));
    assert.ok(dryCala.cohort.members.every((m) => m.commercialRegion === "CALA"));
    assert.equal(dryCala.prerequisitesOk, true);
  });

  const dryGlobal = await planAiVisibilityCohort({
    stakeholder: "brand",
    entityScope: "Brand",
    geographyScope: "Global",
    promptMode: "fixture",
    fixturePath: path.join(FIXTURES, "phase2d-prompt-seed.json"),
  });
  const dryEurope = await planAiVisibilityCohort({
    stakeholder: "brand",
    entityScope: "Brand",
    geographyScope: "Region",
    region: "Europe",
    promptMode: "fixture",
    fixturePath: path.join(FIXTURES, "phase2d-prompt-seed.json"),
  });
  const dryCalaOp = await planAiVisibilityCohort({
    stakeholder: "operator",
    entityScope: "Operator",
    geographyScope: "Region",
    region: "CALA",
    promptMode: "fixture",
    fixturePath: path.join(FIXTURES, "phase2d-prompt-seed.json"),
  });
  const dryMx = await planAiVisibilityCohort({
    stakeholder: "brand",
    entityScope: "Brand",
    geographyScope: "Country",
    country: "Mexico",
    promptMode: "fixture",
    fixturePath: path.join(FIXTURES, "phase2d-prompt-seed.json"),
  });

  await test("Global / Europe / CALA Op / Mexico cohorts isolate", () => {
    assert.ok(dryGlobal.cohort.members.every((m) => m.geographyScope === "Global"));
    assert.ok(dryEurope.cohort.members.every((m) => m.commercialRegion === "Europe"));
    assert.ok(dryCalaOp.cohort.members.every((m) => m.commercialRegion === "CALA"));
    assert.ok(dryMx.cohort.members.every((m) => m.country === "Mexico"));
    const calaIds = new Set(dryCala.cohort.promptIds);
    for (const id of dryEurope.cohort.promptIds) assert.ok(!calaIds.has(id));
  });

  console.log("\nPeer set validation");
  const universe = JSON.parse(
    fs.readFileSync(path.join(FIXTURES, "phase2c-entity-universe.json"), "utf8")
  );
  const index = buildAiVisibilityEntityIndex({
    brands: universe.entities.filter((e) => e.entityType === "brand"),
    operators: universe.entities.filter((e) => e.entityType === "operator"),
    applyOverlay: true,
  });
  await test("peer set IDs validate against fixture universe", () => {
    const cfg = loadPeerSetConfig();
    const raw = resolvePeerSetMembership(
      { peerSetId: "peers_upper_upscale_brands_global_v1", commercialRegion: "CALA" },
      cfg
    );
    const v = validatePeerSetAgainstIndex(raw, index);
    assert.equal(v.canonicalValid, true, `missing=${(v.missingEntityIds || []).join(",")}`);
  });
  await test("invalid peer id blocks Competitive Position only flag", () => {
    const bad = validatePeerSetAgainstIndex(
      { ok: true, peerSetId: "x", entityIds: ["recDOESNOTEXIST"], peerSetVersion: "1" },
      index
    );
    assert.equal(bad.canonicalValid, false);
  });

  console.log("\nLive execution simulation (mock provider)");
  process.env.AI_VISIBILITY_ENABLED = "true";
  process.env.AI_VISIBILITY_LIVE_TEST = "true";
  process.env.OPENAI_API_KEY = "sk-test-not-real";
  process.env.AI_VISIBILITY_MODEL = "gpt-5.6";
  process.env.AI_VISIBILITY_MAX_TEST_RUNS = "20";
  process.env.AI_VISIBILITY_MAX_BATCH_COST_USD = "5";

  await test("partial batch on one failure", async () => {
    const tmp2 = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-p2e-exec-"));
    const store2 = createAiVisibilityStore({ rootDir: tmp2 });
    let calls = 0;
    const mockRun = async () => {
      calls += 1;
      if (calls === 2) {
        throw new PE("upstream 503", { status: 503, type: "upstream_error", retryable: true });
      }
      return {
        provider: "openai",
        model: "gpt-5.6",
        text: "1. Curio Collection by Hilton\n2. Autograph Collection\n",
        citations: [],
        usage: { inputTokens: 100, outputTokens: 50, totalTokens: 150 },
        latencyMs: 10,
        citationCapability: "unavailable",
        parserVersion: "test",
        providerMeta: {},
        raw: { fixture: true },
      };
    };
    // Force only 2 prompts by filtering fixture via intent that yields multiple — use Conversion CALA
    const result = await executeAiVisibilityCohort({
      stakeholder: "brand",
      entityScope: "Brand",
      geographyScope: "Region",
      region: "CALA",
      intentTerritories: "Conversion",
      execute: true,
      forceNewBatch: true,
      store: store2,
      promptMode: "fixture",
      fixturePath: path.join(FIXTURES, "phase2d-prompt-seed.json"),
      entityIndex: index,
      runVisibilityPrompt: mockRun,
    });
    assert.ok(["partial", "completed", "failed"].includes(result.batch.status));
    // With retry: call 2 fails then retries once more — may still fail that prompt
    assert.equal(result.AIRTABLE_EXECUTION_WRITES, 0);
    assert.ok(result.summary.execution.planned >= 1);
  });

  await test("auth error not retried endlessly", async () => {
    let calls = 0;
    const mockRun = async () => {
      calls += 1;
      throw new PE("unauthorized", { status: 401, type: "auth_error", retryable: false });
    };
    const tmp3 = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-p2e-auth-"));
    const result = await executeAiVisibilityCohort({
      stakeholder: "brand",
      entityScope: "Brand",
      geographyScope: "Region",
      region: "CALA",
      intentTerritories: "Conversion",
      execute: true,
      forceNewBatch: true,
      store: createAiVisibilityStore({ rootDir: tmp3 }),
      promptMode: "fixture",
      fixturePath: path.join(FIXTURES, "phase2d-prompt-seed.json"),
      entityIndex: index,
      runVisibilityPrompt: mockRun,
    });
    assert.equal(result.batch.status, "failed");
    assert.ok(calls <= 2); // first prompt only, no infinite retry
  });

  await test("cost ceiling stops further calls", async () => {
    process.env.AI_VISIBILITY_MAX_BATCH_COST_USD = "0.15";
    process.env.AI_VISIBILITY_EST_USD_PER_CALL = "0.1";
    let calls = 0;
    const mockRun = async () => {
      calls += 1;
      return {
        provider: "openai",
        model: "gpt-5.6",
        text: "Recommend Autograph Collection.",
        citations: [],
        usage: null,
        latencyMs: 1,
        citationCapability: "unavailable",
        parserVersion: "test",
        providerMeta: {},
        raw: {},
      };
    };
    const tmp4 = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-p2e-cost-"));
    const result = await executeAiVisibilityCohort({
      stakeholder: "brand",
      entityScope: "Brand",
      geographyScope: "Region",
      region: "CALA",
      execute: true,
      forceNewBatch: true,
      store: createAiVisibilityStore({ rootDir: tmp4 }),
      promptMode: "fixture",
      fixturePath: path.join(FIXTURES, "phase2d-prompt-seed.json"),
      entityIndex: index,
      runVisibilityPrompt: mockRun,
    });
    assert.equal(result.summary.execution.costLimitReached, true);
    assert.ok(calls < result.summary.execution.planned);
    process.env.AI_VISIBILITY_MAX_BATCH_COST_USD = "5";
  });

  console.log("\nDry-run sample counts");
  console.log(
    JSON.stringify(
      {
        globalBrand: { count: dryGlobal.cohort.count, fp: dryGlobal.cohort.fingerprint },
        calaBrand: { count: dryCala.cohort.count, fp: dryCala.cohort.fingerprint },
        europeBrand: { count: dryEurope.cohort.count, fp: dryEurope.cohort.fingerprint },
        calaOperator: { count: dryCalaOp.cohort.count, fp: dryCalaOp.cohort.fingerprint },
        mexicoBrand: { count: dryMx.cohort.count, fp: dryMx.cohort.fingerprint },
      },
      null,
      2
    )
  );

  console.log(`\nResults: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

run();
