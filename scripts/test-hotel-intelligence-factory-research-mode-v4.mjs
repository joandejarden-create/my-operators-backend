#!/usr/bin/env node
/**
 * RESEARCH MODE test suite — permissions, resume, idempotency foundations.
 * Does NOT claim live Airtable persistence validation.
 */
import assert from "node:assert/strict";
import path from "node:path";
import fs from "node:fs";
import os from "node:os";
import { fileURLToPath } from "node:url";
import {
  normalizeMode,
  assertModePermissions,
  HIF_MODE,
} from "../lib/hotel-intelligence/hotel-intelligence-factory/modes-v1.js";
import { runHotelIntelligenceFactory } from "../lib/hotel-intelligence/hotel-intelligence-factory/engine-v1.js";
import {
  loadCheckpoint,
  saveCheckpoint,
  markStageComplete,
  isUnitDone,
  markUnit,
} from "../lib/hotel-intelligence/hotel-intelligence-factory/checkpoint-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DURABLE = fs.mkdtempSync(path.join(os.tmpdir(), "hif-research-"));
let passed = 0;

function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    console.error(`FAIL ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

async function testAsync(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    console.error(`FAIL ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

test("research_mode_normalize", () => {
  assert.equal(normalizeMode("Research"), HIF_MODE.RESEARCH);
});

test("research_cannot_promote", () => {
  const r = assertModePermissions({
    mode: "Research",
    stages: ["PROMOTE"],
    applyPromotion: true,
    manifestPath: "x.json",
    authorization: {},
  });
  assert.equal(r.ok, false);
});

test("research_persist_requires_env", () => {
  delete process.env.HIF_APPLY_RESEARCH;
  const r = assertModePermissions({
    mode: "Research",
    stages: ["PERSIST_RESEARCH"],
    applyResearch: true,
  });
  assert.equal(r.ok, false);
  assert.match(r.error, /HIF_APPLY_RESEARCH/);
});

test("checkpoint_resume_units", () => {
  const runId = "hif_research_test_resume";
  let cp = loadCheckpoint(DURABLE, runId);
  markUnit(cp, "accessions", "000-test");
  markStageComplete(cp, "DISCOVER");
  saveCheckpoint(DURABLE, runId, cp);
  cp = loadCheckpoint(DURABLE, runId);
  assert.equal(isUnitDone(cp, "accessions", "000-test"), true);
  assert.ok(cp.completedStages.includes("DISCOVER"));
});

await testAsync("research_dry_run_not_live_validated", async () => {
  const r = await runHotelIntelligenceFactory({
    cwd: ROOT,
    durableRoot: DURABLE,
    mode: "Research",
    dryRun: true,
    stages: "CONFIG,RECONCILE,MANIFEST,REPORT",
    configPath: path.join(ROOT, "configs/hotel-intelligence-factory/default.v4.json"),
  });
  assert.equal(r.ok, true);
  assert.equal(r.validationClaim, "NOT_LIVE_VALIDATED");
  assert.equal(r.report.validationClaim, "NOT_LIVE_VALIDATED");
});

await testAsync("research_rerun_idempotent_resume_skips", async () => {
  const runId = "hif_research_idempotent_1";
  const r1 = await runHotelIntelligenceFactory({
    cwd: ROOT,
    durableRoot: DURABLE,
    runId,
    mode: "Research",
    dryRun: true,
    stages: "CONFIG,RECONCILE,REPORT",
  });
  assert.equal(r1.ok, true);
  const r2 = await runHotelIntelligenceFactory({
    cwd: ROOT,
    durableRoot: DURABLE,
    runId,
    mode: "Research",
    dryRun: true,
    resume: true,
    stages: "CONFIG,RECONCILE,REPORT",
  });
  assert.equal(r2.ok, true);
  assert.ok(r2.report.stageResults.some((s) => s.status === "SKIPPED_RESUME"));
});

console.log(`\n${passed} research-mode tests passed`);
