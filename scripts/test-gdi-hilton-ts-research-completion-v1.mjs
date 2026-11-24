#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT = path.join(ROOT, "reports/gdi-hilton-ts-research-completion-v1");
const HOTEL_ID = "rec35fExUxCClpOP6";

function read(name) {
  return JSON.parse(fs.readFileSync(path.join(REPORT, name), "utf8"));
}
function check(name, fn) {
  try {
    fn();
    console.log(`  PASS ${name}`);
  } catch (e) {
    console.error(`  FAIL ${name}`);
    throw e;
  }
}

const run = spawnSync(
  process.execPath,
  ["scripts/gdi-hilton-ts-research-completion-v1.mjs"],
  { cwd: ROOT, encoding: "utf8" },
);
assert.equal(run.status, 0, run.stderr || run.stdout);

check("pass8_not_rubber_stamped", () => {
  const r = read("01_PASS8_COMMERCIAL_REVALIDATION.json");
  assert.equal(r.priorPassCount, 8);
  assert.ok(r.retainedCount < 8);
  assert.ok(r.downgradedCount >= 1);
  assert.equal(r.retainedCount + r.downgradedCount, 8);
});

check("hold25_ranked_zero_dollar_recoveries_documented", () => {
  const h = read("02_HOLD25_RANK_AND_ZERO_DOLLAR_SALVAGE.json");
  assert.equal(h.holdCount, 25);
  assert.equal(h.recoveredZeroDollar, 0);
});

check("competitive_beyond_sssp_requires_paid", () => {
  const c = read("03_COMPETITIVE_EXPANSION_ZERO_DOLLAR.json");
  assert.equal(c.newlyVerifiedBeyondSssp, 0);
});

check("paid_plan_awaits_auth_no_spend", () => {
  const p = read("04_PAID_RESEARCH_PREFLIGHT_PLAN.json");
  assert.equal(p.status, "AWAITING_FOUNDER_AUTHORIZATION");
  assert.equal(p.proposedCeilingUsd, 16);
  assert.equal(p.agent, false);
  assert.equal(p.surfe, false);
  const s = read("06_FOUNDER_SCORECARD.json");
  assert.equal(s.spend.actualProviderSpendUsd, 0);
  assert.equal(s.spend.paidCallsExecuted, false);
});

check("candidate_package_and_no_pointer", () => {
  const cert = read("07_CERTIFICATION.json");
  assert.equal(cert.ok, true);
  assert.equal(cert.checks.hiltonPointerNotActivated, true);
  const store = createGdiPublicationPackageStore();
  assert.equal(store.readPointer(HOTEL_ID, "STAGED"), null);
  const score = read("06_FOUNDER_SCORECARD.json");
  assert.equal(score.finalization.total, 5);
  assert.equal(score.finalization.salesReady, 0);
});

console.log("\nOK test-gdi-hilton-ts-research-completion-v1");
