#!/usr/bin/env node
/**
 * Regression: `gdi-weekly-refresh.mjs --hotel-id <rec…> --dry-run`
 *  - accepts --hotel-id (space + = forms) and legacy --hotel=
 *  - requires no SERPAPI_KEY and never calls SerpApi
 *  - writes only under the (isolated) report dir; no data/ evals or hotel-bag writes
 *  - live (non-dry-run) without SERPAPI_KEY fails closed (exit 2, BLOCKED message)
 *
 * No network, no paid spend.
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const SCRIPT = path.join(ROOT, "scripts/gdi-weekly-refresh.mjs");
const HOTEL = "recLuxvwwxID7U2B8"; // has frozen weekly discovery on disk

function sha(file) {
  return fs.existsSync(file)
    ? crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex")
    : null;
}

function snapshotDir(dir) {
  const out = {};
  if (!fs.existsSync(dir)) return out;
  for (const f of fs.readdirSync(dir)) out[f] = sha(path.join(dir, f));
  return out;
}

function run(args, extraEnv = {}) {
  const env = { ...process.env, ...extraEnv };
  delete env.SERPAPI_KEY;
  delete env.SERPAPI_API_KEY;
  return spawnSync("node", [SCRIPT, ...args], {
    cwd: ROOT,
    encoding: "utf8",
    timeout: 120000,
    env: { ...env, ...extraEnv },
  });
}

const evalsDir = path.join(ROOT, "data/group-demand-intelligence/evals");
const bagFile = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL,
  "opportunities.json",
);
const weeklyRunsDir = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL,
  "weekly-runs",
);
const evalsBefore = snapshotDir(evalsDir);
const bagBefore = sha(bagFile);
const weeklyRunsBefore = fs.existsSync(weeklyRunsDir) ? fs.readdirSync(weeklyRunsDir) : [];

function assertDryRunOk(r, label, reportDir) {
  assert.equal(r.status, 0, `${label}: exit 0\n${r.stderr}\n${r.stdout}`);
  const line = r.stdout.trim().split(/\r?\n/).filter(Boolean).pop();
  const summary = JSON.parse(line);
  assert.equal(summary.ok, true, label);
  assert.equal(summary.mode, "DRY_RUN", label);
  assert.equal(summary.hotelId, HOTEL, label);
  assert.equal(summary.serpApiCalls, 0, label);
  const refreshFile = path.join(reportDir, "00-WEEKLY_REFRESH_DRY_RUN.json");
  assert.ok(fs.existsSync(refreshFile), `${label}: report written`);
  const refresh = JSON.parse(fs.readFileSync(refreshFile, "utf8"));
  assert.equal(refresh.dryRun, true, label);
  assert.equal(refresh.guarantees.serpApiCalls, 0, label);
  assert.equal(refresh.guarantees.stagedOverwrite, false, label);
  assert.equal(refresh.guarantees.currentChanged, false, label);
  assert.ok(refresh.counts && typeof refresh.counts.UNCHANGED === "number", `${label}: delta counts`);
  for (const f of ["01-baseline.json", "02-weekly-delta.json", "03-staged-preview.opportunities.json"]) {
    assert.ok(fs.existsSync(path.join(reportDir, f)), `${label}: ${f}`);
  }
  return refresh;
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-weekly-dry-"));
try {
  // 1. --hotel-id <id> --dry-run, no SERPAPI_KEY
  const d1 = path.join(tmp, "space");
  const r1 = run(["--hotel-id", HOTEL, "--dry-run", "--report-dir", d1]);
  assertDryRunOk(r1, "--hotel-id <id> --dry-run", d1);

  // 2. --hotel-id=<id> --dryRun
  const d2 = path.join(tmp, "equals");
  const r2 = run([`--hotel-id=${HOTEL}`, "--dryRun", `--report-dir=${d2}`]);
  assertDryRunOk(r2, "--hotel-id=<id> --dryRun", d2);

  // 3. legacy --hotel=<id> --dry-run
  const d3 = path.join(tmp, "legacy");
  const r3 = run([`--hotel=${HOTEL}`, "--dry-run", "--report-dir", d3]);
  assertDryRunOk(r3, "--hotel=<id> --dry-run", d3);

  // 4. ISO run id (not the hardcoded legacy date) unless overridden
  const refresh1 = JSON.parse(
    fs.readFileSync(path.join(d1, "00-WEEKLY_REFRESH_DRY_RUN.json"), "utf8"),
  );
  assert.match(refresh1.runId, /_\d{8}$/);
  assert.notEqual(refresh1.runId.slice(-8), "20260921", "default run id is date-derived");
  const d4 = path.join(tmp, "override");
  const r4 = run(["--hotel-id", HOTEL, "--dry-run", "--run-date", "20260921", "--report-dir", d4]);
  const refresh4 = assertDryRunOk(r4, "--run-date override", d4);
  assert.ok(refresh4.runId.endsWith("_20260921"));

  // 5. --dry-run + --apply-stage is refused
  const r5 = run(["--hotel-id", HOTEL, "--dry-run", "--apply-stage", "--report-dir", path.join(tmp, "x")]);
  assert.equal(r5.status, 2, "dry-run + apply-stage is blocked");

  // 6. Live without SERPAPI_KEY fails closed before any write (forced no-key guard => no spend)
  const r6 = run(["--hotel-id", HOTEL], { GDI_WEEKLY_REFRESH_FORCE_NO_SERPAPI: "1" });
  assert.equal(r6.status, 2, `live without SERPAPI_KEY fails closed\n${r6.stderr}`);
  assert.match(r6.stderr, /BLOCKED — SERPAPI_KEY required for live weekly discovery/);

  // 7. No mutation of data/ evals, hotel bag, or weekly-runs
  assert.deepEqual(snapshotDir(evalsDir), evalsBefore, "data evals untouched");
  assert.equal(sha(bagFile), bagBefore, "hotel opportunities bag untouched");
  const weeklyRunsAfter = fs.existsSync(weeklyRunsDir) ? fs.readdirSync(weeklyRunsDir) : [];
  assert.deepEqual(weeklyRunsAfter, weeklyRunsBefore, "weekly-runs untouched");
} finally {
  fs.rmSync(tmp, { recursive: true, force: true });
}

console.log("OK test:gdi-weekly-refresh-dry-run-v1");
