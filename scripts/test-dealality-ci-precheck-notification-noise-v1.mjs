#!/usr/bin/env node
/**
 * Regression: CI precheck + notification-noise reduction V1.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const results = [];

function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err.message || err) });
  }
}

check("pr_workflows_have_concurrency_cancel", () => {
  const dir = path.join(ROOT, ".github", "workflows");
  const files = fs.readdirSync(dir).filter((f) => /\.ya?ml$/.test(f));
  const prFiles = files.filter((f) => {
    const text = fs.readFileSync(path.join(dir, f), "utf8");
    return /pull_request/.test(text);
  });
  assert.ok(prFiles.length >= 4, `expected PR workflows, got ${prFiles.join(",")}`);
  for (const f of prFiles) {
    const text = fs.readFileSync(path.join(dir, f), "utf8");
    assert.match(text, /\bconcurrency\s*:/, `${f} missing concurrency`);
    assert.match(text, /cancel-in-progress:\s*true/, `${f} must cancel superseded PR runs`);
  }
});

check("production_release_never_cancels_in_progress", () => {
  const text = fs.readFileSync(
    path.join(ROOT, ".github", "workflows", "dealality-production-release.yml"),
    "utf8"
  );
  assert.match(text, /cancel-in-progress:\s*false/);
});

check("secret_scan_not_disabled", () => {
  const text = fs.readFileSync(path.join(ROOT, ".github", "workflows", "secret-scan-pr.yml"), "utf8");
  assert.match(text, /gitleaks\/gitleaks-action@v2/);
  assert.match(text, /pull_request/);
  assert.doesNotMatch(text, /continue-on-error:\s*true/);
});

check("gitleaks_config_still_detects_generic_keys", () => {
  const text = fs.readFileSync(path.join(ROOT, ".gitleaks.toml"), "utf8");
  assert.match(text, /useDefault\s*=\s*true/);
  assert.match(text, /id\s*=\s*"generic-api-key"/);
  // Narrow allowlists only — no blanket reports/ allowlist.
  assert.doesNotMatch(text, /paths\s*=\s*\[[\s\S]*'''reports\/\*'''/);
});

check("gitleaks_allowlist_covers_feature_flag_labels", () => {
  const text = fs.readFileSync(path.join(ROOT, ".gitleaks.toml"), "utf8");
  assert.match(text, /\[A-Z\]\[A-Z0-9_\]\{2,64\}=\[01\]/);
  assert.match(text, /Bare env var \*names\* as documentation/);
});

check("precheck_script_and_npm_script_exist", () => {
  assert.ok(fs.existsSync(path.join(ROOT, "scripts", "dealality-ci-precheck-v1.mjs")));
  const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, "package.json"), "utf8"));
  assert.equal(pkg.scripts["dealality:ci-precheck"], "node scripts/dealality-ci-precheck-v1.mjs");
  assert.ok(pkg.scripts["test:dealality-ci-precheck-notification-noise-v1"]);
});

check("cursor_discipline_requires_precheck", () => {
  const rule = fs.readFileSync(
    path.join(ROOT, ".cursor", "rules", "dealality-ci-precheck-discipline.mdc"),
    "utf8"
  );
  assert.match(rule, /dealality:ci-precheck/);
  assert.match(rule, /alwaysApply:\s*true/);
  assert.match(rule, /Do not open a PR known to fail avoidable checks/i);
});

check("notification_limitation_documented", () => {
  const doc = fs.readFileSync(
    path.join(ROOT, "docs", "ci", "github-actions-notification-noise-reduction.md"),
    "utf8"
  );
  assert.match(doc, /Don't notify/);
  assert.match(doc, /cannot guarantee/i);
  assert.match(doc, /built-in failure emails/i);
  assert.match(doc, /cancel-in-progress/);
});

check("admin_static_preflight_includes_execution_monitor", () => {
  const text = fs.readFileSync(
    path.join(ROOT, ".github", "workflows", "adp-admin-workspace-navigation.yml"),
    "utf8"
  );
  assert.match(text, /gdi-execution-monitor/);
});

check("precheck_runs_json_mode", () => {
  const r = spawnSync(process.execPath, ["scripts/dealality-ci-precheck-v1.mjs", "--json", "--allow-skip-gitleaks"], {
    cwd: ROOT,
    encoding: "utf8",
  });
  assert.equal(r.status, 0, r.stderr || r.stdout);
  const json = JSON.parse(r.stdout);
  assert.equal(json.contract, "dealality-ci-precheck-v1");
  assert.equal(json.ok, true);
  assert.ok(json.workflows.ok);
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: "dealality-ci-precheck-notification-noise-v1",
      ok: failed.length === 0,
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
    },
    null,
    2
  )
);
process.exit(failed.length ? 1 : 0);
