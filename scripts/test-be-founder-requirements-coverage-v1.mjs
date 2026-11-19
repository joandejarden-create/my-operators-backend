#!/usr/bin/env node
/**
 * CI coverage check: every master requirement must have an explicit disposition
 * and must not be silently missing from the register. Detects CONTRACT-only drift.
 * Does NOT claim product completeness. Gate 2 remains blocked.
 */
import assert from "assert";
import { existsSync, readFileSync } from "fs";
import { join } from "path";
import { spawnSync } from "child_process";

const root = process.cwd();
const registerPath = join(
  root,
  "reports/brand-explorer/founder-requirements-reconciliation/01-master-founder-requirements-register.json",
);

// Regenerate register for freshness
const gen = spawnSync(process.execPath, ["scripts/be-founder-requirements-reconciliation-v1.mjs"], {
  cwd: root,
  encoding: "utf8",
});
if (gen.status !== 0) {
  console.error(gen.stdout || "", gen.stderr || "");
  process.exit(1);
}

assert.ok(existsSync(registerPath), "master register missing");
const master = JSON.parse(readFileSync(registerPath, "utf8"));

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (e) {
    failed += 1;
    console.error("FAIL", name, e.message || e);
  }
}

check("register_has_50_plus_additional", () => {
  assert.ok(master.summary.counts.checklist50 === 50);
  assert.ok(
    master.summary.counts.additional >= 30,
    `expected >=30 additional recovered reqs, got ${master.summary.counts.additional}`,
  );
  assert.ok(master.summary.counts.total >= 80);
});

check("every_requirement_has_status_and_coverage", () => {
  for (const r of master.requirements) {
    assert.ok(r.requirementId, "missing id");
    assert.ok(r.status, `${r.requirementId} missing status`);
    assert.ok(r.coverage, `${r.requirementId} missing coverage`);
    assert.ok(
      typeof r.coverage.founderApproved === "boolean",
      `${r.requirementId} missing G`,
    );
  }
});

check("no_requirement_founder_approved_without_flag", () => {
  for (const r of master.requirements) {
    if (r.status === "FOUNDER_APPROVED") {
      assert.strictEqual(r.coverage.founderApproved, true);
    }
  }
});

check("gate1_not_auto_approved", () => {
  assert.strictEqual(master.gateStatus.gate1.founderApproved, false);
  assert.strictEqual(master.gateStatus.gate2.status, "BLOCKED");
});

check("lifecycle_conflict_documented", () => {
  const conflictPath = join(
    root,
    "reports/brand-explorer/founder-requirements-reconciliation/07-conflicting-requirements-decision-register.json",
  );
  const c = JSON.parse(readFileSync(conflictPath, "utf8"));
  assert.strictEqual(c.conflicts[0].status, "OPEN_CONFLICT");
  assert.strictEqual(c.conflicts[0].documentedCurrentProductionBaseline.count, 6);
});

check("zero_founder_approved_in_summary", () => {
  assert.strictEqual(master.summary.counts.founderApproved, 0);
});

check("contracts_only_report_exists", () => {
  assert.ok(
    existsSync(
      join(root, "reports/brand-explorer/founder-requirements-reconciliation/05-contracts-only-not-product-code.json"),
    ),
  );
});

if (failed) {
  console.error(`\n${failed} coverage check(s) failed`);
  process.exit(1);
}
console.log("\nFounder requirements coverage OK — dispositions present; Gate2 blocked; 0 founder-approved");
