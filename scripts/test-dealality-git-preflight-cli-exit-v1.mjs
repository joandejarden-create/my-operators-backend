#!/usr/bin/env node
/**
 * CLI boundary regression: dealality-git-preflight-v1.mjs must exit nonzero
 * when a prohibited mutation is requested — not only when library helpers fail.
 *
 * Does not perform checkouts, stashes, resets, or worktree mutations.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CLI = path.join(ROOT, "scripts", "dealality-git-preflight-v1.mjs");
const ADP_BRANCH = "feat/adp/gdi-full-e2e-report-client-link-recovery-v1";

function runCli(args) {
  const result = spawnSync(process.execPath, [CLI, ...args], {
    cwd: ROOT,
    encoding: "utf8",
    env: process.env,
  });
  return {
    status: result.status,
    stdout: result.stdout || "",
    stderr: result.stderr || "",
    signal: result.signal,
  };
}

function assertExit(label, args, { expectOk, fragment }) {
  const r = runCli(args);
  const blob = `${r.stdout}\n${r.stderr}`;
  if (expectOk) {
    assert.equal(r.status, 0, `${label}: expected exit 0, got ${r.status}\n${blob}`);
  } else {
    assert.notEqual(r.status, 0, `${label}: expected nonzero exit, got ${r.status}\n${blob}`);
    assert.equal(r.status, 2, `${label}: expected exit 2, got ${r.status}\n${blob}`);
  }
  if (fragment) {
    assert.ok(blob.includes(fragment), `${label}: expected output containing "${fragment}"\n${blob}`);
  }
  console.log(`PASS CLI ${expectOk ? "allow" : "fail-closed"}: ${label} (exit=${r.status})`);
  return r;
}

// Documented path: cross-workstream switch must fail closed at process boundary
{
  const r = assertExit(
    "documented --target-ref ADP branch from GDI",
    ["--op", "switch", "--stream", "gdi", "--target-ref", ADP_BRANCH],
    { expectOk: false, fragment: "CROSS_WORKSTREAM" },
  );
  assert.ok(r.stdout.includes("ok=false") || r.stdout.includes('"ok": false'));
}

// Alias that previously caused false OK must now evaluate and refuse
assertExit(
  "--to-branch alias ADP branch from GDI",
  ["--op", "switch", "--stream", "gdi", "--to-branch", ADP_BRANCH],
  { expectOk: false, fragment: "CROSS_WORKSTREAM" },
);

// Missing target must not silently authorize switch/checkout
assertExit(
  "switch without target-ref",
  ["--op", "switch", "--stream", "gdi"],
  { expectOk: false, fragment: "requires --target-ref" },
);
assertExit(
  "checkout without target-ref",
  ["--op", "checkout", "--stream", "gdi"],
  { expectOk: false, fragment: "requires --target-ref" },
);

// Unknown flag must fail closed (no silent ignore)
assertExit(
  "unknown flag --desired-branch",
  ["--op", "switch", "--stream", "gdi", "--desired-branch", ADP_BRANCH],
  { expectOk: false, fragment: "unknown preflight flag" },
);

// Unclassified target fails closed
assertExit(
  "unclassified feature branch",
  ["--op", "switch", "--stream", "gdi", "--target-ref", "feat/random-unclassified-xyz"],
  { expectOk: false, fragment: "does not match gdi pattern" },
);

// Stash remains blocked without founder auth
assertExit(
  "stash without founder auth",
  ["--op", "stash", "--stream", "gdi"],
  { expectOk: false, fragment: "PRESERVATION" },
);

// Same-workstream home switch allowed when dirty paths are explicitly allowlisted
// (this suite itself dirties the tree while validating the CLI).
assertExit(
  "same-workstream home switch with allow-dirty",
  ["--op", "switch", "--stream", "gdi", "--target-ref", "ws/gdi", "--allow-dirty", "*"],
  { expectOk: true, fragment: "ok=true" },
);

// Dirty switch without allow-dirty must fail closed when the tree is dirty.
{
  const probe = runCli(["--op", "status-check", "--stream", "gdi", "--json"]);
  let dirtyCount = 0;
  try {
    dirtyCount = Number(JSON.parse(probe.stdout)?.facts?.dirtyCount || 0);
  } catch {
    dirtyCount = -1;
  }
  if (dirtyCount > 0) {
    assertExit(
      "dirty switch without allow-dirty",
      ["--op", "switch", "--stream", "gdi", "--target-ref", "ws/gdi"],
      { expectOk: false, fragment: "dirty worktree" },
    );
  } else {
    console.log("SKIP CLI dirty switch (worktree clean during this run)");
  }
}

// Read-only status remains usable even when dirty
assertExit(
  "status-check",
  ["--op", "status-check", "--stream", "gdi"],
  { expectOk: true, fragment: "ok=true" },
);

console.log("\nAll git-preflight CLI exit-code tests PASSED");
