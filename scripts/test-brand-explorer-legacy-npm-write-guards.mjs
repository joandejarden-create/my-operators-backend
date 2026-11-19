#!/usr/bin/env node
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  FACTORY_CRITICAL_NPM_SCRIPTS,
  auditLegacyNpmWriteGuards,
} from "../lib/brand-explorer/canonical/legacy-npm-write-guard-audit.js";
import { legacyApplyRequested } from "../lib/brand-explorer/canonical/legacy-apply-guard.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

{
  assert.equal(legacyApplyRequested(["node", "x.mjs"]), false);
  assert.equal(legacyApplyRequested(["node", "x.mjs", "--apply"]), true);
  assert.equal(legacyApplyRequested(["node", "x.mjs", "--stage", "apply-draft"]), true);
  assert.equal(legacyApplyRequested(["node", "x.mjs", "--stage", "public-release"]), true);
  console.log("PASS legacyApplyRequested detection");
}

{
  const audit = auditLegacyNpmWriteGuards(ROOT);
  assert.equal(audit.guardModuleOk, true);
  assert.equal(audit.factoryCriticalUnguarded.length, 0, audit.factoryCriticalUnguarded.join(","));
  assert.ok(audit.guardedCount >= FACTORY_CRITICAL_NPM_SCRIPTS.length);
  assert.ok(audit.applyCapableCount >= FACTORY_CRITICAL_NPM_SCRIPTS.length);
  console.log(`PASS factory-critical npm guards (${audit.guardedCount}/${audit.applyCapableCount})`);
}

{
  const res = spawnSync(
    process.execPath,
    [path.join(ROOT, "scripts/brand-explorer-legacy-guard.mjs"), "scripts/brand-explorer-full-tab-factory-build.mjs", "--apply"],
    { cwd: ROOT, env: { ...process.env, BRAND_EXPLORER_ALLOW_LEGACY_APPLY: "" }, encoding: "utf8" },
  );
  assert.notEqual(res.status, 0);
  assert.match(String(res.stderr || ""), /Legacy apply blocked/i);
  console.log("PASS legacy-guard blocks --apply without break-glass");
}

console.log("\nAll legacy npm write-guard tests passed.");
