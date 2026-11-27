#!/usr/bin/env node
/**
 * Permanent gate: inter-prompt non-regression controls exist and catch the
 * Integration-cwd + foreign-server.js regression class.
 *
 *   npm run test:dealality-inter-prompt-non-regression-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateCanonicalPort8080Ownership,
  auditLiveCanonical8080Listener,
} from "../lib/dealality-dev-os/canonical-port-8080-ownership-v1.js";
import {
  loadGdiCumulativeFunctionalityManifest,
  validateGdiCumulativeFunctionalityManifest,
} from "../lib/dealality-dev-os/gdi-cumulative-functionality-manifest-v1.js";
import { loadWorktreeRegistry } from "../lib/dealality-dev-os/worktree-registry.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const registry = loadWorktreeRegistry(ROOT);
const gdi = registry.workspaces.find((w) => w.stream === "gdi");
const integration = registry.workspaces.find((w) => w.stream === "integration");

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err && err.message ? err.message : err}`);
  }
}

check("assignment_preflight_script_exists", () => {
  assert.equal(
    fs.existsSync(path.join(ROOT, "scripts/dealality-assignment-preflight-v1.mjs")),
    true,
  );
  assert.equal(
    fs.existsSync(path.join(ROOT, "scripts/dealality-canonical-8080-guard-v1.mjs")),
    true,
  );
});

check("manifest_includes_latest_outlook_capability", () => {
  const m = loadGdiCumulativeFunctionalityManifest();
  assert.ok(m.capabilities.some((c) => c.id === "outlook-12mo-compact-kpis"));
  assert.ok(m.capabilities.some((c) => c.id === "executive-summary"));
  assert.ok(m.capabilities.length >= 19);
  const v = validateGdiCumulativeFunctionalityManifest({ root: ROOT, manifest: m });
  assert.equal(v.ok, true, v.errors.join("; "));
});

check("foreign_verify_tree_cannot_authorize_via_integration_cwd", () => {
  const r = evaluateCanonicalPort8080Ownership({
    cwd: integration.path,
    serverScriptDir: path.join("C:/Dev/dealality", "_verify_pr627_main"),
    requireServerScriptDir: true,
    listenPort: 8080,
    env: { NODE_ENV: "development", DEALALITY_8080_OWNER: "integration" },
    registry,
  });
  assert.equal(r.code, "PORT_8080_SERVER_SCRIPT_MISMATCH");
  assert.equal(r.ok, false);
});

check("gdi_matching_script_still_authorized", () => {
  const r = evaluateCanonicalPort8080Ownership({
    cwd: gdi.path,
    serverScriptDir: gdi.path,
    requireServerScriptDir: true,
    listenPort: 8080,
    env: { NODE_ENV: "development" },
    registry,
  });
  assert.equal(r.ok, true);
  assert.equal(r.code, "AUTHORIZED_GDI_HOME");
});

check("live_audit_helper_returns_structured_result", () => {
  const live = auditLiveCanonical8080Listener({ registry });
  assert.ok(live.code);
  assert.equal(typeof live.ok, "boolean");
  assert.ok(Array.isArray(live.pids));
});

check("agents_and_rules_reference_assignment_preflight", () => {
  const agents = fs.readFileSync(path.join(ROOT, "AGENTS.md"), "utf8");
  assert.match(agents, /dealality:assignment-preflight|INTER_PROMPT_NON_REGRESSION/);
  const rule = fs.readFileSync(
    path.join(ROOT, ".cursor/rules/dealality-integrated-delivery.mdc"),
    "utf8",
  );
  assert.match(rule, /assignment-preflight|INTER_PROMPT_NON_REGRESSION|serverScriptDir/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:dealality-inter-prompt-non-regression-v1");
