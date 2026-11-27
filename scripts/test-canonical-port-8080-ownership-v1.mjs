#!/usr/bin/env node
/**
 * Permanent gate: port 8080 ownership cannot be granted by env flags alone.
 *   npm run test:canonical-port-8080-ownership-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateCanonicalPort8080Ownership,
  buildCanonical8080LaunchEnv,
  resolveDurableGdiPublicationPackageRoot,
  STREAM_DEV_PORTS,
  DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1,
} from "../lib/dealality-dev-os/canonical-port-8080-ownership-v1.js";
import { loadWorktreeRegistry } from "../lib/dealality-dev-os/worktree-registry.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const registry = loadWorktreeRegistry(ROOT);
const gdi = registry.workspaces.find((w) => w.stream === "gdi");
const integration = registry.workspaces.find((w) => w.stream === "integration");
assert.ok(gdi?.path, "gdi workspace registered");
assert.ok(integration?.path, "integration workspace registered");

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

check("contract_constant", () => {
  assert.equal(
    DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1,
    "DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1",
  );
  assert.equal(STREAM_DEV_PORTS.gdi, 8095);
});

check("gdi_home_authorized_for_8080", () => {
  const r = evaluateCanonicalPort8080Ownership({
    cwd: gdi.path,
    listenPort: 8080,
    env: { NODE_ENV: "development" },
    registry,
  });
  assert.equal(r.ok, true);
  assert.equal(r.authorizedFor8080, true);
  assert.equal(r.code, "AUTHORIZED_GDI_HOME");
  assert.equal(r.durablePackageRoot.ok, true);
});

check("env_flags_alone_cannot_authorize_foreign_8080", () => {
  const foreign = path.join("C:/Dev/dealality", "wt-he-unified-theme-v1");
  const r = evaluateCanonicalPort8080Ownership({
    cwd: foreign,
    listenPort: 8080,
    env: {
      NODE_ENV: "development",
      DEALALITY_CANONICAL_CHECKOUT: "1",
      DEALALITY_ALLOW_WORKTREE_SERVER: "1",
    },
    registry,
  });
  assert.equal(r.ok, false);
  assert.equal(r.authorizedFor8080, false);
  assert.equal(r.code, "PORT_8080_UNAUTHORIZED_WORKSPACE");
  assert.equal(r.envBypassIgnored.DEALALITY_CANONICAL_CHECKOUT, true);
});

check("integration_requires_explicit_owner_flag", () => {
  const denied = evaluateCanonicalPort8080Ownership({
    cwd: integration.path,
    listenPort: 8080,
    env: {
      NODE_ENV: "development",
      DEALALITY_CANONICAL_CHECKOUT: "1",
    },
    registry,
  });
  assert.equal(denied.ok, false);
  assert.equal(denied.code, "PORT_8080_UNAUTHORIZED_WORKSPACE");

  const allowed = evaluateCanonicalPort8080Ownership({
    cwd: integration.path,
    listenPort: 8080,
    env: {
      NODE_ENV: "development",
      DEALALITY_8080_OWNER: "integration",
    },
    registry,
  });
  assert.equal(allowed.ok, true);
  assert.equal(allowed.code, "AUTHORIZED_INTEGRATION_HOME");
});

check("feature_port_allowed_with_worktree_flag", () => {
  const foreign = path.join("C:/Dev/dealality", "wt-he-unified-theme-v1");
  const r = evaluateCanonicalPort8080Ownership({
    cwd: foreign,
    listenPort: 8095,
    env: {
      NODE_ENV: "development",
      DEALALITY_ALLOW_WORKTREE_SERVER: "1",
    },
    registry,
  });
  assert.equal(r.ok, true);
  assert.equal(r.authorizedFor8080, false);
  assert.equal(r.code, "AUTHORIZED_DEV_PORT_ALLOW_WORKTREE");
});

check("ci_repo_checkout_authorized_for_8080", () => {
  // Simulate Actions: checkout path equals registryRoot but is not ws-gdi / Integration.
  const ciCheckout = path.join("C:", "actions-runner", "my-operators-backend");
  const r = evaluateCanonicalPort8080Ownership({
    cwd: ciCheckout,
    listenPort: 8080,
    env: {
      NODE_ENV: "development",
      GITHUB_ACTIONS: "true",
      CI: "true",
    },
    registry,
    registryRoot: ciCheckout,
  });
  assert.equal(r.ok, true);
  assert.equal(r.authorizedFor8080, true);
  assert.equal(r.code, "AUTHORIZED_CI_REPO_CHECKOUT");
  assert.equal(r.durablePackageRoot.ok, true);
});

check("ci_flag_cannot_authorize_foreign_worktree_8080", () => {
  const foreign = path.join("C:/Dev/dealality", "wt-he-unified-theme-v1");
  const r = evaluateCanonicalPort8080Ownership({
    cwd: foreign,
    listenPort: 8080,
    env: {
      NODE_ENV: "development",
      GITHUB_ACTIONS: "true",
      CI: "true",
    },
    registry,
    registryRoot: ROOT,
  });
  assert.equal(r.ok, false);
  assert.equal(r.code, "PORT_8080_UNAUTHORIZED_WORKSPACE");
});

check("launch_env_pins_durable_package_root", () => {
  const launch = buildCanonical8080LaunchEnv(
    { NODE_ENV: "development" },
    { cwd: gdi.path, registry },
  );
  assert.equal(launch.ok, true);
  assert.ok(launch.env.GDI_PUBLICATION_PACKAGE_ROOT);
  assert.match(
    launch.env.GDI_PUBLICATION_PACKAGE_ROOT.replace(/\\/g, "/"),
    /bundled-publication-packages$/,
  );
  assert.equal(
    launch.env.DEALALITY_CANONICAL_8080_OWNERSHIP,
    DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1,
  );
});

check("durable_root_has_wyndham_staged_pointer", () => {
  const durable = resolveDurableGdiPublicationPackageRoot({ registry });
  assert.equal(durable.ok, true);
  const stagedPath = path.join(
    durable.root,
    "recnPB3Q02NO8Ma1o",
    "STAGED.json",
  );
  assert.equal(fs.existsSync(stagedPath), true);
  const staged = JSON.parse(fs.readFileSync(stagedPath, "utf8"));
  assert.match(String(staged.publicationVersion), /20261127/);
  assert.ok(!String(staged.publicationVersion).includes("3283ee92"));
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:canonical-port-8080-ownership-v1");
