#!/usr/bin/env node
/**
 * Regression fixtures for Dealality clean-checkout release integrity gate.
 * Local only — does not deploy or mutate production.
 */
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  evaluateSourceTreePreconditions,
  runCleanCheckoutReleaseGate,
  verifyRuntimeIdentityMatch,
  assertNoPublicationMutation,
  assertCleanCheckoutAttestationForDeploy,
  writeCleanCheckoutAttestation,
  createCleanCheckoutWorktree,
  removeCleanCheckoutWorktree,
  verifyCheckoutIntegrity,
} from "../lib/dealality-production-release/clean-checkout-release-gate-v1.js";
import {
  resolveProductImpact,
  resolveMandatoryTests,
  loadRegressionMatrix,
} from "../lib/dealality-production-release/product-impact-resolver-v1.js";
import {
  assertManifestReadyForProduction,
  buildReleaseManifestV1,
} from "../lib/dealality-production-release/release-manifest-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const sha = "e1eec3ee70b38cc5b61ed36075cec0904764f076";
const results = [];

function pass(name, detail = "") {
  results.push({ name, pass: true, detail });
  console.log(`PASS ${name}${detail ? ` — ${detail}` : ""}`);
}
function fail(name, detail) {
  results.push({ name, pass: false, detail: String(detail) });
  console.error(`FAIL ${name} — ${detail}`);
}

const matrix = loadRegressionMatrix(ROOT);
const attestationDir = fs.mkdtempSync(
  path.join(os.tmpdir(), "dealality-cca-")
);

try {
  // 1) Clean valid release (simulated)
  {
    const gate = runCleanCheckoutReleaseGate({
      repoRoot: ROOT,
      approvedSha: sha,
      product: "ADP",
      simulate: true,
      gitStatusPorcelain: "",
      changedFiles: ["lib/ai-demand-positioning/index.js"],
      matrix,
      attestationOutDir: attestationDir,
      rollbackTarget: { sha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
      previousProductionDeployment: { sha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
    });
    assert.equal(gate.ok, true);
    assert.equal(gate.readyForProduction, true);
    assert.equal(gate.attestation?.ok, true);
    const ready = assertManifestReadyForProduction(gate.manifest);
    assert.equal(ready.ok, true);
    pass("clean_valid_release", gate.attestation.attestation.attestationId);
  }

  // 2) Dirty worktree direct deploy forbidden
  {
    const pre = evaluateSourceTreePreconditions({
      gitStatusPorcelain: " M server.js\n",
      sha,
      approvedSha: sha,
    });
    assert.equal(pre.ok, false);
    assert.ok(
      pre.failures.some((f) => f.code === "DIRTY_WORKTREE_DIRECT_DEPLOY_FORBIDDEN")
    );
    pass("dirty_worktree_rejected");
  }

  // 3) Missing untracked module
  {
    const gate = runCleanCheckoutReleaseGate({
      repoRoot: ROOT,
      approvedSha: sha,
      product: "GDI",
      simulate: true,
      gitStatusPorcelain: "",
      untrackedRequiredModules: ["lib/group-demand-intelligence/secret-local-only.js"],
      matrix,
      writeAttestation: false,
    });
    assert.equal(gate.ok, false);
    assert.ok(
      gate.failures.some((f) => f.code === "UNTRACKED_REQUIRED_MODULE")
    );
    pass("missing_untracked_module_fails");
  }

  // 4) Missing production asset
  {
    const gate = runCleanCheckoutReleaseGate({
      repoRoot: ROOT,
      approvedSha: sha,
      product: "ADP",
      simulate: true,
      simulateMissingAsset: "public/owner-ai-demand-share.html",
      gitStatusPorcelain: "",
      matrix,
      writeAttestation: false,
    });
    assert.equal(gate.ok, false);
    assert.ok(gate.failures.some((f) => f.code === "MISSING_PRODUCTION_ASSET"));
    pass("missing_production_asset_fails");
  }

  // 5) Divergent branch
  {
    const gate = runCleanCheckoutReleaseGate({
      repoRoot: ROOT,
      approvedSha: sha,
      product: "AVA",
      simulate: true,
      divergentFromApprovedBase: true,
      divergentDetail: "behind origin/main by 54 commits",
      gitStatusPorcelain: "",
      matrix,
      writeAttestation: false,
    });
    assert.equal(gate.ok, false);
    assert.ok(gate.failures.some((f) => f.code === "DIVERGENT_BRANCH"));
    pass("divergent_branch_fails");
  }

  // 6) Missing mandatory regression test
  {
    const impact = resolveProductImpact({
      primaryProduct: "AVA",
      changedFiles: ["lib/ai-visibility/foo.js"],
      matrix,
    });
    const tests = resolveMandatoryTests({
      products: impact.products,
      matrix,
      root: ROOT,
      packageScripts: {
        // Intentionally omit ava:production-release-preflight and shared tests
        "assert:production-assets": "node x",
      },
    });
    assert.equal(tests.ok, false);
    assert.ok(tests.missing.length >= 1);
    pass("missing_mandatory_regression_test_fails", String(tests.missing.length));
  }

  // 7) Incorrect runtime SHA
  {
    const match = verifyRuntimeIdentityMatch({
      expectedSha: sha,
      runtimePayload: { gitSha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" },
    });
    assert.equal(match.ok, false);
    assert.equal(match.code, "RUNTIME_SHA_MISMATCH");
    pass("incorrect_runtime_sha_fails");
  }

  // 8) Concurrent deployment
  {
    const gate = runCleanCheckoutReleaseGate({
      repoRoot: ROOT,
      approvedSha: sha,
      product: "ADP",
      simulate: true,
      concurrentDeployment: true,
      gitStatusPorcelain: "",
      matrix,
      writeAttestation: false,
    });
    assert.equal(gate.ok, false);
    assert.ok(gate.failures.some((f) => f.code === "CONCURRENT_DEPLOYMENT"));
    pass("concurrent_deployment_fails");
  }

  // 9) Failed build
  {
    const gate = runCleanCheckoutReleaseGate({
      repoRoot: ROOT,
      approvedSha: sha,
      product: "ADP",
      simulate: true,
      simulateBuildFailure: true,
      gitStatusPorcelain: "",
      matrix,
      writeAttestation: false,
    });
    assert.equal(gate.ok, false);
    assert.ok(gate.failures.some((f) => f.code === "BUILD_FAILED"));
    pass("failed_build_fails");
  }

  // 10) Missing dependency
  {
    const gate = runCleanCheckoutReleaseGate({
      repoRoot: ROOT,
      approvedSha: sha,
      product: "ADP",
      simulate: true,
      simulateMissingDependency: true,
      gitStatusPorcelain: "",
      matrix,
      writeAttestation: false,
    });
    assert.equal(gate.ok, false);
    assert.ok(gate.failures.some((f) => f.code === "MISSING_DEPENDENCY"));
    pass("missing_dependency_fails");
  }

  // 11) Unapproved publication mutation
  {
    const pub = assertNoPublicationMutation({
      authorization: {},
      attemptedMutations: { adpDurablePublish: true },
    });
    assert.equal(pub.ok, false);
    assert.ok(
      pub.failures.some((f) => f.code === "UNAPPROVED_PUBLICATION_MUTATION")
    );
    pass("unapproved_publication_mutation_fails");
  }

  // 12) Rollback readiness failure
  {
    const gate = runCleanCheckoutReleaseGate({
      repoRoot: ROOT,
      approvedSha: sha,
      product: "ADP",
      simulate: true,
      simulateRollbackReadinessFailure: true,
      gitStatusPorcelain: "",
      matrix,
      writeAttestation: false,
    });
    assert.equal(gate.ok, false);
    assert.ok(gate.failures.some((f) => f.code === "ROLLBACK_READINESS_FAILURE"));
    pass("rollback_readiness_failure_fails");
  }

  // 13) Lean stage forbidden as source
  {
    const pre = evaluateSourceTreePreconditions({
      gitStatusPorcelain: "",
      sha,
      approvedSha: sha,
      leanStageAsSource: true,
      cwd: "C:/Dev/dealality/_railway-lean-stage",
    });
    assert.equal(pre.ok, false);
    assert.ok(pre.failures.some((f) => f.code === "LEAN_STAGE_FORBIDDEN_AS_SOURCE"));
    pass("lean_stage_forbidden_as_source");
  }

  // 14) Deploy requires attestation matching SHA
  {
    const written = writeCleanCheckoutAttestation({
      outDir: attestationDir,
      gitSha: sha,
      checkoutPath: "/tmp/sim",
      readyForProduction: true,
      productModulesIncluded: ["ADP", "SHARED"],
    });
    const ok = assertCleanCheckoutAttestationForDeploy({
      env: { DEALALITY_CLEAN_CHECKOUT_ATTESTATION: written.path },
      sha,
    });
    assert.equal(ok.ok, true);
    const bad = assertCleanCheckoutAttestationForDeploy({
      env: {},
      sha,
    });
    assert.equal(bad.ok, false);
    const bypass = assertCleanCheckoutAttestationForDeploy({
      env: {
        DEALALITY_CLEAN_CHECKOUT_BYPASS: "1",
        DEALALITY_CLEAN_CHECKOUT_ATTESTATION: written.path,
      },
      sha,
    });
    assert.equal(bypass.ok, false);
    assert.equal(bypass.code, "CLEAN_CHECKOUT_BYPASS_FORBIDDEN");
    pass("deploy_attestation_required_and_bypass_forbidden");
  }

  // 15) Product impact cross-protection
  {
    const adp = resolveProductImpact({
      primaryProduct: "ADP",
      changedFiles: ["lib/ai-demand-positioning/x.js"],
      matrix,
    });
    assert.ok(adp.products.includes("ADP"));
    assert.ok(adp.products.includes("SHARED"));
    const gdi = resolveProductImpact({
      primaryProduct: "GDI",
      changedFiles: ["lib/group-demand-intelligence/x.js"],
      matrix,
    });
    assert.ok(gdi.products.includes("GDI"));
    pass("product_impact_resolver");
  }

  // 16) Manifest rejects illegal skip
  {
    const man = buildReleaseManifestV1({
      gitSha: sha,
      readyForProduction: true,
      cleanCheckout: { ok: true, checkoutPath: "x", attestationId: "y" },
      publicationMutationsAuthorized: false,
      testResults: [{ id: "assert:production-assets", status: "MISSING" }],
    });
    // readyForProduction builder forces false when failures present — set explicitly
    man.readyForProduction = true;
    man.testResults = [{ id: "assert:production-assets", status: "MISSING" }];
    const check = assertManifestReadyForProduction(man);
    assert.equal(check.ok, false);
    pass("manifest_rejects_missing_test");
  }

  // 17) Optional real worktree smoke (skip if git fails)
  {
    const headProc = spawnSync("git", ["rev-parse", "HEAD"], {
      cwd: ROOT,
      encoding: "utf8",
    });
    const head = String(headProc.stdout || "").trim().toLowerCase();
    if (/^[0-9a-f]{40}$/.test(head)) {
      const created = createCleanCheckoutWorktree({
        repoRoot: ROOT,
        sha: head,
        outDir: path.join(
          os.tmpdir(),
          `dealality-cca-wt-${Date.now().toString(36)}`
        ),
      });
      if (created.ok) {
        // Only assert paths that already exist on the committed HEAD tip.
        // Uncommitted gate modules under development must not fail this smoke.
        const integrity = verifyCheckoutIntegrity({
          checkoutRoot: created.outDir,
          expectedSha: head,
          requireGitHead: true,
          requiredPaths: [
            "package.json",
            "package-lock.json",
            "server.js",
            "config/production-required-assets.json",
            "scripts/deploy-production.mjs",
            "scripts/dealality-production-release-coordinator-v1.mjs",
            "lib/dealality-production-release/release-lock-v1.js",
          ],
        });
        removeCleanCheckoutWorktree({
          repoRoot: ROOT,
          outDir: created.outDir,
        });
        assert.equal(integrity.ok, true, JSON.stringify(integrity.failures));
        pass("real_clean_checkout_worktree_smoke", head.slice(0, 12));
      } else {
        pass("real_clean_checkout_worktree_smoke", `skipped:${created.code}`);
      }
    } else {
      pass("real_clean_checkout_worktree_smoke", "skipped:no_head");
    }
  }
} catch (err) {
  fail("uncaught", err && err.stack ? err.stack : err);
}

const summary = {
  contract: "DEALALITY_CLEAN_CHECKOUT_RELEASE_GATE_TEST_RESULTS_V1",
  ok: results.every((r) => r.pass),
  total: results.length,
  passed: results.filter((r) => r.pass).length,
  failed: results.filter((r) => !r.pass).length,
  results,
};
const outPath = path.join(
  ROOT,
  "reports/dealality-clean-checkout-release-gate-v1/DEALALITY_RELEASE_INTEGRITY_TEST_RESULTS.md"
);
const jsonPath = path.join(
  ROOT,
  "reports/dealality-clean-checkout-release-gate-v1/CLEAN_CHECKOUT_GATE_TEST_RESULTS.json"
);
fs.mkdirSync(path.dirname(jsonPath), { recursive: true });
fs.writeFileSync(jsonPath, `${JSON.stringify(summary, null, 2)}\n`);

console.log(
  JSON.stringify(
    { ok: summary.ok, passed: summary.passed, failed: summary.failed, jsonPath },
    null,
    2
  )
);
process.exit(summary.ok ? 0 : 1);
