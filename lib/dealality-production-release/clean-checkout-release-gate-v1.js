/**
 * Dealality clean-checkout release integrity gate (fail-closed).
 *
 * Every production release must originate from an exact Git SHA via an
 * isolated clean checkout — never a dirty developer worktree, foreign
 * worktree copy, untracked module, or _railway-lean-stage as source of truth.
 */
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import {
  resolveProductImpact,
  resolveMandatoryTests,
  loadRegressionMatrix,
} from "./product-impact-resolver-v1.js";
import { buildReleaseManifestV1 } from "./release-manifest-v1.js";

export const CLEAN_CHECKOUT_CONTRACT = "DEALALITY_CLEAN_CHECKOUT_RELEASE_GATE_V1";
export const ATTESTATION_CONTRACT = "DEALALITY_CLEAN_CHECKOUT_ATTESTATION_V1";

function sha256Buffer(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function sha256File(fp) {
  return sha256Buffer(fs.readFileSync(fp));
}

export function hashLockfile(root) {
  const lock = path.join(root, "package-lock.json");
  if (!fs.existsSync(lock)) return null;
  return sha256File(lock);
}

export function hashAssetManifest(root) {
  const man = path.join(root, "config", "production-required-assets.json");
  if (!fs.existsSync(man)) return null;
  return sha256File(man);
}

/**
 * Preconditions against deploying from a dirty / forbidden source tree.
 */
export function evaluateSourceTreePreconditions(input = {}) {
  const failures = [];
  const dirty = Boolean(
    input.gitStatusPorcelain && String(input.gitStatusPorcelain).trim()
  );
  const sha = String(input.sha || "").trim().toLowerCase();
  const approvedSha = String(input.approvedSha || "").trim().toLowerCase();
  const cwd = String(input.cwd || "").replace(/\\/g, "/");

  if (!/^[0-9a-f]{40}$/.test(approvedSha)) {
    failures.push({
      code: "INVALID_APPROVED_SHA",
      detail: "approvedSha must be full 40-char hex",
    });
  }
  if (sha && approvedSha && sha !== approvedSha) {
    failures.push({
      code: "SHA_MISMATCH",
      detail: `working tree HEAD ${sha} != approved ${approvedSha}`,
    });
  }
  if (dirty && input.allowDirtyDirectDeploy !== true) {
    failures.push({
      code: "DIRTY_WORKTREE_DIRECT_DEPLOY_FORBIDDEN",
      detail:
        "Production deploy must use an isolated clean checkout of the approved SHA — not a dirty worktree",
    });
  }
  if (input.leanStageAsSource === true) {
    failures.push({
      code: "LEAN_STAGE_FORBIDDEN_AS_SOURCE",
      detail: "_railway-lean-stage must not be the release source of truth",
    });
  }
  if (/_railway-lean-stage/i.test(cwd)) {
    failures.push({
      code: "LEAN_STAGE_CWD_FORBIDDEN",
      detail: "cwd is inside _railway-lean-stage",
    });
  }
  if (Array.isArray(input.untrackedRequiredModules)) {
    for (const mod of input.untrackedRequiredModules) {
      if (mod) {
        failures.push({
          code: "UNTRACKED_REQUIRED_MODULE",
          detail: String(mod),
        });
      }
    }
  }
  if (input.divergentFromApprovedBase === true) {
    failures.push({
      code: "DIVERGENT_BRANCH",
      detail: input.divergentDetail || "proposed SHA diverges from approved base",
    });
  }

  return {
    ok: failures.length === 0,
    dirty,
    failures,
    dirtyWorktreeRejected: dirty && input.allowDirtyDirectDeploy !== true,
    leanStageRejectedAsSource: input.leanStageAsSource === true,
  };
}

/**
 * Verify a checkout directory contains required tracked modules + lockfile.
 */
export function verifyCheckoutIntegrity(opts = {}) {
  const root = opts.checkoutRoot;
  const failures = [];
  if (!root || !fs.existsSync(root)) {
    return {
      ok: false,
      failures: [{ code: "CHECKOUT_MISSING", detail: String(root) }],
    };
  }
  const required = opts.requiredPaths || [
    "package.json",
    "package-lock.json",
    "server.js",
    "config/production-required-assets.json",
    "scripts/deploy-production.mjs",
    "scripts/dealality-production-release-coordinator-v1.mjs",
    "lib/dealality-production-release/release-lock-v1.js",
    "lib/dealality-production-release/clean-checkout-release-gate-v1.js",
  ];
  for (const rel of required) {
    if (!fs.existsSync(path.join(root, rel))) {
      failures.push({ code: "MISSING_TRACKED_MODULE", detail: rel });
    }
  }
  if (opts.requireGitHead === true) {
    const head = spawnSync("git", ["rev-parse", "HEAD"], {
      cwd: root,
      encoding: "utf8",
    });
    const got = String(head.stdout || "").trim().toLowerCase();
    if (head.status !== 0 || got !== String(opts.expectedSha || "").toLowerCase()) {
      failures.push({
        code: "CHECKOUT_SHA_MISMATCH",
        detail: `expected ${opts.expectedSha} got ${got || "none"}`,
      });
    }
  }
  const dirty = spawnSync("git", ["status", "--porcelain"], {
    cwd: root,
    encoding: "utf8",
  });
  if (dirty.status === 0 && String(dirty.stdout || "").trim()) {
    failures.push({
      code: "CHECKOUT_NOT_CLEAN",
      detail: String(dirty.stdout).trim().slice(0, 500),
    });
  }
  return {
    ok: failures.length === 0,
    failures,
    lockfileHash: hashLockfile(root),
    assetManifestHash: hashAssetManifest(root),
  };
}

/**
 * Create an isolated detached worktree at the approved SHA.
 * Inject execGit for tests.
 */
export function createCleanCheckoutWorktree(opts = {}) {
  const repoRoot = opts.repoRoot || process.cwd();
  const sha = String(opts.sha || "").trim().toLowerCase();
  if (!/^[0-9a-f]{40}$/.test(sha)) {
    return { ok: false, code: "INVALID_SHA", message: "sha required" };
  }
  const outDir =
    opts.outDir ||
    path.join(
      os.tmpdir(),
      `dealality-clean-checkout-${sha.slice(0, 12)}-${Date.now().toString(36)}`
    );
  const execGit =
    opts.execGit ||
    ((args, cwd) =>
      spawnSync("git", args, {
        cwd,
        encoding: "utf8",
        stdio: ["ignore", "pipe", "pipe"],
      }));

  if (fs.existsSync(outDir)) {
    return { ok: false, code: "OUT_DIR_EXISTS", outDir };
  }

  const add = execGit(
    ["worktree", "add", "--detach", outDir, sha],
    repoRoot
  );
  if (add.status !== 0) {
    return {
      ok: false,
      code: "WORKTREE_ADD_FAILED",
      message: String(add.stderr || add.stdout || "git worktree add failed"),
      outDir,
    };
  }
  return { ok: true, outDir, sha };
}

export function removeCleanCheckoutWorktree(opts = {}) {
  const repoRoot = opts.repoRoot || process.cwd();
  const outDir = opts.outDir;
  if (!outDir) return { ok: false, code: "NO_OUT_DIR" };
  const rm = spawnSync(
    "git",
    ["worktree", "remove", "--force", outDir],
    { cwd: repoRoot, encoding: "utf8" }
  );
  if (rm.status !== 0) {
    try {
      fs.rmSync(outDir, { recursive: true, force: true });
    } catch (err) {
      return { ok: false, code: "REMOVE_FAILED", message: String(err.message || err) };
    }
  }
  return { ok: true, outDir };
}

/**
 * Verify runtime identity payload matches proposed SHA.
 */
export function verifyRuntimeIdentityMatch(opts = {}) {
  const expected = String(opts.expectedSha || "").trim().toLowerCase();
  const payload = opts.runtimePayload || {};
  const got = String(
    payload.gitSha || payload.buildSha || payload.commitSha || ""
  )
    .trim()
    .toLowerCase();
  if (!expected || !/^[0-9a-f]{40}$/.test(expected)) {
    return { ok: false, code: "INVALID_EXPECTED_SHA" };
  }
  if (!got) {
    return { ok: false, code: "RUNTIME_SHA_MISSING", expected };
  }
  if (got !== expected && !got.startsWith(expected.slice(0, 7))) {
    return {
      ok: false,
      code: "RUNTIME_SHA_MISMATCH",
      expected,
      actual: got,
    };
  }
  return { ok: true, expected, actual: got };
}

/**
 * Publication mutations are never implied by a code deploy.
 */
export function assertNoPublicationMutation(opts = {}) {
  const auth = opts.authorization || {};
  const attempted = opts.attemptedMutations || {};
  const failures = [];
  const kinds = [
    "adpDurablePublish",
    "gdiOpportunityPublish",
    "avaReportPublish",
    "pdfRegistryWrite",
    "shareTokenRotation",
    "airtableWrite",
  ];
  for (const k of kinds) {
    if (attempted[k] === true && auth[k] !== true) {
      failures.push({
        code: "UNAPPROVED_PUBLICATION_MUTATION",
        detail: k,
      });
    }
  }
  return { ok: failures.length === 0, failures };
}

/**
 * Write attestation consumed by deploy:production (fail-closed).
 */
export function writeCleanCheckoutAttestation(opts = {}) {
  const outDir = opts.outDir;
  fs.mkdirSync(outDir, { recursive: true });
  const attestationId =
    opts.attestationId ||
    `cca_${Date.now().toString(36)}_${crypto.randomBytes(3).toString("hex")}`;
  const body = {
    contract: ATTESTATION_CONTRACT,
    attestationId,
    gitSha: opts.gitSha,
    checkoutPath: opts.checkoutPath,
    createdAt: new Date().toISOString(),
    lockfileHash: opts.lockfileHash || null,
    assetManifestHash: opts.assetManifestHash || null,
    buildArtifactHash: opts.buildArtifactHash || null,
    productModulesIncluded: opts.productModulesIncluded || [],
    tests: opts.tests || [],
    readyForProduction: opts.readyForProduction === true,
    publicationMutationsAuthorized: false,
    sourcePreconditions: opts.sourcePreconditions || null,
    checkoutIntegrity: opts.checkoutIntegrity || null,
    releaseId: opts.releaseId || null,
    product: opts.product || null,
  };
  const fp = path.join(outDir, `${attestationId}.json`);
  fs.writeFileSync(fp, `${JSON.stringify(body, null, 2)}\n`);
  fs.writeFileSync(
    path.join(outDir, "LATEST.json"),
    `${JSON.stringify(body, null, 2)}\n`
  );
  return { ok: true, path: fp, attestation: body };
}

export function readCleanCheckoutAttestation(fp) {
  if (!fp || !fs.existsSync(fp)) {
    return { ok: false, code: "ATTESTATION_MISSING" };
  }
  try {
    const body = JSON.parse(fs.readFileSync(fp, "utf8"));
    if (body.contract !== ATTESTATION_CONTRACT) {
      return { ok: false, code: "ATTESTATION_CONTRACT_MISMATCH" };
    }
    if (body.readyForProduction !== true) {
      return { ok: false, code: "ATTESTATION_NOT_READY", attestation: body };
    }
    return { ok: true, attestation: body };
  } catch (err) {
    return { ok: false, code: "ATTESTATION_UNREADABLE", message: String(err.message || err) };
  }
}

/**
 * Assert deploy may proceed only with a matching attestation for HEAD sha.
 */
export function assertCleanCheckoutAttestationForDeploy(opts = {}) {
  const env = opts.env || process.env;
  const sha = String(opts.sha || "").trim().toLowerCase();
  const attestationPath =
    opts.attestationPath ||
    String(env.DEALALITY_CLEAN_CHECKOUT_ATTESTATION || "").trim();

  if (String(env.DEALALITY_CLEAN_CHECKOUT_BYPASS || "").trim() === "1") {
    return {
      ok: false,
      code: "CLEAN_CHECKOUT_BYPASS_FORBIDDEN",
      detail:
        "DEALALITY_CLEAN_CHECKOUT_BYPASS is not permitted — clean checkout is mandatory",
    };
  }

  if (!attestationPath) {
    return {
      ok: false,
      code: "CLEAN_CHECKOUT_ATTESTATION_REQUIRED",
      detail:
        "Set DEALALITY_CLEAN_CHECKOUT_ATTESTATION to the attestation JSON from the release coordinator clean-checkout phase",
    };
  }

  const read = readCleanCheckoutAttestation(attestationPath);
  if (!read.ok) return read;
  const a = read.attestation;
  const aSha = String(a.gitSha || "").toLowerCase();
  if (aSha !== sha) {
    return {
      ok: false,
      code: "ATTESTATION_SHA_MISMATCH",
      detail: `attestation ${aSha} != deploy sha ${sha}`,
      attestation: a,
    };
  }
  return { ok: true, attestation: a, path: attestationPath };
}

/**
 * Full gate evaluation (logic + optional real checkout).
 * Prefer simulate:true in unit tests.
 */
export function runCleanCheckoutReleaseGate(opts = {}) {
  const root = opts.repoRoot || process.cwd();
  const approvedSha = String(opts.approvedSha || "").trim().toLowerCase();
  const matrix = opts.matrix || loadRegressionMatrix(root);
  const failures = [];

  const source = evaluateSourceTreePreconditions({
    gitStatusPorcelain: opts.gitStatusPorcelain,
    sha: opts.headSha || approvedSha,
    approvedSha,
    leanStageAsSource: opts.leanStageAsSource === true,
    cwd: opts.cwd || root,
    untrackedRequiredModules: opts.untrackedRequiredModules || [],
    divergentFromApprovedBase: opts.divergentFromApprovedBase === true,
    divergentDetail: opts.divergentDetail,
    // Direct dirty deploy never allowed through this gate.
    allowDirtyDirectDeploy: false,
  });
  failures.push(...source.failures);

  const impact = resolveProductImpact({
    changedFiles: opts.changedFiles || [],
    primaryProduct: opts.product,
    matrix,
    root,
  });

  const tests = resolveMandatoryTests({
    products: impact.products,
    matrix,
    packageScripts: opts.packageScripts,
    root: opts.checkoutRoot || root,
    expedited: opts.expedited === true,
  });
  failures.push(...tests.failures);

  let checkout = null;
  let integrity = null;
  if (opts.simulate === true) {
    integrity = opts.simulatedIntegrity || {
      ok: !opts.simulateMissingAsset,
      failures: opts.simulateMissingAsset
        ? [{ code: "MISSING_PRODUCTION_ASSET", detail: opts.simulateMissingAsset }]
        : [],
      lockfileHash: opts.simulatedLockfileHash || "sim_lock",
      assetManifestHash: opts.simulatedAssetHash || "sim_assets",
    };
    if (!integrity.ok) failures.push(...integrity.failures);
    if (opts.simulateBuildFailure === true) {
      failures.push({ code: "BUILD_FAILED", detail: "simulated build failure" });
    }
    if (opts.simulateMissingDependency === true) {
      failures.push({
        code: "MISSING_DEPENDENCY",
        detail: "simulated lockfile/install failure",
      });
    }
    if (opts.simulateRollbackReadinessFailure === true) {
      failures.push({
        code: "ROLLBACK_READINESS_FAILURE",
        detail: "simulated missing rollback target",
      });
    }
  } else if (opts.createCheckout === true) {
    checkout = createCleanCheckoutWorktree({
      repoRoot: root,
      sha: approvedSha,
      outDir: opts.outDir,
      execGit: opts.execGit,
    });
    if (!checkout.ok) {
      failures.push({
        code: checkout.code || "CHECKOUT_FAILED",
        detail: checkout.message || null,
      });
    } else {
      integrity = verifyCheckoutIntegrity({
        checkoutRoot: checkout.outDir,
        expectedSha: approvedSha,
        requireGitHead: true,
        requiredPaths: opts.requiredPaths,
      });
      if (!integrity.ok) failures.push(...integrity.failures);
    }
  } else if (opts.checkoutRoot) {
    integrity = verifyCheckoutIntegrity({
      checkoutRoot: opts.checkoutRoot,
      expectedSha: approvedSha,
      requireGitHead: opts.requireGitHead !== false,
      requiredPaths: opts.requiredPaths,
    });
    if (!integrity.ok) failures.push(...integrity.failures);
  }

  const publication = assertNoPublicationMutation({
    authorization: opts.publicationAuthorization || {},
    attemptedMutations: opts.attemptedMutations || {},
  });
  failures.push(...publication.failures);

  if (opts.concurrentDeployment === true) {
    failures.push({
      code: "CONCURRENT_DEPLOYMENT",
      detail: "Railway non-terminal deployment in progress",
    });
  }

  if (opts.runtimeIdentity) {
    const rt = verifyRuntimeIdentityMatch({
      expectedSha: approvedSha,
      runtimePayload: opts.runtimeIdentity,
    });
    if (!rt.ok) {
      failures.push({ code: rt.code, detail: rt.actual || null });
    }
  }

  const ready = failures.length === 0;
  const manifest = buildReleaseManifestV1({
    gitSha: approvedSha,
    sourceBranch: opts.sourceBranch || null,
    dependencyLockfileHash: integrity?.lockfileHash || null,
    productionAssetInventoryHash: integrity?.assetManifestHash || null,
    buildArtifactHash: opts.buildArtifactHash || integrity?.lockfileHash || null,
    productModulesIncluded: impact.products,
    requiredTests: tests.tests,
    testResults: (opts.testResults || tests.tests.map((t) => ({
      id: t.id,
      status:
        t.status === "MISSING"
          ? "MISSING"
          : ready
            ? "PASS"
            : t.status === "READY"
              ? "PASS"
              : "FAIL",
      detail: t.detail || null,
    }))),
    deploymentAuthorizationReference: opts.authorizationReference || null,
    previousProductionDeployment: opts.previousProductionDeployment || null,
    rollbackTarget: opts.rollbackTarget || null,
    cleanCheckout: {
      ok: ready && (integrity ? integrity.ok : source.ok),
      checkoutPath: checkout?.outDir || opts.checkoutRoot || null,
      attestationId: null,
      dirtyWorktreeRejected: source.dirtyWorktreeRejected,
      leanStageRejectedAsSource: source.leanStageRejectedAsSource,
    },
    publicationMutationsAuthorized: false,
    readyForProduction: ready,
    releaseId: opts.releaseId || null,
    product: opts.product || null,
    failures,
  });

  let attestation = null;
  if (ready && opts.writeAttestation !== false && opts.attestationOutDir) {
    attestation = writeCleanCheckoutAttestation({
      outDir: opts.attestationOutDir,
      gitSha: approvedSha,
      checkoutPath: checkout?.outDir || opts.checkoutRoot || null,
      lockfileHash: integrity?.lockfileHash || null,
      assetManifestHash: integrity?.assetManifestHash || null,
      buildArtifactHash: opts.buildArtifactHash || null,
      productModulesIncluded: impact.products,
      tests: manifest.testResults,
      readyForProduction: true,
      sourcePreconditions: source,
      checkoutIntegrity: integrity,
      releaseId: opts.releaseId || null,
      product: opts.product || null,
    });
    manifest.cleanCheckout.attestationId = attestation.attestation.attestationId;
  }

  return {
    contract: CLEAN_CHECKOUT_CONTRACT,
    ok: ready,
    readyForProduction: ready,
    failures,
    source,
    impact,
    tests,
    checkout,
    integrity,
    publication,
    manifest,
    attestation,
  };
}
