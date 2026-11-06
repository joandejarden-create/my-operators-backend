/**
 * Immutable Dealality production release manifest builder.
 */
import fs from "node:fs";
import path from "node:path";

export const RELEASE_MANIFEST_CONTRACT = "DEALALITY_RELEASE_MANIFEST_V1";
export const RELEASE_MANIFEST_VERSION = "1.0.0";

export function buildReleaseManifestV1(input = {}) {
  const ready =
    input.readyForProduction === true &&
    !(Array.isArray(input.failures) && input.failures.length > 0);

  return {
    contract: RELEASE_MANIFEST_CONTRACT,
    version: RELEASE_MANIFEST_VERSION,
    gitSha: String(input.gitSha || "").toLowerCase() || null,
    sourceBranch: input.sourceBranch ?? null,
    buildTimestamp: input.buildTimestamp || new Date().toISOString(),
    dependencyLockfileHash: input.dependencyLockfileHash ?? null,
    productionAssetInventoryHash: input.productionAssetInventoryHash ?? null,
    buildArtifactHash: input.buildArtifactHash ?? null,
    productModulesIncluded: Array.isArray(input.productModulesIncluded)
      ? input.productModulesIncluded
      : [],
    requiredTests: Array.isArray(input.requiredTests) ? input.requiredTests : [],
    testResults: Array.isArray(input.testResults) ? input.testResults : [],
    deploymentAuthorizationReference:
      input.deploymentAuthorizationReference ?? null,
    previousProductionDeployment: input.previousProductionDeployment ?? null,
    rollbackTarget: input.rollbackTarget ?? null,
    cleanCheckout: input.cleanCheckout || {
      ok: false,
      checkoutPath: null,
      attestationId: null,
      dirtyWorktreeRejected: false,
      leanStageRejectedAsSource: false,
    },
    publicationMutationsAuthorized: input.publicationMutationsAuthorized === true,
    readyForProduction: ready,
    releaseId: input.releaseId ?? null,
    product: input.product ?? null,
    failures: Array.isArray(input.failures) ? input.failures : [],
  };
}

export function assertManifestReadyForProduction(manifest) {
  if (!manifest || manifest.contract !== RELEASE_MANIFEST_CONTRACT) {
    return { ok: false, code: "MANIFEST_CONTRACT_INVALID" };
  }
  if (manifest.readyForProduction !== true) {
    return {
      ok: false,
      code: "NOT_READY_FOR_PRODUCTION",
      failures: manifest.failures || [],
    };
  }
  if (!manifest.cleanCheckout?.ok) {
    return { ok: false, code: "CLEAN_CHECKOUT_NOT_OK" };
  }
  if (manifest.publicationMutationsAuthorized === true) {
    return {
      ok: false,
      code: "PUBLICATION_FLAG_MUST_BE_FALSE_ON_CODE_RELEASE",
      detail:
        "Code-only release manifests must keep publicationMutationsAuthorized=false",
    };
  }
  const illegalSkip = (manifest.testResults || []).some(
    (t) => t.status === "SKIPPED_ILLEGAL" || t.status === "MISSING"
  );
  if (illegalSkip) {
    return { ok: false, code: "MANDATORY_TEST_GAP" };
  }
  return { ok: true };
}

export function writeReleaseManifest(opts = {}) {
  const dir = opts.outDir;
  fs.mkdirSync(dir, { recursive: true });
  const manifest = opts.manifest || buildReleaseManifestV1(opts);
  const name =
    opts.fileName ||
    `${manifest.releaseId || "release"}_${String(manifest.gitSha || "unknown").slice(0, 12)}.json`;
  const fp = path.join(dir, name);
  fs.writeFileSync(fp, `${JSON.stringify(manifest, null, 2)}\n`);
  return { ok: true, path: fp, manifest };
}

export function loadLatestProductionDeployRecord(root = process.cwd()) {
  const latest = path.join(root, "reports", "deployments", "latest-production.json");
  if (!fs.existsSync(latest)) return null;
  try {
    return JSON.parse(fs.readFileSync(latest, "utf8"));
  } catch {
    return null;
  }
}
