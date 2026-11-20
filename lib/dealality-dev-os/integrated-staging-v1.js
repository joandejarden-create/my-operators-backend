/**
 * Dealality Integrated Staging V1 — port 8080 ownership + staging identity.
 *
 * Production separation: updating staging never deploys Railway/production.
 * Product worktrees (ws-*) must not claim port 8080.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const DEALALITY_INTEGRATED_STAGING_V1 = "dealality-integrated-staging-v1";
export const INTEGRATED_STAGING_PORT = 8080;
export const INTEGRATED_STAGING_BASENAME = "ws-integrated-staging";
export const INTEGRATED_STAGING_MARKER = ".dealality-integrated-staging";
export const INTEGRATED_STAGING_DEFAULT_PATH =
  "C:/Dev/dealality/ws-integrated-staging";
export const INTEGRATED_STAGING_HOME_BRANCH = "ws/integrated-staging";

/** Basenames that may listen on 8080 (in addition to staging marker). */
export const PORT_8080_ALLOWED_BASENAMES = Object.freeze([
  INTEGRATED_STAGING_BASENAME,
  // Legacy primary — allowed only until IC migrates founder traffic to staging.
  "my-operators-backend",
  "my-operators-backend-canonical-main",
]);

/** Product / feature homes that must never claim 8080. */
export const PRODUCT_WORKTREE_BASENAMES = Object.freeze([
  "ws-gdi",
  "ws-adp",
  "ws-hotel-explorer",
  "ws-brand-explorer",
]);

function normalizePath(p) {
  return String(p || "")
    .replace(/\\/g, "/")
    .replace(/\/+$/, "");
}

export function checkoutBasename(cwd = process.cwd()) {
  const norm = normalizePath(cwd);
  const parts = norm.split("/");
  return parts[parts.length - 1] || "";
}

export function hasIntegratedStagingMarker(cwd = process.cwd()) {
  try {
    return fs.existsSync(path.join(cwd, INTEGRATED_STAGING_MARKER));
  } catch {
    return false;
  }
}

export function isIntegratedStagingCheckout(cwd = process.cwd()) {
  const base = checkoutBasename(cwd);
  if (base === INTEGRATED_STAGING_BASENAME) return true;
  return hasIntegratedStagingMarker(cwd);
}

export function isProductWorktreeCheckout(cwd = process.cwd()) {
  return PRODUCT_WORKTREE_BASENAMES.includes(checkoutBasename(cwd));
}

/**
 * Whether this checkout may bind port 8080.
 * DEALALITY_CANONICAL_CHECKOUT=1 is NO LONGER sufficient alone — product
 * worktrees must not override 8080 ownership.
 */
export function mayBindPort8080(cwd = process.cwd(), env = process.env) {
  if (isProductWorktreeCheckout(cwd)) return false;
  if (isIntegratedStagingCheckout(cwd)) return true;
  const base = checkoutBasename(cwd);
  if (PORT_8080_ALLOWED_BASENAMES.includes(base)) return true;
  // Explicit staging identity via env (CI / tests only)
  if (env.DEALALITY_INTEGRATED_STAGING === "1" && env.DEALALITY_STAGING_FORCE === "1") {
    return true;
  }
  return false;
}

export function assertPort8080Ownership(cwd = process.cwd(), env = process.env) {
  const port = Number(env.PORT || INTEGRATED_STAGING_PORT);
  if (port !== INTEGRATED_STAGING_PORT) {
    return { ok: true, port, reason: "non_8080" };
  }
  if (mayBindPort8080(cwd, env)) {
    return {
      ok: true,
      port,
      reason: isIntegratedStagingCheckout(cwd)
        ? "integrated_staging"
        : "legacy_canonical_basename",
      basename: checkoutBasename(cwd),
    };
  }
  const err = new Error(
    `Port ${INTEGRATED_STAGING_PORT} is reserved for Dealality Integrated Staging (` +
      `${INTEGRATED_STAGING_DEFAULT_PATH}). Current checkout: ${cwd}`,
  );
  err.code = "PORT_8080_OWNERSHIP_DENIED";
  err.basename = checkoutBasename(cwd);
  throw err;
}

export function emptyStagingState(overrides = {}) {
  return {
    schema: DEALALITY_INTEGRATED_STAGING_V1,
    environment: "INTEGRATED_STAGING",
    productionIsolated: true,
    autoDeployToProduction: false,
    port: INTEGRATED_STAGING_PORT,
    stagingPath: INTEGRATED_STAGING_DEFAULT_PATH,
    homeBranch: INTEGRATED_STAGING_HOME_BRANCH,
    baseMainSha: null,
    baseMainShort: null,
    headSha: null,
    headShort: null,
    stagedFeatureCommits: [],
    includedFeatures: {
      mergedIntoMain: [],
      stagingOnly: [],
      notIntegrated: [],
    },
    lastRefreshAt: null,
    lastRefreshMode: null,
    validationStatus: "NOT_RUN",
    serverStatus: "UNKNOWN",
    knownGood: null,
    notes: [],
    ...overrides,
  };
}

export function stagingStatePath(stagingRoot) {
  return path.join(
    stagingRoot,
    "data",
    "dealality-integrated-staging",
    "CURRENT.json",
  );
}

export function knownGoodStatePath(stagingRoot) {
  return path.join(
    stagingRoot,
    "data",
    "dealality-integrated-staging",
    "KNOWN_GOOD.json",
  );
}

export function readStagingState(stagingRoot) {
  const file = stagingStatePath(stagingRoot);
  if (!fs.existsSync(file)) return emptyStagingState({ stagingPath: stagingRoot });
  try {
    const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
    return { ...emptyStagingState({ stagingPath: stagingRoot }), ...parsed };
  } catch {
    return emptyStagingState({
      stagingPath: stagingRoot,
      notes: ["CURRENT.json unreadable — treating as empty"],
    });
  }
}

export function writeStagingState(stagingRoot, state) {
  const file = stagingStatePath(stagingRoot);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.${process.pid}.tmp`;
  const doc = {
    ...emptyStagingState({ stagingPath: stagingRoot }),
    ...state,
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(tmp, `${JSON.stringify(doc, null, 2)}\n`);
  fs.renameSync(tmp, file);
  return doc;
}

export function promoteKnownGood(stagingRoot, state) {
  const file = knownGoodStatePath(stagingRoot);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const doc = {
    ...state,
    promotedAt: new Date().toISOString(),
  };
  fs.writeFileSync(file, `${JSON.stringify(doc, null, 2)}\n`);
  return doc;
}

export function readKnownGood(stagingRoot) {
  const file = knownGoodStatePath(stagingRoot);
  if (!fs.existsSync(file)) return null;
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
}

export function writeStagingMarker(stagingRoot, meta = {}) {
  const marker = path.join(stagingRoot, INTEGRATED_STAGING_MARKER);
  const body = {
    schema: DEALALITY_INTEGRATED_STAGING_V1,
    port: INTEGRATED_STAGING_PORT,
    createdAt: new Date().toISOString(),
    ...meta,
  };
  fs.writeFileSync(marker, `${JSON.stringify(body, null, 2)}\n`);
  return marker;
}

export function classifyFeatureInclusion(state = {}) {
  const merged = Array.isArray(state.includedFeatures?.mergedIntoMain)
    ? state.includedFeatures.mergedIntoMain
    : [];
  const stagingOnly = Array.isArray(state.includedFeatures?.stagingOnly)
    ? state.includedFeatures.stagingOnly
    : [];
  const notIntegrated = Array.isArray(state.includedFeatures?.notIntegrated)
    ? state.includedFeatures.notIntegrated
    : [];
  return { mergedIntoMain: merged, stagingOnly, notIntegrated };
}

export function buildEnvironmentStatusPayload(state, extras = {}) {
  const features = classifyFeatureInclusion(state);
  return {
    ok: true,
    schema: DEALALITY_INTEGRATED_STAGING_V1,
    environment: "INTEGRATED_STAGING",
    productionIsolated: true,
    autoDeployToProduction: false,
    port: INTEGRATED_STAGING_PORT,
    baseMainCommit: state.baseMainSha || null,
    baseMainShort: state.baseMainShort || null,
    headCommit: state.headSha || null,
    headShort: state.headShort || null,
    additionalStagedFeatureCommits: state.stagedFeatureCommits || [],
    lastStagingRefresh: state.lastRefreshAt || null,
    lastRefreshMode: state.lastRefreshMode || null,
    serverStatus: extras.serverStatus || state.serverStatus || "UNKNOWN",
    validationStatus: state.validationStatus || "NOT_RUN",
    features: {
      mergedIntoMain: features.mergedIntoMain,
      includedOnlyInLocalStaging: features.stagingOnly,
      notYetIntegrated: features.notIntegrated,
    },
    knownGood: state.knownGood || null,
    disclaimer:
      "Unmerged staging-only features are NOT production-ready. Staging updates never auto-deploy.",
    generatedAt: new Date().toISOString(),
  };
}

/** Resolve package root when running from lib/ */
export function packageRootFromHere() {
  return path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
}
