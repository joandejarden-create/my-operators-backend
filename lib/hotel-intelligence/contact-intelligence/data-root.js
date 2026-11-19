/**
 * Contact Intelligence runtime data root resolution + cross-worktree write guard.
 *
 * Precedence (existing):
 *   1. CONTACT_INTELLIGENCE_DATA_ROOT
 *   2. HOTEL_INTELLIGENCE_DATA_DIR/contact-intelligence
 *   3. <serving-repo>/data/hotel-intelligence/contact-intelligence
 *
 * Fail-closed: refuse roots that resolve inside a *foreign* registered workspace.
 * Does not invent a new file-store — configures the existing CI store.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadWorktreeRegistry,
  listRegisteredWorkspaces,
  findWorkspaceByPath,
  normalizePath,
} from "../../dealality-dev-os/worktree-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
/** Repo root that contains this module (the checkout serving the code). */
export const CONTACT_INTELLIGENCE_MODULE_REPO_ROOT = path.resolve(__dirname, "../../..");

export const CONTACT_INTELLIGENCE_DEFAULT_RELATIVE =
  "data/hotel-intelligence/contact-intelligence";

/**
 * @param {object} [opts]
 * @param {NodeJS.ProcessEnv} [opts.env]
 * @param {string} [opts.repoRoot] - override serving repo root
 * @returns {{ root: string, source: string, envOverride: boolean }}
 */
export function resolveContactIntelligenceDataRoot(opts = {}) {
  const env = opts.env || process.env;
  const repoRoot = path.resolve(opts.repoRoot || CONTACT_INTELLIGENCE_MODULE_REPO_ROOT);

  if (env.CONTACT_INTELLIGENCE_DATA_ROOT) {
    return {
      root: path.resolve(String(env.CONTACT_INTELLIGENCE_DATA_ROOT)),
      source: "CONTACT_INTELLIGENCE_DATA_ROOT",
      envOverride: true,
    };
  }
  if (env.HOTEL_INTELLIGENCE_DATA_DIR) {
    return {
      root: path.join(
        path.resolve(String(env.HOTEL_INTELLIGENCE_DATA_DIR)),
        "contact-intelligence",
      ),
      source: "HOTEL_INTELLIGENCE_DATA_DIR",
      envOverride: true,
    };
  }
  return {
    root: path.join(repoRoot, CONTACT_INTELLIGENCE_DEFAULT_RELATIVE),
    source: "MODULE_REPO_DEFAULT",
    envOverride: false,
  };
}

function workspaceContainingPath(registry, absPath) {
  const needle = normalizePath(absPath);
  const workspaces = listRegisteredWorkspaces(registry);
  // Longest registered path prefix wins (nested paths unlikely but safe).
  let best = null;
  for (const ws of workspaces) {
    const base = ws.pathNormalized;
    if (needle === base || needle.startsWith(`${base}/`)) {
      if (!best || base.length > best.pathNormalized.length) best = ws;
    }
  }
  return best;
}

/**
 * Ensure a Contact Intelligence data root is not under a foreign registered workspace.
 * @returns {{ ok: true, root: string, servingWorkspace: object|null, dataWorkspace: object|null, source: string }
 *          |{ ok: false, error: string, root: string, servingWorkspace: object|null, dataWorkspace: object|null, source: string }}
 */
export function assertContactIntelligenceDataRootAffinity(opts = {}) {
  const env = opts.env || process.env;
  const resolved = resolveContactIntelligenceDataRoot(opts);
  const repoRoot = path.resolve(opts.repoRoot || CONTACT_INTELLIGENCE_MODULE_REPO_ROOT);
  let registry;
  try {
    registry = opts.registry || loadWorktreeRegistry(repoRoot);
  } catch (err) {
    // Registry missing → soft-pass with warning metadata (tests / non-registry checkouts).
    return {
      ok: true,
      root: resolved.root,
      source: resolved.source,
      servingWorkspace: null,
      dataWorkspace: null,
      registryWarning: String(err?.message || err).slice(0, 160),
    };
  }

  const servingWorkspace =
    findWorkspaceByPath(registry, repoRoot) ||
    workspaceContainingPath(registry, repoRoot);
  const dataWorkspace = workspaceContainingPath(registry, resolved.root);

  const allowCross =
    String(env.CONTACT_INTELLIGENCE_ALLOW_CROSS_WORKTREE || "").trim() === "1";

  if (
    servingWorkspace &&
    dataWorkspace &&
    servingWorkspace.stream !== dataWorkspace.stream &&
    !allowCross
  ) {
    return {
      ok: false,
      error: "CONTACT_INTELLIGENCE_CROSS_WORKTREE_DATA_ROOT",
      message:
        `Contact Intelligence data root resolves under stream=${dataWorkspace.stream} ` +
        `(${dataWorkspace.pathNormalized}) while server code is stream=${servingWorkspace.stream} ` +
        `(${servingWorkspace.pathNormalized}). Set CONTACT_INTELLIGENCE_DATA_ROOT to a path under the serving workspace ` +
        `(or CONTACT_INTELLIGENCE_ALLOW_CROSS_WORKTREE=1 only with founder authorization).`,
      root: resolved.root,
      source: resolved.source,
      servingWorkspace: {
        stream: servingWorkspace.stream,
        path: servingWorkspace.pathNormalized,
      },
      dataWorkspace: {
        stream: dataWorkspace.stream,
        path: dataWorkspace.pathNormalized,
      },
    };
  }

  return {
    ok: true,
    root: resolved.root,
    source: resolved.source,
    envOverride: resolved.envOverride,
    servingWorkspace: servingWorkspace
      ? { stream: servingWorkspace.stream, path: servingWorkspace.pathNormalized }
      : null,
    dataWorkspace: dataWorkspace
      ? { stream: dataWorkspace.stream, path: dataWorkspace.pathNormalized }
      : null,
  };
}

/**
 * Recommended HE local runtime root (deliberate, Airtable-first staging only).
 * Prefers worktree registry path for stream=he when available.
 */
export function recommendedHotelExplorerContactIntelligenceDataRoot(opts = {}) {
  const repoRoot = path.resolve(opts.repoRoot || CONTACT_INTELLIGENCE_MODULE_REPO_ROOT);
  try {
    const registry = opts.registry || loadWorktreeRegistry(repoRoot);
    const he = listRegisteredWorkspaces(registry).find((ws) => ws.stream === "he");
    if (he?.path) {
      return path.join(he.path, CONTACT_INTELLIGENCE_DEFAULT_RELATIVE);
    }
  } catch (_err) {
    /* fall through */
  }
  return path.join(repoRoot, CONTACT_INTELLIGENCE_DEFAULT_RELATIVE);
}
