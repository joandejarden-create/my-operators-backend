/**
 * Durable factory paths — independent of temporary Git worktrees / branch lifecycle.
 *
 * Priority:
 *   1. HIF_DURABLE_ROOT env
 *   2. DEALALITY_HIF_DURABLE_ROOT env
 *   3. C:/Dev/dealality/hif-durable (Windows Dealality default)
 *   4. <cwd>/data/hotel-intelligence-factory (dev fallback only — warn)
 */

import fs from "node:fs";
import path from "node:path";
import os from "node:os";

export const HIF_DURABLE_PATHS_V1 = "hotel-intelligence-factory-durable-paths-v1";

const WINDOWS_DEFAULT = "C:\\Dev\\dealality\\hif-durable";

export function resolveDurableRoot(opts = {}) {
  const fromEnv =
    String(opts.durableRoot || process.env.HIF_DURABLE_ROOT || process.env.DEALALITY_HIF_DURABLE_ROOT || "").trim();
  if (fromEnv) {
    return { root: path.resolve(fromEnv), source: "ENV", worktreeIndependent: true };
  }
  if (process.platform === "win32" && fs.existsSync(path.dirname(WINDOWS_DEFAULT))) {
    return { root: WINDOWS_DEFAULT, source: "WINDOWS_DEFAULT", worktreeIndependent: true };
  }
  const cwd = opts.cwd || process.cwd();
  return {
    root: path.join(cwd, "data", "hotel-intelligence-factory"),
    source: "CWD_FALLBACK_DEV_ONLY",
    worktreeIndependent: false,
    warning: "Set HIF_DURABLE_ROOT so checkpoints/caches survive worktree/branch cleanup",
  };
}

export function durableLayout(durableRoot) {
  const root = path.resolve(durableRoot);
  return {
    root,
    runs: path.join(root, "runs"),
    cache: path.join(root, "cache"),
    ledgers: path.join(root, "ledgers"),
    reports: path.join(root, "reports"),
    auth: path.join(root, "authorization"),
  };
}

export function ensureDurableLayout(durableRoot) {
  const layout = durableLayout(durableRoot);
  for (const p of Object.values(layout)) {
    if (p === layout.root) continue;
    fs.mkdirSync(p, { recursive: true });
  }
  fs.mkdirSync(layout.root, { recursive: true });
  return layout;
}

export function runDirDurable(durableRoot, runId) {
  return path.join(durableLayout(durableRoot).runs, runId);
}

/** Optional mirror into a repo worktree for PR packaging — never the resume SoT. */
export function repoReportMirrorDir(cwd, runId) {
  return path.join(cwd, "reports", "hotel-intelligence-factory", runId);
}

export default {
  resolveDurableRoot,
  durableLayout,
  ensureDurableLayout,
  runDirDurable,
  repoReportMirrorDir,
};
