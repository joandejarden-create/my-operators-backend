/**
 * Dealality Dev OS — static worktree registry loader (intent only).
 * Live Git state is never treated as authoritative from this file.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
export const REGISTRY_RELATIVE_PATH = "docs/ai-build-system/worktree-registry.json";

export function normalizePath(p) {
  return String(p || "")
    .replace(/\\/g, "/")
    .replace(/\/+$/, "")
    .replace(/^([A-Za-z]):/, (_, d) => d.toUpperCase() + ":");
}

export function loadWorktreeRegistry(root = ROOT) {
  const filePath = path.join(root, REGISTRY_RELATIVE_PATH);
  if (!fs.existsSync(filePath)) {
    throw new Error(`worktree registry missing: ${filePath}`);
  }
  const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
  if (!raw || raw.schemaVersion !== 1 || !Array.isArray(raw.workspaces)) {
    throw new Error("worktree registry schemaVersion must be 1 with workspaces[]");
  }
  const all = [...raw.workspaces, ...(raw.temporaryWorkspaces || [])];
  for (const ws of all) {
    if (!ws.id || !ws.stream || !ws.path || !ws.lifecycle) {
      throw new Error(`invalid workspace entry: ${JSON.stringify(ws)}`);
    }
    ws.pathNormalized = normalizePath(ws.path);
  }
  return raw;
}

export function listRegisteredWorkspaces(registry) {
  return [...(registry.workspaces || []), ...(registry.temporaryWorkspaces || [])];
}

export function findWorkspaceByPath(registry, absPath) {
  const needle = normalizePath(absPath);
  return listRegisteredWorkspaces(registry).find((w) => w.pathNormalized === needle) || null;
}

export function findWorkspaceByStream(registry, stream) {
  const s = String(stream || "").toLowerCase();
  return (registry.workspaces || []).find((w) => w.stream === s) || null;
}

export function isProtectedRef(registry, refName) {
  const name = String(refName || "").replace(/^refs\/heads\//, "");
  const protectedRefs = registry.protectedRefs || [];
  return protectedRefs.some((p) => {
    const n = String(p).replace(/^refs\/heads\//, "");
    return n === name || n === `origin/${name}` || p === refName;
  });
}

export function matchesBranchPattern(workspace, branch) {
  if (!workspace?.expectedBranchPattern) return true;
  try {
    return new RegExp(workspace.expectedBranchPattern).test(String(branch || ""));
  } catch {
    return false;
  }
}
