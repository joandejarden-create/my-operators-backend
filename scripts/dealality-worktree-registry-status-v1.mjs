#!/usr/bin/env node
/**
 * Compare static registry intent vs live Git state (read-only).
 * Product chats may run this; only Integration Controller edits the registry file.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  listRegisteredWorkspaces,
  loadWorktreeRegistry,
  normalizePath,
} from "../lib/dealality-dev-os/worktree-registry.js";
import { collectDirtyPaths, parseWorktreeList, resolveGitRoot } from "../lib/dealality-dev-os/git-preflight.js";
import { execFileSync } from "node:child_process";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const json = process.argv.includes("--json");

function git(root, args) {
  try {
    return execFileSync("git", args, { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  } catch {
    return "";
  }
}

const root = resolveGitRoot(ROOT);
const registry = loadWorktreeRegistry(root);
const live = parseWorktreeList(root);
const liveByPath = new Map(live.map((w) => [normalizePath(w.path), w]));

const report = listRegisteredWorkspaces(registry).map((ws) => {
  const exists = fs.existsSync(ws.path);
  const liveWt = liveByPath.get(ws.pathNormalized) || null;
  let dirtyCount = 0;
  let branch = null;
  let head = null;
  if (liveWt) {
    branch = liveWt.branch || "(detached)";
    head = liveWt.head;
    dirtyCount = collectDirtyPaths(ws.path).length;
  }
  const inconsistencies = [];
  if (ws.lifecycle === "permanent" && ws.status !== "planned" && !exists) {
    inconsistencies.push("registered_permanent_path_missing");
  }
  if (ws.status === "planned" && exists) {
    inconsistencies.push("planned_path_already_exists");
  }
  if (liveWt && ws.expectedBranchPattern) {
    try {
      if (branch && branch !== "(detached)" && !new RegExp(ws.expectedBranchPattern).test(branch)) {
        inconsistencies.push(`branch_pattern_mismatch:${branch}`);
      }
    } catch {
      inconsistencies.push("invalid_branch_pattern");
    }
  }
  if (ws.id === "integration" && dirtyCount > 0) {
    inconsistencies.push("integration_workspace_dirty");
  }
  return {
    id: ws.id,
    stream: ws.stream,
    path: ws.path,
    status: ws.status,
    exists,
    attached: Boolean(liveWt),
    branch,
    head,
    dirtyCount,
    inconsistencies,
  };
});

const orphanLive = live
  .filter((w) => !listRegisteredWorkspaces(registry).some((r) => r.pathNormalized === normalizePath(w.path)))
  .map((w) => ({ path: w.path, branch: w.branch || "(detached)", head: w.head }));

const payload = {
  ok: report.every((r) => r.inconsistencies.length === 0),
  auditedAt: new Date().toISOString(),
  originMain: git(root, ["rev-parse", "origin/main"]),
  registered: report,
  unregisteredLiveWorktrees: orphanLive.length,
  unregisteredSample: orphanLive.slice(0, 15),
};

if (json) {
  process.stdout.write(`${JSON.stringify(payload, null, 2)}\n`);
} else {
  console.log(`[dealality:registry-status] ok=${payload.ok} unregisteredLive=${payload.unregisteredLiveWorktrees}`);
  for (const r of payload.registered) {
    const flag = r.inconsistencies.length ? "DRIFT" : "OK";
    console.log(
      `  [${flag}] ${r.id} exists=${r.exists} attached=${r.attached} branch=${r.branch || "-"} dirty=${r.dirtyCount} ${r.inconsistencies.join(",")}`,
    );
  }
}

process.exit(0);
