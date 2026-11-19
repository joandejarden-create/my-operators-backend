#!/usr/bin/env node
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  findWorkspaceByStream,
  listRegisteredWorkspaces,
  loadWorktreeRegistry,
  normalizePath,
} from "../lib/dealality-dev-os/worktree-registry.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const registry = loadWorktreeRegistry(ROOT);

assert.equal(registry.schemaVersion, 1);
assert.equal(registry.workspaces.length, 5);
for (const id of ["integration", "hotel-explorer", "adp", "gdi", "brand-explorer"]) {
  assert.ok(registry.workspaces.some((w) => w.id === id), `missing workspace ${id}`);
}
assert.equal(findWorkspaceByStream(registry, "he").pathNormalized, normalizePath("C:/Dev/dealality/ws-hotel-explorer"));
assert.equal(findWorkspaceByStream(registry, "be").pathNormalized, normalizePath("C:/Dev/dealality/ws-brand-explorer"));
assert.equal(findWorkspaceByStream(registry, "be").defaultBranch, "ws/brand-explorer");
assert.ok(listRegisteredWorkspaces(registry).some((w) => w.id === "dev-os-governance-v1"));
assert.ok(listRegisteredWorkspaces(registry).some((w) => w.id === "dev-os-post-merge-sync-v1"));
assert.ok((registry.protectedRefs || []).includes("main"));
assert.ok(
  (registry.notes || []).some((n) => String(n).includes("dealality:post-merge-sync")),
  "registry notes must mention post-merge sync",
);
console.log("PASS: worktree registry schema and permanent workspaces");
