#!/usr/bin/env node
/**
 * Mark tracked hotel opportunities.json paths as skip-worktree so
 * git checkout/pull/merge cannot silently replace richer local operational stores.
 *
 * Usage:
 *   node scripts/gdi-protect-hotel-stores-skip-worktree.mjs
 *   node scripts/gdi-protect-hotel-stores-skip-worktree.mjs --unset
 */
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const unset = process.argv.includes("--unset");

function trackedHotelOppPaths() {
  const out = execFileSync(
    "git",
    ["ls-files", "data/group-demand-intelligence/hotels/*/opportunities.json"],
    { cwd: ROOT, encoding: "utf8" }
  );
  return out
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean);
}

const paths = trackedHotelOppPaths();
const results = [];
for (const rel of paths) {
  const abs = path.join(ROOT, rel);
  if (!fs.existsSync(abs)) {
    results.push({ rel, ok: false, reason: "ABSENT" });
    continue;
  }
  try {
    execFileSync(
      "git",
      ["update-index", unset ? "--no-skip-worktree" : "--skip-worktree", rel],
      { cwd: ROOT, stdio: "pipe" }
    );
    results.push({ rel, ok: true, mode: unset ? "no-skip-worktree" : "skip-worktree" });
  } catch (err) {
    results.push({ rel, ok: false, reason: String(err?.message || err) });
  }
}

console.log(
  JSON.stringify(
    {
      ok: results.every((r) => r.ok),
      action: unset ? "unset-skip-worktree" : "set-skip-worktree",
      count: results.length,
      results,
      note: "Local-only protection. Pair with integrity guard tests; do not rely on skip-worktree alone for new clones.",
    },
    null,
    2
  )
);
