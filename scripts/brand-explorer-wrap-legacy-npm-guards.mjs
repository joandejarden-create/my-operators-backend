#!/usr/bin/env node
/**
 * Wrap Brand Explorer apply-capable npm scripts with legacy-guard (fail-closed).
 * Default: factory-critical only. Pass --all to wrap every apply-capable script.
 * Dry-run by default; pass --apply to write package.json.
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { wrapUnguardedLegacyNpmScripts, auditLegacyNpmWriteGuards } from "../lib/brand-explorer/canonical/legacy-npm-write-guard-audit.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const apply = process.argv.includes("--apply");
const all = process.argv.includes("--all");

const pkgPath = join(ROOT, "package.json");
const pkg = JSON.parse(readFileSync(pkgPath, "utf8"));
const before = auditLegacyNpmWriteGuards(ROOT);
const { pkg: next, changed } = wrapUnguardedLegacyNpmScripts(pkg, { factoryCriticalOnly: !all });

if (apply && changed.length) {
  writeFileSync(pkgPath, `${JSON.stringify(next, null, 2)}\n`);
}

const after = apply ? auditLegacyNpmWriteGuards(ROOT) : before;
console.log(
  JSON.stringify(
    {
      apply,
      scope: all ? "all_apply_capable" : "factory_critical_only",
      changedCount: changed.length,
      changed: changed.slice(0, 80),
      before: {
        guarded: before.guardedCount,
        unguarded: before.unguardedCount,
        factoryCriticalUnguarded: before.factoryCriticalUnguarded,
      },
      after: apply
        ? {
            guarded: after.guardedCount,
            unguarded: after.unguardedCount,
            factoryCriticalUnguarded: after.factoryCriticalUnguarded,
          }
        : null,
      writes: apply ? 1 : 0,
    },
    null,
    2,
  ),
);
process.exit(apply && after.factoryCriticalUnguarded.length ? 1 : 0);
