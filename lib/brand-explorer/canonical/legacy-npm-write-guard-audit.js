/**
 * Audit + classify Brand Explorer npm scripts that can write / apply.
 * Fail-closed for factory-critical entry points reachable from the restored factory.
 */
import { existsSync, readFileSync } from "fs";
import { join } from "path";

/** Historical apply/write-capable script name patterns. */
export const APPLY_CAPABLE_SCRIPT_RE =
  /brand-explorer-.*(?:writer|remediation|wave1[2-7]-factory|full-tab-factory-build|active-release-apply|v40-active-release-remediation|factory-promotion|cleanup|apply)/;

/**
 * Factory-reachable high-risk entry points that MUST be npm-wrapped with
 * brand-explorer-legacy-guard.mjs (or refuseLegacyApply) before Phase 2 merge.
 */
export const FACTORY_CRITICAL_NPM_SCRIPTS = Object.freeze([
  "brand-explorer-full-tab-factory-build",
  "brand-explorer-wave12-factory",
  "brand-explorer-wave13-factory",
  "brand-explorer-wave14-factory",
  "brand-explorer-wave15-factory",
  "brand-explorer-v43-active-release-apply",
  "brand-explorer-v40-active-release-remediation",
  "brand-explorer-atomic-collection-remediation-apply",
  "brand-explorer-tab-factory-remediation",
  "brand-explorer-built-blocked-remediation",
  "brand-explorer-rendered-field-completeness-remediation",
]);

function isGuarded(command) {
  const cmd = String(command || "");
  return cmd.includes("brand-explorer-legacy-guard.mjs") || cmd.includes("refuseLegacyApply");
}

export function listApplyCapableScripts(pkg) {
  const rows = [];
  for (const [name, command] of Object.entries(pkg.scripts || {})) {
    const cmd = String(command);
    if (!APPLY_CAPABLE_SCRIPT_RE.test(cmd) && !APPLY_CAPABLE_SCRIPT_RE.test(name)) continue;
    rows.push({
      name,
      command: cmd,
      guarded: isGuarded(cmd),
      factoryCritical: FACTORY_CRITICAL_NPM_SCRIPTS.includes(name),
    });
  }
  return rows;
}

export function auditLegacyNpmWriteGuards(root) {
  const pkgPath = join(root, "package.json");
  const pkg = JSON.parse(readFileSync(pkgPath, "utf8"));
  const rows = listApplyCapableScripts(pkg);
  const unguarded = rows.filter((r) => !r.guarded);
  const factoryCriticalUnguarded = rows.filter((r) => r.factoryCritical && !r.guarded).map((r) => r.name);
  const guardModuleOk =
    existsSync(join(root, "lib/brand-explorer/canonical/legacy-apply-guard.js")) &&
    existsSync(join(root, "scripts/brand-explorer-legacy-guard.mjs"));

  return {
    pass: factoryCriticalUnguarded.length === 0 && guardModuleOk,
    guardModuleOk,
    applyCapableCount: rows.length,
    guardedCount: rows.filter((r) => r.guarded).length,
    unguardedCount: unguarded.length,
    unguardedNames: unguarded.map((r) => r.name).slice(0, 120),
    factoryCritical: FACTORY_CRITICAL_NPM_SCRIPTS.slice(),
    factoryCriticalUnguarded,
    note: "Historical writers remain on disk; factory-critical npm entry points must be fail-closed via legacy-guard wrapper. Break-glass: BRAND_EXPLORER_ALLOW_LEGACY_APPLY=1",
  };
}

/**
 * Wrap unguarded apply-capable package.json scripts with legacy-guard.
 * Idempotent. Does not delete scripts.
 */
export function wrapUnguardedLegacyNpmScripts(pkg, { factoryCriticalOnly = false } = {}) {
  const next = { ...pkg, scripts: { ...pkg.scripts } };
  const changed = [];
  for (const row of listApplyCapableScripts(pkg)) {
    if (row.guarded) continue;
    if (factoryCriticalOnly && !row.factoryCritical) continue;
    const cmd = String(row.command);
    const m = cmd.match(/^node\s+(scripts\/[A-Za-z0-9_./-]+\.mjs)(.*)$/);
    if (!m) continue;
    next.scripts[row.name] = `node scripts/brand-explorer-legacy-guard.mjs ${m[1]}${m[2] || ""}`.trim();
    changed.push(row.name);
  }
  return { pkg: next, changed };
}
