#!/usr/bin/env node
/**
 * Dealality integrated staging — boot preflight (canonical on main).
 *
 * Validates the supported localhost:8080 startup path:
 *   cwd = ws-integrated-staging  →  node server.js
 *   with DEALALITY_CANONICAL_CHECKOUT=1 (or equivalent).
 *
 * Does NOT depend on lib/dealality-dev-os/integrated-staging-v1.js
 * (that module exists only on unmerged PR #496). A preflight that
 * imports the unmerged module will fail closed here with an explicit message.
 *
 * Usage:
 *   node scripts/dealality-integrated-staging-boot-preflight-v1.mjs
 *   npm run dealality:integrated-staging-boot-preflight
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const UNMERGED_STAGING_MODULE = "lib/dealality-dev-os/integrated-staging-v1.js";

const REQUIRED_BOOT_FILES = [
  "server.js",
  "load-env.js",
  "package.json",
  "public/app.html",
  "lib/group-demand-intelligence/gdi-fullenrich-fallback-v1.js",
];

function fail(code, message, details = {}) {
  const payload = { ok: false, code, message, ...details };
  console.error(JSON.stringify(payload, null, 2));
  process.exit(1);
}

function ok(payload) {
  console.log(JSON.stringify({ ok: true, ...payload }, null, 2));
}

async function main() {
  const cwd = process.cwd();
  const cwdBase = cwd.replace(/[\\/]+$/, "").split(/[\\/]/).pop() || "";
  const rootBase = ROOT.replace(/[\\/]+$/, "").split(/[\\/]/).pop() || "";

  // Prefer running from repo root of this script.
  const checkRoot = ROOT;
  const missing = [];
  for (const rel of REQUIRED_BOOT_FILES) {
    const fp = path.join(checkRoot, rel);
    if (!fs.existsSync(fp)) missing.push(rel);
  }
  if (missing.length) {
    fail("MISSING_BOOT_DEPENDENCY", "Required staging boot files are missing", {
      missing,
      root: checkRoot,
    });
  }

  // Explicit guard: do not silently depend on unmerged PR #496 tooling.
  const unmergedPath = path.join(checkRoot, UNMERGED_STAGING_MODULE);
  const unmergedPresent = fs.existsSync(unmergedPath);
  if (process.env.DEALALITY_REQUIRE_UNMERGED_STAGING_MODULE === "1" && !unmergedPresent) {
    fail(
      "UNMERGED_STAGING_MODULE_REQUIRED_BUT_ABSENT",
      `${UNMERGED_STAGING_MODULE} is not on this checkout. It lives on open PR #496 only. Canonical boot uses node server.js — do not require this module.`,
      { module: UNMERGED_STAGING_MODULE, pr: 496 },
    );
  }

  // Detect stale package.json scripts that import the unmerged module.
  const pkg = JSON.parse(fs.readFileSync(path.join(checkRoot, "package.json"), "utf8"));
  const scripts = pkg.scripts || {};
  const staleScripts = [];
  for (const [name, cmd] of Object.entries(scripts)) {
    if (
      /integrated-staging-v1\.js|dealality-integrated-staging-v1\.mjs/i.test(String(cmd)) &&
      !unmergedPresent
    ) {
      staleScripts.push({ name, cmd });
    }
  }
  if (staleScripts.length) {
    fail(
      "STALE_UNMERGED_STAGING_SCRIPT",
      "package.json references integrated-staging tooling that is not present on this checkout (PR #496 unmerged).",
      { staleScripts, module: UNMERGED_STAGING_MODULE },
    );
  }

  // Try dynamic import of any script named in argv[2] as a smoke (optional).
  const maybeScript = process.argv[2];
  if (maybeScript) {
    const abs = path.isAbsolute(maybeScript) ? maybeScript : path.join(checkRoot, maybeScript);
    if (!fs.existsSync(abs)) {
      fail("PREFLIGHT_SCRIPT_MISSING", `Cannot preflight missing script: ${maybeScript}`, {
        abs,
      });
    }
    try {
      await import(pathToFileURL(abs).href);
    } catch (e) {
      fail("PREFLIGHT_IMPORT_FAILED", String(e?.message || e), {
        script: maybeScript,
        hint:
          e?.code === "ERR_MODULE_NOT_FOUND"
            ? "Dependency missing — check whether the import targets unmerged PR #496 (integrated-staging-v1.js)."
            : undefined,
      });
    }
  }

  ok({
    preflight: "dealality-integrated-staging-boot-preflight-v1",
    root: checkRoot,
    cwd,
    cwdBase,
    rootBase,
    requiredBootFiles: REQUIRED_BOOT_FILES.length,
    unmergedStagingModulePresent: unmergedPresent,
    unmergedStagingModulePath: UNMERGED_STAGING_MODULE,
    canonicalBoot: "node server.js (DEALALITY_CANONICAL_CHECKOUT=1 on ws-integrated-staging)",
    note: "PR #496 integrated-staging-v1.js is optional/unmerged — not required for port 8080 boot.",
  });
}

main().catch((e) => {
  fail("PREFLIGHT_CRASH", String(e?.stack || e));
});
