#!/usr/bin/env node
/**
 * Predeploy ESM import smoke — fail if critical modules cannot load.
 * Prevents Railway "Application failed to respond" from boot-time import crashes.
 *
 * NOTE: Do not dynamically import server.js — it binds PORT on load.
 * Use `node --check server.js` for syntax instead.
 */
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";

const root = process.cwd();

const IMPORT_MODULES = [
  "api/group-demand-intelligence.js",
  "lib/group-demand-intelligence/index.js",
  "lib/group-demand-intelligence/opportunity-list-dto.js",
  "lib/group-demand-intelligence/opportunity-persistence.js",
  "lib/decision-outcomes/index.js",
  "lib/http/with-timeout.js",
];

const SYNTAX_CHECK = ["server.js"];

const failures = [];

for (const rel of SYNTAX_CHECK) {
  const abs = path.join(root, rel);
  if (!fs.existsSync(abs)) {
    failures.push({ module: rel, error: "FILE_MISSING" });
    continue;
  }
  const r = spawnSync(process.execPath, ["--check", abs], {
    cwd: root,
    encoding: "utf8",
  });
  if (r.status !== 0) {
    failures.push({
      module: rel,
      error: "SYNTAX_CHECK_FAILED",
      message: (r.stderr || r.stdout || "").slice(0, 500),
    });
  }
}

for (const rel of IMPORT_MODULES) {
  const abs = path.join(root, rel);
  if (!fs.existsSync(abs)) {
    failures.push({ module: rel, error: "FILE_MISSING" });
    continue;
  }
  try {
    await import(pathToFileURL(abs).href);
  } catch (err) {
    failures.push({
      module: rel,
      error: err?.code || "IMPORT_FAILED",
      message: err?.message || String(err),
    });
  }
}

// Guard: index.js must not export from untracked/missing relative modules.
const indexPath = path.join(root, "lib/group-demand-intelligence/index.js");
if (fs.existsSync(indexPath)) {
  const src = fs.readFileSync(indexPath, "utf8");
  const fromRe = /from\s+["'](\.\/[^"']+)["']/g;
  let m;
  while ((m = fromRe.exec(src))) {
    let rel = m[1];
    if (!rel.endsWith(".js")) rel = `${rel}.js`;
    const target = path.join(root, "lib/group-demand-intelligence", rel);
    if (!fs.existsSync(target)) {
      failures.push({
        module: `lib/group-demand-intelligence/index.js → ${m[1]}`,
        error: "EXPORT_TARGET_MISSING",
        message: `Referenced module missing on disk: ${rel}`,
      });
    }
  }
}

const report = {
  ok: failures.length === 0,
  checked: SYNTAX_CHECK.length + IMPORT_MODULES.length,
  failures,
  generatedAt: new Date().toISOString(),
};

fs.mkdirSync(path.join(root, "reports/deployments"), { recursive: true });
fs.writeFileSync(
  path.join(root, "reports/deployments/latest-esm-import-smoke.json"),
  JSON.stringify(report, null, 2)
);

if (failures.length) {
  console.error("FAIL ESM_IMPORT_SMOKE");
  for (const f of failures) {
    console.error(`  ${f.module}: ${f.error} ${f.message || ""}`);
  }
  process.exit(1);
}

console.log("PASS ESM_IMPORT_SMOKE");
for (const rel of [...SYNTAX_CHECK, ...IMPORT_MODULES]) console.log(`  ok ${rel}`);
process.exit(0);
