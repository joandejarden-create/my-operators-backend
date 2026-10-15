#!/usr/bin/env node
/**
 * Fuller GDI quality gate before scoped releases.
 *   npm run gdi:quality:full
 *   npm run gdi:quality:full -- --live
 */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-quality-review");
const live = process.argv.includes("--live");
fs.mkdirSync(OUT, { recursive: true });

const results = [];
function run(label, cmd, args, { optional = false } = {}) {
  console.log(`\n=== ${label} ===`);
  const r = spawnSync(cmd, args, {
    cwd: ROOT,
    stdio: "inherit",
    shell: true,
    env: process.env,
  });
  const status = r.status === 0 ? "pass" : optional ? "skipped_or_fail_optional" : "fail";
  results.push({ label, status, exit: r.status });
  return r.status === 0;
}

let ok = true;
ok = run("quality-quick", "node", ["scripts/gdi-quality-quick.mjs"]) && ok;
ok =
  run("integrity-live", "node", [
    "scripts/test-gdi-quality-integrity-v1.mjs",
    ...(live ? ["--live"] : []),
  ]) && ok;
ok =
  run("stylelint-gdi-css", "npx", [
    "stylelint",
    "--config",
    "config/gdi-quality/stylelint.config.cjs",
    "public/css/group-demand-intelligence.css",
  ]) && ok;
run(
  "knip-gdi-advisory",
  "npx",
  ["knip", "--config", "config/gdi-quality/knip.config.mjs", "--no-exit-code"],
  { optional: true }
);
ok =
  run("access-control-smoke", "node", [
    "scripts/test-gdi-quality-access-control-v1.mjs",
  ]) && ok;
ok =
  run("visual-fixtures", "node", ["scripts/gdi-quality-visual-playwright-v1.mjs"]) &&
  ok;
run(
  "customer-copy-scan",
  "node",
  ["scripts/gdi-quality-customer-copy-scan-v1.mjs", ...(live ? ["--live"] : [])],
  { optional: false }
);

const summary = {
  generatedAt: new Date().toISOString(),
  live,
  ok,
  results,
  deferred: [
    {
      tool: "lychee",
      reason: "Binary not installed; use gdi-quality-link-check-v1 bounded fetch instead",
    },
    {
      tool: "vale",
      reason: "Binary not installed; customer-copy-scan covers enum/leak patterns",
    },
    {
      tool: "css-checker",
      reason: "Deferred — would need Go toolchain; stylelint covers GDI CSS correctness",
    },
    {
      tool: "strix",
      reason:
        "Deferred — requires paid model / controlled staging; do not attack production",
    },
    {
      tool: "lighthouse-ci",
      reason:
        "Deferred this pass — run against share surfaces after auth path is available locally",
    },
  ],
};
fs.writeFileSync(
  path.join(OUT, "full-quality-run.json"),
  JSON.stringify(summary, null, 2) + "\n"
);

if (!ok) {
  console.error("\nFAIL gdi:quality:full");
  process.exit(1);
}
console.log("\nPASS gdi:quality:full");
