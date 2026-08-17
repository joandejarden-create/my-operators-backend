#!/usr/bin/env node
/**
 * Brand Explorer v36D action router (dry-run by default).
 *
 *   npm run brand-explorer-v36d-action-router -- --brands design-hotels,... --dry-run
 */
import "dotenv/config";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  DEFAULT_TEST_BRANDS,
  runV36DActionRouter,
  writeV36DReports,
  V36D_VERSION,
} from "../lib/partner-intelligence/brand-explorer-v36d-action-router.js";
import { parseV36DApplyMode, V36D_APPLY_MODES } from "../lib/partner-intelligence/brand-explorer-apply-gate-enforcer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

const VALIDATION_SCRIPTS = [
  "test:brand-explorer-active-profile-staged-apply",
  "test:partner-intelligence-publish-readiness",
  "test:partner-intelligence-profile-governance-publish",
];

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1].split(",").map((s) => s.trim()).filter(Boolean)
      : [...DEFAULT_TEST_BRANDS];
  const mode = parseV36DApplyMode(argv);
  const dryRun = mode === V36D_APPLY_MODES.DRY_RUN || argv.includes("--dry-run");
  return { brands, dryRun, mode, argv };
}

function runValidation(scriptName) {
  const res = spawnSync("npm", ["run", scriptName], {
    cwd: ROOT,
    shell: true,
    encoding: "utf8",
    env: process.env,
  });
  return { script: scriptName, status: res.status === 0 ? "pass" : "fail", exitCode: res.status };
}

async function main() {
  const { brands, dryRun, mode, argv } = parseArgs(process.argv.slice(2));

  if (!dryRun || mode !== V36D_APPLY_MODES.DRY_RUN) {
    console.error(
      "v36D refuses Airtable writes. Use --dry-run only. Future apply gates are documented but not executed in this version."
    );
    process.exit(1);
  }

  const report = await runV36DActionRouter({ brands, dryRun: true, argv, rootDir: ROOT });
  const validationTests = VALIDATION_SCRIPTS.map(runValidation);
  report.validationTests = validationTests;

  const paths = writeV36DReports(report, ROOT);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docPath}`);
  console.log(`Router: ${V36D_VERSION} (read-only / dry-run)`);

  for (const row of report.batchTable) {
    console.log(`  ${row.brandSlug}: ${row.recommendedAction} (state=${row.currentState})`);
  }

  for (const t of validationTests) {
    console.log(`  ${t.script}: ${t.status}${t.exitCode != null ? ` (${t.exitCode})` : ""}`);
  }

  const mismatches = Object.entries(report.routingMatch || {}).filter(([, ok]) => !ok);
  if (mismatches.length) {
    console.error("Routing mismatches:", mismatches.map(([s]) => s).join(", "));
    process.exit(1);
  }

  const failed = validationTests.filter((t) => t.status !== "pass");
  if (failed.length) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
