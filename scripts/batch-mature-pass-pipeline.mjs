#!/usr/bin/env node
/**
 * Verify, sanitize, and import mature-pass DA + TI fixtures.
 */
import { execSync } from "child_process";
import { existsSync } from "fs";
import { join } from "path";
import {
  MATURE_PASS_BUILDS,
  MATURE_TI_ONLY_BUILDS,
} from "../lib/radar-buildout/mature-pass-build-manifest.js";

const root = process.cwd();
const slugArg = (() => {
  const idx = process.argv.indexOf("--slug");
  return idx >= 0 ? process.argv[idx + 1] : null;
})();
const skipVerify = process.argv.includes("--skip-verify");
const skipImport = process.argv.includes("--skip-import");
const tiOnly = process.argv.includes("--ti-only");
const daOnly = process.argv.includes("--da-only");

function run(cmd) {
  console.log("\n$", cmd);
  execSync(cmd, { cwd: root, stdio: "inherit" });
}

function importFile(script, rel) {
  if (!existsSync(join(root, rel))) {
    console.warn("SKIP missing:", rel);
    return;
  }
  run(
    `node scripts/${script} --file "${rel}" --require-verified-fixture --apply`
  );
}

const daJobs = slugArg
  ? MATURE_PASS_BUILDS.filter((j) => j.slug === slugArg)
  : MATURE_PASS_BUILDS;
const tiOnlyJobs = slugArg
  ? MATURE_TI_ONLY_BUILDS.filter((j) => j.slug === slugArg)
  : MATURE_TI_ONLY_BUILDS;

run("node scripts/build-mature-pass-fixtures.mjs" + (slugArg ? ` --slug ${slugArg}` : ""));

if (!tiOnly) {
  for (const job of daJobs) {
    const candidates = `fixtures/demand-anchors-${job.slug}-candidates.json`;
    const report = `fixtures/demand-anchors-${job.slug}-google-verification-report.json`;
    const real = `fixtures/demand-anchors-${job.slug}-real.json`;
    const countryArg = job.country.includes(" ") ? `"${job.country}"` : job.country;

    if (!skipVerify) {
      run(
        `node scripts/verify-demand-anchors-google.mjs --file ${candidates} --country ${countryArg} --output ${report} --verified-output ${real} --cache --max-requests 200 --allow-medium`
      );
      run(
        `node scripts/merge-candidates-into-verified-fixture.mjs ${candidates}:${real}`
      );
    }
    run(`node scripts/sanitize-demand-anchor-fixtures-for-import.mjs --file ${real}`);
    if (!skipImport) {
      importFile("import-demand-anchors-commit.mjs", real);
    }
  }
}

if (!daOnly) {
  for (const job of [...daJobs, ...tiOnlyJobs]) {
    const tiRel = `fixtures/travel-infrastructure-${job.slug}-real.json`;
    if (!skipImport) {
      importFile("import-travel-infrastructure-commit.mjs", tiRel);
    }
  }
}

console.log("\nMature pass pipeline complete.");
