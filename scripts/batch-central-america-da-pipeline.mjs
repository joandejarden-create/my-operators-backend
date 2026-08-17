#!/usr/bin/env node
/**
 * Build + verify + import Central America countrywide demand anchors.
 *   node scripts/batch-central-america-da-pipeline.mjs
 *   node scripts/batch-central-america-da-pipeline.mjs --slug belize --skip-import
 */
import { execSync } from "child_process";
import { existsSync } from "fs";
import { join } from "path";
import { CENTRAL_AMERICA_COUNTRY_BUILDS } from "../lib/radar-buildout/central-america-build-manifest.js";

const root = process.cwd();
const slugArg = (() => {
  const idx = process.argv.indexOf("--slug");
  return idx >= 0 ? process.argv[idx + 1] : null;
})();
const skipImport = process.argv.includes("--skip-import");
const skipVerify = process.argv.includes("--skip-verify");

const jobs = slugArg
  ? CENTRAL_AMERICA_COUNTRY_BUILDS.filter((j) => j.slug === slugArg)
  : CENTRAL_AMERICA_COUNTRY_BUILDS;

function run(cmd) {
  console.log("\n$", cmd);
  execSync(cmd, { cwd: root, stdio: "inherit" });
}

run("node scripts/build-central-america-countrywide-fixtures.mjs" + (slugArg ? ` --slug ${slugArg}` : ""));

for (const job of jobs) {
  const candidates = `fixtures/demand-anchors-${job.slug}-countrywide-candidates.json`;
  const report = `fixtures/demand-anchors-${job.slug}-google-verification-report.json`;
  const real = `fixtures/demand-anchors-${job.slug}-countrywide-real.json`;
  const micro = `fixtures/demand-anchors-${job.slug}-countrywide-micro-pass.json`;

  if (!skipVerify) {
    const countryArg = job.country.includes(" ") ? `"${job.country}"` : job.country;
    run(
      `node scripts/verify-demand-anchors-google.mjs --file ${candidates} --country ${countryArg} --output ${report} --verified-output ${real} --cache --max-requests 280 --allow-medium`
    );
    run("node scripts/sanitize-demand-anchor-fixtures-for-import.mjs --file " + real);
    if (existsSync(join(root, real))) {
      run(`node scripts/build-market-micro-pass-fixtures.mjs --slug ${job.slug}-countrywide`);
      if (existsSync(join(root, micro))) {
        run("node scripts/sanitize-demand-anchor-fixtures-for-import.mjs --file " + micro);
      }
    }
  }

  if (!skipImport) {
    for (const rel of [real, micro]) {
      if (!existsSync(join(root, rel))) continue;
      run(
        `node scripts/import-demand-anchors-commit.mjs --file ${rel} --require-verified-fixture --apply`
      );
    }
  }
}

console.log("\nCentral America DA pipeline complete.");
