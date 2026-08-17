#!/usr/bin/env node
/**
 * Verify + merge + import South America DA (excluding Brazil).
 */
import { execSync } from "child_process";
import { existsSync } from "fs";
import { join } from "path";
import {
  SOUTH_AMERICA_COUNTRY_BUILDS,
  PERU_LIMA_CUSCO_BUILD,
} from "../lib/radar-buildout/south-america-build-manifest.js";

const root = process.cwd();
const slugArg = (() => {
  const idx = process.argv.indexOf("--slug");
  return idx >= 0 ? process.argv[idx + 1] : null;
})();
const skipImport = process.argv.includes("--skip-import");
const skipVerify = process.argv.includes("--skip-verify");
const peruOnly = process.argv.includes("--peru-only");
const newOnly = process.argv.includes("--new-only");

function run(cmd) {
  console.log("\n$", cmd);
  execSync(cmd, { cwd: root, stdio: "inherit" });
}

const jobs = slugArg
  ? SOUTH_AMERICA_COUNTRY_BUILDS.filter((j) => j.slug === slugArg)
  : SOUTH_AMERICA_COUNTRY_BUILDS;

if (!peruOnly) {
  run(
    "node scripts/build-south-america-countrywide-fixtures.mjs" + (slugArg ? ` --slug ${slugArg}` : "")
  );
}

function processJob(job) {
  const candidates = `fixtures/demand-anchors-${job.slug}-countrywide-candidates.json`;
  const report = `fixtures/demand-anchors-${job.slug}-google-verification-report.json`;
  const real = `fixtures/demand-anchors-${job.slug}-countrywide-real.json`;
  const micro = `fixtures/demand-anchors-${job.slug}-countrywide-micro-pass.json`;
  const countryArg = job.country.includes(" ") ? `"${job.country}"` : job.country;

  if (!skipVerify) {
    run(
      `node scripts/verify-demand-anchors-google.mjs --file ${candidates} --country ${countryArg} --output ${report} --verified-output ${real} --cache --max-requests 280 --allow-medium`
    );
    run(
      `node scripts/merge-candidates-into-verified-fixture.mjs ${candidates}:${real}`
    );
    run(`node scripts/sanitize-demand-anchor-fixtures-for-import.mjs --file ${real}`);
    run(`node scripts/build-market-micro-pass-fixtures.mjs --slug ${job.slug}-countrywide`);
    if (existsSync(join(root, micro))) {
      run(`node scripts/sanitize-demand-anchor-fixtures-for-import.mjs --file ${micro}`);
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

for (const job of jobs) {
  if (peruOnly) continue;
  processJob(job);
}

if (!slugArg || slugArg === "peru-lima-cusco") {
  if (!newOnly) {
    const p = PERU_LIMA_CUSCO_BUILD;
    if (!skipVerify) {
      run(`node scripts/sanitize-demand-anchor-fixtures-for-import.mjs --file ${p.realFixture}`);
      run(`node scripts/build-market-micro-pass-fixtures.mjs --slug peru-lima-cusco`);
      if (existsSync(join(root, p.microFixture))) {
        run(`node scripts/sanitize-demand-anchor-fixtures-for-import.mjs --file ${p.microFixture}`);
      }
    }
    if (!skipImport) {
      for (const rel of [p.realFixture, p.microFixture]) {
        if (!existsSync(join(root, rel))) continue;
        run(
          `node scripts/import-demand-anchors-commit.mjs --file ${rel} --require-verified-fixture --apply`
        );
      }
    }
  }
}

console.log("\nSouth America DA pipeline complete.");
