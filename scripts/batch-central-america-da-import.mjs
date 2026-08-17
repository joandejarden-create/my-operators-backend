#!/usr/bin/env node
/**
 * Import Central America countrywide DA fixtures.
 */
import { execSync } from "child_process";
import { existsSync } from "fs";
import { join } from "path";
import { CENTRAL_AMERICA_COUNTRY_BUILDS } from "../lib/radar-buildout/central-america-build-manifest.js";

const root = process.cwd();

for (const job of CENTRAL_AMERICA_COUNTRY_BUILDS) {
  for (const suffix of ["countrywide-real", "countrywide-micro-pass"]) {
    const rel = `fixtures/demand-anchors-${job.slug}-${suffix}.json`;
    if (!existsSync(join(root, rel))) continue;
    console.log("\nDA IMPORT", rel);
    const out = execSync(
      `node scripts/import-demand-anchors-commit.mjs --file "${rel}" --require-verified-fixture --apply`,
      { cwd: root, encoding: "utf8" }
    );
    console.log(out.split("\n").slice(-4).join("\n"));
  }
}

console.log("\nCentral America DA import complete.");
