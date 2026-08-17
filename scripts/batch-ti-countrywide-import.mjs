#!/usr/bin/env node
/**
 * Import countrywide Travel Infrastructure delta fixtures.
 */
import { execSync } from "child_process";
import { existsSync } from "fs";
import { join } from "path";
import {
  TI_COUNTRYWIDE_BUILDS,
  PERU_TI_BUILD,
  TI_LEGACY_REPAIR_FIXTURES,
} from "../lib/radar-buildout/ti-countrywide-build-manifest.js";

const root = process.cwd();

function importTi(rel) {
  if (!existsSync(join(root, rel))) {
    console.warn("SKIP (missing):", rel);
    return;
  }
  console.log("\nTI IMPORT", rel);
  const out = execSync(
    `node scripts/import-travel-infrastructure-commit.mjs --file "${rel}" --require-verified-fixture --apply`,
    { cwd: root, encoding: "utf8" }
  );
  console.log(out.split("\n").slice(-4).join("\n"));
}

for (const job of TI_COUNTRYWIDE_BUILDS) {
  importTi(`fixtures/travel-infrastructure-${job.slug}-countrywide-real.json`);
}

importTi(PERU_TI_BUILD.fixture);

for (const rel of TI_LEGACY_REPAIR_FIXTURES) {
  importTi(rel);
}

console.log("\nCountrywide TI import complete.");
