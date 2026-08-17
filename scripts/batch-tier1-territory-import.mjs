#!/usr/bin/env node
/**
 * Batch import DA + TI for tier-1 and territory builds.
 */
import { execSync } from "child_process";
import { existsSync } from "fs";
import { join } from "path";

const root = process.cwd();

const DA_JOBS = [
  "fixtures/demand-anchors-colombia-countrywide-real.json",
  "fixtures/demand-anchors-colombia-countrywide-micro-pass.json",
  "fixtures/demand-anchors-panama-countrywide-real.json",
  "fixtures/demand-anchors-panama-countrywide-micro-pass.json",
  "fixtures/demand-anchors-costa-rica-countrywide-real.json",
  "fixtures/demand-anchors-costa-rica-countrywide-micro-pass.json",
  ...[
    "mexico-city",
    "los-cabos",
    "guadalajara",
    "monterrey",
    "puerto-vallarta-riviera-nayarit",
    "merida-yucatan",
    "dominican-republic-mature",
    "cuba",
    "haiti",
    "us-virgin-islands",
    "martinique",
    "guadeloupe",
    "bonaire",
  ].flatMap((slug) => [
    `fixtures/demand-anchors-${slug}-real.json`,
    `fixtures/demand-anchors-${slug}-micro-pass.json`,
  ]),
];

const TI_JOBS = [
  "fixtures/travel-infrastructure-panama-countrywide-real.json",
  "fixtures/travel-infrastructure-costa-rica-countrywide-real.json",
  ...[
    "mexico-city",
    "los-cabos",
    "guadalajara",
    "monterrey",
    "puerto-vallarta-riviera-nayarit",
    "merida-yucatan",
    "dominican-republic-mature",
    "cuba",
    "haiti",
    "us-virgin-islands",
    "martinique",
    "guadeloupe",
    "bonaire",
  ].map((slug) => `fixtures/travel-infrastructure-${slug}-real.json`),
];

const daOnly = process.argv.includes("--da-only");
const tiOnly = process.argv.includes("--ti-only");

function runImport(script, rel) {
  const abs = join(root, rel);
  if (!existsSync(abs)) return;
  console.log(`\n${script.includes("demand") ? "DA" : "TI"} IMPORT ${rel}`);
  try {
    const out = execSync(
      `node scripts/${script} --file "${rel}" --require-verified-fixture --apply`,
      { cwd: root, encoding: "utf8" }
    );
    console.log(out.split("\n").slice(-4).join("\n"));
  } catch (e) {
    console.error(e.stdout?.split("\n").slice(-8).join("\n") || e.message);
  }
}

if (!tiOnly) {
  for (const rel of DA_JOBS) {
    runImport("import-demand-anchors-commit.mjs", rel);
  }
}

if (!daOnly) {
  for (const rel of TI_JOBS) {
    runImport("import-travel-infrastructure-commit.mjs", rel);
  }
}

console.log("\nBatch import complete.");
