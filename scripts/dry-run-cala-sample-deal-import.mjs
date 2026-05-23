#!/usr/bin/env node
/**
 * Build dry-run Airtable import JSON for CALA sample-deal fixtures.
 *
 * Usage:
 *   node scripts/dry-run-cala-sample-deal-import.mjs
 *   node scripts/dry-run-cala-sample-deal-import.mjs --only aeropuerto-cancun-select-service
 *
 * Output: data/cala-sample-import-dry-run/<slug>.import.json + manifest.json
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { validateSampleDealRecord } from "../lib/sample-opportunity-deal-schema.js";
import {
  buildSampleDealImportBundle,
  bundleFieldStats,
} from "../lib/sample-deal-airtable-import.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const FIXTURES_DIR = path.join(ROOT, "fixtures", "sample-deals");
const OUT_DIR = path.join(ROOT, "data", "cala-sample-import-dry-run");

const CALA_FIXTURES = [
  "proyecto-reforma-urban-conversion.example.json",
  "playa-dorada-resort-repositioning.example.json",
  "cartagena-walled-city-collection.example.json",
  "merida-centro-select-service.example.json",
  "san-juan-bay-turnaround.example.json",
  "panama-city-mixed-use-hotel-component.example.json",
  "aeropuerto-cancun-select-service.example.json",
  "cusco-heritage-palace-hotel.example.json",
  "colonial-city-lifestyle-conversion.example.json",
  "riviera-maya-wellness-resort-repositioning.example.json",
  "andean-business-hotel-reflag.example.json",
  "cascadas-lifestyle-hotel-component.example.json",
];

function parseArgs(argv) {
  let only = null;
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--only" && argv[i + 1]) {
      only = argv[++i].replace(/\.example\.json$/, "");
    } else if (argv[i].startsWith("--only=")) {
      only = argv[i].slice("--only=".length).replace(/\.example\.json$/, "");
    }
  }
  return { only };
}

function slugFromFile(file) {
  return file.replace(/\.example\.json$/, "");
}

function main() {
  const { only } = parseArgs(process.argv);
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const manifest = {
    version: 1,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    note: "Review before: node scripts/seed-cala-sample-deals.mjs --apply",
    samples: [],
  };

  let files = CALA_FIXTURES;
  if (only) {
    files = files.filter((f) => slugFromFile(f).includes(only) || f.includes(only));
    if (!files.length) {
      console.error("No fixture matched --only", only);
      process.exit(1);
    }
  }

  for (const file of files) {
    const fixturePath = path.join(FIXTURES_DIR, file);
    const record = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
    const validation = validateSampleDealRecord(record);
    if (!validation.ok) {
      console.error("Validation failed:", file, validation.errors);
      process.exit(1);
    }
    for (const w of validation.warnings) console.warn("WARN", file, w);

    const slug = slugFromFile(file);
    const bundle = buildSampleDealImportBundle(record, { fixtureFile: `fixtures/sample-deals/${file}` });
    bundle.dryRun = true;

    const outPath = path.join(OUT_DIR, `${slug}.import.json`);
    fs.writeFileSync(outPath, JSON.stringify(bundle, null, 2) + "\n", "utf8");

    const stats = bundleFieldStats(bundle);
    manifest.samples.push({
      slug,
      fixtureFile: file,
      sampleId: bundle.sampleId,
      projectName: bundle.projectName,
      expectedReadinessStage: bundle.expectedReadinessStage,
      importFile: `data/cala-sample-import-dry-run/${slug}.import.json`,
      fieldCounts: stats,
      targetListCount: bundle.targetList.length,
      intentionalGapCount: bundle.intentionalGaps.length,
    });
    console.log("Wrote", outPath, stats);
  }

  const manifestPath = path.join(OUT_DIR, "manifest.json");
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + "\n", "utf8");
  console.log("\nManifest:", manifestPath);
  console.log(`Dry-run complete: ${manifest.samples.length} import bundle(s). No Airtable writes.`);
}

main();
