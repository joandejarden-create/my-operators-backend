#!/usr/bin/env node
/**
 * Build CALA growth signal fixture JSON for API and dashboard consumption.
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  CALA_GROWTH_PROFILES,
  buildGrowthSignalCoverageSummary,
  validateCalaGrowthProfiles,
} from "../lib/radar-buildout/growth-signals/index.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const validation = validateCalaGrowthProfiles();
if (!validation.ok) {
  console.error("Growth signal validation failed:");
  for (const row of validation.results) {
    if (row.errors.length) {
      console.error(`- ${row.profile.country} / ${row.profile.submarket}:`, row.errors);
    }
  }
  process.exit(1);
}

const fixture = {
  schema: "cala-submarket-growth-signals-v1",
  generatedAt: new Date().toISOString(),
  description:
    "Tier 2 growth signals for owner/brand early-entry site selection — metadata attached to submarkets and Future Growth Node anchors.",
  summary: buildGrowthSignalCoverageSummary(),
  profiles: CALA_GROWTH_PROFILES,
};

const paths = [
  "fixtures/cala-submarket-growth-signals.json",
  "public/fixtures/cala-submarket-growth-signals.json",
  "data/cala-submarket-growth-signals.json",
];

for (const rel of paths) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("CALA growth signals fixture written");
console.log("Profiles:", fixture.profiles.length);
console.log("Signals:", fixture.summary.totals.signals);
console.log("Countries:", fixture.summary.totals.countries);
if (validation.warningCount) {
  console.log("Warnings:", validation.warningCount);
}
