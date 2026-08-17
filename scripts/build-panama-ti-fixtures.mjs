#!/usr/bin/env node
/**
 * Build Panama countrywide Travel Infrastructure delta fixture.
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildPanamaTiDeltaFixture } from "../lib/radar-buildout/panama-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildPanamaTiDeltaFixture();

const paths = [
  "fixtures/travel-infrastructure-panama-countrywide-real.json",
  "public/fixtures/travel-infrastructure-panama-countrywide-real.json",
];

for (const rel of paths) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Panama TI delta:", fixture.points.length, "records");
console.log("By type:", fixture.summary.byPointType);
