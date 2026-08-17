#!/usr/bin/env node
/**
 * Build Mexico Cancún Travel Infrastructure delta fixture.
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildMexicoCancunTiDeltaFixture } from "../lib/radar-buildout/mexico-cancun-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildMexicoCancunTiDeltaFixture();

const paths = [
  "fixtures/travel-infrastructure-mexico-cancun-riviera-maya-delta-real.json",
  "public/fixtures/travel-infrastructure-mexico-cancun-riviera-maya-delta-real.json",
];

for (const rel of paths) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Mexico Cancún TI delta:", fixture.points.length, "records");
console.log("Corrections flagged:", fixture.corrections.length);
