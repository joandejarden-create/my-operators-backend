#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildSaintKittsAndNevisTiDeltaFixture } from "../lib/radar-buildout/saint-kitts-and-nevis-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildSaintKittsAndNevisTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-saint-kitts-and-nevis-countrywide-real.json",
  "public/fixtures/travel-infrastructure-saint-kitts-and-nevis-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Saint Kitts and Nevis TI delta:", fixture.points.length, "records");
