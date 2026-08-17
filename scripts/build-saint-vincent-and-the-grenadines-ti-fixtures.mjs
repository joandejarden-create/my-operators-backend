#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildSaintVincentAndTheGrenadinesTiDeltaFixture } from "../lib/radar-buildout/saint-vincent-and-the-grenadines-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildSaintVincentAndTheGrenadinesTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-saint-vincent-and-the-grenadines-countrywide-real.json",
  "public/fixtures/travel-infrastructure-saint-vincent-and-the-grenadines-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Saint Vincent and the Grenadines TI delta:", fixture.points.length, "records");
