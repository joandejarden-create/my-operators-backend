#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildSaintLuciaTiDeltaFixture } from "../lib/radar-buildout/saint-lucia-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildSaintLuciaTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-saint-lucia-countrywide-real.json",
  "public/fixtures/travel-infrastructure-saint-lucia-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Saint Lucia TI delta:", fixture.points.length, "records");
