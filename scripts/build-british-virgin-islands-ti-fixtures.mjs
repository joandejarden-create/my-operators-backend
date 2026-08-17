#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildBritishVirginIslandsTiDeltaFixture } from "../lib/radar-buildout/british-virgin-islands-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildBritishVirginIslandsTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-british-virgin-islands-countrywide-real.json",
  "public/fixtures/travel-infrastructure-british-virgin-islands-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("British Virgin Islands TI delta:", fixture.points.length, "records");
