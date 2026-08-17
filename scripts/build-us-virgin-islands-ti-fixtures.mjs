#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildUsVirginIslandsTiDeltaFixture } from "../lib/radar-buildout/us-virgin-islands-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildUsVirginIslandsTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-us-virgin-islands-real.json",
  "public/fixtures/travel-infrastructure-us-virgin-islands-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("U.S. Virgin Islands Countrywide TI:", fixture.points.length, "records");
