#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildBonaireTiDeltaFixture } from "../lib/radar-buildout/bonaire-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildBonaireTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-bonaire-real.json",
  "public/fixtures/travel-infrastructure-bonaire-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Bonaire Countrywide TI:", fixture.points.length, "records");
