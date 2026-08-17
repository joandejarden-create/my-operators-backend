#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildGuadeloupeTiDeltaFixture } from "../lib/radar-buildout/guadeloupe-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildGuadeloupeTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-guadeloupe-real.json",
  "public/fixtures/travel-infrastructure-guadeloupe-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Guadeloupe Countrywide TI:", fixture.points.length, "records");
