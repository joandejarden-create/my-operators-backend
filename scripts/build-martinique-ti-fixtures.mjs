#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildMartiniqueTiDeltaFixture } from "../lib/radar-buildout/martinique-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildMartiniqueTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-martinique-real.json",
  "public/fixtures/travel-infrastructure-martinique-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Martinique Countrywide TI:", fixture.points.length, "records");
